package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const (
	defaultDatabaseURL       = "postgres://pol:pol@localhost:5432/pol_odds?sslmode=disable"
	defaultHTTPTimeoutSec    = 15
	defaultGammaEventsLimit  = 100
	defaultGammaMarketsLimit = 200
	defaultGammaPageSize     = 100
	defaultGammaMaxPages     = 10
	defaultMaxPriceTokens    = 50
	defaultPriceConcurrency  = 4
	defaultAPIRequestRetries = 2
	defaultRetryBaseMS       = 250
	defaultRetryMaxMS        = 2000
	defaultMarketEndGraceHrs = 24
	maxSyncErrorMessageLen   = 1000
)

type syncConfig struct {
	DatabaseURL       string
	GammaBaseURL      string
	CLOBBaseURL       string
	HTTPTimeout       time.Duration
	GammaEventsLimit  int
	GammaMarketsLimit int
	GammaPageSize     int
	GammaMaxPages     int
	SyncMetadata      bool
	SyncPrices        bool
	MaxPriceTokens    int
	PriceConcurrency  int
	PriceTokenIDs     []string
	ActiveMarketsOnly bool
	MarketEndGrace    time.Duration
	Retry             polymarket.RetryConfig
	// Gamma scope filter (2026-05-17 expanded to multi-sport). TagSlugs is
	// comma-separated in env (POLYMARKET_TAG_SLUG=esports,nba,epl,mlb,nhl,tennis)
	// and the sync loops once per slug, deduping events/markets by id so a
	// market appearing in multiple tags is processed once. Empty list = no
	// filter (legacy: pull everything).
	TagSlugs []string
	TagID    string
	Category string
}

type syncSummary struct {
	PagesFetched  int
	EventsSeen    int
	MarketsSeen   int
	TokensSeen    int
	PricesFetched int
	ErrorsCount   int
	Stats         model.PolymarketSyncStats
}

type gammaEventLister interface {
	ListEvents(context.Context, polymarket.ListOptions) ([]polymarket.Event, error)
}

type gammaMarketLister interface {
	ListMarkets(context.Context, polymarket.ListOptions) ([]polymarket.Market, error)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Load .env so DATABASE_URL set in the project root is honored. Without
	// this the command silently falls back to defaultDatabaseURL
	// (localhost:5432) which mismatches the actual DB on port 5433.
	_, _ = config.Load()

	if shouldPrintHelp(os.Args[1:]) {
		printUsage(os.Stdout)
		return
	}

	// Phase 12E: --diagnose-coverage runs a read-only Polymarket Gamma walk and
	// classifies what was returned. It does not open PostgreSQL, does not write
	// rows, and does not start a normal sync. Detected before loadConfig so a
	// failed env-var setup still lets the operator inspect Gamma coverage.
	if remaining, ok := shouldRunDiagnoseCoverage(os.Args[1:]); ok {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := runDiagnoseCoverageCLI(ctx, remaining); err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("polymarket diagnose-coverage canceled")
				return
			}
			slog.Error("polymarket diagnose-coverage failed", "error", err)
			os.Exit(1)
		}
		return
	}

	// Phase 12F: --target-event-id runs a narrow, operator-invoked, read-only
	// ingest of one Gamma event ID and its markets/tokens (and optional fresh
	// CLOB price snapshots). Detected before the env-driven broad sync so the
	// operator does not have to disable the broad sync via env to use it.
	// --dry-run skips PostgreSQL entirely; otherwise the existing
	// DATABASE_URL is opened. The targeted sync never approves aliases and
	// never auto-maps any market.
	if shouldRunTargetedEvent(os.Args[1:]) {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := runTargetedEventCLI(ctx, os.Args[1:]); err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("polymarket targeted-event sync canceled")
				return
			}
			slog.Error("polymarket targeted-event sync failed", "error", err)
			os.Exit(1)
		}
		return
	}

	cfg, err := loadConfig()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// POLYMARKET_SYNC_LOOP_INTERVAL_MS (env, optional): when set > 0,
	// run() repeats with this interval between ticks until SIGINT/SIGTERM.
	// Unset/0 → single tick then exit (backwards compatible).
	loopMs, _ := strconv.Atoi(strings.TrimSpace(os.Getenv("POLYMARKET_SYNC_LOOP_INTERVAL_MS")))
	loopInterval := time.Duration(loopMs) * time.Millisecond

	// POLYMARKET_PRICE_LOOP_INTERVAL_MS (env, optional): when set > 0 AND a
	// sync loop is also enabled, run a SECOND loop that does ONLY the price
	// phase at this (typically shorter) cadence. The main loop is then
	// downgraded to metadata-only. Without this split, the full sync tick
	// takes ~5–6 min (Gamma pagination) so PM prices stay >120 s stale for
	// most of each cycle and signal-engine's `_live` view rejects every edge
	// as STALE_DATA. With it, prices refresh roughly every priceLoopInterval
	// + ~60 s (CLOB fan-out at concurrency 4 across MaxPriceTokens), well
	// inside the freshness gate. Both loops share a mutex so they never
	// double-fetch or double-write at the same instant.
	priceLoopMs, _ := strconv.Atoi(strings.TrimSpace(os.Getenv("POLYMARKET_PRICE_LOOP_INTERVAL_MS")))
	priceLoopInterval := time.Duration(priceLoopMs) * time.Millisecond

	if loopInterval > 0 && priceLoopInterval > 0 {
		// The two phases hit different Polymarket endpoints (Gamma metadata
		// vs. CLOB orderbook prices) and different DB tables (events/markets/
		// tokens vs. price_snapshots), so they run concurrently without
		// serialization. A mutex here would just re-introduce the original
		// hold-the-line-hostage problem the split is meant to fix.
		metaCfg := cfg
		metaCfg.SyncPrices = false
		priceCfg := cfg
		priceCfg.SyncMetadata = false

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			runLoop(ctx, metaCfg, loopInterval, "metadata")
		}()
		go func() {
			defer wg.Done()
			runLoop(ctx, priceCfg, priceLoopInterval, "prices")
		}()
		wg.Wait()
		return
	}

	for {
		err := run(ctx, cfg)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("polymarket sync canceled")
				return
			}
			slog.Error("polymarket sync tick failed", "error", err)
			if loopInterval <= 0 {
				os.Exit(1)
			}
			// In loop mode keep going — transient errors shouldn't kill the
			// daemon. Sleep the interval before retry.
		}
		if loopInterval <= 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(loopInterval):
		}
	}
}

// runLoop drives one phase of the split-mode sync. The phase label only
// shapes log lines — the actual metadata vs. price selection is encoded
// in cfg.SyncMetadata / cfg.SyncPrices, set by the caller before spawning
// each loop.
func runLoop(ctx context.Context, cfg syncConfig, interval time.Duration, phase string) {
	for {
		err := run(ctx, cfg)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("polymarket sync canceled", "phase", phase)
				return
			}
			slog.Error("polymarket sync tick failed", "phase", phase, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// shouldPrintHelp returns true if any provided arg is a help flag. Help is
// detected before loadConfig() so that `polymarket-sync --help` never opens
// PostgreSQL, never constructs an HTTP client, never makes a network request,
// and never starts a sync. cmd/polymarket-sync is configured purely via
// environment variables; CLI args are only consumed for the help discriminator.
func shouldPrintHelp(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "-h", "--help", "help":
			return true
		}
	}
	return false
}

func printUsage(out *os.File) {
	_, _ = fmt.Fprintln(out, `Usage:
  go run ./cmd/polymarket-sync [-h | --help]
  go run ./cmd/polymarket-sync --diagnose-coverage [--endpoint events|markets] \
      [--page-size N] [--max-pages N] [--include-closed] [--include-archived] \
      [--include-inactive] [--order endDate|startDate|id] [--ascending true|false] \
      [--tag <slug>] [--tag-id <id>] [--category <name>] \
      [--query <text>] [--home-team <name>] [--away-team <name>] \
      [--now <RFC3339>] [--http-timeout <duration>] [--output json|table]
  go run ./cmd/polymarket-sync --target-event-id <id> [--dry-run] [--no-prices] \
      [--max-price-tokens N] [--price-concurrency N] [--http-timeout <duration>] \
      [--now <RFC3339>] [--output json|table]

cmd/polymarket-sync is a read-only Polymarket sync. It does not place orders,
does not handle wallets/private keys, and is not a trade-execution or
trade-recommendation surface. It only reads Gamma metadata + CLOB orderbook
prices and writes them to PostgreSQL.

Configuration is environment-driven; there are no positional or value flags
for the normal sync mode. The optional --diagnose-coverage mode is a Phase 12E
read-only Gamma walk that does NOT open PostgreSQL, does NOT write rows, does
NOT auto-map markets, and does NOT approve aliases. It only fetches and
classifies metadata so the operator can see why current sports markets may be
missing locally (wrong order, closed=true returned by default, missing tag/
category filter, page budget too small, etc.).

The optional --target-event-id mode is a Phase 12F narrow, operator-invoked,
read-only ingest of one Gamma event by ID and its markets/tokens (and
optional CLOB price snapshots). It does NOT auto-map any market to a canonical
event, does NOT approve any Polymarket alias, and does NOT call any order or
account endpoint. Use --dry-run to skip PostgreSQL entirely and print only what
would be upserted. After a non-dry-run targeted sync, manually inspect the
event/market/settlement semantics with cmd/match-review search-polymarket
before any alias decision.

Required environment variables:
  DATABASE_URL                       PostgreSQL DSN (default: postgres://pol:pol@localhost:5432/pol_odds?sslmode=disable)

Polymarket source URLs:
  POLYMARKET_GAMMA_BASE_URL          Gamma read-only metadata API base URL
  POLYMARKET_CLOB_BASE_URL           CLOB read-only orderbook/pricing API base URL

HTTP behavior:
  HTTP_TIMEOUT_SECONDS               (default: 15)
  POLYMARKET_API_MAX_RETRIES         (default: 2; non-negative integer)
  POLYMARKET_API_RETRY_BASE_MS       (default: 250)
  POLYMARKET_API_RETRY_MAX_MS        (default: 2000; must be >= base)

Metadata sync:
  POLYMARKET_SYNC_METADATA           true|false (default: true)
  POLYMARKET_GAMMA_EVENTS_LIMIT      total events to fetch (default: 100)
  POLYMARKET_GAMMA_MARKETS_LIMIT     total markets to fetch (default: 200)
  POLYMARKET_GAMMA_PAGE_SIZE         per-page limit (default: 100)
  POLYMARKET_GAMMA_MAX_PAGES         page cap (default: 10)
  POLYMARKET_ACTIVE_MARKETS_ONLY     true|false (default: true)
  POLYMARKET_MARKET_END_GRACE_HOURS  (default: 24)

Price sync:
  POLYMARKET_SYNC_PRICES             true|false (default: true)
  POLYMARKET_MAX_PRICE_TOKENS        per-run cap on tokens to refresh (default: 50)
  POLYMARKET_PRICE_CONCURRENCY       parallel price fetches (default: 4)
  POLYMARKET_PRICE_TOKEN_IDS         optional explicit comma-separated token IDs

Notes:
  - midpoint, last_trade_price, and spread are recorded as analytical metadata
    and are NOT treated as executable. Best bid and best ask are kept separate.
  - Auto-mapping of Polymarket markets to canonical events is NOT performed by
    this command. Use cmd/match-review for explicit, manually-reviewed alias
    decisions.
  - Help mode (this output) does not open PostgreSQL, does not call Polymarket,
    and does not start a sync. Run without --help to perform a sync.`)
}

func run(ctx context.Context, cfg syncConfig) error {
	st, err := store.OpenPolymarket(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer func() { _ = st.Close() }()

	startedAt := time.Now().UTC()
	summary, syncErr := runSync(ctx, st, cfg)
	finishedAt := time.Now().UTC()
	status := model.SourceRunStatusSuccess
	if syncErr != nil {
		status = model.SourceRunStatusError
		if errors.Is(syncErr, context.Canceled) {
			status = model.SourceRunStatusCanceled
		}
		summary.ErrorsCount++
	}
	syncRun := model.PolymarketSyncRun{
		StartedAt:     startedAt,
		FinishedAt:    finishedAt,
		SyncType:      syncType(cfg),
		Status:        status,
		PagesFetched:  summary.PagesFetched,
		EventsSeen:    summary.EventsSeen,
		MarketsSeen:   summary.MarketsSeen,
		TokensSeen:    summary.TokensSeen,
		PricesFetched: summary.PricesFetched,
		ErrorsCount:   summary.ErrorsCount,
		ErrorMessage:  truncateError(syncErr),
	}
	telemetryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := st.InsertPolymarketSyncRun(telemetryCtx, syncRun); err != nil {
		slog.Warn("insert polymarket sync telemetry", "error", err)
	}
	if syncErr != nil {
		return syncErr
	}
	return nil
}

func runSync(ctx context.Context, st *store.PostgresStore, cfg syncConfig) (syncSummary, error) {
	gamma := polymarket.NewClientWithRetry(cfg.GammaBaseURL, cfg.HTTPTimeout, cfg.Retry)
	clob := polymarket.NewCLOBClientWithRetry(cfg.CLOBBaseURL, cfg.HTTPTimeout, cfg.Retry)

	slog.Info("polymarket sync started",
		"gamma_events_limit", cfg.GammaEventsLimit,
		"gamma_markets_limit", cfg.GammaMarketsLimit,
		"gamma_page_size", cfg.GammaPageSize,
		"gamma_max_pages", cfg.GammaMaxPages,
		"sync_metadata", cfg.SyncMetadata,
		"sync_prices", cfg.SyncPrices,
		"max_price_tokens", cfg.MaxPriceTokens,
		"price_concurrency", cfg.PriceConcurrency,
		"active_markets_only", cfg.ActiveMarketsOnly,
	)

	summary := syncSummary{}
	modelTokens := []model.PolymarketToken{}
	modelMarkets := []model.PolymarketMarket{}

	if cfg.SyncMetadata {
		// Multi-tag: iterate the slug list once each. Empty list collapses to
		// a single "" pass (no tag filter). Dedupe by ID across slugs since
		// a market commonly appears in multiple tags (e.g. an NBA game shows
		// up under both "nba" and "basketball").
		slugs := cfg.TagSlugs
		if len(slugs) == 0 {
			slugs = []string{""}
		}
		eventsByID := map[string]polymarket.Event{}
		marketsByID := map[string]polymarket.Market{}
		// Order match-day events to the top: ascending endDate puts upcoming
		// events first, ahead of season-long futures. Also constrain to
		// active+open so we don't bloat the DB with thousands of resolved
		// historical markets (was happening pre-2026-05-17).
		asc, yes, no := true, true, false
		baseScope := scopeFilter{
			TagID:     cfg.TagID,
			Category:  cfg.Category,
			Order:     "endDate",
			Ascending: &asc,
			Active:    &yes,
			Closed:    &no,
		}
		for _, slug := range slugs {
			scope := baseScope
			scope.TagSlug = slug
			evs, pages, err := fetchPagedEvents(ctx, gamma, cfg.GammaEventsLimit, cfg.GammaPageSize, cfg.GammaMaxPages, scope)
			if err != nil {
				return summary, fmt.Errorf("list polymarket events (tag=%q): %w", slug, err)
			}
			summary.PagesFetched += pages
			for _, e := range evs {
				eventsByID[e.ID] = e
			}
			mks, pages, err := fetchPagedMarkets(ctx, gamma, cfg.GammaMarketsLimit, cfg.GammaPageSize, cfg.GammaMaxPages, scope)
			if err != nil {
				return summary, fmt.Errorf("list polymarket markets (tag=%q): %w", slug, err)
			}
			summary.PagesFetched += pages
			for _, m := range mks {
				marketsByID[m.ID] = m
			}
		}
		events := make([]polymarket.Event, 0, len(eventsByID))
		for _, e := range eventsByID {
			events = append(events, e)
		}
		markets := make([]polymarket.Market, 0, len(marketsByID))
		for _, m := range marketsByID {
			markets = append(markets, m)
		}

		modelEvents := modelEventsFromGamma(events)
		modelMarkets, modelTokens = modelMarketsAndTokensFromGamma(events, markets)
		summary.EventsSeen = len(modelEvents)
		summary.MarketsSeen = len(modelMarkets)
		summary.TokensSeen = len(modelTokens)

		var err error
		summary.Stats.EventsUpserted, err = st.UpsertPolymarketEvents(ctx, modelEvents)
		if err != nil {
			return summary, err
		}
		summary.Stats.MarketsUpserted, err = st.UpsertPolymarketMarkets(ctx, modelMarkets)
		if err != nil {
			return summary, err
		}
		summary.Stats.TokensUpserted, err = st.UpsertPolymarketTokens(ctx, modelTokens)
		if err != nil {
			return summary, err
		}
	} else {
		slog.Info("polymarket metadata sync disabled")
	}

	if cfg.SyncPrices {
		priceTokens, err := priceTokensForSync(ctx, st, modelTokens, modelMarkets, cfg)
		if err != nil {
			return summary, err
		}
		if len(cfg.PriceTokenIDs) > 0 {
			slog.Info("polymarket price token allow-list applied", "configured_token_ids", len(cfg.PriceTokenIDs), "selected", len(priceTokens))
		}
		snapshots, failures := fetchPriceSnapshots(ctx, clob, st, priceTokens, cfg.PriceConcurrency)
		summary.Stats.PriceSnapshotFailures = failures
		summary.ErrorsCount += failures
		if ctx.Err() != nil {
			return summary, ctx.Err()
		}
		if len(snapshots) > 0 {
			summary.Stats.PriceSnapshotsInserted, err = st.InsertPolymarketPriceSnapshots(ctx, snapshots)
			if err != nil {
				return summary, err
			}
		}
		summary.PricesFetched = summary.Stats.PriceSnapshotsInserted
	} else {
		slog.Info("polymarket price sync disabled")
	}

	slog.Info("polymarket sync completed",
		"pages_fetched", summary.PagesFetched,
		"events_seen", summary.EventsSeen,
		"markets_seen", summary.MarketsSeen,
		"tokens_seen", summary.TokensSeen,
		"events_upserted", summary.Stats.EventsUpserted,
		"markets_upserted", summary.Stats.MarketsUpserted,
		"tokens_upserted", summary.Stats.TokensUpserted,
		"price_snapshots_inserted", summary.Stats.PriceSnapshotsInserted,
		"price_snapshot_failures", summary.Stats.PriceSnapshotFailures,
	)
	return summary, nil
}

func modelEventsFromGamma(events []polymarket.Event) []model.PolymarketEvent {
	out := make([]model.PolymarketEvent, 0, len(events))
	seen := map[string]struct{}{}
	for _, event := range events {
		eventID := strings.TrimSpace(event.ID)
		if eventID == "" {
			continue
		}
		if _, ok := seen[eventID]; ok {
			continue
		}
		seen[eventID] = struct{}{}
		out = append(out, model.PolymarketEvent{
			PolymarketEventID: eventID,
			Slug:              event.Slug,
			Title:             event.Title,
			Category:          firstNonEmpty(event.Category, event.Sport),
			StartTime:         utcTimePtr(event.StartTime),
			EndTime:           utcTimePtr(event.EndTime),
			Active:            boolPtr(event.Active),
			Closed:            boolPtr(event.Closed),
			Archived:          boolPtr(event.Archived),
			GammaPayload:      event.Raw,
		})
	}
	return out
}

func modelMarketsAndTokensFromGamma(events []polymarket.Event, markets []polymarket.Market) ([]model.PolymarketMarket, []model.PolymarketToken) {
	marketByCondition := map[string]model.PolymarketMarket{}
	tokenByID := map[string]model.PolymarketToken{}

	for _, event := range events {
		for _, market := range event.Markets {
			addMarketAndTokens(marketByCondition, tokenByID, market, event.ID)
		}
	}
	for _, market := range markets {
		addMarketAndTokens(marketByCondition, tokenByID, market, "")
	}

	modelMarkets := make([]model.PolymarketMarket, 0, len(marketByCondition))
	for _, market := range marketByCondition {
		modelMarkets = append(modelMarkets, market)
	}
	sort.SliceStable(modelMarkets, func(i, j int) bool {
		return modelMarkets[i].ConditionID < modelMarkets[j].ConditionID
	})

	modelTokens := make([]model.PolymarketToken, 0, len(tokenByID))
	for _, token := range tokenByID {
		modelTokens = append(modelTokens, token)
	}
	sort.SliceStable(modelTokens, func(i, j int) bool {
		return modelTokens[i].TokenID < modelTokens[j].TokenID
	})
	return modelMarkets, modelTokens
}

// scopeFilter is the per-page base ListOptions overlay that the paged
// fetcher merges in alongside Limit/Offset. Empty fields mean "do not
// constrain". Added 2026-05-17 to support multi-sport scope.
// Order+Ascending matter most for live: ordering by endDate ascending
// surfaces match-day events ahead of season-long prop futures.
type scopeFilter struct {
	TagSlug   string
	TagID     string
	Category  string
	Order     string
	Ascending *bool
	Active    *bool
	Closed    *bool
}

func fetchPagedEvents(ctx context.Context, client gammaEventLister, totalLimit int, pageSize int, maxPages int, scope scopeFilter) ([]polymarket.Event, int, error) {
	return fetchPagedList(ctx, totalLimit, pageSize, maxPages, scope, client.ListEvents)
}

func fetchPagedMarkets(ctx context.Context, client gammaMarketLister, totalLimit int, pageSize int, maxPages int, scope scopeFilter) ([]polymarket.Market, int, error) {
	return fetchPagedList(ctx, totalLimit, pageSize, maxPages, scope, client.ListMarkets)
}

func fetchPagedList[T any](ctx context.Context, totalLimit int, pageSize int, maxPages int, scope scopeFilter, fetch func(context.Context, polymarket.ListOptions) ([]T, error)) ([]T, int, error) {
	if totalLimit <= 0 {
		return nil, 0, nil
	}
	if pageSize <= 0 {
		pageSize = totalLimit
	}
	if pageSize > totalLimit {
		pageSize = totalLimit
	}
	if maxPages <= 0 {
		maxPages = 1
	}

	out := make([]T, 0, totalLimit)
	pages := 0
	for len(out) < totalLimit && pages < maxPages {
		remaining := totalLimit - len(out)
		limit := pageSize
		if remaining < limit {
			limit = remaining
		}
		page, err := fetch(ctx, polymarket.ListOptions{
			Limit:     limit,
			Offset:    len(out),
			TagSlug:   scope.TagSlug,
			TagID:     scope.TagID,
			Category:  scope.Category,
			Order:     scope.Order,
			Ascending: scope.Ascending,
			Active:    scope.Active,
			Closed:    scope.Closed,
		})
		if err != nil {
			return out, pages, err
		}
		pages++
		if len(page) == 0 {
			break
		}
		if len(page) > remaining {
			page = page[:remaining]
		}
		out = append(out, page...)
		if len(page) < limit {
			break
		}
	}
	return out, pages, nil
}

func addMarketAndTokens(markets map[string]model.PolymarketMarket, tokens map[string]model.PolymarketToken, market polymarket.Market, eventID string) {
	marketID := strings.TrimSpace(market.ID)
	conditionID := strings.TrimSpace(market.ConditionID)
	if marketID == "" || conditionID == "" {
		return
	}

	existing, ok := markets[conditionID]
	if !ok || existing.PolymarketEventID == "" {
		markets[conditionID] = model.PolymarketMarket{
			PolymarketMarketID: marketID,
			PolymarketEventID:  strings.TrimSpace(eventID),
			ConditionID:        conditionID,
			Question:           firstNonEmpty(market.Question, market.Title),
			Slug:               market.Slug,
			MarketType:         market.Category,
			Active:             boolPtr(market.Active),
			Closed:             boolPtr(market.Closed),
			Archived:           boolPtr(market.Archived),
			EndTime:            utcTimePtr(market.EndTime),
			GammaPayload:       market.Raw,
		}
	}

	for _, token := range market.Tokens {
		tokenID := strings.TrimSpace(token.TokenID)
		if tokenID == "" {
			continue
		}
		index := token.OutcomeIndex
		tokens[tokenID] = model.PolymarketToken{
			TokenID:            tokenID,
			ConditionID:        conditionID,
			PolymarketMarketID: marketID,
			OutcomeName:        token.OutcomeName,
			OutcomeIndex:       &index,
			Side:               tokenSide(token.OutcomeName, token.OutcomeIndex),
			CLOBPayload:        token.Raw,
		}
	}
}

func selectPriceTokens(tokens []model.PolymarketToken, configuredIDs []string, maxTokens int) []model.PolymarketToken {
	if maxTokens < 1 {
		maxTokens = defaultMaxPriceTokens
	}

	configured := map[string]struct{}{}
	for _, tokenID := range configuredIDs {
		tokenID = strings.TrimSpace(tokenID)
		if tokenID != "" {
			configured[tokenID] = struct{}{}
		}
	}

	out := make([]model.PolymarketToken, 0, len(tokens))
	for _, token := range tokens {
		if len(configured) > 0 {
			if _, ok := configured[token.TokenID]; !ok {
				continue
			}
		}
		out = append(out, token)
		if len(out) >= maxTokens {
			break
		}
	}
	return out
}

func priceTokensForSync(ctx context.Context, st *store.PostgresStore, discoveredTokens []model.PolymarketToken, discoveredMarkets []model.PolymarketMarket, cfg syncConfig) ([]model.PolymarketToken, error) {
	// Always go through the DB so confirmed-alias prioritization from
	// ListPolymarketPriceTokens applies. Discovered tokens were already
	// upserted earlier in the sync run, so the DB sees them too. The
	// in-memory selectPriceTokens path is retained only as a fallback when
	// the DB returns nothing (e.g. first-ever bootstrap with empty schema).
	tokens, err := st.ListPolymarketPriceTokens(ctx, cfg.PriceTokenIDs, cfg.MaxPriceTokens, cfg.ActiveMarketsOnly, cfg.MarketEndGrace)
	if err != nil {
		return nil, err
	}
	if len(tokens) > 0 || len(discoveredTokens) == 0 {
		return tokens, nil
	}
	eligible := filterPriceEligibleTokens(discoveredTokens, discoveredMarkets, cfg.ActiveMarketsOnly, cfg.MarketEndGrace, time.Now().UTC())
	return selectPriceTokens(eligible, cfg.PriceTokenIDs, cfg.MaxPriceTokens), nil
}

func filterPriceEligibleTokens(tokens []model.PolymarketToken, markets []model.PolymarketMarket, activeOnly bool, endGrace time.Duration, now time.Time) []model.PolymarketToken {
	if !activeOnly && endGrace <= 0 {
		return tokens
	}
	marketByCondition := map[string]model.PolymarketMarket{}
	marketByID := map[string]model.PolymarketMarket{}
	for _, market := range markets {
		if market.ConditionID != "" {
			marketByCondition[market.ConditionID] = market
		}
		if market.PolymarketMarketID != "" {
			marketByID[market.PolymarketMarketID] = market
		}
	}

	out := make([]model.PolymarketToken, 0, len(tokens))
	for _, token := range tokens {
		market, ok := marketByCondition[token.ConditionID]
		if !ok && token.PolymarketMarketID != "" {
			market, ok = marketByID[token.PolymarketMarketID]
		}
		if ok && !marketEligibleForPrice(market, activeOnly, endGrace, now) {
			continue
		}
		out = append(out, token)
	}
	return out
}

func marketEligibleForPrice(market model.PolymarketMarket, activeOnly bool, endGrace time.Duration, now time.Time) bool {
	if activeOnly {
		if market.Active != nil && !*market.Active {
			return false
		}
		if market.Closed != nil && *market.Closed {
			return false
		}
		if market.Archived != nil && *market.Archived {
			return false
		}
	}
	if endGrace > 0 && market.EndTime != nil && market.EndTime.Before(now.Add(-endGrace)) {
		return false
	}
	return true
}

func fetchPriceSnapshots(ctx context.Context, clob *polymarket.CLOBClient, st *store.PostgresStore, tokens []model.PolymarketToken, concurrency int) ([]model.PolymarketPriceSnapshot, int) {
	if concurrency < 1 {
		concurrency = 1
	}

	sem := make(chan struct{}, concurrency)
	snapshots := make([]model.PolymarketPriceSnapshot, 0, len(tokens))
	failures := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

loop:
	for _, token := range tokens {
		if ctx.Err() != nil {
			break
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break loop
		}

		token := token
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			snapshot, err := fetchPriceSnapshot(ctx, clob, token)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures++
				if ctx.Err() == nil {
					slog.Warn("fetch polymarket price snapshot", "token_id", token.TokenID, "error", err)
					// 404 "No orderbook exists" → mark token dead so next
					// tick's ListPolymarketPriceTokens excludes it. Other
					// errors (timeouts, network) are NOT marked: they're
					// likely transient and we want to keep trying.
					if st != nil && isOrderbookNotFoundErr(err) {
						if mErr := st.MarkDeadToken(ctx, token.TokenID, err.Error()); mErr != nil {
							slog.Warn("mark dead token", "token_id", token.TokenID, "error", mErr)
						}
					}
				}
				return
			}
			snapshots = append(snapshots, snapshot)
			if st != nil {
				// Best-effort clear; ignore error (rare and non-fatal).
				_ = st.ClearDeadToken(ctx, token.TokenID)
			}
		}()
	}
	wg.Wait()

	sort.SliceStable(snapshots, func(i, j int) bool {
		return snapshots[i].TokenID < snapshots[j].TokenID
	})
	return snapshots, failures
}

// isOrderbookNotFoundErr returns true when the CLOB price-snapshot error
// is the "no orderbook for this token" 404 we see ~22 times per tick on
// dead/archived markets. Detected via substring rather than typed error
// because the polymarket package surfaces the upstream JSON inline.
func isOrderbookNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "404") &&
		strings.Contains(msg, "No orderbook exists for the requested token id")
}

func fetchPriceSnapshot(ctx context.Context, clob *polymarket.CLOBClient, token model.PolymarketToken) (model.PolymarketPriceSnapshot, error) {
	book, err := clob.GetOrderBook(ctx, token.TokenID)
	if err != nil {
		return model.PolymarketPriceSnapshot{}, err
	}

	var midpoint polymarket.CLOBMidpoint
	if m, err := clob.GetMidpoint(ctx, token.TokenID); err == nil {
		midpoint = m
	} else if ctx.Err() != nil {
		return model.PolymarketPriceSnapshot{}, ctx.Err()
	} else {
		slog.Warn("fetch polymarket midpoint", "token_id", token.TokenID, "error", err)
	}

	var spread polymarket.CLOBSpread
	if s, err := clob.GetSpread(ctx, token.TokenID); err == nil {
		spread = s
	} else if ctx.Err() != nil {
		return model.PolymarketPriceSnapshot{}, ctx.Err()
	} else {
		slog.Warn("fetch polymarket spread", "token_id", token.TokenID, "error", err)
	}

	return snapshotFromCLOB(token, book, midpoint, spread, time.Now().UTC()), nil
}

func snapshotFromCLOB(token model.PolymarketToken, book polymarket.CLOBOrderBook, midpoint polymarket.CLOBMidpoint, spread polymarket.CLOBSpread, receivedAt time.Time) model.PolymarketPriceSnapshot {
	bestBid, bestBidSize := bestBidLevel(book.Bids)
	bestAsk, bestAskSize := bestAskLevel(book.Asks)
	midpointPrice := floatPtrFromString(midpoint.MidPrice)
	spreadPrice := spreadValue(bestBid, bestAsk, floatPtrFromString(spread.Spread))
	lastTradePrice := floatPtrFromString(book.LastTradePrice)
	liquidity := totalLiquidity(book.Bids, book.Asks)
	depthSummary := depthSummaryPayload(book, bestBid, bestAsk, bestBidSize, bestAskSize, liquidity)
	payload := combinedPayload(book.Raw, midpoint.Raw, spread.Raw)
	return model.PolymarketPriceSnapshot{
		TokenID:            token.TokenID,
		ConditionID:        firstNonEmpty(book.Market, token.ConditionID),
		PolymarketMarketID: token.PolymarketMarketID,
		BidPrice:           bestBid,
		AskPrice:           bestAsk,
		MidPrice:           midpointPrice,
		LastPrice:          lastTradePrice,
		BestBidSize:        bestBidSize,
		BestAskSize:        bestAskSize,
		BestBid:            bestBid,
		BestAsk:            bestAsk,
		Midpoint:           midpointPrice,
		Spread:             spreadPrice,
		LastTradePrice:     lastTradePrice,
		Liquidity:          liquidity,
		DepthSummary:       depthSummary,
		SourceTimestamp:    sourceTimestampFromString(book.Timestamp),
		RawPayload:         payload,
		Source:             model.SourcePolymarket,
		ReceivedAt:         receivedAt.UTC(),
		CLOBPayload:        payload,
	}
}

func loadConfig() (syncConfig, error) {
	httpTimeout, err := secondsEnv("HTTP_TIMEOUT_SECONDS", defaultHTTPTimeoutSec)
	if err != nil {
		return syncConfig{}, err
	}
	eventsLimit, err := positiveIntEnv("POLYMARKET_GAMMA_EVENTS_LIMIT", defaultGammaEventsLimit)
	if err != nil {
		return syncConfig{}, err
	}
	marketsLimit, err := positiveIntEnv("POLYMARKET_GAMMA_MARKETS_LIMIT", defaultGammaMarketsLimit)
	if err != nil {
		return syncConfig{}, err
	}
	pageSize, err := positiveIntEnv("POLYMARKET_GAMMA_PAGE_SIZE", defaultGammaPageSize)
	if err != nil {
		return syncConfig{}, err
	}
	maxPages, err := positiveIntEnv("POLYMARKET_GAMMA_MAX_PAGES", defaultGammaMaxPages)
	if err != nil {
		return syncConfig{}, err
	}
	maxPriceTokens, err := positiveIntEnv("POLYMARKET_MAX_PRICE_TOKENS", defaultMaxPriceTokens)
	if err != nil {
		return syncConfig{}, err
	}
	priceConcurrency, err := positiveIntEnv("POLYMARKET_PRICE_CONCURRENCY", defaultPriceConcurrency)
	if err != nil {
		return syncConfig{}, err
	}
	syncMetadata, err := boolEnv("POLYMARKET_SYNC_METADATA", true)
	if err != nil {
		return syncConfig{}, err
	}
	syncPrices, err := boolEnv("POLYMARKET_SYNC_PRICES", true)
	if err != nil {
		return syncConfig{}, err
	}
	activeMarketsOnly, err := boolEnv("POLYMARKET_ACTIVE_MARKETS_ONLY", true)
	if err != nil {
		return syncConfig{}, err
	}
	marketEndGrace, err := hoursEnv("POLYMARKET_MARKET_END_GRACE_HOURS", defaultMarketEndGraceHrs)
	if err != nil {
		return syncConfig{}, err
	}
	maxRetries, err := nonNegativeIntEnv("POLYMARKET_API_MAX_RETRIES", defaultAPIRequestRetries)
	if err != nil {
		return syncConfig{}, err
	}
	retryBase, err := millisEnv("POLYMARKET_API_RETRY_BASE_MS", defaultRetryBaseMS)
	if err != nil {
		return syncConfig{}, err
	}
	retryMax, err := millisEnv("POLYMARKET_API_RETRY_MAX_MS", defaultRetryMaxMS)
	if err != nil {
		return syncConfig{}, err
	}
	if retryMax < retryBase {
		return syncConfig{}, fmt.Errorf("POLYMARKET_API_RETRY_MAX_MS must be >= POLYMARKET_API_RETRY_BASE_MS")
	}
	if !syncMetadata && !syncPrices {
		return syncConfig{}, fmt.Errorf("at least one of POLYMARKET_SYNC_METADATA or POLYMARKET_SYNC_PRICES must be true")
	}

	return syncConfig{
		DatabaseURL:       envDefault("DATABASE_URL", defaultDatabaseURL),
		GammaBaseURL:      envDefault("POLYMARKET_GAMMA_BASE_URL", polymarket.DefaultGammaBaseURL),
		CLOBBaseURL:       envDefault("POLYMARKET_CLOB_BASE_URL", polymarket.DefaultCLOBBaseURL),
		HTTPTimeout:       httpTimeout,
		GammaEventsLimit:  eventsLimit,
		GammaMarketsLimit: marketsLimit,
		GammaPageSize:     pageSize,
		GammaMaxPages:     maxPages,
		SyncMetadata:      syncMetadata,
		SyncPrices:        syncPrices,
		MaxPriceTokens:    maxPriceTokens,
		PriceConcurrency:  priceConcurrency,
		PriceTokenIDs:     splitCSV(os.Getenv("POLYMARKET_PRICE_TOKEN_IDS")),
		ActiveMarketsOnly: activeMarketsOnly,
		MarketEndGrace:    marketEndGrace,
		Retry:             polymarket.RetryConfig{MaxRetries: maxRetries, BaseDelay: retryBase, MaxDelay: retryMax},
		TagSlugs:          splitCSV(os.Getenv("POLYMARKET_TAG_SLUG")),
		TagID:             strings.TrimSpace(os.Getenv("POLYMARKET_TAG_ID")),
		Category:          strings.TrimSpace(os.Getenv("POLYMARKET_CATEGORY")),
	}, nil
}

func envDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func secondsEnv(key string, fallback int) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return time.Duration(fallback) * time.Second, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer number of seconds", key)
	}
	return time.Duration(n) * time.Second, nil
}

func hoursEnv(key string, fallback int) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return time.Duration(fallback) * time.Hour, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer number of hours", key)
	}
	return time.Duration(n) * time.Hour, nil
}

func millisEnv(key string, fallback int) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return time.Duration(fallback) * time.Millisecond, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer number of milliseconds", key)
	}
	return time.Duration(n) * time.Millisecond, nil
}

func positiveIntEnv(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

func nonNegativeIntEnv(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return n, nil
}

func boolEnv(key string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return v, nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func utcTimePtr(t *time.Time) *time.Time {
	if t == nil || t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func boolPtr(v *bool) *bool {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func tokenSide(outcome string, index int) string {
	switch strings.ToLower(strings.TrimSpace(outcome)) {
	case "yes":
		return "yes"
	case "no":
		return "no"
	}
	switch index {
	case 0:
		return "yes"
	case 1:
		return "no"
	default:
		return ""
	}
}

func floatPtrFromString(raw string) *float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil
	}
	return &v
}

func bestBidLevel(levels []polymarket.CLOBBookLevel) (*float64, *float64) {
	var bestPrice *float64
	var bestSize *float64
	for _, level := range levels {
		price := floatPtrFromString(level.Price)
		if price == nil {
			continue
		}
		if bestPrice == nil || *price > *bestPrice {
			v := *price
			bestPrice = &v
			bestSize = floatPtrFromString(level.Size)
		}
	}
	return bestPrice, bestSize
}

func bestAskLevel(levels []polymarket.CLOBBookLevel) (*float64, *float64) {
	var bestPrice *float64
	var bestSize *float64
	for _, level := range levels {
		price := floatPtrFromString(level.Price)
		if price == nil {
			continue
		}
		if bestPrice == nil || *price < *bestPrice {
			v := *price
			bestPrice = &v
			bestSize = floatPtrFromString(level.Size)
		}
	}
	return bestPrice, bestSize
}

func spreadValue(bestBid *float64, bestAsk *float64, explicit *float64) *float64 {
	if explicit != nil {
		return explicit
	}
	if bestBid == nil || bestAsk == nil || *bestAsk < *bestBid {
		return nil
	}
	spread := *bestAsk - *bestBid
	return &spread
}

func totalLiquidity(bids []polymarket.CLOBBookLevel, asks []polymarket.CLOBBookLevel) *float64 {
	total := 0.0
	seen := false
	for _, levels := range [][]polymarket.CLOBBookLevel{bids, asks} {
		for _, level := range levels {
			size := floatPtrFromString(level.Size)
			if size == nil {
				continue
			}
			total += *size
			seen = true
		}
	}
	if !seen {
		return nil
	}
	return &total
}

func depthSummaryPayload(book polymarket.CLOBOrderBook, bestBid *float64, bestAsk *float64, bestBidSize *float64, bestAskSize *float64, liquidity *float64) json.RawMessage {
	summary := map[string]any{
		"bid_levels":     len(book.Bids),
		"ask_levels":     len(book.Asks),
		"min_order_size": book.MinOrderSize,
		"tick_size":      book.TickSize,
		"negative_risk":  book.NegativeRisk,
	}
	addFloat(summary, "best_bid", bestBid)
	addFloat(summary, "best_ask", bestAsk)
	addFloat(summary, "best_bid_size", bestBidSize)
	addFloat(summary, "best_ask_size", bestAskSize)
	addFloat(summary, "liquidity", liquidity)

	raw, err := json.Marshal(summary)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func addFloat(summary map[string]any, key string, value *float64) {
	if value != nil {
		summary[key] = *value
	}
}

func sourceTimestampFromString(raw string) *time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if ts, err := time.Parse(time.RFC3339, raw); err == nil {
		u := ts.UTC()
		return &u
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return nil
	}

	var ts time.Time
	switch {
	case n >= 1_000_000_000_000_000_000:
		ts = time.Unix(0, n)
	case n >= 1_000_000_000_000_000:
		ts = time.Unix(0, n*1_000)
	case n >= 1_000_000_000_000:
		ts = time.UnixMilli(n)
	default:
		ts = time.Unix(n, 0)
	}
	u := ts.UTC()
	return &u
}

func combinedPayload(book json.RawMessage, midpoint json.RawMessage, spread json.RawMessage) json.RawMessage {
	payload := map[string]json.RawMessage{}
	if len(book) > 0 && json.Valid(book) {
		payload["book"] = book
	}
	if len(midpoint) > 0 && json.Valid(midpoint) {
		payload["midpoint"] = midpoint
	}
	if len(spread) > 0 && json.Valid(spread) {
		payload["spread"] = spread
	}
	if len(payload) == 0 {
		return json.RawMessage(`{}`)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func syncType(cfg syncConfig) string {
	switch {
	case cfg.SyncMetadata && cfg.SyncPrices:
		return "metadata_price"
	case cfg.SyncMetadata:
		return "metadata"
	case cfg.SyncPrices:
		return "price"
	default:
		return "disabled"
	}
}

func truncateError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	if len(msg) <= maxSyncErrorMessageLen {
		return msg
	}
	return msg[:maxSyncErrorMessageLen]
}
