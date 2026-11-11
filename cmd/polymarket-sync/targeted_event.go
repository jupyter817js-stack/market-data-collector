package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
	"github.com/your-org/pol-odds-collector/internal/store"
)

// Phase 12F: targeted Polymarket Gamma event sync.
//
// `cmd/polymarket-sync --target-event-id <id> [--dry-run] [--no-prices]` is an
// operator-invoked, narrowly-scoped, read-only ingest of one Gamma event ID
// already identified by the Phase 12E discovery diagnostic. It does not auto-
// map the event to any canonical event, does not approve a Polymarket alias,
// does not invoke any order/account/wallet/private-key surface, and does not
// ever decide on its own that the operator should act on a result. Its only
// job is to make the event/markets/tokens — and optionally a fresh price
// snapshot per token — available locally so cmd/match-review search-polymarket
// and cmd/coverage-audit can inspect the candidate before any manual review.

const (
	targetedDefaultHTTPTimeout      = 30 * time.Second
	targetedDefaultMaxPriceTokens   = 32
	targetedDefaultPriceConcurrency = 4
)

type targetedConfig struct {
	EventID          string
	DryRun           bool
	SyncPrices       bool
	MaxPriceTokens   int
	PriceConcurrency int
	HTTPTimeout      time.Duration
	GammaBaseURL     string
	CLOBBaseURL      string
	DatabaseURL      string
	Output           string
	Now              time.Time
}

type targetedReport struct {
	EventID             string                  `json:"event_id"`
	DryRun              bool                    `json:"dry_run"`
	GammaURLExample     string                  `json:"gamma_url_example"`
	Event               targetedEventSummary    `json:"event"`
	Markets             []targetedMarketSummary `json:"markets"`
	UpsertCounts        targetedUpsertCounts    `json:"upsert_counts"`
	PriceSnapshotCounts targetedPriceCounts     `json:"price_snapshot_counts"`
	OperatorNextActions []string                `json:"operator_next_actions"`
	Notes               []string                `json:"notes"`
}

type targetedEventSummary struct {
	ID        string     `json:"id"`
	Slug      string     `json:"slug,omitempty"`
	Title     string     `json:"title,omitempty"`
	StartTime *time.Time `json:"start_time,omitempty"`
	EndTime   *time.Time `json:"end_time,omitempty"`
	Active    *bool      `json:"active,omitempty"`
	Closed    *bool      `json:"closed,omitempty"`
	Archived  *bool      `json:"archived,omitempty"`
	Category  string     `json:"category,omitempty"`
	Tags      []string   `json:"tags,omitempty"`
}

type targetedMarketSummary struct {
	ID          string     `json:"id"`
	ConditionID string     `json:"condition_id,omitempty"`
	Slug        string     `json:"slug,omitempty"`
	Question    string     `json:"question,omitempty"`
	Active      *bool      `json:"active,omitempty"`
	Closed      *bool      `json:"closed,omitempty"`
	Archived    *bool      `json:"archived,omitempty"`
	EndTime     *time.Time `json:"end_time,omitempty"`
	TokenCount  int        `json:"token_count"`
}

type targetedUpsertCounts struct {
	EventsUpserted  int `json:"events_upserted"`
	MarketsUpserted int `json:"markets_upserted"`
	TokensUpserted  int `json:"tokens_upserted"`
}

type targetedPriceCounts struct {
	Attempted int `json:"attempted"`
	Inserted  int `json:"inserted"`
	Failures  int `json:"failures"`
	Skipped   int `json:"skipped"`
}

type targetedEventClient interface {
	GetEvent(ctx context.Context, eventID string) (polymarket.Event, error)
}

type targetedEventStore interface {
	UpsertPolymarketEvents(context.Context, []model.PolymarketEvent) (int, error)
	UpsertPolymarketMarkets(context.Context, []model.PolymarketMarket) (int, error)
	UpsertPolymarketTokens(context.Context, []model.PolymarketToken) (int, error)
	InsertPolymarketPriceSnapshots(context.Context, []model.PolymarketPriceSnapshot) (int, error)
}

type targetedPriceFetcher interface {
	FetchSnapshot(ctx context.Context, token model.PolymarketToken) (model.PolymarketPriceSnapshot, error)
}

func parseTargetedConfig(args []string, out io.Writer) (targetedConfig, error) {
	fs := flag.NewFlagSet("target-event-id", flag.ContinueOnError)
	fs.SetOutput(out)
	cfg := targetedConfig{
		HTTPTimeout:      targetedDefaultHTTPTimeout,
		SyncPrices:       true,
		MaxPriceTokens:   targetedDefaultMaxPriceTokens,
		PriceConcurrency: targetedDefaultPriceConcurrency,
		Output:           "table",
	}
	eventID := fs.String("target-event-id", "", "Polymarket Gamma event ID to ingest (required)")
	eventIDAlias := fs.String("event-id", "", "alias for --target-event-id")
	dryRun := fs.Bool("dry-run", false, "fetch and report only; do not open PostgreSQL or write any rows")
	noPrices := fs.Bool("no-prices", false, "skip CLOB price-snapshot refresh after metadata upsert")
	maxPriceTokens := fs.Int("max-price-tokens", cfg.MaxPriceTokens, "maximum number of tokens to refresh prices for")
	priceConcurrency := fs.Int("price-concurrency", cfg.PriceConcurrency, "parallel price fetches")
	httpTimeout := fs.Duration("http-timeout", cfg.HTTPTimeout, "HTTP timeout per Gamma/CLOB request")
	output := fs.String("output", cfg.Output, "output format: table or json")
	nowFlag := fs.String("now", "", "override current time for deterministic reporting (RFC3339 UTC)")
	if err := fs.Parse(args); err != nil {
		return targetedConfig{}, err
	}

	cfg.EventID = strings.TrimSpace(*eventID)
	if cfg.EventID == "" {
		cfg.EventID = strings.TrimSpace(*eventIDAlias)
	}
	if cfg.EventID == "" {
		return targetedConfig{}, errors.New("--target-event-id is required (operator must supply the exact Gamma event id, e.g. 446691)")
	}
	cfg.DryRun = *dryRun
	cfg.SyncPrices = !(*noPrices)
	if *maxPriceTokens < 0 || *maxPriceTokens > 500 {
		return targetedConfig{}, fmt.Errorf("--max-price-tokens must be in [0, 500]")
	}
	cfg.MaxPriceTokens = *maxPriceTokens
	if *priceConcurrency < 1 || *priceConcurrency > 16 {
		return targetedConfig{}, fmt.Errorf("--price-concurrency must be in [1, 16]")
	}
	cfg.PriceConcurrency = *priceConcurrency
	if *httpTimeout <= 0 {
		return targetedConfig{}, fmt.Errorf("--http-timeout must be positive")
	}
	cfg.HTTPTimeout = *httpTimeout
	cfg.Output = strings.ToLower(strings.TrimSpace(*output))
	if cfg.Output != "table" && cfg.Output != "json" {
		return targetedConfig{}, fmt.Errorf("--output must be 'table' or 'json'")
	}
	if v := strings.TrimSpace(*nowFlag); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return targetedConfig{}, fmt.Errorf("--now must be RFC3339 UTC: %w", err)
		}
		cfg.Now = t.UTC()
	}
	cfg.GammaBaseURL = envDefault("POLYMARKET_GAMMA_BASE_URL", polymarket.DefaultGammaBaseURL)
	cfg.CLOBBaseURL = envDefault("POLYMARKET_CLOB_BASE_URL", polymarket.DefaultCLOBBaseURL)
	cfg.DatabaseURL = envDefault("DATABASE_URL", defaultDatabaseURL)
	return cfg, nil
}

// shouldRunTargetedEvent strips the --target-event-id flag (or its alias) from
// args and signals targeted mode. Detected before loadConfig so --dry-run can
// avoid opening PostgreSQL entirely.
func shouldRunTargetedEvent(args []string) bool {
	for _, arg := range args {
		switch {
		case arg == "--target-event-id", arg == "--event-id":
			return true
		case strings.HasPrefix(arg, "--target-event-id="), strings.HasPrefix(arg, "--event-id="):
			return true
		}
	}
	return false
}

func runTargetedEventCLI(ctx context.Context, args []string) error {
	cfg, err := parseTargetedConfig(args, os.Stderr)
	if err != nil {
		return err
	}
	gamma := polymarket.NewClientWithRetry(cfg.GammaBaseURL, cfg.HTTPTimeout, polymarket.DefaultRetryConfig())
	clob := polymarket.NewCLOBClientWithRetry(cfg.CLOBBaseURL, cfg.HTTPTimeout, polymarket.DefaultRetryConfig())
	priceFetcher := newTargetedCLOBPriceFetcher(clob)

	var st targetedEventStore
	if !cfg.DryRun {
		opened, err := openTargetedStore(ctx, cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("open postgres for targeted sync: %s", redactKnownValue(err.Error(), cfg.DatabaseURL))
		}
		defer func() { _ = opened.Close() }()
		st = opened
	}
	return runTargetedEvent(ctx, cfg, gamma, st, priceFetcher, os.Stdout)
}

// runTargetedEvent is the dependency-injectable entry point used by tests.
// st may be nil when cfg.DryRun is true; in that case no upsert is attempted.
func runTargetedEvent(ctx context.Context, cfg targetedConfig, gamma targetedEventClient, st targetedEventStore, priceFetcher targetedPriceFetcher, out io.Writer) error {
	if cfg.DryRun && st != nil {
		// Defensive: a caller that supplies a store in dry-run mode would be
		// confusing; we ignore the store rather than risk a write.
		st = nil
	}
	if !cfg.DryRun && st == nil {
		return errors.New("targeted sync requires a store unless --dry-run is set")
	}
	if cfg.Now.IsZero() {
		cfg.Now = time.Now().UTC()
	}

	event, err := gamma.GetEvent(ctx, cfg.EventID)
	if err != nil {
		return fmt.Errorf("fetch polymarket event %s: %w", cfg.EventID, err)
	}
	if strings.TrimSpace(event.ID) == "" {
		return fmt.Errorf("polymarket event %s returned an empty id", cfg.EventID)
	}

	report := targetedReport{
		EventID:         cfg.EventID,
		DryRun:          cfg.DryRun,
		GammaURLExample: targetedGammaExampleURL(cfg),
		Event:           targetedEventSummaryOf(event),
		Markets:         targetedMarketSummariesOf(event),
		Notes: []string{
			"targeted sync is read-only; it does not approve aliases, does not auto-map, and does not place orders",
			"midpoint, last trade price, and spread are analytical metadata only; this output is not a trading signal",
		},
	}

	modelEvents := modelEventsFromGamma([]polymarket.Event{event})
	modelMarkets, modelTokens := modelMarketsAndTokensFromGamma([]polymarket.Event{event}, nil)

	if !cfg.DryRun {
		if n, err := st.UpsertPolymarketEvents(ctx, modelEvents); err != nil {
			return fmt.Errorf("upsert polymarket event %s: %w", cfg.EventID, err)
		} else {
			report.UpsertCounts.EventsUpserted = n
		}
		if n, err := st.UpsertPolymarketMarkets(ctx, modelMarkets); err != nil {
			return fmt.Errorf("upsert polymarket markets for event %s: %w", cfg.EventID, err)
		} else {
			report.UpsertCounts.MarketsUpserted = n
		}
		if n, err := st.UpsertPolymarketTokens(ctx, modelTokens); err != nil {
			return fmt.Errorf("upsert polymarket tokens for event %s: %w", cfg.EventID, err)
		} else {
			report.UpsertCounts.TokensUpserted = n
		}
	} else {
		report.UpsertCounts.EventsUpserted = len(modelEvents)
		report.UpsertCounts.MarketsUpserted = len(modelMarkets)
		report.UpsertCounts.TokensUpserted = len(modelTokens)
		report.Notes = append(report.Notes,
			"--dry-run is on; no rows were written to PostgreSQL")
	}

	if cfg.SyncPrices {
		eligible := filterPriceEligibleTokens(modelTokens, modelMarkets, true, 0, cfg.Now)
		if len(eligible) > cfg.MaxPriceTokens {
			report.PriceSnapshotCounts.Skipped = len(eligible) - cfg.MaxPriceTokens
			eligible = eligible[:cfg.MaxPriceTokens]
		}
		report.PriceSnapshotCounts.Attempted = len(eligible)
		if !cfg.DryRun && priceFetcher != nil && len(eligible) > 0 {
			snapshots, failures := fetchTargetedPriceSnapshots(ctx, priceFetcher, eligible, cfg.PriceConcurrency)
			report.PriceSnapshotCounts.Failures = failures
			if len(snapshots) > 0 {
				inserted, err := st.InsertPolymarketPriceSnapshots(ctx, snapshots)
				if err != nil {
					return fmt.Errorf("insert polymarket price snapshots for event %s: %w", cfg.EventID, err)
				}
				report.PriceSnapshotCounts.Inserted = inserted
			}
		} else if cfg.DryRun {
			report.Notes = append(report.Notes,
				"--dry-run is on; no CLOB price snapshots were fetched")
		}
	} else {
		report.Notes = append(report.Notes,
			"--no-prices is on; CLOB price snapshots were not refreshed")
	}

	report.OperatorNextActions = targetedNextActions(report)
	return writeTargetedReport(out, cfg.Output, report)
}

func targetedNextActions(report targetedReport) []string {
	actions := []string{
		"this command does NOT approve a Polymarket alias for any canonical event",
		"manually inspect event direction, market question, outcomes, and settlement semantics before any alias decision",
	}
	closedEvent := report.Event.Closed != nil && *report.Event.Closed
	archivedEvent := report.Event.Archived != nil && *report.Event.Archived
	inactiveEvent := report.Event.Active != nil && !*report.Event.Active
	if closedEvent || archivedEvent || inactiveEvent {
		actions = append(actions,
			"event is NOT active; use only for historical validation, not current comparison")
	} else {
		actions = append(actions,
			"after the targeted sync run cmd/match-review search-polymarket --canonical-event-id <uuid> to confirm the candidate appears with safe lifecycle hints",
			"after manual verification, use cmd/match-review approve with explicit reviewer reason metadata")
	}
	if report.PriceSnapshotCounts.Inserted == 0 && report.PriceSnapshotCounts.Attempted > 0 {
		actions = append(actions,
			"no fresh CLOB price snapshots were inserted; rerun cmd/polymarket-sync (without --target-event-id) or this command without --dry-run to refresh prices")
	}
	if report.PriceSnapshotCounts.Skipped > 0 {
		actions = append(actions,
			fmt.Sprintf("--max-price-tokens capped the price refresh; %d tokens skipped — raise --max-price-tokens if needed", report.PriceSnapshotCounts.Skipped))
	}
	actions = append(actions,
		"executable readiness still requires best bid AND best ask AND depth/liquidity AND freshness; midpoint alone is analytical")
	return actions
}

func writeTargetedReport(out io.Writer, outputFormat string, report targetedReport) error {
	if outputFormat == "json" {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	return writeTargetedTable(out, report)
}

func writeTargetedTable(out io.Writer, report targetedReport) error {
	header := fmt.Sprintf("event_id=%s dry_run=%t title=%q", report.EventID, report.DryRun, report.Event.Title)
	if _, err := fmt.Fprintln(out, header); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "gamma_url_example=%s\n", report.GammaURLExample); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "event active=%s closed=%s archived=%s start_time=%s end_time=%s\n",
		formatTriBoolStr(report.Event.Active),
		formatTriBoolStr(report.Event.Closed),
		formatTriBoolStr(report.Event.Archived),
		formatNullableRFC3339(report.Event.StartTime),
		formatNullableRFC3339(report.Event.EndTime),
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "upsert_counts events=%d markets=%d tokens=%d\n",
		report.UpsertCounts.EventsUpserted, report.UpsertCounts.MarketsUpserted, report.UpsertCounts.TokensUpserted); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "price_snapshots attempted=%d inserted=%d failures=%d skipped=%d\n",
		report.PriceSnapshotCounts.Attempted, report.PriceSnapshotCounts.Inserted,
		report.PriceSnapshotCounts.Failures, report.PriceSnapshotCounts.Skipped); err != nil {
		return err
	}
	for _, m := range report.Markets {
		if _, err := fmt.Fprintf(out, "  market %s condition=%s active=%s closed=%s archived=%s tokens=%d question=%q\n",
			m.ID, m.ConditionID,
			formatTriBoolStr(m.Active), formatTriBoolStr(m.Closed), formatTriBoolStr(m.Archived),
			m.TokenCount, truncate(m.Question, 80)); err != nil {
			return err
		}
	}
	for _, action := range report.OperatorNextActions {
		if _, err := fmt.Fprintf(out, "next_action: %s\n", action); err != nil {
			return err
		}
	}
	for _, note := range report.Notes {
		if _, err := fmt.Fprintf(out, "note: %s\n", note); err != nil {
			return err
		}
	}
	return nil
}

func targetedEventSummaryOf(event polymarket.Event) targetedEventSummary {
	tags := make([]string, 0, len(event.Tags))
	for _, t := range event.Tags {
		switch {
		case t.Slug != "":
			tags = append(tags, t.Slug)
		case t.Label != "":
			tags = append(tags, t.Label)
		}
	}
	return targetedEventSummary{
		ID:        event.ID,
		Slug:      event.Slug,
		Title:     event.Title,
		StartTime: utcTimePtr(event.StartTime),
		EndTime:   utcTimePtr(event.EndTime),
		Active:    boolPtr(event.Active),
		Closed:    boolPtr(event.Closed),
		Archived:  boolPtr(event.Archived),
		Category:  firstNonEmpty(event.Category, event.Sport),
		Tags:      tags,
	}
}

func targetedMarketSummariesOf(event polymarket.Event) []targetedMarketSummary {
	out := make([]targetedMarketSummary, 0, len(event.Markets))
	for _, m := range event.Markets {
		out = append(out, targetedMarketSummary{
			ID:          m.ID,
			ConditionID: m.ConditionID,
			Slug:        m.Slug,
			Question:    m.Question,
			Active:      boolPtr(m.Active),
			Closed:      boolPtr(m.Closed),
			Archived:    boolPtr(m.Archived),
			EndTime:     utcTimePtr(m.EndTime),
			TokenCount:  len(m.Tokens),
		})
	}
	return out
}

func targetedGammaExampleURL(cfg targetedConfig) string {
	base := strings.TrimRight(cfg.GammaBaseURL, "/")
	if base == "" {
		base = polymarket.DefaultGammaBaseURL
	}
	return base + "/events/" + cfg.EventID
}

func redactKnownValue(text, secret string) string {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return text
	}
	return strings.ReplaceAll(text, secret, "<redacted>")
}

// targetedStoreCloser wraps the concrete *store.PostgresStore in the targeted
// store interface plus a Close() method for the CLI lifecycle.
type targetedStoreCloser struct {
	*store.PostgresStore
}

func openTargetedStore(ctx context.Context, databaseURL string) (*targetedStoreCloser, error) {
	st, err := store.OpenPolymarket(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	return &targetedStoreCloser{PostgresStore: st}, nil
}

// targetedCLOBPriceFetcher is the read-only price-fetch helper used by the
// targeted sync. It wraps the existing fetchPriceSnapshot helper used by the
// broader sync; tests can replace it with a fake to keep automated runs
// offline.
type targetedCLOBPriceFetcher struct {
	clob *polymarket.CLOBClient
}

func newTargetedCLOBPriceFetcher(clob *polymarket.CLOBClient) *targetedCLOBPriceFetcher {
	return &targetedCLOBPriceFetcher{clob: clob}
}

func (f *targetedCLOBPriceFetcher) FetchSnapshot(ctx context.Context, token model.PolymarketToken) (model.PolymarketPriceSnapshot, error) {
	return fetchPriceSnapshot(ctx, f.clob, token)
}

// fetchTargetedPriceSnapshots is a small, bounded-concurrency wrapper around a
// targetedPriceFetcher. Mirrors the broader sync's fetchPriceSnapshots but
// takes the interface so tests can swap the CLOB layer for a fake.
func fetchTargetedPriceSnapshots(ctx context.Context, fetcher targetedPriceFetcher, tokens []model.PolymarketToken, concurrency int) ([]model.PolymarketPriceSnapshot, int) {
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
			snapshot, err := fetcher.FetchSnapshot(ctx, token)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures++
				if ctx.Err() == nil {
					slog.Warn("fetch polymarket price snapshot (targeted)", "token_id", token.TokenID, "error", err)
				}
				return
			}
			snapshots = append(snapshots, snapshot)
		}()
	}
	wg.Wait()
	return snapshots, failures
}
