// cmd/polymarket-latency-capture polls the Polymarket CLOB order book for
// every (lsports_fixture_id, polymarket_token_id) watch pair supplied on
// the command line and appends one row per tick per token to
// polymarket_latency_observations. It is the Polymarket half of the
// latency-measurement infrastructure (see memory/latency_measurement_thesis.md).
//
// Polling cadence:
//   Default 1.5 s. PM CLOB has no documented per-token rate cap; we bound
//   ourselves conservatively. The daemon serializes calls per tick so the
//   effective per-token cadence is (interval × token_count). For tight
//   latency measurement, keep watch sets small (1-2 tokens per fixture).
//
// What we record per tick:
//   best_bid  = max(orderbook.bids[].price)        — executable sell value
//   best_ask  = min(orderbook.asks[].price)        — executable buy cost
//   midpoint  = (best_bid + best_ask) / 2          — analytical only
//   spread    = best_ask - best_bid
//   liquidity = sum(top-3 bid sizes × prices) + sum(top-3 ask sizes × prices)
//   price_changed_from_prior = bid OR ask differs from this token's last tick
//
// Usage:
//
//	# Single watch (one fixture, one token):
//	go run ./cmd/polymarket-latency-capture --watch 12345:0xdeadbeef
//
//	# Multiple watches (typical: both home and away tokens for one fixture):
//	go run ./cmd/polymarket-latency-capture --watch 12345:0xaaa --watch 12345:0xbbb
//
//	# Single tick (smoke test):
//	go run ./cmd/polymarket-latency-capture --watch 12345:0xaaa --once
//
//	# Tune cadence:
//	go run ./cmd/polymarket-latency-capture --watch 12345:0xaaa --interval 1s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
	"github.com/your-org/pol-odds-collector/internal/store"
)

type watchSpec struct {
	LSportsFixtureID int64
	TokenID          string
}

type watchFlag struct {
	specs []watchSpec
}

func (w *watchFlag) String() string { return fmt.Sprintf("%v", w.specs) }

func (w *watchFlag) Set(value string) error {
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("--watch must be FIXTURE_ID:TOKEN_ID (got %q)", value)
	}
	fid, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil || fid <= 0 {
		return fmt.Errorf("--watch fixture id must be positive integer (got %q)", parts[0])
	}
	token := strings.TrimSpace(parts[1])
	if token == "" {
		return fmt.Errorf("--watch token id empty (got %q)", value)
	}
	w.specs = append(w.specs, watchSpec{LSportsFixtureID: fid, TokenID: token})
	return nil
}

type tokenState struct {
	conditionID string
	marketID    string
	lastBid     float64
	lastAsk     float64
	hasPrior    bool
}

func main() {
	var (
		watch    watchFlag
		interval = flag.Duration("interval", 1500*time.Millisecond, "per-tick sleep; effective per-token cadence is interval × len(watches)")
		once     = flag.Bool("once", false, "exit after one tick")
	)
	flag.Var(&watch, "watch", "FIXTURE_ID:TOKEN_ID pair; repeat to watch multiple tokens (e.g. home + away)")
	flag.Parse()

	_, _ = config.Load()

	if len(watch.specs) == 0 {
		fail("at least one --watch FIXTURE_ID:TOKEN_ID is required")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	clobBase := envOr("POLYMARKET_CLOB_BASE_URL", polymarket.DefaultCLOBBaseURL)
	dbURL := envOr("DATABASE_URL", "")
	if dbURL == "" {
		fail("DATABASE_URL is required (capture writes polymarket_latency_observations)")
	}

	client := polymarket.NewCLOBClient(clobBase, 15*time.Second)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := store.Open(ctx, dbURL, false, normalize.FreshnessPolicy{})
	if err != nil {
		fail("open postgres: %v", err)
	}
	defer func() { _ = st.Close() }()

	// Enrich each watch with condition_id / market_id from polymarket_tokens
	// if they're already synced locally. Failure is non-fatal; we just
	// record the token_id without enrichment.
	states := make(map[string]*tokenState, len(watch.specs))
	for _, w := range watch.specs {
		condID, marketID, _ := st.LookupPolymarketTokenContext(ctx, w.TokenID)
		states[w.TokenID] = &tokenState{conditionID: condID, marketID: marketID}
		if condID == "" && marketID == "" {
			slog.Warn("token not in polymarket_tokens; observation will have no condition/market enrichment",
				"token_id", w.TokenID, "fixture_id", w.LSportsFixtureID,
				"hint", "run cmd/polymarket-sync --target-event-id <id> first to enrich")
		}
	}

	slog.Info("polymarket-latency-capture started",
		"clob_base", clobBase,
		"watches", len(watch.specs),
		"interval", interval.String(),
		"once", *once,
	)

	for {
		tickStart := time.Now()
		for _, w := range watch.specs {
			if err := ctx.Err(); err != nil {
				return
			}
			if err := captureToken(ctx, client, st, w, states[w.TokenID]); err != nil {
				slog.Error("token tick", "fixture_id", w.LSportsFixtureID, "token_id", w.TokenID, "error", err)
			}
		}
		if *once {
			slog.Info("--once supplied; exiting", "elapsed", time.Since(tickStart).String())
			return
		}
		elapsed := time.Since(tickStart)
		sleep := *interval - elapsed
		if sleep < 100*time.Millisecond {
			sleep = 100 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

func captureToken(ctx context.Context, client *polymarket.CLOBClient, st *store.PostgresStore, w watchSpec, state *tokenState) error {
	book, err := client.GetOrderBook(ctx, w.TokenID)
	if err != nil {
		return err
	}
	receivedAt := time.Now().UTC()

	bestBid, bestAsk, ok := bestBidAsk(book)
	var (
		bidPtr, askPtr *float64
		midPtr         *float64
		spreadPtr      *float64
	)
	if ok {
		b, a := bestBid, bestAsk
		bidPtr, askPtr = &b, &a
		mid := (b + a) / 2
		midPtr = &mid
		sp := a - b
		spreadPtr = &sp
	}
	liqPtr := computeLiquidity(book)

	var lastPricePtr *float64
	if lp, perr := strconv.ParseFloat(strings.TrimSpace(book.LastTradePrice), 64); perr == nil && book.LastTradePrice != "" {
		lastPricePtr = &lp
	}

	var sourcePublish *time.Time
	if tt, ok := parseCLOBTimestamp(book.Timestamp); ok {
		sourcePublish = &tt
	}

	// price_changed_from_prior: compare best bid/ask against last tick's
	// in-memory snapshot for this token.
	var changedPtr *bool
	if state.hasPrior && ok {
		changed := bestBid != state.lastBid || bestAsk != state.lastAsk
		changedPtr = &changed
	}
	if ok {
		state.lastBid = bestBid
		state.lastAsk = bestAsk
		state.hasPrior = true
	}

	obs := model.PolymarketLatencyObservation{
		LSportsFixtureID:      w.LSportsFixtureID,
		PolymarketMarketID:    state.marketID,
		PolymarketConditionID: state.conditionID,
		PolymarketTokenID:     w.TokenID,
		ReceivedAt:            &receivedAt,
		SourcePublishTime:     sourcePublish,
		BestBid:               bidPtr,
		BestAsk:               askPtr,
		Midpoint:              midPtr,
		LastPrice:             lastPricePtr,
		Spread:                spreadPtr,
		Liquidity:             liqPtr,
		PriceChangedFromPrior: changedPtr,
		RawPayload:            json.RawMessage(book.Raw),
	}

	if _, err := st.InsertPolymarketLatencyObservation(ctx, obs); err != nil {
		return err
	}
	logAttrs := []any{
		"fixture_id", w.LSportsFixtureID,
		"token", truncate(w.TokenID, 14),
		"bid", fmtPrice(bidPtr),
		"ask", fmtPrice(askPtr),
		"spread", fmtPrice(spreadPtr),
	}
	if changedPtr != nil {
		logAttrs = append(logAttrs, "changed", *changedPtr)
	}
	slog.Info("captured PM tick", logAttrs...)
	return nil
}

func bestBidAsk(book polymarket.CLOBOrderBook) (bid, ask float64, ok bool) {
	hasBid, hasAsk := false, false
	for _, lvl := range book.Bids {
		p, err := strconv.ParseFloat(strings.TrimSpace(lvl.Price), 64)
		if err != nil {
			continue
		}
		if !hasBid || p > bid {
			bid = p
			hasBid = true
		}
	}
	for _, lvl := range book.Asks {
		p, err := strconv.ParseFloat(strings.TrimSpace(lvl.Price), 64)
		if err != nil {
			continue
		}
		if !hasAsk || p < ask {
			ask = p
			hasAsk = true
		}
	}
	return bid, ask, hasBid && hasAsk
}

// computeLiquidity sums the notional (price × size) for the top 3 bid and
// top 3 ask levels. Returns nil when no levels are parseable.
func computeLiquidity(book polymarket.CLOBOrderBook) *float64 {
	type lvlNotional struct {
		price, size float64
	}
	collect := func(levels []polymarket.CLOBBookLevel) []lvlNotional {
		out := make([]lvlNotional, 0, len(levels))
		for _, l := range levels {
			p, err := strconv.ParseFloat(strings.TrimSpace(l.Price), 64)
			if err != nil {
				continue
			}
			sz, err := strconv.ParseFloat(strings.TrimSpace(l.Size), 64)
			if err != nil {
				continue
			}
			out = append(out, lvlNotional{p, sz})
		}
		return out
	}
	bids := collect(book.Bids)
	asks := collect(book.Asks)
	if len(bids) == 0 && len(asks) == 0 {
		return nil
	}

	sortDesc := func(a []lvlNotional) {
		for i := 1; i < len(a); i++ {
			for j := i; j > 0 && a[j].price > a[j-1].price; j-- {
				a[j], a[j-1] = a[j-1], a[j]
			}
		}
	}
	sortAsc := func(a []lvlNotional) {
		for i := 1; i < len(a); i++ {
			for j := i; j > 0 && a[j].price < a[j-1].price; j-- {
				a[j], a[j-1] = a[j-1], a[j]
			}
		}
	}
	sortDesc(bids)
	sortAsc(asks)

	var notional float64
	for i := 0; i < 3 && i < len(bids); i++ {
		notional += bids[i].price * bids[i].size
	}
	for i := 0; i < 3 && i < len(asks); i++ {
		notional += asks[i].price * asks[i].size
	}
	return &notional
}

// parseCLOBTimestamp accepts the CLOB orderbook Timestamp field (typically
// an epoch-ms string per Polymarket docs) and returns a UTC time. Returns
// false for unparseable / empty values.
func parseCLOBTimestamp(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		switch {
		case n > 1e14:
			return time.UnixMicro(n).UTC(), true
		case n > 1e11:
			return time.UnixMilli(n).UTC(), true
		case n > 1e9:
			return time.Unix(n, 0).UTC(), true
		}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func fmtPrice(p *float64) string {
	if p == nil {
		return "-"
	}
	return strconv.FormatFloat(*p, 'f', 4, 64)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
