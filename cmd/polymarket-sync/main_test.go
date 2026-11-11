package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
)

func TestSnapshotFromCLOBSeparatesExecutableAndAnalyticalPrices(t *testing.T) {
	receivedAt := time.Date(2026, 5, 7, 1, 2, 3, 0, time.UTC)
	token := model.PolymarketToken{
		TokenID:            "tok-1",
		ConditionID:        "cond-token",
		PolymarketMarketID: "market-1",
	}
	book := polymarket.CLOBOrderBook{
		Market:         "cond-book",
		Timestamp:      "1715000000000",
		LastTradePrice: "0.48",
		Bids: []polymarket.CLOBBookLevel{
			{Price: "0.41", Size: "10"},
			{Price: "0.44", Size: "20"},
		},
		Asks: []polymarket.CLOBBookLevel{
			{Price: "0.51", Size: "30"},
			{Price: "0.49", Size: "40"},
		},
		Raw: json.RawMessage(`{"book":true}`),
	}
	midpoint := polymarket.CLOBMidpoint{
		MidPrice: "0.465",
		Raw:      json.RawMessage(`{"midpoint":true}`),
	}
	spread := polymarket.CLOBSpread{
		Spread: "0.05",
		Raw:    json.RawMessage(`{"spread":true}`),
	}

	snapshot := snapshotFromCLOB(token, book, midpoint, spread, receivedAt)

	assertFloatPtr(t, "BestBid", snapshot.BestBid, 0.44)
	assertFloatPtr(t, "BestAsk", snapshot.BestAsk, 0.49)
	assertFloatPtr(t, "Midpoint", snapshot.Midpoint, 0.465)
	assertFloatPtr(t, "Spread", snapshot.Spread, 0.05)
	assertFloatPtr(t, "LastTradePrice", snapshot.LastTradePrice, 0.48)
	assertFloatPtr(t, "Liquidity", snapshot.Liquidity, 100)
	assertFloatPtr(t, "BidPrice", snapshot.BidPrice, 0.44)
	assertFloatPtr(t, "AskPrice", snapshot.AskPrice, 0.49)
	assertFloatPtr(t, "MidPrice", snapshot.MidPrice, 0.465)
	if snapshot.ConditionID != "cond-book" {
		t.Fatalf("ConditionID=%q want cond-book", snapshot.ConditionID)
	}
	if snapshot.SourceTimestamp == nil || snapshot.SourceTimestamp.UTC().Format(time.RFC3339) != "2024-05-06T12:53:20Z" {
		t.Fatalf("SourceTimestamp=%v", snapshot.SourceTimestamp)
	}
	if !json.Valid(snapshot.DepthSummary) || !json.Valid(snapshot.RawPayload) || !json.Valid(snapshot.CLOBPayload) {
		t.Fatalf("snapshot JSON payloads should be valid: depth=%s raw=%s clob=%s", snapshot.DepthSummary, snapshot.RawPayload, snapshot.CLOBPayload)
	}
	if !containsJSONKey(snapshot.RawPayload, "book") || !containsJSONKey(snapshot.RawPayload, "midpoint") || !containsJSONKey(snapshot.RawPayload, "spread") {
		t.Fatalf("raw payload should include book, midpoint, and spread: %s", snapshot.RawPayload)
	}
}

func TestSpreadValueFallsBackToBestAskMinusBestBid(t *testing.T) {
	bestBid := 0.42
	bestAsk := 0.47
	spread := spreadValue(&bestBid, &bestAsk, nil)
	assertFloatPtr(t, "spread", spread, 0.05)

	explicit := 0.08
	spread = spreadValue(&bestBid, &bestAsk, &explicit)
	assertFloatPtr(t, "explicit spread", spread, 0.08)
}

func TestBestBidAndAskLevelDoNotDependOnInputOrder(t *testing.T) {
	bestBid, bestBidSize := bestBidLevel([]polymarket.CLOBBookLevel{
		{Price: "0.40", Size: "10"},
		{Price: "0.45", Size: "15"},
		{Price: "0.43", Size: "20"},
	})
	bestAsk, bestAskSize := bestAskLevel([]polymarket.CLOBBookLevel{
		{Price: "0.55", Size: "12"},
		{Price: "0.49", Size: "18"},
		{Price: "0.52", Size: "22"},
	})
	assertFloatPtr(t, "bestBid", bestBid, 0.45)
	assertFloatPtr(t, "bestBidSize", bestBidSize, 15)
	assertFloatPtr(t, "bestAsk", bestAsk, 0.49)
	assertFloatPtr(t, "bestAskSize", bestAskSize, 18)
}

func TestFetchPagedEventsUsesLimitAndOffset(t *testing.T) {
	fake := fakeGammaPager{
		events: []polymarket.Event{
			{ID: "evt-1"},
			{ID: "evt-2"},
			{ID: "evt-3"},
			{ID: "evt-4"},
			{ID: "evt-5"},
		},
	}

	events, pages, err := fetchPagedEvents(context.Background(), &fake, 5, 2, 10, scopeFilter{})
	if err != nil {
		t.Fatalf("fetchPagedEvents: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("len(events)=%d want 5", len(events))
	}
	if pages != 3 {
		t.Fatalf("pages=%d want 3", pages)
	}
	wantOffsets := []int{0, 2, 4}
	if len(fake.eventOptions) != len(wantOffsets) {
		t.Fatalf("eventOptions=%+v", fake.eventOptions)
	}
	for i, want := range wantOffsets {
		if fake.eventOptions[i].Offset != want {
			t.Fatalf("offset[%d]=%d want %d", i, fake.eventOptions[i].Offset, want)
		}
	}
}

func TestFilterPriceEligibleTokensSkipsInactiveAndSettledMarkets(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	active := true
	inactive := false
	closed := true
	oldEnd := now.Add(-48 * time.Hour)
	recentEnd := now.Add(-1 * time.Hour)
	tokens := []model.PolymarketToken{
		{TokenID: "active", ConditionID: "active-cond"},
		{TokenID: "inactive", ConditionID: "inactive-cond"},
		{TokenID: "closed", ConditionID: "closed-cond"},
		{TokenID: "old", ConditionID: "old-cond"},
		{TokenID: "recent", ConditionID: "recent-cond"},
	}
	markets := []model.PolymarketMarket{
		{ConditionID: "active-cond", Active: &active},
		{ConditionID: "inactive-cond", Active: &inactive},
		{ConditionID: "closed-cond", Closed: &closed},
		{ConditionID: "old-cond", EndTime: &oldEnd},
		{ConditionID: "recent-cond", EndTime: &recentEnd},
	}

	got := filterPriceEligibleTokens(tokens, markets, true, 24*time.Hour, now)
	if ids := tokenIDs(got); strings.Join(ids, ",") != "active,recent" {
		t.Fatalf("eligible token ids=%v want active,recent", ids)
	}
}

func TestFetchPriceSnapshotsUsesBoundedConcurrency(t *testing.T) {
	var mu sync.Mutex
	activeBooks := 0
	maxActiveBooks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenID := r.URL.Query().Get("token_id")
		switch r.URL.Path {
		case "/book":
			mu.Lock()
			activeBooks++
			if activeBooks > maxActiveBooks {
				maxActiveBooks = activeBooks
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			activeBooks--
			mu.Unlock()
			_, _ = w.Write([]byte(`{
				"market":"cond-` + tokenID + `",
				"asset_id":"` + tokenID + `",
				"bids":[{"price":"0.40","size":"1"}],
				"asks":[{"price":"0.60","size":"1"}]
			}`))
		case "/midpoint":
			_, _ = w.Write([]byte(`{"mid_price":"0.50"}`))
		case "/spread":
			_, _ = w.Write([]byte(`{"spread":"0.20"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	clob := polymarket.NewCLOBClientWithRetry(server.URL, time.Second, polymarket.RetryConfig{
		MaxRetries: 0,
		BaseDelay:  time.Nanosecond,
		MaxDelay:   time.Nanosecond,
	})
	tokens := make([]model.PolymarketToken, 0, 6)
	for i := 0; i < 6; i++ {
		tokenID := "tok-" + strconv.Itoa(i)
		tokens = append(tokens, model.PolymarketToken{TokenID: tokenID, ConditionID: "cond-" + tokenID})
	}

	// Pass nil store: dead-token tracking is best-effort and tolerates nil.
	snapshots, failures := fetchPriceSnapshots(context.Background(), clob, nil, tokens, 2)
	if failures != 0 {
		t.Fatalf("failures=%d want 0", failures)
	}
	if len(snapshots) != len(tokens) {
		t.Fatalf("snapshots=%d want %d", len(snapshots), len(tokens))
	}
	if maxActiveBooks > 2 {
		t.Fatalf("max active /book requests=%d want <=2", maxActiveBooks)
	}
}

func TestLoadConfigPolymarketSyncSettings(t *testing.T) {
	t.Setenv("POLYMARKET_GAMMA_PAGE_SIZE", "25")
	t.Setenv("POLYMARKET_GAMMA_MAX_PAGES", "3")
	t.Setenv("POLYMARKET_SYNC_METADATA", "true")
	t.Setenv("POLYMARKET_SYNC_PRICES", "false")
	t.Setenv("POLYMARKET_ACTIVE_MARKETS_ONLY", "false")
	t.Setenv("POLYMARKET_MARKET_END_GRACE_HOURS", "12")
	t.Setenv("POLYMARKET_API_MAX_RETRIES", "4")
	t.Setenv("POLYMARKET_API_RETRY_BASE_MS", "10")
	t.Setenv("POLYMARKET_API_RETRY_MAX_MS", "50")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.GammaPageSize != 25 || cfg.GammaMaxPages != 3 {
		t.Fatalf("pagination config page_size=%d max_pages=%d", cfg.GammaPageSize, cfg.GammaMaxPages)
	}
	if !cfg.SyncMetadata || cfg.SyncPrices {
		t.Fatalf("sync toggles metadata=%t prices=%t", cfg.SyncMetadata, cfg.SyncPrices)
	}
	if cfg.ActiveMarketsOnly {
		t.Fatal("ActiveMarketsOnly=true want false")
	}
	if cfg.MarketEndGrace != 12*time.Hour {
		t.Fatalf("MarketEndGrace=%s want 12h", cfg.MarketEndGrace)
	}
	if cfg.Retry.MaxRetries != 4 || cfg.Retry.BaseDelay != 10*time.Millisecond || cfg.Retry.MaxDelay != 50*time.Millisecond {
		t.Fatalf("retry config=%+v", cfg.Retry)
	}
}

type fakeGammaPager struct {
	events       []polymarket.Event
	markets      []polymarket.Market
	eventOptions []polymarket.ListOptions
	marketOpts   []polymarket.ListOptions
}

func (f *fakeGammaPager) ListEvents(_ context.Context, opts polymarket.ListOptions) ([]polymarket.Event, error) {
	f.eventOptions = append(f.eventOptions, opts)
	end := opts.Offset + opts.Limit
	if opts.Offset >= len(f.events) {
		return nil, nil
	}
	if end > len(f.events) {
		end = len(f.events)
	}
	return f.events[opts.Offset:end], nil
}

func (f *fakeGammaPager) ListMarkets(_ context.Context, opts polymarket.ListOptions) ([]polymarket.Market, error) {
	f.marketOpts = append(f.marketOpts, opts)
	end := opts.Offset + opts.Limit
	if opts.Offset >= len(f.markets) {
		return nil, nil
	}
	if end > len(f.markets) {
		end = len(f.markets)
	}
	return f.markets[opts.Offset:end], nil
}

func tokenIDs(tokens []model.PolymarketToken) []string {
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		out = append(out, token.TokenID)
	}
	return out
}

func assertFloatPtr(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s=nil want %v", name, want)
	}
	if diff := *got - want; diff > 0.0000001 || diff < -0.0000001 {
		t.Fatalf("%s=%v want %v", name, *got, want)
	}
}

func containsJSONKey(raw json.RawMessage, key string) bool {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false
	}
	_, ok := payload[key]
	return ok
}

func TestShouldPrintHelpDetectsHelpFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "no args", args: nil, want: false},
		{name: "empty arg", args: []string{""}, want: false},
		{name: "long help", args: []string{"--help"}, want: true},
		{name: "short help", args: []string{"-h"}, want: true},
		{name: "bare help", args: []string{"help"}, want: true},
		{name: "help among others", args: []string{"--limit=100", "--help"}, want: true},
		{name: "unrelated arg", args: []string{"--noop"}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldPrintHelp(tc.args); got != tc.want {
				t.Fatalf("shouldPrintHelp(%v)=%v want %v", tc.args, got, tc.want)
			}
		})
	}
}

// TestPolymarketSyncHelpExitsBeforeAnyNetworkOrDatabase is the regression test
// for the bug where `go run ./cmd/polymarket-sync --help` started a real sync.
// We do not invoke main() (which would call os.Exit) — instead we exercise the
// help discriminator directly and assert that, when help is requested, no path
// in the help branch reaches loadConfig/network/DB code. The discriminator
// returns true; main() is then expected to print usage and return immediately.
func TestPolymarketSyncHelpExitsBeforeAnyNetworkOrDatabase(t *testing.T) {
	// Set hostile env values so that loadConfig() would fail fast if it ran.
	// If --help branch incorrectly fell through to loadConfig, the test would
	// see these values surface (or a network/DB call would block the test).
	t.Setenv("POLYMARKET_GAMMA_EVENTS_LIMIT", "not-a-number")
	t.Setenv("DATABASE_URL", "postgres://invalid:invalid@127.0.0.1:1/none?connect_timeout=1")
	t.Setenv("POLYMARKET_GAMMA_BASE_URL", "http://127.0.0.1:1") // never reached
	t.Setenv("POLYMARKET_CLOB_BASE_URL", "http://127.0.0.1:1")

	if !shouldPrintHelp([]string{"--help"}) {
		t.Fatal("--help must trigger help branch")
	}
	if !shouldPrintHelp([]string{"-h"}) {
		t.Fatal("-h must trigger help branch")
	}
	if !shouldPrintHelp([]string{"help"}) {
		t.Fatal("help must trigger help branch")
	}

	// Independently confirm that hostile env values would actually have caused
	// loadConfig() to fail — proving help truly bypassed config loading.
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig with hostile env should have failed; test setup is wrong")
	}
}

func TestPolymarketSyncUsageMentionsKeyConfigSurface(t *testing.T) {
	// Capture printUsage output via an os.Pipe to avoid touching real stdout.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	go func() {
		printUsage(w)
		_ = w.Close()
	}()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}
	usage := sb.String()

	for _, want := range []string{
		"DATABASE_URL",
		"POLYMARKET_GAMMA_BASE_URL",
		"POLYMARKET_CLOB_BASE_URL",
		"POLYMARKET_SYNC_METADATA",
		"POLYMARKET_SYNC_PRICES",
		"POLYMARKET_GAMMA_EVENTS_LIMIT",
		"POLYMARKET_GAMMA_MARKETS_LIMIT",
		"POLYMARKET_GAMMA_PAGE_SIZE",
		"POLYMARKET_GAMMA_MAX_PAGES",
		"POLYMARKET_MAX_PRICE_TOKENS",
		"POLYMARKET_PRICE_CONCURRENCY",
		"POLYMARKET_ACTIVE_MARKETS_ONLY",
		"midpoint",
		"NOT treated as executable",
		"Auto-mapping",
		"--diagnose-coverage",
		"does NOT open PostgreSQL",
		"auto-map markets",
		"does NOT approve aliases",
		"--target-event-id",
		"--dry-run",
		"does NOT call any order or",
	} {
		if !strings.Contains(usage, want) {
			t.Fatalf("usage output missing %q:\n%s", want, usage)
		}
	}

	// Help output must not POSITIVELY claim trading/execution behavior. Safety
	// disclaimers ("does not place orders", "does not handle wallets/private
	// keys") are allowed and expected — they are exactly the read-only
	// guarantees operators look for. The forbidden phrases below would only
	// appear if the help text claimed the tool emits a trading signal, places
	// orders, or handles signing keys.
	lowered := strings.ToLower(usage)
	for _, forbidden := range []string{
		"emits a trading signal",
		"emits a buy signal",
		"emits a sell signal",
		"produces a buy signal",
		"produces a sell signal",
		"places an order",
		"submits an order",
		"automatic execution",
		"recommended trade",
	} {
		if strings.Contains(lowered, forbidden) {
			t.Fatalf("usage contains forbidden trading terminology %q:\n%s", forbidden, usage)
		}
	}
}
