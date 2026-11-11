package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/normalize"
)

func TestDBIntegrationBetfairOfflineE2EReplayToComparison(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	store, err := Open(ctx, dsn, true, normalize.FreshnessPolicy{
		FreshAfter: 2 * time.Minute,
		AgingAfter: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	canonicalID := saveSportsbookComparisonFixture(ctx, t, store, db, "sportsbook-e2e-home-away", "Home FC", "Away FC")
	receivedAt := time.UnixMilli(1715000000000).UTC().Add(time.Minute)

	first := replayBetfairFixture(t, "market_cache_messages.jsonl", receivedAt)
	firstStats, err := store.SaveBetfairBatch(ctx, first.RawMessages, first.PersistenceCandidates)
	if err != nil {
		t.Fatalf("save first Betfair E2E replay batch: %v", err)
	}
	if firstStats.RawMessagesInserted != 6 || firstStats.CandidatesInserted != 8 || firstStats.CandidatesUnchanged != 0 || firstStats.CandidatesSkipped != 0 {
		t.Fatalf("first Betfair E2E replay stats=%+v", firstStats)
	}
	assertBetfairE2ECounts(ctx, t, db, betfairE2ECounts{
		RawMessages:    6,
		QuoteHistory:   8,
		LatestQuotes:   8,
		MappedRows:     0,
		RunnerBooks:    0,
		ReadinessRows:  0,
		ComparisonRows: 0,
		ReadyRows:      0,
	})
	assertBetfairRawStreamCount(ctx, t, db, "market_cache_messages.jsonl", 6)

	repeated := replayBetfairFixture(t, "market_cache_messages.jsonl", receivedAt)
	repeatedStats, err := store.SaveBetfairBatch(ctx, repeated.RawMessages, repeated.PersistenceCandidates)
	if err != nil {
		t.Fatalf("save repeated Betfair E2E replay batch: %v", err)
	}
	if repeatedStats.RawMessagesInserted != 6 || repeatedStats.CandidatesInserted != 0 || repeatedStats.CandidatesUnchanged != 8 || repeatedStats.CandidatesSkipped != 0 {
		t.Fatalf("repeated Betfair E2E replay stats=%+v", repeatedStats)
	}
	assertBetfairE2ECounts(ctx, t, db, betfairE2ECounts{
		RawMessages:    12,
		QuoteHistory:   8,
		LatestQuotes:   8,
		MappedRows:     0,
		RunnerBooks:    0,
		ReadinessRows:  0,
		ComparisonRows: 0,
		ReadyRows:      0,
	})
	assertBetfairRawStreamCount(ctx, t, db, "market_cache_messages.jsonl", 12)

	insertBetfairEventAlias(ctx, t, db, "event-1", "Home FC v Away FC", canonicalID, "confirmed")
	assertBetfairMappedRowsForMarket(ctx, t, db, "1.111", 0, "event alias only")

	insertBetfairMarketAlias(ctx, t, db, "1.111", "event-1", canonicalID, "confirmed", "MATCH_ODDS", "h2h")
	assertBetfairMappedRowsForMarket(ctx, t, db, "1.111", 0, "event and market aliases only")

	insertBetfairRunnerAlias(ctx, t, db, "1.111", 101, 0, "Home FC", "Home FC", "home", "confirmed")
	assertBetfairMappedRowsForMarket(ctx, t, db, "1.111", 2, "confirmed event, market, and runner aliases")
	assertBetfairE2EReadinessState(ctx, t, db, "1.111", "suspended")
	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM sportsbook_betfair_moneyline_comparison
        WHERE betfair_market_id = '1.111'
    `); got != 1 {
		t.Fatalf("non-ready comparison rows after aliases=%d want 1", got)
	}
	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM sportsbook_betfair_moneyline_ready_comparison
        WHERE betfair_market_id = '1.111'
    `); got != 0 {
		t.Fatalf("ready comparison rows before changed replay=%d want 0", got)
	}
	beforeBackImplied := scalarFloat(ctx, t, db, `
        SELECT betfair_back_implied_probability::float8
        FROM sportsbook_betfair_moneyline_comparison
        WHERE betfair_market_id = '1.111'
          AND sportsbook_outcome_name = 'Home FC'
    `)
	assertNear(t, beforeBackImplied, 1.0/2.10, 0.000001, "pre-change back implied probability")

	changed := replayBetfairFixture(t, "market_cache_messages_changed.jsonl", receivedAt)
	changedStats, err := store.SaveBetfairBatch(ctx, changed.RawMessages, changed.PersistenceCandidates)
	if err != nil {
		t.Fatalf("save changed Betfair E2E replay batch: %v", err)
	}
	if changedStats.RawMessagesInserted != 2 || changedStats.CandidatesInserted != 4 || changedStats.CandidatesUnchanged != 0 || changedStats.CandidatesSkipped != 0 {
		t.Fatalf("changed Betfair E2E replay stats=%+v", changedStats)
	}
	assertBetfairRawStreamCount(ctx, t, db, "market_cache_messages_changed.jsonl", 2)
	assertBetfairE2ECounts(ctx, t, db, betfairE2ECounts{
		RawMessages:    14,
		QuoteHistory:   12,
		LatestQuotes:   8,
		MappedRows:     2,
		RunnerBooks:    1,
		ReadinessRows:  1,
		ComparisonRows: 1,
		ReadyRows:      1,
	})
	assertBetfairE2EReadinessState(ctx, t, db, "1.111", "ready")
	assertBetfairE2ELatestAndComparison(ctx, t, db)

	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM sportsbook_betfair_moneyline_ready_comparison
        WHERE canonical_event_id = $1::uuid
          AND sportsbook_outcome_name = 'Away FC'
    `, canonicalID); got != 0 {
		t.Fatalf("ready comparison rows for unmatched away outcome=%d want 0", got)
	}
}

type betfairE2ECounts struct {
	RawMessages    int
	QuoteHistory   int
	LatestQuotes   int
	MappedRows     int
	RunnerBooks    int
	ReadinessRows  int
	ComparisonRows int
	ReadyRows      int
}

func assertBetfairE2ECounts(ctx context.Context, t *testing.T, db *sql.DB, want betfairE2ECounts) {
	t.Helper()
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_raw_stream_messages`); got != want.RawMessages {
		t.Fatalf("raw message count=%d want %d", got, want.RawMessages)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != want.QuoteHistory {
		t.Fatalf("quote history count=%d want %d", got, want.QuoteHistory)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_latest_exchange_quotes`); got != want.LatestQuotes {
		t.Fatalf("latest quote count=%d want %d", got, want.LatestQuotes)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM fair_value_ready_betfair_exchange_quotes`); got != want.MappedRows {
		t.Fatalf("mapped Betfair row count=%d want %d", got, want.MappedRows)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_mapped_runner_books`); got != want.RunnerBooks {
		t.Fatalf("runner book count=%d want %d", got, want.RunnerBooks)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_readiness`); got != want.ReadinessRows {
		t.Fatalf("readiness row count=%d want %d", got, want.ReadinessRows)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM sportsbook_betfair_moneyline_comparison`); got != want.ComparisonRows {
		t.Fatalf("comparison row count=%d want %d", got, want.ComparisonRows)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM sportsbook_betfair_moneyline_ready_comparison`); got != want.ReadyRows {
		t.Fatalf("ready comparison row count=%d want %d", got, want.ReadyRows)
	}
}

func assertBetfairRawStreamCount(ctx context.Context, t *testing.T, db *sql.DB, streamID string, want int) {
	t.Helper()
	got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM betfair_raw_stream_messages
        WHERE stream_id = $1
    `, streamID)
	if got != want {
		t.Fatalf("raw stream count for %s=%d want %d", streamID, got, want)
	}
}

func assertBetfairMappedRowsForMarket(ctx context.Context, t *testing.T, db *sql.DB, marketID string, want int, label string) {
	t.Helper()
	got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM fair_value_ready_betfair_exchange_quotes
        WHERE betfair_market_id = $1
    `, marketID)
	if got != want {
		t.Fatalf("mapped rows for %s after %s=%d want %d", marketID, label, got, want)
	}
}

func assertBetfairE2EReadinessState(ctx context.Context, t *testing.T, db *sql.DB, marketID, want string) {
	t.Helper()
	got := scalarString(ctx, t, db, `
        SELECT readiness_state
        FROM betfair_exchange_quote_readiness
        WHERE betfair_market_id = $1
    `, marketID)
	if got != want {
		t.Fatalf("readiness state for %s=%q want %q", marketID, got, want)
	}
}

func assertBetfairE2ELatestAndComparison(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	var backPrice, backSize, layPrice, laySize, ltp, tradedVolume float64
	var marketStatus string
	var inPlay bool
	if err := db.QueryRowContext(ctx, `
        SELECT
            max(price_decimal) FILTER (WHERE exchange_side = 'back')::float8,
            max(available_size) FILTER (WHERE exchange_side = 'back')::float8,
            max(price_decimal) FILTER (WHERE exchange_side = 'lay')::float8,
            max(available_size) FILTER (WHERE exchange_side = 'lay')::float8,
            max(last_traded_price)::float8,
            max(traded_volume)::float8,
            max(market_status),
            bool_or(coalesce(in_play, false))
        FROM betfair_latest_exchange_quotes_view
        WHERE market_id = '1.111'
          AND selection_id = 101
    `).Scan(&backPrice, &backSize, &layPrice, &laySize, &ltp, &tradedVolume, &marketStatus, &inPlay); err != nil {
		t.Fatalf("query E2E latest Betfair state: %v", err)
	}
	assertNear(t, backPrice, 2.30, 0.000001, "latest best back price")
	assertNear(t, backSize, 75, 0.000001, "latest best back size")
	assertNear(t, layPrice, 2.34, 0.000001, "latest best lay price")
	assertNear(t, laySize, 80, 0.000001, "latest best lay size")
	assertNear(t, ltp, 2.20, 0.000001, "latest last traded price metadata")
	assertNear(t, tradedVolume, 120, 0.000001, "latest traded volume metadata")
	if marketStatus != "OPEN" || inPlay {
		t.Fatalf("latest market status/in_play=%q/%t want OPEN/false", marketStatus, inPlay)
	}

	var (
		sportsbookProbability float64
		backImplied           float64
		layImplied            float64
		backGap               float64
		layGap                float64
		comparisonLTP         float64
		comparisonVolume      float64
		readinessState        string
	)
	if err := db.QueryRowContext(ctx, `
        SELECT
            sportsbook_avg_no_vig_probability::float8,
            betfair_back_implied_probability::float8,
            betfair_lay_implied_probability::float8,
            back_probability_gap::float8,
            lay_probability_gap::float8,
            last_traded_price::float8,
            traded_volume::float8,
            betfair_readiness_state
        FROM sportsbook_betfair_moneyline_ready_comparison
        WHERE betfair_market_id = '1.111'
          AND sportsbook_outcome_name = 'Home FC'
    `).Scan(
		&sportsbookProbability,
		&backImplied,
		&layImplied,
		&backGap,
		&layGap,
		&comparisonLTP,
		&comparisonVolume,
		&readinessState,
	); err != nil {
		t.Fatalf("query E2E comparison state: %v", err)
	}
	if readinessState != "ready" {
		t.Fatalf("comparison readiness state=%q want ready", readinessState)
	}
	assertNear(t, sportsbookProbability, 0.525, 0.000001, "sportsbook no-vig probability")
	assertNear(t, backImplied, 1.0/2.30, 0.000001, "changed back implied probability")
	assertNear(t, layImplied, 1.0/2.34, 0.000001, "changed lay implied probability")
	assertNear(t, backGap, 0.525-(1.0/2.30), 0.000001, "changed back probability gap")
	assertNear(t, layGap, (1.0/2.34)-0.525, 0.000001, "changed lay probability gap")
	assertNear(t, comparisonLTP, 2.20, 0.000001, "comparison last traded price metadata")
	assertNear(t, comparisonVolume, 120, 0.000001, "comparison traded volume metadata")
}

func scalarFloat(ctx context.Context, t *testing.T, db *sql.DB, query string, args ...any) float64 {
	t.Helper()
	var got float64
	if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("query scalar float: %v\n%s", err, query)
	}
	return got
}
