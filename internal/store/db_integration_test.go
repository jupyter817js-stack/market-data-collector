package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

func TestDBIntegrationMigrationsFreshAndRerunnable(t *testing.T) {
	ctx, db, _ := newIntegrationDB(t)
	files := integrationMigrationFiles(t)

	applied := applyIntegrationMigrations(ctx, t, db, files)
	if applied != len(files) {
		t.Fatalf("fresh migration applied %d files, want %d", applied, len(files))
	}
	assertMigrationCount(ctx, t, db, len(files))
	assertRelationExists(ctx, t, db, "canonical_events", "r")
	assertRelationExists(ctx, t, db, "fair_value_moneyline_consensus", "v")
	assertRelationExists(ctx, t, db, "sportsbook_polymarket_moneyline_edge_readiness", "v")

	applied = applyIntegrationMigrations(ctx, t, db, files)
	if applied != 0 {
		t.Fatalf("rerun applied %d files, want 0", applied)
	}
	assertMigrationCount(ctx, t, db, len(files))

	// Guard against a silent-DDL-failure regression in cmd/migrate.
	// pgx/v5 stdlib's extended protocol dropped every statement after the
	// first in a multi-statement Exec, so post-migration DBs ended up with
	// 2 views instead of 48 while schema_migrations claimed success. The
	// splitter in cmd/migrate/sqlsplit.go fixes that; if a future change
	// regresses it, the view count crashes and this catches it.
	var viewCount int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM pg_views WHERE schemaname = current_schema()`).Scan(&viewCount); err != nil {
		t.Fatalf("count views: %v", err)
	}
	if viewCount < 40 {
		t.Fatalf("post-migration view count %d < 40 — splitter regression?", viewCount)
	}
}

func TestDBIntegrationSportsbookPolymarketEdgesAndReadiness(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	now := time.Now().UTC().Truncate(time.Second)
	receivedAt := now.Add(-1 * time.Minute)
	commenceAt := now.Add(2 * time.Hour)

	sportsbookStore, err := Open(ctx, dsn, true, normalize.FreshnessPolicy{
		FreshAfter: 2 * time.Minute,
		AgingAfter: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("open sportsbook store: %v", err)
	}
	t.Cleanup(func() { _ = sportsbookStore.Close() })

	stats, err := sportsbookStore.SaveBatch(ctx, sportsbookBatch(receivedAt, commenceAt))
	if err != nil {
		t.Fatalf("save sportsbook batch: %v", err)
	}
	if stats.QuotesInserted != 4 {
		t.Fatalf("quotes inserted=%d want 4", stats.QuotesInserted)
	}

	assertMoneylineConsensus(ctx, t, db)
	canonicalID := scalarString(ctx, t, db, `
        SELECT canonical_event_id::text
        FROM fair_value_moneyline_consensus
        WHERE outcome_name = 'Home FC'
    `)

	polymarketStore, err := OpenPolymarket(ctx, dsn)
	if err != nil {
		t.Fatalf("open polymarket store: %v", err)
	}
	t.Cleanup(func() { _ = polymarketStore.Close() })

	upsertPolymarketFixture(ctx, t, polymarketStore, now)
	insertUnresolvedReview(ctx, t, db, canonicalID, commenceAt)

	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM sportsbook_polymarket_moneyline_edges
        WHERE polymarket_event_id = 'pm-event-1'
    `); got != 0 {
		t.Fatalf("edge rows before confirmed alias=%d want 0", got)
	}
	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM sportsbook_polymarket_moneyline_edge_readiness
        WHERE polymarket_event_id = 'pm-event-1'
          AND readiness_state = 'mapping_uncertain'
    `); got == 0 {
		t.Fatal("unresolved Polymarket review did not surface mapping_uncertain readiness rows")
	}

	confidence := 95.0
	if err := polymarketStore.ApprovePolymarketMatch(ctx, model.PolymarketMatchDecision{
		PolymarketEventID:  "pm-event-1",
		PolymarketMarketID: "pm-market-1",
		ConditionID:        "condition-1",
		CanonicalEventID:   canonicalID,
		Reviewer:           "integration-test",
		ReviewReason:       "fixture teams and start time match",
		ConfidenceScore:    &confidence,
		MatchReason:        "exact fixture approval",
	}); err != nil {
		t.Fatalf("approve polymarket match: %v", err)
	}

	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM sportsbook_polymarket_moneyline_edges
        WHERE polymarket_event_id = 'pm-event-1'
    `); got != 2 {
		t.Fatalf("edge rows after confirmed alias=%d want 2", got)
	}

	var bestAsk, midpoint, edgeToBuyCost float64
	var latestPriceAt time.Time
	if err := db.QueryRowContext(ctx, `
        SELECT
            polymarket_best_ask::float8,
            polymarket_midpoint::float8,
            edge_to_buy_cost::float8,
            polymarket_price_received_at
        FROM sportsbook_polymarket_moneyline_edges
        WHERE polymarket_token_id = 'token-home'
    `).Scan(&bestAsk, &midpoint, &edgeToBuyCost, &latestPriceAt); err != nil {
		t.Fatalf("query home edge row: %v", err)
	}
	assertNear(t, bestAsk, 0.50, 0.000001, "latest best ask")
	assertNear(t, midpoint, 0.48, 0.000001, "latest midpoint")
	assertNear(t, edgeToBuyCost, 0.025, 0.000001, "edge to buy cost")
	if !latestPriceAt.Equal(now.Add(-1 * time.Minute)) {
		t.Fatalf("latest price timestamp=%s want %s", latestPriceAt, now.Add(-1*time.Minute))
	}

	var readinessState, quoteFreshness, priceFreshness string
	var readinessScore int
	if err := db.QueryRowContext(ctx, `
        SELECT readiness_state, readiness_score, quote_freshness, polymarket_price_freshness
        FROM sportsbook_polymarket_moneyline_edge_readiness
        WHERE polymarket_token_id = 'token-home'
    `).Scan(&readinessState, &readinessScore, &quoteFreshness, &priceFreshness); err != nil {
		t.Fatalf("query readiness row: %v", err)
	}
	if readinessState != "ready" {
		t.Fatalf("readiness state=%q want ready", readinessState)
	}
	if readinessScore != 100 {
		t.Fatalf("readiness score=%d want 100", readinessScore)
	}
	if quoteFreshness != "fresh" || priceFreshness != "fresh" {
		t.Fatalf("freshness quote=%q price=%q want fresh/fresh", quoteFreshness, priceFreshness)
	}
}

func TestDBIntegrationRejectedAliasDoesNotUnlockEdge(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	now := time.Now().UTC().Truncate(time.Second)
	receivedAt := now.Add(-1 * time.Minute)
	commenceAt := now.Add(2 * time.Hour)

	sportsbookStore, err := Open(ctx, dsn, true, normalize.FreshnessPolicy{
		FreshAfter: 2 * time.Minute,
		AgingAfter: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("open sportsbook store: %v", err)
	}
	t.Cleanup(func() { _ = sportsbookStore.Close() })
	if _, err := sportsbookStore.SaveBatch(ctx, sportsbookBatch(receivedAt, commenceAt)); err != nil {
		t.Fatalf("save sportsbook batch: %v", err)
	}

	polymarketStore, err := OpenPolymarket(ctx, dsn)
	if err != nil {
		t.Fatalf("open polymarket store: %v", err)
	}
	t.Cleanup(func() { _ = polymarketStore.Close() })
	upsertPolymarketFixture(ctx, t, polymarketStore, now)

	if err := polymarketStore.RejectPolymarketMatch(ctx, model.PolymarketMatchDecision{
		PolymarketEventID:  "pm-event-1",
		PolymarketMarketID: "pm-market-1",
		ConditionID:        "condition-1",
		Reviewer:           "integration-test",
		ReviewReason:       "fixture intentionally rejected",
		MatchReason:        "rejection must not create an alias",
	}); err != nil {
		t.Fatalf("reject polymarket match: %v", err)
	}

	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM sportsbook_polymarket_moneyline_edges
        WHERE polymarket_event_id = 'pm-event-1'
    `); got != 0 {
		t.Fatalf("edge rows after rejected alias=%d want 0", got)
	}
	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM confirmed_polymarket_event_aliases
        WHERE polymarket_event_id = 'pm-event-1'
    `); got != 0 {
		t.Fatalf("confirmed aliases after rejection=%d want 0", got)
	}
	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM match_review_decisions
        WHERE polymarket_event_id = 'pm-event-1'
          AND decision_status = 'rejected'
    `); got != 1 {
		t.Fatalf("rejected decisions=%d want 1", got)
	}
}

func TestDBIntegrationDataQualityAndReadinessStates(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	now := time.Now().UTC().Truncate(time.Second)
	receivedAt := now.Add(-1 * time.Minute)
	commenceAt := now.Add(2 * time.Hour)

	sportsbookStore, err := Open(ctx, dsn, true, normalize.FreshnessPolicy{
		FreshAfter: 2 * time.Minute,
		AgingAfter: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("open sportsbook store: %v", err)
	}
	t.Cleanup(func() { _ = sportsbookStore.Close() })

	saveSportsbookEvent := func(eventID, home, away string, lastUpdate time.Time) string {
		t.Helper()
		if _, err := sportsbookStore.SaveBatch(ctx, sportsbookBatchFor(eventID, home, away, receivedAt, commenceAt, lastUpdate)); err != nil {
			t.Fatalf("save sportsbook event %s: %v", eventID, err)
		}
		return scalarString(ctx, t, db, `
            SELECT id::text
            FROM canonical_events
            WHERE home_team = $1
              AND away_team = $2
        `, home, away)
	}

	missingCanonicalID := saveSportsbookEvent("sportsbook-missing-price", "Missing Price Home", "Missing Price Away", receivedAt.Add(-1*time.Minute))
	wideCanonicalID := saveSportsbookEvent("sportsbook-wide-spread", "Wide Spread Home", "Wide Spread Away", receivedAt.Add(-1*time.Minute))
	staleCanonicalID := saveSportsbookEvent("sportsbook-stale", "Stale Home", "Stale Away", receivedAt.Add(-10*time.Minute))

	polymarketStore, err := OpenPolymarket(ctx, dsn)
	if err != nil {
		t.Fatalf("open polymarket store: %v", err)
	}
	t.Cleanup(func() { _ = polymarketStore.Close() })

	upsertPolymarketFixtureNamed(ctx, t, polymarketStore, now, polymarketFixture{
		EventID:      "pm-missing-price",
		MarketID:     "pm-market-missing-price",
		ConditionID:  "condition-missing-price",
		Home:         "Missing Price Home",
		Away:         "Missing Price Away",
		HomeTokenID:  "token-missing-price-home",
		AwayTokenID:  "token-missing-price-away",
		CanonicalID:  missingCanonicalID,
		WithSnapshot: false,
	})
	upsertPolymarketFixtureNamed(ctx, t, polymarketStore, now, polymarketFixture{
		EventID:      "pm-wide-spread",
		MarketID:     "pm-market-wide-spread",
		ConditionID:  "condition-wide-spread",
		Home:         "Wide Spread Home",
		Away:         "Wide Spread Away",
		HomeTokenID:  "token-wide-spread-home",
		AwayTokenID:  "token-wide-spread-away",
		CanonicalID:  wideCanonicalID,
		WithSnapshot: true,
		BestBid:      0.40,
		BestAsk:      0.55,
		Midpoint:     0.475,
		Spread:       0.15,
		Liquidity:    100,
	})
	upsertPolymarketFixtureNamed(ctx, t, polymarketStore, now, polymarketFixture{
		EventID:      "pm-stale-sportsbook",
		MarketID:     "pm-market-stale-sportsbook",
		ConditionID:  "condition-stale-sportsbook",
		Home:         "Stale Home",
		Away:         "Stale Away",
		HomeTokenID:  "token-stale-sportsbook-home",
		AwayTokenID:  "token-stale-sportsbook-away",
		CanonicalID:  staleCanonicalID,
		WithSnapshot: true,
		BestBid:      0.45,
		BestAsk:      0.49,
		Midpoint:     0.47,
		Spread:       0.04,
		Liquidity:    100,
	})

	assertReadinessState(ctx, t, db, "token-missing-price-home", "missing_price")
	assertReadinessState(ctx, t, db, "token-wide-spread-home", "wide_spread")
	if state := readinessState(ctx, t, db, "token-stale-sportsbook-home"); state == "ready" {
		t.Fatalf("stale sportsbook readiness state=%q want not ready", state)
	}
	reasons := scalarString(ctx, t, db, `
        SELECT array_to_string(readiness_reasons, '; ')
        FROM sportsbook_polymarket_moneyline_edge_readiness
        WHERE polymarket_token_id = 'token-stale-sportsbook-home'
    `)
	if !strings.Contains(strings.ToLower(reasons), "sportsbook") {
		t.Fatalf("stale sportsbook readiness reasons=%q want sportsbook reason", reasons)
	}

	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM data_quality_sportsbook_anomalies
        WHERE issue_type = 'stale_sportsbook_quote'
          AND home_team = 'Stale Home'
    `); got == 0 {
		t.Fatal("expected stale sportsbook anomaly rows")
	}
	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM data_quality_polymarket_anomalies
        WHERE issue_type = 'missing_polymarket_price_snapshot'
          AND condition_id = 'condition-missing-price'
    `); got == 0 {
		t.Fatal("expected missing Polymarket price anomaly rows")
	}
	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM sportsbook_polymarket_moneyline_edges
        WHERE polymarket_event_id = 'pm-missing-price'
    `); got != 0 {
		t.Fatalf("missing-price edge rows=%d want 0", got)
	}
}

func newIntegrationDB(t *testing.T) (context.Context, *sql.DB, string) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("RUN_DB_TESTS")) != "1" {
		t.Skip("skipping DB integration tests; set RUN_DB_TESTS=1 and TEST_DATABASE_URL to run")
	}
	baseDSN := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if baseDSN == "" {
		t.Skip("skipping DB integration tests; TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	admin, err := sql.Open("pgx", baseDSN)
	if err != nil {
		t.Fatalf("open TEST_DATABASE_URL: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("ping TEST_DATABASE_URL: %v", err)
	}

	// Refuse to run if TEST_DATABASE_URL points at a live operational DB.
	// Integration tests set search_path=dbtest_xxx,public so the migration
	// files' unqualified `DROP VIEW IF EXISTS pinnacle_polymarket_*` fall
	// through to public and would destroy the live edge views the
	// signal-engine reads from. A freshly-migrated CI DB has the same
	// schema as a live DB, so view presence is not the right signal —
	// we discriminate by *data* presence. pinnacle_market_history and
	// polymarket_price_snapshots are append-only tables that grow every
	// poll; on CI they are guaranteed empty, on a live DB they have
	// millions of rows.
	var liveDataRows int64
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM (
			(SELECT 1 FROM public.pinnacle_market_history LIMIT 1)
			UNION ALL
			(SELECT 1 FROM public.polymarket_price_snapshots LIMIT 1)
		) t
	`).Scan(&liveDataRows); err != nil {
		// Tables may not exist yet on a truly empty DB (no migrate run);
		// that's fine — proceed.
		liveDataRows = 0
	}
	if liveDataRows > 0 {
		t.Fatalf("TEST_DATABASE_URL points at a DB that already contains operational data in `public` " +
			"(pinnacle_market_history or polymarket_price_snapshots has rows). " +
			"Integration tests would destroy live edge views via unqualified DROP IF EXISTS in the migration files. " +
			"Point TEST_DATABASE_URL at a separate empty database — e.g. spin up a throwaway " +
			"`docker run -d --name pol_postgres_verify -p 5434:5432 -e POSTGRES_USER=pol -e POSTGRES_PASSWORD=pol " +
			"-e POSTGRES_DB=pol_odds postgres:16-alpine` and use postgres://pol:pol@localhost:5434/pol_odds.")
	}

	schema := "dbtest_" + randomHex(t, 6)
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+quoteIdent(schema)); err != nil {
		t.Fatalf("create test schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+quoteIdent(schema)+" CASCADE")
	})

	dsn := dsnWithSearchPath(t, baseDSN, schema)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open isolated test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping isolated test database: %v", err)
	}

	var currentSchema string
	if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&currentSchema); err != nil {
		t.Fatalf("query current schema: %v", err)
	}
	if currentSchema != schema {
		t.Fatalf("current schema=%q want %q; TEST_DATABASE_URL search_path isolation failed", currentSchema, schema)
	}

	return ctx, db, dsn
}

func integrationMigrationFiles(t *testing.T) []string {
	t.Helper()
	dir := filepath.Join("..", "..", "migrations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		files = append(files, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(files)
	if len(files) == 0 {
		t.Fatal("no migration files found")
	}
	return files
}

func applyIntegrationMigrations(ctx context.Context, t *testing.T, db *sql.DB, files []string) int {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename text PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        )
    `); err != nil {
		t.Fatalf("ensure schema_migrations: %v", err)
	}

	applied := 0
	for _, file := range files {
		filename := filepath.Base(file)
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read migration %s: %v", filename, err)
		}
		if strings.TrimSpace(string(body)) == "" {
			t.Fatalf("migration %s is empty", filename)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin migration %s: %v", filename, err)
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()

		if _, err := tx.ExecContext(ctx, `LOCK TABLE schema_migrations IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			t.Fatalf("lock schema_migrations for %s: %v", filename, err)
		}

		var exists bool
		if err := tx.QueryRowContext(ctx, `
            SELECT EXISTS (
                SELECT 1
                FROM schema_migrations
                WHERE filename = $1
            )
        `, filename).Scan(&exists); err != nil {
			t.Fatalf("check migration %s: %v", filename, err)
		}
		if exists {
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit skipped migration %s: %v", filename, err)
			}
			committed = true
			continue
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			t.Fatalf("apply migration %s: %v", filename, err)
		}
		if _, err := tx.ExecContext(ctx, `
            INSERT INTO schema_migrations (filename, applied_at)
            VALUES ($1, now())
        `, filename); err != nil {
			t.Fatalf("record migration %s: %v", filename, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit migration %s: %v", filename, err)
		}
		committed = true
		applied++
	}
	return applied
}

func sportsbookBatch(receivedAt, commenceAt time.Time) model.FetchBatch {
	return sportsbookBatchFor("sportsbook-event-1", "Home FC", "Away FC", receivedAt, commenceAt, receivedAt.Add(-1*time.Minute))
}

func sportsbookBatchFor(eventID, homeTeam, awayTeam string, receivedAt, commenceAt, lastUpdate time.Time) model.FetchBatch {
	payload, _ := json.Marshal(map[string]string{"fixture": "sportsbook"})
	return model.FetchBatch{
		Source:     model.SourceOddsAPI,
		SportKey:   "soccer_test",
		RequestURL: "https://example.test/sports/soccer_test/odds?apiKey=REDACTED",
		StatusCode: 200,
		Headers:    map[string]string{"content-type": "application/json"},
		Payload:    payload,
		PayloadSHA: "fixture-sportsbook-" + eventID,
		ReceivedAt: receivedAt,
		Events: []model.EventOdds{{
			ID:           eventID,
			SportKey:     "soccer_test",
			SportTitle:   "Soccer Test League",
			CommenceTime: commenceAt,
			HomeTeam:     homeTeam,
			AwayTeam:     awayTeam,
			Bookmakers: []model.Bookmaker{
				{
					Key:        "book_a",
					Title:      "Book A",
					LastUpdate: lastUpdate,
					Markets: []model.Market{{
						Key: "h2h",
						Outcomes: []model.Outcome{
							{Name: homeTeam, Price: 2.00},
							{Name: awayTeam, Price: 2.00},
						},
					}},
				},
				{
					Key:        "book_b",
					Title:      "Book B",
					LastUpdate: lastUpdate,
					Markets: []model.Market{{
						Key: "h2h",
						Outcomes: []model.Outcome{
							{Name: homeTeam, Price: 1.80},
							{Name: awayTeam, Price: 2.20},
						},
					}},
				},
			},
		}},
	}
}

func assertMoneylineConsensus(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	sumRows, err := db.QueryContext(ctx, `
        SELECT bookmaker_key, sum(no_vig_probability)::float8
        FROM fair_value_moneyline_by_book
        WHERE home_team = 'Home FC'
          AND away_team = 'Away FC'
        GROUP BY bookmaker_key
    `)
	if err != nil {
		t.Fatalf("query per-book no-vig sums: %v", err)
	}
	defer func() { _ = sumRows.Close() }()
	sumCount := 0
	for sumRows.Next() {
		var bookmaker string
		var probabilitySum float64
		if err := sumRows.Scan(&bookmaker, &probabilitySum); err != nil {
			t.Fatalf("scan per-book no-vig sum: %v", err)
		}
		sumCount++
		assertNear(t, probabilitySum, 1, 0.000001, "per-book no-vig sum for "+bookmaker)
	}
	if err := sumRows.Err(); err != nil {
		t.Fatalf("read per-book no-vig sums: %v", err)
	}
	if sumCount != 2 {
		t.Fatalf("per-book no-vig sum rows=%d want 2", sumCount)
	}

	rows, err := db.QueryContext(ctx, `
        SELECT outcome_side, average_no_vig_probability::float8, bookmaker_count
        FROM fair_value_moneyline_consensus
        WHERE home_team = 'Home FC'
          AND away_team = 'Away FC'
        ORDER BY outcome_side
    `)
	if err != nil {
		t.Fatalf("query consensus: %v", err)
	}
	defer func() { _ = rows.Close() }()

	got := map[string]struct {
		prob  float64
		books int
	}{}
	for rows.Next() {
		var side string
		var prob float64
		var books int
		if err := rows.Scan(&side, &prob, &books); err != nil {
			t.Fatalf("scan consensus: %v", err)
		}
		got[side] = struct {
			prob  float64
			books int
		}{prob: prob, books: books}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read consensus rows: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("consensus rows=%d want 2: %#v", len(got), got)
	}
	assertNear(t, got["home"].prob, 0.525, 0.000001, "home no-vig consensus")
	assertNear(t, got["away"].prob, 0.475, 0.000001, "away no-vig consensus")
	if got["home"].books != 2 || got["away"].books != 2 {
		t.Fatalf("bookmaker counts home=%d away=%d want 2/2", got["home"].books, got["away"].books)
	}
}

func upsertPolymarketFixture(ctx context.Context, t *testing.T, s *PostgresStore, now time.Time) {
	t.Helper()
	upsertPolymarketFixtureNamed(ctx, t, s, now, polymarketFixture{
		EventID:      "pm-event-1",
		MarketID:     "pm-market-1",
		ConditionID:  "condition-1",
		Home:         "Home FC",
		Away:         "Away FC",
		HomeTokenID:  "token-home",
		AwayTokenID:  "token-away",
		WithSnapshot: true,
		BestBid:      0.46,
		BestAsk:      0.50,
		Midpoint:     0.48,
		Spread:       0.04,
		Liquidity:    100,
	})
	oldPriceAt := now.Add(-10 * time.Minute)
	if _, err := s.InsertPolymarketPriceSnapshots(ctx, []model.PolymarketPriceSnapshot{
		polymarketSnapshotFor("token-home", "condition-1", "pm-market-1", 0.44, 0.60, 0.52, 0.16, 20, oldPriceAt),
	}); err != nil {
		t.Fatalf("insert old polymarket price snapshot: %v", err)
	}
}

type polymarketFixture struct {
	EventID      string
	MarketID     string
	ConditionID  string
	Home         string
	Away         string
	HomeTokenID  string
	AwayTokenID  string
	CanonicalID  string
	WithSnapshot bool
	BestBid      float64
	BestAsk      float64
	Midpoint     float64
	Spread       float64
	Liquidity    float64
}

func upsertPolymarketFixtureNamed(ctx context.Context, t *testing.T, s *PostgresStore, now time.Time, fixture polymarketFixture) {
	t.Helper()
	active := true
	closed := false
	archived := false
	endTime := now.Add(3 * time.Hour)
	if _, err := s.UpsertPolymarketEvents(ctx, []model.PolymarketEvent{{
		PolymarketEventID: fixture.EventID,
		Slug:              strings.ToLower(strings.ReplaceAll(fixture.Home+"-vs-"+fixture.Away, " ", "-")),
		Title:             fixture.Home + " vs " + fixture.Away,
		Category:          "sports",
		StartTime:         &endTime,
		Active:            &active,
		Closed:            &closed,
		Archived:          &archived,
		GammaPayload:      json.RawMessage(`{"fixture":"event"}`),
	}}); err != nil {
		t.Fatalf("upsert polymarket event: %v", err)
	}
	if _, err := s.UpsertPolymarketMarkets(ctx, []model.PolymarketMarket{{
		PolymarketMarketID: fixture.MarketID,
		PolymarketEventID:  fixture.EventID,
		ConditionID:        fixture.ConditionID,
		Question:           fixture.Home + " vs " + fixture.Away,
		Slug:               strings.ToLower(strings.ReplaceAll(fixture.Home+"-vs-"+fixture.Away+"-moneyline", " ", "-")),
		MarketType:         "sports",
		Active:             &active,
		Closed:             &closed,
		Archived:           &archived,
		EndTime:            &endTime,
		GammaPayload:       json.RawMessage(`{"fixture":"market"}`),
		CLOBPayload:        json.RawMessage(`{"fixture":"market-clob"}`),
	}}); err != nil {
		t.Fatalf("upsert polymarket market: %v", err)
	}
	homeIndex := 0
	awayIndex := 1
	if _, err := s.UpsertPolymarketTokens(ctx, []model.PolymarketToken{
		{
			TokenID:            fixture.HomeTokenID,
			ConditionID:        fixture.ConditionID,
			PolymarketMarketID: fixture.MarketID,
			OutcomeName:        fixture.Home,
			OutcomeIndex:       &homeIndex,
			Side:               "yes",
			CLOBPayload:        json.RawMessage(`{"fixture":"home-token"}`),
		},
		{
			TokenID:            fixture.AwayTokenID,
			ConditionID:        fixture.ConditionID,
			PolymarketMarketID: fixture.MarketID,
			OutcomeName:        fixture.Away,
			OutcomeIndex:       &awayIndex,
			Side:               "yes",
			CLOBPayload:        json.RawMessage(`{"fixture":"away-token"}`),
		},
	}); err != nil {
		t.Fatalf("upsert polymarket tokens: %v", err)
	}

	if fixture.WithSnapshot {
		latestPriceAt := now.Add(-1 * time.Minute)
		if _, err := s.InsertPolymarketPriceSnapshots(ctx, []model.PolymarketPriceSnapshot{
			polymarketSnapshotFor(fixture.HomeTokenID, fixture.ConditionID, fixture.MarketID, fixture.BestBid, fixture.BestAsk, fixture.Midpoint, fixture.Spread, fixture.Liquidity, latestPriceAt),
			polymarketSnapshotFor(fixture.AwayTokenID, fixture.ConditionID, fixture.MarketID, 0.45, 0.49, 0.47, 0.04, 100, latestPriceAt),
		}); err != nil {
			t.Fatalf("insert polymarket price snapshots: %v", err)
		}
	}

	if fixture.CanonicalID != "" {
		confidence := 95.0
		if err := s.ApprovePolymarketMatch(ctx, model.PolymarketMatchDecision{
			PolymarketEventID:  fixture.EventID,
			PolymarketMarketID: fixture.MarketID,
			ConditionID:        fixture.ConditionID,
			CanonicalEventID:   fixture.CanonicalID,
			Reviewer:           "integration-test",
			ReviewReason:       "fixture approval",
			ConfidenceScore:    &confidence,
			MatchReason:        "fixture match",
		}); err != nil {
			t.Fatalf("approve fixture alias %s: %v", fixture.EventID, err)
		}
	}
}

func polymarketSnapshotFor(tokenID, conditionID, marketID string, bestBid, bestAsk, midpoint, spread, liquidity float64, receivedAt time.Time) model.PolymarketPriceSnapshot {
	return model.PolymarketPriceSnapshot{
		TokenID:            tokenID,
		ConditionID:        conditionID,
		PolymarketMarketID: marketID,
		BestBid:            &bestBid,
		BestAsk:            &bestAsk,
		Midpoint:           &midpoint,
		Spread:             &spread,
		Liquidity:          &liquidity,
		DepthSummary:       json.RawMessage(`{"levels":2}`),
		RawPayload:         json.RawMessage(`{"fixture":"price"}`),
		Source:             model.SourcePolymarket,
		ReceivedAt:         receivedAt,
		CLOBPayload:        json.RawMessage(`{"fixture":"price-clob"}`),
	}
}

func insertUnresolvedReview(ctx context.Context, t *testing.T, db *sql.DB, canonicalID string, commenceAt time.Time) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
        INSERT INTO event_match_reviews (
            source,
            source_event_id,
            source_market_id,
            source_condition_id,
            sport_key,
            league,
            event_title,
            raw_home_team,
            raw_away_team,
            raw_start_time,
            match_status,
            confidence_score,
            reason,
            candidate_canonical_event_ids,
            payload
        ) VALUES (
            'polymarket',
            'pm-event-1',
            'pm-market-1',
            'condition-1',
            'soccer_test',
            'Soccer Test League',
            'Home FC vs Away FC',
            'Home FC',
            'Away FC',
            $2,
            'no_match',
            40,
            'fixture requires manual review',
            ARRAY[$1::uuid],
            '{"fixture":"review"}'::jsonb
        )
    `, canonicalID, commenceAt); err != nil {
		t.Fatalf("insert unresolved review: %v", err)
	}
}

func assertMigrationCount(ctx context.Context, t *testing.T, db *sql.DB, want int) {
	t.Helper()
	got := scalarInt(ctx, t, db, `SELECT count(*) FROM schema_migrations`)
	if got != want {
		t.Fatalf("schema_migrations count=%d want %d", got, want)
	}
}

func assertRelationExists(ctx context.Context, t *testing.T, db *sql.DB, name, relkind string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx, `
        SELECT c.relkind::text
        FROM pg_class c
        JOIN pg_namespace n
          ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relname = $1
    `, name).Scan(&got); err != nil {
		t.Fatalf("relation %s not found in test schema: %v", name, err)
	}
	if got != relkind {
		t.Fatalf("relation %s relkind=%q want %q", name, got, relkind)
	}
}

func scalarInt(ctx context.Context, t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var got int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("query scalar int: %v\n%s", err, query)
	}
	return got
}

func scalarString(ctx context.Context, t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("query scalar string: %v\n%s", err, query)
	}
	return got
}

func readinessState(ctx context.Context, t *testing.T, db *sql.DB, tokenID string) string {
	t.Helper()
	return scalarString(ctx, t, db, `
        SELECT readiness_state
        FROM sportsbook_polymarket_moneyline_edge_readiness
        WHERE polymarket_token_id = $1
    `, tokenID)
}

func assertReadinessState(ctx context.Context, t *testing.T, db *sql.DB, tokenID, want string) {
	t.Helper()
	if got := readinessState(ctx, t, db, tokenID); got != want {
		t.Fatalf("readiness state for token %s=%q want %q", tokenID, got, want)
	}
}

func assertNear(t *testing.T, got, want, tolerance float64, label string) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s=%f want %f", label, got, want)
	}
}

func dsnWithSearchPath(t *testing.T, rawDSN, schema string) string {
	t.Helper()
	parsed, err := url.Parse(rawDSN)
	if err == nil && parsed.Scheme != "" && parsed.Host != "" {
		q := parsed.Query()
		q.Set("search_path", schema+",public")
		parsed.RawQuery = q.Encode()
		return parsed.String()
	}
	if strings.Contains(rawDSN, "search_path=") {
		t.Fatalf("TEST_DATABASE_URL must not set search_path; tests create an isolated schema")
	}
	if strings.TrimSpace(rawDSN) == "" {
		t.Fatal("TEST_DATABASE_URL is empty")
	}
	return rawDSN + " search_path=" + schema + ",public"
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("read random bytes: %v", err)
	}
	return hex.EncodeToString(buf)
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
