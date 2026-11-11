package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

func TestDBIntegrationBetfairMappedViewsRequireConfirmedAliases(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	for _, relation := range []string{
		"betfair_event_aliases",
		"betfair_market_aliases",
		"betfair_runner_aliases",
	} {
		assertRelationExists(ctx, t, db, relation, "r")
	}
	for _, relation := range []string{
		"betfair_unmapped_events",
		"betfair_unmapped_markets",
		"betfair_unmapped_runners",
		"betfair_confirmed_event_aliases",
		"betfair_confirmed_market_aliases",
		"betfair_confirmed_runner_aliases",
		"betfair_mapping_conflicts",
		"fair_value_ready_betfair_exchange_quotes",
	} {
		assertRelationExists(ctx, t, db, relation, "v")
	}

	store, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	receivedAt := time.Now().UTC().Truncate(time.Second)
	back := betfairCandidateFixture("mapped-betfair-back", "mapped-hash-back", 2.1, 20)
	back.ReceivedAt = receivedAt
	lay := back
	lay.QuoteKey = "mapped-betfair-lay"
	lay.QuoteHash = "mapped-hash-lay"
	lay.ExchangeSide = "lay"
	lay.PriceDecimal = 2.16
	lay.AvailableSize = 30
	if _, err := store.SaveBetfairQuoteCandidates(ctx, []model.BetfairExchangeQuoteCandidate{back, lay}); err != nil {
		t.Fatalf("save Betfair mapping fixture quotes: %v", err)
	}

	canonicalID := insertBetfairCanonicalEvent(ctx, t, db, "Home FC", "Away FC")
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM fair_value_ready_betfair_exchange_quotes`); got != 0 {
		t.Fatalf("mapped rows without aliases=%d want 0", got)
	}
	assertBetfairDiagnosticCount(ctx, t, db, "betfair_unmapped_events", 1)
	assertBetfairDiagnosticCount(ctx, t, db, "betfair_unmapped_markets", 1)
	assertBetfairDiagnosticCount(ctx, t, db, "betfair_unmapped_runners", 1)

	insertBetfairEventAlias(ctx, t, db, "event-1", "Home FC v Away FC", canonicalID, "confirmed")
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM fair_value_ready_betfair_exchange_quotes`); got != 0 {
		t.Fatalf("mapped rows with event alias only=%d want 0", got)
	}
	assertBetfairDiagnosticCount(ctx, t, db, "betfair_unmapped_events", 0)
	assertBetfairDiagnosticCount(ctx, t, db, "betfair_unmapped_markets", 1)

	insertBetfairMarketAlias(ctx, t, db, "1.234", "event-1", canonicalID, "confirmed", "MATCH_ODDS", "h2h")
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM fair_value_ready_betfair_exchange_quotes`); got != 0 {
		t.Fatalf("mapped rows without confirmed runner alias=%d want 0", got)
	}
	assertBetfairDiagnosticCount(ctx, t, db, "betfair_unmapped_markets", 0)
	assertBetfairDiagnosticCount(ctx, t, db, "betfair_unmapped_runners", 1)

	insertBetfairRunnerAlias(ctx, t, db, "1.234", 101, 0, "Home FC", "", "", "rejected")
	insertBetfairRunnerAlias(ctx, t, db, "1.234", 101, 0, "Home FC", "", "", "ambiguous")
	insertBetfairRunnerAlias(ctx, t, db, "1.234", 101, 0, "Home FC", "", "", "unresolved")
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM fair_value_ready_betfair_exchange_quotes`); got != 0 {
		t.Fatalf("mapped rows with non-confirmed runner aliases=%d want 0", got)
	}
	assertBetfairDiagnosticCount(ctx, t, db, "betfair_unmapped_runners", 1)

	insertBetfairRunnerAlias(ctx, t, db, "1.234", 101, 0, "Home FC", "Home FC", "home", "confirmed")
	var rows, backRows, layRows int
	var hasLTP, hasTradedVolume, ltpDiffersFromExecutable bool
	if err := db.QueryRowContext(ctx, `
        SELECT
            count(*),
            count(*) FILTER (WHERE exchange_side = 'back'),
            count(*) FILTER (WHERE exchange_side = 'lay'),
            bool_or(last_traded_price IS NOT NULL),
            bool_or(traded_volume IS NOT NULL),
            bool_or(price_decimal <> last_traded_price)
        FROM fair_value_ready_betfair_exchange_quotes
    `).Scan(&rows, &backRows, &layRows, &hasLTP, &hasTradedVolume, &ltpDiffersFromExecutable); err != nil {
		t.Fatalf("query mapped Betfair rows: %v", err)
	}
	if rows != 2 || backRows != 1 || layRows != 1 {
		t.Fatalf("mapped back/lay rows total=%d back=%d lay=%d want 2/1/1", rows, backRows, layRows)
	}
	if !hasLTP || !hasTradedVolume || !ltpDiffersFromExecutable {
		t.Fatalf("mapped metadata ltp=%t traded_volume=%t ltp_differs=%t want all true", hasLTP, hasTradedVolume, ltpDiffersFromExecutable)
	}
	assertBetfairDiagnosticCount(ctx, t, db, "betfair_unmapped_runners", 0)
}

func TestDBIntegrationBetfairMappingConflictView(t *testing.T) {
	ctx, db, _ := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	firstCanonicalID := insertBetfairCanonicalEvent(ctx, t, db, "Conflict Home", "Conflict Away")
	secondCanonicalID := insertBetfairCanonicalEvent(ctx, t, db, "Other Home", "Other Away")

	insertBetfairEventAlias(ctx, t, db, "conflict-event", "Conflict Home v Conflict Away", firstCanonicalID, "confirmed")
	insertBetfairMarketAlias(ctx, t, db, "conflict-market", "conflict-event", secondCanonicalID, "confirmed", "MATCH_ODDS", "h2h")

	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM betfair_mapping_conflicts
        WHERE issue_type = 'confirmed_betfair_event_market_canonical_mismatch'
    `); got != 1 {
		t.Fatalf("event/market mismatch conflicts=%d want 1", got)
	}
}

func insertBetfairCanonicalEvent(ctx context.Context, t *testing.T, db *sql.DB, home, away string) string {
	t.Helper()
	var id string
	if err := db.QueryRowContext(ctx, `
        INSERT INTO canonical_events (
            sport_key,
            league,
            home_team,
            away_team,
            home_team_norm,
            away_team_norm,
            commence_time
        ) VALUES (
            'soccer_test',
            'Soccer Test League',
            $1,
            $2,
            lower($1),
            lower($2),
            '2026-05-07T12:00:00Z'
        )
        RETURNING id::text
    `, home, away).Scan(&id); err != nil {
		t.Fatalf("insert canonical event: %v", err)
	}
	return id
}

func insertBetfairEventAlias(ctx context.Context, t *testing.T, db *sql.DB, eventID, eventName, canonicalID, status string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
        INSERT INTO betfair_event_aliases (
            betfair_event_id,
            betfair_event_name,
            canonical_event_id,
            alias_status,
            confidence_score,
            match_reason,
            reviewer,
            review_reason,
            decided_at
        ) VALUES (
            $1,
            $2,
            NULLIF($3, '')::uuid,
            $4,
            95,
            'fixture mapping',
            'integration-test',
            'fixture review',
            now()
        )
    `, eventID, eventName, canonicalID, status); err != nil {
		t.Fatalf("insert Betfair event alias status=%s: %v", status, err)
	}
}

func insertBetfairMarketAlias(ctx context.Context, t *testing.T, db *sql.DB, marketID, eventID, canonicalID, status, marketType, family string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
        INSERT INTO betfair_market_aliases (
            betfair_market_id,
            betfair_event_id,
            canonical_event_id,
            betfair_market_type,
            canonical_market_family,
            alias_status,
            confidence_score,
            match_reason,
            reviewer,
            review_reason,
            decided_at
        ) VALUES (
            $1,
            $2,
            NULLIF($3, '')::uuid,
            $4,
            $5,
            $6,
            92,
            'fixture market mapping',
            'integration-test',
            'fixture review',
            now()
        )
    `, marketID, eventID, canonicalID, marketType, family, status); err != nil {
		t.Fatalf("insert Betfair market alias status=%s: %v", status, err)
	}
}

func insertBetfairRunnerAlias(ctx context.Context, t *testing.T, db *sql.DB, marketID string, selectionID int64, handicap float64, runnerName, outcomeName, outcomeSide, status string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
        INSERT INTO betfair_runner_aliases (
            betfair_market_id,
            selection_id,
            handicap,
            runner_name,
            canonical_outcome_name,
            canonical_outcome_side,
            alias_status,
            confidence_score,
            match_reason,
            reviewer,
            review_reason,
            decided_at
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            90,
            'fixture runner mapping',
            'integration-test',
            'fixture review',
            now()
        )
    `, marketID, selectionID, handicap, runnerName, outcomeName, outcomeSide, status); err != nil {
		t.Fatalf("insert Betfair runner alias status=%s: %v", status, err)
	}
}

func assertBetfairDiagnosticCount(ctx context.Context, t *testing.T, db *sql.DB, view string, want int) {
	t.Helper()
	got := scalarInt(ctx, t, db, "SELECT count(*) FROM "+quoteIdent(view))
	if got != want {
		t.Fatalf("%s count=%d want %d", view, got, want)
	}
}
