package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

func TestDBIntegrationBetfairSportsbookComparisonViews(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	for _, relation := range []string{
		"sportsbook_betfair_moneyline_comparison",
		"sportsbook_betfair_moneyline_ready_comparison",
		"data_quality_betfair_sportsbook_comparison_anomalies",
		"sportsbook_betfair_comparison_coverage_audit",
	} {
		assertRelationExists(ctx, t, db, relation, "v")
	}

	st, err := Open(ctx, dsn, true, normalize.FreshnessPolicy{
		FreshAfter: 2 * time.Minute,
		AgingAfter: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	noSportsbookCanonicalID := insertBetfairCanonicalEvent(ctx, t, db, "No Sportsbook Home", "No Sportsbook Away")
	saveBetfairComparisonBook(ctx, t, st, betfairComparisonScenario{
		Name:                  "no-sportsbook",
		CanonicalID:           noSportsbookCanonicalID,
		CanonicalOutcomeName:  "No Sportsbook Home",
		CanonicalOutcomeSide:  "home",
		CanonicalMarketFamily: "h2h",
		BackPrice:             2.00,
		LayPrice:              2.04,
	})
	if got := betfairComparisonRowsForMarket(ctx, t, db, "comparison-market-no-sportsbook", false); got != 0 {
		t.Fatalf("comparison rows without sportsbook fair value=%d want 0", got)
	}
	assertBetfairComparisonAnomalyForMarket(ctx, t, db, "comparison-market-no-sportsbook", "missing_sportsbook_consensus")
	assertBetfairCoverageReason(ctx, t, db, noSportsbookCanonicalID, "comparison-market-no-sportsbook", "no_sportsbook_consensus")

	noBetfairCanonicalID := saveSportsbookComparisonFixture(ctx, t, st, db, "sportsbook-comparison-no-betfair", "No Betfair Home", "No Betfair Away")
	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM sportsbook_betfair_moneyline_comparison
        WHERE canonical_event_id = $1::uuid
    `, noBetfairCanonicalID); got != 0 {
		t.Fatalf("comparison rows without Betfair readiness=%d want 0", got)
	}
	assertBetfairCoverageReason(ctx, t, db, noBetfairCanonicalID, "", "mapping_missing")

	mismatchCanonicalID := saveSportsbookComparisonFixture(ctx, t, st, db, "sportsbook-comparison-mismatch", "Mismatch Home", "Mismatch Away")
	saveBetfairComparisonBook(ctx, t, st, betfairComparisonScenario{
		Name:                  "mismatch",
		CanonicalID:           mismatchCanonicalID,
		CanonicalOutcomeName:  "Different Home",
		CanonicalOutcomeSide:  "home",
		CanonicalMarketFamily: "h2h",
		BackPrice:             2.00,
		LayPrice:              2.04,
	})
	if got := betfairComparisonRowsForMarket(ctx, t, db, "comparison-market-mismatch", false); got != 0 {
		t.Fatalf("comparison rows with mismatched outcome name=%d want 0", got)
	}
	assertBetfairCoverageReason(ctx, t, db, mismatchCanonicalID, "comparison-market-mismatch", "no_matching_outcome")

	readyCanonicalID := saveSportsbookComparisonFixture(ctx, t, st, db, "sportsbook-comparison-ready", "Compare Home", "Compare Away")
	saveBetfairComparisonBook(ctx, t, st, betfairComparisonScenario{
		Name:                  "ready",
		CanonicalID:           readyCanonicalID,
		CanonicalOutcomeName:  "Compare Home",
		CanonicalOutcomeSide:  "home",
		CanonicalMarketFamily: "h2h",
		BackPrice:             2.00,
		LayPrice:              2.04,
		BackSize:              25,
		LaySize:               35,
	})
	if got := betfairComparisonRowsForMarket(ctx, t, db, "comparison-market-ready", false); got != 1 {
		t.Fatalf("comparison rows for ready fixture=%d want 1", got)
	}
	if got := betfairComparisonRowsForMarket(ctx, t, db, "comparison-market-ready", true); got != 1 {
		t.Fatalf("ready comparison rows for ready fixture=%d want 1", got)
	}
	assertBetfairCoverageReason(ctx, t, db, readyCanonicalID, "comparison-market-ready", "comparison_ready")
	assertBetfairComparisonCalculations(ctx, t, db)

	wideCanonicalID := saveSportsbookComparisonFixture(ctx, t, st, db, "sportsbook-comparison-wide", "Wide Compare Home", "Wide Compare Away")
	saveBetfairComparisonBook(ctx, t, st, betfairComparisonScenario{
		Name:                  "wide",
		CanonicalID:           wideCanonicalID,
		CanonicalOutcomeName:  "Wide Compare Home",
		CanonicalOutcomeSide:  "home",
		CanonicalMarketFamily: "h2h",
		BackPrice:             2.00,
		LayPrice:              2.30,
	})
	if got := betfairComparisonRowsForMarket(ctx, t, db, "comparison-market-wide", false); got != 1 {
		t.Fatalf("comparison rows for non-ready fixture=%d want 1", got)
	}
	if got := betfairComparisonRowsForMarket(ctx, t, db, "comparison-market-wide", true); got != 0 {
		t.Fatalf("ready comparison rows for non-ready fixture=%d want 0", got)
	}
	state := scalarString(ctx, t, db, `
        SELECT betfair_readiness_state
        FROM sportsbook_betfair_moneyline_comparison
        WHERE betfair_market_id = 'comparison-market-wide'
    `)
	if state != "wide_spread" {
		t.Fatalf("non-ready comparison state=%q want wide_spread", state)
	}
	assertBetfairComparisonAnomalyForMarket(ctx, t, db, "comparison-market-wide", "betfair_not_ready")
	assertBetfairCoverageReason(ctx, t, db, wideCanonicalID, "comparison-market-wide", "stale_or_low_confidence")

	nonH2HCanonicalID := saveSportsbookComparisonFixture(ctx, t, st, db, "sportsbook-comparison-nonh2h", "Totals Home", "Totals Away")
	saveBetfairComparisonBook(ctx, t, st, betfairComparisonScenario{
		Name:                  "nonh2h",
		CanonicalID:           nonH2HCanonicalID,
		CanonicalOutcomeName:  "Totals Home",
		CanonicalOutcomeSide:  "home",
		CanonicalMarketFamily: "totals",
		BackPrice:             2.00,
		LayPrice:              2.04,
	})
	if got := betfairComparisonRowsForMarket(ctx, t, db, "comparison-market-nonh2h", false); got != 0 {
		t.Fatalf("comparison rows for non-h2h Betfair fixture=%d want 0", got)
	}
	assertBetfairCoverageReason(ctx, t, db, nonH2HCanonicalID, "comparison-market-nonh2h", "unsupported_market_family")
}

type betfairComparisonScenario struct {
	Name                  string
	CanonicalID           string
	CanonicalOutcomeName  string
	CanonicalOutcomeSide  string
	CanonicalMarketFamily string
	BackPrice             float64
	BackSize              float64
	LayPrice              float64
	LaySize               float64
}

func saveSportsbookComparisonFixture(ctx context.Context, t *testing.T, st *PostgresStore, db *sql.DB, eventID, home, away string) string {
	t.Helper()
	receivedAt := time.UnixMilli(1715100000000).UTC()
	commenceAt := receivedAt.Add(2 * time.Hour)
	if _, err := st.SaveBatch(ctx, sportsbookBatchFor(eventID, home, away, receivedAt, commenceAt, receivedAt.Add(-1*time.Minute))); err != nil {
		t.Fatalf("save sportsbook comparison fixture %s: %v", eventID, err)
	}
	return scalarString(ctx, t, db, `
        SELECT canonical_event_id::text
        FROM fair_value_moneyline_consensus
        WHERE outcome_name = $1
          AND outcome_side = 'home'
    `, home)
}

func saveBetfairComparisonBook(ctx context.Context, t *testing.T, st *PostgresStore, scenario betfairComparisonScenario) {
	t.Helper()
	if scenario.BackPrice == 0 {
		scenario.BackPrice = 2.00
	}
	if scenario.BackSize == 0 {
		scenario.BackSize = 25
	}
	if scenario.LayPrice == 0 {
		scenario.LayPrice = 2.04
	}
	if scenario.LaySize == 0 {
		scenario.LaySize = 35
	}
	if scenario.CanonicalMarketFamily == "" {
		scenario.CanonicalMarketFamily = "h2h"
	}

	eventID := "comparison-event-" + scenario.Name
	marketID := "comparison-market-" + scenario.Name
	runnerName := "Runner " + scenario.Name
	receivedAt := time.UnixMilli(1715100000000).UTC().Add(time.Duration(len(scenario.Name)) * time.Second)

	back := betfairComparisonCandidate(scenario, eventID, marketID, runnerName, "back", scenario.BackPrice, scenario.BackSize, receivedAt)
	lay := betfairComparisonCandidate(scenario, eventID, marketID, runnerName, "lay", scenario.LayPrice, scenario.LaySize, receivedAt)
	if _, err := st.SaveBetfairQuoteCandidates(ctx, []model.BetfairExchangeQuoteCandidate{back, lay}); err != nil {
		t.Fatalf("save Betfair comparison fixture %s: %v", scenario.Name, err)
	}

	insertBetfairEventAlias(ctx, t, st.db, eventID, "Comparison fixture "+scenario.Name, scenario.CanonicalID, "confirmed")
	insertBetfairMarketAlias(ctx, t, st.db, marketID, eventID, scenario.CanonicalID, "confirmed", "MATCH_ODDS", scenario.CanonicalMarketFamily)
	insertBetfairRunnerAlias(ctx, t, st.db, marketID, 101, 0, runnerName, scenario.CanonicalOutcomeName, scenario.CanonicalOutcomeSide, "confirmed")
}

func betfairComparisonCandidate(scenario betfairComparisonScenario, eventID, marketID, runnerName, side string, price, size float64, receivedAt time.Time) model.BetfairExchangeQuoteCandidate {
	candidate := betfairCandidateFixture(
		fmt.Sprintf("comparison-%s-%s", scenario.Name, side),
		fmt.Sprintf("comparison-hash-%s-%s", scenario.Name, side),
		price,
		size,
	)
	candidate.EventID = eventID
	candidate.EventName = "Comparison fixture " + scenario.Name
	candidate.MarketID = marketID
	candidate.MarketName = "Match Odds"
	candidate.MarketFamily = "match_odds"
	candidate.SelectionID = 101
	candidate.Handicap = 0
	candidate.RunnerName = runnerName
	candidate.ExchangeSide = side
	candidate.PriceDecimal = price
	candidate.AvailableSize = size
	candidate.MarketStatus = "OPEN"
	candidate.InPlay = boolPtr(false)
	candidate.StaleState = "fresh"
	candidate.ConfidenceScore = 80
	candidate.ReceivedAt = receivedAt
	return candidate
}

func betfairComparisonRowsForMarket(ctx context.Context, t *testing.T, db *sql.DB, marketID string, readyOnly bool) int {
	t.Helper()
	view := "sportsbook_betfair_moneyline_comparison"
	if readyOnly {
		view = "sportsbook_betfair_moneyline_ready_comparison"
	}
	return scalarInt(ctx, t, db, "SELECT count(*) FROM "+quoteIdent(view)+" WHERE betfair_market_id = $1", marketID)
}

func assertBetfairComparisonCalculations(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	var (
		sportsbookProbability float64
		sportsbookFairDecimal float64
		backPrice             float64
		backSize              float64
		layPrice              float64
		laySize               float64
		ltp                   float64
		tradedVolume          float64
		backImplied           float64
		layImplied            float64
		backGap               float64
		layGap                float64
		backPriceGap          float64
		layPriceGap           float64
	)
	if err := db.QueryRowContext(ctx, `
        SELECT
            sportsbook_avg_no_vig_probability::float8,
            sportsbook_fair_decimal::float8,
            best_back_price::float8,
            best_back_size::float8,
            best_lay_price::float8,
            best_lay_size::float8,
            last_traded_price::float8,
            traded_volume::float8,
            betfair_back_implied_probability::float8,
            betfair_lay_implied_probability::float8,
            back_probability_gap::float8,
            lay_probability_gap::float8,
            back_price_gap_decimal::float8,
            lay_price_gap_decimal::float8
        FROM sportsbook_betfair_moneyline_comparison
        WHERE betfair_market_id = 'comparison-market-ready'
    `).Scan(
		&sportsbookProbability,
		&sportsbookFairDecimal,
		&backPrice,
		&backSize,
		&layPrice,
		&laySize,
		&ltp,
		&tradedVolume,
		&backImplied,
		&layImplied,
		&backGap,
		&layGap,
		&backPriceGap,
		&layPriceGap,
	); err != nil {
		t.Fatalf("query comparison calculations: %v", err)
	}
	assertNear(t, sportsbookProbability, 0.525, 0.000001, "sportsbook no-vig probability")
	assertNear(t, sportsbookFairDecimal, 1.0/0.525, 0.000001, "sportsbook fair decimal")
	assertNear(t, backPrice, 2.00, 0.000001, "best back price")
	assertNear(t, backSize, 25, 0.000001, "best back size")
	assertNear(t, layPrice, 2.04, 0.000001, "best lay price")
	assertNear(t, laySize, 35, 0.000001, "best lay size")
	assertNear(t, ltp, 2.05, 0.000001, "last traded price metadata")
	assertNear(t, tradedVolume, 100, 0.000001, "traded volume metadata")
	assertNear(t, backImplied, 1.0/2.00, 0.000001, "back implied probability")
	assertNear(t, layImplied, 1.0/2.04, 0.000001, "lay implied probability")
	assertNear(t, backGap, 0.525-(1.0/2.00), 0.000001, "back probability gap")
	assertNear(t, layGap, (1.0/2.04)-0.525, 0.000001, "lay probability gap")
	assertNear(t, backPriceGap, 2.00-(1.0/0.525), 0.000001, "back decimal gap")
	assertNear(t, layPriceGap, 2.04-(1.0/0.525), 0.000001, "lay decimal gap")
}

func assertBetfairComparisonAnomalyForMarket(ctx context.Context, t *testing.T, db *sql.DB, marketID, anomalyType string) {
	t.Helper()
	got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM data_quality_betfair_sportsbook_comparison_anomalies
        WHERE betfair_market_id = $1
          AND anomaly_type = $2
    `, marketID, anomalyType)
	if got == 0 {
		t.Fatalf("comparison anomaly %s for market %s not found", anomalyType, marketID)
	}
}

func assertBetfairCoverageReason(ctx context.Context, t *testing.T, db *sql.DB, canonicalID, marketID, want string) {
	t.Helper()
	query := `
        SELECT gap_reason
        FROM sportsbook_betfair_comparison_coverage_audit
        WHERE canonical_event_id = $1::uuid
          AND (
              ($2 = '' AND betfair_market_id IS NULL)
              OR betfair_market_id = $2
          )
    `
	got := scalarString(ctx, t, db, query, canonicalID, marketID)
	if got != want {
		t.Fatalf("coverage reason canonical=%s market=%s got %q want %q", canonicalID, marketID, got, want)
	}
}
