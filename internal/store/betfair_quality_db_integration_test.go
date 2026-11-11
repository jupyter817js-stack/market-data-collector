package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

func TestDBIntegrationBetfairQualityReadinessViews(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	for _, relation := range []string{
		"betfair_mapped_runner_books",
		"data_quality_betfair_exchange_anomalies",
		"betfair_exchange_quote_readiness",
	} {
		assertRelationExists(ctx, t, db, relation, "v")
	}

	st, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{
		Name:        "unmapped",
		BackPrice:   2.00,
		BackSize:    20,
		LayPrice:    2.04,
		LaySize:     30,
		SkipAliases: true,
	})
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_readiness WHERE readiness_state = 'ready'`); got != 0 {
		t.Fatalf("ready rows without aliases=%d want 0", got)
	}
	assertBetfairAnomalyAtLeast(ctx, t, db, "missing_event_mapping", 1)
	assertBetfairAnomalyAtLeast(ctx, t, db, "missing_market_mapping", 1)
	assertBetfairAnomalyAtLeast(ctx, t, db, "missing_runner_mapping", 1)

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "ready", BackPrice: 2.00, BackSize: 20, LayPrice: 2.04, LaySize: 30})
	assertBetfairReadinessState(ctx, t, db, "market-ready", "ready")
	var backPrice, layPrice, ltp, tradedVolume float64
	var hasBack, hasLay bool
	if err := db.QueryRowContext(ctx, `
        SELECT
            best_back_price::float8,
            best_lay_price::float8,
            last_traded_price::float8,
            traded_volume::float8,
            has_back_side,
            has_lay_side
        FROM betfair_mapped_runner_books
        WHERE betfair_market_id = 'market-ready'
    `).Scan(&backPrice, &layPrice, &ltp, &tradedVolume, &hasBack, &hasLay); err != nil {
		t.Fatalf("query ready runner book: %v", err)
	}
	assertNear(t, backPrice, 2.00, 0.000001, "best back price")
	assertNear(t, layPrice, 2.04, 0.000001, "best lay price")
	assertNear(t, ltp, 2.05, 0.000001, "last traded price metadata")
	assertNear(t, tradedVolume, 100, 0.000001, "traded volume metadata")
	if !hasBack || !hasLay {
		t.Fatalf("book sides hasBack=%t hasLay=%t want true/true", hasBack, hasLay)
	}

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "missing-back", HasBack: boolPtr(false), BackPrice: 2.00, BackSize: 20, LayPrice: 2.04, LaySize: 30})
	assertBetfairReadinessState(ctx, t, db, "market-missing-back", "missing_side")
	assertBetfairAnomalyForMarket(ctx, t, db, "market-missing-back", "missing_back_side")

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "missing-lay", HasLay: boolPtr(false), BackPrice: 2.00, BackSize: 20, LayPrice: 2.04, LaySize: 30})
	assertBetfairReadinessState(ctx, t, db, "market-missing-lay", "missing_side")
	assertBetfairAnomalyForMarket(ctx, t, db, "market-missing-lay", "missing_lay_side")

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "stale", BackPrice: 2.00, BackSize: 20, LayPrice: 2.04, LaySize: 30, StaleState: "stale"})
	assertBetfairReadinessState(ctx, t, db, "market-stale", "stale")
	assertBetfairAnomalyForMarket(ctx, t, db, "market-stale", "stale_quote")

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "aging", BackPrice: 2.00, BackSize: 20, LayPrice: 2.04, LaySize: 30, StaleState: "aging"})
	assertBetfairReadinessState(ctx, t, db, "market-aging", "aging")
	assertBetfairAnomalyForMarket(ctx, t, db, "market-aging", "aging_quote")

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "suspended", BackPrice: 2.00, BackSize: 20, LayPrice: 2.04, LaySize: 30, MarketStatus: "SUSPENDED"})
	assertBetfairReadinessState(ctx, t, db, "market-suspended", "suspended")
	assertBetfairAnomalyForMarket(ctx, t, db, "market-suspended", "suspended_or_closed_market")

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "inplay", BackPrice: 2.00, BackSize: 20, LayPrice: 2.04, LaySize: 30, InPlay: boolPtr(true)})
	assertBetfairReadinessState(ctx, t, db, "market-inplay", "in_play")
	assertBetfairAnomalyForMarket(ctx, t, db, "market-inplay", "in_play_market")

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "wide", BackPrice: 2.00, BackSize: 20, LayPrice: 2.20, LaySize: 30})
	assertBetfairReadinessState(ctx, t, db, "market-wide", "wide_spread")
	assertBetfairAnomalyForMarket(ctx, t, db, "market-wide", "wide_spread")

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "thin", BackPrice: 2.00, BackSize: 5, LayPrice: 2.04, LaySize: 8})
	assertBetfairReadinessState(ctx, t, db, "market-thin", "thin_liquidity")
	assertBetfairAnomalyForMarket(ctx, t, db, "market-thin", "thin_liquidity")

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "low-confidence", BackPrice: 2.00, BackSize: 20, LayPrice: 2.04, LaySize: 30, ConfidenceScore: floatPtr(0.5)})
	assertBetfairReadinessState(ctx, t, db, "market-low-confidence", "low_confidence")
	assertBetfairAnomalyForMarket(ctx, t, db, "market-low-confidence", "low_confidence")

	saveBetfairQualityScenario(ctx, t, db, st, betfairQualityScenario{Name: "negative", BackPrice: 2.10, BackSize: 20, LayPrice: 2.00, LaySize: 30})
	assertBetfairReadinessState(ctx, t, db, "market-negative", "needs_review")
	assertBetfairAnomalyForMarket(ctx, t, db, "market-negative", "negative_spread")
}

func TestDBIntegrationBetfairQualityMappingConflictAnomaly(t *testing.T) {
	ctx, db, _ := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	firstCanonicalID := insertBetfairCanonicalEvent(ctx, t, db, "Quality Conflict Home", "Quality Conflict Away")
	secondCanonicalID := insertBetfairCanonicalEvent(ctx, t, db, "Quality Other Home", "Quality Other Away")

	insertBetfairEventAlias(ctx, t, db, "quality-conflict-event", "Quality Conflict Home v Quality Conflict Away", firstCanonicalID, "confirmed")
	insertBetfairMarketAlias(ctx, t, db, "quality-conflict-market", "quality-conflict-event", secondCanonicalID, "confirmed", "MATCH_ODDS", "h2h")

	if got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM data_quality_betfair_exchange_anomalies
        WHERE anomaly_type = 'mapping_conflict'
          AND betfair_market_id = 'quality-conflict-market'
    `); got == 0 {
		t.Fatalf("mapping conflict anomalies=%d want at least 1", got)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_readiness WHERE readiness_state = 'ready'`); got != 0 {
		t.Fatalf("ready rows with only conflict fixture=%d want 0", got)
	}
}

type betfairQualityScenario struct {
	Name            string
	BackPrice       float64
	BackSize        float64
	LayPrice        float64
	LaySize         float64
	HasBack         *bool
	HasLay          *bool
	StaleState      string
	MarketStatus    string
	InPlay          *bool
	ConfidenceScore *float64
	SkipAliases     bool
}

func saveBetfairQualityScenario(ctx context.Context, t *testing.T, db *sql.DB, st *PostgresStore, scenario betfairQualityScenario) {
	t.Helper()
	if scenario.BackPrice == 0 {
		scenario.BackPrice = 2.00
	}
	if scenario.BackSize == 0 {
		scenario.BackSize = 20
	}
	if scenario.LayPrice == 0 {
		scenario.LayPrice = 2.04
	}
	if scenario.LaySize == 0 {
		scenario.LaySize = 30
	}
	if scenario.HasBack == nil {
		scenario.HasBack = boolPtr(true)
	}
	if scenario.HasLay == nil {
		scenario.HasLay = boolPtr(true)
	}
	if scenario.StaleState == "" {
		scenario.StaleState = "fresh"
	}
	if scenario.MarketStatus == "" {
		scenario.MarketStatus = "OPEN"
	}
	if scenario.InPlay == nil {
		scenario.InPlay = boolPtr(false)
	}
	if scenario.ConfidenceScore == nil {
		scenario.ConfidenceScore = floatPtr(80)
	}

	eventID := "event-" + scenario.Name
	marketID := "market-" + scenario.Name
	runnerName := "Home " + scenario.Name
	receivedAt := time.UnixMilli(1715000000000).UTC().Add(time.Duration(len(scenario.Name)) * time.Second)
	candidates := []model.BetfairExchangeQuoteCandidate{}
	if *scenario.HasBack {
		back := betfairQualityCandidate(scenario, eventID, marketID, runnerName, "back", scenario.BackPrice, scenario.BackSize, receivedAt)
		candidates = append(candidates, back)
	}
	if *scenario.HasLay {
		lay := betfairQualityCandidate(scenario, eventID, marketID, runnerName, "lay", scenario.LayPrice, scenario.LaySize, receivedAt)
		candidates = append(candidates, lay)
	}
	if _, err := st.SaveBetfairQuoteCandidates(ctx, candidates); err != nil {
		t.Fatalf("save Betfair quality scenario %s: %v", scenario.Name, err)
	}

	if scenario.SkipAliases {
		return
	}
	canonicalID := insertBetfairCanonicalEvent(ctx, t, db, "Quality Home "+scenario.Name, "Quality Away "+scenario.Name)
	insertBetfairEventAlias(ctx, t, db, eventID, "Quality Home "+scenario.Name+" v Quality Away "+scenario.Name, canonicalID, "confirmed")
	insertBetfairMarketAlias(ctx, t, db, marketID, eventID, canonicalID, "confirmed", "MATCH_ODDS", "h2h")
	insertBetfairRunnerAlias(ctx, t, db, marketID, 101, 0, runnerName, runnerName, "home", "confirmed")
}

func betfairQualityCandidate(scenario betfairQualityScenario, eventID, marketID, runnerName, side string, price, size float64, receivedAt time.Time) model.BetfairExchangeQuoteCandidate {
	candidate := betfairCandidateFixture(
		fmt.Sprintf("quality-%s-%s", scenario.Name, side),
		fmt.Sprintf("quality-hash-%s-%s", scenario.Name, side),
		price,
		size,
	)
	candidate.EventID = eventID
	candidate.EventName = "Quality fixture " + scenario.Name
	candidate.MarketID = marketID
	candidate.MarketName = "Match Odds"
	candidate.MarketFamily = "match_odds"
	candidate.SelectionID = 101
	candidate.Handicap = 0
	candidate.RunnerName = runnerName
	candidate.ExchangeSide = side
	candidate.PriceDecimal = price
	candidate.AvailableSize = size
	candidate.StaleState = scenario.StaleState
	candidate.MarketStatus = scenario.MarketStatus
	candidate.InPlay = scenario.InPlay
	candidate.ConfidenceScore = *scenario.ConfidenceScore
	candidate.ReceivedAt = receivedAt
	return candidate
}

func assertBetfairReadinessState(ctx context.Context, t *testing.T, db *sql.DB, marketID, want string) {
	t.Helper()
	got := scalarString(ctx, t, db, `
        SELECT readiness_state
        FROM betfair_exchange_quote_readiness
        WHERE betfair_market_id = $1
    `, marketID)
	if got != want {
		t.Fatalf("readiness state for %s=%q want %q", marketID, got, want)
	}
	if want != "ready" {
		if ready := scalarInt(ctx, t, db, `
            SELECT count(*)
            FROM betfair_exchange_quote_readiness
            WHERE betfair_market_id = $1
              AND readiness_state = 'ready'
        `, marketID); ready != 0 {
			t.Fatalf("market %s has ready rows=%d want 0", marketID, ready)
		}
	}
}

func assertBetfairAnomalyForMarket(ctx context.Context, t *testing.T, db *sql.DB, marketID, anomalyType string) {
	t.Helper()
	got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM data_quality_betfair_exchange_anomalies
        WHERE betfair_market_id = $1
          AND anomaly_type = $2
    `, marketID, anomalyType)
	if got == 0 {
		t.Fatalf("anomaly %s for market %s not found", anomalyType, marketID)
	}
}

func assertBetfairAnomalyAtLeast(ctx context.Context, t *testing.T, db *sql.DB, anomalyType string, wantMin int) {
	t.Helper()
	got := scalarInt(ctx, t, db, `
        SELECT count(*)
        FROM data_quality_betfair_exchange_anomalies
        WHERE anomaly_type = $1
    `, anomalyType)
	if got < wantMin {
		t.Fatalf("anomaly %s count=%d want at least %d", anomalyType, got, wantMin)
	}
}

func boolPtr(v bool) *bool {
	return &v
}

func TestBetfairQualityReadinessNoTradingTerms(t *testing.T) {
	sql := readMigration(t, "014_betfair_quality_readiness_views.sql")
	for _, term := range []string{"buy_signal", "sell_signal", "order_placement", "wallet", "private_key", "trade_signal"} {
		if strings.Contains(strings.ToLower(sql), term) {
			t.Fatalf("quality/readiness migration contains prohibited term %q:\n%s", term, sql)
		}
	}
}
