package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

func TestDBIntegrationBetfairMapReviewWorkflow(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	st, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	receivedAt := time.Now().UTC().Truncate(time.Second)
	back := betfairCandidateFixture("review-betfair-back", "review-hash-back", 2.1, 20)
	back.ReceivedAt = receivedAt
	lay := back
	lay.QuoteKey = "review-betfair-lay"
	lay.QuoteHash = "review-hash-lay"
	lay.ExchangeSide = "lay"
	lay.PriceDecimal = 2.16
	lay.AvailableSize = 30
	if _, err := st.SaveBetfairQuoteCandidates(ctx, []model.BetfairExchangeQuoteCandidate{back, lay}); err != nil {
		t.Fatalf("save Betfair review fixture quotes: %v", err)
	}

	if events, err := st.ListBetfairUnmappedEvents(ctx, model.BetfairMapListOptions{Limit: 10}); err != nil {
		t.Fatalf("list unmapped events: %v", err)
	} else if len(events) != 1 || events[0].BetfairEventID != "event-1" {
		t.Fatalf("unmapped events=%+v", events)
	}
	if markets, err := st.ListBetfairUnmappedMarkets(ctx, model.BetfairMapListOptions{Limit: 10}); err != nil {
		t.Fatalf("list unmapped markets: %v", err)
	} else if len(markets) != 1 || markets[0].BetfairMarketID != "1.234" {
		t.Fatalf("unmapped markets=%+v", markets)
	}
	if runners, err := st.ListBetfairUnmappedRunners(ctx, model.BetfairMapListOptions{Limit: 10}); err != nil {
		t.Fatalf("list unmapped runners: %v", err)
	} else if len(runners) != 1 || runners[0].SelectionID != 101 {
		t.Fatalf("unmapped runners=%+v", runners)
	}

	canonicalID := insertBetfairCanonicalEvent(ctx, t, db, "Home FC", "Away FC")
	rejectBetfairFixtures(ctx, t, st)
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM fair_value_ready_betfair_exchange_quotes`); got != 0 {
		t.Fatalf("mapped rows after unrelated rejections=%d want 0", got)
	}

	if err := st.ApproveBetfairEventAlias(ctx, model.BetfairEventAliasDecision{
		BetfairEventID:   "event-1",
		BetfairEventName: "Home FC v Away FC",
		CanonicalEventID: canonicalID,
		Reviewer:         "integration-test",
		ReviewReason:     "event verified",
		MatchReason:      "manual_review",
	}); err != nil {
		t.Fatalf("approve event alias: %v", err)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM fair_value_ready_betfair_exchange_quotes`); got != 0 {
		t.Fatalf("mapped rows with event only=%d want 0", got)
	}

	if err := st.ApproveBetfairMarketAlias(ctx, model.BetfairMarketAliasDecision{
		BetfairMarketID:       "1.234",
		BetfairEventID:        "event-1",
		CanonicalEventID:      canonicalID,
		BetfairMarketType:     "MATCH_ODDS",
		CanonicalMarketFamily: "h2h",
		Reviewer:              "integration-test",
		ReviewReason:          "market verified",
		MatchReason:           "manual_review",
	}); err != nil {
		t.Fatalf("approve market alias: %v", err)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM fair_value_ready_betfair_exchange_quotes`); got != 0 {
		t.Fatalf("mapped rows without runner=%d want 0", got)
	}

	if err := st.RejectBetfairRunnerAlias(ctx, model.BetfairRunnerAliasDecision{
		BetfairMarketID: "1.234",
		SelectionID:     101,
		Handicap:        0,
		RunnerName:      "Home FC",
		Reviewer:        "integration-test",
		ReviewReason:    "temporary runner rejection fixture",
		MatchReason:     "manual_review",
	}); err != nil {
		t.Fatalf("reject runner alias before approval: %v", err)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM fair_value_ready_betfair_exchange_quotes`); got != 0 {
		t.Fatalf("mapped rows with rejected runner=%d want 0", got)
	}

	if err := st.ApproveBetfairRunnerAlias(ctx, model.BetfairRunnerAliasDecision{
		BetfairMarketID:      "1.234",
		SelectionID:          101,
		Handicap:             0,
		RunnerName:           "Home FC",
		CanonicalOutcomeName: "Home FC",
		CanonicalOutcomeSide: "home",
		Reviewer:             "integration-test",
		ReviewReason:         "runner verified",
		MatchReason:          "manual_review",
	}); err != nil {
		t.Fatalf("approve runner alias: %v", err)
	}

	var rows, backRows, layRows int
	var hasLTP, hasTradedVolume bool
	if err := db.QueryRowContext(ctx, `
        SELECT
            count(*),
            count(*) FILTER (WHERE exchange_side = 'back'),
            count(*) FILTER (WHERE exchange_side = 'lay'),
            bool_or(last_traded_price IS NOT NULL),
            bool_or(traded_volume IS NOT NULL)
        FROM fair_value_ready_betfair_exchange_quotes
    `).Scan(&rows, &backRows, &layRows, &hasLTP, &hasTradedVolume); err != nil {
		t.Fatalf("query mapped Betfair rows: %v", err)
	}
	if rows != 2 || backRows != 1 || layRows != 1 {
		t.Fatalf("mapped rows total=%d back=%d lay=%d want 2/1/1", rows, backRows, layRows)
	}
	if !hasLTP || !hasTradedVolume {
		t.Fatalf("mapped metadata ltp=%t traded_volume=%t want true/true", hasLTP, hasTradedVolume)
	}

	if confirmed, err := st.ListConfirmedBetfairEventAliases(ctx, model.BetfairMapListOptions{Limit: 10}); err != nil {
		t.Fatalf("list confirmed events: %v", err)
	} else if len(confirmed) != 1 || confirmed[0].BetfairEventID != "event-1" {
		t.Fatalf("confirmed event aliases=%+v", confirmed)
	}
	if confirmed, err := st.ListConfirmedBetfairMarketAliases(ctx, model.BetfairMapListOptions{Limit: 10}); err != nil {
		t.Fatalf("list confirmed markets: %v", err)
	} else if len(confirmed) != 1 || confirmed[0].CanonicalMarketFamily != "h2h" {
		t.Fatalf("confirmed market aliases=%+v", confirmed)
	}
	if confirmed, err := st.ListConfirmedBetfairRunnerAliases(ctx, model.BetfairMapListOptions{Limit: 10}); err != nil {
		t.Fatalf("list confirmed runners: %v", err)
	} else if len(confirmed) != 1 || confirmed[0].CanonicalOutcomeSide != "home" {
		t.Fatalf("confirmed runner aliases=%+v", confirmed)
	}
}

func TestDBIntegrationBetfairMapReviewRejectsConflictingConfirmedAliases(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	st, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	firstCanonicalID := insertBetfairCanonicalEvent(ctx, t, db, "Conflict Home", "Conflict Away")
	secondCanonicalID := insertBetfairCanonicalEvent(ctx, t, db, "Other Home", "Other Away")

	if err := st.ApproveBetfairEventAlias(ctx, model.BetfairEventAliasDecision{
		BetfairEventID:   "conflict-event",
		CanonicalEventID: firstCanonicalID,
		Reviewer:         "integration-test",
		ReviewReason:     "first event approval",
	}); err != nil {
		t.Fatalf("approve first event alias: %v", err)
	}
	err = st.ApproveBetfairEventAlias(ctx, model.BetfairEventAliasDecision{
		BetfairEventID:   "conflict-event",
		CanonicalEventID: secondCanonicalID,
		Reviewer:         "integration-test",
		ReviewReason:     "conflicting event approval",
	})
	assertConflictError(t, err, "already has a confirmed alias")

	if err := st.ApproveBetfairMarketAlias(ctx, model.BetfairMarketAliasDecision{
		BetfairMarketID:       "conflict-market",
		BetfairEventID:        "conflict-event",
		CanonicalEventID:      firstCanonicalID,
		CanonicalMarketFamily: "h2h",
		Reviewer:              "integration-test",
		ReviewReason:          "first market approval",
	}); err != nil {
		t.Fatalf("approve first market alias: %v", err)
	}
	err = st.ApproveBetfairMarketAlias(ctx, model.BetfairMarketAliasDecision{
		BetfairMarketID:       "conflict-market",
		BetfairEventID:        "conflict-event",
		CanonicalEventID:      firstCanonicalID,
		CanonicalMarketFamily: "totals",
		Reviewer:              "integration-test",
		ReviewReason:          "conflicting market approval",
	})
	assertConflictError(t, err, "confirmed market family")

	if err := st.ApproveBetfairRunnerAlias(ctx, model.BetfairRunnerAliasDecision{
		BetfairMarketID:      "conflict-market",
		SelectionID:          101,
		CanonicalOutcomeName: "Conflict Home",
		CanonicalOutcomeSide: "home",
		Reviewer:             "integration-test",
		ReviewReason:         "first runner approval",
	}); err != nil {
		t.Fatalf("approve first runner alias: %v", err)
	}
	err = st.ApproveBetfairRunnerAlias(ctx, model.BetfairRunnerAliasDecision{
		BetfairMarketID:      "conflict-market",
		SelectionID:          101,
		CanonicalOutcomeName: "Conflict Away",
		CanonicalOutcomeSide: "away",
		Reviewer:             "integration-test",
		ReviewReason:         "conflicting runner approval",
	})
	assertConflictError(t, err, "already has confirmed outcome")
}

func rejectBetfairFixtures(ctx context.Context, t *testing.T, st *PostgresStore) {
	t.Helper()
	if err := st.RejectBetfairEventAlias(ctx, model.BetfairEventAliasDecision{
		BetfairEventID:   "rejected-event",
		BetfairEventName: "Rejected Event",
		Reviewer:         "integration-test",
		ReviewReason:     "event rejected fixture",
	}); err != nil {
		t.Fatalf("reject event alias: %v", err)
	}
	if err := st.RejectBetfairMarketAlias(ctx, model.BetfairMarketAliasDecision{
		BetfairMarketID: "rejected-market",
		BetfairEventID:  "rejected-event",
		Reviewer:        "integration-test",
		ReviewReason:    "market rejected fixture",
	}); err != nil {
		t.Fatalf("reject market alias: %v", err)
	}
	if err := st.RejectBetfairRunnerAlias(ctx, model.BetfairRunnerAliasDecision{
		BetfairMarketID: "rejected-market",
		SelectionID:     999,
		Reviewer:        "integration-test",
		ReviewReason:    "runner rejected fixture",
	}); err != nil {
		t.Fatalf("reject runner alias: %v", err)
	}
}

func assertConflictError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected conflict error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("conflict error=%v want containing %q", err, want)
	}
}
