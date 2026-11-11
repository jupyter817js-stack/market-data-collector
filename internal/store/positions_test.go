package store

import (
	"testing"
	"time"
)

// TestDBIntegrationPositionsRoundtrip exercises migration 040: insert a
// fill, expect one open position; insert a second fill on the same
// token, expect the same row (partial unique index, shares + cost
// summed); close the position via direct_sell and verify the realized
// PnL math. Skipped unless RUN_DB_TESTS=1 + TEST_DATABASE_URL set.
func TestDBIntegrationPositionsRoundtrip(t *testing.T) {
	ctx, db, _ := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	ps := &PositionStore{db: db}

	now := time.Now().UTC().Truncate(time.Second)
	fill1 := Fill{
		PolymarketTokenID:     "tok_home_1",
		PolymarketConditionID: "cond_1",
		MatchupID:             1001,
		OutcomeSide:           "home",
		Side:                  "BUY",
		Shares:                40,
		CostUSDC:              20.0, // entry price 0.50
		FilledAt:              now,
	}
	id1, err := ps.UpsertOpenFromFill(ctx, fill1)
	if err != nil {
		t.Fatalf("upsert fill1: %v", err)
	}
	if id1 == 0 {
		t.Fatal("upsert fill1 returned id 0")
	}

	// Second partial fill on the same token must update the same row.
	fill2 := Fill{
		PolymarketTokenID:     "tok_home_1",
		PolymarketConditionID: "cond_1",
		MatchupID:             1001,
		OutcomeSide:           "home",
		Side:                  "BUY",
		Shares:                10,
		CostUSDC:              5.0, // also 0.50 effective
		FilledAt:              now.Add(2 * time.Second),
	}
	id2, err := ps.UpsertOpenFromFill(ctx, fill2)
	if err != nil {
		t.Fatalf("upsert fill2: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("fill2 created new row id=%d want %d (partial-unique-index merge)", id2, id1)
	}

	var shares, cost, avg float64
	var status string
	if err := db.QueryRowContext(ctx, `
        SELECT shares_filled, cost_usdc, avg_entry_price, status
          FROM positions WHERE id = $1
    `, id1).Scan(&shares, &cost, &avg, &status); err != nil {
		t.Fatalf("read merged position: %v", err)
	}
	if shares != 50 || cost != 25.0 {
		t.Fatalf("merged shares=%v cost=%v want 50 / 25.0", shares, cost)
	}
	if avg < 0.499 || avg > 0.501 {
		t.Fatalf("avg_entry_price=%v want ~0.50", avg)
	}
	if status != "open" {
		t.Fatalf("status=%q want open", status)
	}

	// Close direct_sell at proceeds $30 → +$5 realized.
	if err := ps.CloseWithRealized(ctx, id1, "direct_sell", 30.0, "test close"); err != nil {
		t.Fatalf("CloseWithRealized: %v", err)
	}
	var closedStatus, route string
	var proceeds, realized float64
	if err := db.QueryRowContext(ctx, `
        SELECT status, exit_route, exit_proceeds_usdc, realized_pnl_usdc
          FROM positions WHERE id = $1
    `, id1).Scan(&closedStatus, &route, &proceeds, &realized); err != nil {
		t.Fatalf("read closed position: %v", err)
	}
	if closedStatus != "closed" || route != "direct_sell" {
		t.Fatalf("close mismatch: status=%q route=%q", closedStatus, route)
	}
	if proceeds != 30.0 || realized != 5.0 {
		t.Fatalf("proceeds=%v realized=%v want 30 / 5", proceeds, realized)
	}

	// After close, a fresh fill on the same token MUST open a NEW
	// row (closed positions don't block the partial unique index).
	fill3 := Fill{
		PolymarketTokenID:     "tok_home_1",
		PolymarketConditionID: "cond_1",
		MatchupID:             1001,
		OutcomeSide:           "home",
		Side:                  "BUY",
		Shares:                20,
		CostUSDC:              12.0, // entry 0.60 — fresh open
		FilledAt:              now.Add(time.Hour),
	}
	id3, err := ps.UpsertOpenFromFill(ctx, fill3)
	if err != nil {
		t.Fatalf("upsert fill3 after close: %v", err)
	}
	if id3 == id1 {
		t.Fatalf("fill3 reused closed row id=%d; must open a new one", id3)
	}

	// CloseWithRealized on an already-closed row returns an error
	// (idempotency-by-error rather than silent no-op).
	if err := ps.CloseWithRealized(ctx, id1, "direct_sell", 100, "double-close"); err == nil {
		t.Fatal("expected error closing an already-closed row")
	}

	// Invalid route rejected.
	if err := ps.CloseWithRealized(ctx, id3, "moon", 30, ""); err == nil {
		t.Fatal("expected error on invalid exit_route")
	}
}

func TestDBIntegrationPositionExitRoutesView(t *testing.T) {
	ctx, db, _ := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	// Seed condition with two tokens (binary market) + one price snapshot per.
	if _, err := db.ExecContext(ctx, `
        INSERT INTO polymarket_events
          (polymarket_event_id, title, slug, active, closed)
        VALUES ('ev1','Test event','ev1', true, false);
        INSERT INTO polymarket_markets
          (polymarket_market_id, polymarket_event_id, condition_id, question,
           active, closed)
        VALUES ('m1','ev1','condX','Will home win?', true, false);
        INSERT INTO polymarket_tokens
          (token_id, condition_id, polymarket_market_id, outcome_name, side)
        VALUES ('tokYes','condX','m1','Yes','yes'),
               ('tokNo','condX','m1','No','no');
        INSERT INTO polymarket_price_snapshots
          (token_id, condition_id, polymarket_market_id,
           bid_price, ask_price, mid_price, source, received_at)
        VALUES ('tokYes','condX','m1', 0.55, 0.60, 0.575, 'polymarket', now()),
               ('tokNo' ,'condX','m1', 0.38, 0.43, 0.405, 'polymarket', now());
    `); err != nil {
		t.Fatalf("seed market fixtures: %v", err)
	}

	ps := &PositionStore{db: db}
	if _, err := ps.UpsertOpenFromFill(ctx, Fill{
		PolymarketTokenID:     "tokYes",
		PolymarketConditionID: "condX",
		MatchupID:             4242,
		OutcomeSide:           "home",
		Side:                  "BUY",
		Shares:                100,
		CostUSDC:              50.0, // entry 0.50
		FilledAt:              time.Now().UTC(),
	}); err != nil {
		t.Fatalf("open position: %v", err)
	}

	rows, err := ps.ListOpenExitRoutes(ctx)
	if err != nil {
		t.Fatalf("ListOpenExitRoutes: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	r := rows[0]
	// direct_sell = 100 × 0.55 = 55
	// synthetic_sell = 100 × (1 - 0.43) = 57
	// best = synthetic_sell = 57
	if r.DirectSellValue < 54.99 || r.DirectSellValue > 55.01 {
		t.Fatalf("direct_sell_value=%v want ~55", r.DirectSellValue)
	}
	if r.SyntheticSellValue < 56.99 || r.SyntheticSellValue > 57.01 {
		t.Fatalf("synthetic_sell_value=%v want ~57", r.SyntheticSellValue)
	}
	if r.RecommendedExit != "synthetic_sell" {
		t.Fatalf("recommended_exit=%q want synthetic_sell", r.RecommendedExit)
	}
	if r.BestExitValue < 56.99 || r.BestExitValue > 57.01 {
		t.Fatalf("best_exit_value=%v want ~57", r.BestExitValue)
	}
	if r.UnrealizedPnLUSDC < 6.99 || r.UnrealizedPnLUSDC > 7.01 {
		t.Fatalf("unrealized_pnl=%v want ~7 (57 - 50)", r.UnrealizedPnLUSDC)
	}
}
