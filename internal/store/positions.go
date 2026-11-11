package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/your-org/pol-odds-collector/internal/exit"
)

// PositionStore wraps the positions table introduced in migration 040.
// Reads from clob-fill-monitor (UpsertOpenFromFill on every fill update)
// and from any future close-out path (CloseWithRealized for direct/
// synthetic exits, MarkSettledFromResolution for market-resolution
// payout). The exit-route comparison sits in the
// position_exit_routes view; query it with ListOpenExitRoutes.
type PositionStore struct{ db *sql.DB }

// NewPositionStore wires onto an existing *PostgresStore connection,
// mirroring NewProposedOrderStore / NewPaperStore.
func NewPositionStore(s *PostgresStore) *PositionStore { return &PositionStore{db: s.db} }

// Fill is one observed fill against a proposed order. Shares + cost are
// the INCREMENTAL amounts since the last call (NOT cumulative). The
// fill monitor passes deltas it computes from successive
// OrderInfo.SizeMatched / AveragePrice snapshots.
type Fill struct {
	PolymarketTokenID     string
	PolymarketConditionID string
	MatchupID             int64 // 0 if unknown / legacy
	OutcomeSide           string
	Side                  string // BUY|SELL
	Shares                float64
	CostUSDC              float64 // for BUY: USDC paid; SELL not modeled yet
	FilledAt              time.Time
	SourceProposedOrderID int64 // 0 if standalone
}

// UpsertOpenFromFill adds the fill's shares + cost to the currently-open
// position for the token (per the partial unique index, at most one is
// open per token). When no open position exists yet, one is created. The
// returned id identifies the open position row whether it was inserted
// or updated.
func (p *PositionStore) UpsertOpenFromFill(ctx context.Context, f Fill) (int64, error) {
	if f.Shares <= 0 || f.CostUSDC < 0 {
		return 0, fmt.Errorf("store: UpsertOpenFromFill: invalid shares=%.6f cost=%.6f",
			f.Shares, f.CostUSDC)
	}
	if f.PolymarketTokenID == "" || f.PolymarketConditionID == "" {
		return 0, fmt.Errorf("store: UpsertOpenFromFill: missing token or condition id")
	}
	side := f.Side
	if side == "" {
		side = "BUY"
	}
	matchupID := sql.NullInt64{Int64: f.MatchupID, Valid: f.MatchupID > 0}
	sourceID := sql.NullInt64{Int64: f.SourceProposedOrderID, Valid: f.SourceProposedOrderID > 0}
	filledAt := f.FilledAt
	if filledAt.IsZero() {
		filledAt = time.Now().UTC()
	}

	var id int64
	err := p.db.QueryRowContext(ctx, `
        INSERT INTO positions
            (polymarket_token_id, polymarket_condition_id, matchup_id,
             outcome_side, side, shares_filled, cost_usdc, status,
             opened_at, last_fill_at, source_proposed_order_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'open', $8, $8, $9)
        ON CONFLICT (polymarket_token_id) WHERE status IN ('open','closing')
        DO UPDATE SET
            shares_filled = positions.shares_filled + EXCLUDED.shares_filled,
            cost_usdc     = positions.cost_usdc     + EXCLUDED.cost_usdc,
            last_fill_at  = EXCLUDED.last_fill_at,
            updated_at    = now()
        RETURNING id
    `, f.PolymarketTokenID, f.PolymarketConditionID, matchupID,
		f.OutcomeSide, side, f.Shares, f.CostUSDC, filledAt, sourceID).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("store: UpsertOpenFromFill: %w", err)
	}
	return id, nil
}

// CloseWithRealized transitions an open position to closed and records
// the exit route + realized PnL. Idempotent on re-call of an already-
// closed row (returns the existing realized_pnl).
func (p *PositionStore) CloseWithRealized(ctx context.Context, positionID int64,
	route string, proceedsUSDC float64, noteAppend string) error {
	switch route {
	case "direct_sell", "synthetic_sell", "merge", "expired_settled":
	default:
		return fmt.Errorf("store: CloseWithRealized: invalid route %q", route)
	}
	res, err := p.db.ExecContext(ctx, `
        UPDATE positions
           SET status              = 'closed',
               exit_route          = $2,
               exit_proceeds_usdc  = $3,
               realized_pnl_usdc   = $3 - cost_usdc,
               closed_at           = now(),
               updated_at          = now(),
               notes               = CASE WHEN $4 = '' THEN notes
                                          WHEN notes = '' THEN $4
                                          ELSE notes || E'\n' || $4 END
         WHERE id = $1
           AND status IN ('open','closing')
    `, positionID, route, proceedsUSDC, noteAppend)
	if err != nil {
		return fmt.Errorf("store: CloseWithRealized %d: %w", positionID, err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("store: CloseWithRealized %d: %d rows affected (already closed?)", positionID, n)
	}
	return nil
}

// OpenPosition is one snapshot of a held token plus its exit-route view.
type OpenPosition struct {
	PositionID            int64
	PolymarketTokenID     string
	PolymarketConditionID string
	MatchupID             sql.NullInt64
	OutcomeSide           string
	SharesFilled          float64
	CostUSDC              float64
	AvgEntryPrice         sql.NullFloat64

	HeldBid            sql.NullFloat64
	HeldAsk            sql.NullFloat64
	SumOppositeAsk     sql.NullFloat64
	OppositeTokenCount sql.NullInt64
	DirectSellValue    float64
	SyntheticSellValue float64
	BestExitValue      float64
	RecommendedExit    string
	UnrealizedPnLUSDC  float64
}

// InsertExitSignal persists one classification produced by
// internal/exit.Classify against an open position. Multiple rows per
// position accumulate (tick-by-tick audit). Returns the inserted id.
func (p *PositionStore) InsertExitSignal(ctx context.Context, sig exit.Signal) (int64, error) {
	reasonsJSON, err := json.Marshal(sig.Reasons)
	if err != nil {
		return 0, fmt.Errorf("store: InsertExitSignal marshal reasons: %w", err)
	}
	var id int64
	err = p.db.QueryRowContext(ctx, `
        INSERT INTO exit_signals
            (position_id, state, recommended_route,
             expected_proceeds_usdc, current_unrealized_pnl_usdc,
             unrealized_pnl_pct, held_bid, held_ask, sum_opposite_ask,
             position_age_seconds, reasons)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
        RETURNING id
    `, sig.PositionID, string(sig.State), sig.RecommendedRoute,
		sig.ExpectedProceedsUSDC, sig.UnrealizedPnLUSDC,
		sig.UnrealizedPnLPct, nullFloat(sig.HeldBid), nullFloat(sig.HeldAsk),
		nullFloat(sig.SumOppositeAsk), sig.PositionAgeSeconds,
		string(reasonsJSON)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("store: InsertExitSignal: %w", err)
	}
	return id, nil
}

func nullFloat(v float64) sql.NullFloat64 {
	return sql.NullFloat64{Float64: v, Valid: v > 0}
}

// ListOpenExitRoutes returns one row per open position with the
// computed exit-route candidates. Caller is expected to apply its own
// threshold logic (e.g. "only close if best_exit > cost * 1.02").
func (p *PositionStore) ListOpenExitRoutes(ctx context.Context) ([]OpenPosition, error) {
	rows, err := p.db.QueryContext(ctx, `
        SELECT position_id, polymarket_token_id, polymarket_condition_id,
               matchup_id, outcome_side, shares_filled, cost_usdc,
               avg_entry_price,
               held_bid, held_ask,
               sum_opposite_ask, opposite_token_count,
               direct_sell_value, synthetic_sell_value,
               recommended_exit, best_exit_value, best_unrealized_pnl_usdc
          FROM position_exit_routes
         ORDER BY best_unrealized_pnl_usdc DESC
    `)
	if err != nil {
		return nil, fmt.Errorf("store: ListOpenExitRoutes: %w", err)
	}
	defer rows.Close()
	var out []OpenPosition
	for rows.Next() {
		var op OpenPosition
		if err := rows.Scan(
			&op.PositionID, &op.PolymarketTokenID, &op.PolymarketConditionID,
			&op.MatchupID, &op.OutcomeSide, &op.SharesFilled, &op.CostUSDC,
			&op.AvgEntryPrice,
			&op.HeldBid, &op.HeldAsk,
			&op.SumOppositeAsk, &op.OppositeTokenCount,
			&op.DirectSellValue, &op.SyntheticSellValue,
			&op.RecommendedExit, &op.BestExitValue, &op.UnrealizedPnLUSDC,
		); err != nil {
			return nil, fmt.Errorf("store: ListOpenExitRoutes scan: %w", err)
		}
		out = append(out, op)
	}
	return out, rows.Err()
}
