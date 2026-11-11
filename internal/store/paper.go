package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/your-org/pol-odds-collector/internal/paper"
	sigpkg "github.com/your-org/pol-odds-collector/internal/signal"
)

type PaperStore struct{ db *sql.DB }

func NewPaperStore(s *PostgresStore) *PaperStore { return &PaperStore{db: s.db} }

// ReadBuyCandidates returns BUY_CANDIDATE signals created within
// [windowStart, windowEnd), oldest-first. NULL pm_ask rows are dropped at
// the SQL layer so the simulator only sees fillable rows.
func (p *PaperStore) ReadBuyCandidates(ctx context.Context, windowStart, windowEnd time.Time) ([]paper.SignalRow, error) {
	rows, err := p.db.QueryContext(ctx, `
        SELECT
            id, created_at, matchup_id, outcome_side,
            polymarket_condition_id, COALESCE(polymarket_token_id, ''),
            home_name, away_name, league_name,
            state,
            COALESCE(pm_ask, 0)::float8,
            COALESCE(net_edge_buy, 0)::float8,
            COALESCE(proposed_size_usdc, 0)::float8
          FROM signals
         WHERE state = 'BUY_CANDIDATE'
           AND created_at >= $1
           AND created_at <  $2
           AND pm_ask IS NOT NULL
           AND pm_ask > 0
         ORDER BY created_at ASC
    `, windowStart, windowEnd)
	if err != nil {
		return nil, fmt.Errorf("read buy candidates: %w", err)
	}
	defer rows.Close()
	out := make([]paper.SignalRow, 0, 32)
	for rows.Next() {
		var r paper.SignalRow
		var stateStr string
		if err := rows.Scan(
			&r.ID, &r.CreatedAt, &r.MatchupID, &r.OutcomeSide,
			&r.PolymarketConditionID, &r.PolymarketTokenID,
			&r.HomeName, &r.AwayName, &r.LeagueName,
			&stateStr,
			&r.PMAsk, &r.NetEdgeBuy, &r.ProposedSizeUSDC,
		); err != nil {
			return out, fmt.Errorf("scan candidate: %w", err)
		}
		r.State = sigpkg.State(stateStr)
		out = append(out, r)
	}
	return out, rows.Err()
}

// LatestPolymarketPrices returns the most recent bid/ask/mid per token from
// polymarket_price_snapshots, keyed by token_id. Rows where ANY of bid/ask/
// mid is positive are returned so the simulator can pick a usable mark price
// (mid preferred, then (bid+ask)/2, then ask) — settled tokens often expose
// only ask=0.001 with a null mid and we still want their MTM to count.
func (p *PaperStore) LatestPolymarketPrices(ctx context.Context) (map[string]paper.PriceRow, error) {
	rows, err := p.db.QueryContext(ctx, `
        SELECT DISTINCT ON (token_id)
            token_id,
            COALESCE(bid_price, 0)::float8,
            COALESCE(ask_price, 0)::float8,
            COALESCE(mid_price, 0)::float8,
            received_at
          FROM polymarket_price_snapshots
         WHERE (mid_price IS NOT NULL AND mid_price > 0)
            OR (ask_price IS NOT NULL AND ask_price > 0)
            OR (bid_price IS NOT NULL AND bid_price > 0)
         ORDER BY token_id, received_at DESC
    `)
	if err != nil {
		return nil, fmt.Errorf("read latest pm prices: %w", err)
	}
	defer rows.Close()
	out := make(map[string]paper.PriceRow, 64)
	for rows.Next() {
		var pr paper.PriceRow
		if err := rows.Scan(&pr.PolymarketTokenID, &pr.Bid, &pr.Ask, &pr.Mid, &pr.UpdatedAt); err != nil {
			return out, fmt.Errorf("scan pm price: %w", err)
		}
		out[pr.PolymarketTokenID] = pr
	}
	return out, rows.Err()
}

// LatestResolutions returns the resolved-market map keyed by condition_id.
// Reads from the polymarket_resolved_markets view (migration 025). Markets
// without an extractable winner (e.g. 3-way without a "1" outcome price)
// are skipped.
func (p *PaperStore) LatestResolutions(ctx context.Context) (map[string]paper.Resolution, error) {
	rows, err := p.db.QueryContext(ctx, `
        SELECT
            condition_id,
            COALESCE(winning_token_id, ''),
            COALESCE(winning_outcome_name, ''),
            closed_time
          FROM polymarket_resolved_markets
         WHERE winning_token_id IS NOT NULL
    `)
	if err != nil {
		return nil, fmt.Errorf("read resolutions: %w", err)
	}
	defer rows.Close()
	out := make(map[string]paper.Resolution, 32)
	for rows.Next() {
		var r paper.Resolution
		var closed sql.NullTime
		if err := rows.Scan(&r.ConditionID, &r.WinningTokenID, &r.WinningOutcomeName, &closed); err != nil {
			return out, fmt.Errorf("scan resolution: %w", err)
		}
		if closed.Valid {
			r.ClosedAt = closed.Time
		}
		out[r.ConditionID] = r
	}
	return out, rows.Err()
}

// StartRun creates an in-flight paper_pnl_runs row and returns the id.
func (p *PaperStore) StartRun(ctx context.Context, start, end time.Time, sizeUSDC float64, dedupe bool) (int64, error) {
	var id int64
	err := p.db.QueryRowContext(ctx, `
        INSERT INTO paper_pnl_runs
            (window_start, window_end, proposed_size_usdc, dedupe_per_matchup_outcome, status)
        VALUES ($1, $2, $3, $4, 'running')
        RETURNING id
    `, start, end, sizeUSDC, dedupe).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("start paper run: %w", err)
	}
	return id, nil
}

// FinishRun closes a run row with summary aggregates.
func (p *PaperStore) FinishRun(ctx context.Context, runID int64, s paper.Summary, notes, status string) error {
	var hitRate any
	if s.TradesResolved > 0 {
		hitRate = s.HitRate
	}
	_, err := p.db.ExecContext(ctx, `
        UPDATE paper_pnl_runs SET
            finished_at = now(),
            candidates_seen = $2,
            trades_simulated = $3,
            trades_with_mtm = $4,
            trades_unpriced = $5,
            total_size_usdc = $6,
            total_mtm_pnl_usdc = $7,
            avg_claimed_net_edge_pp = $8,
            avg_mtm_edge_pp = $9,
            trades_resolved = $10,
            trades_won = $11,
            total_realized_pnl_usdc = $12,
            hit_rate = $13,
            notes = $14,
            status = $15
          WHERE id = $1
    `, runID,
		s.CandidatesSeen, s.TradesSimulated, s.TradesWithMTM, s.TradesUnpriced,
		s.TotalSizeUSDC, s.TotalMTMPnLUSDC,
		s.AvgClaimedNetEdgePP, s.AvgMTMEdgePP,
		s.TradesResolved, s.TradesWon, s.TotalRealizedPnLUSDC, hitRate,
		notes, status)
	if err != nil {
		return fmt.Errorf("finish paper run: %w", err)
	}
	return nil
}
