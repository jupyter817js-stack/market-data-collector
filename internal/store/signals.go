package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/your-org/pol-odds-collector/internal/signal"
)

// SignalStore reads the edge view and writes signal/run rows.
type SignalStore struct{ db *sql.DB }

func NewSignalStore(s *PostgresStore) *SignalStore { return &SignalStore{db: s.db} }

// ReadEdges pulls the latest snapshot of pinnacle_polymarket_moneyline_edges.
// Rows where outcome_side='unknown' are excluded by the view itself.
//
// Filter `last_is_live = true` is applied here (NOT in the base view) so
// the dashboard / paper-pnl pre-match audit can still query the full
// edge view, while signal-engine — which is live-only per spec §1 —
// never processes pre-match rows. Pre-match tokens are typically NOT
// in the 300-token priority price loop, so their pm_ask is NULL and
// every tick would otherwise emit MAPPING_INVALID for "PM bid/ask
// missing". Observed live: this filter cuts the MAPPING_INVALID share
// in the signals table from ~50% to its real value.
func (s *SignalStore) ReadEdges(ctx context.Context, depthSizeUSDC float64) ([]signal.EdgeRow, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT
            e.matchup_id,
            e.polymarket_condition_id,
            e.token_id,
            e.outcome_side,
            e.outcome_name,
            e.home_name,
            e.away_name,
            e.league_name,
            COALESCE(e.pinnacle_fair_prob, 0)::float8,
            e.ask_price,
            e.bid_price,
            e.mid_price,
            e.spread,
            e.liquidity,
            COALESCE(e.pinnacle_market_status, ''),
            COALESCE(e.pinnacle_market_version, 0),
            COALESCE(e.pinnacle_overround, 0)::float8,
            COALESCE(e.gross_edge_to_buy, 0)::float8,
            COALESCE(e.gross_edge_to_sell, 0)::float8,
            COALESCE(e.pinnacle_age_sec, 0)::float8,
            COALESCE(e.pm_age_sec, 0)::float8,
            polymarket_depth_avg_price(e.token_id, 'ask', $1::numeric)::float8
              AS depth_avg_ask,
            COALESCE(ms.event_state, '') AS match_event_state,
            COALESCE(ms.match_phase, '') AS match_phase
          FROM pinnacle_polymarket_moneyline_edges e
          LEFT JOIN pinnacle_match_state ms
            ON ms.matchup_id = e.matchup_id
         WHERE e.last_is_live = true
    `, depthSizeUSDC)
	if err != nil {
		return nil, fmt.Errorf("read edges: %w", err)
	}
	defer rows.Close()

	out := make([]signal.EdgeRow, 0, 32)
	for rows.Next() {
		var r signal.EdgeRow
		if err := rows.Scan(
			&r.MatchupID,
			&r.PolymarketConditionID,
			&r.PolymarketTokenID,
			&r.OutcomeSide,
			&r.OutcomeName,
			&r.HomeName,
			&r.AwayName,
			&r.LeagueName,
			&r.PinnacleFairProb,
			&r.PMAsk,
			&r.PMBid,
			&r.PMMid,
			&r.PMSpread,
			&r.PMLiquidity,
			&r.PinnacleMarketStatus,
			&r.PinnacleMarketVersion,
			&r.PinnacleOverround,
			&r.GrossEdgeToBuy,
			&r.GrossEdgeToSell,
			&r.PinnacleAgeSec,
			&r.PMAgeSec,
			&r.DepthAvgAsk,
			&r.MatchEventState,
			&r.MatchPhase,
		); err != nil {
			return out, fmt.Errorf("scan edge row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// InsertSignal appends one signal row. All numeric fields are written as
// 'p_*' parameters; sql.NullFloat64 from the row are passed through (driver
// translates Valid=false to SQL NULL).
func (s *SignalStore) InsertSignal(ctx context.Context, sig signal.Signal) error {
	reasons, _ := json.Marshal(sig.Reasons)
	snap, _ := json.Marshal(sig.EdgeRow)

	_, err := s.db.ExecContext(ctx, `
        INSERT INTO signals (
            matchup_id, polymarket_condition_id, polymarket_token_id,
            outcome_side, outcome_name, home_name, away_name, league_name,
            pinnacle_fair_prob, pm_ask, pm_bid, pm_mid, pm_spread, pm_liquidity,
            pinnacle_market_status, pinnacle_market_version, pinnacle_overround,
            gross_edge_to_buy, gross_edge_to_sell,
            spread_cost, slippage_buffer, latency_buffer, overround_buffer, net_edge_buy,
            pinnacle_age_sec, pm_age_sec,
            proposed_size_usdc, threshold_net_edge,
            state, reasons, edge_snapshot
        ) VALUES (
            $1,$2,$3, $4,$5,$6,$7,$8,
            $9,$10,$11,$12,$13,$14,
            $15,$16,$17,
            $18,$19,
            $20,$21,$22,$23,$24,
            $25,$26,
            $27,$28,
            $29,$30::jsonb,$31::jsonb
        )
    `,
		sig.MatchupID, sig.PolymarketConditionID, nullStr(sig.PolymarketTokenID),
		sig.OutcomeSide, sig.OutcomeName, sig.HomeName, sig.AwayName, sig.LeagueName,
		sig.PinnacleFairProb, sig.PMAsk, sig.PMBid, sig.PMMid, sig.PMSpread, sig.PMLiquidity,
		sig.PinnacleMarketStatus, sig.PinnacleMarketVersion, sig.PinnacleOverround,
		sig.GrossEdgeToBuy, sig.GrossEdgeToSell,
		sig.SpreadCost, sig.SlippageBuffer, sig.LatencyBuffer, sig.OverroundBuffer, sig.NetEdgeBuy,
		sig.PinnacleAgeSec, sig.PMAgeSec,
		sig.ProposedSizeUSDC, sig.ThresholdNetEdge,
		string(sig.State), string(reasons), string(snap),
	)
	if err != nil {
		return fmt.Errorf("insert signal: %w", err)
	}
	return nil
}

// StartRun records the start of a polling tick. Returns the run id.
func (s *SignalStore) StartRun(ctx context.Context) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
        INSERT INTO signal_runs (status) VALUES ('running') RETURNING id
    `).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("start signal run: %w", err)
	}
	return id, nil
}

// FinishRun closes a tick row with totals + state counts.
func (s *SignalStore) FinishRun(ctx context.Context, runID int64, edgeRows, signalsEmitted int, stateCounts map[signal.State]int, status, errMsg string) error {
	// JSON keys must be strings, not signal.State alias.
	counts := map[string]int{}
	for k, v := range stateCounts {
		counts[string(k)] = v
	}
	cj, _ := json.Marshal(counts)
	_, err := s.db.ExecContext(ctx, `
        UPDATE signal_runs SET
            finished_at = now(),
            edge_rows_seen = $2,
            signals_emitted = $3,
            state_counts = $4::jsonb,
            status = $5,
            error_message = $6
          WHERE id = $1
    `, runID, edgeRows, signalsEmitted, string(cj), status, errMsg)
	if err != nil {
		return fmt.Errorf("finish signal run: %w", err)
	}
	return nil
}

// ListRecentSignals returns the last N rows, optionally filtered by state.
func (s *SignalStore) ListRecentSignals(ctx context.Context, state string, limit int) ([]signal.Signal, error) {
	if limit <= 0 {
		limit = 20
	}
	q := `
        SELECT
            id,
            matchup_id, polymarket_condition_id, COALESCE(polymarket_token_id, ''),
            outcome_side, outcome_name, home_name, away_name, league_name,
            COALESCE(pinnacle_fair_prob, 0)::float8,
            pm_ask, pm_bid, pm_mid, pm_spread, pm_liquidity,
            pinnacle_market_status, COALESCE(pinnacle_market_version, 0),
            COALESCE(pinnacle_overround, 0)::float8,
            COALESCE(gross_edge_to_buy, 0)::float8,
            COALESCE(gross_edge_to_sell, 0)::float8,
            COALESCE(spread_cost, 0)::float8,
            COALESCE(slippage_buffer, 0)::float8,
            COALESCE(latency_buffer, 0)::float8,
            COALESCE(overround_buffer, 0)::float8,
            COALESCE(net_edge_buy, 0)::float8,
            COALESCE(pinnacle_age_sec, 0)::float8,
            COALESCE(pm_age_sec, 0)::float8,
            COALESCE(proposed_size_usdc, 0)::float8,
            COALESCE(threshold_net_edge, 0)::float8,
            state, reasons
          FROM signals
    `
	args := []any{}
	if state != "" {
		q += " WHERE state = $1"
		args = append(args, state)
	}
	q += fmt.Sprintf(" ORDER BY created_at DESC LIMIT %d", limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list signals: %w", err)
	}
	defer rows.Close()

	out := []signal.Signal{}
	for rows.Next() {
		var s signal.Signal
		var stateStr string
		var reasonsRaw []byte
		if err := rows.Scan(
			&s.ID,
			&s.MatchupID, &s.PolymarketConditionID, &s.PolymarketTokenID,
			&s.OutcomeSide, &s.OutcomeName, &s.HomeName, &s.AwayName, &s.LeagueName,
			&s.PinnacleFairProb,
			&s.PMAsk, &s.PMBid, &s.PMMid, &s.PMSpread, &s.PMLiquidity,
			&s.PinnacleMarketStatus, &s.PinnacleMarketVersion,
			&s.PinnacleOverround,
			&s.GrossEdgeToBuy, &s.GrossEdgeToSell,
			&s.SpreadCost, &s.SlippageBuffer, &s.LatencyBuffer, &s.OverroundBuffer, &s.NetEdgeBuy,
			&s.PinnacleAgeSec, &s.PMAgeSec,
			&s.ProposedSizeUSDC, &s.ThresholdNetEdge,
			&stateStr, &reasonsRaw,
		); err != nil {
			return out, fmt.Errorf("scan signal: %w", err)
		}
		s.State = signal.State(stateStr)
		_ = json.Unmarshal(reasonsRaw, &s.Reasons)
		out = append(out, s)
	}
	return out, rows.Err()
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
