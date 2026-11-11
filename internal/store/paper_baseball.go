package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/your-org/pol-odds-collector/internal/model"
)

// LookupPolymarketTokenByOutcome resolves (condition_id, outcome team name)
// to the matching CLOB token row. Case-insensitive on outcome_name and
// trims surrounding whitespace. Returns sql.ErrNoRows if no token matches —
// caller should treat that as "skip this signal" rather than fatal.
func (s *PostgresStore) LookupPolymarketTokenByOutcome(
	ctx context.Context, conditionID, outcomeName string,
) (model.PolymarketToken, error) {
	var tok model.PolymarketToken
	conditionID = strings.TrimSpace(conditionID)
	outcomeName = strings.TrimSpace(outcomeName)
	if conditionID == "" || outcomeName == "" {
		return tok, errors.New("paper_baseball: condition_id and outcome_name required")
	}
	const q = `
		SELECT token_id, condition_id, COALESCE(polymarket_market_id, ''),
		       outcome_name, outcome_index, side
		  FROM polymarket_tokens
		 WHERE condition_id = $1
		   AND lower(outcome_name) = lower($2)
		 LIMIT 1`
	var idx sql.NullInt64
	err := s.db.QueryRowContext(ctx, q, conditionID, outcomeName).Scan(
		&tok.TokenID, &tok.ConditionID, &tok.PolymarketMarketID,
		&tok.OutcomeName, &idx, &tok.Side,
	)
	if err != nil {
		return model.PolymarketToken{}, err
	}
	if idx.Valid {
		v := int(idx.Int64)
		tok.OutcomeIndex = &v
	}
	return tok, nil
}

// LookupPolymarketTokensByCondition returns every polymarket_tokens row
// for a given condition_id (typically 2 rows for a winner market —
// home_yes / away_yes). Used by the in-daemon price-capture goroutine to
// pick the two tokens to poll per fixture.
func (s *PostgresStore) LookupPolymarketTokensByCondition(
	ctx context.Context, conditionID string,
) ([]model.PolymarketToken, error) {
	conditionID = strings.TrimSpace(conditionID)
	if conditionID == "" {
		return nil, errors.New("paper_baseball: condition_id required")
	}
	const q = `
		SELECT token_id, condition_id, COALESCE(polymarket_market_id, ''),
		       outcome_name, outcome_index, side
		  FROM polymarket_tokens
		 WHERE condition_id = $1
		 ORDER BY outcome_index NULLS LAST, token_id`
	rows, err := s.db.QueryContext(ctx, q, conditionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.PolymarketToken
	for rows.Next() {
		var t model.PolymarketToken
		var idx sql.NullInt64
		if err := rows.Scan(&t.TokenID, &t.ConditionID, &t.PolymarketMarketID,
			&t.OutcomeName, &idx, &t.Side); err != nil {
			return nil, err
		}
		if idx.Valid {
			v := int(idx.Int64)
			t.OutcomeIndex = &v
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// InsertSyntheticBuySignal writes a row into the existing signals table on
// behalf of an LSPORTS-driven paper trade. Pinnacle freshness columns are
// filled with dummy values (V1 hack) so the signals_buy_candidates_fresh
// view picks the row up. The edge_snapshot JSONB carries the LSPORTS event
// attribution for downstream filtering.
//
// Returns the new signals.id so the caller can link it to the paper audit
// row.
func (s *PostgresStore) InsertSyntheticBuySignal(
	ctx context.Context, in model.SyntheticBuySignal,
) (int64, error) {
	if in.MatchupID <= 0 {
		return 0, errors.New("paper_baseball: matchup_id (=lsports_fixture_id) required")
	}
	if in.PolymarketConditionID == "" {
		return 0, errors.New("paper_baseball: polymarket_condition_id required")
	}
	if in.PolymarketTokenID == "" {
		return 0, errors.New("paper_baseball: polymarket_token_id required")
	}
	if len(in.EdgeSnapshot) == 0 {
		in.EdgeSnapshot = []byte(`{}`)
	}

	const q = `
		INSERT INTO signals (
			matchup_id, polymarket_condition_id, polymarket_token_id,
			outcome_side, outcome_name, home_name, away_name, league_name,
			pinnacle_fair_prob,
			pm_ask, pm_bid, pm_mid, pm_spread, pm_liquidity,
			pinnacle_market_status, pinnacle_market_version, pinnacle_overround,
			gross_edge_to_buy, gross_edge_to_sell,
			spread_cost, slippage_buffer, latency_buffer, overround_buffer, net_edge_buy,
			pinnacle_age_sec, pm_age_sec,
			proposed_size_usdc, threshold_net_edge,
			state, reasons, edge_snapshot
		) VALUES (
			$1, $2, $3,
			$4, $5, $6, $7, $8,
			NULL,
			$9, $10, $11, $12, $13,
			$14, NULL, NULL,
			$15, 0.0,
			0.0, 0.0, 0.0, 0.0, $15,
			0.1, $16,
			$17, 0.0,
			'BUY_CANDIDATE',
			'["lsports_score_change","paper_v1_hack"]'::jsonb,
			$18::jsonb
		)
		RETURNING id`

	var id int64
	err := s.db.QueryRowContext(ctx, q,
		in.MatchupID,
		in.PolymarketConditionID,
		in.PolymarketTokenID,
		in.OutcomeSide,
		in.OutcomeName,
		in.HomeName,
		in.AwayName,
		in.LeagueName,
		nullableFloat(in.PMAsk),
		nullableFloat(in.PMBid),
		nullableFloat(in.PMMid),
		nullableFloat(in.PMSpread),
		nullableFloat(in.PMLiquidity),
		"lsports_score_change", // pinnacle_market_status placeholder
		0.05,                   // gross_edge_to_buy + net_edge_buy (dummy)
		in.PMAgeSec,
		5.00, // proposed_size_usdc default $5 (live trading conservative)
		[]byte(in.EdgeSnapshot),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert synthetic buy signal: %w", err)
	}
	return id, nil
}

// InsertPaperBaseballSignal writes one audit row into paper_baseball_signals.
// Caller computes the five-stage latency timings and the signals.id link.
func (s *PostgresStore) InsertPaperBaseballSignal(
	ctx context.Context, in model.PaperBaseballSignal,
) (int64, error) {
	if in.LSportsFixtureID <= 0 {
		return 0, errors.New("paper_baseball: lsports_fixture_id required")
	}
	if in.ScoringSide != "home" && in.ScoringSide != "away" {
		return 0, fmt.Errorf("paper_baseball: scoring_side must be 'home' or 'away' (got %q)", in.ScoringSide)
	}
	if in.TargetPolymarketTokenID == "" {
		// allow skip rows — token lookup failed — but still record audit
	}
	if len(in.RawPayload) == 0 {
		in.RawPayload = []byte(`{}`)
	}

	const q = `
		INSERT INTO paper_baseball_signals (
			lsports_fixture_id,
			lsports_event_at, lsports_source_publish_at,
			score_before, score_after, scoring_side,
			home_team_name, away_team_name,
			target_polymarket_market_id, target_polymarket_condition_id,
			target_polymarket_token_id, target_outcome_name, target_outcome_side,
			pm_best_ask, pm_best_bid, pm_midpoint, pm_spread, pm_liquidity,
			pm_snapshot_at,
			signal_id, signal_skip_reason,
			lsports_to_daemon_ms, daemon_to_token_lookup_ms,
			token_lookup_to_pm_snapshot_ms, pm_snapshot_to_signal_insert_ms,
			total_signal_latency_ms,
			raw_payload
		) VALUES (
			$1,
			$2, $3,
			$4, $5, $6,
			$7, $8,
			$9, $10,
			$11, $12, $13,
			$14, $15, $16, $17, $18,
			$19,
			$20, $21,
			$22, $23,
			$24, $25,
			$26,
			$27::jsonb
		)
		RETURNING id`

	var id int64
	var signalID sql.NullInt64
	if in.SignalID != nil {
		signalID = sql.NullInt64{Int64: *in.SignalID, Valid: true}
	}

	err := s.db.QueryRowContext(ctx, q,
		in.LSportsFixtureID,
		in.LSportsEventAt,
		nullableTime(in.LSportsSourcePublishAt),
		in.ScoreBefore,
		in.ScoreAfter,
		in.ScoringSide,
		in.HomeTeamName,
		in.AwayTeamName,
		nullableString(in.TargetPolymarketMarketID),
		in.TargetPolymarketConditionID,
		in.TargetPolymarketTokenID,
		in.TargetOutcomeName,
		in.TargetOutcomeSide,
		nullableFloat(in.PMBestAsk),
		nullableFloat(in.PMBestBid),
		nullableFloat(in.PMMidpoint),
		nullableFloat(in.PMSpread),
		nullableFloat(in.PMLiquidity),
		in.PMSnapshotAt,
		signalID,
		in.SignalSkipReason,
		in.LSportsToDaemonMS,
		in.DaemonToTokenLookupMS,
		in.TokenLookupToPMSnapshotMS,
		in.PMSnapshotToSignalInsertMS,
		in.TotalSignalLatencyMS,
		[]byte(in.RawPayload),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert paper_baseball_signals: %w", err)
	}
	return id, nil
}
