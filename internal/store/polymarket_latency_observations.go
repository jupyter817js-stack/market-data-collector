package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/your-org/pol-odds-collector/internal/model"
)

// InsertPolymarketLatencyObservation appends one tick to
// polymarket_latency_observations. received_at defaults to
// clock_timestamp() on the DB side so ms-resolution is preserved.
func (s *PostgresStore) InsertPolymarketLatencyObservation(ctx context.Context, in model.PolymarketLatencyObservation) (int64, error) {
	if in.LSportsFixtureID <= 0 {
		return 0, fmt.Errorf("pm latency observation: lsports_fixture_id must be > 0")
	}
	if in.PolymarketTokenID == "" {
		return 0, errors.New("pm latency observation: polymarket_token_id is required")
	}
	if len(in.RawPayload) == 0 {
		return 0, errors.New("pm latency observation: raw_payload is required (JSONB cannot be NULL)")
	}

	const q = `
		INSERT INTO polymarket_latency_observations (
			lsports_fixture_id,
			polymarket_market_id, polymarket_condition_id, polymarket_token_id,
			received_at, source_publish_time,
			best_bid, best_ask, midpoint, last_price, spread, liquidity,
			price_changed_from_prior,
			raw_payload
		) VALUES (
			$1,
			$2, $3, $4,
			COALESCE($5, clock_timestamp()), $6,
			$7, $8, $9, $10, $11, $12,
			$13,
			$14
		)
		RETURNING id`
	var id int64
	err := s.db.QueryRowContext(ctx, q,
		in.LSportsFixtureID,
		nullableString(in.PolymarketMarketID),
		nullableString(in.PolymarketConditionID),
		in.PolymarketTokenID,
		nullableTime(in.ReceivedAt),
		nullableTime(in.SourcePublishTime),
		nullableFloat(in.BestBid),
		nullableFloat(in.BestAsk),
		nullableFloat(in.Midpoint),
		nullableFloat(in.LastPrice),
		nullableFloat(in.Spread),
		nullableFloat(in.Liquidity),
		nullableBool(in.PriceChangedFromPrior),
		[]byte(in.RawPayload),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert polymarket_latency_observations: %w", err)
	}
	return id, nil
}

// LookupPolymarketTokenContext returns the condition_id and market_id for a
// CLOB token, so the latency daemon can enrich its observations without the
// operator having to supply every identifier on the command line. Returns
// empty strings (not an error) if the token isn't in polymarket_tokens —
// the daemon still records the observation with just the token_id.
func (s *PostgresStore) LookupPolymarketTokenContext(ctx context.Context, tokenID string) (conditionID, marketID string, err error) {
	const q = `
		SELECT COALESCE(condition_id, ''), COALESCE(polymarket_market_id, '')
		  FROM polymarket_tokens
		 WHERE token_id = $1
		 LIMIT 1`
	if err := s.db.QueryRowContext(ctx, q, tokenID).Scan(&conditionID, &marketID); err != nil {
		// No rows is the common case for tokens we haven't synced yet — not
		// an error condition; caller proceeds with empty enrichment.
		return "", "", nil
	}
	return conditionID, marketID, nil
}
