package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/your-org/pol-odds-collector/internal/model"
)

// InsertLSportsInplayObservation appends one raw observation to
// lsports_inplay_observations. The defaults on received_at and created_at
// (clock_timestamp()) supply ms-resolution timestamps when the caller does
// not provide ReceivedAt explicitly — this is the canonical T_lsports for
// latency measurement.
func (s *PostgresStore) InsertLSportsInplayObservation(ctx context.Context, in model.LSportsInplayObservation) (int64, error) {
	if in.LSportsFixtureID <= 0 {
		return 0, fmt.Errorf("lsports inplay observation: fixture_id must be > 0")
	}
	if in.Source == "" {
		return 0, errors.New("lsports inplay observation: source is required (snapshot | amqp_push)")
	}
	if in.ObservationKind == "" {
		return 0, errors.New("lsports inplay observation: observation_kind is required")
	}
	if len(in.RawPayload) == 0 {
		return 0, errors.New("lsports inplay observation: raw_payload is required (JSONB cannot be NULL)")
	}

	const q = `
		INSERT INTO lsports_inplay_observations (
			lsports_fixture_id,
			source,
			observation_kind,
			received_at,
			source_publish_time,
			home_score, away_score, current_period, match_status, is_inplay,
			market_id, bet_name, bet_price, bet_line,
			raw_payload, payload_sha256, notes
		) VALUES (
			$1, $2, $3,
			COALESCE($4, clock_timestamp()),
			$5,
			$6, $7, $8, $9, $10,
			$11, $12, $13, $14,
			$15, $16, $17
		)
		RETURNING id`
	var id int64
	err := s.db.QueryRowContext(ctx, q,
		in.LSportsFixtureID,
		in.Source,
		in.ObservationKind,
		nullableTime(in.ReceivedAt),
		nullableTime(in.SourcePublishTime),
		nullableIntPtr(in.HomeScore),
		nullableIntPtr(in.AwayScore),
		nullableIntPtr(in.CurrentPeriod),
		nullableIntPtr(in.MatchStatus),
		nullableBool(in.IsInplay),
		nullableIntPtr(in.MarketID),
		nullableString(in.BetName),
		nullableFloat(in.BetPrice),
		nullableFloat(in.BetLine),
		[]byte(in.RawPayload),
		nullableString(in.PayloadSHA256),
		nullableString(in.Notes),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert lsports_inplay_observations: %w", err)
	}
	return id, nil
}

// ListOpenLSportsSubscriptionFixtureIDs returns the LSPORTS fixture IDs for
// every subscription whose status is 'pending' or 'committed_inprogress'.
// Used by cmd/lsports-inplay-capture to discover what to poll each tick.
func (s *PostgresStore) ListOpenLSportsSubscriptionFixtureIDs(ctx context.Context) ([]int64, error) {
	const q = `
		SELECT lsports_fixture_id
		  FROM lsports_subscriptions
		 WHERE status IN ('pending', 'committed_inprogress')
		 ORDER BY subscribed_at DESC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list open lsports subscription fixture ids: %w", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

