package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// LatencyDetectStats summarizes how many decisive events were inserted by
// DetectDecisiveEvents on this invocation.
type LatencyDetectStats struct {
	Inserted        int64
	ObservationsSeen int64
}

// DetectDecisiveEvents scans lsports_inplay_observations (livescore kind) for
// the given fixture, finds consecutive observations whose home_score /
// away_score / current_period differ, and inserts one row per transition
// into inplay_decisive_events. Idempotent: re-runs skip events already linked
// to an observation via source_observation_id.
//
// Sport-agnostic by design: it works whenever the upstream parser has
// populated home_score / away_score / current_period in the observation row.
//
// dedupWindow: when > 0, drops candidate transitions that fall within this
// duration of a previous transition for the same fixture (either in this
// batch's candidates or already-inserted events). Default 5 seconds catches
// data-revision flickers (LSPORTS occasionally emits the same logical event
// 2-3 times with brief reverts within 1-3 seconds — observed in Linette and
// Muchova matches on 2026-05-29). For baseball multi-run plays use a smaller
// value (2s) so legitimate fast successive runs are preserved.
func (s *PostgresStore) DetectDecisiveEvents(ctx context.Context, fixtureID int64, dedupWindow time.Duration) (LatencyDetectStats, error) {
	dedupSec := int(dedupWindow.Seconds())
	if dedupSec < 0 {
		dedupSec = 0
	}

	const q = `
		WITH ordered AS (
			SELECT
				id,
				lsports_fixture_id,
				received_at,
				source_publish_time,
				home_score,
				away_score,
				current_period,
				raw_payload,
				LAG(home_score)      OVER w AS prev_home,
				LAG(away_score)      OVER w AS prev_away,
				LAG(current_period)  OVER w AS prev_period
			FROM lsports_inplay_observations
			WHERE observation_kind = 'livescore'
			  AND lsports_fixture_id = $1
			WINDOW w AS (PARTITION BY lsports_fixture_id ORDER BY received_at)
		),
		deltas AS (
			SELECT *,
				CASE
					WHEN prev_period IS NOT NULL
					     AND current_period IS DISTINCT FROM prev_period
					     THEN 'period_change'
					WHEN home_score IS DISTINCT FROM prev_home
					     OR away_score IS DISTINCT FROM prev_away
					     THEN 'score_change'
					ELSE NULL
				END AS detected_kind,
				CASE
					WHEN home_score > COALESCE(prev_home, -1) THEN 'home'
					WHEN away_score > COALESCE(prev_away, -1) THEN 'away'
					ELSE 'neither'
				END AS side
			FROM ordered
			WHERE prev_home IS NOT NULL OR prev_away IS NOT NULL OR prev_period IS NOT NULL
		)
		INSERT INTO inplay_decisive_events (
			lsports_fixture_id, event_kind, event_at, source_publish_at,
			source_observation_id,
			home_score_before, away_score_before,
			home_score_after,  away_score_after,
			benefiting_side, raw_payload
		)
		SELECT
			lsports_fixture_id,
			CASE detected_kind WHEN 'period_change' THEN 'period_change'
			                   ELSE 'other' END,
			received_at,
			source_publish_time,
			id,
			prev_home, prev_away,
			home_score, away_score,
			side,
			raw_payload
		FROM deltas d1
		WHERE detected_kind IS NOT NULL
		  AND NOT EXISTS (
			SELECT 1 FROM inplay_decisive_events e
			WHERE e.source_observation_id = d1.id
		  )
		  -- Dedup against EARLIER candidate transitions in this same batch
		  -- that fall within dedupWindow seconds.
		  AND ($2::int = 0 OR NOT EXISTS (
			SELECT 1 FROM deltas d2
			WHERE d2.lsports_fixture_id = d1.lsports_fixture_id
			  AND d2.detected_kind IS NOT NULL
			  AND d2.received_at < d1.received_at
			  AND d1.received_at - d2.received_at < make_interval(secs => $2)
		  ))
		  -- Dedup against ALREADY-INSERTED events for this fixture within
		  -- dedupWindow seconds. Prevents double-counting on re-runs.
		  AND ($2::int = 0 OR NOT EXISTS (
			SELECT 1 FROM inplay_decisive_events e
			WHERE e.lsports_fixture_id = d1.lsports_fixture_id
			  AND e.event_at < d1.received_at
			  AND d1.received_at - e.event_at < make_interval(secs => $2)
		  ))`
	res, err := s.db.ExecContext(ctx, q, fixtureID, dedupSec)
	if err != nil {
		return LatencyDetectStats{}, fmt.Errorf("detect decisive events: %w", err)
	}
	n, _ := res.RowsAffected()

	var seen int64
	_ = s.db.QueryRowContext(ctx,
		"SELECT count(*) FROM lsports_inplay_observations WHERE lsports_fixture_id=$1 AND observation_kind='livescore'",
		fixtureID,
	).Scan(&seen)
	return LatencyDetectStats{Inserted: n, ObservationsSeen: seen}, nil
}

// LatencyComputeStats summarizes how many latency rows were inserted.
type LatencyComputeStats struct {
	Inserted     int64
	EventsSeen   int64
	NoReaction   int64
}

// ComputeLatencyObservations joins decisive events with subsequent
// price-changed PM observations and populates latency_observations.
// Idempotent. Uses local receive-time as the canonical Δt (per
// memory/latency_measurement_thesis.md: receive_at is the primary method;
// source_publish_time as the auxiliary).
func (s *PostgresStore) ComputeLatencyObservations(ctx context.Context, fixtureID int64, searchWindow time.Duration) (LatencyComputeStats, error) {
	windowSec := int(searchWindow.Seconds())
	if windowSec <= 0 {
		windowSec = 300
	}

	// Run both methods in one transaction so the two perspectives stay in
	// lockstep. method = 'local_receive_time' is canonical (per
	// memory/latency_measurement_thesis.md: receive_at primary). The
	// 'source_publish_time' run uses LSPORTS ServerTimestamp and PM
	// orderbook timestamps as the time axis — sensitive to clock skew but
	// useful as a cross-check.
	const qReceive = `
		INSERT INTO latency_observations (
			inplay_decisive_event_id, method, polymarket_token_id,
			lsports_event_at, pm_first_price_change_at,
			pm_price_before, pm_price_after, lag_seconds,
			search_window_seconds, no_change_reason
		)
		SELECT
			de.id, 'local_receive_time', pm_first.polymarket_token_id,
			de.event_at, pm_first.received_at,
			pm_prev.best_ask, pm_first.best_ask,
			EXTRACT(EPOCH FROM (pm_first.received_at - de.event_at))::numeric(10,3),
			$2, NULL
		FROM inplay_decisive_events de
		JOIN LATERAL (
			SELECT DISTINCT ON (p.polymarket_token_id)
				p.polymarket_token_id, p.received_at, p.best_ask
			FROM polymarket_latency_observations p
			WHERE p.lsports_fixture_id = de.lsports_fixture_id
			  AND p.received_at > de.event_at
			  AND p.received_at < de.event_at + make_interval(secs => $2)
			  AND p.price_changed_from_prior = TRUE
			ORDER BY p.polymarket_token_id, p.received_at ASC
		) pm_first ON TRUE
		LEFT JOIN LATERAL (
			SELECT p.best_ask FROM polymarket_latency_observations p
			WHERE p.lsports_fixture_id = de.lsports_fixture_id
			  AND p.polymarket_token_id = pm_first.polymarket_token_id
			  AND p.received_at <= de.event_at
			ORDER BY p.received_at DESC LIMIT 1
		) pm_prev ON TRUE
		WHERE de.lsports_fixture_id = $1
		  AND NOT EXISTS (
			SELECT 1 FROM latency_observations lo
			WHERE lo.inplay_decisive_event_id = de.id
			  AND lo.method = 'local_receive_time'
			  AND lo.polymarket_token_id = pm_first.polymarket_token_id
		  )`

	// Same query against source_publish_time. We only consider rows where
	// BOTH sides have a non-null publish time so the subtraction is meaningful.
	const qPublish = `
		INSERT INTO latency_observations (
			inplay_decisive_event_id, method, polymarket_token_id,
			lsports_event_at, pm_first_price_change_at,
			pm_price_before, pm_price_after, lag_seconds,
			search_window_seconds, no_change_reason
		)
		SELECT
			de.id, 'source_publish_time', pm_first.polymarket_token_id,
			de.source_publish_at, pm_first.source_publish_time,
			pm_prev.best_ask, pm_first.best_ask,
			EXTRACT(EPOCH FROM (pm_first.source_publish_time - de.source_publish_at))::numeric(10,3),
			$2, NULL
		FROM inplay_decisive_events de
		JOIN LATERAL (
			SELECT DISTINCT ON (p.polymarket_token_id)
				p.polymarket_token_id, p.source_publish_time, p.best_ask
			FROM polymarket_latency_observations p
			WHERE p.lsports_fixture_id = de.lsports_fixture_id
			  AND p.source_publish_time IS NOT NULL
			  AND p.source_publish_time > de.source_publish_at
			  AND p.source_publish_time < de.source_publish_at + make_interval(secs => $2)
			  AND p.price_changed_from_prior = TRUE
			ORDER BY p.polymarket_token_id, p.source_publish_time ASC
		) pm_first ON TRUE
		LEFT JOIN LATERAL (
			SELECT p.best_ask FROM polymarket_latency_observations p
			WHERE p.lsports_fixture_id = de.lsports_fixture_id
			  AND p.polymarket_token_id = pm_first.polymarket_token_id
			  AND p.source_publish_time <= de.source_publish_at
			ORDER BY p.source_publish_time DESC LIMIT 1
		) pm_prev ON TRUE
		WHERE de.lsports_fixture_id = $1
		  AND de.source_publish_at IS NOT NULL
		  AND NOT EXISTS (
			SELECT 1 FROM latency_observations lo
			WHERE lo.inplay_decisive_event_id = de.id
			  AND lo.method = 'source_publish_time'
			  AND lo.polymarket_token_id = pm_first.polymarket_token_id
		  )`

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return LatencyComputeStats{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, qReceive, fixtureID, windowSec)
	if err != nil {
		return LatencyComputeStats{}, fmt.Errorf("compute latency (local_receive_time): %w", err)
	}
	nRecv, _ := res.RowsAffected()

	res2, err := tx.ExecContext(ctx, qPublish, fixtureID, windowSec)
	if err != nil {
		return LatencyComputeStats{}, fmt.Errorf("compute latency (source_publish_time): %w", err)
	}
	nPub, _ := res2.RowsAffected()

	if err := tx.Commit(); err != nil {
		return LatencyComputeStats{}, fmt.Errorf("commit: %w", err)
	}
	n := nRecv + nPub

	var events, noReact int64
	_ = s.db.QueryRowContext(ctx,
		"SELECT count(*) FROM inplay_decisive_events WHERE lsports_fixture_id=$1",
		fixtureID,
	).Scan(&events)
	// Events with no latency row at all (no PM reaction within window for any token).
	_ = s.db.QueryRowContext(ctx, `
		SELECT count(*) FROM inplay_decisive_events de
		WHERE de.lsports_fixture_id=$1
		  AND NOT EXISTS (SELECT 1 FROM latency_observations lo WHERE lo.inplay_decisive_event_id=de.id)`,
		fixtureID,
	).Scan(&noReact)

	return LatencyComputeStats{Inserted: n, EventsSeen: events, NoReaction: noReact}, nil
}

// LatencySummaryRow mirrors a row of the latency_summary view.
type LatencySummaryRow struct {
	Method                   string
	ObservationCount         int64
	WithReaction             int64
	WithoutReaction          int64
	MedianLagSec             *float64
	P95LagSec                *float64
	P99LagSec                *float64
	MinLagSec                *float64
	MaxLagSec                *float64
	MeanLagSec               *float64
	StddevLagSec             *float64
}

func (s *PostgresStore) LatencySummary(ctx context.Context) ([]LatencySummaryRow, error) {
	const q = `
		SELECT method, observation_count,
		       observations_with_pm_reaction,
		       observations_without_pm_reaction,
		       median_lag_sec, p95_lag_sec, p99_lag_sec,
		       min_lag_sec, max_lag_sec, mean_lag_sec, stddev_lag_sec
		  FROM latency_summary
		 ORDER BY method`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query latency_summary: %w", err)
	}
	defer rows.Close()
	var out []LatencySummaryRow
	for rows.Next() {
		var r LatencySummaryRow
		if err := rows.Scan(
			&r.Method, &r.ObservationCount,
			&r.WithReaction, &r.WithoutReaction,
			&r.MedianLagSec, &r.P95LagSec, &r.P99LagSec,
			&r.MinLagSec, &r.MaxLagSec, &r.MeanLagSec, &r.StddevLagSec,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// BackfillStats summarizes how many existing observation rows had their
// home_score / away_score / current_period columns populated retroactively
// from raw_payload via the supplied JSONB paths.
type BackfillStats struct {
	Scanned  int64
	Updated  int64
	Unparsed int64
}

// BackfillScoresFromRaw extracts home/away score and current period values
// from raw_payload via operator-supplied JSONB paths, and updates the
// corresponding columns on lsports_inplay_observations. Used after the
// real LSPORTS livescore schema is discovered (via inspect-raw) so that
// historical observations captured before the upstream parser knew the
// schema can still contribute to decisive-event detection.
//
// Paths use Postgres jsonb #> notation as a comma-separated list of keys,
// e.g. "Body,0,Livescore,Scoreboard,HomeScore". An empty path skips that
// column.
func (s *PostgresStore) BackfillScoresFromRaw(ctx context.Context, fixtureID int64, homePath, awayPath, periodPath string) (BackfillStats, error) {
	if homePath == "" && awayPath == "" && periodPath == "" {
		return BackfillStats{}, fmt.Errorf("backfill: at least one of --home-path / --away-path / --period-path is required")
	}

	// Use COALESCE so unspecified paths leave columns unchanged (NOT NULL'd).
	// pgArray converts the path string to a TEXT[] for jsonb's #> operator.
	const q = `
		UPDATE lsports_inplay_observations
		   SET home_score      = COALESCE(
		         CASE WHEN $2 != '' THEN
		           NULLIF(raw_payload #>> string_to_array($2, ','), '')::INTEGER
		         END,
		         home_score
		       ),
		       away_score      = COALESCE(
		         CASE WHEN $3 != '' THEN
		           NULLIF(raw_payload #>> string_to_array($3, ','), '')::INTEGER
		         END,
		         away_score
		       ),
		       current_period  = COALESCE(
		         CASE WHEN $4 != '' THEN
		           NULLIF(raw_payload #>> string_to_array($4, ','), '')::INTEGER
		         END,
		         current_period
		       )
		 WHERE lsports_fixture_id = $1
		   AND observation_kind = 'livescore'
		   AND raw_payload IS NOT NULL`

	res, err := s.db.ExecContext(ctx, q, fixtureID, homePath, awayPath, periodPath)
	if err != nil {
		return BackfillStats{}, fmt.Errorf("backfill scores: %w", err)
	}
	updated, _ := res.RowsAffected()
	var scanned int64
	_ = s.db.QueryRowContext(ctx,
		"SELECT count(*) FROM lsports_inplay_observations WHERE lsports_fixture_id=$1 AND observation_kind='livescore'",
		fixtureID,
	).Scan(&scanned)
	return BackfillStats{Scanned: scanned, Updated: updated}, nil
}

// SourceComparisonRow mirrors a row of lsports_source_comparison.
type SourceComparisonRow struct {
	LSportsFixtureID         int64
	ObservationKind          string
	SnapshotCount            int64
	AmqpCount                int64
	SnapshotAvgPubToRecvSec  *float64
	AmqpAvgPubToRecvSec      *float64
	SnapshotMedPubToRecvSec  *float64
	AmqpMedPubToRecvSec      *float64
}

func (s *PostgresStore) SourceComparison(ctx context.Context, fixtureID int64) ([]SourceComparisonRow, error) {
	var (
		rows interface {
			Scan(...any) error
			Next() bool
			Close() error
			Err() error
		}
		err error
	)
	if fixtureID > 0 {
		rows, err = s.db.QueryContext(ctx, `
			SELECT lsports_fixture_id, observation_kind,
			       snapshot_count, amqp_count,
			       snapshot_avg_publish_to_receive_sec, amqp_avg_publish_to_receive_sec,
			       snapshot_median_publish_to_receive_sec, amqp_median_publish_to_receive_sec
			  FROM lsports_source_comparison
			 WHERE lsports_fixture_id = $1
			 ORDER BY observation_kind`, fixtureID)
	} else {
		rows, err = s.db.QueryContext(ctx, `
			SELECT lsports_fixture_id, observation_kind,
			       snapshot_count, amqp_count,
			       snapshot_avg_publish_to_receive_sec, amqp_avg_publish_to_receive_sec,
			       snapshot_median_publish_to_receive_sec, amqp_median_publish_to_receive_sec
			  FROM lsports_source_comparison
			 ORDER BY lsports_fixture_id, observation_kind`)
	}
	if err != nil {
		return nil, fmt.Errorf("query source_comparison: %w", err)
	}
	defer rows.Close()
	var out []SourceComparisonRow
	for rows.Next() {
		var r SourceComparisonRow
		if err := rows.Scan(
			&r.LSportsFixtureID, &r.ObservationKind,
			&r.SnapshotCount, &r.AmqpCount,
			&r.SnapshotAvgPubToRecvSec, &r.AmqpAvgPubToRecvSec,
			&r.SnapshotMedPubToRecvSec, &r.AmqpMedPubToRecvSec,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// InspectRawObservation returns the most recent raw_payload for the
// fixture+kind, pretty-printed for schema discovery.
func (s *PostgresStore) InspectRawObservation(ctx context.Context, fixtureID int64, kind string) (string, error) {
	const q = `
		SELECT raw_payload
		  FROM lsports_inplay_observations
		 WHERE lsports_fixture_id = $1 AND observation_kind = $2
		 ORDER BY received_at DESC
		 LIMIT 1`
	var raw json.RawMessage
	if err := s.db.QueryRowContext(ctx, q, fixtureID, kind).Scan(&raw); err != nil {
		return "", fmt.Errorf("query raw observation: %w", err)
	}
	var pretty []byte
	pretty, err := json.MarshalIndent(json.RawMessage(raw), "", "  ")
	if err != nil {
		return string(raw), nil
	}
	return string(pretty), nil
}
