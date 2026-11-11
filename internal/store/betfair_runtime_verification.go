package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/your-org/pol-odds-collector/internal/model"
)

func (s *PostgresStore) GetBetfairRuntimeVerificationReport(ctx context.Context, runID int64) (model.BetfairRuntimeVerificationReport, error) {
	var report model.BetfairRuntimeVerificationReport
	if runID <= 0 {
		return report, fmt.Errorf("get betfair runtime verification report: run id must be positive")
	}

	run, err := s.GetBetfairRuntimeRun(ctx, runID)
	if err != nil {
		return report, err
	}
	report.RuntimeRun = model.BetfairRuntimeVerificationRunSummary{
		RunID:               run.ID,
		RunLabel:            run.RunLabel,
		StartedAt:           run.StartedAt,
		StoppedAt:           run.FinishedAt,
		Status:              run.Status,
		SinkType:            run.SinkType,
		MarketIDs:           append([]string(nil), run.MarketIDs...),
		RawMessagesInserted: run.RawMessagesInserted,
		CandidatesInserted:  run.CandidatesInserted,
		SinkErrors:          run.SinkErrors,
		FlushErrors:         run.FlushErrors,
	}

	if report.QuoteCandidate, err = s.betfairRuntimeQuoteCandidateSummary(ctx, runID); err != nil {
		return report, err
	}
	if report.MappedQuote, err = s.betfairRuntimeMappedQuoteSummary(ctx, runID); err != nil {
		return report, err
	}
	if report.Readiness, err = s.betfairRuntimeReadinessVerification(ctx, runID); err != nil {
		return report, err
	}
	report.Classification = classifyBetfairRuntimeVerification(report)
	return report, nil
}

func (s *PostgresStore) betfairRuntimeQuoteCandidateSummary(ctx context.Context, runID int64) (model.BetfairRuntimeQuoteCandidateSummary, error) {
	var summary model.BetfairRuntimeQuoteCandidateSummary
	var minOdds, maxOdds, minSize, maxSize sql.NullFloat64
	err := s.db.QueryRowContext(ctx, `
        WITH r AS (
            SELECT *
            FROM betfair_runtime_runs
            WHERE id = $1
        )
        SELECT
            count(h.id),
            count(DISTINCT h.market_id) FILTER (WHERE h.id IS NOT NULL),
            count(DISTINCT (h.selection_id, h.handicap)) FILTER (WHERE h.id IS NOT NULL),
            coalesce(bool_or(h.exchange_side = 'back') FILTER (WHERE h.id IS NOT NULL), false),
            coalesce(bool_or(h.exchange_side = 'lay') FILTER (WHERE h.id IS NOT NULL), false),
            min(h.price_decimal)::double precision,
            max(h.price_decimal)::double precision,
            min(h.available_size)::double precision,
            max(h.available_size)::double precision
        FROM r
        LEFT JOIN betfair_exchange_quote_history h
          ON h.received_at >= r.started_at
         AND h.received_at <= coalesce(r.finished_at, now()) + interval '5 minutes'
         AND (
             r.market_ids_count = 0
             OR EXISTS (
                 SELECT 1
                 FROM jsonb_array_elements_text(r.market_ids) mids(market_id)
                 WHERE mids.market_id = h.market_id
             )
         )
    `, runID).Scan(
		&summary.CandidateCount,
		&summary.DistinctMarketCount,
		&summary.DistinctRunnerCount,
		&summary.HasBack,
		&summary.HasLay,
		&minOdds,
		&maxOdds,
		&minSize,
		&maxSize,
	)
	if err != nil {
		return summary, fmt.Errorf("query betfair runtime quote candidate summary %d: %w", runID, err)
	}
	summary.MinOdds = floatPtrFromNull(minOdds)
	summary.MaxOdds = floatPtrFromNull(maxOdds)
	summary.MinAvailableSize = floatPtrFromNull(minSize)
	summary.MaxAvailableSize = floatPtrFromNull(maxSize)
	return summary, nil
}

func (s *PostgresStore) betfairRuntimeMappedQuoteSummary(ctx context.Context, runID int64) (model.BetfairRuntimeMappedQuoteSummary, error) {
	summary := model.BetfairRuntimeMappedQuoteSummary{
		CanonicalEventIDs:       []string{},
		CanonicalMarketIDs:      []string{},
		RunnerAliasStatusCounts: map[string]int64{},
	}

	rows, err := s.db.QueryContext(ctx, `
        WITH r AS (
            SELECT *
            FROM betfair_runtime_runs
            WHERE id = $1
        ),
        mapped AS (
            SELECT DISTINCT
                q.canonical_event_id::text AS canonical_event_id,
                CASE
                    WHEN q.line IS NULL THEN q.canonical_event_id::text || ':' || q.canonical_market_family
                    ELSE q.canonical_event_id::text || ':' || q.canonical_market_family || ':' || q.line::text
                END AS canonical_market_id
            FROM r
            JOIN fair_value_ready_betfair_exchange_quotes q
              ON (
                  r.market_ids_count = 0
                  OR EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements_text(r.market_ids) mids(market_id)
                      WHERE mids.market_id = q.betfair_market_id
                  )
              )
        )
        SELECT canonical_event_id, canonical_market_id
        FROM mapped
        ORDER BY canonical_event_id, canonical_market_id
    `, runID)
	if err != nil {
		return summary, fmt.Errorf("query betfair runtime canonical mappings %d: %w", runID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var canonicalEventID, canonicalMarketID string
		if err := rows.Scan(&canonicalEventID, &canonicalMarketID); err != nil {
			return summary, fmt.Errorf("scan betfair runtime canonical mapping: %w", err)
		}
		summary.CanonicalEventIDs = appendUniqueString(summary.CanonicalEventIDs, canonicalEventID)
		summary.CanonicalMarketIDs = appendUniqueString(summary.CanonicalMarketIDs, canonicalMarketID)
	}
	if err := rows.Err(); err != nil {
		return summary, fmt.Errorf("iterate betfair runtime canonical mappings: %w", err)
	}

	err = s.db.QueryRowContext(ctx, `
        WITH r AS (
            SELECT *
            FROM betfair_runtime_runs
            WHERE id = $1
        ),
        mapped AS (
            SELECT DISTINCT q.betfair_market_id, q.selection_id, q.handicap
            FROM r
            JOIN fair_value_ready_betfair_exchange_quotes q
              ON (
                  r.market_ids_count = 0
                  OR EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements_text(r.market_ids) mids(market_id)
                      WHERE mids.market_id = q.betfair_market_id
                  )
              )
        )
        SELECT count(*)
        FROM mapped
    `, runID).Scan(&summary.MappedRunnersCount)
	if err != nil {
		return summary, fmt.Errorf("query betfair runtime mapped runner count %d: %w", runID, err)
	}

	rows, err = s.db.QueryContext(ctx, `
        WITH r AS (
            SELECT *
            FROM betfair_runtime_runs
            WHERE id = $1
        ),
        observed AS (
            SELECT DISTINCT q.market_id AS betfair_market_id, q.selection_id, q.handicap
            FROM r
            JOIN betfair_latest_exchange_quotes_view q
              ON (
                  r.market_ids_count = 0
                  OR EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements_text(r.market_ids) mids(market_id)
                      WHERE mids.market_id = q.market_id
                  )
              )
        ),
        alias_status AS (
            SELECT
                observed.betfair_market_id,
                observed.selection_id,
                observed.handicap,
                coalesce(confirmed.alias_status, latest.alias_status, 'missing') AS alias_status
            FROM observed
            LEFT JOIN betfair_confirmed_runner_aliases confirmed
              ON confirmed.betfair_market_id = observed.betfair_market_id
             AND confirmed.selection_id = observed.selection_id
             AND confirmed.handicap = observed.handicap
            LEFT JOIN LATERAL (
                SELECT a.alias_status
                FROM betfair_runner_aliases a
                WHERE a.betfair_market_id = observed.betfair_market_id
                  AND a.selection_id = observed.selection_id
                  AND a.handicap = observed.handicap
                ORDER BY a.updated_at DESC, a.id DESC
                LIMIT 1
            ) latest ON confirmed.alias_id IS NULL
        )
        SELECT alias_status, count(*)
        FROM alias_status
        GROUP BY alias_status
        ORDER BY alias_status
    `, runID)
	if err != nil {
		return summary, fmt.Errorf("query betfair runtime runner alias statuses %d: %w", runID, err)
	}
	defer rows.Close()

	var observedRunners int64
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return summary, fmt.Errorf("scan betfair runtime runner alias status: %w", err)
		}
		summary.RunnerAliasStatusCounts[status] = count
		observedRunners += count
	}
	if err := rows.Err(); err != nil {
		return summary, fmt.Errorf("iterate betfair runtime runner alias statuses: %w", err)
	}
	if observedRunners > summary.MappedRunnersCount {
		summary.UnmappedRunnersCount = observedRunners - summary.MappedRunnersCount
	}
	sort.Strings(summary.CanonicalEventIDs)
	sort.Strings(summary.CanonicalMarketIDs)
	return summary, nil
}

func (s *PostgresStore) betfairRuntimeReadinessVerification(ctx context.Context, runID int64) (model.BetfairRuntimeReadinessVerification, error) {
	summary := model.BetfairRuntimeReadinessVerification{
		ReadinessStateCounts: map[string]int64{},
		AnomalyReasonCounts:  map[string]int64{},
	}

	rows, err := s.db.QueryContext(ctx, `
        WITH r AS (
            SELECT *
            FROM betfair_runtime_runs
            WHERE id = $1
        )
        SELECT readiness.readiness_state, count(*)
        FROM r
        JOIN betfair_exchange_quote_readiness readiness
          ON (
              r.market_ids_count = 0
              OR EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements_text(r.market_ids) mids(market_id)
                  WHERE mids.market_id = readiness.betfair_market_id
              )
          )
        GROUP BY readiness.readiness_state
        ORDER BY readiness.readiness_state
    `, runID)
	if err != nil {
		return summary, fmt.Errorf("query betfair runtime readiness states %d: %w", runID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return summary, fmt.Errorf("scan betfair runtime readiness state: %w", err)
		}
		summary.ReadinessStateCounts[state] = count
		switch {
		case state == "ready":
			summary.ReadyRowCount += count
		case betfairReadinessStateIsBlocked(state):
			summary.BlockedRowCount += count
		default:
			summary.WarningRowCount += count
		}
	}
	if err := rows.Err(); err != nil {
		return summary, fmt.Errorf("iterate betfair runtime readiness states: %w", err)
	}

	rows, err = s.db.QueryContext(ctx, `
        WITH r AS (
            SELECT *
            FROM betfair_runtime_runs
            WHERE id = $1
        )
        SELECT anomaly.anomaly_type, count(*)
        FROM r
        JOIN data_quality_betfair_exchange_anomalies anomaly
          ON anomaly.betfair_market_id IS NOT NULL
         AND (
              r.market_ids_count = 0
              OR EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements_text(r.market_ids) mids(market_id)
                  WHERE mids.market_id = anomaly.betfair_market_id
              )
         )
        GROUP BY anomaly.anomaly_type
        ORDER BY anomaly.anomaly_type
    `, runID)
	if err != nil {
		return summary, fmt.Errorf("query betfair runtime anomaly counts %d: %w", runID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var reason string
		var count int64
		if err := rows.Scan(&reason, &count); err != nil {
			return summary, fmt.Errorf("scan betfair runtime anomaly count: %w", err)
		}
		summary.AnomalyReasonCounts[reason] = count
		switch reason {
		case "stale_quote":
			summary.StaleQuoteCount = count
		case "low_confidence":
			summary.LowConfidenceCount = count
		case "thin_liquidity":
			summary.ThinLiquidityCount = count
		}
	}
	if err := rows.Err(); err != nil {
		return summary, fmt.Errorf("iterate betfair runtime anomaly counts: %w", err)
	}
	return summary, nil
}

func classifyBetfairRuntimeVerification(report model.BetfairRuntimeVerificationReport) model.BetfairRuntimeVerificationClassification {
	return model.BetfairRuntimeVerificationClassification{
		IngestionPass: report.RuntimeRun.Status == model.BetfairRuntimeRunStatusSuccess &&
			report.RuntimeRun.RawMessagesInserted > 0 &&
			report.RuntimeRun.CandidatesInserted > 0 &&
			report.RuntimeRun.SinkErrors == 0 &&
			report.RuntimeRun.FlushErrors == 0,
		MappingPass: report.MappedQuote.MappedRunnersCount > 0 &&
			report.MappedQuote.UnmappedRunnersCount == 0,
		ReadinessPass: report.Readiness.ReadyRowCount > 0 &&
			report.Readiness.WarningRowCount == 0 &&
			report.Readiness.BlockedRowCount == 0,
		ExecutionAllowed: false,
	}
}

func betfairReadinessStateIsBlocked(state string) bool {
	switch state {
	case "mapping_conflict", "missing_side", "suspended", "needs_review":
		return true
	default:
		return false
	}
}

func floatPtrFromNull(value sql.NullFloat64) *float64 {
	if !value.Valid {
		return nil
	}
	v := value.Float64
	return &v
}

func appendUniqueString(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
