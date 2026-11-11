package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

func (s *PostgresStore) StartBetfairRuntimeRun(ctx context.Context, input model.BetfairRuntimeRunStart) (int64, error) {
	startedAt := input.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	} else {
		startedAt = startedAt.UTC()
	}
	marketIDs := cleanStrings(input.MarketIDs)
	marketIDsJSON, err := json.Marshal(marketIDs)
	if err != nil {
		return 0, fmt.Errorf("marshal betfair runtime market ids: %w", err)
	}
	runtimeMode := strings.TrimSpace(input.RuntimeMode)
	if runtimeMode == "" {
		runtimeMode = "fake"
	}

	var id int64
	err = s.db.QueryRowContext(ctx, `
        INSERT INTO betfair_runtime_runs (
            run_label, stream_id, runtime_mode, transport_mode, sink_type,
            started_at, status, market_ids_count, market_ids,
            duration_limit_seconds, max_messages
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, 'running', $7, $8::jsonb,
            $9, $10
        )
        RETURNING id
    `,
		strings.TrimSpace(input.RunLabel),
		strings.TrimSpace(input.StreamID),
		runtimeMode,
		strings.TrimSpace(input.TransportMode),
		strings.TrimSpace(input.SinkType),
		startedAt,
		len(marketIDs),
		string(marketIDsJSON),
		nullableIntPtr(input.DurationLimitSeconds),
		nullableIntPtr(input.MaxMessages),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("start betfair runtime run telemetry: %w", err)
	}
	return id, nil
}

func (s *PostgresStore) FinishBetfairRuntimeRun(ctx context.Context, runID int64, input model.BetfairRuntimeRunFinish) error {
	if runID <= 0 {
		return fmt.Errorf("finish betfair runtime run telemetry: run id must be positive")
	}
	finishedAt := input.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = time.Now().UTC()
	} else {
		finishedAt = finishedAt.UTC()
	}
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = model.BetfairRuntimeRunStatusFailed
	}

	result, err := s.db.ExecContext(ctx, `
        UPDATE betfair_runtime_runs
        SET
            stream_id = $2,
            finished_at = $3,
            status = $4,
            readiness_state = $5,
            stopped_reason = $6,
            failure_reason = $7,
            messages_read = $8,
            market_change_messages = $9,
            heartbeat_messages = $10,
            status_messages = $11,
            raw_messages_seen = $12,
            raw_messages_flushed = $13,
            raw_messages_inserted = $14,
            candidates_emitted = $15,
            candidate_batches_seen = $16,
            candidate_batches_flushed = $17,
            candidates_inserted = $18,
            candidates_unchanged = $19,
            candidates_skipped = $20,
            flush_count = $21,
            flush_errors = $22,
            sink_errors = $23,
            markets_seen = $24,
            reconnect_count = $25,
            last_clk = $26,
            last_initial_clk = $27,
            last_message_time = $28,
            last_candidate_time = $29,
            last_sink_flush_time = $30,
            heartbeat_state = $31,
            elapsed_ms = $32,
            exit_code = $33,
            updated_at = now()
        WHERE id = $1
    `,
		runID,
		strings.TrimSpace(input.StreamID),
		finishedAt,
		status,
		strings.TrimSpace(input.ReadinessState),
		strings.TrimSpace(input.StoppedReason),
		strings.TrimSpace(input.FailureReason),
		input.MessagesRead,
		input.MarketChangeMessages,
		input.HeartbeatMessages,
		input.StatusMessages,
		input.RawMessagesSeen,
		input.RawMessagesFlushed,
		input.RawMessagesInserted,
		input.CandidatesEmitted,
		input.CandidateBatchesSeen,
		input.CandidateBatchesFlushed,
		input.CandidatesInserted,
		input.CandidatesUnchanged,
		input.CandidatesSkipped,
		input.FlushCount,
		input.FlushErrors,
		input.SinkErrors,
		input.MarketsSeen,
		input.ReconnectCount,
		strings.TrimSpace(input.LastClock),
		strings.TrimSpace(input.LastInitialClock),
		nullableTime(input.LastMessageTime),
		nullableTime(input.LastCandidateTime),
		nullableTime(input.LastSinkFlushTime),
		strings.TrimSpace(input.HeartbeatState),
		input.ElapsedMS,
		input.ExitCode,
	)
	if err != nil {
		return fmt.Errorf("finish betfair runtime run telemetry %d: %w", runID, err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("finish betfair runtime run telemetry %d: run not found", runID)
	}
	return nil
}

func (s *PostgresStore) GetBetfairRuntimeRun(ctx context.Context, runID int64) (model.BetfairRuntimeRun, error) {
	var run model.BetfairRuntimeRun
	if runID <= 0 {
		return run, fmt.Errorf("get betfair runtime run: run id must be positive")
	}
	row := s.db.QueryRowContext(ctx, betfairRuntimeRunSelectSQL()+` WHERE id = $1`, runID)
	run, err := scanBetfairRuntimeRun(row)
	if err != nil {
		return run, fmt.Errorf("get betfair runtime run %d: %w", runID, err)
	}
	return run, nil
}

func (s *PostgresStore) ListBetfairRuntimeRuns(ctx context.Context, limit int) ([]model.BetfairRuntimeRun, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, betfairRuntimeRunSelectSQL()+` ORDER BY started_at DESC, id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list betfair runtime runs: %w", err)
	}
	defer rows.Close()

	runs := []model.BetfairRuntimeRun{}
	for rows.Next() {
		run, err := scanBetfairRuntimeRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan betfair runtime run: %w", err)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate betfair runtime runs: %w", err)
	}
	return runs, nil
}

func (s *PostgresStore) GetBetfairRuntimePostflight(ctx context.Context, runID int64) (model.BetfairRuntimePostflight, error) {
	var report model.BetfairRuntimePostflight
	if runID <= 0 {
		return report, fmt.Errorf("get betfair runtime postflight: run id must be positive")
	}

	row := s.db.QueryRowContext(ctx, betfairRuntimeRunSelectSQL()+`
        WHERE id = $1
    `, runID)
	run, err := scanBetfairRuntimeRun(row)
	if err != nil {
		return report, fmt.Errorf("get betfair runtime postflight run %d: %w", runID, err)
	}
	report.Run = run

	err = s.db.QueryRowContext(ctx, `
        SELECT
            linked_raw_message_count,
            linked_quote_history_rows,
            latest_quote_count,
            mapped_quote_count,
            ready_runner_book_count,
            ready_comparison_count
        FROM betfair_runtime_run_summary
        WHERE id = $1
    `, runID).Scan(
		&report.LinkedRawMessageCount,
		&report.QuoteHistoryRows,
		&report.LatestQuoteCount,
		&report.MappedQuoteCount,
		&report.ReadyRunnerBookCount,
		&report.ReadyComparisonCount,
	)
	if err != nil {
		return report, fmt.Errorf("get betfair runtime postflight summary %d: %w", runID, err)
	}

	readinessCounts, err := s.betfairRuntimeReadinessCounts(ctx, run.MarketIDs)
	if err != nil {
		return report, err
	}
	report.ReadinessStateCounts = readinessCounts
	report.Warnings = betfairRuntimePostflightWarnings(report)
	return report, nil
}

func (s *PostgresStore) betfairRuntimeReadinessCounts(ctx context.Context, marketIDs []string) ([]model.BetfairRuntimeReadinessCount, error) {
	marketIDsJSON, err := json.Marshal(cleanStrings(marketIDs))
	if err != nil {
		return nil, fmt.Errorf("marshal betfair runtime postflight market ids: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `
        SELECT readiness_state, count(*) AS row_count
        FROM betfair_exchange_quote_readiness r
        WHERE jsonb_array_length($1::jsonb) = 0
           OR EXISTS (
               SELECT 1
               FROM jsonb_array_elements_text($1::jsonb) mids(market_id)
               WHERE mids.market_id = r.betfair_market_id
           )
        GROUP BY readiness_state
        ORDER BY readiness_state
    `, string(marketIDsJSON))
	if err != nil {
		return nil, fmt.Errorf("query betfair runtime readiness counts: %w", err)
	}
	defer rows.Close()

	counts := []model.BetfairRuntimeReadinessCount{}
	for rows.Next() {
		var count model.BetfairRuntimeReadinessCount
		if err := rows.Scan(&count.ReadinessState, &count.RowCount); err != nil {
			return nil, fmt.Errorf("scan betfair runtime readiness count: %w", err)
		}
		counts = append(counts, count)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate betfair runtime readiness counts: %w", err)
	}
	return counts, nil
}

func betfairRuntimePostflightWarnings(report model.BetfairRuntimePostflight) []string {
	warnings := []string{}
	if report.Run.Status == model.BetfairRuntimeRunStatusFailed || report.Run.Status == model.BetfairRuntimeRunStatusCanceled {
		warnings = append(warnings, "runtime run ended "+report.Run.Status)
	}
	if report.Run.SinkErrors > 0 || report.Run.FlushErrors > 0 {
		warnings = append(warnings, "runtime run reported sink or flush errors")
	}
	if report.LinkedRawMessageCount == 0 {
		warnings = append(warnings, "no linked raw stream messages found")
	}
	if report.Run.CandidatesEmitted == 0 {
		warnings = append(warnings, "no exchange quote candidates emitted")
	}
	if report.LatestQuoteCount == 0 {
		warnings = append(warnings, "no latest Betfair exchange quotes found for runtime market scope")
	}
	if report.MappedQuoteCount == 0 {
		warnings = append(warnings, "no confirmed mapped Betfair exchange quotes found for runtime market scope")
	}
	if report.ReadyRunnerBookCount == 0 {
		warnings = append(warnings, "no ready Betfair runner books found for runtime market scope")
	}
	if report.ReadyComparisonCount == 0 {
		warnings = append(warnings, "no ready sportsbook-vs-Betfair comparison rows found for runtime market scope")
	}
	return warnings
}

func betfairRuntimeRunSelectSQL() string {
	return `
        SELECT
            id,
            run_label,
            stream_id,
            runtime_mode,
            transport_mode,
            sink_type,
            started_at,
            finished_at,
            status,
            readiness_state,
            stopped_reason,
            failure_reason,
            market_ids_count,
            market_ids,
            duration_limit_seconds,
            max_messages,
            messages_read,
            market_change_messages,
            heartbeat_messages,
            status_messages,
            raw_messages_seen,
            raw_messages_flushed,
            raw_messages_inserted,
            candidates_emitted,
            candidate_batches_seen,
            candidate_batches_flushed,
            candidates_inserted,
            candidates_unchanged,
            candidates_skipped,
            flush_count,
            flush_errors,
            sink_errors,
            markets_seen,
            reconnect_count,
            last_clk,
            last_initial_clk,
            last_message_time,
            last_candidate_time,
            last_sink_flush_time,
            heartbeat_state,
            elapsed_ms,
            exit_code,
            created_at,
            updated_at
        FROM betfair_runtime_runs
    `
}

type betfairRuntimeRunScanner interface {
	Scan(dest ...any) error
}

func scanBetfairRuntimeRun(scanner betfairRuntimeRunScanner) (model.BetfairRuntimeRun, error) {
	var run model.BetfairRuntimeRun
	var finishedAt sql.NullTime
	var marketIDsRaw []byte
	var durationLimit sql.NullInt64
	var maxMessages sql.NullInt64
	var lastMessageTime sql.NullTime
	var lastCandidateTime sql.NullTime
	var lastSinkFlushTime sql.NullTime
	var elapsedMS sql.NullInt64
	var exitCode sql.NullInt64

	err := scanner.Scan(
		&run.ID,
		&run.RunLabel,
		&run.StreamID,
		&run.RuntimeMode,
		&run.TransportMode,
		&run.SinkType,
		&run.StartedAt,
		&finishedAt,
		&run.Status,
		&run.ReadinessState,
		&run.StoppedReason,
		&run.FailureReason,
		&run.MarketIDsCount,
		&marketIDsRaw,
		&durationLimit,
		&maxMessages,
		&run.MessagesRead,
		&run.MarketChangeMessages,
		&run.HeartbeatMessages,
		&run.StatusMessages,
		&run.RawMessagesSeen,
		&run.RawMessagesFlushed,
		&run.RawMessagesInserted,
		&run.CandidatesEmitted,
		&run.CandidateBatchesSeen,
		&run.CandidateBatchesFlushed,
		&run.CandidatesInserted,
		&run.CandidatesUnchanged,
		&run.CandidatesSkipped,
		&run.FlushCount,
		&run.FlushErrors,
		&run.SinkErrors,
		&run.MarketsSeen,
		&run.ReconnectCount,
		&run.LastClock,
		&run.LastInitialClock,
		&lastMessageTime,
		&lastCandidateTime,
		&lastSinkFlushTime,
		&run.HeartbeatState,
		&elapsedMS,
		&exitCode,
		&run.CreatedAt,
		&run.UpdatedAt,
	)
	if err != nil {
		return run, err
	}
	if finishedAt.Valid {
		ts := finishedAt.Time.UTC()
		run.FinishedAt = &ts
	}
	if len(marketIDsRaw) > 0 {
		if err := json.Unmarshal(marketIDsRaw, &run.MarketIDs); err != nil {
			return run, fmt.Errorf("decode runtime market ids: %w", err)
		}
	}
	if durationLimit.Valid {
		v := int(durationLimit.Int64)
		run.DurationLimitSeconds = &v
	}
	if maxMessages.Valid {
		v := int(maxMessages.Int64)
		run.MaxMessages = &v
	}
	if lastMessageTime.Valid {
		ts := lastMessageTime.Time.UTC()
		run.LastMessageTime = &ts
	}
	if lastCandidateTime.Valid {
		ts := lastCandidateTime.Time.UTC()
		run.LastCandidateTime = &ts
	}
	if lastSinkFlushTime.Valid {
		ts := lastSinkFlushTime.Time.UTC()
		run.LastSinkFlushTime = &ts
	}
	if elapsedMS.Valid {
		v := elapsedMS.Int64
		run.ElapsedMS = &v
	}
	if exitCode.Valid {
		v := int(exitCode.Int64)
		run.ExitCode = &v
	}
	run.StartedAt = run.StartedAt.UTC()
	run.CreatedAt = run.CreatedAt.UTC()
	run.UpdatedAt = run.UpdatedAt.UTC()
	return run, nil
}
