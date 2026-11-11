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

func (s *PostgresStore) StartBetfairLivePilotRun(ctx context.Context, input model.BetfairLivePilotRunStart) (int64, error) {
	startedAt := input.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	} else {
		startedAt = startedAt.UTC()
	}
	marketIDs := cleanStrings(input.MarketIDs)
	marketIDsJSON, err := json.Marshal(marketIDs)
	if err != nil {
		return 0, fmt.Errorf("marshal betfair live pilot market ids: %w", err)
	}

	var id int64
	err = s.db.QueryRowContext(ctx, `
        INSERT INTO betfair_live_pilot_runs (
            run_label, stream_id, started_at, status,
            address_redacted, server_name, market_ids_count, market_ids,
            duration_limit_seconds, max_messages
        ) VALUES (
            $1, $2, $3, 'running',
            $4, $5, $6, $7::jsonb,
            $8, $9
        )
        RETURNING id
    `,
		strings.TrimSpace(input.RunLabel),
		strings.TrimSpace(input.StreamID),
		startedAt,
		strings.TrimSpace(input.AddressRedacted),
		strings.TrimSpace(input.ServerName),
		len(marketIDs),
		string(marketIDsJSON),
		nullableIntPtr(input.DurationLimitSeconds),
		nullableIntPtr(input.MaxMessages),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("start betfair live pilot run telemetry: %w", err)
	}
	return id, nil
}

func (s *PostgresStore) FinishBetfairLivePilotRun(ctx context.Context, runID int64, input model.BetfairLivePilotRunFinish) error {
	if runID <= 0 {
		return fmt.Errorf("finish betfair live pilot run telemetry: run id must be positive")
	}
	finishedAt := input.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = time.Now().UTC()
	} else {
		finishedAt = finishedAt.UTC()
	}
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = model.BetfairLivePilotRunStatusFailed
	}

	result, err := s.db.ExecContext(ctx, `
        UPDATE betfair_live_pilot_runs
        SET
            finished_at = $2,
            status = $3,
            stop_reason = $4,
            failure_reason = $5,
            messages_read = $6,
            market_change_messages = $7,
            heartbeat_messages = $8,
            status_messages = $9,
            raw_messages_seen = $10,
            raw_messages_inserted = $11,
            candidates_emitted = $12,
            candidates_inserted = $13,
            candidates_unchanged = $14,
            candidates_skipped = $15,
            markets_seen = $16,
            last_clk = $17,
            last_initial_clk = $18,
            last_message_time = $19,
            heartbeat_state = $20,
            exit_code = $21,
            elapsed_ms = $22,
            updated_at = now()
        WHERE id = $1
    `,
		runID,
		finishedAt,
		status,
		strings.TrimSpace(input.StopReason),
		strings.TrimSpace(input.FailureReason),
		input.MessagesRead,
		input.MarketChangeMessages,
		input.HeartbeatMessages,
		input.StatusMessages,
		input.RawMessagesSeen,
		input.RawMessagesInserted,
		input.CandidatesEmitted,
		input.CandidatesInserted,
		input.CandidatesUnchanged,
		input.CandidatesSkipped,
		input.MarketsSeen,
		strings.TrimSpace(input.LastClock),
		strings.TrimSpace(input.LastInitialClock),
		nullableTime(input.LastMessageTime),
		strings.TrimSpace(input.HeartbeatState),
		input.ExitCode,
		input.ElapsedMS,
	)
	if err != nil {
		return fmt.Errorf("finish betfair live pilot run telemetry %d: %w", runID, err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("finish betfair live pilot run telemetry %d: run not found", runID)
	}
	return nil
}

func (s *PostgresStore) GetBetfairLivePilotRun(ctx context.Context, runID int64) (model.BetfairLivePilotRun, error) {
	var run model.BetfairLivePilotRun
	if runID <= 0 {
		return run, fmt.Errorf("get betfair live pilot run: run id must be positive")
	}
	row := s.db.QueryRowContext(ctx, betfairLivePilotRunSelectSQL()+` WHERE id = $1`, runID)
	run, err := scanBetfairLivePilotRun(row)
	if err != nil {
		return run, fmt.Errorf("get betfair live pilot run %d: %w", runID, err)
	}
	return run, nil
}

func (s *PostgresStore) ListBetfairLivePilotRuns(ctx context.Context, limit int) ([]model.BetfairLivePilotRun, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, betfairLivePilotRunSelectSQL()+` ORDER BY started_at DESC, id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list betfair live pilot runs: %w", err)
	}
	defer rows.Close()

	runs := []model.BetfairLivePilotRun{}
	for rows.Next() {
		run, err := scanBetfairLivePilotRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan betfair live pilot run: %w", err)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate betfair live pilot runs: %w", err)
	}
	return runs, nil
}

func (s *PostgresStore) GetBetfairLivePilotPostflight(ctx context.Context, runID int64) (model.BetfairLivePilotPostflight, error) {
	var report model.BetfairLivePilotPostflight
	if runID <= 0 {
		return report, fmt.Errorf("get betfair live pilot postflight: run id must be positive")
	}

	row := s.db.QueryRowContext(ctx, betfairLivePilotRunSelectSQL()+`
        WHERE id = $1
    `, runID)
	run, err := scanBetfairLivePilotRun(row)
	if err != nil {
		return report, fmt.Errorf("get betfair live pilot postflight run %d: %w", runID, err)
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
        FROM betfair_live_pilot_run_summary
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
		return report, fmt.Errorf("get betfair live pilot postflight summary %d: %w", runID, err)
	}

	readinessCounts, err := s.betfairPilotReadinessCounts(ctx, run.MarketIDs)
	if err != nil {
		return report, err
	}
	report.ReadinessStateCounts = readinessCounts
	report.Warnings = betfairPilotPostflightWarnings(report)
	return report, nil
}

func (s *PostgresStore) betfairPilotReadinessCounts(ctx context.Context, marketIDs []string) ([]model.BetfairLivePilotReadinessCount, error) {
	marketIDsJSON, err := json.Marshal(cleanStrings(marketIDs))
	if err != nil {
		return nil, fmt.Errorf("marshal betfair live pilot postflight market ids: %w", err)
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
		return nil, fmt.Errorf("query betfair live pilot readiness counts: %w", err)
	}
	defer rows.Close()

	counts := []model.BetfairLivePilotReadinessCount{}
	for rows.Next() {
		var count model.BetfairLivePilotReadinessCount
		if err := rows.Scan(&count.ReadinessState, &count.RowCount); err != nil {
			return nil, fmt.Errorf("scan betfair live pilot readiness count: %w", err)
		}
		counts = append(counts, count)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate betfair live pilot readiness counts: %w", err)
	}
	return counts, nil
}

func betfairPilotPostflightWarnings(report model.BetfairLivePilotPostflight) []string {
	warnings := []string{}
	if report.Run.Status == model.BetfairLivePilotRunStatusFailed || report.Run.Status == model.BetfairLivePilotRunStatusCanceled {
		warnings = append(warnings, "pilot run ended "+report.Run.Status)
	}
	if report.LinkedRawMessageCount == 0 {
		warnings = append(warnings, "no linked raw stream messages found")
	}
	if report.Run.CandidatesEmitted == 0 {
		warnings = append(warnings, "no exchange quote candidates emitted")
	}
	if report.LatestQuoteCount == 0 {
		warnings = append(warnings, "no latest Betfair exchange quotes found for pilot market scope")
	}
	if report.MappedQuoteCount == 0 {
		warnings = append(warnings, "no confirmed mapped Betfair exchange quotes found for pilot market scope")
	}
	if report.ReadyRunnerBookCount == 0 {
		warnings = append(warnings, "no ready Betfair runner books found for pilot market scope")
	}
	if report.ReadyComparisonCount == 0 {
		warnings = append(warnings, "no ready sportsbook-vs-Betfair comparison rows found for pilot market scope")
	}
	return warnings
}

func betfairLivePilotRunSelectSQL() string {
	return `
        SELECT
            id,
            run_label,
            stream_id,
            started_at,
            finished_at,
            status,
            stop_reason,
            failure_reason,
            address_redacted,
            server_name,
            market_ids_count,
            market_ids,
            duration_limit_seconds,
            max_messages,
            messages_read,
            market_change_messages,
            heartbeat_messages,
            status_messages,
            raw_messages_seen,
            raw_messages_inserted,
            candidates_emitted,
            candidates_inserted,
            candidates_unchanged,
            candidates_skipped,
            markets_seen,
            last_clk,
            last_initial_clk,
            last_message_time,
            heartbeat_state,
            exit_code,
            elapsed_ms,
            created_at,
            updated_at
        FROM betfair_live_pilot_runs
    `
}

type betfairLivePilotRunScanner interface {
	Scan(dest ...any) error
}

func scanBetfairLivePilotRun(scanner betfairLivePilotRunScanner) (model.BetfairLivePilotRun, error) {
	var run model.BetfairLivePilotRun
	var finishedAt sql.NullTime
	var marketIDsRaw []byte
	var durationLimit sql.NullInt64
	var maxMessages sql.NullInt64
	var lastMessageTime sql.NullTime
	var exitCode sql.NullInt64
	var elapsedMS sql.NullInt64

	err := scanner.Scan(
		&run.ID,
		&run.RunLabel,
		&run.StreamID,
		&run.StartedAt,
		&finishedAt,
		&run.Status,
		&run.StopReason,
		&run.FailureReason,
		&run.AddressRedacted,
		&run.ServerName,
		&run.MarketIDsCount,
		&marketIDsRaw,
		&durationLimit,
		&maxMessages,
		&run.MessagesRead,
		&run.MarketChangeMessages,
		&run.HeartbeatMessages,
		&run.StatusMessages,
		&run.RawMessagesSeen,
		&run.RawMessagesInserted,
		&run.CandidatesEmitted,
		&run.CandidatesInserted,
		&run.CandidatesUnchanged,
		&run.CandidatesSkipped,
		&run.MarketsSeen,
		&run.LastClock,
		&run.LastInitialClock,
		&lastMessageTime,
		&run.HeartbeatState,
		&exitCode,
		&elapsedMS,
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
			return run, fmt.Errorf("decode market ids: %w", err)
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
	if exitCode.Valid {
		v := int(exitCode.Int64)
		run.ExitCode = &v
	}
	if elapsedMS.Valid {
		v := elapsedMS.Int64
		run.ElapsedMS = &v
	}
	run.StartedAt = run.StartedAt.UTC()
	run.CreatedAt = run.CreatedAt.UTC()
	run.UpdatedAt = run.UpdatedAt.UTC()
	return run, nil
}

func cleanStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func nullableIntPtr(v *int) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*v), Valid: true}
}
