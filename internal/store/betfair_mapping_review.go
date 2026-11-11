package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

const (
	defaultBetfairMapReviewLimit = 50
	maxBetfairMapReviewLimit     = 500
	defaultBetfairMapConfidence  = 1.0
)

const selectBetfairUnmappedEventsSQL = `
        SELECT
            betfair_event_id,
            betfair_event_name,
            quote_history_rows,
            market_count,
            latest_received_at,
            latest_alias_status,
            latest_alias_confidence_score::float8,
            latest_alias_match_reason,
            latest_alias_updated_at
        FROM betfair_unmapped_events
        WHERE ($2 = '' OR betfair_event_id = $2)
          AND ($3 = '' OR latest_alias_status = $3)
        ORDER BY latest_received_at DESC NULLS LAST, betfair_event_id
        LIMIT $1
    `

const selectBetfairUnmappedMarketsSQL = `
        SELECT
            betfair_market_id,
            betfair_event_id,
            betfair_event_name,
            betfair_market_name,
            betfair_market_family,
            quote_history_rows,
            runner_count,
            latest_received_at,
            latest_alias_status,
            latest_alias_market_family,
            latest_alias_confidence_score::float8,
            latest_alias_match_reason,
            latest_alias_updated_at
        FROM betfair_unmapped_markets
        WHERE ($2 = '' OR betfair_event_id = $2)
          AND ($3 = '' OR betfair_market_id = $3)
          AND ($4 = '' OR latest_alias_status = $4)
        ORDER BY latest_received_at DESC NULLS LAST, betfair_market_id
        LIMIT $1
    `

const selectBetfairUnmappedRunnersSQL = `
        SELECT
            betfair_market_id,
            betfair_event_id,
            betfair_event_name,
            betfair_market_name,
            selection_id,
            handicap::float8,
            runner_name,
            quote_history_rows,
            exchange_side_count,
            latest_received_at,
            latest_alias_status,
            latest_alias_outcome_name,
            latest_alias_outcome_side,
            latest_alias_confidence_score::float8,
            latest_alias_match_reason,
            latest_alias_updated_at
        FROM betfair_unmapped_runners
        WHERE ($2 = '' OR betfair_event_id = $2)
          AND ($3 = '' OR betfair_market_id = $3)
          AND ($4 = '' OR latest_alias_status = $4)
        ORDER BY latest_received_at DESC NULLS LAST, betfair_market_id, selection_id, handicap
        LIMIT $1
    `

const selectBetfairMappingConflictsSQL = `
        SELECT
            issue_type,
            severity,
            reason,
            betfair_event_id,
            betfair_market_id,
            selection_id,
            handicap::float8,
            canonical_event_id::text,
            canonical_event_ids::text,
            canonical_event_count,
            observed_at,
            details
        FROM betfair_mapping_conflicts
        WHERE ($2 = '' OR betfair_event_id = $2)
          AND ($3 = '' OR betfair_market_id = $3)
        ORDER BY
            CASE severity
                WHEN 'error' THEN 1
                WHEN 'warning' THEN 2
                ELSE 3
            END,
            observed_at DESC NULLS LAST,
            issue_type
        LIMIT $1
    `

const selectBetfairConfirmedEventAliasesSQL = `
        SELECT
            alias_id,
            betfair_event_id,
            betfair_event_name,
            canonical_event_id::text,
            sport_key,
            league,
            home_team,
            away_team,
            commence_time,
            alias_status,
            confidence_score::float8,
            match_reason,
            reviewer,
            review_reason,
            decided_at,
            alias_updated_at
        FROM betfair_confirmed_event_aliases
        WHERE ($2 = '' OR betfair_event_id = $2)
        ORDER BY alias_updated_at DESC, alias_id DESC
        LIMIT $1
    `

const selectBetfairConfirmedMarketAliasesSQL = `
        SELECT
            alias_id,
            betfair_market_id,
            betfair_event_id,
            canonical_event_id::text,
            sport_key,
            league,
            home_team,
            away_team,
            commence_time,
            betfair_market_type,
            canonical_market_family,
            line::float8,
            alias_status,
            confidence_score::float8,
            match_reason,
            reviewer,
            review_reason,
            decided_at,
            alias_updated_at
        FROM betfair_confirmed_market_aliases
        WHERE ($2 = '' OR betfair_event_id = $2)
          AND ($3 = '' OR betfair_market_id = $3)
        ORDER BY alias_updated_at DESC, alias_id DESC
        LIMIT $1
    `

const selectBetfairConfirmedRunnerAliasesSQL = `
        SELECT
            alias_id,
            betfair_market_id,
            selection_id,
            handicap::float8,
            runner_name,
            canonical_outcome_name,
            canonical_outcome_side,
            alias_status,
            confidence_score::float8,
            match_reason,
            reviewer,
            review_reason,
            decided_at,
            alias_updated_at
        FROM betfair_confirmed_runner_aliases
        WHERE ($2 = '' OR betfair_market_id = $2)
        ORDER BY alias_updated_at DESC, alias_id DESC
        LIMIT $1
    `

const selectConfirmedBetfairEventAliasForUpdateSQL = `
        SELECT id, canonical_event_id::text
        FROM betfair_event_aliases
        WHERE betfair_event_id = $1
          AND alias_status = 'confirmed'
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        FOR UPDATE
    `

const insertBetfairEventAliasSQL = `
        INSERT INTO betfair_event_aliases (
            betfair_event_id,
            betfair_event_name,
            canonical_event_id,
            alias_status,
            confidence_score,
            match_reason,
            reviewer,
            review_reason,
            decided_at,
            updated_at
        ) VALUES (
            $1,
            $2,
            $3::uuid,
            $4,
            $5,
            $6,
            $7,
            $8,
            now(),
            now()
        )
    `

const updateBetfairEventAliasSQL = `
        UPDATE betfair_event_aliases
        SET betfair_event_name = $2,
            canonical_event_id = $3::uuid,
            alias_status = 'confirmed',
            confidence_score = $4,
            match_reason = $5,
            reviewer = $6,
            review_reason = $7,
            decided_at = now(),
            updated_at = now()
        WHERE id = $1
    `

const insertBetfairEventRejectionSQL = `
        INSERT INTO betfair_event_aliases (
            betfair_event_id,
            betfair_event_name,
            alias_status,
            match_reason,
            reviewer,
            review_reason,
            decided_at,
            updated_at
        ) VALUES (
            $1,
            $2,
            'rejected',
            $3,
            $4,
            $5,
            now(),
            now()
        )
    `

const selectConfirmedBetfairMarketAliasForUpdateSQL = `
        SELECT id, canonical_event_id::text, canonical_market_family, line::float8
        FROM betfair_market_aliases
        WHERE betfair_market_id = $1
          AND alias_status = 'confirmed'
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        FOR UPDATE
    `

const insertBetfairMarketAliasSQL = `
        INSERT INTO betfair_market_aliases (
            betfair_market_id,
            betfair_event_id,
            canonical_event_id,
            betfair_market_type,
            canonical_market_family,
            line,
            alias_status,
            confidence_score,
            match_reason,
            reviewer,
            review_reason,
            decided_at,
            updated_at
        ) VALUES (
            $1,
            $2,
            $3::uuid,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            now(),
            now()
        )
    `

const updateBetfairMarketAliasSQL = `
        UPDATE betfair_market_aliases
        SET betfair_event_id = $2,
            canonical_event_id = $3::uuid,
            betfair_market_type = $4,
            canonical_market_family = $5,
            line = $6,
            alias_status = 'confirmed',
            confidence_score = $7,
            match_reason = $8,
            reviewer = $9,
            review_reason = $10,
            decided_at = now(),
            updated_at = now()
        WHERE id = $1
    `

const insertBetfairMarketRejectionSQL = `
        INSERT INTO betfair_market_aliases (
            betfair_market_id,
            betfair_event_id,
            alias_status,
            match_reason,
            reviewer,
            review_reason,
            decided_at,
            updated_at
        ) VALUES (
            $1,
            $2,
            'rejected',
            $3,
            $4,
            $5,
            now(),
            now()
        )
    `

const selectConfirmedBetfairRunnerAliasForUpdateSQL = `
        SELECT id, canonical_outcome_name, canonical_outcome_side
        FROM betfair_runner_aliases
        WHERE betfair_market_id = $1
          AND selection_id = $2
          AND handicap = $3
          AND alias_status = 'confirmed'
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        FOR UPDATE
    `

const insertBetfairRunnerAliasSQL = `
        INSERT INTO betfair_runner_aliases (
            betfair_market_id,
            selection_id,
            handicap,
            runner_name,
            canonical_outcome_name,
            canonical_outcome_side,
            alias_status,
            confidence_score,
            match_reason,
            reviewer,
            review_reason,
            decided_at,
            updated_at
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            now(),
            now()
        )
    `

const updateBetfairRunnerAliasSQL = `
        UPDATE betfair_runner_aliases
        SET runner_name = $4,
            canonical_outcome_name = $5,
            canonical_outcome_side = $6,
            alias_status = 'confirmed',
            confidence_score = $7,
            match_reason = $8,
            reviewer = $9,
            review_reason = $10,
            decided_at = now(),
            updated_at = now()
        WHERE id = $1
          AND betfair_market_id = $2
          AND selection_id = $3
    `

const insertBetfairRunnerRejectionSQL = `
        INSERT INTO betfair_runner_aliases (
            betfair_market_id,
            selection_id,
            handicap,
            runner_name,
            alias_status,
            match_reason,
            reviewer,
            review_reason,
            decided_at,
            updated_at
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            'rejected',
            $5,
            $6,
            $7,
            now(),
            now()
        )
    `

func (s *PostgresStore) ListBetfairUnmappedEvents(ctx context.Context, opts model.BetfairMapListOptions) ([]model.BetfairUnmappedEvent, error) {
	opts = normalizeBetfairMapListOptions(opts)
	rows, err := s.db.QueryContext(ctx, selectBetfairUnmappedEventsSQL, opts.Limit, opts.EventID, opts.Status)
	if err != nil {
		return nil, fmt.Errorf("list unmapped betfair events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.BetfairUnmappedEvent{}
	for rows.Next() {
		item, err := scanBetfairUnmappedEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list unmapped betfair events: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListBetfairUnmappedMarkets(ctx context.Context, opts model.BetfairMapListOptions) ([]model.BetfairUnmappedMarket, error) {
	opts = normalizeBetfairMapListOptions(opts)
	rows, err := s.db.QueryContext(ctx, selectBetfairUnmappedMarketsSQL, opts.Limit, opts.EventID, opts.MarketID, opts.Status)
	if err != nil {
		return nil, fmt.Errorf("list unmapped betfair markets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.BetfairUnmappedMarket{}
	for rows.Next() {
		item, err := scanBetfairUnmappedMarket(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list unmapped betfair markets: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListBetfairUnmappedRunners(ctx context.Context, opts model.BetfairMapListOptions) ([]model.BetfairUnmappedRunner, error) {
	opts = normalizeBetfairMapListOptions(opts)
	rows, err := s.db.QueryContext(ctx, selectBetfairUnmappedRunnersSQL, opts.Limit, opts.EventID, opts.MarketID, opts.Status)
	if err != nil {
		return nil, fmt.Errorf("list unmapped betfair runners: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.BetfairUnmappedRunner{}
	for rows.Next() {
		item, err := scanBetfairUnmappedRunner(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list unmapped betfair runners: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListBetfairMappingConflicts(ctx context.Context, opts model.BetfairMapListOptions) ([]model.BetfairMappingConflict, error) {
	opts = normalizeBetfairMapListOptions(opts)
	rows, err := s.db.QueryContext(ctx, selectBetfairMappingConflictsSQL, opts.Limit, opts.EventID, opts.MarketID)
	if err != nil {
		return nil, fmt.Errorf("list betfair mapping conflicts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.BetfairMappingConflict{}
	for rows.Next() {
		item, err := scanBetfairMappingConflict(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list betfair mapping conflicts: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListConfirmedBetfairEventAliases(ctx context.Context, opts model.BetfairMapListOptions) ([]model.BetfairConfirmedEventAlias, error) {
	opts = normalizeBetfairMapListOptions(opts)
	rows, err := s.db.QueryContext(ctx, selectBetfairConfirmedEventAliasesSQL, opts.Limit, opts.EventID)
	if err != nil {
		return nil, fmt.Errorf("list confirmed betfair event aliases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.BetfairConfirmedEventAlias{}
	for rows.Next() {
		item, err := scanBetfairConfirmedEventAlias(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list confirmed betfair event aliases: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListConfirmedBetfairMarketAliases(ctx context.Context, opts model.BetfairMapListOptions) ([]model.BetfairConfirmedMarketAlias, error) {
	opts = normalizeBetfairMapListOptions(opts)
	rows, err := s.db.QueryContext(ctx, selectBetfairConfirmedMarketAliasesSQL, opts.Limit, opts.EventID, opts.MarketID)
	if err != nil {
		return nil, fmt.Errorf("list confirmed betfair market aliases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.BetfairConfirmedMarketAlias{}
	for rows.Next() {
		item, err := scanBetfairConfirmedMarketAlias(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list confirmed betfair market aliases: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListConfirmedBetfairRunnerAliases(ctx context.Context, opts model.BetfairMapListOptions) ([]model.BetfairConfirmedRunnerAlias, error) {
	opts = normalizeBetfairMapListOptions(opts)
	rows, err := s.db.QueryContext(ctx, selectBetfairConfirmedRunnerAliasesSQL, opts.Limit, opts.MarketID)
	if err != nil {
		return nil, fmt.Errorf("list confirmed betfair runner aliases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.BetfairConfirmedRunnerAlias{}
	for rows.Next() {
		item, err := scanBetfairConfirmedRunnerAlias(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list confirmed betfair runner aliases: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ApproveBetfairEventAlias(ctx context.Context, decision model.BetfairEventAliasDecision) error {
	decision.AliasStatus = "confirmed"
	decision = normalizeBetfairEventDecision(decision)
	if err := validateBetfairEventApproval(decision); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := ensureCanonicalEvent(ctx, tx, decision.CanonicalEventID); err != nil {
		return err
	}
	aliasID, err := confirmedBetfairEventAlias(ctx, tx, decision)
	if err != nil {
		return err
	}
	if aliasID > 0 {
		if _, err := tx.ExecContext(ctx,
			updateBetfairEventAliasSQL,
			aliasID,
			decision.BetfairEventName,
			decision.CanonicalEventID,
			nullableFloat(decision.ConfidenceScore),
			decision.MatchReason,
			decision.Reviewer,
			decision.ReviewReason,
		); err != nil {
			return fmt.Errorf("update confirmed betfair event alias: %w", err)
		}
	} else if _, err := tx.ExecContext(ctx,
		insertBetfairEventAliasSQL,
		decision.BetfairEventID,
		decision.BetfairEventName,
		decision.CanonicalEventID,
		"confirmed",
		nullableFloat(decision.ConfidenceScore),
		decision.MatchReason,
		decision.Reviewer,
		decision.ReviewReason,
	); err != nil {
		return fmt.Errorf("insert confirmed betfair event alias: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit betfair event approval: %w", err)
	}
	return nil
}

func (s *PostgresStore) RejectBetfairEventAlias(ctx context.Context, decision model.BetfairEventAliasDecision) error {
	decision.AliasStatus = "rejected"
	decision = normalizeBetfairEventDecision(decision)
	if err := validateBetfairEventRejection(decision); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := ensureNoConfirmedBetfairEventAlias(ctx, tx, decision.BetfairEventID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		insertBetfairEventRejectionSQL,
		decision.BetfairEventID,
		decision.BetfairEventName,
		decision.MatchReason,
		decision.Reviewer,
		decision.ReviewReason,
	); err != nil {
		return fmt.Errorf("insert rejected betfair event alias: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit betfair event rejection: %w", err)
	}
	return nil
}

func (s *PostgresStore) ApproveBetfairMarketAlias(ctx context.Context, decision model.BetfairMarketAliasDecision) error {
	decision.AliasStatus = "confirmed"
	decision = normalizeBetfairMarketDecision(decision)
	if err := validateBetfairMarketApproval(decision); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := ensureCanonicalEvent(ctx, tx, decision.CanonicalEventID); err != nil {
		return err
	}
	aliasID, err := confirmedBetfairMarketAlias(ctx, tx, decision)
	if err != nil {
		return err
	}
	if aliasID > 0 {
		if _, err := tx.ExecContext(ctx,
			updateBetfairMarketAliasSQL,
			aliasID,
			decision.BetfairEventID,
			decision.CanonicalEventID,
			decision.BetfairMarketType,
			decision.CanonicalMarketFamily,
			nullableFloat(decision.Line),
			nullableFloat(decision.ConfidenceScore),
			decision.MatchReason,
			decision.Reviewer,
			decision.ReviewReason,
		); err != nil {
			return fmt.Errorf("update confirmed betfair market alias: %w", err)
		}
	} else if _, err := tx.ExecContext(ctx,
		insertBetfairMarketAliasSQL,
		decision.BetfairMarketID,
		decision.BetfairEventID,
		decision.CanonicalEventID,
		decision.BetfairMarketType,
		decision.CanonicalMarketFamily,
		nullableFloat(decision.Line),
		"confirmed",
		nullableFloat(decision.ConfidenceScore),
		decision.MatchReason,
		decision.Reviewer,
		decision.ReviewReason,
	); err != nil {
		return fmt.Errorf("insert confirmed betfair market alias: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit betfair market approval: %w", err)
	}
	return nil
}

func (s *PostgresStore) RejectBetfairMarketAlias(ctx context.Context, decision model.BetfairMarketAliasDecision) error {
	decision.AliasStatus = "rejected"
	decision = normalizeBetfairMarketDecision(decision)
	if err := validateBetfairMarketRejection(decision); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := ensureNoConfirmedBetfairMarketAlias(ctx, tx, decision.BetfairMarketID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		insertBetfairMarketRejectionSQL,
		decision.BetfairMarketID,
		decision.BetfairEventID,
		decision.MatchReason,
		decision.Reviewer,
		decision.ReviewReason,
	); err != nil {
		return fmt.Errorf("insert rejected betfair market alias: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit betfair market rejection: %w", err)
	}
	return nil
}

func (s *PostgresStore) ApproveBetfairRunnerAlias(ctx context.Context, decision model.BetfairRunnerAliasDecision) error {
	decision.AliasStatus = "confirmed"
	decision = normalizeBetfairRunnerDecision(decision)
	if err := validateBetfairRunnerApproval(decision); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	aliasID, err := confirmedBetfairRunnerAlias(ctx, tx, decision)
	if err != nil {
		return err
	}
	if aliasID > 0 {
		if _, err := tx.ExecContext(ctx,
			updateBetfairRunnerAliasSQL,
			aliasID,
			decision.BetfairMarketID,
			decision.SelectionID,
			decision.RunnerName,
			decision.CanonicalOutcomeName,
			decision.CanonicalOutcomeSide,
			nullableFloat(decision.ConfidenceScore),
			decision.MatchReason,
			decision.Reviewer,
			decision.ReviewReason,
		); err != nil {
			return fmt.Errorf("update confirmed betfair runner alias: %w", err)
		}
	} else if _, err := tx.ExecContext(ctx,
		insertBetfairRunnerAliasSQL,
		decision.BetfairMarketID,
		decision.SelectionID,
		decision.Handicap,
		decision.RunnerName,
		decision.CanonicalOutcomeName,
		decision.CanonicalOutcomeSide,
		"confirmed",
		nullableFloat(decision.ConfidenceScore),
		decision.MatchReason,
		decision.Reviewer,
		decision.ReviewReason,
	); err != nil {
		return fmt.Errorf("insert confirmed betfair runner alias: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit betfair runner approval: %w", err)
	}
	return nil
}

func (s *PostgresStore) RejectBetfairRunnerAlias(ctx context.Context, decision model.BetfairRunnerAliasDecision) error {
	decision.AliasStatus = "rejected"
	decision = normalizeBetfairRunnerDecision(decision)
	if err := validateBetfairRunnerRejection(decision); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := ensureNoConfirmedBetfairRunnerAlias(ctx, tx, decision); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		insertBetfairRunnerRejectionSQL,
		decision.BetfairMarketID,
		decision.SelectionID,
		decision.Handicap,
		decision.RunnerName,
		decision.MatchReason,
		decision.Reviewer,
		decision.ReviewReason,
	); err != nil {
		return fmt.Errorf("insert rejected betfair runner alias: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit betfair runner rejection: %w", err)
	}
	return nil
}

func normalizeBetfairMapListOptions(opts model.BetfairMapListOptions) model.BetfairMapListOptions {
	opts.EventID = strings.TrimSpace(opts.EventID)
	opts.MarketID = strings.TrimSpace(opts.MarketID)
	opts.Status = strings.ToLower(strings.TrimSpace(opts.Status))
	opts.Limit = boundedBetfairMapReviewLimit(opts.Limit)
	return opts
}

func boundedBetfairMapReviewLimit(limit int) int {
	if limit <= 0 {
		return defaultBetfairMapReviewLimit
	}
	if limit > maxBetfairMapReviewLimit {
		return maxBetfairMapReviewLimit
	}
	return limit
}

func normalizeBetfairEventDecision(decision model.BetfairEventAliasDecision) model.BetfairEventAliasDecision {
	decision.BetfairEventID = strings.TrimSpace(decision.BetfairEventID)
	decision.BetfairEventName = strings.TrimSpace(decision.BetfairEventName)
	decision.CanonicalEventID = strings.TrimSpace(decision.CanonicalEventID)
	decision.AliasStatus = strings.ToLower(strings.TrimSpace(decision.AliasStatus))
	decision.Reviewer = strings.TrimSpace(decision.Reviewer)
	decision.ReviewReason = strings.TrimSpace(decision.ReviewReason)
	decision.MatchReason = firstNonEmpty(decision.MatchReason, "manual_review")
	decision.MatchReason = strings.TrimSpace(decision.MatchReason)
	if decision.AliasStatus == "confirmed" && decision.ConfidenceScore == nil {
		confidence := defaultBetfairMapConfidence
		decision.ConfidenceScore = &confidence
	}
	return decision
}

func normalizeBetfairMarketDecision(decision model.BetfairMarketAliasDecision) model.BetfairMarketAliasDecision {
	decision.BetfairMarketID = strings.TrimSpace(decision.BetfairMarketID)
	decision.BetfairEventID = strings.TrimSpace(decision.BetfairEventID)
	decision.CanonicalEventID = strings.TrimSpace(decision.CanonicalEventID)
	decision.BetfairMarketType = strings.TrimSpace(decision.BetfairMarketType)
	decision.CanonicalMarketFamily = strings.ToLower(strings.TrimSpace(decision.CanonicalMarketFamily))
	decision.AliasStatus = strings.ToLower(strings.TrimSpace(decision.AliasStatus))
	decision.Reviewer = strings.TrimSpace(decision.Reviewer)
	decision.ReviewReason = strings.TrimSpace(decision.ReviewReason)
	decision.MatchReason = firstNonEmpty(decision.MatchReason, "manual_review")
	decision.MatchReason = strings.TrimSpace(decision.MatchReason)
	if decision.AliasStatus == "confirmed" && decision.ConfidenceScore == nil {
		confidence := defaultBetfairMapConfidence
		decision.ConfidenceScore = &confidence
	}
	return decision
}

func normalizeBetfairRunnerDecision(decision model.BetfairRunnerAliasDecision) model.BetfairRunnerAliasDecision {
	decision.BetfairMarketID = strings.TrimSpace(decision.BetfairMarketID)
	decision.RunnerName = strings.TrimSpace(decision.RunnerName)
	decision.CanonicalOutcomeName = strings.TrimSpace(decision.CanonicalOutcomeName)
	decision.CanonicalOutcomeSide = strings.ToLower(strings.TrimSpace(decision.CanonicalOutcomeSide))
	decision.AliasStatus = strings.ToLower(strings.TrimSpace(decision.AliasStatus))
	decision.Reviewer = strings.TrimSpace(decision.Reviewer)
	decision.ReviewReason = strings.TrimSpace(decision.ReviewReason)
	decision.MatchReason = firstNonEmpty(decision.MatchReason, "manual_review")
	decision.MatchReason = strings.TrimSpace(decision.MatchReason)
	if decision.AliasStatus == "confirmed" && decision.ConfidenceScore == nil {
		confidence := defaultBetfairMapConfidence
		decision.ConfidenceScore = &confidence
	}
	return decision
}

func validateBetfairEventApproval(decision model.BetfairEventAliasDecision) error {
	if decision.BetfairEventID == "" {
		return errors.New("betfair event id is required")
	}
	if decision.CanonicalEventID == "" {
		return errors.New("canonical event id is required")
	}
	return validateBetfairReviewMetadata(decision.Reviewer, decision.ReviewReason, decision.ConfidenceScore)
}

func validateBetfairEventRejection(decision model.BetfairEventAliasDecision) error {
	if decision.BetfairEventID == "" {
		return errors.New("betfair event id is required")
	}
	return validateBetfairReviewMetadata(decision.Reviewer, decision.ReviewReason, nil)
}

func validateBetfairMarketApproval(decision model.BetfairMarketAliasDecision) error {
	if decision.BetfairMarketID == "" {
		return errors.New("betfair market id is required")
	}
	if decision.BetfairEventID == "" {
		return errors.New("betfair event id is required")
	}
	if decision.CanonicalEventID == "" {
		return errors.New("canonical event id is required")
	}
	if !validBetfairMarketFamily(decision.CanonicalMarketFamily) {
		return fmt.Errorf("canonical market family %q is invalid", decision.CanonicalMarketFamily)
	}
	return validateBetfairReviewMetadata(decision.Reviewer, decision.ReviewReason, decision.ConfidenceScore)
}

func validateBetfairMarketRejection(decision model.BetfairMarketAliasDecision) error {
	if decision.BetfairMarketID == "" {
		return errors.New("betfair market id is required")
	}
	return validateBetfairReviewMetadata(decision.Reviewer, decision.ReviewReason, nil)
}

func validateBetfairRunnerApproval(decision model.BetfairRunnerAliasDecision) error {
	if decision.BetfairMarketID == "" {
		return errors.New("betfair market id is required")
	}
	if decision.SelectionID == 0 {
		return errors.New("selection id is required")
	}
	if decision.CanonicalOutcomeName == "" {
		return errors.New("canonical outcome name is required")
	}
	if !validBetfairOutcomeSide(decision.CanonicalOutcomeSide) {
		return fmt.Errorf("canonical outcome side %q is invalid", decision.CanonicalOutcomeSide)
	}
	return validateBetfairReviewMetadata(decision.Reviewer, decision.ReviewReason, decision.ConfidenceScore)
}

func validateBetfairRunnerRejection(decision model.BetfairRunnerAliasDecision) error {
	if decision.BetfairMarketID == "" {
		return errors.New("betfair market id is required")
	}
	if decision.SelectionID == 0 {
		return errors.New("selection id is required")
	}
	return validateBetfairReviewMetadata(decision.Reviewer, decision.ReviewReason, nil)
}

func validateBetfairReviewMetadata(reviewer, reason string, confidence *float64) error {
	if strings.TrimSpace(reviewer) == "" {
		return errors.New("reviewer is required")
	}
	if strings.TrimSpace(reason) == "" {
		return errors.New("review reason is required")
	}
	if confidence != nil && (*confidence < 0 || *confidence > 100) {
		return errors.New("confidence must be between 0 and 100")
	}
	return nil
}

func validBetfairMarketFamily(value string) bool {
	switch value {
	case "h2h", "spreads", "totals", "other":
		return true
	default:
		return false
	}
}

func validBetfairOutcomeSide(value string) bool {
	switch value {
	case "home", "away", "draw", "over", "under", "other":
		return true
	default:
		return false
	}
}

func confirmedBetfairEventAlias(ctx context.Context, tx *sql.Tx, decision model.BetfairEventAliasDecision) (int64, error) {
	var aliasID int64
	var canonicalID string
	err := tx.QueryRowContext(ctx, selectConfirmedBetfairEventAliasForUpdateSQL, decision.BetfairEventID).Scan(&aliasID, &canonicalID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("check confirmed betfair event alias: %w", err)
	}
	if canonicalID != decision.CanonicalEventID {
		return 0, fmt.Errorf("betfair event %s already has a confirmed alias to canonical event %s", decision.BetfairEventID, canonicalID)
	}
	return aliasID, nil
}

func ensureNoConfirmedBetfairEventAlias(ctx context.Context, tx *sql.Tx, eventID string) error {
	var aliasID int64
	var canonicalID string
	err := tx.QueryRowContext(ctx, selectConfirmedBetfairEventAliasForUpdateSQL, eventID).Scan(&aliasID, &canonicalID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check confirmed betfair event alias before rejection: %w", err)
	}
	return fmt.Errorf("betfair event %s already has a confirmed alias to canonical event %s", eventID, canonicalID)
}

func confirmedBetfairMarketAlias(ctx context.Context, tx *sql.Tx, decision model.BetfairMarketAliasDecision) (int64, error) {
	var aliasID int64
	var canonicalID, family string
	var line sql.NullFloat64
	err := tx.QueryRowContext(ctx, selectConfirmedBetfairMarketAliasForUpdateSQL, decision.BetfairMarketID).Scan(&aliasID, &canonicalID, &family, &line)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("check confirmed betfair market alias: %w", err)
	}
	if canonicalID != decision.CanonicalEventID {
		return 0, fmt.Errorf("betfair market %s already has a confirmed alias to canonical event %s", decision.BetfairMarketID, canonicalID)
	}
	if family != decision.CanonicalMarketFamily {
		return 0, fmt.Errorf("betfair market %s already has confirmed market family %s", decision.BetfairMarketID, family)
	}
	if !nullableFloatEqual(line, decision.Line) {
		return 0, fmt.Errorf("betfair market %s already has a different confirmed line", decision.BetfairMarketID)
	}
	return aliasID, nil
}

func ensureNoConfirmedBetfairMarketAlias(ctx context.Context, tx *sql.Tx, marketID string) error {
	var aliasID int64
	var canonicalID, family string
	var line sql.NullFloat64
	err := tx.QueryRowContext(ctx, selectConfirmedBetfairMarketAliasForUpdateSQL, marketID).Scan(&aliasID, &canonicalID, &family, &line)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check confirmed betfair market alias before rejection: %w", err)
	}
	return fmt.Errorf("betfair market %s already has a confirmed alias to canonical event %s", marketID, canonicalID)
}

func confirmedBetfairRunnerAlias(ctx context.Context, tx *sql.Tx, decision model.BetfairRunnerAliasDecision) (int64, error) {
	var aliasID int64
	var outcomeName, outcomeSide string
	err := tx.QueryRowContext(ctx,
		selectConfirmedBetfairRunnerAliasForUpdateSQL,
		decision.BetfairMarketID,
		decision.SelectionID,
		decision.Handicap,
	).Scan(&aliasID, &outcomeName, &outcomeSide)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("check confirmed betfair runner alias: %w", err)
	}
	if outcomeName != decision.CanonicalOutcomeName || outcomeSide != decision.CanonicalOutcomeSide {
		return 0, fmt.Errorf("betfair runner %s/%d/%.4f already has confirmed outcome %s/%s", decision.BetfairMarketID, decision.SelectionID, decision.Handicap, outcomeName, outcomeSide)
	}
	return aliasID, nil
}

func ensureNoConfirmedBetfairRunnerAlias(ctx context.Context, tx *sql.Tx, decision model.BetfairRunnerAliasDecision) error {
	var aliasID int64
	var outcomeName, outcomeSide string
	err := tx.QueryRowContext(ctx,
		selectConfirmedBetfairRunnerAliasForUpdateSQL,
		decision.BetfairMarketID,
		decision.SelectionID,
		decision.Handicap,
	).Scan(&aliasID, &outcomeName, &outcomeSide)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check confirmed betfair runner alias before rejection: %w", err)
	}
	return fmt.Errorf("betfair runner %s/%d/%.4f already has confirmed outcome %s/%s", decision.BetfairMarketID, decision.SelectionID, decision.Handicap, outcomeName, outcomeSide)
}

func nullableFloatEqual(value sql.NullFloat64, expected *float64) bool {
	if !value.Valid && expected == nil {
		return true
	}
	if value.Valid != (expected != nil) {
		return false
	}
	return value.Float64 == *expected
}

func scanBetfairUnmappedEvent(row matchReviewScanner) (model.BetfairUnmappedEvent, error) {
	var item model.BetfairUnmappedEvent
	var name, status, reason sql.NullString
	var latestReceived, latestUpdated sql.NullTime
	var confidence sql.NullFloat64
	if err := row.Scan(
		&item.BetfairEventID,
		&name,
		&item.QuoteHistoryRows,
		&item.MarketCount,
		&latestReceived,
		&status,
		&confidence,
		&reason,
		&latestUpdated,
	); err != nil {
		return item, fmt.Errorf("scan unmapped betfair event: %w", err)
	}
	item.BetfairEventName = name.String
	item.LatestAliasStatus = status.String
	item.LatestAliasMatchReason = reason.String
	item.LatestReceivedAt = nullableTimePtr(latestReceived)
	item.LatestAliasUpdatedAt = nullableTimePtr(latestUpdated)
	item.LatestAliasConfidenceScore = nullableFloatPtr(confidence)
	return item, nil
}

func scanBetfairUnmappedMarket(row matchReviewScanner) (model.BetfairUnmappedMarket, error) {
	var item model.BetfairUnmappedMarket
	var eventID, eventName, marketName, marketFamily, status, aliasFamily, reason sql.NullString
	var latestReceived, latestUpdated sql.NullTime
	var confidence sql.NullFloat64
	if err := row.Scan(
		&item.BetfairMarketID,
		&eventID,
		&eventName,
		&marketName,
		&marketFamily,
		&item.QuoteHistoryRows,
		&item.RunnerCount,
		&latestReceived,
		&status,
		&aliasFamily,
		&confidence,
		&reason,
		&latestUpdated,
	); err != nil {
		return item, fmt.Errorf("scan unmapped betfair market: %w", err)
	}
	item.BetfairEventID = eventID.String
	item.BetfairEventName = eventName.String
	item.BetfairMarketName = marketName.String
	item.BetfairMarketFamily = marketFamily.String
	item.LatestAliasStatus = status.String
	item.LatestAliasMarketFamily = aliasFamily.String
	item.LatestAliasMatchReason = reason.String
	item.LatestReceivedAt = nullableTimePtr(latestReceived)
	item.LatestAliasUpdatedAt = nullableTimePtr(latestUpdated)
	item.LatestAliasConfidenceScore = nullableFloatPtr(confidence)
	return item, nil
}

func scanBetfairUnmappedRunner(row matchReviewScanner) (model.BetfairUnmappedRunner, error) {
	var item model.BetfairUnmappedRunner
	var eventID, eventName, marketName, runnerName, status, outcomeName, outcomeSide, reason sql.NullString
	var latestReceived, latestUpdated sql.NullTime
	var confidence sql.NullFloat64
	if err := row.Scan(
		&item.BetfairMarketID,
		&eventID,
		&eventName,
		&marketName,
		&item.SelectionID,
		&item.Handicap,
		&runnerName,
		&item.QuoteHistoryRows,
		&item.ExchangeSideCount,
		&latestReceived,
		&status,
		&outcomeName,
		&outcomeSide,
		&confidence,
		&reason,
		&latestUpdated,
	); err != nil {
		return item, fmt.Errorf("scan unmapped betfair runner: %w", err)
	}
	item.BetfairEventID = eventID.String
	item.BetfairEventName = eventName.String
	item.BetfairMarketName = marketName.String
	item.RunnerName = runnerName.String
	item.LatestAliasStatus = status.String
	item.LatestAliasOutcomeName = outcomeName.String
	item.LatestAliasOutcomeSide = outcomeSide.String
	item.LatestAliasMatchReason = reason.String
	item.LatestReceivedAt = nullableTimePtr(latestReceived)
	item.LatestAliasUpdatedAt = nullableTimePtr(latestUpdated)
	item.LatestAliasConfidenceScore = nullableFloatPtr(confidence)
	return item, nil
}

func scanBetfairMappingConflict(row matchReviewScanner) (model.BetfairMappingConflict, error) {
	var item model.BetfairMappingConflict
	var eventID, marketID, canonicalID, canonicalIDs sql.NullString
	var selectionID sql.NullInt64
	var handicap sql.NullFloat64
	var observedAt sql.NullTime
	var details []byte
	if err := row.Scan(
		&item.IssueType,
		&item.Severity,
		&item.Reason,
		&eventID,
		&marketID,
		&selectionID,
		&handicap,
		&canonicalID,
		&canonicalIDs,
		&item.CanonicalEventCount,
		&observedAt,
		&details,
	); err != nil {
		return item, fmt.Errorf("scan betfair mapping conflict: %w", err)
	}
	item.BetfairEventID = eventID.String
	item.BetfairMarketID = marketID.String
	item.CanonicalEventID = canonicalID.String
	item.CanonicalEventIDs = canonicalIDs.String
	item.ObservedAt = nullableTimePtr(observedAt)
	if selectionID.Valid {
		v := selectionID.Int64
		item.SelectionID = &v
	}
	if handicap.Valid {
		v := handicap.Float64
		item.Handicap = &v
	}
	if len(details) > 0 && json.Valid(details) {
		item.Details = json.RawMessage(details)
	}
	return item, nil
}

func scanBetfairConfirmedEventAlias(row matchReviewScanner) (model.BetfairConfirmedEventAlias, error) {
	var item model.BetfairConfirmedEventAlias
	var name, sportKey, league, home, away, status, reason, reviewer, reviewReason sql.NullString
	var confidence sql.NullFloat64
	var decidedAt sql.NullTime
	if err := row.Scan(
		&item.AliasID,
		&item.BetfairEventID,
		&name,
		&item.CanonicalEventID,
		&sportKey,
		&league,
		&home,
		&away,
		&item.CommenceTime,
		&status,
		&confidence,
		&reason,
		&reviewer,
		&reviewReason,
		&decidedAt,
		&item.AliasUpdatedAt,
	); err != nil {
		return item, fmt.Errorf("scan confirmed betfair event alias: %w", err)
	}
	item.BetfairEventName = name.String
	item.SportKey = sportKey.String
	item.League = league.String
	item.HomeTeam = home.String
	item.AwayTeam = away.String
	item.AliasStatus = status.String
	item.MatchReason = reason.String
	item.Reviewer = reviewer.String
	item.ReviewReason = reviewReason.String
	item.ConfidenceScore = nullableFloatPtr(confidence)
	item.DecidedAt = nullableTimePtr(decidedAt)
	return item, nil
}

func scanBetfairConfirmedMarketAlias(row matchReviewScanner) (model.BetfairConfirmedMarketAlias, error) {
	var item model.BetfairConfirmedMarketAlias
	var eventID, sportKey, league, home, away, marketType, family, status, reason, reviewer, reviewReason sql.NullString
	var line, confidence sql.NullFloat64
	var decidedAt sql.NullTime
	if err := row.Scan(
		&item.AliasID,
		&item.BetfairMarketID,
		&eventID,
		&item.CanonicalEventID,
		&sportKey,
		&league,
		&home,
		&away,
		&item.CommenceTime,
		&marketType,
		&family,
		&line,
		&status,
		&confidence,
		&reason,
		&reviewer,
		&reviewReason,
		&decidedAt,
		&item.AliasUpdatedAt,
	); err != nil {
		return item, fmt.Errorf("scan confirmed betfair market alias: %w", err)
	}
	item.BetfairEventID = eventID.String
	item.SportKey = sportKey.String
	item.League = league.String
	item.HomeTeam = home.String
	item.AwayTeam = away.String
	item.BetfairMarketType = marketType.String
	item.CanonicalMarketFamily = family.String
	item.AliasStatus = status.String
	item.MatchReason = reason.String
	item.Reviewer = reviewer.String
	item.ReviewReason = reviewReason.String
	item.Line = nullableFloatPtr(line)
	item.ConfidenceScore = nullableFloatPtr(confidence)
	item.DecidedAt = nullableTimePtr(decidedAt)
	return item, nil
}

func scanBetfairConfirmedRunnerAlias(row matchReviewScanner) (model.BetfairConfirmedRunnerAlias, error) {
	var item model.BetfairConfirmedRunnerAlias
	var runnerName, outcomeName, outcomeSide, status, reason, reviewer, reviewReason sql.NullString
	var confidence sql.NullFloat64
	var decidedAt sql.NullTime
	if err := row.Scan(
		&item.AliasID,
		&item.BetfairMarketID,
		&item.SelectionID,
		&item.Handicap,
		&runnerName,
		&outcomeName,
		&outcomeSide,
		&status,
		&confidence,
		&reason,
		&reviewer,
		&reviewReason,
		&decidedAt,
		&item.AliasUpdatedAt,
	); err != nil {
		return item, fmt.Errorf("scan confirmed betfair runner alias: %w", err)
	}
	item.RunnerName = runnerName.String
	item.CanonicalOutcomeName = outcomeName.String
	item.CanonicalOutcomeSide = outcomeSide.String
	item.AliasStatus = status.String
	item.MatchReason = reason.String
	item.Reviewer = reviewer.String
	item.ReviewReason = reviewReason.String
	item.ConfidenceScore = nullableFloatPtr(confidence)
	item.DecidedAt = nullableTimePtr(decidedAt)
	return item, nil
}

func nullableFloatPtr(value sql.NullFloat64) *float64 {
	if !value.Valid {
		return nil
	}
	v := value.Float64
	return &v
}

func nullableTimePtr(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	v := value.Time.UTC()
	return &v
}
