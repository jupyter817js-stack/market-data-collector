package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/your-org/pol-odds-collector/internal/model"
)

const (
	defaultMatchReviewLimit = 50
	maxMatchReviewLimit     = 500
)

const selectPolymarketMatchReviewsSQL = `
        SELECT
            review_id,
            polymarket_event_id,
            polymarket_market_id,
            condition_id,
            slug,
            title,
            sport_key,
            league,
            raw_start_time,
            match_status,
            confidence_score::float8,
            match_reason,
            candidate_canonical_event_ids::text,
            created_at,
            updated_at
        FROM %s
        ORDER BY created_at DESC, review_id DESC
        LIMIT $1
    `

const selectConfirmedPolymarketAliasesSQL = `
        SELECT
            alias_id,
            polymarket_event_id,
            polymarket_market_id,
            condition_id,
            canonical_event_id::text,
            sport_key,
            home_team,
            away_team,
            commence_time,
            match_status,
            confidence_score::float8,
            match_reason,
            reviewer,
            review_reason,
            decision_source,
            decided_at,
            alias_updated_at
        FROM confirmed_polymarket_event_aliases
        ORDER BY alias_updated_at DESC, alias_id DESC
        LIMIT $1
    `

const selectPolymarketEventExistsSQL = `
        SELECT EXISTS (
            SELECT 1
            FROM polymarket_events
            WHERE polymarket_event_id = $1
        )
    `

const selectCanonicalEventExistsSQL = `
        SELECT EXISTS (
            SELECT 1
            FROM canonical_events
            WHERE id = $1::uuid
        )
    `

const selectPolymarketMarketForDecisionSQL = `
        SELECT polymarket_market_id, condition_id
        FROM polymarket_markets
        WHERE polymarket_event_id = $1
          AND ($2 = '' OR polymarket_market_id = $2)
          AND ($3 = '' OR condition_id = $3)
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
    `

const selectConfirmedAliasConflictsForUpdateSQL = `
        SELECT canonical_event_id::text
        FROM polymarket_event_aliases
        WHERE lower(match_status) = 'confirmed'
          AND (
              polymarket_event_id = $1
              OR ($2 <> '' AND polymarket_market_id = $2)
              OR ($3 <> '' AND condition_id = $3)
          )
        FOR UPDATE
    `

const selectExactConfirmedAliasForUpdateSQL = `
        SELECT id, canonical_event_id::text
        FROM polymarket_event_aliases
        WHERE coalesce(polymarket_event_id, '') = $1
          AND coalesce(polymarket_market_id, '') = $2
          AND coalesce(condition_id, '') = $3
          AND lower(match_status) = 'confirmed'
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        FOR UPDATE
    `

const insertPolymarketConfirmedAliasSQL = `
        INSERT INTO polymarket_event_aliases (
            canonical_event_id,
            polymarket_event_id,
            polymarket_market_id,
            condition_id,
            match_status,
            match_confidence,
            notes,
            updated_at
        ) VALUES (
            $1::uuid,
            $2,
            $3,
            $4,
            'confirmed',
            $5,
            $6,
            now()
        )
    `

const updatePolymarketConfirmedAliasSQL = `
        UPDATE polymarket_event_aliases
        SET canonical_event_id = $2::uuid,
            match_status = 'confirmed',
            match_confidence = $3,
            notes = $4,
            updated_at = now()
        WHERE id = $1
    `

const selectManualEventAliasForUpdateSQL = `
        SELECT canonical_event_id::text
        FROM manual_event_aliases
        WHERE source = $1
          AND source_event_id = $2
          AND source_market_id = $3
          AND source_condition_id = $4
        FOR UPDATE
    `

const upsertManualEventAliasSQL = `
        INSERT INTO manual_event_aliases (
            source,
            source_event_id,
            source_market_id,
            source_condition_id,
            canonical_event_id,
            sport_key,
            reason,
            created_by,
            updated_at
        )
        SELECT
            $1,
            $2,
            $3,
            $4,
            $5::uuid,
            ce.sport_key,
            $6,
            $7,
            now()
        FROM canonical_events ce
        WHERE ce.id = $5::uuid
        ON CONFLICT (source, source_event_id, source_market_id, source_condition_id)
        DO UPDATE SET
            reason = EXCLUDED.reason,
            created_by = EXCLUDED.created_by,
            updated_at = now()
        WHERE manual_event_aliases.canonical_event_id = EXCLUDED.canonical_event_id
    `

const insertMatchReviewDecisionSQL = `
        INSERT INTO match_review_decisions (
            source,
            polymarket_event_id,
            polymarket_market_id,
            condition_id,
            canonical_event_id,
            decision_status,
            confidence_score,
            match_reason,
            reviewer,
            review_reason,
            decision_source,
            decided_at,
            updated_at
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5::uuid,
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

func (s *PostgresStore) ListPolymarketMatchReviews(ctx context.Context, status string, limit int) ([]model.PolymarketMatchReview, error) {
	view, err := polymarketMatchReviewView(status)
	if err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(selectPolymarketMatchReviewsSQL, view), boundedMatchReviewLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("list %s polymarket match reviews: %w", status, err)
	}
	defer func() { _ = rows.Close() }()

	reviews := []model.PolymarketMatchReview{}
	for rows.Next() {
		review, err := scanPolymarketMatchReview(rows)
		if err != nil {
			return nil, err
		}
		reviews = append(reviews, review)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list %s polymarket match reviews: %w", status, err)
	}
	return reviews, nil
}

func (s *PostgresStore) ListConfirmedPolymarketAliases(ctx context.Context, limit int) ([]model.PolymarketConfirmedAlias, error) {
	rows, err := s.db.QueryContext(ctx, selectConfirmedPolymarketAliasesSQL, boundedMatchReviewLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("list confirmed polymarket aliases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	aliases := []model.PolymarketConfirmedAlias{}
	for rows.Next() {
		alias, err := scanPolymarketConfirmedAlias(rows)
		if err != nil {
			return nil, err
		}
		aliases = append(aliases, alias)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list confirmed polymarket aliases: %w", err)
	}
	return aliases, nil
}

func (s *PostgresStore) ApprovePolymarketMatch(ctx context.Context, decision model.PolymarketMatchDecision) error {
	decision.DecisionStatus = model.MatchDecisionApproved
	decision = normalizeMatchDecision(decision)
	if err := validateApprovalDecision(decision); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := ensurePolymarketEvent(ctx, tx, decision.PolymarketEventID); err != nil {
		return err
	}
	if err := ensureCanonicalEvent(ctx, tx, decision.CanonicalEventID); err != nil {
		return err
	}

	marketID, conditionID, err := resolveDecisionMarket(ctx, tx, decision)
	if err != nil {
		return err
	}
	decision.PolymarketMarketID = marketID
	decision.ConditionID = conditionID

	if err := ensureNoConflictingConfirmedAlias(ctx, tx, decision); err != nil {
		return err
	}
	aliasID, err := exactConfirmedAlias(ctx, tx, decision)
	if err != nil {
		return err
	}
	if aliasID > 0 {
		if _, err := tx.ExecContext(ctx,
			updatePolymarketConfirmedAliasSQL,
			aliasID,
			decision.CanonicalEventID,
			nullableFloat(decision.ConfidenceScore),
			decision.MatchReason,
		); err != nil {
			return fmt.Errorf("update confirmed polymarket alias: %w", err)
		}
	} else if _, err := tx.ExecContext(ctx,
		insertPolymarketConfirmedAliasSQL,
		decision.CanonicalEventID,
		nullableString(decision.PolymarketEventID),
		nullableString(decision.PolymarketMarketID),
		nullableString(decision.ConditionID),
		nullableFloat(decision.ConfidenceScore),
		decision.MatchReason,
	); err != nil {
		return fmt.Errorf("insert confirmed polymarket alias: %w", err)
	}

	if err := upsertManualAlias(ctx, tx, decision); err != nil {
		return err
	}
	if err := insertMatchReviewDecision(ctx, tx, decision); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit match approval: %w", err)
	}
	return nil
}

func (s *PostgresStore) RejectPolymarketMatch(ctx context.Context, decision model.PolymarketMatchDecision) error {
	decision.DecisionStatus = model.MatchDecisionRejected
	decision = normalizeMatchDecision(decision)
	if err := validateRejectionDecision(decision); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := ensurePolymarketEvent(ctx, tx, decision.PolymarketEventID); err != nil {
		return err
	}
	marketID, conditionID, err := resolveDecisionMarket(ctx, tx, decision)
	if err != nil {
		return err
	}
	decision.PolymarketMarketID = marketID
	decision.ConditionID = conditionID
	if err := ensureNoConfirmedAliasBeforeReject(ctx, tx, decision); err != nil {
		return err
	}
	if err := insertMatchReviewDecision(ctx, tx, decision); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit match rejection: %w", err)
	}
	return nil
}

func polymarketMatchReviewView(status string) (string, error) {
	switch strings.TrimSpace(status) {
	case "no_match", "unresolved":
		return "unresolved_polymarket_event_matches", nil
	case "ambiguous":
		return "ambiguous_polymarket_event_matches", nil
	default:
		return "", fmt.Errorf("unsupported polymarket match review status %q", status)
	}
}

func boundedMatchReviewLimit(limit int) int {
	if limit <= 0 {
		return defaultMatchReviewLimit
	}
	if limit > maxMatchReviewLimit {
		return maxMatchReviewLimit
	}
	return limit
}

type matchReviewScanner interface {
	Scan(dest ...any) error
}

func scanPolymarketMatchReview(row matchReviewScanner) (model.PolymarketMatchReview, error) {
	var review model.PolymarketMatchReview
	var marketID, conditionID, slug, title, sportKey, league, reason, candidateIDs sql.NullString
	var startTime sql.NullTime
	var confidence sql.NullFloat64
	if err := row.Scan(
		&review.ReviewID,
		&review.PolymarketEventID,
		&marketID,
		&conditionID,
		&slug,
		&title,
		&sportKey,
		&league,
		&startTime,
		&review.MatchStatus,
		&confidence,
		&reason,
		&candidateIDs,
		&review.CreatedAt,
		&review.UpdatedAt,
	); err != nil {
		return review, fmt.Errorf("scan polymarket match review: %w", err)
	}
	review.PolymarketMarketID = marketID.String
	review.ConditionID = conditionID.String
	review.Slug = slug.String
	review.Title = title.String
	review.SportKey = sportKey.String
	review.League = league.String
	review.MatchReason = reason.String
	review.CandidateCanonicalEventIDs = candidateIDs.String
	if startTime.Valid {
		t := startTime.Time.UTC()
		review.RawStartTime = &t
	}
	if confidence.Valid {
		v := confidence.Float64
		review.ConfidenceScore = &v
	}
	return review, nil
}

func scanPolymarketConfirmedAlias(row matchReviewScanner) (model.PolymarketConfirmedAlias, error) {
	var alias model.PolymarketConfirmedAlias
	var eventID, marketID, conditionID, sportKey, homeTeam, awayTeam, status, reason sql.NullString
	var reviewer, reviewReason, decisionSource sql.NullString
	var confidence sql.NullFloat64
	var decidedAt sql.NullTime
	if err := row.Scan(
		&alias.AliasID,
		&eventID,
		&marketID,
		&conditionID,
		&alias.CanonicalEventID,
		&sportKey,
		&homeTeam,
		&awayTeam,
		&alias.CommenceTime,
		&status,
		&confidence,
		&reason,
		&reviewer,
		&reviewReason,
		&decisionSource,
		&decidedAt,
		&alias.AliasUpdatedAt,
	); err != nil {
		return alias, fmt.Errorf("scan confirmed polymarket alias: %w", err)
	}
	alias.PolymarketEventID = eventID.String
	alias.PolymarketMarketID = marketID.String
	alias.ConditionID = conditionID.String
	alias.SportKey = sportKey.String
	alias.HomeTeam = homeTeam.String
	alias.AwayTeam = awayTeam.String
	alias.MatchStatus = status.String
	alias.MatchReason = reason.String
	alias.Reviewer = reviewer.String
	alias.ReviewReason = reviewReason.String
	alias.DecisionSource = decisionSource.String
	if confidence.Valid {
		v := confidence.Float64
		alias.ConfidenceScore = &v
	}
	if decidedAt.Valid {
		t := decidedAt.Time.UTC()
		alias.DecidedAt = &t
	}
	return alias, nil
}

func validateApprovalDecision(decision model.PolymarketMatchDecision) error {
	if decision.PolymarketEventID == "" {
		return errors.New("polymarket event id is required")
	}
	if decision.CanonicalEventID == "" {
		return errors.New("canonical event id is required")
	}
	return nil
}

func validateRejectionDecision(decision model.PolymarketMatchDecision) error {
	if decision.PolymarketEventID == "" {
		return errors.New("polymarket event id is required")
	}
	if decision.CanonicalEventID != "" {
		return errors.New("canonical event id must be empty for rejection")
	}
	if decision.ReviewReason == "" {
		return errors.New("review reason is required for rejection")
	}
	return nil
}

func normalizeMatchDecision(decision model.PolymarketMatchDecision) model.PolymarketMatchDecision {
	decision.PolymarketEventID = strings.TrimSpace(decision.PolymarketEventID)
	decision.PolymarketMarketID = strings.TrimSpace(decision.PolymarketMarketID)
	decision.ConditionID = strings.TrimSpace(decision.ConditionID)
	decision.CanonicalEventID = strings.TrimSpace(decision.CanonicalEventID)
	decision.DecisionStatus = strings.TrimSpace(decision.DecisionStatus)
	decision.DecisionSource = strings.TrimSpace(decision.DecisionSource)
	decision.Reviewer = strings.TrimSpace(decision.Reviewer)
	decision.ReviewReason = strings.TrimSpace(decision.ReviewReason)
	decision.MatchReason = strings.TrimSpace(decision.MatchReason)
	if decision.DecisionSource == "" {
		decision.DecisionSource = "manual"
	}
	return decision
}

func ensurePolymarketEvent(ctx context.Context, tx *sql.Tx, eventID string) error {
	exists, err := boolQuery(ctx, tx, selectPolymarketEventExistsSQL, eventID)
	if err != nil {
		return fmt.Errorf("check polymarket event %s: %w", eventID, err)
	}
	if !exists {
		return fmt.Errorf("polymarket event %s does not exist", eventID)
	}
	return nil
}

func ensureCanonicalEvent(ctx context.Context, tx *sql.Tx, canonicalID string) error {
	exists, err := boolQuery(ctx, tx, selectCanonicalEventExistsSQL, canonicalID)
	if err != nil {
		return fmt.Errorf("check canonical event %s: %w", canonicalID, err)
	}
	if !exists {
		return fmt.Errorf("canonical event %s does not exist", canonicalID)
	}
	return nil
}

func resolveDecisionMarket(ctx context.Context, tx *sql.Tx, decision model.PolymarketMatchDecision) (string, string, error) {
	if decision.PolymarketMarketID == "" && decision.ConditionID == "" {
		return "", "", nil
	}

	var marketID, conditionID string
	err := tx.QueryRowContext(ctx,
		selectPolymarketMarketForDecisionSQL,
		decision.PolymarketEventID,
		decision.PolymarketMarketID,
		decision.ConditionID,
	).Scan(&marketID, &conditionID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", fmt.Errorf("polymarket market or condition does not belong to event %s", decision.PolymarketEventID)
	}
	if err != nil {
		return "", "", fmt.Errorf("resolve polymarket market for event %s: %w", decision.PolymarketEventID, err)
	}
	return marketID, conditionID, nil
}

func ensureNoConflictingConfirmedAlias(ctx context.Context, tx *sql.Tx, decision model.PolymarketMatchDecision) error {
	rows, err := tx.QueryContext(ctx,
		selectConfirmedAliasConflictsForUpdateSQL,
		decision.PolymarketEventID,
		decision.PolymarketMarketID,
		decision.ConditionID,
	)
	if err != nil {
		return fmt.Errorf("check confirmed polymarket alias conflicts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var canonicalID string
		if err := rows.Scan(&canonicalID); err != nil {
			return fmt.Errorf("scan confirmed polymarket alias conflict: %w", err)
		}
		if canonicalID != decision.CanonicalEventID {
			return fmt.Errorf("polymarket event %s already has a confirmed alias to canonical event %s", decision.PolymarketEventID, canonicalID)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("check confirmed polymarket alias conflicts: %w", err)
	}
	return nil
}

func ensureNoConfirmedAliasBeforeReject(ctx context.Context, tx *sql.Tx, decision model.PolymarketMatchDecision) error {
	rows, err := tx.QueryContext(ctx,
		selectConfirmedAliasConflictsForUpdateSQL,
		decision.PolymarketEventID,
		decision.PolymarketMarketID,
		decision.ConditionID,
	)
	if err != nil {
		return fmt.Errorf("check confirmed polymarket aliases before rejection: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if rows.Next() {
		var canonicalID string
		if err := rows.Scan(&canonicalID); err != nil {
			return fmt.Errorf("scan confirmed polymarket alias before rejection: %w", err)
		}
		return fmt.Errorf("polymarket event %s already has a confirmed alias to canonical event %s", decision.PolymarketEventID, canonicalID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("check confirmed polymarket aliases before rejection: %w", err)
	}
	return nil
}

func exactConfirmedAlias(ctx context.Context, tx *sql.Tx, decision model.PolymarketMatchDecision) (int64, error) {
	var aliasID int64
	var canonicalID string
	err := tx.QueryRowContext(ctx,
		selectExactConfirmedAliasForUpdateSQL,
		decision.PolymarketEventID,
		decision.PolymarketMarketID,
		decision.ConditionID,
	).Scan(&aliasID, &canonicalID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("check exact confirmed polymarket alias: %w", err)
	}
	if canonicalID != decision.CanonicalEventID {
		return 0, fmt.Errorf("confirmed alias already points to canonical event %s", canonicalID)
	}
	return aliasID, nil
}

func upsertManualAlias(ctx context.Context, tx *sql.Tx, decision model.PolymarketMatchDecision) error {
	var canonicalID string
	err := tx.QueryRowContext(ctx,
		selectManualEventAliasForUpdateSQL,
		model.SourcePolymarket,
		decision.PolymarketEventID,
		decision.PolymarketMarketID,
		decision.ConditionID,
	).Scan(&canonicalID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check manual event alias: %w", err)
	}
	if canonicalID != "" && canonicalID != decision.CanonicalEventID {
		return fmt.Errorf("manual alias already points to canonical event %s", canonicalID)
	}

	createdBy := firstNonEmpty(decision.Reviewer, decision.DecisionSource, "manual")
	result, err := tx.ExecContext(ctx,
		upsertManualEventAliasSQL,
		model.SourcePolymarket,
		decision.PolymarketEventID,
		decision.PolymarketMarketID,
		decision.ConditionID,
		decision.CanonicalEventID,
		decision.ReviewReason,
		createdBy,
	)
	if err != nil {
		return fmt.Errorf("upsert manual event alias: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return errors.New("manual alias conflict: existing alias points to a different canonical event")
	}
	return nil
}

func insertMatchReviewDecision(ctx context.Context, tx *sql.Tx, decision model.PolymarketMatchDecision) error {
	var canonicalID sql.NullString
	if decision.CanonicalEventID != "" {
		canonicalID = sql.NullString{String: decision.CanonicalEventID, Valid: true}
	}

	if _, err := tx.ExecContext(ctx,
		insertMatchReviewDecisionSQL,
		model.SourcePolymarket,
		decision.PolymarketEventID,
		nullableString(decision.PolymarketMarketID),
		nullableString(decision.ConditionID),
		canonicalID,
		decision.DecisionStatus,
		nullableFloat(decision.ConfidenceScore),
		decision.MatchReason,
		decision.Reviewer,
		decision.ReviewReason,
		decision.DecisionSource,
	); err != nil {
		return fmt.Errorf("insert match review decision: %w", err)
	}
	return nil
}

func boolQuery(ctx context.Context, tx *sql.Tx, query string, args ...any) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, query, args...).Scan(&exists)
	return exists, err
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
