package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

// Phase 12H: manual Polymarket token-outcome alias workflow.
//
// All decisions are operator-supplied and validated server-side. There is no
// auto-mapping, no auto-approval, no inference from title order, and no path
// from this code to any order/account/wallet/private-key surface. Confirmed
// aliases require an exact token_id, an exact canonical_event_id, an explicit
// canonical outcome name AND side, a non-empty reviewer, and a non-empty
// reason. The store also rejects any second-confirmation that would conflict
// with an existing confirmed alias on the same token, on the same canonical
// event side, or on the same canonical event with a different mapping.

const (
	tokenAliasStatusConfirmed  = "confirmed"
	tokenAliasStatusRejected   = "rejected"
	tokenAliasStatusUnresolved = "unresolved"
	tokenAliasStatusAmbiguous  = "ambiguous"

	defaultTokenAliasListLimit = 50
	maxTokenAliasListLimit     = 200
)

// ApprovePolymarketTokenAlias inserts (or updates) a confirmed token-to-
// canonical-outcome mapping after validating all of the alias invariants:
//
//   - token_id, canonical_event_id, canonical_outcome_name,
//     canonical_outcome_side, reviewer, and reason are all required and
//     non-empty
//   - token_id must reference an existing polymarket_tokens row
//   - canonical_event_id must reference an existing canonical_events row
//   - the token must not already have a confirmed alias to a different
//     canonical_event_id
//   - the token must not already have a confirmed alias with a different
//     outcome side or outcome name
//   - the canonical_event_id must not already have a confirmed alias on the
//     same outcome_side mapped to a different token
//   - decision_source defaults to "manual"
//
// On success the row is written inside a single transaction and decided_at
// reflects wall-clock time at insert.
func (s *PostgresStore) ApprovePolymarketTokenAlias(ctx context.Context, decision model.PolymarketTokenAliasDecision) error {
	decision.DecisionStatus = tokenAliasStatusConfirmed
	decision = normalizeTokenAliasDecision(decision)
	if err := validateConfirmedTokenAliasDecision(decision); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	condID, marketID, err := resolveTokenAliasMarket(ctx, tx, decision)
	if err != nil {
		return err
	}
	decision.ConditionID = condID
	decision.PolymarketMarketID = marketID

	if err := ensureCanonicalEvent(ctx, tx, decision.CanonicalEventID); err != nil {
		return err
	}

	if err := ensureNoConflictingConfirmedTokenAlias(ctx, tx, decision); err != nil {
		return err
	}

	// upsert: if a confirmed alias for this token+canonical_event already
	// exists with the same outcome_name + side, update it (operators may
	// re-confirm with a refined reason or confidence). Anything else is a
	// conflict and is rejected by ensureNoConflictingConfirmedTokenAlias above.
	existingID, err := exactConfirmedTokenAliasID(ctx, tx, decision)
	if err != nil {
		return err
	}
	if existingID > 0 {
		if _, err := tx.ExecContext(ctx, `
            UPDATE polymarket_token_aliases
            SET condition_id = $2,
                polymarket_market_id = $3,
                canonical_outcome_name = $4,
                canonical_outcome_side = $5,
                confidence_score = $6,
                reviewer = $7,
                reason = $8,
                decision_source = $9,
                decided_at = now(),
                updated_at = now()
            WHERE id = $1
        `, existingID,
			nullableString(decision.ConditionID),
			nullableString(decision.PolymarketMarketID),
			decision.CanonicalOutcomeName,
			decision.CanonicalOutcomeSide,
			nullableFloat(decision.ConfidenceScore),
			decision.Reviewer,
			decision.Reason,
			decision.DecisionSource,
		); err != nil {
			return fmt.Errorf("update confirmed polymarket token alias: %w", err)
		}
	} else if _, err := tx.ExecContext(ctx, `
        INSERT INTO polymarket_token_aliases (
            token_id, condition_id, polymarket_market_id,
            canonical_event_id, canonical_outcome_name, canonical_outcome_side,
            status, confidence_score, reviewer, reason, decision_source,
            decided_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, 'confirmed', $7, $8, $9, $10, now(), now())
    `,
		decision.TokenID,
		nullableString(decision.ConditionID),
		nullableString(decision.PolymarketMarketID),
		decision.CanonicalEventID,
		decision.CanonicalOutcomeName,
		decision.CanonicalOutcomeSide,
		nullableFloat(decision.ConfidenceScore),
		decision.Reviewer,
		decision.Reason,
		decision.DecisionSource,
	); err != nil {
		return fmt.Errorf("insert confirmed polymarket token alias: %w", err)
	}

	return tx.Commit()
}

// RejectPolymarketTokenAlias records an operator decision that an alias
// candidate is NOT a valid mapping. Reviewer + reason are still required so
// future auditors can see why the candidate was rejected. The token_id and
// canonical_event_id (when supplied) are validated like the confirmed path.
// Rejected rows do not block future confirmed-alias attempts and do not
// participate in the edges view.
func (s *PostgresStore) RejectPolymarketTokenAlias(ctx context.Context, decision model.PolymarketTokenAliasDecision) error {
	decision.DecisionStatus = tokenAliasStatusRejected
	decision = normalizeTokenAliasDecision(decision)
	if err := validateRejectedTokenAliasDecision(decision); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Token id reference must exist.
	if _, _, err := resolveTokenAliasMarket(ctx, tx, decision); err != nil {
		return err
	}
	if decision.CanonicalEventID != "" {
		if err := ensureCanonicalEvent(ctx, tx, decision.CanonicalEventID); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `
        INSERT INTO polymarket_token_aliases (
            token_id, condition_id, polymarket_market_id,
            canonical_event_id, canonical_outcome_name, canonical_outcome_side,
            status, confidence_score, reviewer, reason, decision_source,
            decided_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, 'rejected', $7, $8, $9, $10, now(), now())
    `,
		decision.TokenID,
		nullableString(decision.ConditionID),
		nullableString(decision.PolymarketMarketID),
		nullableString(decision.CanonicalEventID),
		decision.CanonicalOutcomeName,
		decision.CanonicalOutcomeSide,
		nullableFloat(decision.ConfidenceScore),
		decision.Reviewer,
		decision.Reason,
		decision.DecisionSource,
	); err != nil {
		return fmt.Errorf("insert rejected polymarket token alias: %w", err)
	}
	return tx.Commit()
}

// ListPolymarketTokenAliases returns aliases joined with canonical event and
// token metadata. Filters on canonical_event_id, token_id, and status are
// optional.
func (s *PostgresStore) ListPolymarketTokenAliases(ctx context.Context, opts model.PolymarketTokenAliasListOptions) ([]model.PolymarketTokenAlias, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultTokenAliasListLimit
	}
	if limit > maxTokenAliasListLimit {
		limit = maxTokenAliasListLimit
	}
	canonicalID := strings.TrimSpace(opts.CanonicalEventID)
	tokenID := strings.TrimSpace(opts.TokenID)
	status := strings.TrimSpace(strings.ToLower(opts.Status))

	rows, err := s.db.QueryContext(ctx, `
        SELECT
            a.id,
            a.token_id,
            coalesce(a.condition_id, ''),
            coalesce(a.polymarket_market_id, ''),
            coalesce(a.canonical_event_id::text, ''),
            ce.sport_key,
            ce.home_team,
            ce.away_team,
            ce.commence_time,
            a.canonical_outcome_name,
            a.canonical_outcome_side,
            a.status,
            a.confidence_score,
            a.reviewer,
            a.reason,
            a.decision_source,
            a.decided_at,
            a.updated_at,
            coalesce(pt.outcome_name, ''),
            coalesce(pt.side, '')
        FROM polymarket_token_aliases a
        LEFT JOIN canonical_events ce ON ce.id = a.canonical_event_id
        LEFT JOIN polymarket_tokens pt ON pt.token_id = a.token_id
        WHERE ($1 = '' OR a.canonical_event_id = $1::uuid)
          AND ($2 = '' OR a.token_id = $2)
          AND ($3 = '' OR lower(a.status) = $3)
        ORDER BY a.decided_at DESC, a.id DESC
        LIMIT $4
    `, canonicalID, tokenID, status, limit)
	if err != nil {
		return nil, fmt.Errorf("list polymarket token aliases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.PolymarketTokenAlias{}
	for rows.Next() {
		var alias model.PolymarketTokenAlias
		var commence sql.NullTime
		var sportKey, homeTeam, awayTeam sql.NullString
		var confidence sql.NullFloat64
		if err := rows.Scan(
			&alias.AliasID,
			&alias.TokenID,
			&alias.ConditionID,
			&alias.PolymarketMarketID,
			&alias.CanonicalEventID,
			&sportKey,
			&homeTeam,
			&awayTeam,
			&commence,
			&alias.CanonicalOutcomeName,
			&alias.CanonicalOutcomeSide,
			&alias.Status,
			&confidence,
			&alias.Reviewer,
			&alias.Reason,
			&alias.DecisionSource,
			&alias.DecidedAt,
			&alias.UpdatedAt,
			&alias.PolymarketOutcomeName,
			&alias.PolymarketYesNoSide,
		); err != nil {
			return nil, fmt.Errorf("scan polymarket token alias: %w", err)
		}
		alias.SportKey = sportKey.String
		alias.HomeTeam = homeTeam.String
		alias.AwayTeam = awayTeam.String
		if commence.Valid {
			alias.CommenceTime = commence.Time.UTC()
		}
		if confidence.Valid {
			v := confidence.Float64
			alias.ConfidenceScore = &v
		}
		out = append(out, alias)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate polymarket token aliases: %w", err)
	}
	return out, nil
}

// normalizeTokenAliasDecision trims whitespace and lowercases the side label so
// "Home" / "home" / "HOME" are not treated as different mappings.
func normalizeTokenAliasDecision(d model.PolymarketTokenAliasDecision) model.PolymarketTokenAliasDecision {
	d.TokenID = strings.TrimSpace(d.TokenID)
	d.ConditionID = strings.TrimSpace(d.ConditionID)
	d.PolymarketMarketID = strings.TrimSpace(d.PolymarketMarketID)
	d.CanonicalEventID = strings.TrimSpace(d.CanonicalEventID)
	d.CanonicalOutcomeName = strings.TrimSpace(d.CanonicalOutcomeName)
	d.CanonicalOutcomeSide = strings.ToLower(strings.TrimSpace(d.CanonicalOutcomeSide))
	d.Reviewer = strings.TrimSpace(d.Reviewer)
	d.Reason = strings.TrimSpace(d.Reason)
	d.DecisionSource = strings.TrimSpace(d.DecisionSource)
	if d.DecisionSource == "" {
		d.DecisionSource = "manual"
	}
	return d
}

// ErrTokenAliasInvalid is the sentinel returned for client-side validation
// failures. The wrapped message describes which invariant was violated.
var ErrTokenAliasInvalid = errors.New("polymarket token alias decision invalid")

func validateConfirmedTokenAliasDecision(d model.PolymarketTokenAliasDecision) error {
	if d.TokenID == "" {
		return fmt.Errorf("%w: token_id is required", ErrTokenAliasInvalid)
	}
	if d.CanonicalEventID == "" {
		return fmt.Errorf("%w: canonical_event_id is required", ErrTokenAliasInvalid)
	}
	if d.CanonicalOutcomeName == "" {
		return fmt.Errorf("%w: canonical_outcome_name is required for a confirmed alias", ErrTokenAliasInvalid)
	}
	if d.CanonicalOutcomeSide == "" {
		return fmt.Errorf("%w: canonical_outcome_side is required for a confirmed alias (e.g. home, away, draw, over, under)", ErrTokenAliasInvalid)
	}
	if d.Reviewer == "" {
		return fmt.Errorf("%w: reviewer is required for a confirmed alias", ErrTokenAliasInvalid)
	}
	if d.Reason == "" {
		return fmt.Errorf("%w: reason is required for a confirmed alias (operator must explicitly state why this token maps to this outcome)", ErrTokenAliasInvalid)
	}
	if d.ConfidenceScore != nil && (*d.ConfidenceScore < 0 || *d.ConfidenceScore > 100) {
		return fmt.Errorf("%w: confidence_score must be in [0, 100]", ErrTokenAliasInvalid)
	}
	return nil
}

func validateRejectedTokenAliasDecision(d model.PolymarketTokenAliasDecision) error {
	if d.TokenID == "" {
		return fmt.Errorf("%w: token_id is required", ErrTokenAliasInvalid)
	}
	if d.Reviewer == "" {
		return fmt.Errorf("%w: reviewer is required for a rejected alias", ErrTokenAliasInvalid)
	}
	if d.Reason == "" {
		return fmt.Errorf("%w: reason is required for a rejected alias", ErrTokenAliasInvalid)
	}
	if d.ConfidenceScore != nil && (*d.ConfidenceScore < 0 || *d.ConfidenceScore > 100) {
		return fmt.Errorf("%w: confidence_score must be in [0, 100]", ErrTokenAliasInvalid)
	}
	return nil
}

// resolveTokenAliasMarket confirms the token exists and looks up the
// authoritative condition_id and polymarket_market_id from polymarket_tokens.
// Operator-supplied condition_id / polymarket_market_id (if any) MUST match
// the token's stored values, otherwise the decision is rejected.
func resolveTokenAliasMarket(ctx context.Context, tx *sql.Tx, d model.PolymarketTokenAliasDecision) (string, string, error) {
	var conditionID, marketID sql.NullString
	err := tx.QueryRowContext(ctx, `
        SELECT condition_id, coalesce(polymarket_market_id, '')
        FROM polymarket_tokens
        WHERE token_id = $1
    `, d.TokenID).Scan(&conditionID, &marketID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", fmt.Errorf("%w: token_id %s does not exist locally; sync the Polymarket event first", ErrTokenAliasInvalid, d.TokenID)
		}
		return "", "", fmt.Errorf("look up polymarket_tokens.token_id=%s: %w", d.TokenID, err)
	}
	if d.ConditionID != "" && conditionID.Valid && d.ConditionID != conditionID.String {
		return "", "", fmt.Errorf("%w: condition_id %s does not match token's stored condition_id %s", ErrTokenAliasInvalid, d.ConditionID, conditionID.String)
	}
	if d.PolymarketMarketID != "" && marketID.Valid && marketID.String != "" && d.PolymarketMarketID != marketID.String {
		return "", "", fmt.Errorf("%w: polymarket_market_id %s does not match token's stored market_id %s", ErrTokenAliasInvalid, d.PolymarketMarketID, marketID.String)
	}
	return conditionID.String, marketID.String, nil
}

// ensureNoConflictingConfirmedTokenAlias rejects a confirmed-alias attempt
// that would conflict with an existing confirmed alias.
//
// Rejected conflicts:
//   - same token_id confirmed to a DIFFERENT canonical_event_id
//   - same token_id confirmed with a DIFFERENT canonical_outcome_side or
//     canonical_outcome_name on the SAME canonical_event_id (the operator
//     would have to reject the existing one first to switch the mapping)
//   - same canonical_event_id + canonical_outcome_side already confirmed to a
//     DIFFERENT token_id (each side has at most one token mapping)
func ensureNoConflictingConfirmedTokenAlias(ctx context.Context, tx *sql.Tx, d model.PolymarketTokenAliasDecision) error {
	// Same token, different canonical event.
	var foundCanonical sql.NullString
	if err := tx.QueryRowContext(ctx, `
        SELECT canonical_event_id::text
        FROM polymarket_token_aliases
        WHERE token_id = $1 AND status = 'confirmed' AND canonical_event_id <> $2::uuid
        LIMIT 1
    `, d.TokenID, d.CanonicalEventID).Scan(&foundCanonical); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check existing token alias canonical mapping: %w", err)
	}
	if foundCanonical.Valid && foundCanonical.String != "" {
		return fmt.Errorf("%w: token %s is already confirmed against canonical_event_id %s; reject that alias before re-mapping", ErrTokenAliasInvalid, d.TokenID, foundCanonical.String)
	}

	// Same token + same canonical event but different side or different name.
	var foundSide, foundName sql.NullString
	if err := tx.QueryRowContext(ctx, `
        SELECT canonical_outcome_side, canonical_outcome_name
        FROM polymarket_token_aliases
        WHERE token_id = $1 AND status = 'confirmed' AND canonical_event_id = $2::uuid
        LIMIT 1
    `, d.TokenID, d.CanonicalEventID).Scan(&foundSide, &foundName); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check existing token alias outcome side: %w", err)
	}
	if foundSide.Valid {
		if strings.ToLower(foundSide.String) != d.CanonicalOutcomeSide {
			return fmt.Errorf("%w: token %s is already confirmed to outcome side %s; reject that alias before re-mapping", ErrTokenAliasInvalid, d.TokenID, foundSide.String)
		}
		if foundName.Valid && foundName.String != d.CanonicalOutcomeName {
			return fmt.Errorf("%w: token %s is already confirmed with outcome name %q; reject that alias before changing the name", ErrTokenAliasInvalid, d.TokenID, foundName.String)
		}
	}

	// Same canonical event + same outcome side already confirmed to a different
	// token. Operators must reject the existing side-mapping first.
	var foundOtherToken sql.NullString
	if err := tx.QueryRowContext(ctx, `
        SELECT token_id
        FROM polymarket_token_aliases
        WHERE canonical_event_id = $1::uuid
          AND status = 'confirmed'
          AND lower(canonical_outcome_side) = $2
          AND token_id <> $3
        LIMIT 1
    `, d.CanonicalEventID, d.CanonicalOutcomeSide, d.TokenID).Scan(&foundOtherToken); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check existing canonical side mapping: %w", err)
	}
	if foundOtherToken.Valid && foundOtherToken.String != "" {
		return fmt.Errorf("%w: canonical_event_id %s outcome side %s is already confirmed to a different token (%s); reject that alias before re-mapping", ErrTokenAliasInvalid, d.CanonicalEventID, d.CanonicalOutcomeSide, foundOtherToken.String)
	}
	return nil
}

func exactConfirmedTokenAliasID(ctx context.Context, tx *sql.Tx, d model.PolymarketTokenAliasDecision) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
        SELECT id
        FROM polymarket_token_aliases
        WHERE token_id = $1
          AND status = 'confirmed'
          AND canonical_event_id = $2::uuid
          AND lower(canonical_outcome_side) = $3
          AND canonical_outcome_name = $4
        LIMIT 1
    `, d.TokenID, d.CanonicalEventID, d.CanonicalOutcomeSide, d.CanonicalOutcomeName).Scan(&id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("look up exact confirmed token alias: %w", err)
	}
	return id, nil
}

// time import marker: keeps `time` referenced even when CommenceTime is the
// only consumer. Removing this would break `go vet` on systems that briefly
// strip the field while editing.
var _ = time.UTC
