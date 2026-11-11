package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/your-org/pol-odds-collector/internal/model"
)

// InsertLSportsSubscription writes a new 'pending' row after a successful
// /Fixtures/Subscribe REST call. It enforces:
//   - operator and subscribe_reason are non-empty (server-side too)
//   - no other open (pending / committed_inprogress) row exists for the
//     same lsports_fixture_id (matches the partial unique index)
//
// The caller is expected to perform the LSPORTS REST call and ledger insert
// within the same logical action; if the REST call fails, do NOT call this.
// If the REST call succeeded but this insert fails (e.g., a duplicate open
// row), the caller must Unsubscribe to refund — otherwise the slot leaks.
func (s *PostgresStore) InsertLSportsSubscription(ctx context.Context, in model.LSportsSubscribeInput) (int64, error) {
	in.Operator = strings.TrimSpace(in.Operator)
	in.SubscribeReason = strings.TrimSpace(in.SubscribeReason)
	if in.Operator == "" {
		return 0, errors.New("lsports subscription: operator is required")
	}
	if in.SubscribeReason == "" {
		return 0, errors.New("lsports subscription: subscribe_reason is required")
	}
	if in.LSportsFixtureID <= 0 {
		return 0, fmt.Errorf("lsports subscription: lsports_fixture_id must be > 0 (got %d)", in.LSportsFixtureID)
	}

	const q = `
		INSERT INTO lsports_subscriptions (
			lsports_fixture_id,
			lsports_sport_id,
			lsports_league_id,
			fixture_name,
			home_participant,
			away_participant,
			commence_time,
			status,
			quota_remaining_before_subscribe,
			quota_remaining_after_subscribe,
			operator,
			subscribe_reason,
			polymarket_coverage_verified,
			polymarket_coverage_notes,
			polymarket_canonical_event_id,
			subscribed_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			'pending',
			$8, $9, $10, $11, $12, $13, $14,
			now()
		)
		RETURNING id`

	var id int64
	err := s.db.QueryRowContext(ctx, q,
		in.LSportsFixtureID,
		nullableInt(in.LSportsSportID),
		nullableInt(in.LSportsLeagueID),
		nullableString(in.FixtureName),
		nullableString(in.HomeParticipant),
		nullableString(in.AwayParticipant),
		nullableTime(in.CommenceTime),
		in.QuotaRemainingBeforeSubscribe,
		in.QuotaRemainingAfterSubscribe,
		in.Operator,
		in.SubscribeReason,
		in.PolymarketCoverageVerified,
		nullableString(in.PolymarketCoverageNotes),
		nullableString(in.PolymarketCanonicalEventID),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert lsports_subscriptions: %w", err)
	}
	return id, nil
}

// MarkLSportsSubscriptionRefunded transitions a 'pending' row to 'refunded'
// after a successful /Fixtures/Unsubscribe call that occurred BEFORE the
// match went InProgress. Records the operator-supplied reason and the
// post-unsubscribe quota snapshot so a later audit can verify the refund.
func (s *PostgresStore) MarkLSportsSubscriptionRefunded(
	ctx context.Context,
	fixtureID int64,
	operator, reason string,
	quotaAfter int,
) error {
	return s.markLSportsTransition(ctx, fixtureID, operator, reason, quotaAfter,
		[]string{"pending"},
		"refunded",
	)
}

// MarkLSportsSubscriptionUnsubscribedTooLate transitions a 'pending' or
// 'committed_inprogress' row to 'unsubscribed_too_late' when Unsubscribe
// was called AFTER InProgress (no refund occurred). This records the
// operational mistake so the budget breakdown reflects what really happened.
func (s *PostgresStore) MarkLSportsSubscriptionUnsubscribedTooLate(
	ctx context.Context,
	fixtureID int64,
	operator, reason string,
	quotaAfter int,
) error {
	return s.markLSportsTransition(ctx, fixtureID, operator, reason, quotaAfter,
		[]string{"pending", "committed_inprogress"},
		"unsubscribed_too_late",
	)
}

// MarkLSportsSubscriptionInProgress transitions 'pending' to
// 'committed_inprogress' when the babysitter (or operator) observes the
// match crossing the InProgress boundary. From this point the slot is
// permanently consumed.
func (s *PostgresStore) MarkLSportsSubscriptionInProgress(ctx context.Context, fixtureID int64) error {
	const q = `
		UPDATE lsports_subscriptions
		   SET status = 'committed_inprogress',
		       inprogress_at = COALESCE(inprogress_at, now())
		 WHERE lsports_fixture_id = $1
		   AND status = 'pending'`
	res, err := s.db.ExecContext(ctx, q, fixtureID)
	if err != nil {
		return fmt.Errorf("mark lsports subscription in_progress: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no pending lsports subscription for fixture %d (already transitioned or not found)", fixtureID)
	}
	return nil
}

// MarkLSportsSubscriptionSettled transitions a 'committed_inprogress' row to
// 'settled' when the match finishes. The slot stays consumed.
func (s *PostgresStore) MarkLSportsSubscriptionSettled(ctx context.Context, fixtureID int64) error {
	const q = `
		UPDATE lsports_subscriptions
		   SET status = 'settled',
		       settled_at = COALESCE(settled_at, now())
		 WHERE lsports_fixture_id = $1
		   AND status = 'committed_inprogress'`
	res, err := s.db.ExecContext(ctx, q, fixtureID)
	if err != nil {
		return fmt.Errorf("mark lsports subscription settled: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no committed_inprogress lsports subscription for fixture %d", fixtureID)
	}
	return nil
}

func (s *PostgresStore) markLSportsTransition(
	ctx context.Context,
	fixtureID int64,
	operator, reason string,
	quotaAfter int,
	allowedFromStatuses []string,
	toStatus string,
) error {
	operator = strings.TrimSpace(operator)
	reason = strings.TrimSpace(reason)
	if operator == "" {
		return errors.New("lsports subscription transition: operator is required")
	}
	if reason == "" {
		return errors.New("lsports subscription transition: reason is required")
	}
	if len(allowedFromStatuses) == 0 {
		return errors.New("lsports subscription transition: allowed_from_statuses is empty")
	}

	placeholders := make([]string, len(allowedFromStatuses))
	args := []any{
		toStatus,
		reason,
		quotaAfter,
		fixtureID,
	}
	for i, st := range allowedFromStatuses {
		placeholders[i] = fmt.Sprintf("$%d", len(args)+1)
		args = append(args, st)
	}

	q := fmt.Sprintf(`
		UPDATE lsports_subscriptions
		   SET status = $1,
		       unsubscribe_reason = $2,
		       quota_remaining_after_unsubscribe = $3,
		       unsubscribed_at = COALESCE(unsubscribed_at, now())
		 WHERE lsports_fixture_id = $4
		   AND status IN (%s)`,
		strings.Join(placeholders, ", "),
	)

	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("lsports subscription transition: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no eligible lsports subscription for fixture %d (allowed from %v)", fixtureID, allowedFromStatuses)
	}
	return nil
}

// HasOpenLSportsSubscription returns true when an open ('pending' or
// 'committed_inprogress') row already exists for the fixture. The CLI uses
// this as a pre-check so the operator sees a clear error before any REST
// call is attempted (defense-in-depth alongside the partial unique index).
func (s *PostgresStore) HasOpenLSportsSubscription(ctx context.Context, fixtureID int64) (bool, error) {
	const q = `
		SELECT EXISTS (
			SELECT 1 FROM lsports_subscriptions
			 WHERE lsports_fixture_id = $1
			   AND status IN ('pending', 'committed_inprogress')
		)`
	var exists bool
	if err := s.db.QueryRowContext(ctx, q, fixtureID).Scan(&exists); err != nil {
		return false, fmt.Errorf("query open lsports subscription: %w", err)
	}
	return exists, nil
}

// LSportsBudgetSummary returns the lsports_subscription_budget_summary view.
type LSportsBudgetSummary struct {
	TotalRows            int64
	PendingNow           int64
	CommittedInProgress  int64
	Refunded             int64
	UnsubscribedTooLate  int64
	Settled              int64
	PermanentBurn        int64
	PendingLast24h       int64
}

func (s *PostgresStore) LSportsBudgetSummary(ctx context.Context) (LSportsBudgetSummary, error) {
	const q = `
		SELECT total_rows,
		       pending_now,
		       committed_inprogress,
		       refunded,
		       unsubscribed_too_late,
		       settled,
		       permanent_burn,
		       pending_last_24h
		  FROM lsports_subscription_budget_summary`
	var b LSportsBudgetSummary
	if err := s.db.QueryRowContext(ctx, q).Scan(
		&b.TotalRows,
		&b.PendingNow,
		&b.CommittedInProgress,
		&b.Refunded,
		&b.UnsubscribedTooLate,
		&b.Settled,
		&b.PermanentBurn,
		&b.PendingLast24h,
	); err != nil {
		return LSportsBudgetSummary{}, fmt.Errorf("query lsports budget summary: %w", err)
	}
	return b, nil
}

