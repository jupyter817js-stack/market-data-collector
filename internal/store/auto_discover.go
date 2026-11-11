package store

import (
	"context"
	"fmt"
	"time"
)

// CountRecentAMQPLivescore returns how many livescore observations with
// source='amqp_push' arrived for the given fixture within the last
// `window`. Used by auto-discovery to verify that an LSPORTS Subscribe
// is producing data (Trial-accessible) rather than silently blocked
// (Premium-only).
func (s *PostgresStore) CountRecentAMQPLivescore(ctx context.Context, fixtureID int64, window time.Duration) (int, error) {
	const q = `
		SELECT count(*)
		  FROM lsports_inplay_observations
		 WHERE lsports_fixture_id = $1
		   AND observation_kind = 'livescore'
		   AND source = 'amqp_push'
		   AND received_at > now() - $2::interval`
	var n int
	if err := s.db.QueryRowContext(ctx, q, fixtureID, fmt.Sprintf("%d milliseconds", window.Milliseconds())).Scan(&n); err != nil {
		return 0, fmt.Errorf("count recent amqp livescore: %w", err)
	}
	return n, nil
}
