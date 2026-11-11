package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/your-org/pol-odds-collector/internal/executor"
	sigpkg "github.com/your-org/pol-odds-collector/internal/signal"
)

// ExecutorStore reads fresh BUY candidates and persists planned orders.
type ExecutorStore struct{ db *sql.DB }

func NewExecutorStore(s *PostgresStore) *ExecutorStore { return &ExecutorStore{db: s.db} }

// FreshBuyCandidates reads from the signals_buy_candidates_fresh view
// (migration 023). Each row is the most-recent BUY_CANDIDATE per
// (matchup, outcome) whose freshness is still acceptable.
func (e *ExecutorStore) FreshBuyCandidates(ctx context.Context) ([]sigpkg.Signal, error) {
	rows, err := e.db.QueryContext(ctx, `
        SELECT
            id, matchup_id, polymarket_condition_id, COALESCE(polymarket_token_id, ''),
            outcome_side, outcome_name, home_name, away_name, league_name,
            COALESCE(pinnacle_fair_prob, 0)::float8,
            pm_ask, pm_bid, pm_mid,
            pinnacle_market_status, COALESCE(pinnacle_market_version, 0),
            COALESCE(gross_edge_to_buy, 0)::float8,
            COALESCE(gross_edge_to_sell, 0)::float8,
            COALESCE(spread_cost, 0)::float8,
            COALESCE(slippage_buffer, 0)::float8,
            COALESCE(latency_buffer, 0)::float8,
            COALESCE(overround_buffer, 0)::float8,
            COALESCE(net_edge_buy, 0)::float8,
            COALESCE(pinnacle_age_sec, 0)::float8,
            COALESCE(pm_age_sec, 0)::float8,
            COALESCE(proposed_size_usdc, 0)::float8,
            COALESCE(threshold_net_edge, 0)::float8,
            state
          FROM signals_buy_candidates_fresh
    `)
	if err != nil {
		return nil, fmt.Errorf("read fresh buy candidates: %w", err)
	}
	defer rows.Close()
	out := []sigpkg.Signal{}
	for rows.Next() {
		var s sigpkg.Signal
		var stateStr string
		if err := rows.Scan(
			&s.ID,
			&s.MatchupID, &s.PolymarketConditionID, &s.PolymarketTokenID,
			&s.OutcomeSide, &s.OutcomeName, &s.HomeName, &s.AwayName, &s.LeagueName,
			&s.PinnacleFairProb,
			&s.PMAsk, &s.PMBid, &s.PMMid,
			&s.PinnacleMarketStatus, &s.PinnacleMarketVersion,
			&s.GrossEdgeToBuy, &s.GrossEdgeToSell,
			&s.SpreadCost, &s.SlippageBuffer, &s.LatencyBuffer, &s.OverroundBuffer, &s.NetEdgeBuy,
			&s.PinnacleAgeSec, &s.PMAgeSec,
			&s.ProposedSizeUSDC, &s.ThresholdNetEdge,
			&stateStr,
		); err != nil {
			return out, fmt.Errorf("scan candidate: %w", err)
		}
		s.State = sigpkg.State(stateStr)
		out = append(out, s)
	}
	return out, rows.Err()
}

// InsertProposedOrder writes one OrderSpec to proposed_orders. The unique
// (matchup, outcome_side, minute) index acts as a backstop against
// burst-firing the same logical order twice; we detect that error here
// and return ErrDuplicate so the caller can keep going.
var ErrDuplicate = errors.New("executor: duplicate proposed order for this (matchup, side, minute)")

func (e *ExecutorStore) InsertProposedOrder(ctx context.Context, spec executor.OrderSpec, notes string) error {
	_, err := e.db.ExecContext(ctx, `
        INSERT INTO proposed_orders (
            source_signal_id, matchup_id, polymarket_condition_id, polymarket_token_id,
            outcome_side, outcome_name, home_name, away_name, league_name,
            side, order_type, size_usdc, price,
            maker_amount_6dec, taker_amount_6dec, expiration_unix, salt, signature_type,
            claimed_net_edge, pinnacle_fair_prob, pinnacle_age_sec, pm_age_sec,
            status, notes
        ) VALUES (
            $1,$2,$3,$4, $5,$6,$7,$8,$9,
            $10,$11,$12,$13,
            $14,$15,$16,$17::numeric,$18,
            $19,$20,$21,$22,
            'planned', $23
        )
    `,
		spec.SourceSignalID, spec.MatchupID, spec.ConditionID, spec.TokenID,
		spec.OutcomeSide, spec.OutcomeName, spec.HomeName, spec.AwayName, spec.LeagueName,
		string(spec.Side), string(spec.OrderType), spec.SizeUSDC, spec.Price,
		spec.MakerAmount.Int64(), spec.TakerAmount.Int64(), spec.Expiration, spec.Salt.String(), int(spec.SignatureType),
		spec.ClaimedNetEdge, spec.PinnacleFairProb, spec.PinnacleAgeSec, spec.PMAgeSec,
		notes,
	)
	if err != nil {
		// pgx surfaces unique-violation messages with SQLSTATE 23505 in the
		// error string. Substring-matching avoids pulling in a pgx-error
		// dependency just for this one branch.
		if isUniqueViolation(err) {
			return ErrDuplicate
		}
		return fmt.Errorf("insert proposed order: %w", err)
	}
	return nil
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	// pgx driver surfaces unique violations as text containing "23505" in
	// the error chain. Cheap-and-safe substring match avoids importing
	// pgx error types just for this.
	msg := err.Error()
	return substr(msg, "duplicate key value") || substr(msg, "23505")
}

func substr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ListProposedOrders returns recent rows for inspection, oldest-first
// within the page so operators see chronological context.
func (e *ExecutorStore) ListProposedOrders(ctx context.Context, since time.Duration, status string, limit int) ([]ProposedOrderRow, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `
        SELECT id, created_at, status, source_signal_id, matchup_id,
               polymarket_condition_id, polymarket_token_id,
               outcome_side, outcome_name, home_name, away_name, league_name,
               side, order_type, size_usdc, price,
               maker_amount_6dec, taker_amount_6dec, expiration_unix, salt,
               claimed_net_edge, pinnacle_fair_prob,
               COALESCE(pinnacle_age_sec, 0)::float8,
               COALESCE(pm_age_sec, 0)::float8,
               notes
          FROM proposed_orders
         WHERE created_at >= $1
    `
	args := []any{time.Now().UTC().Add(-since)}
	if status != "" {
		q += ` AND status = $2`
		args = append(args, status)
	}
	q += fmt.Sprintf(" ORDER BY created_at DESC LIMIT %d", limit)
	rows, err := e.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list proposed orders: %w", err)
	}
	defer rows.Close()
	out := []ProposedOrderRow{}
	for rows.Next() {
		var r ProposedOrderRow
		if err := rows.Scan(
			&r.ID, &r.CreatedAt, &r.Status, &r.SourceSignalID, &r.MatchupID,
			&r.ConditionID, &r.TokenID,
			&r.OutcomeSide, &r.OutcomeName, &r.HomeName, &r.AwayName, &r.LeagueName,
			&r.Side, &r.OrderType, &r.SizeUSDC, &r.Price,
			&r.MakerAmount6Dec, &r.TakerAmount6Dec, &r.ExpirationUnix, &r.Salt,
			&r.ClaimedNetEdge, &r.PinnacleFairProb, &r.PinnacleAgeSec, &r.PMAgeSec,
			&r.Notes,
		); err != nil {
			return out, fmt.Errorf("scan proposed order: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ProposedOrderRow is the inspection view of one row in proposed_orders.
type ProposedOrderRow struct {
	ID               int64
	CreatedAt        time.Time
	Status           string
	SourceSignalID   int64
	MatchupID        int64
	ConditionID      string
	TokenID          string
	OutcomeSide      string
	OutcomeName      string
	HomeName         string
	AwayName         string
	LeagueName       string
	Side             string
	OrderType        string
	SizeUSDC         float64
	Price            float64
	MakerAmount6Dec  int64
	TakerAmount6Dec  int64
	ExpirationUnix   int64
	Salt             string // numeric → text to preserve uint256
	ClaimedNetEdge   float64
	PinnacleFairProb float64
	PinnacleAgeSec   float64
	PMAgeSec         float64
	Notes            string
}
