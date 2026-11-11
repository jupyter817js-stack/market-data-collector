package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/your-org/pol-odds-collector/internal/executor"
)

// ProposedOrderStore writes the audit trail for orders the executor
// either planned, submitted, or had rejected. Status enum:
//
//	planned    — built and signed locally; not yet POSTed
//	sent       — POSTed to /order, server returned success
//	rejected   — POST failed (network or server-side rejection)
//	filled     — fill monitor saw the order matched (future phase)
//	canceled   — operator/system canceled before fill (future phase)
type ProposedOrderStore struct{ db *sql.DB }

// NewProposedOrderStore wires onto an existing *PostgresStore connection.
func NewProposedOrderStore(s *PostgresStore) *ProposedOrderStore {
	return &ProposedOrderStore{db: s.db}
}

// InsertPlanned writes a row with status='planned'. Returns the inserted
// id. Caller should immediately attempt SubmitOrder; on success/failure
// call UpdateStatus to advance the row.
func (p *ProposedOrderStore) InsertPlanned(ctx context.Context, spec executor.OrderSpec, notes string) (int64, error) {
	if spec.SourceSignalID <= 0 {
		return 0, fmt.Errorf("store: proposed_orders: SourceSignalID required (got %d)", spec.SourceSignalID)
	}
	if spec.MakerAmount == nil || spec.TakerAmount == nil || spec.Salt == nil {
		return 0, fmt.Errorf("store: proposed_orders: nil big.Int field")
	}
	var id int64
	err := p.db.QueryRowContext(ctx, `
        INSERT INTO proposed_orders (
            source_signal_id, matchup_id,
            polymarket_condition_id, polymarket_token_id,
            outcome_side, outcome_name, home_name, away_name, league_name,
            side, order_type, size_usdc, price,
            maker_amount_6dec, taker_amount_6dec, expiration_unix,
            salt, signature_type,
            claimed_net_edge, pinnacle_fair_prob,
            pinnacle_age_sec, pm_age_sec,
            status, notes
        ) VALUES (
            $1, $2,
            $3, $4,
            $5, $6, $7, $8, $9,
            $10, $11, $12, $13,
            $14, $15, $16,
            $17, $18,
            $19, $20,
            $21, $22,
            'planned', $23
        )
        RETURNING id
    `,
		spec.SourceSignalID, spec.MatchupID,
		spec.ConditionID, spec.TokenID,
		spec.OutcomeSide, spec.OutcomeName, spec.HomeName, spec.AwayName, spec.LeagueName,
		string(spec.Side), string(spec.OrderType), spec.SizeUSDC, spec.Price,
		spec.MakerAmount.Int64(), spec.TakerAmount.Int64(), spec.Expiration,
		spec.Salt.String(), int(spec.SignatureType),
		spec.ClaimedNetEdge, spec.PinnacleFairProb,
		spec.PinnacleAgeSec, spec.PMAgeSec,
		notes,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("store: insert proposed_orders: %w", err)
	}
	return id, nil
}

// UpdateStatus advances a row's status and appends to notes. status must
// be one of the values allowed by the column's implicit enum.
func (p *ProposedOrderStore) UpdateStatus(ctx context.Context, id int64, status, noteAppend string) error {
	switch status {
	case "planned", "sent", "rejected", "filled", "canceled":
	default:
		return fmt.Errorf("store: UpdateStatus: bad status %q", status)
	}
	res, err := p.db.ExecContext(ctx, `
        UPDATE proposed_orders
           SET status = $2,
               notes  = CASE WHEN $3 = '' THEN notes
                             WHEN notes = '' THEN $3
                             ELSE notes || E'\n' || $3
                        END
         WHERE id = $1
    `, id, status, noteAppend)
	if err != nil {
		return fmt.Errorf("store: update proposed_orders %d: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("store: update proposed_orders %d: %d rows affected", id, n)
	}
	return nil
}

// RecordSent transitions planned → sent and stamps clob_order_id +
// last_status_check_at. The clob_order_id is what the fill monitor
// uses to poll GET /data/order/{id}.
func (p *ProposedOrderStore) RecordSent(ctx context.Context, id int64, clobOrderID, noteAppend string) error {
	if clobOrderID == "" {
		return fmt.Errorf("store: RecordSent: empty clob_order_id")
	}
	res, err := p.db.ExecContext(ctx, `
        UPDATE proposed_orders
           SET status = 'sent',
               clob_order_id = $2,
               last_status_check_at = now(),
               notes = CASE WHEN $3 = '' THEN notes
                            WHEN notes = '' THEN $3
                            ELSE notes || E'\n' || $3
                       END
         WHERE id = $1
           AND status = 'planned'
    `, id, clobOrderID, noteAppend)
	if err != nil {
		return fmt.Errorf("store: RecordSent %d: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("store: RecordSent %d: %d rows affected (was the row already past 'planned'?)", id, n)
	}
	return nil
}

// InFlightOrder is the projection the fill monitor needs to poll one row
// AND write any resulting fill into the positions table (matchup_id,
// token, condition, outcome_side are all required for that join).
type InFlightOrder struct {
	ID                    int64
	ClobOrderID           string
	SignalID              int64
	CreatedAt             time.Time
	LastCheckedAt         sql.NullTime
	MatchupID             int64
	PolymarketTokenID     string
	PolymarketConditionID string
	OutcomeSide           string
	Side                  string // BUY|SELL
}

// ListInFlight returns rows with status='sent' (and a non-empty
// clob_order_id) that have either never been checked or whose last
// check is older than `staleAfter`. Ordered NULLS FIRST then by oldest
// check time so newly-sent orders get attention first.
//
// Caller limits the page size via `limit` to keep one tick cheap; the
// CLOB rate-limits GET /data/order at a few requests per second per key.
func (p *ProposedOrderStore) ListInFlight(ctx context.Context, staleAfter time.Duration, limit int) ([]InFlightOrder, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := p.db.QueryContext(ctx, `
        SELECT id, clob_order_id, source_signal_id, created_at, last_status_check_at,
               matchup_id, polymarket_token_id, polymarket_condition_id,
               outcome_side, side
          FROM proposed_orders
         WHERE status = 'sent'
           AND clob_order_id <> ''
           AND (last_status_check_at IS NULL
                OR last_status_check_at < now() - ($1::text)::interval)
         ORDER BY last_status_check_at NULLS FIRST, created_at
         LIMIT $2
    `, fmt.Sprintf("%d seconds", int(staleAfter.Seconds())), limit)
	if err != nil {
		return nil, fmt.Errorf("store: ListInFlight: %w", err)
	}
	defer rows.Close()
	var out []InFlightOrder
	for rows.Next() {
		var r InFlightOrder
		if err := rows.Scan(&r.ID, &r.ClobOrderID, &r.SignalID, &r.CreatedAt, &r.LastCheckedAt,
			&r.MatchupID, &r.PolymarketTokenID, &r.PolymarketConditionID,
			&r.OutcomeSide, &r.Side); err != nil {
			return nil, fmt.Errorf("store: ListInFlight scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// BumpStatusCheck advances last_status_check_at without changing status.
// Used when a poll returned a non-terminal status (still live/delayed).
func (p *ProposedOrderStore) BumpStatusCheck(ctx context.Context, id int64, noteAppend string) error {
	_, err := p.db.ExecContext(ctx, `
        UPDATE proposed_orders
           SET last_status_check_at = now(),
               notes = CASE WHEN $2 = '' THEN notes
                            WHEN notes = '' THEN $2
                            ELSE notes || E'\n' || $2
                       END
         WHERE id = $1
    `, id, noteAppend)
	if err != nil {
		return fmt.Errorf("store: BumpStatusCheck %d: %w", id, err)
	}
	return nil
}

// ExposureForToken sums size_usdc for non-rejected/canceled rows on
// (tokenID, side) created in the last window. This is the live
// open-position estimate used by the risk gate to cap per-outcome
// exposure. 'planned' is included because it's already-signed-and-
// audited — about to be submitted.
func (p *ProposedOrderStore) ExposureForToken(ctx context.Context, tokenID, side string, window time.Duration) (float64, error) {
	var total sql.NullFloat64
	err := p.db.QueryRowContext(ctx, `
        SELECT COALESCE(SUM(size_usdc), 0)::float8
          FROM proposed_orders
         WHERE polymarket_token_id = $1
           AND side = $2
           AND status IN ('planned','sent','filled')
           AND created_at > now() - ($3::text)::interval
    `, tokenID, side, fmt.Sprintf("%d seconds", int(window.Seconds()))).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("store: ExposureForToken: %w", err)
	}
	return total.Float64, nil
}

// TotalExposure sums size_usdc across all non-rejected/canceled rows in
// the window. Used to cap aggregate live exposure (defense against
// many small orders adding up to a big number).
func (p *ProposedOrderStore) TotalExposure(ctx context.Context, window time.Duration) (float64, error) {
	var total sql.NullFloat64
	err := p.db.QueryRowContext(ctx, `
        SELECT COALESCE(SUM(size_usdc), 0)::float8
          FROM proposed_orders
         WHERE status IN ('planned','sent','filled')
           AND created_at > now() - ($1::text)::interval
    `, fmt.Sprintf("%d seconds", int(window.Seconds()))).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("store: TotalExposure: %w", err)
	}
	return total.Float64, nil
}

// CountOrdersInWindow returns the number of proposed_orders rows
// (any status except canceled/rejected) created in the window. The
// risk gate uses this for a coarse "X orders / hour" cap.
func (p *ProposedOrderStore) CountOrdersInWindow(ctx context.Context, window time.Duration) (int, error) {
	var n int
	err := p.db.QueryRowContext(ctx, `
        SELECT count(*)
          FROM proposed_orders
         WHERE status NOT IN ('rejected','canceled')
           AND created_at > now() - ($1::text)::interval
    `, fmt.Sprintf("%d seconds", int(window.Seconds()))).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: CountOrdersInWindow: %w", err)
	}
	return n, nil
}

// HasRecentOrderFor reports whether a non-rejected row already exists
// for (matchup, token, side) within the cooldown window. The
// always-on submitter uses this to dedupe — without it, a signal that
// stays BUY_CANDIDATE across many signal-engine ticks would fire one
// order per tick.
func (p *ProposedOrderStore) HasRecentOrderFor(ctx context.Context, matchupID int64, tokenID, side string, window time.Duration) (bool, error) {
	var present bool
	err := p.db.QueryRowContext(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM proposed_orders
             WHERE matchup_id = $1
               AND polymarket_token_id = $2
               AND side = $3
               AND status NOT IN ('rejected','canceled')
               AND created_at > now() - ($4::text)::interval
        )
    `, matchupID, tokenID, side, fmt.Sprintf("%d seconds", int(window.Seconds()))).Scan(&present)
	if err != nil {
		return false, fmt.Errorf("store: HasRecentOrderFor: %w", err)
	}
	return present, nil
}

// MarkTerminal transitions a sent row to a terminal status (filled,
// canceled, rejected) and stamps last_status_check_at. Status enum
// is enforced; only allowed terminals are accepted (not 'planned' /
// 'sent', which are non-terminal).
func (p *ProposedOrderStore) MarkTerminal(ctx context.Context, id int64, status, noteAppend string) error {
	switch status {
	case "filled", "canceled", "rejected":
	default:
		return fmt.Errorf("store: MarkTerminal: non-terminal status %q", status)
	}
	res, err := p.db.ExecContext(ctx, `
        UPDATE proposed_orders
           SET status = $2,
               last_status_check_at = now(),
               notes = CASE WHEN $3 = '' THEN notes
                            WHEN notes = '' THEN $3
                            ELSE notes || E'\n' || $3
                       END
         WHERE id = $1
    `, id, status, noteAppend)
	if err != nil {
		return fmt.Errorf("store: MarkTerminal %d: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("store: MarkTerminal %d: %d rows affected", id, n)
	}
	return nil
}
