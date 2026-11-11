-- Phase 9 (Exit half): per-tick classification for open positions. The
-- BUY side has signals + proposed_orders; the EXIT side now mirrors
-- that with exit_signals (decision audit) wired to positions. An exit
-- decision is "should we close this open position right now, by which
-- route, and why" — independent of whether the operator has armed the
-- exit executor yet (still gated on Step 7).
--
-- Multiple exit_signals rows per position over time (tick-by-tick
-- evaluation), latest row is the live decision. The state column drives
-- the future clob-exit-submit / clob-exit-execute daemons; until those
-- ship the rows are read-only telemetry.

CREATE TABLE IF NOT EXISTS exit_signals (
    id bigserial PRIMARY KEY,
    position_id bigint NOT NULL REFERENCES positions(id) ON DELETE CASCADE,
    state text NOT NULL CHECK (state IN (
        'HOLD',
        'EXIT_PROFIT',
        'EXIT_STOP_LOSS',
        'EXIT_TIME',
        'EXIT_LIQUIDITY_RISK',
        'EXIT_MARKET_SETTLED'
    )),
    recommended_route text NOT NULL DEFAULT 'direct_sell'
        CHECK (recommended_route IN ('direct_sell','synthetic_sell','merge','expired_settled')),
    expected_proceeds_usdc numeric(24,6),
    current_unrealized_pnl_usdc numeric(24,6),
    unrealized_pnl_pct numeric(10,4),
    held_bid numeric(10,6),
    held_ask numeric(10,6),
    sum_opposite_ask numeric(10,6),
    position_age_seconds bigint,
    reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS exit_signals_position_idx
    ON exit_signals (position_id, created_at DESC);

-- Most-recent non-HOLD lookup for the (future) executor + dashboard.
CREATE INDEX IF NOT EXISTS exit_signals_state_idx
    ON exit_signals (state, created_at DESC)
    WHERE state <> 'HOLD';

-- The latest decision per open position, joined to the exit route view
-- so the dashboard / operator can answer "what's the system saying about
-- my open positions right now?" with a single query.
CREATE OR REPLACE VIEW exit_signals_latest AS
SELECT DISTINCT ON (es.position_id)
    es.id, es.position_id, es.state, es.recommended_route,
    es.expected_proceeds_usdc, es.current_unrealized_pnl_usdc,
    es.unrealized_pnl_pct, es.held_bid, es.held_ask,
    es.sum_opposite_ask, es.position_age_seconds,
    es.reasons, es.created_at,
    p.polymarket_token_id, p.polymarket_condition_id, p.matchup_id,
    p.outcome_side, p.shares_filled, p.cost_usdc, p.opened_at
FROM exit_signals es
JOIN positions p ON p.id = es.position_id
WHERE p.status IN ('open','closing')
ORDER BY es.position_id, es.created_at DESC;
