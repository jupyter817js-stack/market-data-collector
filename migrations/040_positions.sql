-- Phase 9 Position Manager scaffolding. Today clob-fill-monitor flips
-- proposed_orders.status to 'filled' when a CLOB order completes, but
-- nothing aggregates those fills into a position view — so MTM /
-- realized PnL, exit-route comparison, and "should I close?" decisions
-- have nowhere to live. This migration adds:
--
--   positions               : one OPEN row per held token (partial
--                             fills accumulate). Closes via direct
--                             sell, synthetic sell, or merge — the
--                             route is recorded for audit.
--
--   position_exit_routes    : per-open-position view of the three
--                             exit candidates, picks the best, and
--                             reports unrealized PnL at that exit.
--                             Direct = shares × current bid. Synthetic
--                             = shares × (1 − sum of opposite asks);
--                             buying the opposite token(s) gives a
--                             complete set whose merge value is $1.
--                             Works for 2-way AND 3-way (soccer)
--                             because opposite_token_count drives the
--                             ask sum.
--
-- Until Step 7 ships actual fills, the table stays empty and the view
-- returns zero rows — both ship to production with no behavior change.

CREATE TABLE IF NOT EXISTS positions (
    id bigserial PRIMARY KEY,
    polymarket_token_id text NOT NULL,
    polymarket_condition_id text NOT NULL,
    matchup_id bigint,
    outcome_side text NOT NULL DEFAULT '',
    side text NOT NULL DEFAULT 'BUY'
        CHECK (side IN ('BUY','SELL')),
    shares_filled numeric(24,6) NOT NULL DEFAULT 0
        CHECK (shares_filled >= 0),
    cost_usdc numeric(24,6) NOT NULL DEFAULT 0
        CHECK (cost_usdc >= 0),
    avg_entry_price numeric(10,6) GENERATED ALWAYS AS
        (CASE WHEN shares_filled > 0 THEN cost_usdc / shares_filled END) STORED,
    status text NOT NULL DEFAULT 'open'
        CHECK (status IN ('open','closing','closed')),
    opened_at timestamptz NOT NULL DEFAULT now(),
    last_fill_at timestamptz,
    closed_at timestamptz,
    exit_route text NOT NULL DEFAULT ''
        CHECK (exit_route IN ('','direct_sell','synthetic_sell','merge','expired_settled')),
    exit_proceeds_usdc numeric(24,6),
    realized_pnl_usdc numeric(24,6),
    source_proposed_order_id bigint REFERENCES proposed_orders(id) ON DELETE SET NULL,
    notes text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- One open position per held token. closed positions can pile up freely.
CREATE UNIQUE INDEX IF NOT EXISTS positions_one_open_per_token_idx
    ON positions (polymarket_token_id)
    WHERE status IN ('open','closing');

CREATE INDEX IF NOT EXISTS positions_token_idx
    ON positions (polymarket_token_id, opened_at DESC);

CREATE INDEX IF NOT EXISTS positions_status_idx
    ON positions (status, opened_at DESC);

CREATE INDEX IF NOT EXISTS positions_condition_idx
    ON positions (polymarket_condition_id, opened_at DESC);

CREATE OR REPLACE VIEW position_exit_routes AS
WITH open_pos AS (
    SELECT * FROM positions WHERE status IN ('open','closing')
),
held_token_price AS (
    SELECT DISTINCT ON (token_id)
        token_id, bid_price, ask_price, mid_price, received_at
      FROM polymarket_price_snapshots
     ORDER BY token_id, received_at DESC
),
opposite_asks AS (
    SELECT p.id AS position_id,
           SUM(COALESCE(htp.ask_price, 1))::numeric(10,6) AS sum_opposite_ask,
           COUNT(pt.token_id)                              AS opposite_token_count
      FROM open_pos p
      JOIN polymarket_tokens pt
        ON pt.condition_id = p.polymarket_condition_id
       AND pt.token_id <> p.polymarket_token_id
      LEFT JOIN held_token_price htp ON htp.token_id = pt.token_id
     GROUP BY p.id
)
SELECT
    p.id AS position_id,
    p.polymarket_token_id,
    p.polymarket_condition_id,
    p.matchup_id,
    p.outcome_side,
    p.shares_filled,
    p.cost_usdc,
    p.avg_entry_price,
    p.status,
    p.opened_at,
    htp.bid_price       AS held_bid,
    htp.ask_price       AS held_ask,
    htp.mid_price       AS held_mid,
    htp.received_at     AS held_price_at,
    oa.sum_opposite_ask,
    oa.opposite_token_count,
    -- Direct sell: shares × current bid.
    (p.shares_filled * COALESCE(htp.bid_price, 0))::numeric(24,6) AS direct_sell_value,
    -- Synthetic sell: buy a complete set of opposite tokens at their
    -- asks, then redeem the now-complete YES+NO bundle for $1 each.
    -- Net = shares × (1 − sum_opposite_ask). Clamp at 0 so an
    -- overpriced opposite ladder doesn't show negative proceeds.
    (p.shares_filled * GREATEST(0, 1 - COALESCE(oa.sum_opposite_ask, 1)))::numeric(24,6)
        AS synthetic_sell_value,
    CASE
        WHEN (p.shares_filled * COALESCE(htp.bid_price, 0))
           >= (p.shares_filled * GREATEST(0, 1 - COALESCE(oa.sum_opposite_ask, 1)))
            THEN 'direct_sell'
        ELSE 'synthetic_sell'
    END AS recommended_exit,
    GREATEST(
        p.shares_filled * COALESCE(htp.bid_price, 0),
        p.shares_filled * GREATEST(0, 1 - COALESCE(oa.sum_opposite_ask, 1))
    )::numeric(24,6) AS best_exit_value,
    (GREATEST(
        p.shares_filled * COALESCE(htp.bid_price, 0),
        p.shares_filled * GREATEST(0, 1 - COALESCE(oa.sum_opposite_ask, 1))
    ) - p.cost_usdc)::numeric(24,6) AS best_unrealized_pnl_usdc
  FROM open_pos p
  LEFT JOIN held_token_price htp ON htp.token_id = p.polymarket_token_id
  LEFT JOIN opposite_asks    oa  ON oa.position_id = p.id;
