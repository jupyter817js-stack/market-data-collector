-- Planner-only audit table. cmd/executor-plan reads
-- signals_buy_candidates_fresh, builds the order body that WOULD be
-- submitted to https://clob.polymarket.com/order, and records it here. No
-- actual on-chain signing or HTTP submission yet — those land in a later
-- commit alongside wallet, EIP-712, and HMAC L2 auth code.

CREATE TABLE IF NOT EXISTS proposed_orders (
    id bigserial PRIMARY KEY,
    -- provenance
    source_signal_id bigint NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
    matchup_id bigint NOT NULL,
    polymarket_condition_id text NOT NULL,
    polymarket_token_id text NOT NULL,
    outcome_side text NOT NULL,
    outcome_name text NOT NULL DEFAULT '',
    home_name text NOT NULL DEFAULT '',
    away_name text NOT NULL DEFAULT '',
    league_name text NOT NULL DEFAULT '',

    -- order body fields that would be POSTed
    side text NOT NULL,                          -- BUY | SELL
    order_type text NOT NULL DEFAULT 'FAK',      -- GTC | FOK | GTD | FAK
    size_usdc numeric(12,4) NOT NULL,
    price numeric(10,6) NOT NULL,                -- entry price (signal pm_ask for BUY)
    maker_amount_6dec bigint NOT NULL,           -- as in JSON body
    taker_amount_6dec bigint NOT NULL,
    expiration_unix bigint NOT NULL DEFAULT 0,   -- 0 = GTC no-expiry
    salt numeric(80) NOT NULL,                   -- uint256; numeric to hold 78 digits
    signature_type smallint NOT NULL DEFAULT 0,  -- 0=EOA

    -- snapshot of why we'd do this (decision audit)
    claimed_net_edge numeric(10,6) NOT NULL,
    pinnacle_fair_prob numeric(10,6) NOT NULL,
    pinnacle_age_sec numeric(10,3),
    pm_age_sec numeric(10,3),

    status text NOT NULL DEFAULT 'planned',      -- planned | sent | rejected | canceled | filled
    notes text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS proposed_orders_signal_idx
    ON proposed_orders (source_signal_id);
CREATE INDEX IF NOT EXISTS proposed_orders_matchup_idx
    ON proposed_orders (matchup_id, created_at DESC);
CREATE INDEX IF NOT EXISTS proposed_orders_status_idx
    ON proposed_orders (status, created_at DESC);
CREATE INDEX IF NOT EXISTS proposed_orders_token_idx
    ON proposed_orders (polymarket_token_id, created_at DESC);

-- Anti-spam dedupe is handled at the application layer (cmd/executor-plan
-- catches ErrDuplicate from the store). A partial unique index keyed on
-- date_trunc(minute, timestamptz) would be the ideal backstop but Postgres
-- rejects it because timezone-dependent functions are not IMMUTABLE and
-- index expressions must be. Application-only is fine for our single-
-- writer planner; revisit if multiple planners ever run concurrently.
