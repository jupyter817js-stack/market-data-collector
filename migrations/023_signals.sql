-- Dry-run signal log. The signal engine reads pinnacle_polymarket_cs2_moneyline_edges
-- on a polling cadence, computes net edge, classifies state, and appends one
-- row here per (matchup, outcome) per tick. No execution is gated by this
-- table — it is purely an audit trail for paper-PnL validation. Step 5
-- introduces an executor that reads only the latest BUY_CANDIDATE rows.

CREATE TABLE IF NOT EXISTS signals (
    id bigserial PRIMARY KEY,

    -- identity
    matchup_id bigint NOT NULL,
    polymarket_condition_id text NOT NULL,
    polymarket_token_id text,
    outcome_side text NOT NULL,
    outcome_name text NOT NULL DEFAULT '',
    home_name text NOT NULL DEFAULT '',
    away_name text NOT NULL DEFAULT '',
    league_name text NOT NULL DEFAULT '',

    -- inputs
    pinnacle_fair_prob numeric(10,6),
    pm_ask numeric(10,6),
    pm_bid numeric(10,6),
    pm_mid numeric(10,6),
    pm_spread numeric(10,6),
    pm_liquidity numeric(14,2),
    pinnacle_market_status text NOT NULL DEFAULT '',
    pinnacle_market_version bigint,
    pinnacle_overround numeric(10,6),

    -- gross / cost / net edge
    gross_edge_to_buy numeric(10,6),
    gross_edge_to_sell numeric(10,6),
    spread_cost numeric(10,6),
    slippage_buffer numeric(10,6),
    latency_buffer numeric(10,6),
    overround_buffer numeric(10,6),
    net_edge_buy numeric(10,6),

    -- freshness
    pinnacle_age_sec numeric(10,3),
    pm_age_sec numeric(10,3),

    -- sizing + classification
    proposed_size_usdc numeric(10,2),
    threshold_net_edge numeric(10,6),
    state text NOT NULL,
    reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
    edge_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,

    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS signals_matchup_idx ON signals (matchup_id, created_at DESC);
CREATE INDEX IF NOT EXISTS signals_state_idx ON signals (state, created_at DESC);
CREATE INDEX IF NOT EXISTS signals_created_idx ON signals (created_at DESC);
CREATE INDEX IF NOT EXISTS signals_token_idx ON signals (polymarket_token_id, created_at DESC);

-- Per-tick telemetry. Useful for paper-PnL replay and rate-limit tuning.
CREATE TABLE IF NOT EXISTS signal_runs (
    id bigserial PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    edge_rows_seen int NOT NULL DEFAULT 0,
    signals_emitted int NOT NULL DEFAULT 0,
    state_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_message text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'running'
);

CREATE INDEX IF NOT EXISTS signal_runs_started_idx ON signal_runs (started_at DESC);

-- Latest signal per (matchup, outcome_side) — what an executor would consult.
CREATE OR REPLACE VIEW signals_latest_per_outcome AS
SELECT DISTINCT ON (matchup_id, outcome_side)
    id,
    matchup_id, polymarket_condition_id, polymarket_token_id,
    outcome_side, outcome_name, home_name, away_name, league_name,
    pinnacle_fair_prob, pm_ask, pm_bid, pm_mid,
    pinnacle_market_status, pinnacle_market_version,
    gross_edge_to_buy, gross_edge_to_sell, net_edge_buy,
    spread_cost, slippage_buffer, latency_buffer, overround_buffer,
    pinnacle_age_sec, pm_age_sec,
    proposed_size_usdc, threshold_net_edge,
    state, reasons,
    created_at
FROM signals
ORDER BY matchup_id, outcome_side, created_at DESC;

-- BUY candidates currently fresh (executor read target — Step 5).
CREATE OR REPLACE VIEW signals_buy_candidates_fresh AS
SELECT *
FROM signals_latest_per_outcome
WHERE state = 'BUY_CANDIDATE'
  AND pinnacle_age_sec <= 15
  AND pm_age_sec <= 15;
