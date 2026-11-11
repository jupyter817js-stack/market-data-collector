-- Paper baseball trading signal audit + latency telemetry.
--
-- One row per (LSPORTS score-change event) we considered. Records:
--   1. Originating LSPORTS event (fixture, score before/after, scoring side).
--   2. PM token chosen (target token + outcome name + market id).
--   3. PM book snapshot at signal time (best ask/bid, midpoint).
--   4. Five-stage latency breakdown in milliseconds (AMQP delivery, daemon
--      processing, token lookup, CLOB snapshot, signal insert).
--   5. Link to the signals(id) row inserted into the existing signal pipeline
--      so cmd/clob-auto-submit -really-submit=false can pick it up and write
--      proposed_orders('planned') without any network call.
--
-- This is paper trading only. No order is sent to Polymarket from this code
-- path. The companion cmd/clob-auto-submit is the only daemon that could,
-- and only when explicitly run with -really-submit=true (which is NOT how
-- the paper trading workflow runs it).

CREATE TABLE IF NOT EXISTS paper_baseball_signals (
    id bigserial PRIMARY KEY,
    lsports_fixture_id bigint NOT NULL,

    -- LSPORTS event detail
    lsports_event_at timestamptz NOT NULL,
    lsports_source_publish_at timestamptz,
    score_before text NOT NULL,
    score_after text NOT NULL,
    scoring_side text NOT NULL CHECK (scoring_side IN ('home','away')),
    home_team_name text NOT NULL DEFAULT '',
    away_team_name text NOT NULL DEFAULT '',

    -- PM token mapping target
    target_polymarket_market_id text,
    target_polymarket_condition_id text NOT NULL,
    target_polymarket_token_id text NOT NULL,
    target_outcome_name text NOT NULL DEFAULT '',
    target_outcome_side text NOT NULL DEFAULT '',

    -- PM book snapshot at signal time
    pm_best_ask numeric(12,6),
    pm_best_bid numeric(12,6),
    pm_midpoint numeric(12,6),
    pm_spread numeric(12,6),
    pm_liquidity numeric(14,2),
    pm_snapshot_at timestamptz NOT NULL,

    -- Result
    signal_id bigint REFERENCES signals(id),
    signal_skip_reason text NOT NULL DEFAULT '',

    -- Latency stages (ms)
    lsports_to_daemon_ms integer NOT NULL DEFAULT 0,
    daemon_to_token_lookup_ms integer NOT NULL DEFAULT 0,
    token_lookup_to_pm_snapshot_ms integer NOT NULL DEFAULT 0,
    pm_snapshot_to_signal_insert_ms integer NOT NULL DEFAULT 0,
    total_signal_latency_ms integer NOT NULL DEFAULT 0,

    raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS paper_baseball_signals_fix_time_idx
    ON paper_baseball_signals (lsports_fixture_id, lsports_event_at DESC);

CREATE INDEX IF NOT EXISTS paper_baseball_signals_signal_idx
    ON paper_baseball_signals (signal_id)
    WHERE signal_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS paper_baseball_signals_created_idx
    ON paper_baseball_signals (created_at DESC);

-- Per-signal MTM P&L view. Joins signal to the first PM observation
-- recorded AT LEAST 5 minutes after the signal, computes (exit_mid -
-- entry_mid) * size_usdc / entry_mid as a long-position P&L. NULL exit
-- price means the match ended (or PM watcher stopped) before the 5m
-- window expired — surface those for inspection rather than dropping.
CREATE OR REPLACE VIEW paper_baseball_pnl_5min AS
SELECT
    pbs.id AS paper_signal_id,
    pbs.lsports_fixture_id,
    pbs.signal_id,
    pbs.target_polymarket_token_id,
    pbs.target_outcome_name,
    pbs.score_before,
    pbs.score_after,
    pbs.scoring_side,
    pbs.pm_midpoint AS entry_midpoint,
    pbs.pm_snapshot_at AS entry_at,
    (SELECT plo.midpoint
       FROM polymarket_latency_observations plo
      WHERE plo.lsports_fixture_id  = pbs.lsports_fixture_id
        AND plo.polymarket_token_id = pbs.target_polymarket_token_id
        AND plo.received_at >= pbs.pm_snapshot_at + interval '5 minutes'
      ORDER BY plo.received_at
      LIMIT 1) AS exit_midpoint_5min,
    (SELECT plo.received_at
       FROM polymarket_latency_observations plo
      WHERE plo.lsports_fixture_id  = pbs.lsports_fixture_id
        AND plo.polymarket_token_id = pbs.target_polymarket_token_id
        AND plo.received_at >= pbs.pm_snapshot_at + interval '5 minutes'
      ORDER BY plo.received_at
      LIMIT 1) AS exit_at,
    10.0::numeric(10,2) AS notional_usdc,
    pbs.total_signal_latency_ms,
    pbs.signal_skip_reason,
    pbs.created_at
FROM paper_baseball_signals pbs;
