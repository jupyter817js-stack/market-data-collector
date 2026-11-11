-- Paper-PnL audit. Step 4 produces signals; this layer answers "if I had
-- traded every BUY_CANDIDATE at its recorded PM ask, where would I be now?"
-- v0 is mark-to-market only — realized PnL requires Polymarket resolution
-- data which is not yet ingested. Add a paper_pnl_realized layer later.
--
-- No paper_positions table: positions are derived deterministically from
-- the signals stream + the simulator config (first BUY_CANDIDATE per
-- (matchup, outcome_side) wins, single fill at the recorded pm_ask). The
-- simulator re-computes on demand so changing the config (size, dedupe
-- window) doesn't require schema migration.

CREATE TABLE IF NOT EXISTS paper_pnl_runs (
    id bigserial PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    -- input window
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    -- config snapshot
    proposed_size_usdc numeric(10,2) NOT NULL,
    dedupe_per_matchup_outcome boolean NOT NULL DEFAULT true,
    -- counters
    candidates_seen int NOT NULL DEFAULT 0,
    trades_simulated int NOT NULL DEFAULT 0,
    trades_with_mtm int NOT NULL DEFAULT 0,
    trades_unpriced int NOT NULL DEFAULT 0,
    -- aggregates
    total_size_usdc numeric(14,2) NOT NULL DEFAULT 0,
    total_mtm_pnl_usdc numeric(14,4) NOT NULL DEFAULT 0,
    avg_claimed_net_edge_pp numeric(10,4) NOT NULL DEFAULT 0,
    avg_mtm_edge_pp numeric(10,4) NOT NULL DEFAULT 0,
    notes text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'running'
);

CREATE INDEX IF NOT EXISTS paper_pnl_runs_started_idx ON paper_pnl_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS paper_pnl_runs_window_idx ON paper_pnl_runs (window_start, window_end);
