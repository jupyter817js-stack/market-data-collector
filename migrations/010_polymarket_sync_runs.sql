CREATE TABLE IF NOT EXISTS polymarket_sync_runs (
    id bigserial PRIMARY KEY,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    sync_type text NOT NULL,
    status text NOT NULL CHECK (status IN ('success', 'error', 'canceled')),
    pages_fetched int NOT NULL DEFAULT 0 CHECK (pages_fetched >= 0),
    events_seen int NOT NULL DEFAULT 0 CHECK (events_seen >= 0),
    markets_seen int NOT NULL DEFAULT 0 CHECK (markets_seen >= 0),
    tokens_seen int NOT NULL DEFAULT 0 CHECK (tokens_seen >= 0),
    prices_fetched int NOT NULL DEFAULT 0 CHECK (prices_fetched >= 0),
    errors_count int NOT NULL DEFAULT 0 CHECK (errors_count >= 0),
    error_message text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS polymarket_sync_runs_type_started_idx
    ON polymarket_sync_runs (sync_type, started_at DESC);

CREATE INDEX IF NOT EXISTS polymarket_sync_runs_status_started_idx
    ON polymarket_sync_runs (status, started_at DESC);
