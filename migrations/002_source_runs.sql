CREATE TABLE IF NOT EXISTS source_runs (
    id bigserial PRIMARY KEY,
    source text NOT NULL,
    sport_key text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    status text NOT NULL CHECK (status IN ('success', 'error', 'canceled')),
    status_code int,
    request_url text NOT NULL DEFAULT '',
    response_headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_message text NOT NULL DEFAULT '',
    events_seen int NOT NULL DEFAULT 0 CHECK (events_seen >= 0),
    events_skipped_live int NOT NULL DEFAULT 0 CHECK (events_skipped_live >= 0),
    quotes_seen int NOT NULL DEFAULT 0 CHECK (quotes_seen >= 0),
    quotes_inserted int NOT NULL DEFAULT 0 CHECK (quotes_inserted >= 0),
    quotes_unchanged int NOT NULL DEFAULT 0 CHECK (quotes_unchanged >= 0),
    raw_snapshots_inserted int NOT NULL DEFAULT 0 CHECK (raw_snapshots_inserted >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS source_runs_source_sport_started_idx
    ON source_runs (source, sport_key, started_at DESC);

CREATE INDEX IF NOT EXISTS source_runs_status_started_idx
    ON source_runs (status, started_at DESC);
