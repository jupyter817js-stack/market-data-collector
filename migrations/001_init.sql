CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS canonical_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sport_key text NOT NULL,
    league text NOT NULL DEFAULT '',
    home_team text NOT NULL,
    away_team text NOT NULL,
    home_team_norm text NOT NULL,
    away_team_norm text NOT NULL,
    commence_time timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'scheduled',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (sport_key, home_team_norm, away_team_norm, commence_time)
);

CREATE TABLE IF NOT EXISTS source_event_aliases (
    source text NOT NULL,
    source_event_id text NOT NULL,
    canonical_event_id uuid NOT NULL REFERENCES canonical_events(id) ON DELETE CASCADE,
    sport_key text NOT NULL,
    raw_home_team text NOT NULL,
    raw_away_team text NOT NULL,
    raw_commence_time timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, source_event_id)
);

CREATE TABLE IF NOT EXISTS raw_snapshots (
    id bigserial PRIMARY KEY,
    source text NOT NULL,
    request_url text NOT NULL,
    received_at timestamptz NOT NULL,
    status_code int NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload jsonb NOT NULL,
    payload_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS raw_snapshots_source_received_idx
    ON raw_snapshots (source, received_at DESC);

CREATE TABLE IF NOT EXISTS quote_history (
    id bigserial PRIMARY KEY,
    canonical_event_id uuid NOT NULL REFERENCES canonical_events(id) ON DELETE CASCADE,
    source text NOT NULL,
    source_event_id text NOT NULL,
    bookmaker_key text NOT NULL,
    bookmaker_title text NOT NULL,
    market_key text NOT NULL,
    outcome_name text NOT NULL,
    outcome_side text NOT NULL,
    line numeric(12,4),
    price_decimal numeric(12,6) NOT NULL,
    source_last_update timestamptz,
    received_at timestamptz NOT NULL,
    written_at timestamptz NOT NULL DEFAULT now(),
    in_play boolean NOT NULL,
    age_ms bigint,
    stale_state text NOT NULL,
    confidence_score numeric(5,2) NOT NULL,
    quote_key text NOT NULL,
    quote_hash text NOT NULL,
    raw_snapshot_id bigint NOT NULL REFERENCES raw_snapshots(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS quote_history_event_time_idx
    ON quote_history (canonical_event_id, written_at DESC);

CREATE INDEX IF NOT EXISTS quote_history_key_time_idx
    ON quote_history (quote_key, written_at DESC);

CREATE INDEX IF NOT EXISTS quote_history_source_book_market_idx
    ON quote_history (source, bookmaker_key, market_key, written_at DESC);

CREATE TABLE IF NOT EXISTS latest_quotes (
    quote_key text PRIMARY KEY,
    quote_history_id bigint NOT NULL REFERENCES quote_history(id) ON DELETE CASCADE,
    quote_hash text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW fair_value_ready_quotes AS
SELECT
    ce.sport_key,
    ce.league,
    ce.id AS canonical_event_id,
    ce.home_team,
    ce.away_team,
    ce.commence_time,
    q.source,
    q.source_event_id,
    q.bookmaker_key,
    q.bookmaker_title,
    q.market_key,
    q.outcome_name,
    q.outcome_side,
    q.line,
    q.price_decimal,
    q.source_last_update,
    q.received_at,
    q.written_at,
    q.in_play,
    q.age_ms,
    q.stale_state,
    q.confidence_score,
    q.raw_snapshot_id
FROM latest_quotes lq
JOIN quote_history q ON q.id = lq.quote_history_id
JOIN canonical_events ce ON ce.id = q.canonical_event_id;
