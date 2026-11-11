CREATE TABLE IF NOT EXISTS manual_event_aliases (
    id bigserial PRIMARY KEY,
    source text NOT NULL,
    source_event_id text NOT NULL,
    source_market_id text NOT NULL DEFAULT '',
    source_condition_id text NOT NULL DEFAULT '',
    canonical_event_id uuid NOT NULL REFERENCES canonical_events(id) ON DELETE CASCADE,
    sport_key text NOT NULL DEFAULT '',
    reason text NOT NULL DEFAULT '',
    created_by text NOT NULL DEFAULT 'manual',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, source_event_id, source_market_id, source_condition_id)
);

CREATE INDEX IF NOT EXISTS manual_event_aliases_source_event_idx
    ON manual_event_aliases (source, source_event_id);

CREATE INDEX IF NOT EXISTS manual_event_aliases_canonical_event_idx
    ON manual_event_aliases (canonical_event_id);

CREATE TABLE IF NOT EXISTS event_match_reviews (
    id bigserial PRIMARY KEY,
    source text NOT NULL,
    source_event_id text NOT NULL,
    source_market_id text NOT NULL DEFAULT '',
    source_condition_id text NOT NULL DEFAULT '',
    sport_key text NOT NULL DEFAULT '',
    league text NOT NULL DEFAULT '',
    event_title text NOT NULL DEFAULT '',
    raw_home_team text NOT NULL DEFAULT '',
    raw_away_team text NOT NULL DEFAULT '',
    raw_start_time timestamptz,
    match_status text NOT NULL,
    confidence_score numeric(5,2),
    reason text NOT NULL DEFAULT '',
    candidate_canonical_event_ids uuid[] NOT NULL DEFAULT '{}'::uuid[],
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (match_status IN ('no_match', 'ambiguous'))
);

CREATE INDEX IF NOT EXISTS event_match_reviews_source_event_idx
    ON event_match_reviews (source, source_event_id);

CREATE INDEX IF NOT EXISTS event_match_reviews_status_idx
    ON event_match_reviews (match_status, created_at DESC);

CREATE INDEX IF NOT EXISTS event_match_reviews_candidate_ids_idx
    ON event_match_reviews USING gin (candidate_canonical_event_ids);
