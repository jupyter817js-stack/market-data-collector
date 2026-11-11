CREATE TABLE IF NOT EXISTS polymarket_events (
    id bigserial PRIMARY KEY,
    polymarket_event_id text NOT NULL,
    slug text NOT NULL DEFAULT '',
    title text NOT NULL DEFAULT '',
    description text NOT NULL DEFAULT '',
    category text NOT NULL DEFAULT '',
    start_time timestamptz,
    end_time timestamptz,
    active boolean,
    closed boolean,
    archived boolean,
    gamma_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    discovered_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (polymarket_event_id)
);

CREATE INDEX IF NOT EXISTS polymarket_events_event_id_idx
    ON polymarket_events (polymarket_event_id);

CREATE INDEX IF NOT EXISTS polymarket_events_slug_idx
    ON polymarket_events (slug);

CREATE TABLE IF NOT EXISTS polymarket_markets (
    id bigserial PRIMARY KEY,
    polymarket_market_id text NOT NULL,
    polymarket_event_id text REFERENCES polymarket_events(polymarket_event_id) ON DELETE SET NULL,
    condition_id text NOT NULL,
    question text NOT NULL DEFAULT '',
    slug text NOT NULL DEFAULT '',
    market_type text NOT NULL DEFAULT '',
    active boolean,
    closed boolean,
    archived boolean,
    end_time timestamptz,
    gamma_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    clob_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    discovered_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (polymarket_market_id),
    UNIQUE (condition_id)
);

CREATE INDEX IF NOT EXISTS polymarket_markets_event_id_idx
    ON polymarket_markets (polymarket_event_id);

CREATE INDEX IF NOT EXISTS polymarket_markets_condition_id_idx
    ON polymarket_markets (condition_id);

CREATE INDEX IF NOT EXISTS polymarket_markets_slug_idx
    ON polymarket_markets (slug);

CREATE TABLE IF NOT EXISTS polymarket_tokens (
    token_id text PRIMARY KEY,
    condition_id text NOT NULL REFERENCES polymarket_markets(condition_id) ON DELETE CASCADE,
    polymarket_market_id text REFERENCES polymarket_markets(polymarket_market_id) ON DELETE SET NULL,
    outcome_name text NOT NULL DEFAULT '',
    outcome_index int,
    side text NOT NULL DEFAULT '',
    clob_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS polymarket_tokens_token_id_idx
    ON polymarket_tokens (token_id);

CREATE INDEX IF NOT EXISTS polymarket_tokens_condition_id_idx
    ON polymarket_tokens (condition_id);

CREATE TABLE IF NOT EXISTS polymarket_price_snapshots (
    id bigserial PRIMARY KEY,
    token_id text NOT NULL REFERENCES polymarket_tokens(token_id) ON DELETE CASCADE,
    condition_id text NOT NULL,
    polymarket_market_id text,
    bid_price numeric(18,8),
    ask_price numeric(18,8),
    mid_price numeric(18,8),
    last_price numeric(18,8),
    best_bid_size numeric(24,8),
    best_ask_size numeric(24,8),
    source text NOT NULL DEFAULT 'polymarket',
    received_at timestamptz NOT NULL,
    clob_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS polymarket_price_snapshots_token_id_idx
    ON polymarket_price_snapshots (token_id);

CREATE INDEX IF NOT EXISTS polymarket_price_snapshots_received_at_idx
    ON polymarket_price_snapshots (received_at DESC);

CREATE INDEX IF NOT EXISTS polymarket_price_snapshots_token_received_idx
    ON polymarket_price_snapshots (token_id, received_at DESC);

CREATE TABLE IF NOT EXISTS polymarket_event_aliases (
    id bigserial PRIMARY KEY,
    canonical_event_id uuid NOT NULL REFERENCES canonical_events(id) ON DELETE CASCADE,
    polymarket_event_id text REFERENCES polymarket_events(polymarket_event_id) ON DELETE CASCADE,
    polymarket_market_id text REFERENCES polymarket_markets(polymarket_market_id) ON DELETE CASCADE,
    condition_id text,
    match_status text NOT NULL DEFAULT 'candidate',
    match_confidence numeric(5,2),
    notes text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        polymarket_event_id IS NOT NULL
        OR polymarket_market_id IS NOT NULL
        OR condition_id IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS polymarket_event_aliases_canonical_event_idx
    ON polymarket_event_aliases (canonical_event_id);

CREATE INDEX IF NOT EXISTS polymarket_event_aliases_event_id_idx
    ON polymarket_event_aliases (polymarket_event_id);

CREATE INDEX IF NOT EXISTS polymarket_event_aliases_condition_id_idx
    ON polymarket_event_aliases (condition_id);
