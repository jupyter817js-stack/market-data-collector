CREATE TABLE IF NOT EXISTS betfair_raw_stream_messages (
    id bigserial PRIMARY KEY,
    source text NOT NULL DEFAULT 'betfair' CHECK (source = 'betfair'),
    stream_id text NOT NULL DEFAULT '',
    op text NOT NULL DEFAULT '',
    market_id text NOT NULL DEFAULT '',
    publish_time_ms bigint CHECK (publish_time_ms IS NULL OR publish_time_ms >= 0),
    source_publish_time timestamptz,
    change_type text NOT NULL DEFAULT '',
    clk text NOT NULL DEFAULT '',
    initial_clk text NOT NULL DEFAULT '',
    raw_json jsonb NOT NULL,
    payload_sha256 text NOT NULL,
    received_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS betfair_raw_stream_messages_market_received_idx
    ON betfair_raw_stream_messages (market_id, received_at DESC);

CREATE INDEX IF NOT EXISTS betfair_raw_stream_messages_op_received_idx
    ON betfair_raw_stream_messages (op, received_at DESC);

CREATE INDEX IF NOT EXISTS betfair_raw_stream_messages_payload_hash_idx
    ON betfair_raw_stream_messages (payload_sha256);

CREATE INDEX IF NOT EXISTS betfair_raw_stream_messages_received_idx
    ON betfair_raw_stream_messages (received_at DESC);

CREATE TABLE IF NOT EXISTS betfair_exchange_quote_history (
    id bigserial PRIMARY KEY,
    source text NOT NULL DEFAULT 'betfair' CHECK (source = 'betfair'),
    quote_key text NOT NULL,
    quote_hash text NOT NULL,
    market_id text NOT NULL,
    selection_id bigint NOT NULL CHECK (selection_id <> 0),
    handicap numeric(12,4) NOT NULL DEFAULT 0,
    runner_name text NOT NULL DEFAULT '',
    market_name text NOT NULL DEFAULT '',
    event_id text NOT NULL DEFAULT '',
    event_name text NOT NULL DEFAULT '',
    market_family text NOT NULL DEFAULT '',
    exchange_side text NOT NULL CHECK (exchange_side IN ('back', 'lay')),
    price_decimal numeric(18,8) NOT NULL CHECK (price_decimal > 1.0),
    available_size numeric(24,8) NOT NULL CHECK (available_size > 0),
    last_traded_price numeric(18,8),
    traded_volume numeric(24,8),
    market_status text NOT NULL DEFAULT '',
    in_play boolean,
    market_time text NOT NULL DEFAULT '',
    source_publish_time_ms bigint CHECK (source_publish_time_ms IS NULL OR source_publish_time_ms >= 0),
    source_publish_time timestamptz,
    reducer_updated_at timestamptz,
    received_at timestamptz NOT NULL,
    age_ms bigint CHECK (age_ms IS NULL OR age_ms >= 0),
    stale_state text NOT NULL CHECK (stale_state IN ('fresh', 'aging', 'stale', 'unknown')),
    confidence_score numeric(5,2) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 100),
    raw_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS betfair_exchange_quote_history_key_received_idx
    ON betfair_exchange_quote_history (quote_key, received_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS betfair_exchange_quote_history_market_runner_idx
    ON betfair_exchange_quote_history (market_id, selection_id, handicap);

CREATE INDEX IF NOT EXISTS betfair_exchange_quote_history_market_side_idx
    ON betfair_exchange_quote_history (market_id, exchange_side);

CREATE INDEX IF NOT EXISTS betfair_exchange_quote_history_source_publish_idx
    ON betfair_exchange_quote_history (source_publish_time DESC);

CREATE INDEX IF NOT EXISTS betfair_exchange_quote_history_market_time_idx
    ON betfair_exchange_quote_history (market_time);

CREATE INDEX IF NOT EXISTS betfair_exchange_quote_history_stale_confidence_idx
    ON betfair_exchange_quote_history (stale_state, confidence_score DESC);

CREATE TABLE IF NOT EXISTS betfair_latest_exchange_quotes (
    quote_key text PRIMARY KEY,
    history_id bigint NOT NULL REFERENCES betfair_exchange_quote_history(id) ON DELETE CASCADE,
    quote_hash text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW betfair_latest_exchange_quotes_view AS
SELECT
    h.id AS history_id,
    h.source,
    h.quote_key,
    h.quote_hash,
    h.market_id,
    h.selection_id,
    h.handicap,
    h.runner_name,
    h.market_name,
    h.event_id,
    h.event_name,
    h.market_family,
    h.exchange_side,
    h.price_decimal,
    h.available_size,
    h.last_traded_price,
    h.traded_volume,
    h.market_status,
    h.in_play,
    h.market_time,
    h.source_publish_time_ms,
    h.source_publish_time,
    h.reducer_updated_at,
    h.received_at,
    h.age_ms,
    h.stale_state,
    h.confidence_score,
    h.raw_metadata,
    h.created_at,
    l.updated_at AS latest_updated_at
FROM betfair_latest_exchange_quotes l
JOIN betfair_exchange_quote_history h
  ON h.id = l.history_id;
