-- LSPORTS-vs-Polymarket latency measurement schema.
--
-- Trial thesis: when a decisive in-play event (goal, set-won, run scored)
-- crosses the wire, does the Polymarket CLOB react with a measurable lag
-- against the LSPORTS feed? See memory/latency_measurement_thesis.md.
--
-- Every TIMESTAMPTZ default in this file is clock_timestamp() — NOT now() —
-- because now() returns transaction start time and would defeat ms-resolution
-- measurement. Prices are NUMERIC (no float), payloads are JSONB so future
-- field discovery is possible without re-capturing.

-- 1) Raw LSPORTS in-play observations.
--    One row per livescore / market / settlement / fixture-meta message
--    pulled via REST snapshot OR pushed via AMQP. Append-only; no dedup at
--    this layer — analysis chooses how to collapse duplicates.
CREATE TABLE IF NOT EXISTS lsports_inplay_observations (
    id                  BIGSERIAL PRIMARY KEY,
    lsports_fixture_id  BIGINT NOT NULL,

    -- Source channel and message kind. Lets us measure AMQP-vs-snapshot
    -- latency on the LSPORTS side independently.
    source TEXT NOT NULL CHECK (source IN ('snapshot', 'amqp_push')),
    observation_kind TEXT NOT NULL CHECK (observation_kind IN (
        'livescore', 'market', 'settlement', 'fixture_meta', 'heartbeat', 'keepalive', 'other'
    )),

    -- Timestamps. received_at is OUR clock; source_publish_time is LSPORTS's
    -- (when present). Comparisons MUST use received_at for the canonical Δt.
    received_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    source_publish_time TIMESTAMPTZ,

    -- Livescore facets (NULL for non-livescore observations).
    home_score          INTEGER,
    away_score          INTEGER,
    current_period      INTEGER,
    match_status        INTEGER,
    is_inplay           BOOLEAN,

    -- Market facets (NULL for non-market observations).
    market_id           INTEGER,
    bet_name            TEXT,
    bet_price           NUMERIC(12,6),
    bet_line            NUMERIC(12,6),

    -- Full raw payload for post-hoc field discovery.
    raw_payload         JSONB NOT NULL,
    payload_sha256      TEXT,

    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS lsports_inplay_observations_fix_time_idx
    ON lsports_inplay_observations (lsports_fixture_id, received_at DESC);
CREATE INDEX IF NOT EXISTS lsports_inplay_observations_kind_idx
    ON lsports_inplay_observations (observation_kind);
CREATE INDEX IF NOT EXISTS lsports_inplay_observations_source_idx
    ON lsports_inplay_observations (source);

-- 2) Polymarket CLOB observations paired with a watched LSPORTS fixture.
--    Daemon (cmd/polymarket-latency-capture, next phase) polls /price for
--    each token mapped to a subscribed LSPORTS fixture at high cadence
--    (1-3 s) and appends one row per tick. Append-only.
CREATE TABLE IF NOT EXISTS polymarket_latency_observations (
    id                       BIGSERIAL PRIMARY KEY,
    lsports_fixture_id       BIGINT NOT NULL,

    polymarket_market_id     TEXT NOT NULL,
    polymarket_condition_id  TEXT,
    polymarket_token_id      TEXT NOT NULL,

    received_at              TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    source_publish_time      TIMESTAMPTZ,

    -- Best bid / best ask are the only executable prices. Midpoint and
    -- last_price are analytical-only (do NOT use as fills).
    best_bid                 NUMERIC(12,6),
    best_ask                 NUMERIC(12,6),
    midpoint                 NUMERIC(12,6),
    last_price               NUMERIC(12,6),
    spread                   NUMERIC(12,6),
    liquidity                NUMERIC(14,4),

    -- Did this row's executable price differ from the previous row for the
    -- same token? Filled by the capture daemon comparing against its last
    -- emitted row in-memory; null when unknown. This lets the latency
    -- analysis job find "first price change after event T" efficiently.
    price_changed_from_prior BOOLEAN,

    raw_payload              JSONB NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS polymarket_latency_observations_fix_time_idx
    ON polymarket_latency_observations (lsports_fixture_id, received_at DESC);
CREATE INDEX IF NOT EXISTS polymarket_latency_observations_token_time_idx
    ON polymarket_latency_observations (polymarket_token_id, received_at DESC);
CREATE INDEX IF NOT EXISTS polymarket_latency_observations_changed_idx
    ON polymarket_latency_observations (lsports_fixture_id, received_at DESC)
    WHERE price_changed_from_prior;

-- 3) Decisive in-play events. Derived from (1) by a parser. One row per
--    detected event we expect PM to react to. event_at is the LSPORTS
--    side's local receive time; this is the "T_lsports" in latency formula.
CREATE TABLE IF NOT EXISTS inplay_decisive_events (
    id                       BIGSERIAL PRIMARY KEY,
    lsports_fixture_id       BIGINT NOT NULL,

    -- 'goal'             — soccer goal scored
    -- 'set_won'          — tennis set completed
    -- 'break_point_won'  — tennis break point converted
    -- 'match_point_won'  — tennis match point converted
    -- 'run_scored'       — baseball run
    -- 'inning_change'    — baseball inning transition
    -- 'period_change'    — generic period/quarter/half boundary
    -- 'status_change'    — InProgress / Finished / Suspended transition
    -- 'other'            — operator-flagged
    event_kind TEXT NOT NULL CHECK (event_kind IN (
        'goal', 'set_won', 'break_point_won', 'match_point_won',
        'run_scored', 'inning_change', 'period_change', 'status_change', 'other'
    )),

    -- Local-clock event timestamp. THIS is the latency baseline.
    event_at                 TIMESTAMPTZ NOT NULL,
    source_publish_at        TIMESTAMPTZ,
    detected_at              TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    -- Pointer back to the underlying observation row(s).
    source_observation_id    BIGINT REFERENCES lsports_inplay_observations(id),

    -- Score facets for goals / runs. Null for other event kinds.
    home_score_before        INTEGER,
    away_score_before        INTEGER,
    home_score_after         INTEGER,
    away_score_after         INTEGER,

    -- The team that scored / won the point, when meaningful. 'home' / 'away'.
    benefiting_side          TEXT CHECK (benefiting_side IN ('home', 'away', 'neither')),

    notes                    TEXT,
    raw_payload              JSONB,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS inplay_decisive_events_fix_time_idx
    ON inplay_decisive_events (lsports_fixture_id, event_at DESC);
CREATE INDEX IF NOT EXISTS inplay_decisive_events_kind_idx
    ON inplay_decisive_events (event_kind);

-- 4) Latency observations. For each decisive event, the FIRST PM observation
--    after T that shows a price change (or window expiry). Computed by an
--    analysis job — NOT by the capture daemons. Allows the same event to
--    have multiple rows (one per token tracked, one per method).
CREATE TABLE IF NOT EXISTS latency_observations (
    id                              BIGSERIAL PRIMARY KEY,
    inplay_decisive_event_id        BIGINT NOT NULL REFERENCES inplay_decisive_events(id) ON DELETE CASCADE,

    -- Method: 'local_receive_time' (canonical) or 'source_publish_time'
    -- (clock-skew-sensitive auxiliary).
    method TEXT NOT NULL CHECK (method IN ('local_receive_time', 'source_publish_time')),

    polymarket_token_id             TEXT NOT NULL,

    lsports_event_at                TIMESTAMPTZ NOT NULL,
    pm_first_price_change_at        TIMESTAMPTZ,
    pm_price_before                 NUMERIC(12,6),
    pm_price_after                  NUMERIC(12,6),
    lag_seconds                     NUMERIC(10,3),

    -- Search window the analysis used (e.g. up to 300 s after event_at).
    search_window_seconds           INTEGER,

    -- Why no PM change observed within window, when lag_seconds IS NULL.
    no_change_reason                TEXT,

    notes                           TEXT,
    computed_at                     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS latency_observations_event_idx
    ON latency_observations (inplay_decisive_event_id);
CREATE INDEX IF NOT EXISTS latency_observations_lag_idx
    ON latency_observations (lag_seconds);
CREATE INDEX IF NOT EXISTS latency_observations_method_idx
    ON latency_observations (method);

-- 5) Summary view: latency distribution per method.
--    Use against latency_observations to read off the headline thesis answer.
CREATE OR REPLACE VIEW latency_summary AS
SELECT
    method,
    count(*)                                                              AS observation_count,
    count(*) FILTER (WHERE lag_seconds IS NOT NULL)                       AS observations_with_pm_reaction,
    count(*) FILTER (WHERE lag_seconds IS NULL)                           AS observations_without_pm_reaction,
    percentile_cont(0.5)  WITHIN GROUP (ORDER BY lag_seconds) FILTER (WHERE lag_seconds IS NOT NULL) AS median_lag_sec,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY lag_seconds) FILTER (WHERE lag_seconds IS NOT NULL) AS p95_lag_sec,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY lag_seconds) FILTER (WHERE lag_seconds IS NOT NULL) AS p99_lag_sec,
    min(lag_seconds)                                                      AS min_lag_sec,
    max(lag_seconds)                                                      AS max_lag_sec,
    avg(lag_seconds)                                                      AS mean_lag_sec,
    stddev_pop(lag_seconds)                                               AS stddev_lag_sec
FROM latency_observations
GROUP BY method;

-- 6) Per-event-kind cross-tab for a finer-grained thesis answer.
CREATE OR REPLACE VIEW latency_by_event_kind AS
SELECT
    de.event_kind,
    lo.method,
    count(*)                                                              AS observation_count,
    percentile_cont(0.5)  WITHIN GROUP (ORDER BY lo.lag_seconds) FILTER (WHERE lo.lag_seconds IS NOT NULL) AS median_lag_sec,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY lo.lag_seconds) FILTER (WHERE lo.lag_seconds IS NOT NULL) AS p95_lag_sec,
    avg(lo.lag_seconds)                                                   AS mean_lag_sec,
    count(*) FILTER (WHERE lo.lag_seconds IS NULL)                        AS no_reaction_count
FROM inplay_decisive_events de
JOIN latency_observations lo ON lo.inplay_decisive_event_id = de.id
GROUP BY de.event_kind, lo.method;
