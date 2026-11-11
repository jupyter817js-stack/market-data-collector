CREATE TABLE IF NOT EXISTS betfair_event_aliases (
    id bigserial PRIMARY KEY,
    source text NOT NULL DEFAULT 'betfair' CHECK (source = 'betfair'),
    betfair_event_id text NOT NULL,
    betfair_event_name text NOT NULL DEFAULT '',
    canonical_event_id uuid REFERENCES canonical_events(id) ON DELETE CASCADE,
    alias_status text NOT NULL DEFAULT 'unresolved',
    confidence_score numeric(5,2) CHECK (confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 100)),
    match_reason text NOT NULL DEFAULT '',
    reviewer text NOT NULL DEFAULT '',
    review_reason text NOT NULL DEFAULT '',
    decided_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (btrim(betfair_event_id) <> ''),
    CHECK (alias_status IN ('confirmed', 'rejected', 'unresolved', 'ambiguous')),
    CHECK (
        (alias_status = 'confirmed' AND canonical_event_id IS NOT NULL)
        OR alias_status <> 'confirmed'
    )
);

CREATE INDEX IF NOT EXISTS betfair_event_aliases_event_id_idx
    ON betfair_event_aliases (betfair_event_id);

CREATE INDEX IF NOT EXISTS betfair_event_aliases_canonical_event_idx
    ON betfair_event_aliases (canonical_event_id);

CREATE INDEX IF NOT EXISTS betfair_event_aliases_status_idx
    ON betfair_event_aliases (alias_status);

CREATE INDEX IF NOT EXISTS betfair_event_aliases_decided_at_idx
    ON betfair_event_aliases (decided_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS betfair_event_aliases_confirmed_event_unique
    ON betfair_event_aliases (betfair_event_id)
    WHERE alias_status = 'confirmed';

CREATE TABLE IF NOT EXISTS betfair_market_aliases (
    id bigserial PRIMARY KEY,
    source text NOT NULL DEFAULT 'betfair' CHECK (source = 'betfair'),
    betfair_market_id text NOT NULL,
    betfair_event_id text NOT NULL DEFAULT '',
    canonical_event_id uuid REFERENCES canonical_events(id) ON DELETE CASCADE,
    betfair_market_type text NOT NULL DEFAULT '',
    canonical_market_family text NOT NULL DEFAULT '',
    line numeric(12,4),
    alias_status text NOT NULL DEFAULT 'unresolved',
    confidence_score numeric(5,2) CHECK (confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 100)),
    match_reason text NOT NULL DEFAULT '',
    reviewer text NOT NULL DEFAULT '',
    review_reason text NOT NULL DEFAULT '',
    decided_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (btrim(betfair_market_id) <> ''),
    CHECK (alias_status IN ('confirmed', 'rejected', 'unresolved', 'ambiguous')),
    CHECK (canonical_market_family IN ('', 'h2h', 'spreads', 'totals', 'other')),
    CHECK (
        (
            alias_status = 'confirmed'
            AND canonical_event_id IS NOT NULL
            AND btrim(canonical_market_family) <> ''
        )
        OR alias_status <> 'confirmed'
    )
);

CREATE INDEX IF NOT EXISTS betfair_market_aliases_market_id_idx
    ON betfair_market_aliases (betfair_market_id);

CREATE INDEX IF NOT EXISTS betfair_market_aliases_event_id_idx
    ON betfair_market_aliases (betfair_event_id);

CREATE INDEX IF NOT EXISTS betfair_market_aliases_canonical_event_idx
    ON betfair_market_aliases (canonical_event_id);

CREATE INDEX IF NOT EXISTS betfair_market_aliases_status_idx
    ON betfair_market_aliases (alias_status);

CREATE INDEX IF NOT EXISTS betfair_market_aliases_decided_at_idx
    ON betfair_market_aliases (decided_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS betfair_market_aliases_confirmed_market_unique
    ON betfair_market_aliases (betfair_market_id)
    WHERE alias_status = 'confirmed';

CREATE TABLE IF NOT EXISTS betfair_runner_aliases (
    id bigserial PRIMARY KEY,
    source text NOT NULL DEFAULT 'betfair' CHECK (source = 'betfair'),
    betfair_market_id text NOT NULL,
    selection_id bigint NOT NULL CHECK (selection_id <> 0),
    handicap numeric(12,4) NOT NULL DEFAULT 0,
    runner_name text NOT NULL DEFAULT '',
    canonical_outcome_name text NOT NULL DEFAULT '',
    canonical_outcome_side text NOT NULL DEFAULT '',
    alias_status text NOT NULL DEFAULT 'unresolved',
    confidence_score numeric(5,2) CHECK (confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 100)),
    match_reason text NOT NULL DEFAULT '',
    reviewer text NOT NULL DEFAULT '',
    review_reason text NOT NULL DEFAULT '',
    decided_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (btrim(betfair_market_id) <> ''),
    CHECK (alias_status IN ('confirmed', 'rejected', 'unresolved', 'ambiguous')),
    CHECK (canonical_outcome_side IN ('', 'home', 'away', 'draw', 'over', 'under', 'other')),
    CHECK (
        (
            alias_status = 'confirmed'
            AND btrim(canonical_outcome_name) <> ''
            AND btrim(canonical_outcome_side) <> ''
        )
        OR alias_status <> 'confirmed'
    )
);

CREATE INDEX IF NOT EXISTS betfair_runner_aliases_market_id_idx
    ON betfair_runner_aliases (betfair_market_id);

CREATE INDEX IF NOT EXISTS betfair_runner_aliases_market_runner_idx
    ON betfair_runner_aliases (betfair_market_id, selection_id, handicap);

CREATE INDEX IF NOT EXISTS betfair_runner_aliases_status_idx
    ON betfair_runner_aliases (alias_status);

CREATE INDEX IF NOT EXISTS betfair_runner_aliases_decided_at_idx
    ON betfair_runner_aliases (decided_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS betfair_runner_aliases_confirmed_runner_unique
    ON betfair_runner_aliases (betfair_market_id, selection_id, handicap)
    WHERE alias_status = 'confirmed';

CREATE OR REPLACE VIEW betfair_confirmed_event_aliases AS
SELECT
    a.id AS alias_id,
    a.betfair_event_id,
    a.betfair_event_name,
    a.canonical_event_id,
    ce.sport_key,
    ce.league,
    ce.home_team,
    ce.away_team,
    ce.commence_time,
    a.alias_status,
    a.confidence_score,
    a.match_reason,
    a.reviewer,
    a.review_reason,
    a.decided_at,
    a.updated_at AS alias_updated_at
FROM betfair_event_aliases a
JOIN canonical_events ce
  ON ce.id = a.canonical_event_id
WHERE a.alias_status = 'confirmed';

CREATE OR REPLACE VIEW betfair_confirmed_market_aliases AS
SELECT
    a.id AS alias_id,
    a.betfair_market_id,
    a.betfair_event_id,
    a.canonical_event_id,
    ce.sport_key,
    ce.league,
    ce.home_team,
    ce.away_team,
    ce.commence_time,
    a.betfair_market_type,
    a.canonical_market_family,
    a.line,
    a.alias_status,
    a.confidence_score,
    a.match_reason,
    a.reviewer,
    a.review_reason,
    a.decided_at,
    a.updated_at AS alias_updated_at
FROM betfair_market_aliases a
JOIN canonical_events ce
  ON ce.id = a.canonical_event_id
WHERE a.alias_status = 'confirmed';

CREATE OR REPLACE VIEW betfair_confirmed_runner_aliases AS
SELECT
    a.id AS alias_id,
    a.betfair_market_id,
    a.selection_id,
    a.handicap,
    a.runner_name,
    a.canonical_outcome_name,
    a.canonical_outcome_side,
    a.alias_status,
    a.confidence_score,
    a.match_reason,
    a.reviewer,
    a.review_reason,
    a.decided_at,
    a.updated_at AS alias_updated_at
FROM betfair_runner_aliases a
WHERE a.alias_status = 'confirmed';

CREATE OR REPLACE VIEW betfair_unmapped_events AS
WITH observed_events AS (
    SELECT
        h.event_id AS betfair_event_id,
        max(h.event_name) AS betfair_event_name,
        count(*) AS quote_history_rows,
        count(DISTINCT h.market_id) AS market_count,
        max(h.received_at) AS latest_received_at
    FROM betfair_exchange_quote_history h
    WHERE btrim(h.event_id) <> ''
    GROUP BY h.event_id
),
latest_alias AS (
    SELECT DISTINCT ON (a.betfair_event_id)
        a.betfair_event_id,
        a.alias_status,
        a.confidence_score,
        a.match_reason,
        a.updated_at
    FROM betfair_event_aliases a
    ORDER BY a.betfair_event_id, a.updated_at DESC, a.id DESC
)
SELECT
    oe.betfair_event_id,
    oe.betfair_event_name,
    oe.quote_history_rows,
    oe.market_count,
    oe.latest_received_at,
    COALESCE(la.alias_status, 'missing') AS latest_alias_status,
    la.confidence_score AS latest_alias_confidence_score,
    la.match_reason AS latest_alias_match_reason,
    la.updated_at AS latest_alias_updated_at
FROM observed_events oe
LEFT JOIN betfair_confirmed_event_aliases confirmed
  ON confirmed.betfair_event_id = oe.betfair_event_id
LEFT JOIN latest_alias la
  ON la.betfair_event_id = oe.betfair_event_id
WHERE confirmed.alias_id IS NULL;

CREATE OR REPLACE VIEW betfair_unmapped_markets AS
WITH observed_markets AS (
    SELECT
        h.market_id AS betfair_market_id,
        max(h.event_id) AS betfair_event_id,
        max(h.event_name) AS betfair_event_name,
        max(h.market_name) AS betfair_market_name,
        max(h.market_family) AS betfair_market_family,
        count(*) AS quote_history_rows,
        count(DISTINCT (h.selection_id, h.handicap)) AS runner_count,
        max(h.received_at) AS latest_received_at
    FROM betfair_exchange_quote_history h
    WHERE btrim(h.market_id) <> ''
    GROUP BY h.market_id
),
latest_alias AS (
    SELECT DISTINCT ON (a.betfair_market_id)
        a.betfair_market_id,
        a.alias_status,
        a.canonical_market_family,
        a.confidence_score,
        a.match_reason,
        a.updated_at
    FROM betfair_market_aliases a
    ORDER BY a.betfair_market_id, a.updated_at DESC, a.id DESC
)
SELECT
    om.betfair_market_id,
    om.betfair_event_id,
    om.betfair_event_name,
    om.betfair_market_name,
    om.betfair_market_family,
    om.quote_history_rows,
    om.runner_count,
    om.latest_received_at,
    COALESCE(la.alias_status, 'missing') AS latest_alias_status,
    la.canonical_market_family AS latest_alias_market_family,
    la.confidence_score AS latest_alias_confidence_score,
    la.match_reason AS latest_alias_match_reason,
    la.updated_at AS latest_alias_updated_at
FROM observed_markets om
LEFT JOIN betfair_confirmed_market_aliases confirmed
  ON confirmed.betfair_market_id = om.betfair_market_id
LEFT JOIN latest_alias la
  ON la.betfair_market_id = om.betfair_market_id
WHERE confirmed.alias_id IS NULL;

CREATE OR REPLACE VIEW betfair_unmapped_runners AS
WITH observed_runners AS (
    SELECT
        h.market_id AS betfair_market_id,
        max(h.event_id) AS betfair_event_id,
        max(h.event_name) AS betfair_event_name,
        max(h.market_name) AS betfair_market_name,
        h.selection_id,
        h.handicap,
        max(h.runner_name) AS runner_name,
        count(*) AS quote_history_rows,
        count(DISTINCT h.exchange_side) AS exchange_side_count,
        max(h.received_at) AS latest_received_at
    FROM betfair_exchange_quote_history h
    WHERE btrim(h.market_id) <> ''
    GROUP BY h.market_id, h.selection_id, h.handicap
),
latest_alias AS (
    SELECT DISTINCT ON (a.betfair_market_id, a.selection_id, a.handicap)
        a.betfair_market_id,
        a.selection_id,
        a.handicap,
        a.alias_status,
        a.canonical_outcome_name,
        a.canonical_outcome_side,
        a.confidence_score,
        a.match_reason,
        a.updated_at
    FROM betfair_runner_aliases a
    ORDER BY a.betfair_market_id, a.selection_id, a.handicap, a.updated_at DESC, a.id DESC
)
SELECT
    observed.betfair_market_id,
    observed.betfair_event_id,
    observed.betfair_event_name,
    observed.betfair_market_name,
    observed.selection_id,
    observed.handicap,
    observed.runner_name,
    observed.quote_history_rows,
    observed.exchange_side_count,
    observed.latest_received_at,
    COALESCE(la.alias_status, 'missing') AS latest_alias_status,
    la.canonical_outcome_name AS latest_alias_outcome_name,
    la.canonical_outcome_side AS latest_alias_outcome_side,
    la.confidence_score AS latest_alias_confidence_score,
    la.match_reason AS latest_alias_match_reason,
    la.updated_at AS latest_alias_updated_at
FROM observed_runners observed
LEFT JOIN betfair_confirmed_runner_aliases confirmed
  ON confirmed.betfair_market_id = observed.betfair_market_id
 AND confirmed.selection_id = observed.selection_id
 AND confirmed.handicap = observed.handicap
LEFT JOIN latest_alias la
  ON la.betfair_market_id = observed.betfair_market_id
 AND la.selection_id = observed.selection_id
 AND la.handicap = observed.handicap
WHERE confirmed.alias_id IS NULL;

CREATE OR REPLACE VIEW betfair_mapping_conflicts AS
WITH confirmed_events AS (
    SELECT *
    FROM betfair_event_aliases
    WHERE alias_status = 'confirmed'
),
confirmed_markets AS (
    SELECT *
    FROM betfair_market_aliases
    WHERE alias_status = 'confirmed'
),
confirmed_runners AS (
    SELECT *
    FROM betfair_runner_aliases
    WHERE alias_status = 'confirmed'
),
event_conflicts AS (
    SELECT
        betfair_event_id,
        array_agg(DISTINCT canonical_event_id ORDER BY canonical_event_id) AS canonical_event_ids,
        count(DISTINCT canonical_event_id) AS canonical_event_count,
        max(updated_at) AS observed_at
    FROM confirmed_events
    GROUP BY betfair_event_id
    HAVING count(DISTINCT canonical_event_id) > 1
),
market_conflicts AS (
    SELECT
        betfair_market_id,
        array_agg(DISTINCT canonical_event_id ORDER BY canonical_event_id) AS canonical_event_ids,
        count(DISTINCT canonical_event_id) AS canonical_event_count,
        max(updated_at) AS observed_at
    FROM confirmed_markets
    GROUP BY betfair_market_id
    HAVING count(DISTINCT canonical_event_id) > 1
),
event_market_mismatches AS (
    SELECT
        e.betfair_event_id,
        m.betfair_market_id,
        e.canonical_event_id AS event_canonical_event_id,
        m.canonical_event_id AS market_canonical_event_id,
        greatest(e.updated_at, m.updated_at) AS observed_at
    FROM confirmed_markets m
    JOIN confirmed_events e
      ON e.betfair_event_id = m.betfair_event_id
    WHERE btrim(m.betfair_event_id) <> ''
      AND e.canonical_event_id <> m.canonical_event_id
),
confirmed_markets_without_events AS (
    SELECT
        m.betfair_event_id,
        m.betfair_market_id,
        m.canonical_event_id,
        m.updated_at AS observed_at
    FROM confirmed_markets m
    LEFT JOIN confirmed_events e
      ON e.betfair_event_id = m.betfair_event_id
     AND e.canonical_event_id = m.canonical_event_id
    WHERE btrim(m.betfair_event_id) <> ''
      AND e.id IS NULL
),
confirmed_runners_without_markets AS (
    SELECT
        r.betfair_market_id,
        r.selection_id,
        r.handicap,
        r.updated_at AS observed_at
    FROM confirmed_runners r
    LEFT JOIN confirmed_markets m
      ON m.betfair_market_id = r.betfair_market_id
    WHERE m.id IS NULL
)
SELECT
    'conflicting_confirmed_betfair_event_aliases'::text AS issue_type,
    'error'::text AS severity,
    'same Betfair event has confirmed aliases to multiple canonical events'::text AS reason,
    betfair_event_id,
    NULL::text AS betfair_market_id,
    NULL::bigint AS selection_id,
    NULL::numeric(12,4) AS handicap,
    NULL::uuid AS canonical_event_id,
    canonical_event_ids,
    canonical_event_count,
    observed_at,
    jsonb_build_object(
        'conflict_scope', 'betfair_event_id',
        'canonical_event_ids', canonical_event_ids
    ) AS details
FROM event_conflicts

UNION ALL

SELECT
    'conflicting_confirmed_betfair_market_aliases'::text AS issue_type,
    'error'::text AS severity,
    'same Betfair market has confirmed aliases to multiple canonical events'::text AS reason,
    NULL::text AS betfair_event_id,
    betfair_market_id,
    NULL::bigint AS selection_id,
    NULL::numeric(12,4) AS handicap,
    NULL::uuid AS canonical_event_id,
    canonical_event_ids,
    canonical_event_count,
    observed_at,
    jsonb_build_object(
        'conflict_scope', 'betfair_market_id',
        'canonical_event_ids', canonical_event_ids
    ) AS details
FROM market_conflicts

UNION ALL

SELECT
    'confirmed_betfair_event_market_canonical_mismatch'::text AS issue_type,
    'error'::text AS severity,
    'confirmed Betfair event and market aliases disagree on canonical event'::text AS reason,
    betfair_event_id,
    betfair_market_id,
    NULL::bigint AS selection_id,
    NULL::numeric(12,4) AS handicap,
    market_canonical_event_id AS canonical_event_id,
    ARRAY[event_canonical_event_id, market_canonical_event_id]::uuid[] AS canonical_event_ids,
    2::bigint AS canonical_event_count,
    observed_at,
    jsonb_build_object(
        'event_canonical_event_id', event_canonical_event_id,
        'market_canonical_event_id', market_canonical_event_id
    ) AS details
FROM event_market_mismatches

UNION ALL

SELECT
    'confirmed_betfair_market_without_confirmed_event'::text AS issue_type,
    'warning'::text AS severity,
    'confirmed Betfair market alias has no matching confirmed event alias'::text AS reason,
    betfair_event_id,
    betfair_market_id,
    NULL::bigint AS selection_id,
    NULL::numeric(12,4) AS handicap,
    canonical_event_id,
    ARRAY[canonical_event_id]::uuid[] AS canonical_event_ids,
    1::bigint AS canonical_event_count,
    observed_at,
    jsonb_build_object(
        'conflict_scope', 'market_without_event_alias'
    ) AS details
FROM confirmed_markets_without_events

UNION ALL

SELECT
    'confirmed_betfair_runner_without_confirmed_market'::text AS issue_type,
    'warning'::text AS severity,
    'confirmed Betfair runner alias has no confirmed market alias'::text AS reason,
    NULL::text AS betfair_event_id,
    betfair_market_id,
    selection_id,
    handicap,
    NULL::uuid AS canonical_event_id,
    ARRAY[]::uuid[] AS canonical_event_ids,
    0::bigint AS canonical_event_count,
    observed_at,
    jsonb_build_object(
        'conflict_scope', 'runner_without_market_alias'
    ) AS details
FROM confirmed_runners_without_markets;

CREATE OR REPLACE VIEW fair_value_ready_betfair_exchange_quotes AS
SELECT
    q.source,
    ce.id AS canonical_event_id,
    ce.sport_key,
    ce.league,
    ce.home_team,
    ce.away_team,
    ce.commence_time,
    q.event_id AS betfair_event_id,
    q.event_name AS betfair_event_name,
    q.market_id AS betfair_market_id,
    q.market_name AS betfair_market_name,
    q.selection_id,
    q.handicap,
    q.runner_name,
    ma.betfair_market_type,
    ma.canonical_market_family,
    ma.line,
    ra.canonical_outcome_name,
    ra.canonical_outcome_side,
    q.exchange_side,
    q.price_decimal,
    q.available_size,
    q.last_traded_price,
    q.traded_volume,
    q.market_status,
    q.in_play,
    q.market_time,
    q.source_publish_time_ms,
    q.source_publish_time,
    q.reducer_updated_at,
    q.received_at,
    q.latest_updated_at,
    q.stale_state,
    q.confidence_score,
    q.quote_key,
    q.quote_hash,
    ea.alias_id AS event_alias_id,
    ea.confidence_score AS event_alias_confidence_score,
    ea.match_reason AS event_alias_match_reason,
    ma.alias_id AS market_alias_id,
    ma.confidence_score AS market_alias_confidence_score,
    ma.match_reason AS market_alias_match_reason,
    ra.alias_id AS runner_alias_id,
    ra.confidence_score AS runner_alias_confidence_score,
    ra.match_reason AS runner_alias_match_reason,
    q.raw_metadata
FROM betfair_latest_exchange_quotes_view q
JOIN betfair_confirmed_event_aliases ea
  ON ea.betfair_event_id = q.event_id
JOIN canonical_events ce
  ON ce.id = ea.canonical_event_id
JOIN betfair_confirmed_market_aliases ma
  ON ma.betfair_market_id = q.market_id
 AND ma.canonical_event_id = ea.canonical_event_id
 AND (
     btrim(ma.betfair_event_id) = ''
     OR ma.betfair_event_id = q.event_id
 )
JOIN betfair_confirmed_runner_aliases ra
  ON ra.betfair_market_id = q.market_id
 AND ra.selection_id = q.selection_id
 AND ra.handicap = q.handicap
WHERE q.exchange_side IN ('back', 'lay');
