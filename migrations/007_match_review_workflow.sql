CREATE TABLE IF NOT EXISTS match_review_decisions (
    id bigserial PRIMARY KEY,
    source text NOT NULL DEFAULT 'polymarket',
    polymarket_event_id text NOT NULL REFERENCES polymarket_events(polymarket_event_id) ON DELETE CASCADE,
    polymarket_market_id text REFERENCES polymarket_markets(polymarket_market_id) ON DELETE SET NULL,
    condition_id text REFERENCES polymarket_markets(condition_id) ON DELETE SET NULL,
    canonical_event_id uuid REFERENCES canonical_events(id) ON DELETE CASCADE,
    decision_status text NOT NULL,
    confidence_score numeric(5,2),
    match_reason text NOT NULL DEFAULT '',
    reviewer text NOT NULL DEFAULT '',
    review_reason text NOT NULL DEFAULT '',
    decision_source text NOT NULL DEFAULT 'manual',
    decided_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (decision_status IN ('approved', 'rejected')),
    CHECK (
        (decision_status = 'approved' AND canonical_event_id IS NOT NULL)
        OR
        (decision_status = 'rejected' AND canonical_event_id IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS match_review_decisions_polymarket_event_idx
    ON match_review_decisions (polymarket_event_id);

CREATE INDEX IF NOT EXISTS match_review_decisions_canonical_event_idx
    ON match_review_decisions (canonical_event_id);

CREATE INDEX IF NOT EXISTS match_review_decisions_status_idx
    ON match_review_decisions (decision_status);

CREATE INDEX IF NOT EXISTS match_review_decisions_decided_at_idx
    ON match_review_decisions (decided_at DESC);

CREATE OR REPLACE VIEW unresolved_polymarket_event_matches AS
WITH latest_decisions AS (
    SELECT DISTINCT ON (polymarket_event_id)
        polymarket_event_id,
        decision_status,
        decided_at
    FROM match_review_decisions
    WHERE source = 'polymarket'
    ORDER BY polymarket_event_id, decided_at DESC, id DESC
)
SELECT
    emr.id AS review_id,
    emr.source_event_id AS polymarket_event_id,
    emr.source_market_id AS polymarket_market_id,
    emr.source_condition_id AS condition_id,
    pe.slug,
    COALESCE(NULLIF(pe.title, ''), emr.event_title) AS title,
    emr.sport_key,
    emr.league,
    emr.raw_start_time,
    emr.match_status,
    emr.confidence_score,
    emr.reason AS match_reason,
    emr.candidate_canonical_event_ids,
    emr.payload,
    emr.created_at,
    emr.updated_at
FROM event_match_reviews emr
LEFT JOIN polymarket_events pe
  ON pe.polymarket_event_id = emr.source_event_id
LEFT JOIN latest_decisions ld
  ON ld.polymarket_event_id = emr.source_event_id
WHERE emr.source = 'polymarket'
  AND emr.match_status = 'no_match'
  AND ld.polymarket_event_id IS NULL;

CREATE OR REPLACE VIEW ambiguous_polymarket_event_matches AS
WITH latest_decisions AS (
    SELECT DISTINCT ON (polymarket_event_id)
        polymarket_event_id,
        decision_status,
        decided_at
    FROM match_review_decisions
    WHERE source = 'polymarket'
    ORDER BY polymarket_event_id, decided_at DESC, id DESC
)
SELECT
    emr.id AS review_id,
    emr.source_event_id AS polymarket_event_id,
    emr.source_market_id AS polymarket_market_id,
    emr.source_condition_id AS condition_id,
    pe.slug,
    COALESCE(NULLIF(pe.title, ''), emr.event_title) AS title,
    emr.sport_key,
    emr.league,
    emr.raw_start_time,
    emr.match_status,
    emr.confidence_score,
    emr.reason AS match_reason,
    emr.candidate_canonical_event_ids,
    emr.payload,
    emr.created_at,
    emr.updated_at
FROM event_match_reviews emr
LEFT JOIN polymarket_events pe
  ON pe.polymarket_event_id = emr.source_event_id
LEFT JOIN latest_decisions ld
  ON ld.polymarket_event_id = emr.source_event_id
WHERE emr.source = 'polymarket'
  AND emr.match_status = 'ambiguous'
  AND ld.polymarket_event_id IS NULL;

CREATE OR REPLACE VIEW confirmed_polymarket_event_aliases AS
SELECT
    pa.id AS alias_id,
    pa.polymarket_event_id,
    pa.polymarket_market_id,
    pa.condition_id,
    pa.canonical_event_id,
    ce.sport_key,
    ce.home_team,
    ce.away_team,
    ce.commence_time,
    pa.match_status,
    COALESCE(d.confidence_score, pa.match_confidence) AS confidence_score,
    COALESCE(NULLIF(d.match_reason, ''), pa.notes) AS match_reason,
    COALESCE(d.reviewer, '') AS reviewer,
    COALESCE(d.review_reason, '') AS review_reason,
    COALESCE(d.decision_source, '') AS decision_source,
    d.decided_at,
    pa.updated_at AS alias_updated_at
FROM polymarket_event_aliases pa
JOIN canonical_events ce
  ON ce.id = pa.canonical_event_id
LEFT JOIN LATERAL (
    SELECT
        decision.confidence_score,
        decision.match_reason,
        decision.reviewer,
        decision.review_reason,
        decision.decision_source,
        decision.decided_at
    FROM match_review_decisions decision
    WHERE decision.source = 'polymarket'
      AND decision.decision_status = 'approved'
      AND decision.polymarket_event_id IS NOT DISTINCT FROM pa.polymarket_event_id
      AND decision.polymarket_market_id IS NOT DISTINCT FROM pa.polymarket_market_id
      AND decision.condition_id IS NOT DISTINCT FROM pa.condition_id
      AND decision.canonical_event_id = pa.canonical_event_id
    ORDER BY decision.decided_at DESC, decision.id DESC
    LIMIT 1
) d ON true
WHERE lower(pa.match_status) = 'confirmed';
