CREATE OR REPLACE VIEW data_quality_sportsbook_anomalies AS
WITH h2h_book_sides AS (
    SELECT
        canonical_event_id,
        sport_key,
        league,
        home_team,
        away_team,
        commence_time,
        source,
        bookmaker_key,
        bool_or(outcome_side = 'home') AS has_home_side,
        bool_or(outcome_side = 'away') AS has_away_side,
        array_agg(DISTINCT outcome_side ORDER BY outcome_side) AS present_outcome_sides,
        max(received_at) AS latest_received_at
    FROM fair_value_ready_quotes
    WHERE market_key = 'h2h'
    GROUP BY
        canonical_event_id,
        sport_key,
        league,
        home_team,
        away_team,
        commence_time,
        source,
        bookmaker_key
),
h2h_bookmaker_counts AS (
    SELECT
        canonical_event_id,
        sport_key,
        league,
        home_team,
        away_team,
        commence_time,
        count(DISTINCT (source, bookmaker_key)) AS bookmaker_count,
        max(received_at) AS latest_received_at
    FROM fair_value_moneyline_by_book
    GROUP BY
        canonical_event_id,
        sport_key,
        league,
        home_team,
        away_team,
        commence_time
)
SELECT
    'stale_sportsbook_quote'::text AS issue_type,
    'warning'::text AS severity,
    'latest sportsbook quote is marked stale'::text AS reason,
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    source,
    bookmaker_key,
    market_key,
    outcome_name,
    outcome_side,
    received_at AS observed_at,
    jsonb_build_object(
        'stale_state', stale_state,
        'source_last_update', source_last_update,
        'age_ms', age_ms,
        'confidence_score', confidence_score
    ) AS details
FROM fair_value_ready_quotes
WHERE stale_state = 'stale'

UNION ALL

SELECT
    'missing_h2h_side'::text AS issue_type,
    'error'::text AS severity,
    concat_ws(
        '; ',
        CASE WHEN NOT has_home_side THEN 'missing home side' END,
        CASE WHEN NOT has_away_side THEN 'missing away side' END
    ) AS reason,
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    source,
    bookmaker_key,
    'h2h'::text AS market_key,
    NULL::text AS outcome_name,
    NULL::text AS outcome_side,
    latest_received_at AS observed_at,
    jsonb_build_object(
        'present_outcome_sides', present_outcome_sides,
        'has_home_side', has_home_side,
        'has_away_side', has_away_side
    ) AS details
FROM h2h_book_sides
WHERE NOT has_home_side
   OR NOT has_away_side

UNION ALL

SELECT
    'too_few_h2h_bookmakers'::text AS issue_type,
    'warning'::text AS severity,
    'h2h fair-value consensus has fewer than 2 contributing bookmakers'::text AS reason,
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    NULL::text AS source,
    NULL::text AS bookmaker_key,
    'h2h'::text AS market_key,
    NULL::text AS outcome_name,
    NULL::text AS outcome_side,
    latest_received_at AS observed_at,
    jsonb_build_object(
        'bookmaker_count', bookmaker_count,
        'minimum_expected_bookmakers', 2
    ) AS details
FROM h2h_bookmaker_counts
WHERE bookmaker_count < 2;

CREATE OR REPLACE VIEW data_quality_polymarket_anomalies AS
WITH latest_token_prices AS (
    SELECT DISTINCT ON (pps.token_id)
        pps.token_id,
        pps.condition_id,
        pps.polymarket_market_id,
        pps.best_bid,
        pps.best_ask,
        pps.midpoint,
        pps.spread,
        pps.liquidity,
        pps.received_at,
        pps.source_timestamp
    FROM polymarket_price_snapshots pps
    ORDER BY pps.token_id, pps.received_at DESC, pps.id DESC
),
token_price_anomalies AS (
    SELECT
        CASE
            WHEN ltp.token_id IS NULL THEN 'missing_polymarket_price_snapshot'
            ELSE 'stale_polymarket_price_snapshot'
        END AS issue_type,
        CASE
            WHEN ltp.token_id IS NULL THEN 'error'
            ELSE 'warning'
        END AS severity,
        CASE
            WHEN ltp.token_id IS NULL THEN 'Polymarket token has no price snapshot'
            ELSE 'latest Polymarket token price snapshot is older than 15 minutes'
        END AS reason,
        pm.polymarket_event_id,
        pt.polymarket_market_id,
        pt.condition_id,
        pt.token_id,
        COALESCE(NULLIF(pe.title, ''), pm.question, pt.outcome_name) AS title,
        ltp.received_at AS observed_at,
        jsonb_build_object(
            'price_recency_threshold', '15 minutes',
            'latest_received_at', ltp.received_at,
            'best_bid', ltp.best_bid,
            'best_ask', ltp.best_ask,
            'midpoint', ltp.midpoint,
            'spread', ltp.spread,
            'liquidity', ltp.liquidity
        ) AS details
    FROM polymarket_tokens pt
    LEFT JOIN polymarket_markets pm
      ON pm.condition_id = pt.condition_id
    LEFT JOIN polymarket_events pe
      ON pe.polymarket_event_id = pm.polymarket_event_id
    LEFT JOIN latest_token_prices ltp
      ON ltp.token_id = pt.token_id
    WHERE ltp.token_id IS NULL
       OR ltp.received_at < now() - interval '15 minutes'
),
crossed_or_invalid_books AS (
    SELECT
        'invalid_polymarket_best_bid_ask'::text AS issue_type,
        'error'::text AS severity,
        'latest Polymarket best ask is below best bid'::text AS reason,
        pm.polymarket_event_id,
        pt.polymarket_market_id,
        pt.condition_id,
        pt.token_id,
        COALESCE(NULLIF(pe.title, ''), pm.question, pt.outcome_name) AS title,
        ltp.received_at AS observed_at,
        jsonb_build_object(
            'best_bid', ltp.best_bid,
            'best_ask', ltp.best_ask,
            'spread', ltp.spread,
            'source_timestamp', ltp.source_timestamp
        ) AS details
    FROM polymarket_tokens pt
    JOIN latest_token_prices ltp
      ON ltp.token_id = pt.token_id
    LEFT JOIN polymarket_markets pm
      ON pm.condition_id = pt.condition_id
    LEFT JOIN polymarket_events pe
      ON pe.polymarket_event_id = pm.polymarket_event_id
    WHERE ltp.best_bid IS NOT NULL
      AND ltp.best_ask IS NOT NULL
      AND ltp.best_ask < ltp.best_bid
)
SELECT
    issue_type,
    severity,
    reason,
    polymarket_event_id,
    polymarket_market_id,
    condition_id,
    token_id,
    title,
    observed_at,
    details
FROM token_price_anomalies

UNION ALL

SELECT
    issue_type,
    severity,
    reason,
    polymarket_event_id,
    polymarket_market_id,
    condition_id,
    token_id,
    title,
    observed_at,
    details
FROM crossed_or_invalid_books

UNION ALL

SELECT
    'unresolved_polymarket_event'::text AS issue_type,
    'review'::text AS severity,
    'Polymarket event remains unresolved and needs review'::text AS reason,
    polymarket_event_id,
    polymarket_market_id,
    condition_id,
    NULL::text AS token_id,
    title,
    updated_at AS observed_at,
    jsonb_build_object(
        'match_status', match_status,
        'confidence_score', confidence_score,
        'match_reason', match_reason,
        'candidate_canonical_event_ids', candidate_canonical_event_ids
    ) AS details
FROM unresolved_polymarket_event_matches

UNION ALL

SELECT
    'ambiguous_polymarket_event'::text AS issue_type,
    'review'::text AS severity,
    'Polymarket event has ambiguous canonical candidates and needs review'::text AS reason,
    polymarket_event_id,
    polymarket_market_id,
    condition_id,
    NULL::text AS token_id,
    title,
    updated_at AS observed_at,
    jsonb_build_object(
        'match_status', match_status,
        'confidence_score', confidence_score,
        'match_reason', match_reason,
        'candidate_canonical_event_ids', candidate_canonical_event_ids
    ) AS details
FROM ambiguous_polymarket_event_matches;

CREATE OR REPLACE VIEW data_quality_alias_conflicts AS
WITH confirmed_aliases AS (
    SELECT
        pa.id AS alias_id,
        pa.polymarket_event_id,
        pa.polymarket_market_id,
        pa.condition_id,
        pa.canonical_event_id,
        pa.match_confidence,
        pa.updated_at
    FROM polymarket_event_aliases pa
    WHERE lower(pa.match_status) = 'confirmed'
),
event_conflicts AS (
    SELECT
        polymarket_event_id,
        array_agg(DISTINCT canonical_event_id ORDER BY canonical_event_id) AS canonical_event_ids,
        count(DISTINCT canonical_event_id) AS canonical_event_count,
        max(updated_at) AS observed_at
    FROM confirmed_aliases
    WHERE polymarket_event_id IS NOT NULL
    GROUP BY polymarket_event_id
    HAVING count(DISTINCT canonical_event_id) > 1
),
market_conflicts AS (
    SELECT
        polymarket_event_id,
        polymarket_market_id,
        condition_id,
        array_agg(DISTINCT canonical_event_id ORDER BY canonical_event_id) AS canonical_event_ids,
        count(DISTINCT canonical_event_id) AS canonical_event_count,
        max(updated_at) AS observed_at
    FROM confirmed_aliases
    WHERE polymarket_market_id IS NOT NULL
       OR condition_id IS NOT NULL
    GROUP BY polymarket_event_id, polymarket_market_id, condition_id
    HAVING count(DISTINCT canonical_event_id) > 1
),
confirmed_without_sportsbook AS (
    SELECT
        ca.alias_id,
        ca.polymarket_event_id,
        ca.polymarket_market_id,
        ca.condition_id,
        ca.canonical_event_id,
        ca.alias_updated_at AS observed_at
    FROM confirmed_polymarket_event_aliases ca
    LEFT JOIN fair_value_moneyline_consensus fv
      ON fv.canonical_event_id = ca.canonical_event_id
    WHERE fv.canonical_event_id IS NULL
)
SELECT
    'conflicting_confirmed_polymarket_event_aliases'::text AS issue_type,
    'error'::text AS severity,
    'same Polymarket event has confirmed aliases to multiple canonical events'::text AS reason,
    polymarket_event_id,
    NULL::text AS polymarket_market_id,
    NULL::text AS condition_id,
    NULL::uuid AS canonical_event_id,
    canonical_event_ids,
    canonical_event_count,
    observed_at,
    jsonb_build_object(
        'conflict_scope', 'polymarket_event_id',
        'canonical_event_ids', canonical_event_ids
    ) AS details
FROM event_conflicts

UNION ALL

SELECT
    'conflicting_confirmed_polymarket_market_aliases'::text AS issue_type,
    'error'::text AS severity,
    'same Polymarket market or condition has confirmed aliases to multiple canonical events'::text AS reason,
    polymarket_event_id,
    polymarket_market_id,
    condition_id,
    NULL::uuid AS canonical_event_id,
    canonical_event_ids,
    canonical_event_count,
    observed_at,
    jsonb_build_object(
        'conflict_scope', 'polymarket_market_or_condition',
        'canonical_event_ids', canonical_event_ids
    ) AS details
FROM market_conflicts

UNION ALL

SELECT
    'confirmed_alias_without_current_sportsbook_quote'::text AS issue_type,
    'warning'::text AS severity,
    'confirmed Polymarket alias has no current h2h sportsbook consensus row'::text AS reason,
    polymarket_event_id,
    polymarket_market_id,
    condition_id,
    canonical_event_id,
    ARRAY[canonical_event_id]::uuid[] AS canonical_event_ids,
    1::bigint AS canonical_event_count,
    observed_at,
    jsonb_build_object(
        'alias_id', alias_id,
        'expected_view', 'fair_value_moneyline_consensus'
    ) AS details
FROM confirmed_without_sportsbook;

CREATE OR REPLACE VIEW data_quality_edge_readiness_inputs AS
WITH resolved_aliases AS (
    SELECT
        ca.alias_id,
        ca.polymarket_event_id,
        COALESCE(ca.polymarket_market_id, pm.polymarket_market_id) AS polymarket_market_id,
        COALESCE(ca.condition_id, pm.condition_id) AS condition_id,
        ca.canonical_event_id,
        ca.sport_key,
        ca.home_team,
        ca.away_team,
        ca.commence_time,
        ca.match_status,
        ca.confidence_score,
        ca.alias_updated_at
    FROM confirmed_polymarket_event_aliases ca
    LEFT JOIN polymarket_markets pm
      ON (
          ca.condition_id IS NOT NULL
          AND ca.condition_id = pm.condition_id
      )
      OR (
          ca.polymarket_market_id IS NOT NULL
          AND ca.polymarket_market_id = pm.polymarket_market_id
      )
),
latest_token_prices AS (
    SELECT DISTINCT ON (pps.token_id)
        pps.token_id,
        pps.best_bid,
        pps.best_ask,
        pps.midpoint,
        pps.spread,
        pps.liquidity,
        pps.received_at,
        pps.source_timestamp
    FROM polymarket_price_snapshots pps
    ORDER BY pps.token_id, pps.received_at DESC, pps.id DESC
),
readiness AS (
    SELECT
        ra.alias_id,
        ra.canonical_event_id,
        ra.sport_key,
        ra.home_team,
        ra.away_team,
        ra.commence_time,
        ra.polymarket_event_id,
        ra.polymarket_market_id,
        ra.condition_id,
        pt.token_id,
        pt.outcome_name AS polymarket_outcome_name,
        ra.match_status AS mapping_status,
        ra.confidence_score AS mapping_confidence,
        fv.outcome_name AS sportsbook_outcome,
        fv.average_no_vig_probability AS sportsbook_fair_probability,
        fv.latest_received_at AS sportsbook_latest_received_at,
        ltp.best_bid AS polymarket_best_bid,
        ltp.best_ask AS polymarket_best_ask,
        ltp.midpoint AS polymarket_midpoint,
        ltp.spread AS polymarket_spread,
        ltp.liquidity AS polymarket_liquidity,
        ltp.received_at AS polymarket_price_received_at,
        edge.edge_to_buy_cost,
        edge.analytical_midpoint_edge,
        (ra.alias_id IS NOT NULL) AS has_confirmed_alias,
        (pt.token_id IS NOT NULL) AS has_polymarket_token,
        (fv.canonical_event_id IS NOT NULL) AS has_sportsbook_consensus,
        (ltp.token_id IS NOT NULL) AS has_polymarket_price_snapshot,
        COALESCE(ltp.received_at >= now() - interval '15 minutes', false) AS has_recent_polymarket_price,
        (edge.canonical_event_id IS NOT NULL) AS has_edge_row,
        ra.alias_updated_at
    FROM resolved_aliases ra
    LEFT JOIN polymarket_tokens pt
      ON pt.condition_id = ra.condition_id
    LEFT JOIN latest_token_prices ltp
      ON ltp.token_id = pt.token_id
    LEFT JOIN fair_value_moneyline_consensus fv
      ON fv.canonical_event_id = ra.canonical_event_id
     AND trim(regexp_replace(lower(coalesce(fv.outcome_name, '')), '[^a-z0-9]+', ' ', 'g'))
       = trim(regexp_replace(lower(coalesce(pt.outcome_name, '')), '[^a-z0-9]+', ' ', 'g'))
    LEFT JOIN sportsbook_polymarket_moneyline_edges edge
      ON edge.canonical_event_id = ra.canonical_event_id
     AND edge.polymarket_token_id = pt.token_id
)
SELECT
    CASE
        WHEN has_edge_row THEN 'ready'
        WHEN has_polymarket_price_snapshot AND NOT has_recent_polymarket_price THEN 'degraded'
        ELSE 'blocked'
    END AS readiness_status,
    concat_ws(
        '; ',
        CASE WHEN condition_id IS NULL THEN 'confirmed alias has no market/condition mapping' END,
        CASE WHEN NOT has_polymarket_token THEN 'no Polymarket token for alias condition' END,
        CASE WHEN NOT has_sportsbook_consensus THEN 'no matching current sportsbook h2h consensus outcome' END,
        CASE WHEN NOT has_polymarket_price_snapshot THEN 'no Polymarket price snapshot for token' END,
        CASE WHEN has_polymarket_price_snapshot AND NOT has_recent_polymarket_price THEN 'Polymarket price snapshot older than 15 minutes' END,
        CASE WHEN has_polymarket_token AND has_sportsbook_consensus AND has_polymarket_price_snapshot AND NOT has_edge_row THEN 'inputs exist but edge row is not produced' END
    ) AS reason,
    alias_id,
    canonical_event_id,
    sport_key,
    home_team,
    away_team,
    commence_time,
    polymarket_event_id,
    polymarket_market_id,
    condition_id,
    token_id,
    polymarket_outcome_name,
    sportsbook_outcome,
    sportsbook_fair_probability,
    polymarket_best_bid,
    polymarket_best_ask,
    polymarket_midpoint,
    polymarket_spread,
    polymarket_liquidity,
    edge_to_buy_cost,
    analytical_midpoint_edge,
    mapping_status,
    mapping_confidence,
    has_confirmed_alias,
    has_polymarket_token,
    has_sportsbook_consensus,
    has_polymarket_price_snapshot,
    has_recent_polymarket_price,
    has_edge_row,
    sportsbook_latest_received_at,
    polymarket_price_received_at,
    jsonb_build_object(
        'price_recency_threshold', '15 minutes',
        'alias_updated_at', alias_updated_at
    ) AS details
FROM readiness;
