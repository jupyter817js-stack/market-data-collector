CREATE OR REPLACE VIEW sportsbook_polymarket_moneyline_edge_readiness AS
WITH thresholds AS (
    SELECT
        interval '5 minutes' AS sportsbook_stale_after,
        interval '15 minutes' AS polymarket_stale_after,
        2::bigint AS min_bookmaker_count,
        70::numeric AS min_mapping_confidence,
        0.05::numeric AS max_spread,
        1::numeric AS min_liquidity
),
latest_token_prices AS (
    SELECT DISTINCT ON (pps.token_id)
        pps.token_id,
        pps.condition_id,
        pps.polymarket_market_id,
        COALESCE(pps.best_bid, pps.bid_price) AS polymarket_best_bid,
        COALESCE(pps.best_ask, pps.ask_price) AS polymarket_best_ask,
        COALESCE(pps.midpoint, pps.mid_price) AS polymarket_midpoint,
        COALESCE(
            pps.spread,
            CASE
                WHEN COALESCE(pps.best_ask, pps.ask_price) IS NOT NULL
                 AND COALESCE(pps.best_bid, pps.bid_price) IS NOT NULL
                 AND COALESCE(pps.best_ask, pps.ask_price) >= COALESCE(pps.best_bid, pps.bid_price)
                THEN COALESCE(pps.best_ask, pps.ask_price) - COALESCE(pps.best_bid, pps.bid_price)
            END
        ) AS polymarket_spread,
        pps.liquidity AS polymarket_liquidity,
        pps.received_at AS polymarket_price_received_at,
        pps.source_timestamp AS polymarket_source_timestamp
    FROM polymarket_price_snapshots pps
    ORDER BY pps.token_id, pps.received_at DESC, pps.id DESC
),
confirmed_inputs AS (
    SELECT
        'confirmed_alias'::text AS input_kind,
        dq.alias_id,
        dq.canonical_event_id,
        COALESCE(edge.sport_key, dq.sport_key, fv.sport_key) AS sport_key,
        fv.league,
        COALESCE(edge.home_team, dq.home_team, fv.home_team) AS home_team,
        COALESCE(edge.away_team, dq.away_team, fv.away_team) AS away_team,
        COALESCE(edge.commence_time, dq.commence_time, fv.commence_time) AS commence_time,
        dq.polymarket_event_id,
        dq.polymarket_market_id,
        dq.condition_id,
        dq.token_id AS polymarket_token_id,
        dq.polymarket_outcome_name,
        COALESCE(edge.sportsbook_outcome, dq.sportsbook_outcome, fv.outcome_name) AS sportsbook_outcome,
        fv.outcome_side AS sportsbook_outcome_side,
        COALESCE(edge.sportsbook_fair_probability, dq.sportsbook_fair_probability, fv.average_no_vig_probability) AS sportsbook_fair_probability,
        fv.min_no_vig_probability AS sportsbook_min_fair_probability,
        fv.max_no_vig_probability AS sportsbook_max_fair_probability,
        COALESCE(edge.sportsbook_bookmaker_count, fv.bookmaker_count) AS sportsbook_bookmaker_count,
        COALESCE(edge.polymarket_best_bid, dq.polymarket_best_bid) AS polymarket_best_bid,
        COALESCE(edge.polymarket_best_ask, dq.polymarket_best_ask) AS polymarket_best_ask,
        COALESCE(edge.polymarket_midpoint, dq.polymarket_midpoint) AS polymarket_midpoint,
        COALESCE(edge.polymarket_spread, dq.polymarket_spread) AS polymarket_spread,
        COALESCE(edge.polymarket_liquidity, dq.polymarket_liquidity) AS polymarket_liquidity,
        COALESCE(edge.sportsbook_latest_received_at, dq.sportsbook_latest_received_at, fv.latest_received_at) AS sportsbook_latest_received_at,
        COALESCE(edge.polymarket_price_received_at, dq.polymarket_price_received_at) AS polymarket_price_received_at,
        edge.polymarket_source_timestamp,
        dq.mapping_status AS alias_status,
        dq.mapping_confidence AS alias_confidence,
        NULL::text AS match_review_status,
        NULL::numeric AS match_review_confidence,
        NULL::text AS match_reason,
        ARRAY[]::uuid[] AS candidate_canonical_event_ids,
        dq.has_edge_row,
        dq.reason AS input_reason,
        dq.details AS input_details
    FROM data_quality_edge_readiness_inputs dq
    LEFT JOIN sportsbook_polymarket_moneyline_edges edge
      ON edge.canonical_event_id = dq.canonical_event_id
     AND edge.polymarket_token_id = dq.token_id
    LEFT JOIN fair_value_moneyline_consensus fv
      ON fv.canonical_event_id = dq.canonical_event_id
     AND trim(regexp_replace(lower(coalesce(fv.outcome_name, '')), '[^a-z0-9]+', ' ', 'g'))
       = trim(regexp_replace(lower(coalesce(coalesce(edge.sportsbook_outcome, dq.sportsbook_outcome), dq.polymarket_outcome_name, '')), '[^a-z0-9]+', ' ', 'g'))
),
review_inputs AS (
    SELECT
        'match_review'::text AS input_kind,
        NULL::bigint AS alias_id,
        NULL::uuid AS canonical_event_id,
        review.sport_key,
        review.league,
        NULL::text AS home_team,
        NULL::text AS away_team,
        review.raw_start_time AS commence_time,
        review.polymarket_event_id,
        NULLIF(review.polymarket_market_id, '') AS polymarket_market_id,
        NULLIF(review.condition_id, '') AS condition_id,
        pt.token_id AS polymarket_token_id,
        pt.outcome_name AS polymarket_outcome_name,
        NULL::text AS sportsbook_outcome,
        NULL::text AS sportsbook_outcome_side,
        NULL::numeric AS sportsbook_fair_probability,
        NULL::numeric AS sportsbook_min_fair_probability,
        NULL::numeric AS sportsbook_max_fair_probability,
        NULL::bigint AS sportsbook_bookmaker_count,
        ltp.polymarket_best_bid,
        ltp.polymarket_best_ask,
        ltp.polymarket_midpoint,
        ltp.polymarket_spread,
        ltp.polymarket_liquidity,
        NULL::timestamptz AS sportsbook_latest_received_at,
        ltp.polymarket_price_received_at,
        ltp.polymarket_source_timestamp,
        'unconfirmed'::text AS alias_status,
        NULL::numeric AS alias_confidence,
        review.match_status AS match_review_status,
        review.confidence_score AS match_review_confidence,
        review.match_reason,
        review.candidate_canonical_event_ids,
        false AS has_edge_row,
        'Polymarket event is still in manual match review'::text AS input_reason,
        jsonb_build_object(
            'review_id', review.review_id,
            'candidate_canonical_event_ids', review.candidate_canonical_event_ids,
            'title', review.title
        ) AS input_details
    FROM (
        SELECT * FROM unresolved_polymarket_event_matches
        UNION ALL
        SELECT * FROM ambiguous_polymarket_event_matches
    ) review
    LEFT JOIN polymarket_tokens pt
      ON pt.condition_id = NULLIF(review.condition_id, '')
    LEFT JOIN latest_token_prices ltp
      ON ltp.token_id = pt.token_id
),
base_inputs AS (
    SELECT * FROM confirmed_inputs
    UNION ALL
    SELECT * FROM review_inputs
),
evaluated AS (
    SELECT
        base_inputs.*,
        thresholds.sportsbook_stale_after,
        thresholds.polymarket_stale_after,
        thresholds.min_bookmaker_count,
        thresholds.min_mapping_confidence,
        thresholds.max_spread,
        thresholds.min_liquidity,
        CASE
            WHEN sportsbook_latest_received_at IS NULL THEN 'missing'
            WHEN sportsbook_latest_received_at < now() - thresholds.sportsbook_stale_after THEN 'stale'
            WHEN sportsbook_latest_received_at < now() - interval '2 minutes' THEN 'aging'
            ELSE 'fresh'
        END AS quote_freshness,
        CASE
            WHEN polymarket_price_received_at IS NULL THEN 'missing'
            WHEN polymarket_price_received_at < now() - thresholds.polymarket_stale_after THEN 'stale'
            WHEN polymarket_price_received_at < now() - interval '5 minutes' THEN 'aging'
            ELSE 'fresh'
        END AS polymarket_price_freshness,
        (
            canonical_event_id IS NULL
            OR lower(coalesce(alias_status, '')) <> 'confirmed'
            OR alias_confidence IS NULL
            OR alias_confidence < thresholds.min_mapping_confidence
        ) AS mapping_uncertain,
        (
            sportsbook_fair_probability IS NULL
            OR sportsbook_outcome IS NULL
            OR sportsbook_outcome = ''
        ) AS missing_sportsbook_consensus,
        (
            sportsbook_bookmaker_count IS NULL
            OR sportsbook_bookmaker_count < thresholds.min_bookmaker_count
        ) AS insufficient_books,
        (
            sportsbook_latest_received_at IS NULL
            OR sportsbook_latest_received_at < now() - thresholds.sportsbook_stale_after
        ) AS stale_sportsbook,
        (
            polymarket_best_bid IS NULL
            OR polymarket_best_ask IS NULL
        ) AS missing_price,
        (
            polymarket_price_received_at IS NULL
            OR polymarket_price_received_at < now() - thresholds.polymarket_stale_after
        ) AS stale_polymarket,
        (
            polymarket_liquidity IS NULL
            OR polymarket_liquidity < thresholds.min_liquidity
        ) AS thin_liquidity,
        (
            polymarket_spread IS NOT NULL
            AND polymarket_spread > thresholds.max_spread
        ) AS wide_spread,
        NOT has_edge_row AS missing_edge_row
    FROM base_inputs
    CROSS JOIN thresholds
)
SELECT
    input_kind,
    alias_id,
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    polymarket_event_id,
    polymarket_market_id,
    condition_id,
    polymarket_token_id,
    polymarket_outcome_name,
    sportsbook_outcome,
    sportsbook_outcome_side,
    sportsbook_fair_probability,
    sportsbook_min_fair_probability,
    sportsbook_max_fair_probability,
    sportsbook_bookmaker_count AS bookmaker_count,
    polymarket_midpoint,
    polymarket_best_bid AS best_bid,
    polymarket_best_ask AS best_ask,
    polymarket_spread AS spread,
    polymarket_liquidity AS liquidity,
    polymarket_price_received_at AS latest_price_received_at,
    polymarket_source_timestamp,
    alias_status,
    alias_confidence,
    match_review_status,
    match_review_confidence,
    match_reason,
    candidate_canonical_event_ids,
    sportsbook_latest_received_at,
    quote_freshness,
    polymarket_price_freshness,
    has_edge_row,
    CASE
        WHEN mapping_uncertain THEN 'mapping_uncertain'
        WHEN missing_sportsbook_consensus THEN 'needs_review'
        WHEN insufficient_books THEN 'insufficient_books'
        WHEN stale_sportsbook THEN 'stale_sportsbook'
        WHEN missing_price THEN 'missing_price'
        WHEN stale_polymarket THEN 'stale_polymarket'
        WHEN thin_liquidity THEN 'thin_liquidity'
        WHEN wide_spread THEN 'wide_spread'
        WHEN missing_edge_row THEN 'needs_review'
        ELSE 'ready'
    END AS readiness_state,
    greatest(
        0,
        100
        - CASE WHEN mapping_uncertain THEN 35 ELSE 0 END
        - CASE WHEN missing_sportsbook_consensus THEN 25 ELSE 0 END
        - CASE WHEN insufficient_books THEN 20 ELSE 0 END
        - CASE WHEN stale_sportsbook THEN 20 ELSE 0 END
        - CASE WHEN missing_price THEN 30 ELSE 0 END
        - CASE WHEN stale_polymarket THEN 25 ELSE 0 END
        - CASE WHEN thin_liquidity THEN 15 ELSE 0 END
        - CASE WHEN wide_spread THEN 15 ELSE 0 END
        - CASE WHEN missing_edge_row THEN 10 ELSE 0 END
    )::int AS readiness_score,
    array_remove(ARRAY[
        CASE WHEN mapping_uncertain THEN 'mapping is not confirmed or confidence is below threshold' END,
        CASE WHEN missing_sportsbook_consensus THEN 'matching sportsbook moneyline consensus is missing' END,
        CASE WHEN insufficient_books THEN 'sportsbook consensus has fewer than 2 contributing bookmakers' END,
        CASE WHEN stale_sportsbook THEN 'sportsbook quote freshness is missing or older than 5 minutes' END,
        CASE WHEN missing_price THEN 'Polymarket best bid or best ask is missing; midpoint alone is analytical' END,
        CASE WHEN stale_polymarket THEN 'Polymarket price snapshot is missing or older than 15 minutes' END,
        CASE WHEN thin_liquidity THEN 'Polymarket liquidity is missing or below threshold' END,
        CASE WHEN wide_spread THEN 'Polymarket spread is wider than 0.05 probability units' END,
        CASE WHEN missing_edge_row THEN 'current edge view does not produce this row' END,
        NULLIF(input_reason, '')
    ], NULL) AS readiness_reasons,
    jsonb_build_object(
        'sportsbook_stale_after', sportsbook_stale_after::text,
        'polymarket_stale_after', polymarket_stale_after::text,
        'min_bookmaker_count', min_bookmaker_count,
        'min_mapping_confidence', min_mapping_confidence,
        'max_spread', max_spread,
        'min_liquidity', min_liquidity,
        'input_details', input_details
    ) AS readiness_details
FROM evaluated;
