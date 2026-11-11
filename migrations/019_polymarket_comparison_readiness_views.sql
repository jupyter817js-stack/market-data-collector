CREATE OR REPLACE VIEW polymarket_comparison_coverage_audit AS
WITH thresholds AS (
    SELECT
        interval '15 minutes' AS polymarket_stale_after,
        interval '5 minutes'  AS polymarket_aging_after,
        1::numeric            AS min_liquidity
),
sportsbook AS (
    SELECT
        canonical_event_id,
        count(*) AS sportsbook_consensus_outcome_count,
        max(latest_received_at) AS latest_sportsbook_received_at
    FROM fair_value_moneyline_consensus
    GROUP BY canonical_event_id
),
betfair_mapped AS (
    SELECT
        canonical_event_id,
        count(*) AS betfair_mapped_quote_count,
        max(received_at) AS latest_betfair_received_at
    FROM fair_value_ready_betfair_exchange_quotes
    GROUP BY canonical_event_id
),
betfair_ready AS (
    SELECT
        canonical_event_id,
        count(*) FILTER (WHERE readiness_state = 'ready') AS betfair_ready_runner_book_count
    FROM betfair_exchange_quote_readiness
    GROUP BY canonical_event_id
),
polymarket_aliases AS (
    SELECT DISTINCT ON (canonical_event_id, condition_id)
        alias_id,
        canonical_event_id,
        polymarket_event_id,
        polymarket_market_id,
        condition_id,
        match_status,
        confidence_score AS alias_confidence,
        alias_updated_at
    FROM confirmed_polymarket_event_aliases
    WHERE condition_id IS NOT NULL
    ORDER BY canonical_event_id, condition_id, alias_updated_at DESC, alias_id DESC
),
polymarket_market_meta AS (
    SELECT
        condition_id,
        polymarket_market_id,
        polymarket_event_id,
        question,
        active,
        closed,
        archived,
        end_time
    FROM polymarket_markets
),
polymarket_token_summary AS (
    SELECT
        condition_id,
        count(*) AS polymarket_token_count
    FROM polymarket_tokens
    GROUP BY condition_id
),
latest_polymarket_prices AS (
    SELECT DISTINCT ON (pps.token_id)
        pps.token_id,
        pps.condition_id,
        pps.polymarket_market_id,
        COALESCE(pps.best_bid, pps.bid_price) AS best_bid,
        COALESCE(pps.best_ask, pps.ask_price) AS best_ask,
        COALESCE(pps.midpoint, pps.mid_price) AS midpoint,
        COALESCE(
            pps.spread,
            CASE
                WHEN COALESCE(pps.best_ask, pps.ask_price) IS NOT NULL
                 AND COALESCE(pps.best_bid, pps.bid_price) IS NOT NULL
                 AND COALESCE(pps.best_ask, pps.ask_price) >= COALESCE(pps.best_bid, pps.bid_price)
                THEN COALESCE(pps.best_ask, pps.ask_price) - COALESCE(pps.best_bid, pps.bid_price)
            END
        ) AS spread,
        pps.liquidity,
        pps.depth_summary,
        pps.received_at,
        pps.source_timestamp
    FROM polymarket_price_snapshots pps
    ORDER BY pps.token_id, pps.received_at DESC, pps.id DESC
),
polymarket_price_summary AS (
    SELECT
        pt.condition_id,
        count(DISTINCT pt.token_id) FILTER (WHERE lp.token_id IS NOT NULL) AS polymarket_price_snapshot_count,
        max(lp.received_at) AS latest_polymarket_price_time,
        bool_or(lp.best_bid IS NOT NULL) AS has_best_bid,
        bool_or(lp.best_ask IS NOT NULL) AS has_best_ask,
        bool_or(
            lp.liquidity IS NOT NULL
            OR (lp.depth_summary IS NOT NULL AND lp.depth_summary <> '{}'::jsonb)
        ) AS has_depth_or_liquidity,
        max(lp.liquidity) AS polymarket_max_liquidity,
        min(lp.spread)    AS polymarket_min_spread
    FROM polymarket_tokens pt
    LEFT JOIN latest_polymarket_prices lp ON lp.token_id = pt.token_id
    GROUP BY pt.condition_id
),
polymarket_edge_count AS (
    SELECT
        canonical_event_id,
        condition_id,
        count(*) AS polymarket_edge_row_count
    FROM sportsbook_polymarket_moneyline_edges
    GROUP BY canonical_event_id, condition_id
),
polymarket_edge_readiness_summary AS (
    SELECT
        canonical_event_id,
        condition_id,
        string_agg(DISTINCT readiness_state, ',' ORDER BY readiness_state) AS readiness_states,
        count(*) FILTER (WHERE readiness_state = 'ready') AS ready_polymarket_edge_row_count
    FROM sportsbook_polymarket_moneyline_edge_readiness
    WHERE canonical_event_id IS NOT NULL
    GROUP BY canonical_event_id, condition_id
),
base AS (
    SELECT
        ce.id AS canonical_event_id,
        ce.sport_key,
        ce.league,
        ce.home_team,
        ce.away_team,
        ce.commence_time,
        pa.alias_id AS polymarket_alias_id,
        coalesce(pa.polymarket_event_id, pmm.polymarket_event_id) AS polymarket_event_id,
        coalesce(pa.polymarket_market_id, pmm.polymarket_market_id) AS polymarket_market_id,
        pa.condition_id AS polymarket_condition_id,
        pa.match_status AS polymarket_alias_status,
        pa.alias_confidence AS polymarket_alias_confidence,
        pa.alias_updated_at AS polymarket_alias_updated_at,
        pmm.question AS polymarket_question,
        pmm.active AS polymarket_market_active,
        pmm.closed AS polymarket_market_closed,
        pmm.archived AS polymarket_market_archived,
        pmm.end_time AS polymarket_market_end_time,
        coalesce(pts.polymarket_token_count, 0) AS polymarket_token_count,
        coalesce(pps.polymarket_price_snapshot_count, 0) AS polymarket_price_snapshot_count,
        pps.latest_polymarket_price_time,
        coalesce(pps.has_best_bid, false) AS has_best_bid,
        coalesce(pps.has_best_ask, false) AS has_best_ask,
        coalesce(pps.has_depth_or_liquidity, false) AS has_depth_or_liquidity,
        pps.polymarket_max_liquidity,
        pps.polymarket_min_spread,
        coalesce(sb.sportsbook_consensus_outcome_count, 0) AS sportsbook_consensus_outcome_count,
        sb.latest_sportsbook_received_at,
        coalesce(bm.betfair_mapped_quote_count, 0) AS betfair_mapped_quote_count,
        bm.latest_betfair_received_at,
        coalesce(br.betfair_ready_runner_book_count, 0) AS betfair_ready_runner_book_count,
        coalesce(pec.polymarket_edge_row_count, 0) AS polymarket_edge_row_count,
        coalesce(pers.ready_polymarket_edge_row_count, 0) AS ready_polymarket_edge_row_count,
        coalesce(pers.readiness_states, '') AS readiness_states
    FROM canonical_events ce
    LEFT JOIN polymarket_aliases pa
      ON pa.canonical_event_id = ce.id
    LEFT JOIN polymarket_market_meta pmm
      ON pa.condition_id IS NOT NULL
     AND pmm.condition_id = pa.condition_id
    LEFT JOIN polymarket_token_summary pts
      ON pa.condition_id IS NOT NULL
     AND pts.condition_id = pa.condition_id
    LEFT JOIN polymarket_price_summary pps
      ON pa.condition_id IS NOT NULL
     AND pps.condition_id = pa.condition_id
    LEFT JOIN sportsbook sb
      ON sb.canonical_event_id = ce.id
    LEFT JOIN betfair_mapped bm
      ON bm.canonical_event_id = ce.id
    LEFT JOIN betfair_ready br
      ON br.canonical_event_id = ce.id
    LEFT JOIN polymarket_edge_count pec
      ON pec.canonical_event_id = ce.id
     AND pec.condition_id IS NOT DISTINCT FROM pa.condition_id
    LEFT JOIN polymarket_edge_readiness_summary pers
      ON pers.canonical_event_id = ce.id
     AND pers.condition_id IS NOT DISTINCT FROM pa.condition_id
)
SELECT
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    polymarket_alias_id,
    polymarket_event_id,
    polymarket_market_id,
    polymarket_condition_id,
    coalesce(polymarket_alias_status, '') AS polymarket_alias_status,
    polymarket_alias_confidence,
    polymarket_alias_updated_at,
    coalesce(polymarket_question, '') AS polymarket_question,
    polymarket_market_active,
    polymarket_market_closed,
    polymarket_market_archived,
    polymarket_market_end_time,
    polymarket_token_count,
    polymarket_price_snapshot_count,
    latest_polymarket_price_time,
    has_best_bid,
    has_best_ask,
    has_depth_or_liquidity,
    polymarket_max_liquidity,
    polymarket_min_spread,
    sportsbook_consensus_outcome_count,
    latest_sportsbook_received_at,
    betfair_mapped_quote_count,
    betfair_ready_runner_book_count,
    latest_betfair_received_at,
    polymarket_edge_row_count,
    ready_polymarket_edge_row_count,
    readiness_states,
    CASE
        WHEN latest_polymarket_price_time IS NULL THEN 'missing'
        WHEN latest_polymarket_price_time < now() - (SELECT polymarket_stale_after FROM thresholds) THEN 'stale'
        WHEN latest_polymarket_price_time < now() - (SELECT polymarket_aging_after FROM thresholds) THEN 'aging'
        ELSE 'fresh'
    END AS polymarket_stale_state,
    CASE
        WHEN polymarket_market_archived IS TRUE THEN 'archived'
        WHEN polymarket_market_closed IS TRUE THEN 'closed'
        WHEN polymarket_condition_id IS NULL THEN 'unknown'
        WHEN polymarket_market_active IS FALSE THEN 'inactive'
        ELSE 'open'
    END AS resolution_status,
    false AS resolution_verified,
    (sportsbook_consensus_outcome_count > 0) AS has_sportsbook_consensus,
    (betfair_ready_runner_book_count > 0) AS has_ready_betfair,
    (polymarket_alias_id IS NOT NULL) AS has_confirmed_polymarket_alias,
    (polymarket_market_id IS NOT NULL) AS has_polymarket_market_metadata,
    (polymarket_token_count > 0) AS has_polymarket_tokens,
    (has_best_bid AND has_best_ask) AS has_best_bid_and_ask,
    (
        sportsbook_consensus_outcome_count > 0
        AND polymarket_alias_id IS NOT NULL
        AND polymarket_token_count > 0
        AND has_best_ask
    ) AS gross_comparison_possible,
    false AS net_comparison_possible,
    CASE
        WHEN sportsbook_consensus_outcome_count = 0 AND betfair_ready_runner_book_count = 0
            THEN 'external_fair_value_missing'
        WHEN sportsbook_consensus_outcome_count = 0
            THEN 'sportsbook_consensus_missing'
        WHEN polymarket_alias_id IS NULL
            THEN 'polymarket_mapping_missing'
        WHEN polymarket_token_count = 0
            THEN 'polymarket_token_missing'
        WHEN polymarket_price_snapshot_count = 0 OR NOT has_best_ask OR NOT has_best_bid
            THEN 'polymarket_price_missing'
        WHEN latest_polymarket_price_time IS NULL
          OR latest_polymarket_price_time < now() - (SELECT polymarket_stale_after FROM thresholds)
            THEN 'polymarket_price_stale'
        WHEN NOT has_depth_or_liquidity
          OR (polymarket_max_liquidity IS NOT NULL
              AND polymarket_max_liquidity < (SELECT min_liquidity FROM thresholds))
            THEN 'polymarket_liquidity_low'
        WHEN polymarket_market_archived IS TRUE OR polymarket_market_closed IS TRUE
            THEN 'resolution_unverified'
        ELSE 'comparison_ready'
    END AS gap_reason,
    CASE
        WHEN sportsbook_consensus_outcome_count = 0 AND betfair_ready_runner_book_count = 0
            THEN 'add sportsbook moneyline coverage or wait for Betfair to become ready before comparing Polymarket'
        WHEN sportsbook_consensus_outcome_count = 0
            THEN 'add sportsbook moneyline coverage for this canonical event'
        WHEN polymarket_alias_id IS NULL
            THEN 'manually map and confirm a Polymarket alias for this canonical event'
        WHEN polymarket_token_count = 0
            THEN 'sync Polymarket tokens for the mapped market'
        WHEN polymarket_price_snapshot_count = 0 OR NOT has_best_ask OR NOT has_best_bid
            THEN 'run polymarket-sync to capture best bid and best ask snapshots'
        WHEN latest_polymarket_price_time IS NULL
          OR latest_polymarket_price_time < now() - (SELECT polymarket_stale_after FROM thresholds)
            THEN 'refresh Polymarket price snapshots before interpreting comparison'
        WHEN NOT has_depth_or_liquidity
          OR (polymarket_max_liquidity IS NOT NULL
              AND polymarket_max_liquidity < (SELECT min_liquidity FROM thresholds))
            THEN 'inspect Polymarket order book depth before interpreting comparison'
        WHEN polymarket_market_archived IS TRUE OR polymarket_market_closed IS TRUE
            THEN 'verify settlement and resolution semantics manually before interpreting comparison'
        ELSE 'gross comparison inputs are present; net comparison still requires a fee or cost model that is not implemented in this project'
    END AS next_manual_action,
    (
        sportsbook_consensus_outcome_count > 0
        AND polymarket_alias_id IS NOT NULL
        AND polymarket_token_count > 0
        AND has_best_ask AND has_best_bid
        AND latest_polymarket_price_time IS NOT NULL
        AND latest_polymarket_price_time >= now() - (SELECT polymarket_stale_after FROM thresholds)
        AND has_depth_or_liquidity
        AND (polymarket_max_liquidity IS NULL
             OR polymarket_max_liquidity >= (SELECT min_liquidity FROM thresholds))
        AND coalesce(polymarket_market_archived, false) = false
        AND coalesce(polymarket_market_closed, false) = false
    ) AS comparison_ready
FROM base;
