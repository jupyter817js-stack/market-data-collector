CREATE OR REPLACE VIEW sportsbook_betfair_moneyline_comparison AS
WITH sportsbook AS (
    SELECT
        canonical_event_id,
        sport_key,
        league,
        home_team,
        away_team,
        commence_time,
        outcome_name,
        outcome_side,
        average_no_vig_probability,
        min_no_vig_probability,
        max_no_vig_probability,
        bookmaker_count,
        latest_received_at
    FROM fair_value_moneyline_consensus
    WHERE average_no_vig_probability IS NOT NULL
      AND average_no_vig_probability > 0
),
betfair AS (
    SELECT *
    FROM betfair_exchange_quote_readiness
    WHERE canonical_market_family = 'h2h'
)
SELECT
    s.canonical_event_id,
    s.sport_key,
    s.league,
    s.home_team,
    s.away_team,
    s.commence_time,
    s.outcome_name AS sportsbook_outcome_name,
    s.outcome_side AS sportsbook_outcome_side,
    s.average_no_vig_probability AS sportsbook_avg_no_vig_probability,
    s.min_no_vig_probability AS sportsbook_min_no_vig_probability,
    s.max_no_vig_probability AS sportsbook_max_no_vig_probability,
    s.bookmaker_count AS sportsbook_bookmaker_count,
    s.latest_received_at AS sportsbook_latest_received_at,
    CASE
        WHEN s.average_no_vig_probability > 0
        THEN 1::numeric / s.average_no_vig_probability
    END AS sportsbook_fair_decimal,
    b.betfair_event_id,
    b.betfair_market_id,
    b.selection_id,
    b.handicap,
    b.runner_name,
    b.canonical_outcome_name,
    b.canonical_outcome_side,
    b.best_back_price,
    b.best_back_size,
    b.best_lay_price,
    b.best_lay_size,
    b.back_lay_spread,
    b.relative_spread,
    b.last_traded_price,
    b.traded_volume,
    b.market_status,
    b.in_play,
    b.market_time,
    b.source_publish_time,
    b.received_at AS betfair_received_at,
    b.latest_updated_at AS betfair_latest_updated_at,
    b.readiness_state AS betfair_readiness_state,
    b.readiness_score AS betfair_readiness_score,
    b.readiness_reasons AS betfair_readiness_reasons,
    b.stale_state AS betfair_stale_state,
    b.confidence_score AS betfair_quote_confidence_score,
    b.normalized_confidence AS betfair_normalized_confidence,
    b.event_alias_confidence_score AS betfair_event_alias_confidence_score,
    b.market_alias_confidence_score AS betfair_market_alias_confidence_score,
    b.runner_alias_confidence_score AS betfair_runner_alias_confidence_score,
    b.back_quote_key,
    b.lay_quote_key,
    b.back_quote_hash,
    b.lay_quote_hash,
    CASE
        WHEN b.best_back_price IS NOT NULL
         AND b.best_back_price > 1
        THEN 1::numeric / b.best_back_price
    END AS betfair_back_implied_probability,
    CASE
        WHEN b.best_lay_price IS NOT NULL
         AND b.best_lay_price > 1
        THEN 1::numeric / b.best_lay_price
    END AS betfair_lay_implied_probability,
    CASE
        WHEN s.average_no_vig_probability > 0
         AND b.best_back_price IS NOT NULL
         AND b.best_back_price > 1
        THEN s.average_no_vig_probability - (1::numeric / b.best_back_price)
    END AS back_probability_gap,
    CASE
        WHEN s.average_no_vig_probability > 0
         AND b.best_lay_price IS NOT NULL
         AND b.best_lay_price > 1
        THEN (1::numeric / b.best_lay_price) - s.average_no_vig_probability
    END AS lay_probability_gap,
    CASE
        WHEN s.average_no_vig_probability > 0
         AND b.best_back_price IS NOT NULL
        THEN b.best_back_price - (1::numeric / s.average_no_vig_probability)
    END AS back_price_gap_decimal,
    CASE
        WHEN s.average_no_vig_probability > 0
         AND b.best_lay_price IS NOT NULL
        THEN b.best_lay_price - (1::numeric / s.average_no_vig_probability)
    END AS lay_price_gap_decimal
FROM sportsbook s
JOIN betfair b
  ON b.canonical_event_id = s.canonical_event_id
 AND b.canonical_outcome_name = s.outcome_name
 AND b.canonical_outcome_side = s.outcome_side;

CREATE OR REPLACE VIEW sportsbook_betfair_moneyline_ready_comparison AS
SELECT *
FROM sportsbook_betfair_moneyline_comparison
WHERE betfair_readiness_state = 'ready';

CREATE OR REPLACE VIEW data_quality_betfair_sportsbook_comparison_anomalies AS
WITH sportsbook AS (
    SELECT
        canonical_event_id,
        sport_key,
        league,
        home_team,
        away_team,
        commence_time,
        outcome_name,
        outcome_side,
        bookmaker_count,
        latest_received_at
    FROM fair_value_moneyline_consensus
),
betfair AS (
    SELECT *
    FROM betfair_exchange_quote_readiness
    WHERE canonical_market_family = 'h2h'
),
matched AS (
    SELECT *
    FROM sportsbook_betfair_moneyline_comparison
),
missing_ready_betfair AS (
    SELECT
        'missing_ready_betfair_row'::text AS anomaly_type,
        'review'::text AS severity,
        s.canonical_event_id,
        s.sport_key,
        s.league,
        s.home_team,
        s.away_team,
        s.commence_time,
        s.outcome_name AS sportsbook_outcome_name,
        s.outcome_side AS sportsbook_outcome_side,
        NULL::text AS betfair_event_id,
        NULL::text AS betfair_market_id,
        NULL::bigint AS selection_id,
        NULL::numeric(12,4) AS handicap,
        NULL::text AS canonical_outcome_name,
        NULL::text AS canonical_outcome_side,
        NULL::text AS betfair_readiness_state,
        'sportsbook moneyline consensus has no exact matched ready Betfair h2h runner book'::text AS reason,
        s.latest_received_at AS sportsbook_latest_received_at,
        NULL::timestamptz AS betfair_received_at
    FROM sportsbook s
    WHERE NOT EXISTS (
        SELECT 1
        FROM matched m
        WHERE m.canonical_event_id = s.canonical_event_id
          AND m.sportsbook_outcome_name = s.outcome_name
          AND m.sportsbook_outcome_side = s.outcome_side
          AND m.betfair_readiness_state = 'ready'
    )
),
missing_sportsbook AS (
    SELECT
        'missing_sportsbook_consensus'::text AS anomaly_type,
        'review'::text AS severity,
        b.canonical_event_id,
        b.sport_key,
        b.league,
        b.home_team,
        b.away_team,
        b.commence_time,
        NULL::text AS sportsbook_outcome_name,
        NULL::text AS sportsbook_outcome_side,
        b.betfair_event_id,
        b.betfair_market_id,
        b.selection_id,
        b.handicap,
        b.canonical_outcome_name,
        b.canonical_outcome_side,
        b.readiness_state AS betfair_readiness_state,
        'ready Betfair h2h runner book has no exact matched sportsbook moneyline consensus outcome'::text AS reason,
        NULL::timestamptz AS sportsbook_latest_received_at,
        b.received_at AS betfair_received_at
    FROM betfair b
    WHERE b.readiness_state = 'ready'
      AND NOT EXISTS (
        SELECT 1
        FROM sportsbook s
        WHERE s.canonical_event_id = b.canonical_event_id
          AND s.outcome_name = b.canonical_outcome_name
          AND s.outcome_side = b.canonical_outcome_side
    )
),
betfair_not_ready AS (
    SELECT
        'betfair_not_ready'::text AS anomaly_type,
        'review'::text AS severity,
        m.canonical_event_id,
        m.sport_key,
        m.league,
        m.home_team,
        m.away_team,
        m.commence_time,
        m.sportsbook_outcome_name,
        m.sportsbook_outcome_side,
        m.betfair_event_id,
        m.betfair_market_id,
        m.selection_id,
        m.handicap,
        m.canonical_outcome_name,
        m.canonical_outcome_side,
        m.betfair_readiness_state,
        'exact matched Betfair h2h runner book is not ready'::text AS reason,
        m.sportsbook_latest_received_at,
        m.betfair_received_at
    FROM matched m
    WHERE m.betfair_readiness_state <> 'ready'
),
insufficient_sportsbook AS (
    SELECT
        'insufficient_sportsbook_bookmaker_count'::text AS anomaly_type,
        'review'::text AS severity,
        m.canonical_event_id,
        m.sport_key,
        m.league,
        m.home_team,
        m.away_team,
        m.commence_time,
        m.sportsbook_outcome_name,
        m.sportsbook_outcome_side,
        m.betfair_event_id,
        m.betfair_market_id,
        m.selection_id,
        m.handicap,
        m.canonical_outcome_name,
        m.canonical_outcome_side,
        m.betfair_readiness_state,
        'sportsbook moneyline consensus has fewer than two contributing bookmakers'::text AS reason,
        m.sportsbook_latest_received_at,
        m.betfair_received_at
    FROM matched m
    WHERE m.sportsbook_bookmaker_count < 2
)
SELECT *
FROM missing_ready_betfair
UNION ALL
SELECT *
FROM missing_sportsbook
UNION ALL
SELECT *
FROM betfair_not_ready
UNION ALL
SELECT *
FROM insufficient_sportsbook;
