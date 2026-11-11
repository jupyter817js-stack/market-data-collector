CREATE OR REPLACE VIEW betfair_mapped_runner_books AS
WITH mapped AS (
    SELECT *
    FROM fair_value_ready_betfair_exchange_quotes
),
paired AS (
    SELECT
        min(source) AS source,
        canonical_event_id,
        min(sport_key) AS sport_key,
        min(league) AS league,
        min(home_team) AS home_team,
        min(away_team) AS away_team,
        min(commence_time) AS commence_time,
        min(betfair_event_id) AS betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        max(runner_name) AS runner_name,
        canonical_market_family,
        line,
        canonical_outcome_name,
        canonical_outcome_side,
        max(price_decimal) FILTER (WHERE exchange_side = 'back') AS best_back_price,
        max(available_size) FILTER (WHERE exchange_side = 'back') AS best_back_size,
        max(price_decimal) FILTER (WHERE exchange_side = 'lay') AS best_lay_price,
        max(available_size) FILTER (WHERE exchange_side = 'lay') AS best_lay_size,
        max(quote_key) FILTER (WHERE exchange_side = 'back') AS back_quote_key,
        max(quote_key) FILTER (WHERE exchange_side = 'lay') AS lay_quote_key,
        max(quote_hash) FILTER (WHERE exchange_side = 'back') AS back_quote_hash,
        max(quote_hash) FILTER (WHERE exchange_side = 'lay') AS lay_quote_hash,
        coalesce(
            max(last_traded_price) FILTER (WHERE exchange_side = 'back'),
            max(last_traded_price) FILTER (WHERE exchange_side = 'lay')
        ) AS last_traded_price,
        coalesce(
            max(traded_volume) FILTER (WHERE exchange_side = 'back'),
            max(traded_volume) FILTER (WHERE exchange_side = 'lay')
        ) AS traded_volume,
        max(market_status) AS market_status,
        bool_or(coalesce(in_play, false)) AS in_play,
        max(market_time) AS market_time,
        max(source_publish_time) AS source_publish_time,
        max(received_at) AS received_at,
        max(latest_updated_at) AS latest_updated_at,
        CASE
            WHEN bool_or(stale_state = 'stale') THEN 'stale'
            WHEN bool_or(stale_state = 'aging') THEN 'aging'
            WHEN bool_or(stale_state = 'unknown') THEN 'unknown'
            ELSE 'fresh'
        END AS stale_state,
        min(confidence_score) AS confidence_score,
        min(event_alias_confidence_score) AS event_alias_confidence_score,
        min(market_alias_confidence_score) AS market_alias_confidence_score,
        min(runner_alias_confidence_score) AS runner_alias_confidence_score,
        bool_or(exchange_side = 'back') AS has_back_side,
        bool_or(exchange_side = 'lay') AS has_lay_side
    FROM mapped
    GROUP BY
        canonical_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        canonical_market_family,
        line,
        canonical_outcome_name,
        canonical_outcome_side
)
SELECT
    source,
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    betfair_event_id,
    betfair_market_id,
    selection_id,
    handicap,
    runner_name,
    canonical_market_family,
    line,
    canonical_outcome_name,
    canonical_outcome_side,
    best_back_price,
    best_back_size,
    best_lay_price,
    best_lay_size,
    back_quote_key,
    lay_quote_key,
    back_quote_hash,
    lay_quote_hash,
    last_traded_price,
    traded_volume,
    market_status,
    in_play,
    market_time,
    source_publish_time,
    received_at,
    latest_updated_at,
    stale_state,
    confidence_score,
    event_alias_confidence_score,
    market_alias_confidence_score,
    runner_alias_confidence_score,
    has_back_side,
    has_lay_side,
    CASE
        WHEN has_back_side
         AND has_lay_side
         AND best_back_price IS NOT NULL
         AND best_lay_price IS NOT NULL
        THEN best_lay_price - best_back_price
    END AS back_lay_spread,
    CASE
        WHEN has_back_side
         AND has_lay_side
         AND best_back_price IS NOT NULL
         AND best_back_price > 0
         AND best_lay_price IS NOT NULL
        THEN (best_lay_price - best_back_price) / best_back_price
    END AS relative_spread,
    CASE
        WHEN has_back_side
         AND has_lay_side
         AND best_back_size IS NOT NULL
         AND best_lay_size IS NOT NULL
        THEN least(best_back_size, best_lay_size)
    END AS min_available_size,
    coalesce(best_back_size, 0) + coalesce(best_lay_size, 0) AS total_visible_size
FROM paired;

CREATE OR REPLACE VIEW data_quality_betfair_exchange_anomalies AS
WITH thresholds AS (
    SELECT
        0.05::numeric AS wide_relative_spread,
        10::numeric AS min_visible_size,
        0.70::numeric AS min_normalized_confidence
),
books AS (
    SELECT
        b.*,
        least(
            CASE
                WHEN b.confidence_score IS NULL THEN 0
                WHEN b.confidence_score > 1 THEN b.confidence_score / 100
                ELSE b.confidence_score
            END,
            CASE
                WHEN b.event_alias_confidence_score IS NULL THEN 0
                WHEN b.event_alias_confidence_score > 1 THEN b.event_alias_confidence_score / 100
                ELSE b.event_alias_confidence_score
            END,
            CASE
                WHEN b.market_alias_confidence_score IS NULL THEN 0
                WHEN b.market_alias_confidence_score > 1 THEN b.market_alias_confidence_score / 100
                ELSE b.market_alias_confidence_score
            END,
            CASE
                WHEN b.runner_alias_confidence_score IS NULL THEN 0
                WHEN b.runner_alias_confidence_score > 1 THEN b.runner_alias_confidence_score / 100
                ELSE b.runner_alias_confidence_score
            END
        ) AS normalized_confidence
    FROM betfair_mapped_runner_books b
),
book_anomalies AS (
    SELECT
        'missing_back_side'::text AS anomaly_type,
        'error'::text AS severity,
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        canonical_market_family,
        canonical_outcome_name,
        NULL::text AS exchange_side,
        'mapped Betfair runner is missing a best available back side'::text AS reason,
        source_publish_time,
        received_at,
        market_status,
        in_play,
        stale_state,
        confidence_score
    FROM books
    WHERE NOT has_back_side

    UNION ALL

    SELECT
        'missing_lay_side',
        'error',
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        canonical_market_family,
        canonical_outcome_name,
        NULL::text,
        'mapped Betfair runner is missing a best available lay side',
        source_publish_time,
        received_at,
        market_status,
        in_play,
        stale_state,
        confidence_score
    FROM books
    WHERE NOT has_lay_side

    UNION ALL

    SELECT
        'stale_quote',
        'warning',
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        canonical_market_family,
        canonical_outcome_name,
        NULL::text,
        'mapped Betfair runner book contains stale source state',
        source_publish_time,
        received_at,
        market_status,
        in_play,
        stale_state,
        confidence_score
    FROM books
    WHERE stale_state = 'stale'

    UNION ALL

    SELECT
        'aging_quote',
        'warning',
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        canonical_market_family,
        canonical_outcome_name,
        NULL::text,
        'mapped Betfair runner book contains aging source state',
        source_publish_time,
        received_at,
        market_status,
        in_play,
        stale_state,
        confidence_score
    FROM books
    WHERE stale_state = 'aging'

    UNION ALL

    SELECT
        'suspended_or_closed_market',
        'error',
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        canonical_market_family,
        canonical_outcome_name,
        NULL::text,
        'Betfair market status is suspended, closed, inactive, or settled',
        source_publish_time,
        received_at,
        market_status,
        in_play,
        stale_state,
        confidence_score
    FROM books
    WHERE upper(coalesce(market_status, '')) IN ('SUSPENDED', 'CLOSED', 'INACTIVE', 'SETTLED')

    UNION ALL

    SELECT
        'in_play_market',
        'warning',
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        canonical_market_family,
        canonical_outcome_name,
        NULL::text,
        'Betfair market is in-play and is not ready in the offline foundation workflow',
        source_publish_time,
        received_at,
        market_status,
        in_play,
        stale_state,
        confidence_score
    FROM books
    WHERE in_play

    UNION ALL

    SELECT
        'negative_spread',
        'error',
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        canonical_market_family,
        canonical_outcome_name,
        NULL::text,
        'Betfair best lay price is below best back price',
        source_publish_time,
        received_at,
        market_status,
        in_play,
        stale_state,
        confidence_score
    FROM books
    WHERE back_lay_spread < 0

    UNION ALL

    SELECT
        'wide_spread',
        'warning',
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        canonical_market_family,
        canonical_outcome_name,
        NULL::text,
        'Betfair relative back/lay spread is greater than 5%',
        source_publish_time,
        received_at,
        market_status,
        in_play,
        stale_state,
        confidence_score
    FROM books
    CROSS JOIN thresholds
    WHERE relative_spread > thresholds.wide_relative_spread

    UNION ALL

    SELECT
        'thin_liquidity',
        'warning',
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        canonical_market_family,
        canonical_outcome_name,
        NULL::text,
        'Betfair minimum visible back/lay size is below 10',
        source_publish_time,
        received_at,
        market_status,
        in_play,
        stale_state,
        confidence_score
    FROM books
    CROSS JOIN thresholds
    WHERE min_available_size IS NOT NULL
      AND min_available_size < thresholds.min_visible_size

    UNION ALL

    SELECT
        'low_confidence',
        'warning',
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        canonical_market_family,
        canonical_outcome_name,
        NULL::text,
        'Betfair quote or mapping confidence is below 0.70 after scale normalization',
        source_publish_time,
        received_at,
        market_status,
        in_play,
        stale_state,
        confidence_score
    FROM books
    CROSS JOIN thresholds
    WHERE normalized_confidence < thresholds.min_normalized_confidence
),
mapping_anomalies AS (
    SELECT
        'mapping_conflict'::text AS anomaly_type,
        severity,
        canonical_event_id,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        NULL::text AS runner_name,
        NULL::text AS canonical_market_family,
        NULL::text AS canonical_outcome_name,
        NULL::text AS exchange_side,
        reason,
        NULL::timestamptz AS source_publish_time,
        observed_at AS received_at,
        NULL::text AS market_status,
        NULL::boolean AS in_play,
        NULL::text AS stale_state,
        NULL::numeric AS confidence_score
    FROM betfair_mapping_conflicts

    UNION ALL

    SELECT
        'missing_event_mapping',
        'review',
        NULL::uuid,
        betfair_event_id,
        NULL::text,
        NULL::bigint,
        NULL::numeric(12,4),
        NULL::text,
        NULL::text,
        NULL::text,
        NULL::text,
        'Betfair event has replay/imported quotes but no confirmed event alias',
        NULL::timestamptz,
        latest_received_at,
        NULL::text,
        NULL::boolean,
        latest_alias_status,
        latest_alias_confidence_score
    FROM betfair_unmapped_events

    UNION ALL

    SELECT
        'missing_market_mapping',
        'review',
        NULL::uuid,
        betfair_event_id,
        betfair_market_id,
        NULL::bigint,
        NULL::numeric(12,4),
        NULL::text,
        betfair_market_family,
        NULL::text,
        NULL::text,
        'Betfair market has replay/imported quotes but no confirmed market alias',
        NULL::timestamptz,
        latest_received_at,
        NULL::text,
        NULL::boolean,
        latest_alias_status,
        latest_alias_confidence_score
    FROM betfair_unmapped_markets

    UNION ALL

    SELECT
        'missing_runner_mapping',
        'review',
        NULL::uuid,
        betfair_event_id,
        betfair_market_id,
        selection_id,
        handicap,
        runner_name,
        NULL::text,
        NULL::text,
        NULL::text,
        'Betfair runner has replay/imported quotes but no confirmed runner alias',
        NULL::timestamptz,
        latest_received_at,
        NULL::text,
        NULL::boolean,
        latest_alias_status,
        latest_alias_confidence_score
    FROM betfair_unmapped_runners
)
SELECT *
FROM book_anomalies
UNION ALL
SELECT *
FROM mapping_anomalies;

CREATE OR REPLACE VIEW betfair_exchange_quote_readiness AS
WITH thresholds AS (
    SELECT
        0.05::numeric AS wide_relative_spread,
        10::numeric AS min_visible_size,
        0.70::numeric AS min_normalized_confidence
),
evaluated AS (
    SELECT
        b.*,
        EXISTS (
            SELECT 1
            FROM betfair_mapping_conflicts c
            WHERE (
                c.betfair_event_id IS NOT NULL
                AND c.betfair_event_id = b.betfair_event_id
            )
               OR (
                c.betfair_market_id IS NOT NULL
                AND c.betfair_market_id = b.betfair_market_id
            )
               OR (
                c.betfair_market_id IS NOT NULL
                AND c.betfair_market_id = b.betfair_market_id
                AND c.selection_id IS NOT NULL
                AND c.selection_id = b.selection_id
                AND c.handicap IS NOT NULL
                AND c.handicap = b.handicap
            )
        ) AS has_mapping_conflict,
        upper(coalesce(b.market_status, '')) IN ('SUSPENDED', 'CLOSED', 'INACTIVE', 'SETTLED') AS is_suspended_or_closed,
        least(
            CASE
                WHEN b.confidence_score IS NULL THEN 0
                WHEN b.confidence_score > 1 THEN b.confidence_score / 100
                ELSE b.confidence_score
            END,
            CASE
                WHEN b.event_alias_confidence_score IS NULL THEN 0
                WHEN b.event_alias_confidence_score > 1 THEN b.event_alias_confidence_score / 100
                ELSE b.event_alias_confidence_score
            END,
            CASE
                WHEN b.market_alias_confidence_score IS NULL THEN 0
                WHEN b.market_alias_confidence_score > 1 THEN b.market_alias_confidence_score / 100
                ELSE b.market_alias_confidence_score
            END,
            CASE
                WHEN b.runner_alias_confidence_score IS NULL THEN 0
                WHEN b.runner_alias_confidence_score > 1 THEN b.runner_alias_confidence_score / 100
                ELSE b.runner_alias_confidence_score
            END
        ) AS normalized_confidence
    FROM betfair_mapped_runner_books b
)
SELECT
    CASE
        WHEN has_mapping_conflict THEN 'mapping_conflict'
        WHEN NOT has_back_side OR NOT has_lay_side THEN 'missing_side'
        WHEN stale_state = 'stale' THEN 'stale'
        WHEN stale_state = 'aging' THEN 'aging'
        WHEN is_suspended_or_closed THEN 'suspended'
        WHEN in_play THEN 'in_play'
        WHEN back_lay_spread < 0 THEN 'needs_review'
        WHEN relative_spread > thresholds.wide_relative_spread THEN 'wide_spread'
        WHEN min_available_size IS NULL OR min_available_size < thresholds.min_visible_size THEN 'thin_liquidity'
        WHEN normalized_confidence < thresholds.min_normalized_confidence THEN 'low_confidence'
        WHEN stale_state <> 'fresh' THEN 'needs_review'
        ELSE 'ready'
    END AS readiness_state,
    greatest(
        0,
        100
        - CASE WHEN has_mapping_conflict THEN 40 ELSE 0 END
        - CASE WHEN NOT has_back_side OR NOT has_lay_side THEN 35 ELSE 0 END
        - CASE WHEN stale_state = 'stale' THEN 30 ELSE 0 END
        - CASE WHEN stale_state = 'aging' THEN 15 ELSE 0 END
        - CASE WHEN is_suspended_or_closed THEN 35 ELSE 0 END
        - CASE WHEN in_play THEN 25 ELSE 0 END
        - CASE WHEN back_lay_spread < 0 THEN 40 ELSE 0 END
        - CASE WHEN relative_spread > thresholds.wide_relative_spread THEN 20 ELSE 0 END
        - CASE WHEN min_available_size IS NULL OR min_available_size < thresholds.min_visible_size THEN 20 ELSE 0 END
        - CASE WHEN normalized_confidence < thresholds.min_normalized_confidence THEN 20 ELSE 0 END
        - CASE WHEN stale_state NOT IN ('fresh', 'aging', 'stale') THEN 20 ELSE 0 END
    )::int AS readiness_score,
    array_remove(ARRAY[
        CASE WHEN has_mapping_conflict THEN 'Betfair mapping conflict is present' END,
        CASE WHEN NOT has_back_side THEN 'missing best available back side' END,
        CASE WHEN NOT has_lay_side THEN 'missing best available lay side' END,
        CASE WHEN stale_state = 'stale' THEN 'quote freshness is stale' END,
        CASE WHEN stale_state = 'aging' THEN 'quote freshness is aging' END,
        CASE WHEN is_suspended_or_closed THEN 'market status is suspended, closed, inactive, or settled' END,
        CASE WHEN in_play THEN 'market is in-play and not supported for readiness' END,
        CASE WHEN back_lay_spread < 0 THEN 'best lay price is below best back price' END,
        CASE WHEN relative_spread > thresholds.wide_relative_spread THEN 'relative back/lay spread is greater than 5%' END,
        CASE WHEN min_available_size IS NULL THEN 'minimum visible size cannot be computed' END,
        CASE WHEN min_available_size < thresholds.min_visible_size THEN 'minimum visible size is below 10' END,
        CASE WHEN normalized_confidence < thresholds.min_normalized_confidence THEN 'quote or mapping confidence is below 0.70 after scale normalization' END,
        CASE WHEN stale_state NOT IN ('fresh', 'aging', 'stale') THEN 'quote freshness is unknown' END
    ], NULL) AS readiness_reasons,
    source,
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    betfair_event_id,
    betfair_market_id,
    selection_id,
    handicap,
    runner_name,
    canonical_market_family,
    line,
    canonical_outcome_name,
    canonical_outcome_side,
    best_back_price,
    best_back_size,
    best_lay_price,
    best_lay_size,
    back_lay_spread,
    relative_spread,
    min_available_size,
    total_visible_size,
    last_traded_price,
    traded_volume,
    market_status,
    in_play,
    market_time,
    source_publish_time,
    received_at,
    latest_updated_at,
    stale_state,
    confidence_score,
    normalized_confidence,
    event_alias_confidence_score,
    market_alias_confidence_score,
    runner_alias_confidence_score,
    back_quote_key,
    lay_quote_key,
    back_quote_hash,
    lay_quote_hash,
    has_back_side,
    has_lay_side,
    has_mapping_conflict
FROM evaluated
CROSS JOIN thresholds;
