ALTER TABLE polymarket_price_snapshots
    ADD COLUMN IF NOT EXISTS best_bid numeric(18,8),
    ADD COLUMN IF NOT EXISTS best_ask numeric(18,8),
    ADD COLUMN IF NOT EXISTS midpoint numeric(18,8),
    ADD COLUMN IF NOT EXISTS spread numeric(18,8),
    ADD COLUMN IF NOT EXISTS last_trade_price numeric(18,8),
    ADD COLUMN IF NOT EXISTS liquidity numeric(24,8),
    ADD COLUMN IF NOT EXISTS depth_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS source_timestamp timestamptz,
    ADD COLUMN IF NOT EXISTS raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb;

UPDATE polymarket_price_snapshots
SET
    best_bid = COALESCE(best_bid, bid_price),
    best_ask = COALESCE(best_ask, ask_price),
    midpoint = COALESCE(midpoint, mid_price),
    spread = COALESCE(
        spread,
        CASE
            WHEN ask_price IS NOT NULL
             AND bid_price IS NOT NULL
             AND ask_price >= bid_price
            THEN ask_price - bid_price
        END
    ),
    last_trade_price = COALESCE(last_trade_price, last_price),
    liquidity = COALESCE(
        liquidity,
        CASE
            WHEN best_bid_size IS NOT NULL
              OR best_ask_size IS NOT NULL
            THEN COALESCE(best_bid_size, 0) + COALESCE(best_ask_size, 0)
        END
    ),
    depth_summary = CASE
        WHEN depth_summary = '{}'::jsonb
         AND (best_bid_size IS NOT NULL OR best_ask_size IS NOT NULL)
        THEN jsonb_strip_nulls(jsonb_build_object(
            'best_bid_size', best_bid_size,
            'best_ask_size', best_ask_size
        ))
        ELSE depth_summary
    END,
    raw_payload = CASE
        WHEN raw_payload = '{}'::jsonb
         AND clob_payload <> '{}'::jsonb
        THEN clob_payload
        ELSE raw_payload
    END
WHERE best_bid IS NULL
   OR best_ask IS NULL
   OR midpoint IS NULL
   OR spread IS NULL
   OR last_trade_price IS NULL
   OR liquidity IS NULL
   OR depth_summary = '{}'::jsonb
   OR raw_payload = '{}'::jsonb;

CREATE INDEX IF NOT EXISTS polymarket_price_snapshots_market_received_idx
    ON polymarket_price_snapshots (polymarket_market_id, received_at DESC);

CREATE INDEX IF NOT EXISTS polymarket_price_snapshots_token_received_idx
    ON polymarket_price_snapshots (token_id, received_at DESC);

CREATE INDEX IF NOT EXISTS polymarket_price_snapshots_condition_received_idx
    ON polymarket_price_snapshots (condition_id, received_at DESC);

DROP VIEW IF EXISTS sportsbook_polymarket_moneyline_edges;

CREATE VIEW sportsbook_polymarket_moneyline_edges AS
WITH confirmed_market_aliases AS (
    SELECT DISTINCT ON (pa.canonical_event_id, pm.condition_id)
        pa.id AS alias_id,
        pa.canonical_event_id,
        COALESCE(pa.polymarket_event_id, pm.polymarket_event_id) AS polymarket_event_id,
        pm.polymarket_market_id,
        pm.condition_id,
        pm.question AS polymarket_question,
        pa.match_status,
        pa.match_confidence,
        pa.updated_at AS alias_updated_at
    FROM polymarket_event_aliases pa
    JOIN polymarket_markets pm
      ON (
          pa.condition_id IS NOT NULL
          AND pa.condition_id = pm.condition_id
      )
      OR (
          pa.polymarket_market_id IS NOT NULL
          AND pa.polymarket_market_id = pm.polymarket_market_id
      )
    WHERE lower(pa.match_status) = 'confirmed'
    ORDER BY pa.canonical_event_id, pm.condition_id, pa.updated_at DESC, pa.id DESC
),
latest_polymarket_prices AS (
    SELECT
        ranked.token_id,
        ranked.condition_id,
        ranked.polymarket_market_id,
        COALESCE(ranked.best_bid, ranked.bid_price) AS polymarket_best_bid,
        COALESCE(ranked.best_ask, ranked.ask_price) AS polymarket_best_ask,
        COALESCE(ranked.midpoint, ranked.mid_price) AS polymarket_midpoint,
        COALESCE(
            ranked.spread,
            CASE
                WHEN COALESCE(ranked.best_ask, ranked.ask_price) IS NOT NULL
                 AND COALESCE(ranked.best_bid, ranked.bid_price) IS NOT NULL
                 AND COALESCE(ranked.best_ask, ranked.ask_price) >= COALESCE(ranked.best_bid, ranked.bid_price)
                THEN COALESCE(ranked.best_ask, ranked.ask_price) - COALESCE(ranked.best_bid, ranked.bid_price)
            END
        ) AS polymarket_spread,
        COALESCE(ranked.last_trade_price, ranked.last_price) AS polymarket_last_trade_price,
        ranked.liquidity AS polymarket_liquidity,
        ranked.depth_summary,
        ranked.source_timestamp,
        ranked.received_at,
        COALESCE(ranked.best_ask, ranked.ask_price) AS latest_polymarket_price,
        CASE
            WHEN COALESCE(ranked.best_ask, ranked.ask_price) IS NOT NULL THEN 'best_ask_buy_cost'
        END AS polymarket_price_source
    FROM (
        SELECT
            pps.*,
            row_number() OVER (
                PARTITION BY pps.token_id
                ORDER BY pps.received_at DESC, pps.id DESC
            ) AS rn
        FROM polymarket_price_snapshots pps
    ) ranked
    WHERE ranked.rn = 1
),
edge_candidates AS (
    SELECT
        fv.canonical_event_id,
        fv.sport_key,
        fv.league,
        fv.home_team,
        fv.away_team,
        fv.commence_time,
        fv.outcome_name AS sportsbook_outcome,
        fv.outcome_side AS sportsbook_outcome_side,
        fv.average_no_vig_probability AS sportsbook_fair_probability,
        fv.min_no_vig_probability AS sportsbook_min_fair_probability,
        fv.max_no_vig_probability AS sportsbook_max_fair_probability,
        fv.bookmaker_count AS sportsbook_bookmaker_count,
        alias.polymarket_event_id,
        alias.polymarket_market_id,
        alias.condition_id,
        alias.polymarket_question,
        pt.token_id AS polymarket_token_id,
        pt.outcome_name AS polymarket_outcome_name,
        NULLIF(lower(trim(pt.side)), '') AS polymarket_yes_no_side,
        lp.polymarket_best_bid,
        lp.polymarket_best_ask,
        lp.polymarket_midpoint,
        lp.polymarket_spread,
        lp.polymarket_last_trade_price,
        lp.polymarket_liquidity,
        lp.source_timestamp AS polymarket_source_timestamp,
        lp.latest_polymarket_price,
        lp.polymarket_price_source,
        fv.average_no_vig_probability - lp.polymarket_best_ask AS edge,
        fv.average_no_vig_probability - lp.polymarket_best_ask AS edge_to_buy_cost,
        fv.average_no_vig_probability - lp.polymarket_best_bid AS edge_to_sell_value,
        fv.average_no_vig_probability - lp.polymarket_midpoint AS analytical_midpoint_edge,
        fv.latest_received_at AS sportsbook_latest_received_at,
        lp.received_at AS polymarket_price_received_at,
        alias.match_status AS mapping_status,
        alias.match_confidence AS mapping_confidence,
        alias.alias_updated_at,
        trim(regexp_replace(lower(coalesce(fv.outcome_name, '')), '[^a-z0-9]+', ' ', 'g')) AS sportsbook_outcome_norm,
        trim(regexp_replace(lower(coalesce(pt.outcome_name, '')), '[^a-z0-9]+', ' ', 'g')) AS polymarket_outcome_norm
    FROM fair_value_moneyline_consensus fv
    JOIN confirmed_market_aliases alias
      ON alias.canonical_event_id = fv.canonical_event_id
    JOIN polymarket_tokens pt
      ON pt.condition_id = alias.condition_id
    JOIN latest_polymarket_prices lp
      ON lp.token_id = pt.token_id
)
SELECT
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    sportsbook_outcome,
    sportsbook_outcome_side,
    sportsbook_fair_probability,
    sportsbook_min_fair_probability,
    sportsbook_max_fair_probability,
    sportsbook_bookmaker_count,
    polymarket_event_id,
    polymarket_market_id,
    condition_id,
    polymarket_question,
    polymarket_token_id,
    polymarket_outcome_name,
    polymarket_yes_no_side,
    latest_polymarket_price,
    polymarket_price_source,
    edge,
    sportsbook_latest_received_at,
    polymarket_price_received_at,
    mapping_status,
    mapping_confidence,
    alias_updated_at,
    polymarket_best_bid,
    polymarket_best_ask,
    polymarket_midpoint,
    polymarket_spread,
    polymarket_last_trade_price,
    polymarket_liquidity,
    polymarket_source_timestamp,
    edge_to_buy_cost,
    edge_to_sell_value,
    analytical_midpoint_edge
FROM edge_candidates
WHERE polymarket_best_ask IS NOT NULL
  AND sportsbook_outcome_norm <> ''
  AND sportsbook_outcome_norm = polymarket_outcome_norm;
