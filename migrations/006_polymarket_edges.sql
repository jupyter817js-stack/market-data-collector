CREATE OR REPLACE VIEW sportsbook_polymarket_moneyline_edges AS
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
        ranked.bid_price,
        ranked.ask_price,
        ranked.mid_price,
        ranked.last_price,
        ranked.received_at,
        CASE
            WHEN ranked.mid_price IS NOT NULL THEN ranked.mid_price
            WHEN ranked.bid_price IS NOT NULL AND ranked.ask_price IS NOT NULL THEN (ranked.bid_price + ranked.ask_price) / 2
            WHEN ranked.last_price IS NOT NULL THEN ranked.last_price
            WHEN ranked.ask_price IS NOT NULL THEN ranked.ask_price
            WHEN ranked.bid_price IS NOT NULL THEN ranked.bid_price
        END AS latest_polymarket_price,
        CASE
            WHEN ranked.mid_price IS NOT NULL THEN 'mid_price'
            WHEN ranked.bid_price IS NOT NULL AND ranked.ask_price IS NOT NULL THEN 'bid_ask_mid'
            WHEN ranked.last_price IS NOT NULL THEN 'last_price'
            WHEN ranked.ask_price IS NOT NULL THEN 'ask_price'
            WHEN ranked.bid_price IS NOT NULL THEN 'bid_price'
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
        lp.latest_polymarket_price,
        lp.polymarket_price_source,
        fv.average_no_vig_probability - lp.latest_polymarket_price AS edge,
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
    alias_updated_at
FROM edge_candidates
WHERE latest_polymarket_price IS NOT NULL
  AND sportsbook_outcome_norm <> ''
  AND sportsbook_outcome_norm = polymarket_outcome_norm;
