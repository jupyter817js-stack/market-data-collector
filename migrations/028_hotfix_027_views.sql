-- Hotfix: migration 027 was marked applied but the
-- pinnacle_polymarket_moneyline_edges + cs2 views are missing from public
-- (only present in test_027/test_fresh schemas). Re-create them.
-- Strictly additive: no DROP, plain CREATE OR REPLACE on non-existent views.

CREATE OR REPLACE VIEW pinnacle_polymarket_moneyline_edges AS
WITH pinnacle_fair AS (
    SELECT
        m.id AS matchup_id,
        l.id AS league_id,
        l.sport_id AS pinnacle_sport_id,
        l.name AS league_name,
        l.league_group,
        m.home_name,
        m.away_name,
        m.start_time,
        m.last_is_live,
        h.status AS pinnacle_market_status,
        h.version AS pinnacle_market_version,
        h.received_at AS pinnacle_received_at,
        (SELECT (e->>'price')::int FROM jsonb_array_elements(h.prices) e WHERE e->>'designation' = 'home') AS home_amer,
        (SELECT (e->>'price')::int FROM jsonb_array_elements(h.prices) e WHERE e->>'designation' = 'away') AS away_amer
    FROM pinnacle_matchups m
    JOIN pinnacle_leagues l ON l.id = m.league_id
    JOIN pinnacle_market_latest ml ON ml.matchup_id = m.id
    JOIN pinnacle_market_history h ON h.id = ml.history_id
    WHERE h.market_type = 'moneyline'
      AND h.period = 0
      AND h.is_alternate = false
      AND h.status = 'open'
      AND m.matchup_type = 'matchup'
),
pinnacle_novig AS (
    SELECT
        pf.*,
        pinnacle_american_to_implied_prob(pf.home_amer) AS home_raw_prob,
        pinnacle_american_to_implied_prob(pf.away_amer) AS away_raw_prob
    FROM pinnacle_fair pf
    WHERE pf.home_amer IS NOT NULL AND pf.away_amer IS NOT NULL
),
pinnacle_novig_normalized AS (
    SELECT
        pn.*,
        CASE WHEN (pn.home_raw_prob + pn.away_raw_prob) > 0
             THEN pn.home_raw_prob / (pn.home_raw_prob + pn.away_raw_prob) END AS home_fair_prob,
        CASE WHEN (pn.home_raw_prob + pn.away_raw_prob) > 0
             THEN pn.away_raw_prob / (pn.home_raw_prob + pn.away_raw_prob) END AS away_fair_prob,
        (pn.home_raw_prob + pn.away_raw_prob) - 1.0 AS pinnacle_overround
    FROM pinnacle_novig pn
),
joined AS (
    SELECT
        pn.matchup_id,
        pn.league_id,
        pn.pinnacle_sport_id,
        pn.league_name,
        pn.league_group,
        pn.home_name,
        pn.away_name,
        pn.start_time,
        pn.last_is_live,
        pn.pinnacle_market_status,
        pn.pinnacle_market_version,
        pn.pinnacle_received_at,
        pn.home_amer,
        pn.away_amer,
        pn.home_fair_prob,
        pn.away_fair_prob,
        pn.pinnacle_overround,
        pa.polymarket_condition_id,
        pa.match_confidence,
        pm.polymarket_market_id,
        pm.polymarket_event_id,
        pm.question AS polymarket_question,
        pt.token_id,
        pt.outcome_name,
        pp.bid_price,
        pp.ask_price,
        pp.mid_price,
        pp.last_trade_price,
        pp.spread,
        pp.liquidity,
        pp.received_at AS pm_received_at,
        CASE
            WHEN pn.home_name <> '' AND LOWER(pt.outcome_name) LIKE '%' || LOWER(pn.home_name) || '%' THEN 'home'
            WHEN pn.away_name <> '' AND LOWER(pt.outcome_name) LIKE '%' || LOWER(pn.away_name) || '%' THEN 'away'
            WHEN pn.home_name <> '' AND LOWER(pn.home_name) LIKE '%' || LOWER(pt.outcome_name) || '%' THEN 'home'
            WHEN pn.away_name <> '' AND LOWER(pn.away_name) LIKE '%' || LOWER(pt.outcome_name) || '%' THEN 'away'
            ELSE 'unknown'
        END AS outcome_side
    FROM pinnacle_novig_normalized pn
    JOIN pinnacle_event_aliases pa
      ON pa.pinnacle_matchup_id = pn.matchup_id
     AND pa.match_status = 'confirmed'
    JOIN polymarket_markets pm
      ON pm.condition_id = pa.polymarket_condition_id
    JOIN polymarket_tokens pt
      ON pt.condition_id = pm.condition_id
    LEFT JOIN polymarket_latest_token_prices pp
      ON pp.token_id = pt.token_id
)
SELECT
    j.*,
    CASE j.outcome_side
        WHEN 'home' THEN j.home_fair_prob
        WHEN 'away' THEN j.away_fair_prob END AS pinnacle_fair_prob,
    CASE j.outcome_side
        WHEN 'home' THEN j.home_fair_prob - j.ask_price
        WHEN 'away' THEN j.away_fair_prob - j.ask_price END AS gross_edge_to_buy,
    CASE j.outcome_side
        WHEN 'home' THEN j.bid_price - j.home_fair_prob
        WHEN 'away' THEN j.bid_price - j.away_fair_prob END AS gross_edge_to_sell,
    EXTRACT(EPOCH FROM (now() - j.pinnacle_received_at))                              AS pinnacle_age_sec,
    EXTRACT(EPOCH FROM (now() - COALESCE(j.pm_received_at, j.pinnacle_received_at)))  AS pm_age_sec
FROM joined j
WHERE j.outcome_side <> 'unknown';

CREATE OR REPLACE VIEW pinnacle_polymarket_cs2_moneyline_edges AS
SELECT * FROM pinnacle_polymarket_moneyline_edges
 WHERE pinnacle_sport_id = 12
   AND (LOWER(league_name) LIKE '%cs2%' OR LOWER(league_name) LIKE '%cs:go%' OR LOWER(league_name) LIKE '%counter%');
