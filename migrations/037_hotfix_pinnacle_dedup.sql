-- Pinnacle issues multiple matchup_type='matchup' rows for the same
-- real-world game. Examples observed 2026-05-17:
--   Jeju SK v Anyang             — 3 matchup_ids ('matchup' type), 2 live simultaneously
--   Jeonbuk Motors v Gimcheon S. — 2 'matchup' live at once
-- All have identical home_name/away_name/start_time. The duplicates
-- carry the same odds (Pinnacle prices them once per game), so taking
-- ANY one of them is correct — but emitting the full Cartesian product
-- into the edge view triples downstream BUY_CANDIDATE noise and
-- pollutes paper_pnl_runs.
--
-- Fix: in both pinnacle_soccer (3-way) and pinnacle_fair (2-way)
-- CTEs, rank duplicates by (sport_id, normalized home, normalized
-- away, start_time) and keep only the row whose market_history row
-- is most recently updated. Pinnacle stops updating the orphan
-- entries quickly, so "most recent received_at" is a reliable
-- signal for "the canonical live entry".

DROP VIEW IF EXISTS pinnacle_polymarket_moneyline_edges_live;
DROP VIEW IF EXISTS pinnacle_polymarket_cs2_moneyline_edges;
DROP VIEW IF EXISTS pinnacle_polymarket_moneyline_edges;
DROP VIEW IF EXISTS pinnacle_polymarket_soccer_3way_edges;

CREATE VIEW pinnacle_polymarket_soccer_3way_edges AS
WITH pinnacle_soccer_raw AS (
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
        (SELECT (e->>'price')::int FROM jsonb_array_elements(h.prices) e WHERE e->>'designation' = 'draw') AS draw_amer,
        (SELECT (e->>'price')::int FROM jsonb_array_elements(h.prices) e WHERE e->>'designation' = 'away') AS away_amer,
        ROW_NUMBER() OVER (
            PARTITION BY l.sport_id,
                         normalize_team_name(m.home_name),
                         normalize_team_name(m.away_name),
                         m.start_time
            ORDER BY h.received_at DESC, m.id DESC
        ) AS dup_rank
    FROM pinnacle_matchups m
    JOIN pinnacle_leagues l       ON l.id = m.league_id
    JOIN pinnacle_market_latest ml ON ml.matchup_id = m.id
    JOIN pinnacle_market_history h ON h.id = ml.history_id
    WHERE l.sport_id = 29
      AND h.market_type = 'moneyline'
      AND h.period = 0
      AND h.is_alternate = false
      AND h.status = 'open'
      AND m.matchup_type = 'matchup'
      AND m.home_name <> ''
      AND m.away_name <> ''
      AND length(m.home_name) >= 4
      AND length(m.away_name) >= 4
      AND h.received_at > now() - interval '5 minutes'
),
pinnacle_soccer AS (
    SELECT * FROM pinnacle_soccer_raw WHERE dup_rank = 1
),
pinnacle_3way_fair AS (
    SELECT p.*,
           pinnacle_american_to_implied_prob(p.home_amer) AS home_raw,
           pinnacle_american_to_implied_prob(p.draw_amer) AS draw_raw,
           pinnacle_american_to_implied_prob(p.away_amer) AS away_raw
      FROM pinnacle_soccer p
     WHERE p.home_amer IS NOT NULL
       AND p.draw_amer IS NOT NULL
       AND p.away_amer IS NOT NULL
),
pinnacle_3way_norm AS (
    SELECT p.*,
           normalize_team_name(p.home_name) AS home_norm,
           normalize_team_name(p.away_name) AS away_norm,
           CASE WHEN (p.home_raw + p.draw_raw + p.away_raw) > 0
                THEN p.home_raw / (p.home_raw + p.draw_raw + p.away_raw) END AS home_fair_prob,
           CASE WHEN (p.home_raw + p.draw_raw + p.away_raw) > 0
                THEN p.draw_raw / (p.home_raw + p.draw_raw + p.away_raw) END AS draw_fair_prob,
           CASE WHEN (p.home_raw + p.draw_raw + p.away_raw) > 0
                THEN p.away_raw / (p.home_raw + p.draw_raw + p.away_raw) END AS away_fair_prob,
           (p.home_raw + p.draw_raw + p.away_raw) - 1.0 AS pinnacle_overround
      FROM pinnacle_3way_fair p
),
pm_classified AS (
    SELECT
        p.matchup_id, p.league_id, p.pinnacle_sport_id, p.league_name, p.league_group,
        p.home_name, p.away_name, p.start_time, p.last_is_live,
        p.pinnacle_market_status, p.pinnacle_market_version, p.pinnacle_received_at,
        p.home_amer, p.away_amer,
        p.home_fair_prob, p.away_fair_prob, p.draw_fair_prob, p.pinnacle_overround,
        pm.condition_id  AS polymarket_condition_id,
        pe.polymarket_event_id,
        pm.polymarket_market_id,
        pm.question      AS polymarket_question,
        p.home_norm, p.away_norm,
        CASE
            WHEN pm.question ILIKE '%end in a draw%'                                 THEN 'draw'
            WHEN pm.question ILIKE 'Will %win%'
                 AND all_words_present(p.home_norm, normalize_team_name(pm.question))
                 AND NOT all_words_present(p.away_norm, normalize_team_name(pm.question))
                                                                                     THEN 'home'
            WHEN pm.question ILIKE 'Will %win%'
                 AND all_words_present(p.away_norm, normalize_team_name(pm.question))
                 AND NOT all_words_present(p.home_norm, normalize_team_name(pm.question))
                                                                                     THEN 'away'
            ELSE NULL
        END AS outcome_side
    FROM pinnacle_3way_norm p
    JOIN polymarket_events pe
      ON pe.active = true
     AND pe.closed = false
     AND all_words_present(p.home_norm, normalize_team_name(pe.title))
     AND all_words_present(p.away_norm, normalize_team_name(pe.title))
    JOIN polymarket_markets pm
      ON pm.polymarket_event_id = pe.polymarket_event_id
     AND pm.active = true
     AND pm.closed = false
),
pm_yes_priced AS (
    SELECT c.*,
           pt.token_id, pt.outcome_name,
           pp.bid_price, pp.ask_price, pp.mid_price, pp.last_trade_price,
           pp.spread, pp.liquidity, pp.received_at AS pm_received_at
      FROM pm_classified c
      JOIN polymarket_tokens pt
        ON pt.condition_id = c.polymarket_condition_id
       AND pt.side = 'yes'
      LEFT JOIN polymarket_latest_token_prices pp
        ON pp.token_id = pt.token_id
     WHERE c.outcome_side IS NOT NULL
)
SELECT
    j.matchup_id, j.league_id, j.pinnacle_sport_id, j.league_name, j.league_group,
    j.home_name, j.away_name, j.start_time, j.last_is_live,
    j.pinnacle_market_status, j.pinnacle_market_version, j.pinnacle_received_at,
    j.home_amer, j.away_amer,
    j.home_fair_prob, j.away_fair_prob, j.pinnacle_overround,
    j.polymarket_condition_id,
    NULL::numeric(5,2) AS match_confidence,
    j.polymarket_market_id, j.polymarket_event_id, j.polymarket_question,
    j.token_id, j.outcome_name,
    j.bid_price, j.ask_price, j.mid_price, j.last_trade_price,
    j.spread, j.liquidity, j.pm_received_at,
    j.outcome_side,
    CASE j.outcome_side
        WHEN 'home' THEN j.home_fair_prob
        WHEN 'draw' THEN j.draw_fair_prob
        WHEN 'away' THEN j.away_fair_prob END AS pinnacle_fair_prob,
    CASE j.outcome_side
        WHEN 'home' THEN j.home_fair_prob - j.ask_price
        WHEN 'draw' THEN j.draw_fair_prob - j.ask_price
        WHEN 'away' THEN j.away_fair_prob - j.ask_price END AS gross_edge_to_buy,
    CASE j.outcome_side
        WHEN 'home' THEN j.bid_price - j.home_fair_prob
        WHEN 'draw' THEN j.bid_price - j.draw_fair_prob
        WHEN 'away' THEN j.bid_price - j.away_fair_prob END AS gross_edge_to_sell,
    EXTRACT(EPOCH FROM (now() - j.pinnacle_received_at))                              AS pinnacle_age_sec,
    EXTRACT(EPOCH FROM (now() - COALESCE(j.pm_received_at, j.pinnacle_received_at)))  AS pm_age_sec
FROM pm_yes_priced j;

-- Base view with the same dedup applied to the 2-way branch (sport_id<>29).
CREATE VIEW pinnacle_polymarket_moneyline_edges AS
WITH pinnacle_fair_raw AS (
    SELECT
        m.id AS matchup_id,
        l.id AS league_id,
        l.sport_id AS pinnacle_sport_id,
        l.name AS league_name,
        l.league_group,
        m.home_name, m.away_name, m.start_time, m.last_is_live,
        h.status AS pinnacle_market_status,
        h.version AS pinnacle_market_version,
        h.received_at AS pinnacle_received_at,
        (SELECT (e->>'price')::int FROM jsonb_array_elements(h.prices) e WHERE e->>'designation' = 'home') AS home_amer,
        (SELECT (e->>'price')::int FROM jsonb_array_elements(h.prices) e WHERE e->>'designation' = 'away') AS away_amer,
        ROW_NUMBER() OVER (
            PARTITION BY l.sport_id,
                         normalize_team_name(m.home_name),
                         normalize_team_name(m.away_name),
                         m.start_time
            ORDER BY h.received_at DESC, m.id DESC
        ) AS dup_rank
    FROM pinnacle_matchups m
    JOIN pinnacle_leagues l ON l.id = m.league_id
    JOIN pinnacle_market_latest ml ON ml.matchup_id = m.id
    JOIN pinnacle_market_history h ON h.id = ml.history_id
    WHERE h.market_type = 'moneyline'
      AND h.period = 0
      AND h.is_alternate = false
      AND h.status = 'open'
      AND m.matchup_type = 'matchup'
      AND l.sport_id <> 29
      AND h.received_at > now() - interval '5 minutes'
),
pinnacle_fair AS (
    SELECT * FROM pinnacle_fair_raw WHERE dup_rank = 1
),
pinnacle_novig AS (
    SELECT pf.*,
           pinnacle_american_to_implied_prob(pf.home_amer) AS home_raw_prob,
           pinnacle_american_to_implied_prob(pf.away_amer) AS away_raw_prob
      FROM pinnacle_fair pf
     WHERE pf.home_amer IS NOT NULL AND pf.away_amer IS NOT NULL
),
pinnacle_novig_normalized AS (
    SELECT pn.*,
           CASE WHEN (pn.home_raw_prob + pn.away_raw_prob) > 0
                THEN pn.home_raw_prob / (pn.home_raw_prob + pn.away_raw_prob) END AS home_fair_prob,
           CASE WHEN (pn.home_raw_prob + pn.away_raw_prob) > 0
                THEN pn.away_raw_prob / (pn.home_raw_prob + pn.away_raw_prob) END AS away_fair_prob,
           (pn.home_raw_prob + pn.away_raw_prob) - 1.0 AS pinnacle_overround
      FROM pinnacle_novig pn
),
two_way_joined AS (
    SELECT
        pn.matchup_id, pn.league_id, pn.pinnacle_sport_id, pn.league_name, pn.league_group,
        pn.home_name, pn.away_name, pn.start_time, pn.last_is_live,
        pn.pinnacle_market_status, pn.pinnacle_market_version, pn.pinnacle_received_at,
        pn.home_amer, pn.away_amer,
        pn.home_fair_prob, pn.away_fair_prob, pn.pinnacle_overround,
        pa.polymarket_condition_id, pa.match_confidence,
        pm.polymarket_market_id, pm.polymarket_event_id,
        pm.question AS polymarket_question,
        pt.token_id, pt.outcome_name,
        pp.bid_price, pp.ask_price, pp.mid_price, pp.last_trade_price,
        pp.spread, pp.liquidity, pp.received_at AS pm_received_at,
        CASE
            WHEN pn.home_name <> '' AND length(pn.home_name) >= 4
                 AND LOWER(pt.outcome_name) ~ ('\y' || LOWER(pn.home_name) || '\y') THEN 'home'
            WHEN pn.away_name <> '' AND length(pn.away_name) >= 4
                 AND LOWER(pt.outcome_name) ~ ('\y' || LOWER(pn.away_name) || '\y') THEN 'away'
            WHEN pn.home_name <> '' AND length(pt.outcome_name) >= 4
                 AND LOWER(pn.home_name) ~ ('\y' || LOWER(pt.outcome_name) || '\y') THEN 'home'
            WHEN pn.away_name <> '' AND length(pt.outcome_name) >= 4
                 AND LOWER(pn.away_name) ~ ('\y' || LOWER(pt.outcome_name) || '\y') THEN 'away'
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
    j.matchup_id, j.league_id, j.pinnacle_sport_id, j.league_name, j.league_group,
    j.home_name, j.away_name, j.start_time, j.last_is_live,
    j.pinnacle_market_status, j.pinnacle_market_version, j.pinnacle_received_at,
    j.home_amer, j.away_amer,
    j.home_fair_prob, j.away_fair_prob, j.pinnacle_overround,
    j.polymarket_condition_id, j.match_confidence,
    j.polymarket_market_id, j.polymarket_event_id, j.polymarket_question,
    j.token_id, j.outcome_name,
    j.bid_price, j.ask_price, j.mid_price, j.last_trade_price,
    j.spread, j.liquidity, j.pm_received_at,
    j.outcome_side,
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
FROM two_way_joined j
WHERE j.outcome_side <> 'unknown'

UNION ALL

SELECT * FROM pinnacle_polymarket_soccer_3way_edges;

CREATE VIEW pinnacle_polymarket_cs2_moneyline_edges AS
SELECT * FROM pinnacle_polymarket_moneyline_edges
 WHERE pinnacle_sport_id = 12
   AND (LOWER(league_name) LIKE '%cs2%' OR LOWER(league_name) LIKE '%cs:go%' OR LOWER(league_name) LIKE '%counter%');

CREATE VIEW pinnacle_polymarket_moneyline_edges_live AS
SELECT * FROM pinnacle_polymarket_moneyline_edges
 WHERE last_is_live = true
   AND ask_price IS NOT NULL
   AND ask_price > 0.005
   AND pm_age_sec < 120
   AND pinnacle_age_sec < 60;
