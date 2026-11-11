-- B1: Totals (over/under) edge view. Pinnacle stores total markets as
-- market_type='total' with prices[].designation in ('over','under') and
-- a shared `points` line value; Polymarket lists totals as binary
-- questions like "Team A vs. Team B: O/U 5.5" whose YES token is the
-- "Over X.5" side. Edge logic mirrors the moneyline view:
--
--   pinnacle_over_fair_prob  = pinnacle_over_raw / (over_raw + under_raw)
--   gross_edge_over_to_buy   = pinnacle_over_fair_prob - pm_yes_ask
--
-- This migration ships the view only (read-only audit / dashboard
-- surface). Wiring the signals table + signal-engine to read from it
-- is a follow-up commit because `signals.outcome_side` currently
-- enumerates home/draw/away and would need an `over`/`under` extension.
--
-- Matching strategy:
--   * Pinnacle side: period=0, is_alternate=false, status='open',
--     current ml.last_polled_at within 5 min (post-039 freshness).
--   * PM side: question matches a regex that pulls (home, away, line)
--     and the line equals Pinnacle's `points` value exactly. Alternate
--     lines (Pinnacle's is_alternate=true) ARE intentionally excluded
--     here; the main line is the one PM most commonly mirrors.
--   * Bridge: pinnacle_event_aliases.confirmed for the same matchup_id
--     is NOT required — pinnacle_event_aliases is per-condition (the
--     moneyline condition), totals are a separate PM market. We match
--     by team-name word-boundary on the PM question instead, same way
--     the 3-way soccer view does for non-aliased events.

CREATE OR REPLACE VIEW pinnacle_polymarket_totals_candidates AS
WITH pinnacle_totals_raw AS (
    SELECT
        m.id  AS matchup_id,
        l.id  AS league_id,
        l.sport_id AS pinnacle_sport_id,
        l.name AS league_name,
        m.home_name, m.away_name, m.start_time, m.last_is_live,
        h.status AS pinnacle_market_status,
        h.version AS pinnacle_market_version,
        ml.last_polled_at AS pinnacle_received_at,
        (SELECT (e->>'points')::numeric FROM jsonb_array_elements(h.prices) e
           WHERE e->>'designation' = 'over' LIMIT 1) AS line,
        (SELECT (e->>'price')::int FROM jsonb_array_elements(h.prices) e
           WHERE e->>'designation' = 'over' LIMIT 1) AS over_amer,
        (SELECT (e->>'price')::int FROM jsonb_array_elements(h.prices) e
           WHERE e->>'designation' = 'under' LIMIT 1) AS under_amer
    FROM pinnacle_matchups m
    JOIN pinnacle_leagues l       ON l.id = m.league_id
    JOIN pinnacle_market_latest ml ON ml.matchup_id = m.id
    JOIN pinnacle_market_history h ON h.id = ml.history_id
    WHERE h.market_type = 'total'
      AND h.period = 0
      AND h.is_alternate = false
      AND h.status = 'open'
      AND m.matchup_type = 'matchup'
      AND ml.last_polled_at > now() - interval '5 minutes'
      AND length(m.home_name) >= 4
      AND length(m.away_name) >= 4
),
pinnacle_totals_fair AS (
    SELECT pt.*,
           pinnacle_american_to_implied_prob(pt.over_amer)  AS over_raw,
           pinnacle_american_to_implied_prob(pt.under_amer) AS under_raw
      FROM pinnacle_totals_raw pt
     WHERE pt.line IS NOT NULL
       AND pt.over_amer IS NOT NULL
       AND pt.under_amer IS NOT NULL
),
pinnacle_totals_novig AS (
    SELECT ptf.*,
           CASE WHEN (over_raw + under_raw) > 0
                THEN over_raw  / (over_raw + under_raw) END AS over_fair_prob,
           CASE WHEN (over_raw + under_raw) > 0
                THEN under_raw / (over_raw + under_raw) END AS under_fair_prob,
           (over_raw + under_raw) - 1.0 AS pinnacle_overround
      FROM pinnacle_totals_fair ptf
),
pm_totals_parsed AS (
    -- Extract (home, away, line) from PM question patterns:
    --   "Hannover 96 vs. 1. FC Nürnberg: O/U 5.5"
    --   "Tampa Bay Rays vs. Detroit Tigers: O/U 8.5"
    -- We accept ' vs ' or ' vs. ' and any pre-O/U qualifier so
    -- tennis-set / first-half lines also match.
    SELECT
        pm.condition_id        AS polymarket_condition_id,
        pm.polymarket_market_id,
        pm.polymarket_event_id,
        pm.question,
        (regexp_match(pm.question, '^(.+?)\s+vs\.?\s+(.+?):.*O/U\s+(\d+(?:\.\d+)?)$', 'i'))[1] AS pm_home,
        (regexp_match(pm.question, '^(.+?)\s+vs\.?\s+(.+?):.*O/U\s+(\d+(?:\.\d+)?)$', 'i'))[2] AS pm_away,
        (regexp_match(pm.question, '^(.+?)\s+vs\.?\s+(.+?):.*O/U\s+(\d+(?:\.\d+)?)$', 'i'))[3]::numeric AS pm_line
      FROM polymarket_markets pm
     WHERE pm.active = true
       AND pm.closed = false
       AND pm.question ~* ':\s*(match\s+|set\s+\d+\s+games\s+|game\s+\d+\s+)?O/U\s+\d+(\.\d+)?$'
),
pm_yes_priced AS (
    SELECT p.*,
           pt.token_id, pt.outcome_name,
           pp.bid_price, pp.ask_price, pp.mid_price,
           pp.last_trade_price, pp.spread, pp.liquidity,
           pp.received_at AS pm_received_at
      FROM pm_totals_parsed p
      JOIN polymarket_tokens pt
        ON pt.condition_id = p.polymarket_condition_id
       AND pt.side = 'yes'
      LEFT JOIN polymarket_latest_token_prices pp
        ON pp.token_id = pt.token_id
)
SELECT
    pn.matchup_id, pn.league_id, pn.pinnacle_sport_id, pn.league_name,
    pn.home_name, pn.away_name, pn.start_time, pn.last_is_live,
    pn.pinnacle_market_status, pn.pinnacle_market_version, pn.pinnacle_received_at,
    pn.line AS pinnacle_line, pn.over_amer, pn.under_amer,
    pn.over_fair_prob, pn.under_fair_prob, pn.pinnacle_overround,
    yp.polymarket_condition_id,
    yp.polymarket_market_id, yp.polymarket_event_id,
    yp.question AS polymarket_question, yp.pm_line,
    yp.token_id, yp.outcome_name,
    yp.bid_price, yp.ask_price, yp.mid_price, yp.last_trade_price,
    yp.spread, yp.liquidity, yp.pm_received_at,
    -- PM YES = "Over X.5" by convention on this question pattern.
    'over'::text AS outcome_side,
    pn.over_fair_prob AS pinnacle_fair_prob,
    (pn.over_fair_prob  - yp.ask_price) AS gross_edge_to_buy,
    (yp.bid_price - pn.over_fair_prob)  AS gross_edge_to_sell,
    EXTRACT(EPOCH FROM (now() - pn.pinnacle_received_at))                                AS pinnacle_age_sec,
    EXTRACT(EPOCH FROM (now() - COALESCE(yp.pm_received_at, pn.pinnacle_received_at)))    AS pm_age_sec
  FROM pinnacle_totals_novig pn
  JOIN pm_yes_priced yp
    ON pn.line = yp.pm_line
   AND length(pn.home_name) >= 4
   AND length(pn.away_name) >= 4
   AND length(normalize_team_name(pn.home_name)) >= 4
   AND length(normalize_team_name(pn.away_name)) >= 4
   AND all_words_present(normalize_team_name(pn.home_name),
                         normalize_team_name(COALESCE(yp.pm_home, '')))
   AND all_words_present(normalize_team_name(pn.away_name),
                         normalize_team_name(COALESCE(yp.pm_away, '')));
