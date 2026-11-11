-- B-priority fix: priority_polymarket_tokens currently includes every
-- token referenced by a confirmed 2-way alias plus every 3-way soccer
-- YES token whose Pinnacle counterpart is live OR upcoming within 24h.
-- Today's count: 562. POLYMARKET_MAX_PRICE_TOKENS cap is 300, so the
-- ORDER BY truncation drops 262 priority tokens — and pre-match
-- candidates often outrank live ones simply because their metadata
-- updated_at is more recent.
--
-- Observed effect (live :5433): the Pumas UNAM vs Pachuca match was
-- live with confirmed 3-way alias logic, but its three "Will X win?"
-- YES tokens never got priced. signal-engine read the edge view, saw
-- pm_ask IS NULL, and emitted MAPPING_INVALID — 20,890 rows in one
-- hour just from this one match's repeated tick attempts. ~50% of
-- the signals.state distribution.
--
-- This migration drops the existing view and re-creates it with a
-- live_priority column:
--   2 = matchup is currently live (m.last_is_live = true)
--   1 = imminent (start_time within 24h)
--   0 = neither (kept just for the 2-way alias branch — older PM
--       events where Pinnacle has already deactivated; rare)
--
-- The caller (internal/store/polymarket.go::ListPolymarketPriceTokens)
-- ORDERs by ppt.live_priority DESC first so the live tokens are
-- always inside the cap even when the priority universe grows past
-- it. Companion .env tuning: bump POLYMARKET_MAX_PRICE_TOKENS so we
-- have headroom past the live tier (handled separately).

DROP VIEW IF EXISTS priority_polymarket_tokens;

CREATE VIEW priority_polymarket_tokens AS
SELECT pt.token_id,
       'alias_2way'::text AS source,
       CASE
           WHEN bool_or(m.last_is_live) THEN 2
           WHEN bool_or(m.start_time IS NOT NULL
                        AND m.start_time BETWEEN now()
                                            AND now() + interval '24 hours')
                THEN 1
           ELSE 0
       END AS live_priority
  FROM polymarket_tokens pt
  JOIN pinnacle_event_aliases pea
    ON pea.polymarket_condition_id = pt.condition_id
   AND pea.match_status = 'confirmed'
  LEFT JOIN pinnacle_matchups m
    ON m.id = pea.pinnacle_matchup_id
 GROUP BY pt.token_id

UNION

SELECT pt.token_id,
       'soccer_3way'::text AS source,
       CASE
           WHEN bool_or(m.last_is_live) THEN 2
           WHEN bool_or(m.start_time BETWEEN now()
                                        AND now() + interval '24 hours')
                THEN 1
           ELSE 0
       END AS live_priority
  FROM polymarket_tokens pt
  JOIN polymarket_markets pm
    ON pm.condition_id = pt.condition_id
   AND pm.active = true
   AND pm.closed = false
   AND (pm.question ILIKE '%end in a draw%'
        OR pm.question ILIKE 'Will %win%')
  JOIN polymarket_events pe
    ON pe.polymarket_event_id = pm.polymarket_event_id
   AND pe.active = true
   AND pe.closed = false
  JOIN pinnacle_matchups m
    ON all_words_present(normalize_team_name(m.home_name),
                         normalize_team_name(pe.title))
   AND all_words_present(normalize_team_name(m.away_name),
                         normalize_team_name(pe.title))
   AND m.matchup_type = 'matchup'
   AND length(m.home_name) >= 4
   AND length(m.away_name) >= 4
   AND length(normalize_team_name(m.home_name)) >= 4
   AND length(normalize_team_name(m.away_name)) >= 4
   AND (m.last_is_live = true
        OR (m.start_time IS NOT NULL
            AND m.start_time BETWEEN now() AND now() + interval '24 hours'))
  JOIN pinnacle_leagues l
    ON l.id = m.league_id
   AND l.sport_id = 29
 WHERE pt.side = 'yes'
 GROUP BY pt.token_id;
