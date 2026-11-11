-- priority_polymarket_tokens
--
-- Single source of truth for "tokens we actually need PM prices on".
-- polymarket-sync's per-tick price-fetch budget is bounded (POLYMARKET_
-- MAX_PRICE_TOKENS, currently 300) and there are ~31k tokens in the
-- universe, so ListPolymarketPriceTokens must prioritise the tokens
-- that any edge view will actually read.
--
-- Two sources:
--   (a) 2-way path — any token whose market is referenced by a
--       confirmed pinnacle_event_alias. This is what the existing
--       sync prioritization handled.
--   (b) 3-way soccer path — the Yes side of binary markets
--       ("Will X win?" / "...end in a draw?") inside PM events whose
--       title contains both team names of a live or imminent
--       (within 24h) Pinnacle soccer matchup. Aliases are not yet
--       written for the 3-way path, so we have to recompute the
--       match by title.
--
-- Returned columns are minimal on purpose (token_id + source tag); the
-- caller LEFT JOINs by token_id to bias ORDER BY.

CREATE OR REPLACE VIEW priority_polymarket_tokens AS
SELECT pt.token_id, 'alias_2way' AS source
  FROM polymarket_tokens pt
  JOIN pinnacle_event_aliases pea
    ON pea.polymarket_condition_id = pt.condition_id
   AND pea.match_status = 'confirmed'
UNION
SELECT DISTINCT pt.token_id, 'soccer_3way' AS source
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
    ON LOWER(pe.title) ~ ('\y' || LOWER(m.home_name) || '\y')
   AND LOWER(pe.title) ~ ('\y' || LOWER(m.away_name) || '\y')
   AND m.matchup_type = 'matchup'
   AND length(m.home_name) >= 4
   AND length(m.away_name) >= 4
   AND (m.last_is_live = true
        OR (m.start_time IS NOT NULL
            AND m.start_time BETWEEN now() AND now() + interval '24 hours'))
  JOIN pinnacle_leagues l
    ON l.id = m.league_id
   AND l.sport_id = 29
 WHERE pt.side = 'yes';
