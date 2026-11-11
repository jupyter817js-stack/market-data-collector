-- A2 (alternative): match phase + recent-event proxies inferred from
-- the Pinnacle data we already store. The matchup endpoint does NOT
-- return score/clock fields (verified live: raw_payload has only
-- isLive / liveStatus / period; no scores object), and external API
-- exploration to find a separate score endpoint is unavailable in the
-- current operating environment. This migration extracts the
-- next-best-thing from existing tables:
--
--   1. match_phase: which Pinnacle period markets are open / closed /
--      suspended. For soccer this lets us name pre / first_half /
--      halftime / second_half / ended; for other sports we get
--      pre / live / ended without sub-phases.
--
--   2. event_proxy: count of status='suspended' rows in
--      pinnacle_market_history over the last 5 min on the period=0
--      moneyline. Pinnacle suspends odds the moment a goal /
--      red card / penalty / major play is observed, so a non-zero
--      count is a "something just happened" signal even without
--      knowing what.
--
--   3. version_churn_5m: how many version updates landed on the
--      period=0 moneyline in the last 5 min. Larger = more
--      volatility (Pinnacle reacting to ongoing play). Zero = the
--      market has gone quiet, often a sign the game ended but
--      last_is_live didn't flip yet.
--
--   4. last_data_age_sec: time since the latest moneyline
--      pinnacle_market_history row arrived. The companion to
--      last_polled_at (migration 039) but observed at the history
--      layer, so it captures "no version changes" even if polling
--      continues.
--
-- Together these give signal-engine + the dashboard a working
-- substitute for score/clock: the phase tells you where the game is
-- and the event_proxy tells you whether something important
-- happened recently. A score mismatch with Polymarket can still be
-- caught — if Pinnacle's phase is 'ended' but PM's question is
-- pre-match style, that's MAPPING_INVALID territory.
--
-- This view stays read-only here; signal-engine integration is a
-- follow-up commit.

CREATE OR REPLACE VIEW pinnacle_match_state AS
WITH ml_latest AS (
    -- Latest moneyline (main, not alternate) market state per
    -- (matchup, period). pinnacle_market_latest gives the pointer.
    SELECT
        m.id AS matchup_id,
        l.sport_id,
        l.name AS league_name,
        m.home_name, m.away_name,
        m.start_time,
        m.last_is_live,
        m.matchup_type,
        h.period,
        h.status,
        h.version,
        h.received_at AS history_received_at,
        ml.last_polled_at
      FROM pinnacle_matchups m
      JOIN pinnacle_leagues l       ON l.id = m.league_id
      JOIN pinnacle_market_latest ml ON ml.matchup_id = m.id
      JOIN pinnacle_market_history h ON h.id = ml.history_id
     WHERE h.market_type = 'moneyline'
       AND h.is_alternate = false
       AND m.matchup_type = 'matchup'
),
per_matchup AS (
    SELECT
        matchup_id, sport_id, league_name, home_name, away_name,
        start_time, last_is_live,
        array_agg(period ORDER BY period)
            FILTER (WHERE status = 'open')      AS open_periods,
        array_agg(period ORDER BY period)
            FILTER (WHERE status = 'closed')    AS closed_periods,
        array_agg(period ORDER BY period)
            FILTER (WHERE status = 'suspended') AS suspended_periods,
        max(history_received_at)                AS last_history_at,
        max(last_polled_at)                     AS last_polled_at
      FROM ml_latest
     GROUP BY matchup_id, sport_id, league_name, home_name, away_name,
              start_time, last_is_live
),
activity AS (
    -- Past-5-minute churn computed directly from history (not
    -- latest), so we catch repeated version flips even if the
    -- pointer didn't move.
    SELECT
        h.matchup_id,
        count(*) FILTER (WHERE h.status = 'suspended')                AS suspensions_5m,
        count(*)                                                      AS versions_5m,
        max(h.received_at)                                            AS last_change_at
      FROM pinnacle_market_history h
     WHERE h.market_type = 'moneyline'
       AND h.period = 0
       AND h.is_alternate = false
       AND h.received_at > now() - interval '5 minutes'
     GROUP BY h.matchup_id
)
SELECT
    p.matchup_id, p.sport_id, p.league_name,
    p.home_name, p.away_name, p.start_time, p.last_is_live,
    p.open_periods, p.closed_periods, p.suspended_periods,
    COALESCE(a.suspensions_5m, 0)                       AS suspensions_5m,
    COALESCE(a.versions_5m,    0)                       AS versions_5m,
    a.last_change_at,
    p.last_polled_at,
    EXTRACT(EPOCH FROM (now() - p.last_polled_at))::int  AS last_polled_age_sec,
    EXTRACT(EPOCH FROM (now() - COALESCE(a.last_change_at, p.last_polled_at)))::int AS last_change_age_sec,
    EXTRACT(EPOCH FROM (now() - p.start_time)) / 60      AS minutes_since_kickoff,
    -- Phase classification. Soccer (29) gets sub-phases; others get
    -- the coarse pre/live/ended trio.
    CASE
        WHEN p.start_time IS NULL                                                 THEN 'unknown'
        WHEN p.start_time > now()                                                  THEN 'pre'
        WHEN p.sport_id = 29 THEN
            CASE
                WHEN p.open_periods @> ARRAY[0]::int[]
                     AND p.open_periods @> ARRAY[1]::int[]
                                                                                  THEN 'first_half'
                WHEN p.open_periods @> ARRAY[0]::int[]
                     AND NOT (p.open_periods @> ARRAY[1]::int[])
                     AND EXTRACT(EPOCH FROM (now() - p.start_time))/60 < 60       THEN 'halftime'
                WHEN p.open_periods @> ARRAY[0]::int[]
                     AND NOT (p.open_periods @> ARRAY[1]::int[])                  THEN 'second_half'
                WHEN p.closed_periods @> ARRAY[0]::int[]                          THEN 'ended'
                WHEN p.suspended_periods @> ARRAY[0]::int[]                       THEN 'suspended'
                ELSE 'unknown'
            END
        ELSE
            CASE
                WHEN p.open_periods @> ARRAY[0]::int[]      THEN 'live'
                WHEN p.closed_periods @> ARRAY[0]::int[]    THEN 'ended'
                WHEN p.suspended_periods @> ARRAY[0]::int[] THEN 'suspended'
                ELSE 'unknown'
            END
    END AS match_phase,
    -- "Did something just happen?" — non-zero suspensions in the
    -- last 5 min strongly implies a goal / card / VAR review etc.
    CASE
        WHEN COALESCE(a.suspensions_5m, 0) > 0 THEN 'recent_event'
        WHEN COALESCE(a.versions_5m, 0)   = 0 THEN 'dormant'
        ELSE                                       'stable'
    END AS event_state
FROM per_matchup p
LEFT JOIN activity a ON a.matchup_id = p.matchup_id;
