-- Bridge layer between Pinnacle matchups (sharp anchor) and Polymarket
-- markets (execution venue). Lives alongside polymarket_event_aliases (which
-- was built for the legacy Odds-API sportsbook flow); pinnacle_event_aliases
-- is the live-pipeline equivalent that maps Pinnacle matchup_id directly to
-- a Polymarket condition_id without going through canonical_events. This
-- shorter path is acceptable because the live scope is one-source (Pinnacle)
-- → one-target (Polymarket); no multi-book canonicalization needed.

-- Helper: American odds → raw implied probability. Marked IMMUTABLE so the
-- planner can inline / index. Returns 0 for input 0 (treat as missing).
CREATE OR REPLACE FUNCTION pinnacle_american_to_implied_prob(american integer)
RETURNS numeric
LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    IF american IS NULL OR american = 0 THEN
        RETURN 0;
    ELSIF american > 0 THEN
        RETURN 100.0 / (american + 100)::numeric;
    ELSE
        RETURN (-american)::numeric / ((-american) + 100)::numeric;
    END IF;
END;
$$;

-- Manual alias table. Operator workflow:
--   1. Auto-matcher writes match_status='candidate' rows with similarity score
--      in match_confidence (0-100). See pinnacle_polymarket_match_candidates view.
--   2. Operator inspects and runs:
--        UPDATE pinnacle_event_aliases SET match_status='confirmed'
--        WHERE pinnacle_matchup_id=... AND polymarket_condition_id=...;
--   3. Only 'confirmed' rows feed the edge view.
CREATE TABLE IF NOT EXISTS pinnacle_event_aliases (
    id bigserial PRIMARY KEY,
    pinnacle_matchup_id bigint NOT NULL REFERENCES pinnacle_matchups(id) ON DELETE CASCADE,
    polymarket_condition_id text NOT NULL REFERENCES polymarket_markets(condition_id) ON DELETE CASCADE,
    polymarket_event_id text REFERENCES polymarket_events(polymarket_event_id) ON DELETE SET NULL,
    polymarket_market_id text REFERENCES polymarket_markets(polymarket_market_id) ON DELETE SET NULL,
    match_status text NOT NULL DEFAULT 'candidate'
        CHECK (match_status IN ('candidate', 'confirmed', 'rejected')),
    match_confidence numeric(5,2),
    notes text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (pinnacle_matchup_id, polymarket_condition_id)
);

CREATE INDEX IF NOT EXISTS pinnacle_event_aliases_matchup_idx
    ON pinnacle_event_aliases (pinnacle_matchup_id);
CREATE INDEX IF NOT EXISTS pinnacle_event_aliases_condition_idx
    ON pinnacle_event_aliases (polymarket_condition_id);
CREATE INDEX IF NOT EXISTS pinnacle_event_aliases_status_idx
    ON pinnacle_event_aliases (match_status)
    WHERE match_status = 'confirmed';

-- Convenience view: only confirmed aliases.
CREATE OR REPLACE VIEW confirmed_pinnacle_polymarket_aliases AS
SELECT *
FROM pinnacle_event_aliases
WHERE match_status = 'confirmed';

-- Latest Polymarket bid/ask/mid per token.
CREATE OR REPLACE VIEW polymarket_latest_token_prices AS
SELECT DISTINCT ON (token_id)
    token_id,
    condition_id,
    polymarket_market_id,
    bid_price,
    ask_price,
    mid_price,
    last_trade_price,
    spread,
    liquidity,
    source_timestamp,
    received_at
FROM polymarket_price_snapshots
ORDER BY token_id, received_at DESC;

-- The headline edge view. Joins Pinnacle no-vig fair on each side (home/away)
-- to Polymarket's best bid/ask per outcome, computes gross-edge-to-buy at
-- both PM bid (sell) and ask (buy). NO net-edge subtraction yet (fee, slip,
-- model uncertainty) — that's the Step 4 signal engine's job. This view is
-- read-only and contains only data, no signal classification.
--
-- Filters:
--   - only Pinnacle market_type='moneyline', period=0, is_alternate=false, status='open'
--   - only sport_id=12 (E Sports) and league name like CS2/CS:GO/counter-strike
--   - only confirmed aliases
--   - outcome alignment via case-insensitive substring match between PM
--     token outcome_name and Pinnacle home_name / away_name
CREATE OR REPLACE VIEW pinnacle_polymarket_cs2_moneyline_edges AS
WITH pinnacle_fair AS (
    SELECT
        m.id AS matchup_id,
        l.id AS league_id,
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
      AND l.sport_id = 12
      AND (LOWER(l.name) LIKE '%cs2%' OR LOWER(l.name) LIKE '%cs:go%' OR LOWER(l.name) LIKE '%counter%')
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
             THEN pn.home_raw_prob / (pn.home_raw_prob + pn.away_raw_prob)
             ELSE NULL
        END AS home_fair_prob,
        CASE WHEN (pn.home_raw_prob + pn.away_raw_prob) > 0
             THEN pn.away_raw_prob / (pn.home_raw_prob + pn.away_raw_prob)
             ELSE NULL
        END AS away_fair_prob,
        (pn.home_raw_prob + pn.away_raw_prob) - 1.0 AS pinnacle_overround
    FROM pinnacle_novig pn
),
joined AS (
    SELECT
        pn.matchup_id,
        pn.league_id,
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
        WHEN 'away' THEN j.away_fair_prob
        ELSE NULL
    END AS pinnacle_fair_prob,
    -- gross edge if we BUY this outcome on PM at the best ask
    CASE j.outcome_side
        WHEN 'home' THEN j.home_fair_prob - j.ask_price
        WHEN 'away' THEN j.away_fair_prob - j.ask_price
        ELSE NULL
    END AS gross_edge_to_buy,
    -- gross edge if we SELL this outcome on PM at the best bid (i.e. we
    -- already hold it). Positive value here means PM bid > fair, so selling
    -- now is profitable in expectation.
    CASE j.outcome_side
        WHEN 'home' THEN j.bid_price - j.home_fair_prob
        WHEN 'away' THEN j.bid_price - j.away_fair_prob
        ELSE NULL
    END AS gross_edge_to_sell,
    EXTRACT(EPOCH FROM (now() - j.pinnacle_received_at)) AS pinnacle_age_sec,
    EXTRACT(EPOCH FROM (now() - COALESCE(j.pm_received_at, j.pinnacle_received_at))) AS pm_age_sec
FROM joined j
WHERE j.outcome_side <> 'unknown';

-- Candidate-match suggestions for the operator review workflow. Reads
-- Pinnacle CS2 matchups against the Polymarket esports event titles and
-- returns rows whose title contains both team names. The matcher CLI
-- (`cmd/match-pinnacle-polymarket`, future) will write the high-confidence
-- ones into pinnacle_event_aliases.
CREATE OR REPLACE VIEW pinnacle_polymarket_match_candidates AS
SELECT
    m.id AS pinnacle_matchup_id,
    l.id AS pinnacle_league_id,
    l.name AS pinnacle_league_name,
    m.home_name,
    m.away_name,
    m.start_time AS pinnacle_start_time,
    pe.polymarket_event_id,
    pe.slug AS polymarket_event_slug,
    pe.title AS polymarket_event_title,
    pe.start_time AS polymarket_start_time,
    pm.polymarket_market_id,
    pm.condition_id AS polymarket_condition_id,
    pm.question AS polymarket_question,
    CASE
        WHEN LOWER(pe.title) LIKE '%' || LOWER(m.home_name) || '%'
         AND LOWER(pe.title) LIKE '%' || LOWER(m.away_name) || '%'
        THEN 85.0  -- both team names in title
        WHEN LOWER(pe.title) LIKE '%' || LOWER(m.home_name) || '%'
          OR LOWER(pe.title) LIKE '%' || LOWER(m.away_name) || '%'
        THEN 40.0  -- one team name in title (weak)
        ELSE 0.0
    END AS suggested_confidence
FROM pinnacle_matchups m
JOIN pinnacle_leagues l ON l.id = m.league_id
JOIN polymarket_events pe
  ON LOWER(pe.title) LIKE '%counter-strike%'
JOIN polymarket_markets pm
  ON pm.polymarket_event_id = pe.polymarket_event_id
WHERE l.sport_id = 12
  AND (LOWER(l.name) LIKE '%cs2%' OR LOWER(l.name) LIKE '%cs:go%' OR LOWER(l.name) LIKE '%counter%')
  AND m.matchup_type = 'matchup'
  AND m.home_name <> ''
  AND m.away_name <> ''
  AND (
       (LOWER(pe.title) LIKE '%' || LOWER(m.home_name) || '%')
    OR (LOWER(pe.title) LIKE '%' || LOWER(m.away_name) || '%')
  );
