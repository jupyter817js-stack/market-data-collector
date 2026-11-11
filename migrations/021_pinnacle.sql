-- Pinnacle guest-API live odds storage. Pinnacle is the same line engine as
-- PS3838, so this serves as the sharp anchor for live in-play comparison
-- against Polymarket. Schema is intentionally namespaced separately from
-- sportsbook_* (which is The Odds API shaped) because Pinnacle live carries
-- per-period structure (Full / Map 1 / Map 2 / ...), an American odds format
-- requiring conversion, and a per-market `version` number that we use for
-- change-only history persistence.

CREATE TABLE IF NOT EXISTS pinnacle_sports (
    id int PRIMARY KEY,
    name text NOT NULL,
    primary_market_type text NOT NULL DEFAULT '',
    last_matchup_count int NOT NULL DEFAULT 0,
    last_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pinnacle_leagues (
    id int PRIMARY KEY,
    sport_id int NOT NULL,
    name text NOT NULL,
    league_group text NOT NULL DEFAULT '',
    last_matchup_count int NOT NULL DEFAULT 0,
    is_featured boolean NOT NULL DEFAULT false,
    discovered_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pinnacle_leagues_sport_idx ON pinnacle_leagues (sport_id);
CREATE INDEX IF NOT EXISTS pinnacle_leagues_active_idx ON pinnacle_leagues (last_matchup_count) WHERE last_matchup_count > 0;

CREATE TABLE IF NOT EXISTS pinnacle_matchups (
    id bigint PRIMARY KEY,
    league_id int NOT NULL,
    matchup_type text NOT NULL DEFAULT '',
    start_time timestamptz,
    home_name text NOT NULL DEFAULT '',
    away_name text NOT NULL DEFAULT '',
    home_id bigint,
    away_id bigint,
    last_status text NOT NULL DEFAULT '',
    last_live_status int NOT NULL DEFAULT 0,
    last_is_live boolean NOT NULL DEFAULT false,
    last_has_markets boolean NOT NULL DEFAULT false,
    last_has_live boolean NOT NULL DEFAULT false,
    last_period int NOT NULL DEFAULT 0,
    raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    discovered_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pinnacle_matchups_league_idx ON pinnacle_matchups (league_id);
CREATE INDEX IF NOT EXISTS pinnacle_matchups_live_idx ON pinnacle_matchups (last_is_live) WHERE last_is_live;
CREATE INDEX IF NOT EXISTS pinnacle_matchups_start_idx ON pinnacle_matchups (start_time);

-- Append-only change history. Unique on (matchup_id, market_key, version) so
-- the collector can blindly INSERT ON CONFLICT DO NOTHING and let the row
-- count tell us whether the version actually changed.
CREATE TABLE IF NOT EXISTS pinnacle_market_history (
    id bigserial PRIMARY KEY,
    matchup_id bigint NOT NULL REFERENCES pinnacle_matchups(id) ON DELETE CASCADE,
    period int NOT NULL,
    market_type text NOT NULL,
    market_key text NOT NULL,
    side text NOT NULL DEFAULT '',
    is_alternate boolean NOT NULL DEFAULT false,
    status text NOT NULL,
    version bigint NOT NULL,
    prices jsonb NOT NULL,
    limits jsonb NOT NULL DEFAULT '[]'::jsonb,
    raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    received_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (matchup_id, market_key, version)
);

CREATE INDEX IF NOT EXISTS pinnacle_market_history_matchup_idx
    ON pinnacle_market_history (matchup_id, market_key, received_at DESC);
CREATE INDEX IF NOT EXISTS pinnacle_market_history_received_idx
    ON pinnacle_market_history (received_at DESC);

CREATE TABLE IF NOT EXISTS pinnacle_market_latest (
    matchup_id bigint NOT NULL REFERENCES pinnacle_matchups(id) ON DELETE CASCADE,
    market_key text NOT NULL,
    history_id bigint NOT NULL REFERENCES pinnacle_market_history(id) ON DELETE CASCADE,
    version bigint NOT NULL,
    status text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (matchup_id, market_key)
);

CREATE TABLE IF NOT EXISTS pinnacle_collector_runs (
    id bigserial PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    sport_id int NOT NULL,
    leagues_seen int NOT NULL DEFAULT 0,
    matchups_seen int NOT NULL DEFAULT 0,
    markets_seen int NOT NULL DEFAULT 0,
    markets_inserted int NOT NULL DEFAULT 0,
    markets_unchanged int NOT NULL DEFAULT 0,
    http_errors int NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'running',
    error_message text NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS pinnacle_collector_runs_started_idx ON pinnacle_collector_runs (started_at DESC);

-- Convenience view: latest Pinnacle moneyline (Period 0 = full match) per
-- matchup, filtered to esports CS2 leagues. Read-only, never written to.
CREATE OR REPLACE VIEW pinnacle_cs2_moneyline_latest AS
SELECT
    m.id              AS matchup_id,
    l.id              AS league_id,
    l.name            AS league_name,
    l.league_group    AS league_group,
    m.home_name,
    m.away_name,
    m.home_id,
    m.away_id,
    m.start_time,
    m.last_is_live,
    m.last_status,
    h.status          AS market_status,
    h.version         AS market_version,
    h.prices          AS prices,
    h.limits          AS limits,
    h.received_at     AS odds_received_at
FROM pinnacle_matchups m
JOIN pinnacle_leagues  l  ON l.id = m.league_id
JOIN pinnacle_market_latest ml ON ml.matchup_id = m.id
JOIN pinnacle_market_history h ON h.id = ml.history_id
WHERE h.market_type = 'moneyline'
  AND h.period = 0
  AND h.is_alternate = false
  AND l.sport_id = 12
  AND (LOWER(l.name) LIKE '%cs2%' OR LOWER(l.name) LIKE '%cs:go%' OR LOWER(l.name) LIKE '%counter%')
  AND m.matchup_type = 'matchup';
