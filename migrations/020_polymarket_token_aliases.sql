-- Phase 12H: manual Polymarket token outcome aliases.
-- Phase 12G left the system with a confirmed event/market alias for the NBA
-- Cleveland Cavaliers vs Detroit Pistons canonical event but zero gross edge
-- rows because the existing edges view joined the sportsbook moneyline
-- outcome name (e.g. "Cleveland Cavaliers") to the Polymarket token outcome
-- name (e.g. "Cavaliers") via a normalized string match. Phase 12H adds an
-- explicit, manually-reviewed token-to-outcome mapping table and updates the
-- gross edges view to prefer confirmed token aliases when present.
--
-- Read-only / manual-review only. No auto-mapping. No auto-approval.
-- No trading, order/account stream, wallet/private-key handling, order
-- placement, or order cancellation.

CREATE TABLE IF NOT EXISTS polymarket_token_aliases (
    id bigserial PRIMARY KEY,
    token_id text NOT NULL REFERENCES polymarket_tokens(token_id) ON DELETE CASCADE,
    condition_id text REFERENCES polymarket_markets(condition_id) ON DELETE SET NULL,
    polymarket_market_id text REFERENCES polymarket_markets(polymarket_market_id) ON DELETE SET NULL,
    canonical_event_id uuid NOT NULL REFERENCES canonical_events(id) ON DELETE CASCADE,
    canonical_outcome_name text NOT NULL,
    canonical_outcome_side text NOT NULL,
    status text NOT NULL,
    confidence_score numeric(5,2),
    reviewer text NOT NULL,
    reason text NOT NULL,
    decision_source text NOT NULL DEFAULT 'manual',
    decided_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (status IN ('confirmed', 'rejected', 'unresolved', 'ambiguous')),
    CHECK (
        -- A confirmed alias must carry an outcome name AND side AND a non-empty
        -- reviewer AND a non-empty reason. Rejected/unresolved/ambiguous rows
        -- only need the reviewer + reason because they are still operator
        -- decisions, just not approval decisions.
        (status <> 'confirmed')
        OR (
            length(trim(canonical_outcome_name)) > 0
            AND length(trim(canonical_outcome_side)) > 0
            AND length(trim(reviewer)) > 0
            AND length(trim(reason)) > 0
        )
    )
);

CREATE INDEX IF NOT EXISTS polymarket_token_aliases_token_idx
    ON polymarket_token_aliases (token_id);

CREATE INDEX IF NOT EXISTS polymarket_token_aliases_canonical_event_idx
    ON polymarket_token_aliases (canonical_event_id);

CREATE INDEX IF NOT EXISTS polymarket_token_aliases_status_idx
    ON polymarket_token_aliases (status);

-- One confirmed alias per token at a time. A second confirm on the same token
-- (whether to the same canonical event or a different one) must be rejected
-- by the application before insert; this partial unique index is the second
-- line of defense.
CREATE UNIQUE INDEX IF NOT EXISTS polymarket_token_aliases_one_confirmed_per_token
    ON polymarket_token_aliases (token_id)
    WHERE status = 'confirmed';

-- One confirmed alias per (canonical_event_id, canonical_outcome_side) — a
-- canonical event has at most one home, one away, one draw, one over, etc.
-- and confirming two tokens to the same side is a mistake.
CREATE UNIQUE INDEX IF NOT EXISTS polymarket_token_aliases_one_confirmed_per_side
    ON polymarket_token_aliases (canonical_event_id, canonical_outcome_side)
    WHERE status = 'confirmed';

-- Convenience read-only view of confirmed token aliases joined with the
-- canonical event they map to. Operator-friendly column order.
CREATE OR REPLACE VIEW confirmed_polymarket_token_aliases AS
SELECT
    a.id AS alias_id,
    a.token_id,
    a.condition_id,
    a.polymarket_market_id,
    a.canonical_event_id,
    ce.sport_key,
    ce.home_team,
    ce.away_team,
    ce.commence_time,
    a.canonical_outcome_name,
    a.canonical_outcome_side,
    a.confidence_score,
    a.reviewer,
    a.reason,
    a.decision_source,
    a.decided_at,
    a.updated_at,
    pt.outcome_name AS polymarket_outcome_name,
    pt.side AS polymarket_yes_no_side
FROM polymarket_token_aliases a
JOIN canonical_events ce ON ce.id = a.canonical_event_id
LEFT JOIN polymarket_tokens pt ON pt.token_id = a.token_id
WHERE lower(a.status) = 'confirmed';

-- Updated edges view. CREATE OR REPLACE is safe because the column list,
-- order, and types are unchanged from migration 008. The change is in the
-- join logic: when ANY confirmed token alias exists for the
-- (canonical_event_id, condition_id) pair, the operator's manual mapping is
-- the authoritative match; the legacy outcome-name string match is bypassed
-- for that pair entirely. When no confirmed token alias exists for the pair,
-- the legacy behavior (normalized outcome-name string match) is preserved
-- for backward compatibility with events that already produce edge rows.
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
-- has_confirmed_token_alias_for_pair flags the (canonical_event_id, condition_id)
-- pairs that have at least one confirmed token alias. For those pairs we will
-- ONLY honor the alias-based match.
pair_has_confirmed_token_alias AS (
    SELECT DISTINCT
        ta.canonical_event_id,
        pt.condition_id
    FROM polymarket_token_aliases ta
    JOIN polymarket_tokens pt ON pt.token_id = ta.token_id
    WHERE lower(ta.status) = 'confirmed'
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
        trim(regexp_replace(lower(coalesce(pt.outcome_name, '')), '[^a-z0-9]+', ' ', 'g')) AS polymarket_outcome_norm,
        EXISTS (
            SELECT 1 FROM polymarket_token_aliases ta
            WHERE ta.token_id = pt.token_id
              AND ta.canonical_event_id = fv.canonical_event_id
              AND lower(ta.status) = 'confirmed'
              AND lower(coalesce(ta.canonical_outcome_side, '')) = lower(coalesce(fv.outcome_side, ''))
        ) AS matched_via_token_alias,
        EXISTS (
            SELECT 1 FROM pair_has_confirmed_token_alias p
            WHERE p.canonical_event_id = fv.canonical_event_id
              AND p.condition_id = pt.condition_id
        ) AS pair_has_confirmed_alias
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
  AND (
      -- Operator-confirmed token alias is the authoritative match for the
      -- pair. The legacy string-match fallback is bypassed for any pair that
      -- has any confirmed token alias.
      matched_via_token_alias
      OR (
          NOT pair_has_confirmed_alias
          AND sportsbook_outcome_norm <> ''
          AND sportsbook_outcome_norm = polymarket_outcome_norm
      )
  );
