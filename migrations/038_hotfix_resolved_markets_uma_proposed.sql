-- Step 6 paper-PnL accumulation was blocked at 1 resolved trade / 7 sims:
-- only `closed=true` markets surface in polymarket_resolved_markets, but
-- Polymarket settles via UMA optimistic oracle, which routes a market
-- through a "proposed" state for ~2-3 hours after the outcome is known
-- before flipping closed=true. In that window the market still has
-- decisive outcomePrices (e.g. ["0.0005", "0.9995"] instead of ["0", "1"])
-- and a valid clobTokenIds list, so the winner IS determinable — Gamma
-- just hasn't promoted closed yet.
--
-- This hotfix widens the view to surface those proposed markets too, and
-- relaxes the winner-extraction predicate from `price = '1'` (exact) to
-- `price::numeric >= 0.99` so the proposed price 0.9995 still picks the
-- right token.
--
-- Risk: UMA disputes that reverse a proposed outcome are extremely rare
-- in sports (we've observed zero in this codebase's history). The cost
-- of paper-marking the wrong winner for one trade during the rare
-- dispute is acceptable for Step 6 hit-rate accumulation. Real-money
-- settlement should continue to gate on closed=true downstream when
-- Step 7 ships.

DROP VIEW IF EXISTS polymarket_resolved_markets;

CREATE VIEW polymarket_resolved_markets AS
WITH parsed AS (
    SELECT
        m.condition_id,
        m.polymarket_market_id,
        m.polymarket_event_id,
        m.question,
        (m.gamma_payload->>'outcomes')::jsonb       AS outcomes,
        (m.gamma_payload->>'outcomePrices')::jsonb  AS outcome_prices,
        (m.gamma_payload->>'clobTokenIds')::jsonb   AS clob_token_ids,
        m.gamma_payload->>'umaResolutionStatus'     AS uma_resolution_status,
        NULLIF(m.gamma_payload->>'closedTime','')::timestamptz AS closed_time,
        m.gamma_payload->>'resolvedBy'              AS resolved_by
    FROM polymarket_markets m
    WHERE (m.closed = true
           OR m.gamma_payload->>'umaResolutionStatus' = 'proposed')
      AND m.gamma_payload ? 'outcomePrices'
      AND m.gamma_payload ? 'clobTokenIds'
)
SELECT
    p.condition_id,
    p.polymarket_market_id,
    p.polymarket_event_id,
    p.question,
    p.outcomes,
    p.outcome_prices,
    p.clob_token_ids,
    p.uma_resolution_status,
    p.closed_time,
    p.resolved_by,
    (
        SELECT p.clob_token_ids->>(e.ord - 1)::int
          FROM jsonb_array_elements_text(p.outcome_prices) WITH ORDINALITY AS e(price, ord)
         WHERE e.price ~ '^[0-9.]+$' AND e.price::numeric >= 0.99
         LIMIT 1
    ) AS winning_token_id,
    (
        SELECT p.outcomes->>(e.ord - 1)::int
          FROM jsonb_array_elements_text(p.outcome_prices) WITH ORDINALITY AS e(price, ord)
         WHERE e.price ~ '^[0-9.]+$' AND e.price::numeric >= 0.99
         LIMIT 1
    ) AS winning_outcome_name
FROM parsed p;
