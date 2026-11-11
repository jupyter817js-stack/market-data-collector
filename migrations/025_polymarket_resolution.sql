-- Polymarket resolution extraction. Gamma payloads of closed markets already
-- carry the resolved winner inside polymarket_markets.gamma_payload, but the
-- fields are encoded as escaped JSON strings inside the JSONB (e.g.
-- `"outcomePrices": "[\"0\",\"1\"]"`). This migration exposes them as
-- structured columns via a view and computes the winning token id, so paper
-- realized-PnL and the future executor can join on a single text key.
--
-- No table changes; polymarket-sync continues to write raw payloads as-is.
-- If the view's CASE logic ever proves too lossy (e.g. weird 3-way markets
-- in esports props), promote these to extracted columns in a later migration.

CREATE OR REPLACE VIEW polymarket_resolved_markets AS
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
    WHERE m.closed = true
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
    -- jsonb arrays are 0-indexed via ->>N. We unroll outcomePrices with
    -- ordinality so we handle any N-way market correctly.
    (
        SELECT p.clob_token_ids->>(e.ord - 1)::int
          FROM jsonb_array_elements_text(p.outcome_prices) WITH ORDINALITY AS e(price, ord)
         WHERE e.price = '1'
         LIMIT 1
    ) AS winning_token_id,
    (
        SELECT p.outcomes->>(e.ord - 1)::int
          FROM jsonb_array_elements_text(p.outcome_prices) WITH ORDINALITY AS e(price, ord)
         WHERE e.price = '1'
         LIMIT 1
    ) AS winning_outcome_name
FROM parsed p;

-- Extension to paper_pnl_runs for the realized layer. trades_won is
-- nullable until we have realized data for that run (so we can distinguish
-- "0 wins" from "no resolved trades yet").
ALTER TABLE paper_pnl_runs
    ADD COLUMN IF NOT EXISTS trades_resolved int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS trades_won int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS total_realized_pnl_usdc numeric(14,4) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hit_rate numeric(5,4);
