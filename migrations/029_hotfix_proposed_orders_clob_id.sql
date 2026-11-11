-- Phase 4 prep: proposed_orders needs a first-class column for the
-- Polymarket CLOB order id returned by POST /order so the fill monitor
-- can poll GET /data/order/{id} without parsing JSON out of notes.
-- Also adds last_status_check_at so the poller can prioritize rows
-- that haven't been re-checked recently.
--
-- Strictly additive: ALTER ADD COLUMN IF NOT EXISTS, no data rewrite.

ALTER TABLE proposed_orders
    ADD COLUMN IF NOT EXISTS clob_order_id text NOT NULL DEFAULT '';

ALTER TABLE proposed_orders
    ADD COLUMN IF NOT EXISTS last_status_check_at timestamptz;

-- Partial index speeds up "what's still in flight?" — fill monitor
-- query pattern. Most rows transition through 'sent' quickly so the
-- index stays small.
CREATE INDEX IF NOT EXISTS proposed_orders_inflight_idx
    ON proposed_orders (last_status_check_at NULLS FIRST, created_at)
 WHERE status = 'sent';
