-- The _live view trusted ask_price simply because it was non-null. But
-- PM CLOB tokens go stale at very different rates per-token (some are
-- updated every tick, others get 1 quote per day). A 21-min-old ask
-- against a fresh Pinnacle line generates fake 20pp+ "edges" that the
-- operator MUST not act on.
--
-- Add a freshness ceiling — default 120s (PM mid-cycle quote, anything
-- older means the market is sleeping). The dashboard's "top live edges"
-- section now only contains rows where BOTH sides of the comparison
-- are fresh.

DROP VIEW IF EXISTS pinnacle_polymarket_moneyline_edges_live;
CREATE VIEW pinnacle_polymarket_moneyline_edges_live AS
SELECT * FROM pinnacle_polymarket_moneyline_edges
 WHERE last_is_live = true
   AND ask_price IS NOT NULL
   AND ask_price > 0.005
   AND pm_age_sec < 120
   AND pinnacle_age_sec < 60;
