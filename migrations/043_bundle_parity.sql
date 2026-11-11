-- B2: Same-event bundle parity engine. Spec §12: in a fair binary
-- market the YES + NO prices sum to 1; in a 3-way the three "Will X
-- win?" YES asks sum to 1. When the sum is BELOW 1 (after spread) we
-- have a buy-all arbitrage — every dollar of complete set bought
-- returns $1 on settlement regardless of outcome. When it's ABOVE 1
-- + threshold, the market is overpriced relative to the certainty
-- it sells (would be a sell-all opportunity for someone with
-- inventory).
--
-- This view ships the parity arithmetic over the data we already
-- collect:
--
--   pinnacle_polymarket_bundle_parity
--     – per matchup_id + condition_id family
--     – sum of all YES-side asks across mutually-exclusive outcomes
--     – sum of all bids (for the sell direction)
--     – num_legs (2 = binary, 3 = soccer 3-way; degenerate <2 dropped)
--     – buy_all_cost / sell_all_proceeds in 1-share-per-leg units
--     – parity_state: 'arb_buy' / 'arb_sell' / 'tight' / 'wide_buy'
--                     / 'wide_sell'
--
-- Threshold defaults are baked at the spec's "tight enough to ignore"
-- band of 0.005 = 0.5 cent on $1; tunable downstream by adding a
-- filter ON parity_state. The future bundle-aware exit engine can
-- read this to decide between selling one leg and buying the missing
-- legs to form a complete set + redeem.

CREATE OR REPLACE VIEW pinnacle_polymarket_bundle_parity AS
WITH yes_legs AS (
    -- One row per (matchup, condition_family, token) where token is the
    -- YES side of a mutually-exclusive outcome. Family = the PM event
    -- for 3-way soccer (3 conditions tied to one match); = the single
    -- condition for 2-way money-line.
    SELECT
        m.id  AS matchup_id,
        l.sport_id AS pinnacle_sport_id,
        m.home_name, m.away_name, m.start_time, m.last_is_live,
        pa.polymarket_event_id      AS family_key,
        pa.polymarket_condition_id,
        pt.token_id, pt.outcome_name,
        pp.bid_price, pp.ask_price, pp.mid_price,
        pp.received_at AS pm_received_at
    FROM pinnacle_matchups m
    JOIN pinnacle_leagues l ON l.id = m.league_id
    JOIN pinnacle_event_aliases pa
      ON pa.pinnacle_matchup_id = m.id
     AND pa.match_status = 'confirmed'
    JOIN polymarket_markets pm
      ON pm.condition_id = pa.polymarket_condition_id
     AND pm.active = true
     AND pm.closed = false
    JOIN polymarket_tokens pt
      ON pt.condition_id = pm.condition_id
     AND pt.side = 'yes'
    LEFT JOIN polymarket_latest_token_prices pp
      ON pp.token_id = pt.token_id
    WHERE m.matchup_type = 'matchup'
      -- Drop settled-marker prices (ask floor = 0.001) and bid floor
      -- markets so we don't surface "0.001 × num_legs ≈ 0" as false
      -- arbitrage. Only price-active legs participate in the sum.
      AND (pp.ask_price IS NULL OR pp.ask_price > 0.005)
      AND (pp.received_at IS NULL OR pp.received_at > now() - interval '5 minutes')
),
bundled AS (
    SELECT
        matchup_id,
        pinnacle_sport_id,
        max(home_name)  AS home_name,
        max(away_name)  AS away_name,
        max(start_time) AS start_time,
        bool_or(last_is_live) AS last_is_live,
        family_key,
        count(*)                                                 AS num_legs,
        sum(ask_price) FILTER (WHERE ask_price IS NOT NULL)      AS sum_ask,
        sum(bid_price) FILTER (WHERE bid_price IS NOT NULL)      AS sum_bid,
        count(*) FILTER (WHERE ask_price IS NULL OR ask_price = 0) AS legs_missing_ask,
        max(pm_received_at)                                      AS pm_latest_received_at,
        array_agg(token_id ORDER BY outcome_name)                AS token_ids,
        array_agg(outcome_name ORDER BY outcome_name)            AS outcome_names
    FROM yes_legs
    GROUP BY matchup_id, pinnacle_sport_id, family_key
)
SELECT
    b.matchup_id,
    b.pinnacle_sport_id,
    b.home_name, b.away_name, b.start_time, b.last_is_live,
    b.family_key  AS polymarket_event_id,
    b.num_legs,
    b.sum_ask     AS sum_yes_ask,
    b.sum_bid     AS sum_yes_bid,
    b.legs_missing_ask,
    b.token_ids,
    b.outcome_names,
    b.pm_latest_received_at,
    EXTRACT(EPOCH FROM (now() - b.pm_latest_received_at))::numeric(10,2) AS pm_age_sec,
    -- buy_all_cost: if you bought 1 share of every leg you would pay
    -- this in USDC and be guaranteed $1 back regardless of outcome.
    -- A value below 1 is an immediate arbitrage; above 1 is the
    -- overround the market is currently charging.
    b.sum_ask AS buy_all_cost,
    (1 - COALESCE(b.sum_ask, 1))::numeric(10,4) AS arb_buy_pp,
    -- sell_all_proceeds: if you held a full bundle and sold every leg
    -- at its bid you'd receive this. Above 1 means the market is
    -- offering more than dollar-cost for the complete set you hold.
    b.sum_bid AS sell_all_proceeds,
    (COALESCE(b.sum_bid, 1) - 1)::numeric(10,4) AS arb_sell_pp,
    CASE
        WHEN b.num_legs < 2 OR b.legs_missing_ask > 0 THEN 'incomplete'
        WHEN b.sum_ask <  1.0 - 0.005                  THEN 'arb_buy'
        WHEN b.sum_bid >  1.0 + 0.005                  THEN 'arb_sell'
        WHEN b.sum_ask <= 1.01                         THEN 'tight'
        WHEN b.sum_ask <= 1.05                         THEN 'wide_buy'
        ELSE                                                'wide_sell'
    END AS parity_state
FROM bundled b;
