-- B3: depth-adjusted average price. polymarket-sync already fetches the
-- full /book ladder per token tick and stores it in raw_payload->'book'
-- (verified live: 5-10 levels per side per snapshot). We just never
-- spent the levels — bestBid / bestAsk are surfaced but a $20 buy might
-- walk into level 2 or 3 and pay a worse average price than the top.
--
-- This migration adds polymarket_depth_avg_price(token_id, side, size_usdc)
-- — the weighted-average fill price for a market order of that size
-- against the latest snapshot's ladder. Returns NULL when the book
-- doesn't have enough cumulative depth to cover the size.
--
-- Signal-engine integration (use depth-adjusted instead of best_ask
-- inside gross_edge_to_buy) is deferred to a follow-up; today the
-- function exists as a SQL primitive the dashboard / paper-pnl can
-- query.

CREATE OR REPLACE FUNCTION polymarket_depth_avg_price(
    p_token_id text,
    p_side     text,    -- 'ask' (BUYing taker) or 'bid' (SELLing taker)
    p_size_usdc numeric
) RETURNS numeric
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_book           jsonb;
    v_levels         jsonb;
    v_level          jsonb;
    v_price          numeric;
    v_level_shares   numeric;
    v_usdc_remaining numeric := p_size_usdc;
    v_shares_taken   numeric := 0;
    v_usdc_taken     numeric := 0;
BEGIN
    IF p_size_usdc IS NULL OR p_size_usdc <= 0 THEN RETURN NULL; END IF;
    IF p_side NOT IN ('ask','bid') THEN RETURN NULL; END IF;

    SELECT raw_payload->'book' INTO v_book
      FROM polymarket_price_snapshots
     WHERE token_id = p_token_id
     ORDER BY received_at DESC
     LIMIT 1;
    IF v_book IS NULL THEN RETURN NULL; END IF;

    IF p_side = 'ask' THEN
        v_levels := v_book->'asks';
    ELSE
        v_levels := v_book->'bids';
    END IF;

    IF v_levels IS NULL OR jsonb_array_length(v_levels) = 0 THEN
        RETURN NULL;
    END IF;

    FOR v_level IN SELECT * FROM jsonb_array_elements(v_levels) LOOP
        v_price        := NULLIF(v_level->>'price','')::numeric;
        v_level_shares := NULLIF(v_level->>'size' ,'')::numeric;
        IF v_price IS NULL OR v_level_shares IS NULL THEN CONTINUE; END IF;
        IF v_price <= 0 OR v_level_shares <= 0     THEN CONTINUE; END IF;

        DECLARE
            v_level_usdc numeric := v_price * v_level_shares;
        BEGIN
            IF v_level_usdc >= v_usdc_remaining THEN
                -- This level alone covers the remaining size.
                v_shares_taken := v_shares_taken + (v_usdc_remaining / v_price);
                v_usdc_taken   := v_usdc_taken   + v_usdc_remaining;
                v_usdc_remaining := 0;
                EXIT;
            ELSE
                -- Take the whole level, advance.
                v_shares_taken := v_shares_taken + v_level_shares;
                v_usdc_taken   := v_usdc_taken   + v_level_usdc;
                v_usdc_remaining := v_usdc_remaining - v_level_usdc;
            END IF;
        END;
    END LOOP;

    IF v_usdc_remaining > 0 THEN
        -- Book ran out of depth before we filled the order.
        RETURN NULL;
    END IF;
    IF v_shares_taken = 0 THEN RETURN NULL; END IF;
    RETURN v_usdc_taken / v_shares_taken;
END;
$$;

-- Convenience view: depth-adjusted prices at the canonical trade sizes
-- (matches SIGNAL_PROPOSED_SIZE_USDC and the risk cap), one row per
-- token. NULL when the ladder can't cover.
CREATE OR REPLACE VIEW polymarket_token_depth_adjusted AS
SELECT DISTINCT ON (s.token_id)
    s.token_id,
    s.condition_id,
    s.received_at AS snapshot_at,
    s.bid_price          AS top_bid,
    s.ask_price          AS top_ask,
    polymarket_depth_avg_price(s.token_id, 'ask', 20)  AS depth_avg_ask_20,
    polymarket_depth_avg_price(s.token_id, 'ask', 50)  AS depth_avg_ask_50,
    polymarket_depth_avg_price(s.token_id, 'ask', 200) AS depth_avg_ask_200,
    polymarket_depth_avg_price(s.token_id, 'bid', 20)  AS depth_avg_bid_20,
    polymarket_depth_avg_price(s.token_id, 'bid', 50)  AS depth_avg_bid_50
  FROM polymarket_price_snapshots s
 ORDER BY s.token_id, s.received_at DESC;
