-- polymarket_sync hits dead orderbooks (status 404) on ~22 tokens per
-- tick. With a 300-token budget per tick that's ~7% wasted budget +
-- pointless log noise. Track those tokens and skip them for 24h.
--
-- "Dead" definition: the CLOB endpoint /book?token_id=<X> consistently
-- returns 404 "No orderbook exists for the requested token id". Could
-- be a token whose market was archived but is still flagged active in
-- gamma, or a CLOB-side data-issue token.
--
-- Schema is intentionally simple: one row per token. consecutive_404s
-- climbs each tick; skip_until is recomputed on each 404 with
-- exponential backoff (1h → 6h → 24h → cap 24h). A successful price
-- snapshot zeroes the counter and clears skip_until.

CREATE TABLE IF NOT EXISTS polymarket_dead_tokens (
    token_id text PRIMARY KEY,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    consecutive_404s int NOT NULL DEFAULT 0,
    skip_until timestamptz NOT NULL DEFAULT now(),
    last_error text NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS polymarket_dead_tokens_skip_until_idx
    ON polymarket_dead_tokens (skip_until);
