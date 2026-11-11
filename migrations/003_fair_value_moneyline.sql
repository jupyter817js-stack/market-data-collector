CREATE OR REPLACE VIEW fair_value_moneyline_by_book AS
WITH moneyline_quotes AS (
    SELECT
        source,
        canonical_event_id,
        sport_key,
        league,
        home_team,
        away_team,
        commence_time,
        bookmaker_key,
        outcome_name,
        outcome_side,
        price_decimal,
        (1::numeric / NULLIF(price_decimal, 0)) AS implied_probability,
        source_last_update,
        received_at,
        stale_state,
        confidence_score
    FROM fair_value_ready_quotes
    WHERE market_key = 'h2h'
      AND stale_state <> 'stale'
      AND price_decimal > 0
),
book_totals AS (
    SELECT
        *,
        sum(implied_probability) OVER (
            PARTITION BY source, canonical_event_id, bookmaker_key
        ) AS total_implied_probability
    FROM moneyline_quotes
)
SELECT
    source,
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    bookmaker_key,
    outcome_name,
    outcome_side,
    price_decimal,
    implied_probability,
    implied_probability / NULLIF(total_implied_probability, 0) AS no_vig_probability,
    source_last_update,
    received_at,
    stale_state,
    confidence_score
FROM book_totals
WHERE total_implied_probability > 0;

CREATE OR REPLACE VIEW fair_value_moneyline_consensus AS
SELECT
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    outcome_name,
    outcome_side,
    avg(no_vig_probability) AS average_no_vig_probability,
    min(no_vig_probability) AS min_no_vig_probability,
    max(no_vig_probability) AS max_no_vig_probability,
    count(DISTINCT (source, bookmaker_key)) AS bookmaker_count,
    max(received_at) AS latest_received_at
FROM fair_value_moneyline_by_book
GROUP BY
    canonical_event_id,
    sport_key,
    league,
    home_team,
    away_team,
    commence_time,
    outcome_name,
    outcome_side;
