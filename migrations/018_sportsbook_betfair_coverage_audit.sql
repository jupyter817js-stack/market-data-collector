CREATE OR REPLACE VIEW sportsbook_betfair_comparison_coverage_audit AS
WITH mapped_markets AS (
    SELECT
        canonical_event_id,
        min(sport_key) AS sport_key,
        min(league) AS league,
        min(home_team) AS home_team,
        min(away_team) AS away_team,
        min(commence_time) AS commence_time,
        betfair_market_id,
        max(canonical_market_family) AS canonical_market_family,
        count(*) AS mapped_betfair_quote_count,
        count(DISTINCT (selection_id, handicap)) AS mapped_betfair_runner_count,
        max(received_at) AS latest_betfair_received_at
    FROM fair_value_ready_betfair_exchange_quotes
    GROUP BY canonical_event_id, betfair_market_id
),
readiness AS (
    SELECT
        canonical_event_id,
        betfair_market_id,
        count(*) AS betfair_runner_book_count,
        count(*) FILTER (WHERE readiness_state = 'ready') AS ready_betfair_runner_book_count,
        string_agg(DISTINCT readiness_state, ',' ORDER BY readiness_state) AS readiness_states
    FROM betfair_exchange_quote_readiness
    GROUP BY canonical_event_id, betfair_market_id
),
sportsbook AS (
    SELECT
        canonical_event_id,
        count(*) AS sportsbook_consensus_outcome_count,
        max(latest_received_at) AS latest_sportsbook_received_at
    FROM fair_value_moneyline_consensus
    GROUP BY canonical_event_id
),
comparison AS (
    SELECT
        canonical_event_id,
        betfair_market_id,
        count(*) AS comparison_row_count,
        count(*) FILTER (WHERE betfair_readiness_state = 'ready') AS ready_comparison_row_count
    FROM sportsbook_betfair_moneyline_comparison
    GROUP BY canonical_event_id, betfair_market_id
),
base AS (
    SELECT
        ce.id AS canonical_event_id,
        ce.sport_key,
        ce.league,
        ce.home_team,
        ce.away_team,
        ce.commence_time,
        mm.betfair_market_id,
        mm.canonical_market_family,
        coalesce(mm.mapped_betfair_quote_count, 0) AS mapped_betfair_quote_count,
        coalesce(mm.mapped_betfair_runner_count, 0) AS mapped_betfair_runner_count,
        mm.latest_betfair_received_at
    FROM canonical_events ce
    LEFT JOIN mapped_markets mm
      ON mm.canonical_event_id = ce.id
)
SELECT
    b.canonical_event_id,
    b.sport_key,
    b.league,
    b.home_team,
    b.away_team,
    b.commence_time,
    b.betfair_market_id,
    coalesce(b.canonical_market_family, '') AS canonical_market_family,
    b.mapped_betfair_quote_count,
    b.mapped_betfair_runner_count,
    coalesce(r.betfair_runner_book_count, 0) AS betfair_runner_book_count,
    coalesce(r.ready_betfair_runner_book_count, 0) AS ready_betfair_runner_book_count,
    coalesce(s.sportsbook_consensus_outcome_count, 0) AS sportsbook_consensus_outcome_count,
    coalesce(c.comparison_row_count, 0) AS comparison_row_count,
    coalesce(c.ready_comparison_row_count, 0) AS ready_comparison_row_count,
    coalesce(r.readiness_states, '') AS readiness_states,
    b.latest_betfair_received_at,
    s.latest_sportsbook_received_at,
    CASE
        WHEN b.mapped_betfair_quote_count = 0 THEN 'mapping_missing'
        WHEN coalesce(b.canonical_market_family, '') <> 'h2h' THEN 'unsupported_market_family'
        WHEN coalesce(s.sportsbook_consensus_outcome_count, 0) = 0 THEN 'no_sportsbook_consensus'
        WHEN coalesce(r.ready_betfair_runner_book_count, 0) = 0 THEN
            CASE
                WHEN coalesce(r.readiness_states, '') ~ '(^|,)(aging|low_confidence|stale|thin_liquidity|wide_spread)(,|$)'
                THEN 'stale_or_low_confidence'
                ELSE 'betfair_not_ready'
            END
        WHEN coalesce(c.comparison_row_count, 0) = 0 THEN 'no_matching_outcome'
        WHEN coalesce(c.ready_comparison_row_count, 0) = 0 THEN 'comparison_missing'
        ELSE 'comparison_ready'
    END AS gap_reason,
    b.mapped_betfair_quote_count > 0 AS betfair_mapped,
    coalesce(r.ready_betfair_runner_book_count, 0) > 0 AS betfair_ready_runner_books,
    coalesce(s.sportsbook_consensus_outcome_count, 0) > 0 AS sportsbook_consensus,
    coalesce(c.ready_comparison_row_count, 0) > 0 AS comparison_ready
FROM base b
LEFT JOIN readiness r
  ON r.canonical_event_id = b.canonical_event_id
 AND r.betfair_market_id IS NOT DISTINCT FROM b.betfair_market_id
LEFT JOIN sportsbook s
  ON s.canonical_event_id = b.canonical_event_id
LEFT JOIN comparison c
  ON c.canonical_event_id = b.canonical_event_id
 AND c.betfair_market_id IS NOT DISTINCT FROM b.betfair_market_id;
