-- Phase D analytical surfaces.
--
-- 1. lsports_source_comparison: per (fixture, observation_kind), counts and
--    publish-to-receive lag stats for the two LSPORTS source channels
--    (REST snapshot polling vs AMQP push). This is the headline answer to
--    "is push faster than poll, and by how much?".
--
-- 2. lsports_amqp_vs_snapshot_pairs: helpers that match approximately-
--    contemporaneous snapshot and amqp_push observations on the same
--    fixture+kind so analysis can compare publish-to-receive on a
--    per-event basis.
--
-- Both views are read-only — no writes from this migration.

CREATE OR REPLACE VIEW lsports_source_comparison AS
SELECT
    lsports_fixture_id,
    observation_kind,
    count(*) FILTER (WHERE source = 'snapshot')  AS snapshot_count,
    count(*) FILTER (WHERE source = 'amqp_push') AS amqp_count,
    min(received_at) FILTER (WHERE source = 'snapshot')  AS snapshot_first_at,
    max(received_at) FILTER (WHERE source = 'snapshot')  AS snapshot_last_at,
    min(received_at) FILTER (WHERE source = 'amqp_push') AS amqp_first_at,
    max(received_at) FILTER (WHERE source = 'amqp_push') AS amqp_last_at,

    -- publish_to_receive_sec = how long it took us to receive a message
    -- after LSPORTS server-side published it. Smaller = faster path. AMQP
    -- push should be measurably smaller than snapshot polling.
    avg(EXTRACT(EPOCH FROM (received_at - source_publish_time))) FILTER (
        WHERE source = 'snapshot'  AND source_publish_time IS NOT NULL
    )::numeric(10,3) AS snapshot_avg_publish_to_receive_sec,
    percentile_cont(0.5) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (received_at - source_publish_time))
    ) FILTER (
        WHERE source = 'snapshot' AND source_publish_time IS NOT NULL
    )::numeric(10,3) AS snapshot_median_publish_to_receive_sec,

    avg(EXTRACT(EPOCH FROM (received_at - source_publish_time))) FILTER (
        WHERE source = 'amqp_push' AND source_publish_time IS NOT NULL
    )::numeric(10,3) AS amqp_avg_publish_to_receive_sec,
    percentile_cont(0.5) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (received_at - source_publish_time))
    ) FILTER (
        WHERE source = 'amqp_push' AND source_publish_time IS NOT NULL
    )::numeric(10,3) AS amqp_median_publish_to_receive_sec
FROM lsports_inplay_observations
GROUP BY lsports_fixture_id, observation_kind;

-- A coarser top-level summary (across all fixtures).
CREATE OR REPLACE VIEW lsports_source_comparison_global AS
SELECT
    observation_kind,
    count(*) FILTER (WHERE source = 'snapshot')  AS snapshot_count,
    count(*) FILTER (WHERE source = 'amqp_push') AS amqp_count,
    avg(EXTRACT(EPOCH FROM (received_at - source_publish_time))) FILTER (
        WHERE source = 'snapshot'  AND source_publish_time IS NOT NULL
    )::numeric(10,3) AS snapshot_avg_publish_to_receive_sec,
    avg(EXTRACT(EPOCH FROM (received_at - source_publish_time))) FILTER (
        WHERE source = 'amqp_push' AND source_publish_time IS NOT NULL
    )::numeric(10,3) AS amqp_avg_publish_to_receive_sec,
    percentile_cont(0.5) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (received_at - source_publish_time))
    ) FILTER (
        WHERE source = 'snapshot' AND source_publish_time IS NOT NULL
    )::numeric(10,3) AS snapshot_median_publish_to_receive_sec,
    percentile_cont(0.5) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (received_at - source_publish_time))
    ) FILTER (
        WHERE source = 'amqp_push' AND source_publish_time IS NOT NULL
    )::numeric(10,3) AS amqp_median_publish_to_receive_sec
FROM lsports_inplay_observations
WHERE source_publish_time IS NOT NULL
GROUP BY observation_kind;
