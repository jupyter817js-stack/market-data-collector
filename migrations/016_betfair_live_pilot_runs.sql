CREATE TABLE IF NOT EXISTS betfair_live_pilot_runs (
    id bigserial PRIMARY KEY,
    run_label text NOT NULL DEFAULT '',
    stream_id text NOT NULL DEFAULT '',
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('running', 'success', 'failed', 'canceled')),
    stop_reason text NOT NULL DEFAULT '',
    failure_reason text NOT NULL DEFAULT '',
    address_redacted text NOT NULL DEFAULT '',
    server_name text NOT NULL DEFAULT '',
    market_ids_count integer NOT NULL DEFAULT 0 CHECK (market_ids_count >= 0),
    market_ids jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(market_ids) = 'array'),
    duration_limit_seconds integer CHECK (duration_limit_seconds IS NULL OR duration_limit_seconds > 0),
    max_messages integer CHECK (max_messages IS NULL OR max_messages > 0),
    messages_read integer NOT NULL DEFAULT 0 CHECK (messages_read >= 0),
    market_change_messages integer NOT NULL DEFAULT 0 CHECK (market_change_messages >= 0),
    heartbeat_messages integer NOT NULL DEFAULT 0 CHECK (heartbeat_messages >= 0),
    status_messages integer NOT NULL DEFAULT 0 CHECK (status_messages >= 0),
    raw_messages_seen integer NOT NULL DEFAULT 0 CHECK (raw_messages_seen >= 0),
    raw_messages_inserted integer NOT NULL DEFAULT 0 CHECK (raw_messages_inserted >= 0),
    candidates_emitted integer NOT NULL DEFAULT 0 CHECK (candidates_emitted >= 0),
    candidates_inserted integer NOT NULL DEFAULT 0 CHECK (candidates_inserted >= 0),
    candidates_unchanged integer NOT NULL DEFAULT 0 CHECK (candidates_unchanged >= 0),
    candidates_skipped integer NOT NULL DEFAULT 0 CHECK (candidates_skipped >= 0),
    markets_seen integer NOT NULL DEFAULT 0 CHECK (markets_seen >= 0),
    last_clk text NOT NULL DEFAULT '',
    last_initial_clk text NOT NULL DEFAULT '',
    last_message_time timestamptz,
    heartbeat_state text NOT NULL DEFAULT '',
    exit_code integer,
    elapsed_ms bigint CHECK (elapsed_ms IS NULL OR elapsed_ms >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS betfair_live_pilot_runs_started_idx
    ON betfair_live_pilot_runs (started_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS betfair_live_pilot_runs_finished_idx
    ON betfair_live_pilot_runs (finished_at DESC NULLS LAST, id DESC);

CREATE INDEX IF NOT EXISTS betfair_live_pilot_runs_status_idx
    ON betfair_live_pilot_runs (status, started_at DESC);

CREATE INDEX IF NOT EXISTS betfair_live_pilot_runs_label_idx
    ON betfair_live_pilot_runs (run_label, started_at DESC)
    WHERE run_label <> '';

CREATE INDEX IF NOT EXISTS betfair_live_pilot_runs_stream_idx
    ON betfair_live_pilot_runs (stream_id)
    WHERE stream_id <> '';

CREATE OR REPLACE VIEW betfair_live_pilot_run_summary AS
SELECT
    p.*,
    coalesce(raw_counts.raw_message_count, 0)::bigint AS linked_raw_message_count,
    coalesce(history_counts.quote_history_rows, 0)::bigint AS linked_quote_history_rows,
    coalesce(latest_counts.latest_quote_count, 0)::bigint AS latest_quote_count,
    coalesce(mapped_counts.mapped_quote_count, 0)::bigint AS mapped_quote_count,
    coalesce(readiness_counts.ready_runner_book_count, 0)::bigint AS ready_runner_book_count,
    coalesce(comparison_counts.ready_comparison_count, 0)::bigint AS ready_comparison_count
FROM betfair_live_pilot_runs p
LEFT JOIN LATERAL (
    SELECT count(*) AS raw_message_count
    FROM betfair_raw_stream_messages r
    WHERE p.stream_id <> ''
      AND r.stream_id = p.stream_id
) raw_counts ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS quote_history_rows
    FROM betfair_exchange_quote_history h
    WHERE h.received_at >= p.started_at
      AND h.received_at <= coalesce(p.finished_at, now()) + interval '5 minutes'
      AND (
          p.market_ids_count = 0
          OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements_text(p.market_ids) mids(market_id)
              WHERE mids.market_id = h.market_id
          )
      )
) history_counts ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS latest_quote_count
    FROM betfair_latest_exchange_quotes_view q
    WHERE p.market_ids_count = 0
       OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements_text(p.market_ids) mids(market_id)
           WHERE mids.market_id = q.market_id
       )
) latest_counts ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS mapped_quote_count
    FROM fair_value_ready_betfair_exchange_quotes q
    WHERE p.market_ids_count = 0
       OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements_text(p.market_ids) mids(market_id)
           WHERE mids.market_id = q.betfair_market_id
       )
) mapped_counts ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS ready_runner_book_count
    FROM betfair_exchange_quote_readiness r
    WHERE r.readiness_state = 'ready'
      AND (
          p.market_ids_count = 0
          OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements_text(p.market_ids) mids(market_id)
              WHERE mids.market_id = r.betfair_market_id
          )
      )
) readiness_counts ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS ready_comparison_count
    FROM sportsbook_betfair_moneyline_ready_comparison c
    WHERE p.market_ids_count = 0
       OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements_text(p.market_ids) mids(market_id)
           WHERE mids.market_id = c.betfair_market_id
       )
) comparison_counts ON true;

CREATE OR REPLACE VIEW betfair_live_pilot_recent_runs AS
SELECT *
FROM betfair_live_pilot_run_summary
ORDER BY started_at DESC, id DESC
LIMIT 50;

CREATE OR REPLACE VIEW data_quality_betfair_live_pilot_runs AS
SELECT
    'pilot_failed_or_canceled'::text AS issue_type,
    CASE WHEN status = 'failed' THEN 'error' ELSE 'warning' END AS severity,
    id AS run_id,
    run_label,
    stream_id,
    status,
    stop_reason,
    failure_reason,
    started_at,
    finished_at,
    'pilot run did not finish successfully'::text AS reason
FROM betfair_live_pilot_run_summary
WHERE status IN ('failed', 'canceled')
UNION ALL
SELECT
    'pilot_no_raw_messages',
    'warning',
    id,
    run_label,
    stream_id,
    status,
    stop_reason,
    failure_reason,
    started_at,
    finished_at,
    'pilot run inserted zero linked raw stream messages'
FROM betfair_live_pilot_run_summary
WHERE status = 'success'
  AND linked_raw_message_count = 0
UNION ALL
SELECT
    'pilot_no_candidates',
    'warning',
    id,
    run_label,
    stream_id,
    status,
    stop_reason,
    failure_reason,
    started_at,
    finished_at,
    'pilot run emitted zero exchange quote candidates'
FROM betfair_live_pilot_run_summary
WHERE status = 'success'
  AND candidates_emitted = 0
UNION ALL
SELECT
    'pilot_no_latest_quotes',
    'review',
    id,
    run_label,
    stream_id,
    status,
    stop_reason,
    failure_reason,
    started_at,
    finished_at,
    'pilot run has no latest Betfair exchange quote rows for its market scope'
FROM betfair_live_pilot_run_summary
WHERE status = 'success'
  AND latest_quote_count = 0
UNION ALL
SELECT
    'pilot_no_mapped_rows',
    'review',
    id,
    run_label,
    stream_id,
    status,
    stop_reason,
    failure_reason,
    started_at,
    finished_at,
    'pilot run has no confirmed mapped Betfair exchange quote rows for its market scope'
FROM betfair_live_pilot_run_summary
WHERE status = 'success'
  AND mapped_quote_count = 0
UNION ALL
SELECT
    'pilot_no_ready_rows',
    'review',
    id,
    run_label,
    stream_id,
    status,
    stop_reason,
    failure_reason,
    started_at,
    finished_at,
    'pilot run has no ready Betfair runner-book rows for its market scope'
FROM betfair_live_pilot_run_summary
WHERE status = 'success'
  AND ready_runner_book_count = 0
UNION ALL
SELECT
    'pilot_no_ready_comparisons',
    'review',
    id,
    run_label,
    stream_id,
    status,
    stop_reason,
    failure_reason,
    started_at,
    finished_at,
    'pilot run has no ready sportsbook-vs-Betfair comparison rows for its market scope'
FROM betfair_live_pilot_run_summary
WHERE status = 'success'
  AND ready_comparison_count = 0;
