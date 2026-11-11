-- LSPORTS TRADE360 subscription ledger.
--
-- Trial cap is 100 fixtures over 14 days. LSPORTS Support 2026-05-27
-- confirmed the (C) refund semantic:
--   Subscribe()   deducts 1 quota immediately.
--   Unsubscribe() refunds the quota ONLY if the match has not yet
--                 transitioned to InProgress.
--   InProgress    is the point of no return — the slot is permanently
--                 consumed and Unsubscribe is a no-op for accounting.
--
-- The /Package/GetPackageQuota REST call gives the live remaining count
-- but cannot tell us WHERE the consumed slots went, which fixtures are
-- redeemable, or how the budget was used across the 14 days. This table
-- is the local source of truth for that breakdown.
--
-- Every cmd/lsports-subscribe and cmd/lsports-unsubscribe action writes
-- to this table inside the same transaction as the REST call. If the
-- REST call fails the ledger insert/update is rolled back.

CREATE TABLE IF NOT EXISTS lsports_subscriptions (
    id BIGSERIAL PRIMARY KEY,

    -- Fixture identity (LSPORTS side)
    lsports_fixture_id BIGINT NOT NULL,
    lsports_sport_id   INTEGER,
    lsports_league_id  INTEGER,
    fixture_name       TEXT,
    home_participant   TEXT,
    away_participant   TEXT,
    commence_time      TIMESTAMPTZ,

    -- Subscription lifecycle. Transitions are operator- or babysitter-
    -- driven (no automatic refund) so the timeline below mirrors what
    -- actually happened, not what was intended.
    --
    --   pending               Subscribed; match has NOT yet gone InProgress.
    --                         Unsubscribe would refund the slot.
    --   committed_inprogress  Match transitioned to InProgress while
    --                         subscribed. Slot is permanently consumed.
    --   refunded              Unsubscribed before InProgress; slot returned.
    --   unsubscribed_too_late Unsubscribed after InProgress; slot NOT returned.
    --                         (Records the operational error so we learn from it.)
    --   settled               Match finished; live data is complete.
    status TEXT NOT NULL DEFAULT 'pending',
    CONSTRAINT lsports_subscriptions_status_chk CHECK (status IN (
        'pending',
        'committed_inprogress',
        'refunded',
        'unsubscribed_too_late',
        'settled'
    )),

    -- Quota accounting snapshots. Subscribe writes BEFORE/AFTER. Unsubscribe
    -- writes its own AFTER (delta tells us if a refund actually happened).
    quota_remaining_before_subscribe  INTEGER,
    quota_remaining_after_subscribe   INTEGER,
    quota_remaining_after_unsubscribe INTEGER,

    -- Reviewer / audit trail.
    operator           TEXT NOT NULL,
    subscribe_reason   TEXT NOT NULL,
    unsubscribe_reason TEXT,

    -- Polymarket coverage check (operator-attested at subscribe time).
    -- We refuse to spend a slot on a fixture that has no Polymarket
    -- comparison market — that would be quota waste with zero signal
    -- value. The CLI may be overridden with an explicit reason.
    polymarket_coverage_verified  BOOLEAN NOT NULL DEFAULT FALSE,
    polymarket_coverage_notes     TEXT,
    polymarket_canonical_event_id UUID,

    -- Lifecycle timestamps.
    subscribed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    inprogress_at   TIMESTAMPTZ,
    unsubscribed_at TIMESTAMPTZ,
    settled_at      TIMESTAMPTZ,

    notes      TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT lsports_subscriptions_reason_nonempty CHECK (
        length(btrim(operator)) > 0 AND length(btrim(subscribe_reason)) > 0
    )
);

-- One open subscription per fixture at a time. A second Subscribe attempt
-- against a fixture currently in 'pending' or 'committed_inprogress' must
-- not be allowed to silently double-charge the budget.
CREATE UNIQUE INDEX IF NOT EXISTS lsports_subscriptions_one_open_per_fixture
    ON lsports_subscriptions (lsports_fixture_id)
    WHERE status IN ('pending', 'committed_inprogress');

CREATE INDEX IF NOT EXISTS lsports_subscriptions_status_idx
    ON lsports_subscriptions (status);

CREATE INDEX IF NOT EXISTS lsports_subscriptions_commence_time_idx
    ON lsports_subscriptions (commence_time);

CREATE INDEX IF NOT EXISTS lsports_subscriptions_subscribed_at_idx
    ON lsports_subscriptions (subscribed_at DESC);

-- updated_at auto-maintain.
CREATE OR REPLACE FUNCTION lsports_subscriptions_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS lsports_subscriptions_touch_updated_at_trg
    ON lsports_subscriptions;
CREATE TRIGGER lsports_subscriptions_touch_updated_at_trg
    BEFORE UPDATE ON lsports_subscriptions
    FOR EACH ROW EXECUTE FUNCTION lsports_subscriptions_touch_updated_at();

-- Operational summary view: how the 100-fixture trial budget was spent.
-- Counts are over all-time rows; for the trial window filter on
-- subscribed_at >= '2026-05-27' externally.
CREATE OR REPLACE VIEW lsports_subscription_budget_summary AS
SELECT
    count(*)                                          AS total_rows,
    count(*) FILTER (WHERE status = 'pending')        AS pending_now,
    count(*) FILTER (WHERE status = 'committed_inprogress') AS committed_inprogress,
    count(*) FILTER (WHERE status = 'refunded')       AS refunded,
    count(*) FILTER (WHERE status = 'unsubscribed_too_late') AS unsubscribed_too_late,
    count(*) FILTER (WHERE status = 'settled')        AS settled,
    -- Permanent burn = anything that ever went InProgress (committed +
    -- settled + unsubscribed_too_late). This is what counts against the 100.
    count(*) FILTER (WHERE status IN (
        'committed_inprogress', 'settled', 'unsubscribed_too_late'
    ))                                                 AS permanent_burn,
    count(*) FILTER (WHERE
        status = 'pending'
        AND subscribed_at > now() - interval '24 hours'
    )                                                  AS pending_last_24h
FROM lsports_subscriptions;
