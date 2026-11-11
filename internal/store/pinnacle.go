package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/your-org/pol-odds-collector/internal/source/pinnacle"
)

// PinnacleStore implements pinnacle.Sink against a *sql.DB. Reuses the
// existing PostgresStore connection so we don't open a second pool.
type PinnacleStore struct {
	db *sql.DB
}

// NewPinnacleStore returns a PinnacleStore that writes through s.db.
func NewPinnacleStore(s *PostgresStore) *PinnacleStore {
	return &PinnacleStore{db: s.db}
}

// UpsertLeagues records each active league. Sport row is upserted as needed.
func (p *PinnacleStore) UpsertLeagues(ctx context.Context, sportID int, leagues []pinnacle.League) error {
	if _, err := p.db.ExecContext(ctx, `
        INSERT INTO pinnacle_sports (id, name, last_seen_at)
        VALUES ($1, $2, now())
        ON CONFLICT (id) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
    `, sportID, fmt.Sprintf("sport_%d", sportID)); err != nil {
		return fmt.Errorf("upsert pinnacle_sports: %w", err)
	}
	for _, l := range leagues {
		if _, err := p.db.ExecContext(ctx, `
            INSERT INTO pinnacle_leagues
                (id, sport_id, name, league_group, last_matchup_count, is_featured, last_seen_at)
            VALUES ($1,$2,$3,$4,$5,$6, now())
            ON CONFLICT (id) DO UPDATE SET
                sport_id = EXCLUDED.sport_id,
                name = EXCLUDED.name,
                league_group = EXCLUDED.league_group,
                last_matchup_count = EXCLUDED.last_matchup_count,
                is_featured = EXCLUDED.is_featured,
                last_seen_at = EXCLUDED.last_seen_at
        `, l.ID, sportID, l.Name, l.Group, l.MatchupCount, l.IsFeatured); err != nil {
			return fmt.Errorf("upsert pinnacle_leagues id=%d: %w", l.ID, err)
		}
	}
	return nil
}

// UpsertMatchups records each matchup in the league. Returns the count of
// rows that were newly inserted (vs updated).
func (p *PinnacleStore) UpsertMatchups(ctx context.Context, leagueID int, matchups []pinnacle.Matchup) (int, error) {
	newCount := 0
	for _, m := range matchups {
		raw, err := json.Marshal(m)
		if err != nil {
			return newCount, fmt.Errorf("marshal matchup %d: %w", m.ID, err)
		}
		homeName, awayName, homeID, awayID := m.HomeAway()
		var startTime any
		if !m.StartTime.IsZero() {
			startTime = m.StartTime
		}
		res, err := p.db.ExecContext(ctx, `
            INSERT INTO pinnacle_matchups
                (id, league_id, matchup_type, start_time, home_name, away_name, home_id, away_id,
                 last_status, last_live_status, last_is_live, last_has_markets, last_has_live, last_period,
                 raw_payload, last_seen_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
                    $9,$10,$11,$12,$13,$14,
                    $15::jsonb, now())
            ON CONFLICT (id) DO UPDATE SET
                league_id = EXCLUDED.league_id,
                matchup_type = EXCLUDED.matchup_type,
                start_time = EXCLUDED.start_time,
                home_name = EXCLUDED.home_name,
                away_name = EXCLUDED.away_name,
                home_id = EXCLUDED.home_id,
                away_id = EXCLUDED.away_id,
                last_status = EXCLUDED.last_status,
                last_live_status = EXCLUDED.last_live_status,
                last_is_live = EXCLUDED.last_is_live,
                last_has_markets = EXCLUDED.last_has_markets,
                last_has_live = EXCLUDED.last_has_live,
                last_period = EXCLUDED.last_period,
                raw_payload = EXCLUDED.raw_payload,
                last_seen_at = EXCLUDED.last_seen_at
        `, m.ID, leagueID, m.Type, startTime, homeName, awayName, nullableInt64(homeID), nullableInt64(awayID),
			m.Status, m.LiveStatus, m.IsLive, m.HasMarkets, m.HasLive, m.Period,
			string(raw))
		if err != nil {
			return newCount, fmt.Errorf("upsert pinnacle_matchups id=%d: %w", m.ID, err)
		}
		if affected, _ := res.RowsAffected(); affected == 1 {
			// pg upsert returns 1 for both insert and update; we lose the
			// distinction here, but it's not load-bearing.
			newCount++
		}
	}
	return newCount, nil
}

// SaveMarketSnapshot inserts into pinnacle_market_history on version change
// and updates pinnacle_market_latest pointer. Returns inserted=true if the
// version was new (i.e. this is a genuine change).
func (p *PinnacleStore) SaveMarketSnapshot(ctx context.Context, m pinnacle.Market) (bool, error) {
	pricesJSON, err := json.Marshal(m.Prices)
	if err != nil {
		return false, fmt.Errorf("marshal prices: %w", err)
	}
	limitsJSON, err := json.Marshal(m.Limits)
	if err != nil {
		return false, fmt.Errorf("marshal limits: %w", err)
	}
	rawJSON, err := json.Marshal(m)
	if err != nil {
		return false, fmt.Errorf("marshal raw: %w", err)
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var historyID int64
	err = tx.QueryRowContext(ctx, `
        INSERT INTO pinnacle_market_history
            (matchup_id, period, market_type, market_key, side, is_alternate,
             status, version, prices, limits, raw_payload, received_at)
        VALUES ($1,$2,$3,$4,$5,$6,
                $7,$8,$9::jsonb,$10::jsonb,$11::jsonb, now())
        ON CONFLICT (matchup_id, market_key, version) DO NOTHING
        RETURNING id
    `, m.MatchupID, m.Period, m.Type, m.Key, m.Side, m.IsAlternate,
		m.Status, m.Version, string(pricesJSON), string(limitsJSON), string(rawJSON)).
		Scan(&historyID)

	switch {
	case err == sql.ErrNoRows:
		// Version already present — no change to record. Still bump
		// last_polled_at so freshness gates measure "did we observe
		// this market recently?" rather than "did the odds move
		// recently?" (migration 039 split those semantics).
		if _, err := tx.ExecContext(ctx, `
            UPDATE pinnacle_market_latest
               SET last_polled_at = now()
             WHERE matchup_id = $1 AND market_key = $2
        `, m.MatchupID, m.Key); err != nil {
			return false, fmt.Errorf("bump pinnacle_market_latest.last_polled_at: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit: %w", err)
		}
		return false, nil
	case err != nil:
		return false, fmt.Errorf("insert pinnacle_market_history: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
        INSERT INTO pinnacle_market_latest
            (matchup_id, market_key, history_id, version, status, received_at, last_polled_at)
        VALUES ($1,$2,$3,$4,$5, now(), now())
        ON CONFLICT (matchup_id, market_key) DO UPDATE SET
            history_id     = EXCLUDED.history_id,
            version        = EXCLUDED.version,
            status         = EXCLUDED.status,
            received_at    = EXCLUDED.received_at,
            last_polled_at = EXCLUDED.last_polled_at
    `, m.MatchupID, m.Key, historyID, m.Version, m.Status); err != nil {
		return false, fmt.Errorf("upsert pinnacle_market_latest: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}

// SweepStaleLiveFlags clears last_is_live=true on matchups for a sport
// that haven't been re-seen by the matchup-refresh loop in `maxAge`.
// Pinnacle removes finished matches from `/sports/{id}/matchups` instead
// of marking them isLive=false, so without this sweep the DB accumulates
// stale `last_is_live=true` rows after every match completes.
func (p *PinnacleStore) SweepStaleLiveFlags(ctx context.Context, sportID int, maxAge time.Duration) (int64, error) {
	res, err := p.db.ExecContext(ctx, `
        UPDATE pinnacle_matchups m
           SET last_is_live = false
          FROM pinnacle_leagues l
         WHERE m.league_id = l.id
           AND l.sport_id = $1
           AND m.last_is_live = true
           AND m.last_seen_at < now() - ($2::text)::interval
    `, sportID, fmt.Sprintf("%d seconds", int(maxAge.Seconds())))
	if err != nil {
		return 0, fmt.Errorf("sweep stale live flags sport=%d: %w", sportID, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// StartRun creates a pinnacle_collector_runs row in 'running' status and
// returns its id. Telemetry is best-effort; a failure to insert returns the
// error but callers should not block on it.
func (p *PinnacleStore) StartRun(ctx context.Context, sportID int) (int64, error) {
	var id int64
	err := p.db.QueryRowContext(ctx, `
        INSERT INTO pinnacle_collector_runs (sport_id, status)
        VALUES ($1, 'running')
        RETURNING id
    `, sportID).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("start pinnacle_collector_runs: %w", err)
	}
	return id, nil
}

// FinishRun closes out the run row with summary counters and final status.
func (p *PinnacleStore) FinishRun(ctx context.Context, runID int64, s pinnacle.RunSummary) error {
	_, err := p.db.ExecContext(ctx, `
        UPDATE pinnacle_collector_runs SET
            finished_at = now(),
            leagues_seen = $2,
            matchups_seen = $3,
            markets_seen = $4,
            markets_inserted = $5,
            markets_unchanged = $6,
            http_errors = $7,
            status = $8,
            error_message = $9
        WHERE id = $1
    `, runID, s.LeaguesSeen, s.MatchupsSeen, s.MarketsSeen, s.MarketsInserted,
		s.MarketsUnchanged, s.HTTPErrors, s.Status, s.ErrorMessage)
	if err != nil {
		return fmt.Errorf("finish pinnacle_collector_runs: %w", err)
	}
	return nil
}

func nullableInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}
