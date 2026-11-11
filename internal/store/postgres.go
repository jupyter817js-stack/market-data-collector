package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

type PostgresStore struct {
	db           *sql.DB
	prematchOnly bool
	freshness    normalize.FreshnessPolicy
}

func Open(ctx context.Context, databaseURL string, prematchOnly bool, freshness normalize.FreshnessPolicy) (*PostgresStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &PostgresStore{db: db, prematchOnly: prematchOnly, freshness: freshness}, nil
}

func (s *PostgresStore) Close() error { return s.db.Close() }

func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// CurrentDatabaseName returns the result of SELECT current_database() against
// the store's connection. Useful when callers suspect their DATABASE_URL
// landed on the wrong database — e.g., the snapshot capture daemon reporting
// "table does not exist" when the migration was just applied.
func CurrentDatabaseName(ctx context.Context, s *PostgresStore) (string, error) {
	var name string
	if err := s.db.QueryRowContext(ctx, "SELECT current_database()").Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

func (s *PostgresStore) InsertSourceRun(ctx context.Context, run model.SourceRun) error {
	headers := run.ResponseHeaders
	if headers == nil {
		headers = map[string]string{}
	}
	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
        INSERT INTO source_runs (
            source, sport_key, started_at, finished_at,
            status, status_code, request_url, response_headers,
            error_message, events_seen, events_skipped_live,
            quotes_seen, quotes_inserted, quotes_unchanged,
            raw_snapshots_inserted
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8::jsonb,
            $9, $10, $11,
            $12, $13, $14,
            $15
        )
    `,
		run.Source,
		run.SportKey,
		run.StartedAt,
		run.FinishedAt,
		run.Status,
		nullableStatusCode(run.StatusCode),
		run.RequestURL,
		string(headersJSON),
		run.ErrorMessage,
		run.Stats.EventsSeen,
		run.Stats.EventsSkippedLive,
		run.Stats.QuotesSeen,
		run.Stats.QuotesInserted,
		run.Stats.QuotesUnchanged,
		run.Stats.RawSnapshotsInserted,
	)
	if err != nil {
		return fmt.Errorf("insert source run: %w", err)
	}
	return nil
}

func (s *PostgresStore) SaveBatch(ctx context.Context, batch model.FetchBatch) (model.SaveStats, error) {
	stats := model.SaveStats{}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return stats, err
	}
	defer func() { _ = tx.Rollback() }()

	rawSnapshotID, err := insertRawSnapshot(ctx, tx, batch)
	if err != nil {
		return stats, err
	}
	stats.RawSnapshotsInserted = 1

	for _, event := range batch.Events {
		stats.EventsSeen++
		if s.prematchOnly && event.CommenceTime.Before(batch.ReceivedAt) {
			stats.EventsSkippedLive++
			continue
		}

		quotes := normalize.EventToQuotes(event, batch.Source, batch.ReceivedAt, s.freshness)
		if len(quotes) == 0 {
			continue
		}

		first := quotes[0]
		canonicalID, err := upsertCanonicalEvent(ctx, tx, first)
		if err != nil {
			return stats, err
		}
		if err := upsertSourceAlias(ctx, tx, first, canonicalID); err != nil {
			return stats, err
		}

		for _, q := range quotes {
			stats.QuotesSeen++
			inserted, err := insertQuoteIfChanged(ctx, tx, canonicalID, rawSnapshotID, q)
			if err != nil {
				return stats, err
			}
			if inserted {
				stats.QuotesInserted++
			} else {
				stats.QuotesUnchanged++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return stats, err
	}
	return stats, nil
}

func insertRawSnapshot(ctx context.Context, tx *sql.Tx, batch model.FetchBatch) (int64, error) {
	headers, err := json.Marshal(batch.Headers)
	if err != nil {
		return 0, err
	}
	payload := batch.Payload
	if !json.Valid(payload) {
		payload, err = json.Marshal(map[string]string{"raw": string(batch.Payload)})
		if err != nil {
			return 0, err
		}
	}
	var id int64
	err = tx.QueryRowContext(ctx, `
        INSERT INTO raw_snapshots (source, request_url, received_at, status_code, headers, payload, payload_hash)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)
        RETURNING id
    `, batch.Source, batch.RequestURL, batch.ReceivedAt, batch.StatusCode, string(headers), string(payload), batch.PayloadSHA).Scan(&id)
	return id, err
}

func upsertCanonicalEvent(ctx context.Context, tx *sql.Tx, q normalize.Quote) (string, error) {
	league := strings.TrimSpace(q.League)
	if league == "" {
		league = q.SportKey
	}

	var id string
	err := tx.QueryRowContext(ctx, `
        INSERT INTO canonical_events (
            sport_key, league, home_team, away_team, home_team_norm, away_team_norm, commence_time, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        ON CONFLICT (sport_key, home_team_norm, away_team_norm, commence_time)
        DO UPDATE SET
            league = EXCLUDED.league,
            home_team = EXCLUDED.home_team,
            away_team = EXCLUDED.away_team,
            updated_at = now()
        RETURNING id
    `, q.SportKey, league, q.HomeTeam, q.AwayTeam, q.HomeTeamNorm, q.AwayTeamNorm, q.CommenceTime).Scan(&id)
	return id, err
}

func upsertSourceAlias(ctx context.Context, tx *sql.Tx, q normalize.Quote, canonicalID string) error {
	_, err := tx.ExecContext(ctx, `
        INSERT INTO source_event_aliases (
            source, source_event_id, canonical_event_id, sport_key, raw_home_team, raw_away_team, raw_commence_time, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        ON CONFLICT (source, source_event_id)
        DO UPDATE SET
            canonical_event_id = EXCLUDED.canonical_event_id,
            sport_key = EXCLUDED.sport_key,
            raw_home_team = EXCLUDED.raw_home_team,
            raw_away_team = EXCLUDED.raw_away_team,
            raw_commence_time = EXCLUDED.raw_commence_time,
            updated_at = now()
    `, q.Source, q.SourceEventID, canonicalID, q.SportKey, q.HomeTeam, q.AwayTeam, q.CommenceTime)
	return err
}

func insertQuoteIfChanged(ctx context.Context, tx *sql.Tx, canonicalID string, rawSnapshotID int64, q normalize.Quote) (bool, error) {
	var existingHash string
	err := tx.QueryRowContext(ctx, `SELECT quote_hash FROM latest_quotes WHERE quote_key = $1`, q.QuoteKey).Scan(&existingHash)
	if err == nil && existingHash == q.QuoteHash {
		return false, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	var historyID int64
	err = tx.QueryRowContext(ctx, `
        INSERT INTO quote_history (
            canonical_event_id, source, source_event_id,
            bookmaker_key, bookmaker_title, market_key,
            outcome_name, outcome_side, line, price_decimal,
            source_last_update, received_at, in_play,
            age_ms, stale_state, confidence_score,
            quote_key, quote_hash, raw_snapshot_id
        ) VALUES (
            $1, $2, $3,
            $4, $5, $6,
            $7, $8, $9, $10,
            $11, $12, $13,
            $14, $15, $16,
            $17, $18, $19
        )
        RETURNING id
    `,
		canonicalID, q.Source, q.SourceEventID,
		q.BookmakerKey, q.BookmakerTitle, q.MarketKey,
		q.OutcomeName, q.OutcomeSide, nullableFloat(q.Line), q.PriceDecimal,
		nullableTime(q.SourceLastUpdate), q.ReceivedAt, q.InPlay,
		nullableInt(q.AgeMS), q.StaleState, q.ConfidenceScore,
		q.QuoteKey, q.QuoteHash, rawSnapshotID,
	).Scan(&historyID)
	if err != nil {
		return false, fmt.Errorf("insert quote history: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
        INSERT INTO latest_quotes (quote_key, quote_history_id, quote_hash, updated_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (quote_key)
        DO UPDATE SET
            quote_history_id = EXCLUDED.quote_history_id,
            quote_hash = EXCLUDED.quote_hash,
            updated_at = now()
    `, q.QuoteKey, historyID, q.QuoteHash)
	if err != nil {
		return false, fmt.Errorf("upsert latest quote: %w", err)
	}
	return true, nil
}

func nullableFloat(v *float64) sql.NullFloat64 {
	if v == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: *v, Valid: true}
}

func nullableTime(v *time.Time) sql.NullTime {
	if v == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: v.UTC(), Valid: true}
}

func nullableInt(v *int64) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *v, Valid: true}
}

func nullableStatusCode(v *int) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*v), Valid: true}
}
