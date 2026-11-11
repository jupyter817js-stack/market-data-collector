package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/replay"
)

const (
	defaultReplayLimit = 20
	maxReplayLimit     = 500
)

func (s *PostgresStore) ListReplayRawSnapshots(ctx context.Context, filter replay.SnapshotFilter) ([]replay.RawSnapshot, error) {
	if filter.ID > 0 {
		row := s.db.QueryRowContext(ctx, `
            SELECT id, source, request_url, received_at, status_code, payload
            FROM raw_snapshots
            WHERE id = $1
        `, filter.ID)
		snapshot, err := scanReplayRawSnapshot(row)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("raw snapshot %d not found", filter.ID)
			}
			return nil, err
		}
		return []replay.RawSnapshot{snapshot}, nil
	}

	source := strings.TrimSpace(filter.Source)
	if source == "" {
		source = model.SourceOddsAPI
	}

	args := []any{source}
	clauses := []string{"source = $1"}
	if filter.From != nil {
		args = append(args, filter.From.UTC())
		clauses = append(clauses, fmt.Sprintf("received_at >= $%d", len(args)))
	}
	if filter.To != nil {
		args = append(args, filter.To.UTC())
		clauses = append(clauses, fmt.Sprintf("received_at <= $%d", len(args)))
	}
	if sportKey := strings.TrimSpace(filter.SportKey); sportKey != "" {
		args = append(args, "/sports/"+url.PathEscape(sportKey)+"/odds/")
		clauses = append(clauses, fmt.Sprintf("strpos(request_url, $%d) > 0", len(args)))
	}

	args = append(args, boundedReplayLimit(filter.Limit))
	query := fmt.Sprintf(`
        SELECT id, source, request_url, received_at, status_code, payload
        FROM raw_snapshots
        WHERE %s
        ORDER BY received_at DESC, id DESC
        LIMIT $%d
    `, strings.Join(clauses, " AND "), len(args))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list replay raw snapshots: %w", err)
	}
	defer func() { _ = rows.Close() }()

	snapshots := []replay.RawSnapshot{}
	for rows.Next() {
		snapshot, err := scanReplayRawSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list replay raw snapshots: %w", err)
	}
	return snapshots, nil
}

func (s *PostgresStore) ReplayQuoteHistory(ctx context.Context, rawSnapshotID int64) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT quote_key, quote_hash
        FROM quote_history
        WHERE raw_snapshot_id = $1
        ORDER BY quote_key, id DESC
    `, rawSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("list quote history for raw snapshot %d: %w", rawSnapshotID, err)
	}
	defer func() { _ = rows.Close() }()

	out := map[string]string{}
	for rows.Next() {
		var quoteKey, quoteHash string
		if err := rows.Scan(&quoteKey, &quoteHash); err != nil {
			return nil, fmt.Errorf("scan quote history for raw snapshot %d: %w", rawSnapshotID, err)
		}
		if _, exists := out[quoteKey]; !exists {
			out[quoteKey] = quoteHash
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list quote history for raw snapshot %d: %w", rawSnapshotID, err)
	}
	return out, nil
}

func (s *PostgresStore) ReplayLatestQuoteHashes(ctx context.Context, quoteKeys []string) (map[string]string, error) {
	out := map[string]string{}
	for _, quoteKey := range quoteKeys {
		quoteKey = strings.TrimSpace(quoteKey)
		if quoteKey == "" {
			continue
		}
		if _, exists := out[quoteKey]; exists {
			continue
		}

		var quoteHash string
		err := s.db.QueryRowContext(ctx, `
            SELECT quote_hash
            FROM latest_quotes
            WHERE quote_key = $1
        `, quoteKey).Scan(&quoteHash)
		if err == nil {
			out[quoteKey] = quoteHash
			continue
		}
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("lookup latest quote %s: %w", quoteKey, err)
		}
	}
	return out, nil
}

type replayRawSnapshotScanner interface {
	Scan(dest ...any) error
}

func scanReplayRawSnapshot(row replayRawSnapshotScanner) (replay.RawSnapshot, error) {
	var snapshot replay.RawSnapshot
	var payload []byte
	if err := row.Scan(
		&snapshot.ID,
		&snapshot.Source,
		&snapshot.RequestURL,
		&snapshot.ReceivedAt,
		&snapshot.StatusCode,
		&payload,
	); err != nil {
		return snapshot, fmt.Errorf("scan raw snapshot: %w", err)
	}
	if !json.Valid(payload) {
		return snapshot, fmt.Errorf("raw snapshot %d has invalid json payload", snapshot.ID)
	}
	snapshot.Payload = append(snapshot.Payload[:0], payload...)
	snapshot.ReceivedAt = snapshot.ReceivedAt.UTC()
	return snapshot, nil
}

func boundedReplayLimit(limit int) int {
	if limit <= 0 {
		return defaultReplayLimit
	}
	if limit > maxReplayLimit {
		return maxReplayLimit
	}
	return limit
}
