package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

const upsertPolymarketEventSQL = `
        INSERT INTO polymarket_events (
            polymarket_event_id, slug, title, description,
            category, start_time, end_time, active,
            closed, archived, gamma_payload, updated_at
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10, $11::jsonb, now()
        )
        ON CONFLICT (polymarket_event_id)
        DO UPDATE SET
            slug = EXCLUDED.slug,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            category = EXCLUDED.category,
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            active = EXCLUDED.active,
            closed = EXCLUDED.closed,
            archived = EXCLUDED.archived,
            gamma_payload = EXCLUDED.gamma_payload,
            discovered_at = now(),
            updated_at = now()
    `

const upsertPolymarketMarketSQL = `
        INSERT INTO polymarket_markets (
            polymarket_market_id, polymarket_event_id, condition_id,
            question, slug, market_type, active,
            closed, archived, end_time, gamma_payload,
            clob_payload, updated_at
        ) VALUES (
            $1, $2, $3,
            $4, $5, $6, $7,
            $8, $9, $10, $11::jsonb,
            $12::jsonb, now()
        )
        ON CONFLICT (condition_id)
        DO UPDATE SET
            polymarket_market_id = EXCLUDED.polymarket_market_id,
            polymarket_event_id = EXCLUDED.polymarket_event_id,
            question = EXCLUDED.question,
            slug = EXCLUDED.slug,
            market_type = EXCLUDED.market_type,
            active = EXCLUDED.active,
            closed = EXCLUDED.closed,
            archived = EXCLUDED.archived,
            end_time = EXCLUDED.end_time,
            gamma_payload = EXCLUDED.gamma_payload,
            clob_payload = EXCLUDED.clob_payload,
            discovered_at = now(),
            updated_at = now()
    `

const upsertPolymarketTokenSQL = `
        INSERT INTO polymarket_tokens (
            token_id, condition_id, polymarket_market_id,
            outcome_name, outcome_index, side, clob_payload,
            updated_at
        ) VALUES (
            $1, $2, $3,
            $4, $5, $6, $7::jsonb,
            now()
        )
        ON CONFLICT (token_id)
        DO UPDATE SET
            condition_id = EXCLUDED.condition_id,
            polymarket_market_id = EXCLUDED.polymarket_market_id,
            outcome_name = EXCLUDED.outcome_name,
            outcome_index = EXCLUDED.outcome_index,
            side = EXCLUDED.side,
            clob_payload = EXCLUDED.clob_payload,
            updated_at = now()
    `

const insertPolymarketPriceSnapshotSQL = `
        INSERT INTO polymarket_price_snapshots (
            token_id, condition_id, polymarket_market_id,
            bid_price, ask_price, mid_price, last_price,
            best_bid_size, best_ask_size, source,
            received_at, clob_payload, best_bid, best_ask,
            midpoint, spread, last_trade_price, liquidity,
            depth_summary, source_timestamp, raw_payload
        ) VALUES (
            $1, $2, $3,
            $4, $5, $6, $7,
            $8, $9, $10,
            $11, $12::jsonb, $13, $14,
            $15, $16, $17, $18,
            $19::jsonb, $20, $21::jsonb
        )
    `

const insertPolymarketSyncRunSQL = `
        INSERT INTO polymarket_sync_runs (
            started_at, finished_at, sync_type, status,
            pages_fetched, events_seen, markets_seen, tokens_seen,
            prices_fetched, errors_count, error_message
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10, $11
        )
    `

func OpenPolymarket(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) UpsertPolymarketEvents(ctx context.Context, events []model.PolymarketEvent) (int, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	count := 0
	for _, event := range events {
		eventID := strings.TrimSpace(event.PolymarketEventID)
		if eventID == "" {
			return count, fmt.Errorf("polymarket event id is required")
		}
		payload, err := jsonPayload(event.GammaPayload)
		if err != nil {
			return count, fmt.Errorf("polymarket event %s payload: %w", eventID, err)
		}
		if _, err := tx.ExecContext(ctx, upsertPolymarketEventSQL,
			eventID,
			event.Slug,
			event.Title,
			event.Description,
			event.Category,
			nullableTime(event.StartTime),
			nullableTime(event.EndTime),
			nullableBool(event.Active),
			nullableBool(event.Closed),
			nullableBool(event.Archived),
			payload,
		); err != nil {
			return count, fmt.Errorf("upsert polymarket event %s: %w", eventID, err)
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return count, err
	}
	return count, nil
}

func (s *PostgresStore) UpsertPolymarketMarkets(ctx context.Context, markets []model.PolymarketMarket) (int, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	count := 0
	for _, market := range markets {
		marketID := strings.TrimSpace(market.PolymarketMarketID)
		conditionID := strings.TrimSpace(market.ConditionID)
		if marketID == "" {
			return count, fmt.Errorf("polymarket market id is required")
		}
		if conditionID == "" {
			return count, fmt.Errorf("polymarket condition id is required")
		}
		gammaPayload, err := jsonPayload(market.GammaPayload)
		if err != nil {
			return count, fmt.Errorf("polymarket market %s gamma payload: %w", marketID, err)
		}
		clobPayload, err := jsonPayload(market.CLOBPayload)
		if err != nil {
			return count, fmt.Errorf("polymarket market %s clob payload: %w", marketID, err)
		}
		if _, err := tx.ExecContext(ctx, upsertPolymarketMarketSQL,
			marketID,
			nullableString(market.PolymarketEventID),
			conditionID,
			market.Question,
			market.Slug,
			market.MarketType,
			nullableBool(market.Active),
			nullableBool(market.Closed),
			nullableBool(market.Archived),
			nullableTime(market.EndTime),
			gammaPayload,
			clobPayload,
		); err != nil {
			return count, fmt.Errorf("upsert polymarket market %s: %w", marketID, err)
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return count, err
	}
	return count, nil
}

func (s *PostgresStore) UpsertPolymarketTokens(ctx context.Context, tokens []model.PolymarketToken) (int, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	count := 0
	for _, token := range tokens {
		tokenID := strings.TrimSpace(token.TokenID)
		conditionID := strings.TrimSpace(token.ConditionID)
		if tokenID == "" {
			return count, fmt.Errorf("polymarket token id is required")
		}
		if conditionID == "" {
			return count, fmt.Errorf("polymarket token condition id is required")
		}
		payload, err := jsonPayload(token.CLOBPayload)
		if err != nil {
			return count, fmt.Errorf("polymarket token %s payload: %w", tokenID, err)
		}
		if _, err := tx.ExecContext(ctx, upsertPolymarketTokenSQL,
			tokenID,
			conditionID,
			nullableString(token.PolymarketMarketID),
			token.OutcomeName,
			nullableIntValue(token.OutcomeIndex),
			token.Side,
			payload,
		); err != nil {
			return count, fmt.Errorf("upsert polymarket token %s: %w", tokenID, err)
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return count, err
	}
	return count, nil
}

func (s *PostgresStore) InsertPolymarketPriceSnapshots(ctx context.Context, snapshots []model.PolymarketPriceSnapshot) (int, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	count := 0
	for _, snapshot := range snapshots {
		tokenID := strings.TrimSpace(snapshot.TokenID)
		conditionID := strings.TrimSpace(snapshot.ConditionID)
		if tokenID == "" {
			return count, fmt.Errorf("polymarket price snapshot token id is required")
		}
		if conditionID == "" {
			return count, fmt.Errorf("polymarket price snapshot condition id is required")
		}
		source := strings.TrimSpace(snapshot.Source)
		if source == "" {
			source = model.SourcePolymarket
		}
		receivedAt := snapshot.ReceivedAt
		if receivedAt.IsZero() {
			receivedAt = time.Now().UTC()
		}
		payload, err := jsonPayload(snapshot.CLOBPayload)
		if err != nil {
			return count, fmt.Errorf("polymarket price snapshot %s payload: %w", tokenID, err)
		}
		depthSummary, err := jsonPayload(snapshot.DepthSummary)
		if err != nil {
			return count, fmt.Errorf("polymarket price snapshot %s depth summary: %w", tokenID, err)
		}
		rawPayload := snapshot.RawPayload
		if len(rawPayload) == 0 {
			rawPayload = snapshot.CLOBPayload
		}
		rawPayloadJSON, err := jsonPayload(rawPayload)
		if err != nil {
			return count, fmt.Errorf("polymarket price snapshot %s raw payload: %w", tokenID, err)
		}
		if _, err := tx.ExecContext(ctx, insertPolymarketPriceSnapshotSQL,
			tokenID,
			conditionID,
			nullableString(snapshot.PolymarketMarketID),
			nullableFloat(snapshot.BidPrice),
			nullableFloat(snapshot.AskPrice),
			nullableFloat(snapshot.MidPrice),
			nullableFloat(snapshot.LastPrice),
			nullableFloat(snapshot.BestBidSize),
			nullableFloat(snapshot.BestAskSize),
			source,
			receivedAt.UTC(),
			payload,
			nullableFloat(snapshot.BestBid),
			nullableFloat(snapshot.BestAsk),
			nullableFloat(snapshot.Midpoint),
			nullableFloat(snapshot.Spread),
			nullableFloat(snapshot.LastTradePrice),
			nullableFloat(snapshot.Liquidity),
			depthSummary,
			nullableTime(snapshot.SourceTimestamp),
			rawPayloadJSON,
		); err != nil {
			return count, fmt.Errorf("insert polymarket price snapshot %s: %w", tokenID, err)
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return count, err
	}
	return count, nil
}

func (s *PostgresStore) ListPolymarketPriceTokens(ctx context.Context, configuredTokenIDs []string, limit int, activeMarketsOnly bool, endGrace time.Duration) ([]model.PolymarketToken, error) {
	if limit <= 0 {
		return nil, nil
	}

	configured := map[string]struct{}{}
	for _, tokenID := range configuredTokenIDs {
		tokenID = strings.TrimSpace(tokenID)
		if tokenID != "" {
			configured[tokenID] = struct{}{}
		}
	}

	args := []any{}
	clauses := []string{"pt.token_id <> ''"}
	if activeMarketsOnly {
		clauses = append(clauses,
			"pm.active IS DISTINCT FROM false",
			"pm.closed IS DISTINCT FROM true",
			"pm.archived IS DISTINCT FROM true",
		)
		if endGrace > 0 {
			args = append(args, time.Now().UTC().Add(-endGrace))
			clauses = append(clauses, fmt.Sprintf("(pm.end_time IS NULL OR pm.end_time >= $%d)", len(args)))
		}
	}
	if len(configured) > 0 {
		placeholders := make([]string, 0, len(configured))
		for tokenID := range configured {
			args = append(args, tokenID)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		clauses = append(clauses, "pt.token_id IN ("+strings.Join(placeholders, ", ")+")")
	}
	args = append(args, limit)

	// Tokens that appear in priority_polymarket_tokens — confirmed 2-way
	// aliases plus 3-way soccer YES tokens for matched events — are the
	// ones that actually feed the edge views. Without prioritizing them
	// we blow the per-tick budget on the 30k+ long-tail and leave the
	// mapped tokens stale.
	//
	// polymarket_dead_tokens excludes orderbook-less tokens for the
	// duration of their backoff window so we don't waste price budget
	// re-asking a 404 every tick.
	query := fmt.Sprintf(`
        SELECT
            pt.token_id,
            pt.condition_id,
            coalesce(pt.polymarket_market_id, '') AS polymarket_market_id,
            pt.outcome_name,
            pt.outcome_index,
            pt.side,
            pt.clob_payload
        FROM polymarket_tokens pt
        LEFT JOIN polymarket_markets pm
          ON pm.condition_id = pt.condition_id
        LEFT JOIN priority_polymarket_tokens ppt
          ON ppt.token_id = pt.token_id
        LEFT JOIN polymarket_dead_tokens dead
          ON dead.token_id = pt.token_id
         AND dead.skip_until > now()
        WHERE %s
          AND dead.token_id IS NULL
        ORDER BY (ppt.token_id IS NOT NULL) DESC,
                 COALESCE(ppt.live_priority, 0) DESC,
                 pt.updated_at DESC, pt.token_id
        LIMIT $%d
    `, strings.Join(clauses, " AND "), len(args))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list polymarket price tokens: %w", err)
	}
	defer func() { _ = rows.Close() }()

	tokens := []model.PolymarketToken{}
	for rows.Next() {
		var token model.PolymarketToken
		var outcomeIndex sql.NullInt64
		if err := rows.Scan(
			&token.TokenID,
			&token.ConditionID,
			&token.PolymarketMarketID,
			&token.OutcomeName,
			&outcomeIndex,
			&token.Side,
			&token.CLOBPayload,
		); err != nil {
			return nil, fmt.Errorf("scan polymarket price token: %w", err)
		}
		if outcomeIndex.Valid {
			v := int(outcomeIndex.Int64)
			token.OutcomeIndex = &v
		}
		tokens = append(tokens, token)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list polymarket price tokens: %w", err)
	}
	return tokens, nil
}

// MarkDeadToken records a 404 (or repeated failure) for tokenID. Each
// consecutive 404 backs the skip window off exponentially up to a 24h
// cap so a sometimes-flapping orderbook doesn't get permanently
// blacklisted. The first 404 alone is enough to skip for an hour —
// orderbook recovery within an hour would be unusual.
func (s *PostgresStore) MarkDeadToken(ctx context.Context, tokenID, errMsg string) error {
	if strings.TrimSpace(tokenID) == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO polymarket_dead_tokens (token_id, first_seen_at, last_seen_at,
                                            consecutive_404s, skip_until, last_error)
        VALUES ($1, now(), now(), 1, now() + interval '1 hour', $2)
        ON CONFLICT (token_id) DO UPDATE SET
            last_seen_at     = now(),
            consecutive_404s = polymarket_dead_tokens.consecutive_404s + 1,
            skip_until       = now() + (
                CASE
                    WHEN polymarket_dead_tokens.consecutive_404s + 1 >= 5 THEN interval '24 hours'
                    WHEN polymarket_dead_tokens.consecutive_404s + 1 >= 3 THEN interval '6 hours'
                    ELSE interval '1 hour'
                END
            ),
            last_error       = EXCLUDED.last_error
    `, tokenID, truncateStr(errMsg, 240))
	if err != nil {
		return fmt.Errorf("store: MarkDeadToken: %w", err)
	}
	return nil
}

// ClearDeadToken zeroes the dead-token counter when a fetch succeeds.
// Called from the success branch of the price-snapshot loop. Cheap to
// call even when the token wasn't dead — UPDATE with 0 rows affected.
func (s *PostgresStore) ClearDeadToken(ctx context.Context, tokenID string) error {
	if strings.TrimSpace(tokenID) == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
        DELETE FROM polymarket_dead_tokens WHERE token_id = $1
    `, tokenID)
	if err != nil {
		return fmt.Errorf("store: ClearDeadToken: %w", err)
	}
	return nil
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func (s *PostgresStore) InsertPolymarketSyncRun(ctx context.Context, run model.PolymarketSyncRun) error {
	_, err := s.db.ExecContext(ctx, insertPolymarketSyncRunSQL,
		run.StartedAt.UTC(),
		run.FinishedAt.UTC(),
		run.SyncType,
		run.Status,
		run.PagesFetched,
		run.EventsSeen,
		run.MarketsSeen,
		run.TokensSeen,
		run.PricesFetched,
		run.ErrorsCount,
		run.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("insert polymarket sync run: %w", err)
	}
	return nil
}

func nullableString(v string) sql.NullString {
	v = strings.TrimSpace(v)
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}

func nullableBool(v *bool) sql.NullBool {
	if v == nil {
		return sql.NullBool{}
	}
	return sql.NullBool{Bool: *v, Valid: true}
}

func nullableIntValue(v *int) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*v), Valid: true}
}

func jsonPayload(raw json.RawMessage) (string, error) {
	raw = json.RawMessage(strings.TrimSpace(string(raw)))
	if len(raw) == 0 {
		return "{}", nil
	}
	if !json.Valid(raw) {
		return "", fmt.Errorf("invalid json")
	}
	return string(raw), nil
}
