package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

const insertBetfairRawStreamMessageSQL = `
        INSERT INTO betfair_raw_stream_messages (
            source, stream_id, op, market_id,
            publish_time_ms, source_publish_time, change_type,
            clk, initial_clk, raw_json, payload_sha256,
            received_at
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7,
            $8, $9, $10::jsonb, $11,
            $12
        )
    `

const insertBetfairExchangeQuoteHistorySQL = `
        INSERT INTO betfair_exchange_quote_history (
            source, quote_key, quote_hash,
            market_id, selection_id, handicap,
            runner_name, market_name, event_id, event_name,
            market_family, exchange_side, price_decimal, available_size,
            last_traded_price, traded_volume, market_status, in_play,
            market_time, source_publish_time_ms, source_publish_time,
            reducer_updated_at, received_at, age_ms, stale_state,
            confidence_score, raw_metadata
        ) VALUES (
            $1, $2, $3,
            $4, $5, $6,
            $7, $8, $9, $10,
            $11, $12, $13, $14,
            $15, $16, $17, $18,
            $19, $20, $21,
            $22, $23, $24, $25,
            $26, $27::jsonb
        )
        RETURNING id
    `

const upsertBetfairLatestExchangeQuoteSQL = `
        INSERT INTO betfair_latest_exchange_quotes (quote_key, history_id, quote_hash, updated_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (quote_key)
        DO UPDATE SET
            history_id = EXCLUDED.history_id,
            quote_hash = EXCLUDED.quote_hash,
            updated_at = now()
    `

func (s *PostgresStore) SaveBetfairRawMessages(ctx context.Context, messages []model.BetfairRawStreamMessage) (model.BetfairSaveStats, error) {
	stats := model.BetfairSaveStats{}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return stats, err
	}
	defer func() { _ = tx.Rollback() }()

	inserted, err := insertBetfairRawMessages(ctx, tx, messages)
	if err != nil {
		return stats, err
	}
	stats.RawMessagesInserted = inserted

	if err := tx.Commit(); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *PostgresStore) SaveBetfairQuoteCandidates(ctx context.Context, candidates []model.BetfairExchangeQuoteCandidate) (model.BetfairSaveStats, error) {
	stats := model.BetfairSaveStats{}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return stats, err
	}
	defer func() { _ = tx.Rollback() }()

	quoteStats, err := saveBetfairQuoteCandidates(ctx, tx, candidates)
	if err != nil {
		return stats, err
	}
	stats.CandidatesSeen = quoteStats.CandidatesSeen
	stats.CandidatesInserted = quoteStats.CandidatesInserted
	stats.CandidatesUnchanged = quoteStats.CandidatesUnchanged
	stats.CandidatesSkipped = quoteStats.CandidatesSkipped

	if err := tx.Commit(); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *PostgresStore) SaveBetfairBatch(ctx context.Context, messages []model.BetfairRawStreamMessage, candidates []model.BetfairExchangeQuoteCandidate) (model.BetfairSaveStats, error) {
	stats := model.BetfairSaveStats{}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return stats, err
	}
	defer func() { _ = tx.Rollback() }()

	inserted, err := insertBetfairRawMessages(ctx, tx, messages)
	if err != nil {
		return stats, err
	}
	stats.RawMessagesInserted = inserted

	quoteStats, err := saveBetfairQuoteCandidates(ctx, tx, candidates)
	if err != nil {
		return stats, err
	}
	stats.CandidatesSeen = quoteStats.CandidatesSeen
	stats.CandidatesInserted = quoteStats.CandidatesInserted
	stats.CandidatesUnchanged = quoteStats.CandidatesUnchanged
	stats.CandidatesSkipped = quoteStats.CandidatesSkipped

	if err := tx.Commit(); err != nil {
		return stats, err
	}
	return stats, nil
}

func insertBetfairRawMessages(ctx context.Context, tx *sql.Tx, messages []model.BetfairRawStreamMessage) (int, error) {
	inserted := 0
	for i, message := range messages {
		if err := insertBetfairRawMessage(ctx, tx, message); err != nil {
			return inserted, fmt.Errorf("insert betfair raw stream message %d: %w", i, err)
		}
		inserted++
	}
	return inserted, nil
}

func insertBetfairRawMessage(ctx context.Context, tx *sql.Tx, message model.BetfairRawStreamMessage) error {
	source := betfairSource(message.Source)
	rawJSON, err := jsonPayload(message.RawJSON)
	if err != nil {
		return fmt.Errorf("raw json: %w", err)
	}
	payloadHash := strings.TrimSpace(message.PayloadSHA256)
	if payloadHash == "" {
		payloadHash = betfairPayloadHash(rawJSON)
	}
	receivedAt := message.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}

	_, err = tx.ExecContext(ctx, insertBetfairRawStreamMessageSQL,
		source,
		strings.TrimSpace(message.StreamID),
		strings.TrimSpace(message.Op),
		strings.TrimSpace(message.MarketID),
		nullableInt(message.PublishTimeMS),
		betfairSourcePublishTime(message.PublishTimeMS, message.SourcePublishTime),
		strings.TrimSpace(message.ChangeType),
		strings.TrimSpace(message.Clock),
		strings.TrimSpace(message.InitialClock),
		rawJSON,
		payloadHash,
		receivedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("insert betfair raw stream message: %w", err)
	}
	return nil
}

func saveBetfairQuoteCandidates(ctx context.Context, tx *sql.Tx, candidates []model.BetfairExchangeQuoteCandidate) (model.BetfairSaveStats, error) {
	stats := model.BetfairSaveStats{}
	for _, candidate := range candidates {
		stats.CandidatesSeen++
		normalized, ok := normalizeBetfairQuoteCandidate(candidate)
		if !ok {
			stats.CandidatesSkipped++
			continue
		}

		inserted, err := insertBetfairQuoteCandidateIfChanged(ctx, tx, normalized)
		if err != nil {
			return stats, err
		}
		if inserted {
			stats.CandidatesInserted++
		} else {
			stats.CandidatesUnchanged++
		}
	}
	return stats, nil
}

func insertBetfairQuoteCandidateIfChanged(ctx context.Context, tx *sql.Tx, candidate model.BetfairExchangeQuoteCandidate) (bool, error) {
	var existingHash string
	err := tx.QueryRowContext(ctx, `
        SELECT quote_hash
        FROM betfair_latest_exchange_quotes
        WHERE quote_key = $1
    `, candidate.QuoteKey).Scan(&existingHash)
	if err == nil && existingHash == candidate.QuoteHash {
		return false, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return false, fmt.Errorf("select latest betfair exchange quote %s: %w", candidate.QuoteKey, err)
	}

	rawMetadata, err := jsonPayload(candidate.RawMetadata)
	if err != nil {
		return false, fmt.Errorf("betfair candidate %s raw metadata: %w", candidate.QuoteKey, err)
	}

	var historyID int64
	err = tx.QueryRowContext(ctx, insertBetfairExchangeQuoteHistorySQL,
		betfairSource(candidate.Source),
		candidate.QuoteKey,
		candidate.QuoteHash,
		candidate.MarketID,
		candidate.SelectionID,
		candidate.Handicap,
		candidate.RunnerName,
		candidate.MarketName,
		candidate.EventID,
		candidate.EventName,
		candidate.MarketFamily,
		candidate.ExchangeSide,
		candidate.PriceDecimal,
		candidate.AvailableSize,
		nullableFloat(candidate.LastTradedPrice),
		nullableFloat(candidate.TradedVolume),
		candidate.MarketStatus,
		nullableBool(candidate.InPlay),
		candidate.MarketTime,
		nullableInt(candidate.SourcePublishTimeMS),
		nullableTime(candidate.SourcePublishTime),
		nullableTime(candidate.ReducerUpdatedAt),
		candidate.ReceivedAt.UTC(),
		nullableInt(candidate.AgeMS),
		candidate.StaleState,
		candidate.ConfidenceScore,
		rawMetadata,
	).Scan(&historyID)
	if err != nil {
		return false, fmt.Errorf("insert betfair exchange quote history %s: %w", candidate.QuoteKey, err)
	}

	_, err = tx.ExecContext(ctx, upsertBetfairLatestExchangeQuoteSQL,
		candidate.QuoteKey,
		historyID,
		candidate.QuoteHash,
	)
	if err != nil {
		return false, fmt.Errorf("upsert latest betfair exchange quote %s: %w", candidate.QuoteKey, err)
	}
	return true, nil
}

func normalizeBetfairQuoteCandidate(candidate model.BetfairExchangeQuoteCandidate) (model.BetfairExchangeQuoteCandidate, bool) {
	candidate.Source = betfairSource(candidate.Source)
	candidate.QuoteKey = strings.TrimSpace(candidate.QuoteKey)
	candidate.QuoteHash = strings.TrimSpace(candidate.QuoteHash)
	candidate.MarketID = strings.TrimSpace(candidate.MarketID)
	candidate.ExchangeSide = strings.ToLower(strings.TrimSpace(candidate.ExchangeSide))
	candidate.StaleState = strings.ToLower(strings.TrimSpace(candidate.StaleState))
	if candidate.StaleState == "" {
		candidate.StaleState = "unknown"
	}
	if candidate.ReceivedAt.IsZero() {
		candidate.ReceivedAt = time.Now().UTC()
	}
	candidate.ReceivedAt = candidate.ReceivedAt.UTC()
	candidate.RunnerName = strings.TrimSpace(candidate.RunnerName)
	candidate.MarketName = strings.TrimSpace(candidate.MarketName)
	candidate.EventID = strings.TrimSpace(candidate.EventID)
	candidate.EventName = strings.TrimSpace(candidate.EventName)
	candidate.MarketFamily = strings.TrimSpace(candidate.MarketFamily)
	candidate.MarketStatus = strings.TrimSpace(candidate.MarketStatus)
	candidate.MarketTime = strings.TrimSpace(candidate.MarketTime)
	if len(candidate.RawMetadata) == 0 {
		candidate.RawMetadata = betfairQuoteMetadata(candidate)
	}

	if !validBetfairQuoteCandidate(candidate) {
		return candidate, false
	}
	return candidate, true
}

func validBetfairQuoteCandidate(candidate model.BetfairExchangeQuoteCandidate) bool {
	if candidate.MarketID == "" {
		return false
	}
	if candidate.SelectionID == 0 {
		return false
	}
	if candidate.ExchangeSide != "back" && candidate.ExchangeSide != "lay" {
		return false
	}
	if !validFinite(candidate.Handicap) {
		return false
	}
	if !validFinite(candidate.PriceDecimal) || candidate.PriceDecimal <= 1 {
		return false
	}
	if !validFinite(candidate.AvailableSize) || candidate.AvailableSize <= 0 {
		return false
	}
	if !validOptionalFinite(candidate.LastTradedPrice) || !validOptionalFinite(candidate.TradedVolume) {
		return false
	}
	if !validOptionalNonNegativeInt(candidate.SourcePublishTimeMS) || !validOptionalNonNegativeInt(candidate.AgeMS) {
		return false
	}
	if !validFinite(candidate.ConfidenceScore) || candidate.ConfidenceScore < 0 || candidate.ConfidenceScore > 100 {
		return false
	}
	if candidate.QuoteKey == "" || candidate.QuoteHash == "" {
		return false
	}
	switch candidate.StaleState {
	case "fresh", "aging", "stale", "unknown":
		return true
	default:
		return false
	}
}

func betfairSource(source string) string {
	source = strings.TrimSpace(source)
	if source == "" {
		return model.SourceBetfair
	}
	return model.SourceBetfair
}

func betfairPayloadHash(payload string) string {
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func betfairSourcePublishTime(publishTimeMS *int64, sourcePublishTime *time.Time) sql.NullTime {
	if sourcePublishTime != nil {
		return nullableTime(sourcePublishTime)
	}
	if publishTimeMS == nil {
		return sql.NullTime{}
	}
	if *publishTimeMS < 0 {
		return sql.NullTime{}
	}
	ts := time.UnixMilli(*publishTimeMS).UTC()
	return sql.NullTime{Time: ts, Valid: true}
}

func validFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func validOptionalFinite(v *float64) bool {
	return v == nil || validFinite(*v)
}

func validOptionalNonNegativeInt(v *int64) bool {
	return v == nil || *v >= 0
}

func betfairQuoteMetadata(candidate model.BetfairExchangeQuoteCandidate) json.RawMessage {
	raw, err := json.Marshal(map[string]any{
		"source":                 betfairSource(candidate.Source),
		"market_id":              strings.TrimSpace(candidate.MarketID),
		"selection_id":           candidate.SelectionID,
		"handicap":               candidate.Handicap,
		"exchange_side":          strings.ToLower(strings.TrimSpace(candidate.ExchangeSide)),
		"last_traded_price":      candidate.LastTradedPrice,
		"traded_volume":          candidate.TradedVolume,
		"source_publish_time_ms": candidate.SourcePublishTimeMS,
		"age_ms":                 candidate.AgeMS,
	})
	if err != nil {
		return nil
	}
	return raw
}
