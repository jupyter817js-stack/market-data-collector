package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

func TestDBIntegrationBetfairPersistenceChangeOnly(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	assertRelationExists(ctx, t, db, "betfair_raw_stream_messages", "r")
	assertRelationExists(ctx, t, db, "betfair_exchange_quote_history", "r")
	assertRelationExists(ctx, t, db, "betfair_latest_exchange_quotes", "r")
	assertRelationExists(ctx, t, db, "betfair_latest_exchange_quotes_view", "v")

	store, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	receivedAt := time.Now().UTC().Truncate(time.Second)
	raw := []model.BetfairRawStreamMessage{
		betfairRawFixture("stream-1", receivedAt),
		betfairRawFixture("stream-1", receivedAt.Add(time.Second)),
	}
	rawStats, err := store.SaveBetfairRawMessages(ctx, raw)
	if err != nil {
		t.Fatalf("save betfair raw messages: %v", err)
	}
	if rawStats.RawMessagesInserted != 2 {
		t.Fatalf("raw messages inserted=%d want 2", rawStats.RawMessagesInserted)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_raw_stream_messages WHERE stream_id = 'stream-1'`); got != 2 {
		t.Fatalf("raw message count=%d want 2", got)
	}

	candidate := betfairCandidateFixture("betfair-key-1", "hash-1", 2.1, 20)
	candidate.ReceivedAt = receivedAt
	stats, err := store.SaveBetfairQuoteCandidates(ctx, []model.BetfairExchangeQuoteCandidate{candidate})
	if err != nil {
		t.Fatalf("save first betfair candidate: %v", err)
	}
	if stats.CandidatesSeen != 1 || stats.CandidatesInserted != 1 || stats.CandidatesUnchanged != 0 || stats.CandidatesSkipped != 0 {
		t.Fatalf("first candidate stats=%+v", stats)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history WHERE quote_key = 'betfair-key-1'`); got != 1 {
		t.Fatalf("history count after first insert=%d want 1", got)
	}
	assertBetfairLatest(ctx, t, db, "betfair-key-1", "hash-1", 2.1, 20)

	stats, err = store.SaveBetfairQuoteCandidates(ctx, []model.BetfairExchangeQuoteCandidate{candidate})
	if err != nil {
		t.Fatalf("save unchanged betfair candidate: %v", err)
	}
	if stats.CandidatesSeen != 1 || stats.CandidatesInserted != 0 || stats.CandidatesUnchanged != 1 || stats.CandidatesSkipped != 0 {
		t.Fatalf("unchanged candidate stats=%+v", stats)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history WHERE quote_key = 'betfair-key-1'`); got != 1 {
		t.Fatalf("history count after unchanged insert=%d want 1", got)
	}

	changed := candidate
	changed.QuoteHash = "hash-2"
	changed.PriceDecimal = 2.2
	changed.AvailableSize = 21
	changed.ReceivedAt = receivedAt.Add(2 * time.Second)
	stats, err = store.SaveBetfairQuoteCandidates(ctx, []model.BetfairExchangeQuoteCandidate{changed})
	if err != nil {
		t.Fatalf("save changed betfair candidate: %v", err)
	}
	if stats.CandidatesSeen != 1 || stats.CandidatesInserted != 1 || stats.CandidatesUnchanged != 0 || stats.CandidatesSkipped != 0 {
		t.Fatalf("changed candidate stats=%+v", stats)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history WHERE quote_key = 'betfair-key-1'`); got != 2 {
		t.Fatalf("history count after changed insert=%d want 2", got)
	}
	assertBetfairLatest(ctx, t, db, "betfair-key-1", "hash-2", 2.2, 21)

	invalid := changed
	invalid.QuoteKey = "betfair-invalid"
	invalid.PriceDecimal = 1
	stats, err = store.SaveBetfairQuoteCandidates(ctx, []model.BetfairExchangeQuoteCandidate{invalid})
	if err != nil {
		t.Fatalf("save invalid betfair candidate: %v", err)
	}
	if stats.CandidatesSeen != 1 || stats.CandidatesSkipped != 1 || stats.CandidatesInserted != 0 {
		t.Fatalf("invalid candidate stats=%+v", stats)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history WHERE quote_key = 'betfair-invalid'`); got != 0 {
		t.Fatalf("invalid history count=%d want 0", got)
	}
}

func TestDBIntegrationBetfairBatchRollsBackOnCandidateFailure(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	store, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	raw := betfairRawFixture("rollback-stream", time.Now().UTC())
	candidate := betfairCandidateFixture("rollback-key", "rollback-hash", 2.1, 20)
	candidate.RawMetadata = json.RawMessage(`{bad`)

	if _, err := store.SaveBetfairBatch(ctx, []model.BetfairRawStreamMessage{raw}, []model.BetfairExchangeQuoteCandidate{candidate}); err == nil {
		t.Fatal("save betfair batch with invalid metadata returned nil error")
	} else if !strings.Contains(err.Error(), "raw metadata") {
		t.Fatalf("rollback error=%v want raw metadata context", err)
	}

	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_raw_stream_messages WHERE stream_id = 'rollback-stream'`); got != 0 {
		t.Fatalf("rollback raw count=%d want 0", got)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history WHERE quote_key = 'rollback-key'`); got != 0 {
		t.Fatalf("rollback history count=%d want 0", got)
	}
}

func betfairRawFixture(streamID string, receivedAt time.Time) model.BetfairRawStreamMessage {
	publishMS := int64(1715000000000)
	sourcePublishTime := time.UnixMilli(publishMS).UTC()
	return model.BetfairRawStreamMessage{
		Source:            model.SourceBetfair,
		StreamID:          streamID,
		Op:                "mcm",
		MarketID:          "1.234",
		PublishTimeMS:     &publishMS,
		SourcePublishTime: &sourcePublishTime,
		ChangeType:        "UPDATE",
		Clock:             "clk-1",
		InitialClock:      "initial-clk-1",
		RawJSON:           json.RawMessage(`{"op":"mcm","id":1,"pt":1715000000000,"mc":[{"id":"1.234"}]}`),
		ReceivedAt:        receivedAt,
	}
}

func assertBetfairLatest(ctx context.Context, t *testing.T, db *sql.DB, quoteKey, wantHash string, wantPrice, wantSize float64) {
	t.Helper()
	var gotHash string
	var gotPrice, gotSize float64
	var exchangeSide string
	if err := db.QueryRowContext(ctx, `
        SELECT quote_hash, price_decimal::float8, available_size::float8, exchange_side
        FROM betfair_latest_exchange_quotes_view
        WHERE quote_key = $1
    `, quoteKey).Scan(&gotHash, &gotPrice, &gotSize, &exchangeSide); err != nil {
		t.Fatalf("query betfair latest %s: %v", quoteKey, err)
	}
	if gotHash != wantHash {
		t.Fatalf("latest hash=%q want %q", gotHash, wantHash)
	}
	assertNear(t, gotPrice, wantPrice, 0.000001, "betfair latest price")
	assertNear(t, gotSize, wantSize, 0.000001, "betfair latest size")
	if exchangeSide != "back" {
		t.Fatalf("exchange side=%q want back", exchangeSide)
	}
}
