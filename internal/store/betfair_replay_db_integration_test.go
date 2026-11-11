package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/betfair"
)

func TestDBIntegrationBetfairReplayPipelineWriteSemantics(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	store, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	receivedAt := time.UnixMilli(1715000000000).UTC().Add(time.Minute)
	first := replayBetfairFixture(t, "market_cache_messages.jsonl", receivedAt)
	firstStats, err := store.SaveBetfairBatch(ctx, first.RawMessages, first.PersistenceCandidates)
	if err != nil {
		t.Fatalf("save first replay batch: %v", err)
	}
	if firstStats.RawMessagesInserted != 6 || firstStats.CandidatesInserted != 8 || firstStats.CandidatesUnchanged != 0 || firstStats.CandidatesSkipped != 0 {
		t.Fatalf("first replay stats=%+v", firstStats)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_raw_stream_messages`); got != 6 {
		t.Fatalf("raw count after first replay=%d want 6", got)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != 8 {
		t.Fatalf("history count after first replay=%d want 8", got)
	}

	repeated := replayBetfairFixture(t, "market_cache_messages.jsonl", receivedAt)
	repeatedStats, err := store.SaveBetfairBatch(ctx, repeated.RawMessages, repeated.PersistenceCandidates)
	if err != nil {
		t.Fatalf("save repeated replay batch: %v", err)
	}
	if repeatedStats.RawMessagesInserted != 6 || repeatedStats.CandidatesInserted != 0 || repeatedStats.CandidatesUnchanged != 8 || repeatedStats.CandidatesSkipped != 0 {
		t.Fatalf("repeated replay stats=%+v", repeatedStats)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_raw_stream_messages`); got != 12 {
		t.Fatalf("raw count after repeated replay=%d want 12", got)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != 8 {
		t.Fatalf("history count after repeated replay=%d want 8", got)
	}

	changed := replayBetfairFixture(t, "market_cache_messages_changed.jsonl", receivedAt)
	changedBack := requireReplayCandidate(t, changed.Candidates, "1.111", 101, betfair.ExchangeSideBack)
	changedStats, err := store.SaveBetfairBatch(ctx, changed.RawMessages, changed.PersistenceCandidates)
	if err != nil {
		t.Fatalf("save changed replay batch: %v", err)
	}
	if changedStats.RawMessagesInserted != 2 || changedStats.CandidatesInserted != 4 || changedStats.CandidatesUnchanged != 0 || changedStats.CandidatesSkipped != 0 {
		t.Fatalf("changed replay stats=%+v", changedStats)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_raw_stream_messages`); got != 14 {
		t.Fatalf("raw count after changed replay=%d want 14", got)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != 12 {
		t.Fatalf("history count after changed replay=%d want 12", got)
	}
	assertBetfairLatest(ctx, t, db, changedBack.QuoteKey, changedBack.QuoteHash, changedBack.PriceDecimal, changedBack.AvailableSize)
}

func replayBetfairFixture(t *testing.T, name string, receivedAt time.Time) betfair.ReplayResult {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "testdata", "betfair", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	result, err := betfair.ReplayStream(context.Background(), strings.NewReader(string(body)), betfair.ReplayOptions{
		Format:     betfair.ReplayFormatJSONL,
		StreamID:   name,
		ReceivedAt: receivedAt,
		QuoteOptions: betfair.QuoteEmitOptions{
			Now:        receivedAt,
			FreshAfter: 2 * time.Minute,
			AgingAfter: 5 * time.Minute,
		},
	})
	if err != nil {
		t.Fatalf("replay fixture %s: %v", name, err)
	}
	return result
}

func requireReplayCandidate(t *testing.T, candidates []betfair.QuoteCandidate, marketID string, selectionID int64, side betfair.ExchangeSide) betfair.QuoteCandidate {
	t.Helper()
	for _, candidate := range candidates {
		if candidate.MarketID == marketID && candidate.SelectionID == selectionID && candidate.ExchangeSide == side {
			return candidate
		}
	}
	t.Fatalf("candidate not found market=%q selection=%d side=%q", marketID, selectionID, side)
	return betfair.QuoteCandidate{}
}
