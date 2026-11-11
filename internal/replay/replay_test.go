package replay

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

func TestDecodeOddsAPIEventsFromFixture(t *testing.T) {
	events, err := DecodeOddsAPIEvents(loadOddsAPIPayload(t))
	if err != nil {
		t.Fatalf("decode odds api events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events len=%d want 2", len(events))
	}
	if events[0].ID != "fixture-event-prematch" {
		t.Fatalf("first event id=%q", events[0].ID)
	}
}

func TestReplayOddsAPISnapshotReportsQuoteDiffs(t *testing.T) {
	const knownQuoteKey = "895b8104f3961d21c6b39e8b9c07e39ae80d7003360d882e612360dfddd43818" // gitleaks:allow -- deterministic test fixture, not a credential
	const knownQuoteHash = "69c89c7b4dd1007d85569a7f6d2a4b10b124939318d4880ece25f0a656bd49c3"

	snapshot := fixtureSnapshot(t)
	history := map[string]string{
		knownQuoteKey: knownQuoteHash,
		"stored-only": "stored-only-hash",
	}
	latest := map[string]string{
		knownQuoteKey: knownQuoteHash,
	}

	report, err := ReplayOddsAPISnapshot(snapshot, fixturePolicy(), "", history, latest)
	if err != nil {
		t.Fatalf("replay snapshot: %v", err)
	}

	if report.SnapshotID != snapshot.ID {
		t.Fatalf("snapshot id=%d want %d", report.SnapshotID, snapshot.ID)
	}
	if report.SportKey != "basketball_nba" {
		t.Fatalf("sport key=%q", report.SportKey)
	}
	if report.EventCount != 2 {
		t.Fatalf("event count=%d want 2", report.EventCount)
	}
	if report.NormalizedQuoteCount != 10 {
		t.Fatalf("quote count=%d want 10", report.NormalizedQuoteCount)
	}
	if report.HistoryMatches != 1 || report.MissingFromHistory != 9 || report.StoredHistoryOnlyCnt != 1 {
		t.Fatalf("history summary match=%d missing=%d stored_only=%d", report.HistoryMatches, report.MissingFromHistory, report.StoredHistoryOnlyCnt)
	}
	if report.LatestMatches != 1 || report.MissingFromLatest != 9 {
		t.Fatalf("latest summary match=%d missing=%d", report.LatestMatches, report.MissingFromLatest)
	}

	found := false
	for _, quote := range report.Quotes {
		if quote.QuoteKey == knownQuoteKey {
			found = true
			if quote.QuoteHash != knownQuoteHash {
				t.Fatalf("known quote hash=%q want %q", quote.QuoteHash, knownQuoteHash)
			}
			if quote.HistoryStatus != HistoryMatch || quote.LatestStatus != LatestMatch {
				t.Fatalf("known quote statuses history=%q latest=%q", quote.HistoryStatus, quote.LatestStatus)
			}
		}
	}
	if !found {
		t.Fatalf("known quote key %s not found", knownQuoteKey)
	}
}

func TestReplayOddsAPISnapshotDetectsHashDiff(t *testing.T) {
	const knownQuoteKey = "895b8104f3961d21c6b39e8b9c07e39ae80d7003360d882e612360dfddd43818" // gitleaks:allow -- deterministic test fixture, not a credential

	report, err := ReplayOddsAPISnapshot(fixtureSnapshot(t), fixturePolicy(), "", map[string]string{
		knownQuoteKey: "different-hash",
	}, map[string]string{
		knownQuoteKey: "different-latest-hash",
	})
	if err != nil {
		t.Fatalf("replay snapshot: %v", err)
	}
	if report.HistoryHashDiffs != 1 {
		t.Fatalf("history hash diffs=%d want 1", report.HistoryHashDiffs)
	}
	if report.LatestHashDiffs != 1 {
		t.Fatalf("latest hash diffs=%d want 1", report.LatestHashDiffs)
	}
}

func TestReplayOddsAPISnapshotSportFilter(t *testing.T) {
	report, err := ReplayOddsAPISnapshot(fixtureSnapshot(t), fixturePolicy(), "baseball_mlb", nil, nil)
	if err != nil {
		t.Fatalf("replay snapshot: %v", err)
	}
	if !report.SkippedBySportFilter {
		t.Fatal("expected snapshot to be skipped by sport filter")
	}
	if report.EventCount != 0 || report.NormalizedQuoteCount != 0 {
		t.Fatalf("filtered report event_count=%d quote_count=%d", report.EventCount, report.NormalizedQuoteCount)
	}
}

func fixtureSnapshot(t *testing.T) RawSnapshot {
	t.Helper()
	return RawSnapshot{
		ID:         42,
		Source:     model.SourceOddsAPI,
		RequestURL: "https://api.the-odds-api.com/v4/sports/basketball_nba/odds/?apiKey=redacted",
		ReceivedAt: mustTime(t, "2026-05-06T12:04:00Z"),
		StatusCode: 200,
		Payload:    loadOddsAPIPayload(t),
	}
}

func fixturePolicy() normalize.FreshnessPolicy {
	return normalize.FreshnessPolicy{
		FreshAfter: 2 * time.Minute,
		AgingAfter: 5 * time.Minute,
	}
}

func loadOddsAPIPayload(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "oddsapi", "sample_odds_response.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return body
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("parse time %q: %v", raw, err)
	}
	return ts
}
