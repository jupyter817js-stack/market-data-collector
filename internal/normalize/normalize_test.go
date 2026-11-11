package normalize

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

func TestEventTeamNorm(t *testing.T) {
	got := EventTeamNorm("  New-York   Knicks! ")
	if got != "new york knicks" {
		t.Fatalf("unexpected normalized team: %q", got)
	}
}

func TestOutcomeSide(t *testing.T) {
	cases := []struct {
		name   string
		home   string
		away   string
		market string
		want   string
	}{
		{"Boston Celtics", "Boston Celtics", "Miami Heat", "h2h", "home"},
		{"Miami Heat", "Boston Celtics", "Miami Heat", "spreads", "away"},
		{"Draw", "Chelsea", "Arsenal", "h2h", "draw"},
		{"Over", "Boston Celtics", "Miami Heat", "totals", "over"},
		{"Under", "Boston Celtics", "Miami Heat", "totals", "under"},
	}
	for _, tc := range cases {
		got := OutcomeSide(tc.name, tc.home, tc.away, tc.market)
		if got != tc.want {
			t.Fatalf("OutcomeSide(%q)=%q want %q", tc.name, got, tc.want)
		}
	}
}

func TestDecodeOddsAPIFixtureIntoEvents(t *testing.T) {
	events := loadOddsAPIFixture(t)

	if len(events) != 2 {
		t.Fatalf("events len=%d want 2", len(events))
	}
	event := events[0]
	if event.ID != "fixture-event-prematch" {
		t.Fatalf("event ID=%q", event.ID)
	}
	if event.SportKey != "basketball_nba" {
		t.Fatalf("sport key=%q", event.SportKey)
	}
	if event.HomeTeam != "Boston Celtics" || event.AwayTeam != "Miami Heat" {
		t.Fatalf("teams=%q/%q", event.HomeTeam, event.AwayTeam)
	}
	if len(event.Bookmakers) != 2 {
		t.Fatalf("bookmakers len=%d want 2", len(event.Bookmakers))
	}
	if got := event.Bookmakers[0].LastUpdate; !got.Equal(mustParseTime(t, "2026-05-06T12:00:00Z")) {
		t.Fatalf("bookmaker last_update=%s", got)
	}
	if event.Bookmakers[0].Markets[1].LastUpdate == nil {
		t.Fatal("spread market last_update is nil")
	}
	if got := *event.Bookmakers[0].Markets[1].LastUpdate; !got.Equal(mustParseTime(t, "2026-05-06T12:03:00Z")) {
		t.Fatalf("spread market last_update=%s", got)
	}
}

func TestEventToQuotesFromOddsAPIFixture(t *testing.T) {
	events := loadOddsAPIFixture(t)
	receivedAt := mustParseTime(t, "2026-05-06T12:04:00Z")
	policy := FreshnessPolicy{
		FreshAfter: 2 * time.Minute,
		AgingAfter: 5 * time.Minute,
	}

	quotes := EventToQuotes(events[0], model.SourceOddsAPI, receivedAt, policy)
	if len(quotes) != 8 {
		t.Fatalf("quotes len=%d want 8", len(quotes))
	}

	homeH2H := findQuote(t, quotes, "draftkings", "h2h", "Boston Celtics")
	if homeH2H.OutcomeSide != "home" {
		t.Fatalf("h2h home outcome side=%q", homeH2H.OutcomeSide)
	}
	if homeH2H.Line != nil {
		t.Fatalf("h2h line=%v want nil", *homeH2H.Line)
	}
	if homeH2H.StaleState != "aging" {
		t.Fatalf("h2h stale state=%q want aging", homeH2H.StaleState)
	}
	if homeH2H.SourceLastUpdate == nil || !homeH2H.SourceLastUpdate.Equal(mustParseTime(t, "2026-05-06T12:00:00Z")) {
		t.Fatalf("h2h source_last_update=%v", homeH2H.SourceLastUpdate)
	}
	if homeH2H.AgeMS == nil || *homeH2H.AgeMS != int64((4*time.Minute).Milliseconds()) {
		t.Fatalf("h2h age_ms=%v", homeH2H.AgeMS)
	}
	if homeH2H.InPlay {
		t.Fatal("prematch h2h quote is in_play")
	}

	spreadHome := findQuote(t, quotes, "draftkings", "spreads", "Boston Celtics")
	if spreadHome.OutcomeSide != "home" {
		t.Fatalf("spread home outcome side=%q", spreadHome.OutcomeSide)
	}
	if spreadHome.Line == nil || *spreadHome.Line != -3.5 {
		t.Fatalf("spread home line=%v want -3.5", spreadHome.Line)
	}
	if spreadHome.StaleState != "fresh" {
		t.Fatalf("spread stale state=%q want fresh", spreadHome.StaleState)
	}
	if spreadHome.SourceLastUpdate == nil || !spreadHome.SourceLastUpdate.Equal(mustParseTime(t, "2026-05-06T12:03:00Z")) {
		t.Fatalf("spread source_last_update=%v", spreadHome.SourceLastUpdate)
	}

	over := findQuote(t, quotes, "draftkings", "totals", "Over")
	if over.OutcomeSide != "over" {
		t.Fatalf("total over outcome side=%q", over.OutcomeSide)
	}
	if over.Line == nil || *over.Line != 214.5 {
		t.Fatalf("total over line=%v want 214.5", over.Line)
	}

	missingTime := findQuote(t, quotes, "no_timestamps", "h2h", "Boston Celtics")
	if missingTime.StaleState != "unknown" {
		t.Fatalf("missing timestamp stale state=%q want unknown", missingTime.StaleState)
	}
	if missingTime.SourceLastUpdate != nil {
		t.Fatalf("missing timestamp source_last_update=%v want nil", missingTime.SourceLastUpdate)
	}
	if missingTime.AgeMS != nil {
		t.Fatalf("missing timestamp age_ms=%v want nil", *missingTime.AgeMS)
	}
	if missingTime.ConfidenceScore != 35 {
		t.Fatalf("missing timestamp confidence=%v want 35", missingTime.ConfidenceScore)
	}
}

func TestQuoteKeyAndHashAreStableForFixture(t *testing.T) {
	events := loadOddsAPIFixture(t)
	receivedAt := mustParseTime(t, "2026-05-06T12:04:00Z")
	policy := FreshnessPolicy{
		FreshAfter: 2 * time.Minute,
		AgingAfter: 5 * time.Minute,
	}

	quotes := EventToQuotes(events[0], model.SourceOddsAPI, receivedAt, policy)
	homeH2H := findQuote(t, quotes, "draftkings", "h2h", "Boston Celtics")

	const wantQuoteKey = "895b8104f3961d21c6b39e8b9c07e39ae80d7003360d882e612360dfddd43818" // gitleaks:allow -- deterministic test fixture, not a credential
	const wantQuoteHash = "69c89c7b4dd1007d85569a7f6d2a4b10b124939318d4880ece25f0a656bd49c3"
	if homeH2H.QuoteKey != wantQuoteKey {
		t.Fatalf("quote key=%q want %q", homeH2H.QuoteKey, wantQuoteKey)
	}
	if homeH2H.QuoteHash != wantQuoteHash {
		t.Fatalf("quote hash=%q want %q", homeH2H.QuoteHash, wantQuoteHash)
	}

	replayed := EventToQuotes(events[0], model.SourceOddsAPI, receivedAt, policy)
	replayedHomeH2H := findQuote(t, replayed, "draftkings", "h2h", "Boston Celtics")
	if replayedHomeH2H.QuoteKey != homeH2H.QuoteKey {
		t.Fatalf("replayed quote key=%q want %q", replayedHomeH2H.QuoteKey, homeH2H.QuoteKey)
	}
	if replayedHomeH2H.QuoteHash != homeH2H.QuoteHash {
		t.Fatalf("replayed quote hash=%q want %q", replayedHomeH2H.QuoteHash, homeH2H.QuoteHash)
	}

	changedPrice := homeH2H
	changedPrice.PriceDecimal = 1.81
	if got := QuoteKey(changedPrice); got != homeH2H.QuoteKey {
		t.Fatalf("quote key after price change=%q want %q", got, homeH2H.QuoteKey)
	}
	if got := QuoteHash(changedPrice); got == homeH2H.QuoteHash {
		t.Fatalf("quote hash did not change after price change: %q", got)
	}
}

func TestPrematchAndInPlayFromFixture(t *testing.T) {
	events := loadOddsAPIFixture(t)
	receivedAt := mustParseTime(t, "2026-05-06T12:04:00Z")
	policy := FreshnessPolicy{
		FreshAfter: 2 * time.Minute,
		AgingAfter: 5 * time.Minute,
	}

	prematchQuotes := EventToQuotes(events[0], model.SourceOddsAPI, receivedAt, policy)
	for _, q := range prematchQuotes {
		if q.InPlay {
			t.Fatalf("prematch event quote marked in_play: %+v", q)
		}
	}

	liveQuotes := EventToQuotes(events[1], model.SourceOddsAPI, receivedAt, policy)
	if len(liveQuotes) != 2 {
		t.Fatalf("live quotes len=%d want 2", len(liveQuotes))
	}
	for _, q := range liveQuotes {
		if !q.InPlay {
			t.Fatalf("live event quote not marked in_play: %+v", q)
		}
	}
}

func loadOddsAPIFixture(t *testing.T) []model.EventOdds {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "oddsapi", "sample_odds_response.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var events []model.EventOdds
	if err := json.Unmarshal(body, &events); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	return events
}

func findQuote(t *testing.T, quotes []Quote, bookmakerKey, marketKey, outcomeName string) Quote {
	t.Helper()
	for _, q := range quotes {
		if q.BookmakerKey == bookmakerKey && q.MarketKey == marketKey && q.OutcomeName == outcomeName {
			return q
		}
	}
	t.Fatalf("quote not found bookmaker=%q market=%q outcome=%q", bookmakerKey, marketKey, outcomeName)
	return Quote{}
}

func mustParseTime(t *testing.T, raw string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("parse time %q: %v", raw, err)
	}
	return ts
}
