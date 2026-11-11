package betfair

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEmitQuoteCandidatesFromInitialImage(t *testing.T) {
	messages := fixtureMessages(t, "market_cache_messages.jsonl")
	cache := newTestMarketCache()
	applyMessages(t, cache, messages[:2]...)
	snapshot, ok := cache.SnapshotMarket("1.111")
	if !ok {
		t.Fatal("market snapshot missing")
	}

	candidates, err := EmitQuoteCandidates(snapshot, freshQuoteOptions())
	if err != nil {
		t.Fatalf("emit candidates: %v", err)
	}
	if len(candidates) != 4 {
		t.Fatalf("candidate count=%d want 4", len(candidates))
	}

	back := requireCandidate(t, candidates, "1.111", 101, 0, ExchangeSideBack)
	lay := requireCandidate(t, candidates, "1.111", 101, 0, ExchangeSideLay)
	if back.Source != SourceBetfair {
		t.Fatalf("source=%q want betfair", back.Source)
	}
	if back.EventID != "event-1" || back.EventName != "Home FC v Away FC" {
		t.Fatalf("event id/name=%q/%q", back.EventID, back.EventName)
	}
	if back.MarketName != "Match Odds" || back.MarketFamily != "match_odds" {
		t.Fatalf("market name/family=%q/%q", back.MarketName, back.MarketFamily)
	}
	if back.MarketStatus != "OPEN" {
		t.Fatalf("market status=%q", back.MarketStatus)
	}
	if back.InPlay == nil || *back.InPlay {
		t.Fatalf("in-play=%v want false", back.InPlay)
	}
	if back.MarketTime != "2026-05-07T12:00:00.000Z" {
		t.Fatalf("market time=%q", back.MarketTime)
	}
	if back.SourcePublishTimeMS == nil || *back.SourcePublishTimeMS != 1715000000000 {
		t.Fatalf("source publish ms=%v", back.SourcePublishTimeMS)
	}
	if back.SourcePublishTime == nil || !back.SourcePublishTime.Equal(time.UnixMilli(1715000000000).UTC()) {
		t.Fatalf("source publish time=%v", back.SourcePublishTime)
	}
	if !back.ReducerUpdatedAt.Equal(testCacheTime()) {
		t.Fatalf("reducer updated at=%v", back.ReducerUpdatedAt)
	}
	if back.RunnerName != "Home FC" {
		t.Fatalf("runner name=%q", back.RunnerName)
	}
	if back.PriceDecimal != 2.08 || back.AvailableSize != 50 {
		t.Fatalf("back price/size=%v/%v", back.PriceDecimal, back.AvailableSize)
	}
	if lay.PriceDecimal != 2.12 || lay.AvailableSize != 60 {
		t.Fatalf("lay price/size=%v/%v", lay.PriceDecimal, lay.AvailableSize)
	}
	requireFloatPtr(t, "last traded price", back.LastTradedPrice, 2.1)
	requireFloatPtr(t, "traded volume", back.TradedVolume, 100)
	if back.PriceDecimal == *back.LastTradedPrice {
		t.Fatal("last traded price was emitted as executable back price")
	}
	if lay.PriceDecimal == *lay.LastTradedPrice {
		t.Fatal("last traded price was emitted as executable lay price")
	}
	if back.QuoteKey == lay.QuoteKey {
		t.Fatal("back and lay candidates share quote key")
	}
	if back.QuoteKey == "" || back.QuoteHash == "" {
		t.Fatalf("empty key/hash key=%q hash=%q", back.QuoteKey, back.QuoteHash)
	}
	if back.StaleState != QuoteFreshnessFresh {
		t.Fatalf("stale state=%q want fresh", back.StaleState)
	}
	if back.ConfidenceScore != 80 {
		t.Fatalf("confidence=%v want 80", back.ConfidenceScore)
	}
}

func TestEmitQuoteCandidatesAfterDeltaAndHashMateriality(t *testing.T) {
	messages := fixtureMessages(t, "market_cache_messages.jsonl")
	cache := newTestMarketCache()
	applyMessages(t, cache, messages[:2]...)
	initialSnapshot, _ := cache.SnapshotMarket("1.111")
	initial, err := EmitQuoteCandidates(initialSnapshot, freshQuoteOptions())
	if err != nil {
		t.Fatalf("emit initial candidates: %v", err)
	}
	initialBack := requireCandidate(t, initial, "1.111", 101, 0, ExchangeSideBack)

	applyMessages(t, cache, messages[3])
	deltaSnapshot, _ := cache.SnapshotMarket("1.111")
	updated, err := EmitQuoteCandidates(deltaSnapshot, freshQuoteOptions())
	if err != nil {
		t.Fatalf("emit updated candidates: %v", err)
	}
	updatedBack := requireCandidate(t, updated, "1.111", 101, 0, ExchangeSideBack)
	if updatedBack.QuoteKey != initialBack.QuoteKey {
		t.Fatalf("quote key changed across price delta: %q vs %q", updatedBack.QuoteKey, initialBack.QuoteKey)
	}
	if updatedBack.QuoteHash == initialBack.QuoteHash {
		t.Fatal("quote hash did not change after price/size delta")
	}
	if updatedBack.PriceDecimal != 2.1 || updatedBack.AvailableSize != 70 {
		t.Fatalf("updated back price/size=%v/%v", updatedBack.PriceDecimal, updatedBack.AvailableSize)
	}
	requireFloatPtr(t, "updated ltp", updatedBack.LastTradedPrice, 2.12)
	requireFloatPtr(t, "updated tv", updatedBack.TradedVolume, 100)

	priceChanged := initialBack
	priceChanged.PriceDecimal = 2.2
	priceChanged.QuoteHash = QuoteCandidateHash(priceChanged)
	if priceChanged.QuoteHash == initialBack.QuoteHash {
		t.Fatal("quote hash did not change when executable price changed")
	}

	sizeChanged := initialBack
	sizeChanged.AvailableSize = 51
	sizeChanged.QuoteHash = QuoteCandidateHash(sizeChanged)
	if sizeChanged.QuoteHash == initialBack.QuoteHash {
		t.Fatal("quote hash did not change when available size changed")
	}
}

func TestQuoteCandidateKeysAndHashesAreStableForRepeatedReplay(t *testing.T) {
	messages := fixtureMessages(t, "market_cache_messages.jsonl")
	first := newTestMarketCache()
	second := newTestMarketCache()
	applyMessages(t, first, messages...)
	applyMessages(t, second, messages...)

	firstCandidates, err := EmitAllQuoteCandidates(first, freshQuoteOptions())
	if err != nil {
		t.Fatalf("emit first candidates: %v", err)
	}
	secondCandidates, err := EmitAllQuoteCandidates(second, freshQuoteOptions())
	if err != nil {
		t.Fatalf("emit second candidates: %v", err)
	}
	if !reflect.DeepEqual(candidateKeyHashes(firstCandidates), candidateKeyHashes(secondCandidates)) {
		t.Fatalf("candidate key/hash replay mismatch:\nfirst=%v\nsecond=%v", candidateKeyHashes(firstCandidates), candidateKeyHashes(secondCandidates))
	}
}

func TestQuoteCandidateFreshnessClassification(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	opts := QuoteEmitOptions{
		Now:        now,
		FreshAfter: time.Minute,
		AgingAfter: 3 * time.Minute,
	}

	cases := []struct {
		name        string
		sourceTime  time.Time
		wantState   string
		wantAgeMS   *int64
		wantMinimum float64
	}{
		{"fresh", now.Add(-30 * time.Second), QuoteFreshnessFresh, int64Ptr(30000), 80},
		{"aging", now.Add(-2 * time.Minute), QuoteFreshnessAging, int64Ptr(120000), 70},
		{"stale", now.Add(-4 * time.Minute), QuoteFreshnessStale, int64Ptr(240000), 55},
		{"unknown", time.Time{}, QuoteFreshnessUnknown, nil, 40},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := validManualSnapshot()
			snapshot.SourcePublishTime = tc.sourceTime
			candidates, err := EmitQuoteCandidates(snapshot, opts)
			if err != nil {
				t.Fatalf("emit candidates: %v", err)
			}
			candidate := requireCandidate(t, candidates, "manual-market", 1, 0, ExchangeSideBack)
			if candidate.StaleState != tc.wantState {
				t.Fatalf("stale state=%q want %q", candidate.StaleState, tc.wantState)
			}
			if !sameInt64Ptr(candidate.AgeMS, tc.wantAgeMS) {
				t.Fatalf("age ms=%v want %v", candidate.AgeMS, tc.wantAgeMS)
			}
			if candidate.ConfidenceScore != tc.wantMinimum {
				t.Fatalf("confidence=%v want %v", candidate.ConfidenceScore, tc.wantMinimum)
			}
		})
	}
}

func TestQuoteCandidateConfidenceReductions(t *testing.T) {
	opts := freshQuoteOptions()
	complete, err := EmitQuoteCandidates(validManualSnapshot(), opts)
	if err != nil {
		t.Fatalf("emit complete candidates: %v", err)
	}
	if got := requireCandidate(t, complete, "manual-market", 1, 0, ExchangeSideBack).ConfidenceScore; got != 80 {
		t.Fatalf("complete confidence=%v want 80", got)
	}

	missingRunner := validManualSnapshot()
	missingRunner.Runners[0].Name = ""
	emitted, err := EmitQuoteCandidates(missingRunner, opts)
	if err != nil {
		t.Fatalf("emit missing-runner candidates: %v", err)
	}
	if got := requireCandidate(t, emitted, "manual-market", 1, 0, ExchangeSideBack).ConfidenceScore; got != 70 {
		t.Fatalf("missing runner confidence=%v want 70", got)
	}

	suspended := validManualSnapshot()
	suspended.Status = "SUSPENDED"
	emitted, err = EmitQuoteCandidates(suspended, opts)
	if err != nil {
		t.Fatalf("emit suspended candidates: %v", err)
	}
	if got := requireCandidate(t, emitted, "manual-market", 1, 0, ExchangeSideBack).ConfidenceScore; got != 55 {
		t.Fatalf("suspended confidence=%v want 55", got)
	}

	missingDefinition := validManualSnapshot()
	missingDefinition.Status = ""
	missingDefinition.Name = ""
	missingDefinition.EventID = ""
	missingDefinition.MarketTime = ""
	emitted, err = EmitQuoteCandidates(missingDefinition, opts)
	if err != nil {
		t.Fatalf("emit missing-definition candidates: %v", err)
	}
	if got := requireCandidate(t, emitted, "manual-market", 1, 0, ExchangeSideBack).ConfidenceScore; got != 70 {
		t.Fatalf("missing definition confidence=%v want 70", got)
	}
}

func TestEmitQuoteCandidatesSkipsInvalidPricesSizesAndMissingSelectionID(t *testing.T) {
	snapshot := validManualSnapshot()
	snapshot.Runners = []RunnerSnapshot{
		{SelectionID: 0, Name: "No ID", BestBackPrice: float64Ptr(2), BestBackSize: float64Ptr(10)},
		{SelectionID: 1, Name: "Too Low", BestBackPrice: float64Ptr(1), BestBackSize: float64Ptr(10)},
		{SelectionID: 2, Name: "Zero Size", BestBackPrice: float64Ptr(2), BestBackSize: float64Ptr(0)},
		{SelectionID: 3, Name: "Negative Size", BestBackPrice: float64Ptr(2), BestBackSize: float64Ptr(-1)},
		{SelectionID: 4, Name: "NaN Price", BestBackPrice: float64Ptr(math.NaN()), BestBackSize: float64Ptr(10)},
		{SelectionID: 5, Name: "Valid", BestBackPrice: float64Ptr(2.4), BestBackSize: float64Ptr(9)},
	}

	candidates, err := EmitQuoteCandidates(snapshot, freshQuoteOptions())
	if err != nil {
		t.Fatalf("emit candidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidate count=%d want 1: %+v", len(candidates), candidates)
	}
	candidate := candidates[0]
	if candidate.SelectionID != 5 || candidate.PriceDecimal != 2.4 || candidate.AvailableSize != 9 {
		t.Fatalf("unexpected emitted candidate=%+v", candidate)
	}
}

func TestEmitQuoteCandidatesRejectsMalformedSnapshot(t *testing.T) {
	_, err := EmitQuoteCandidates(MarketSnapshot{
		Runners: []RunnerSnapshot{{
			SelectionID:   1,
			BestBackPrice: float64Ptr(2),
			BestBackSize:  float64Ptr(10),
		}},
	}, freshQuoteOptions())
	if err == nil {
		t.Fatal("malformed market snapshot emitted successfully")
	}
	if !strings.Contains(err.Error(), "market id is required") {
		t.Fatalf("malformed snapshot error=%v", err)
	}
}

func TestEmitAllQuoteCandidatesMultipleMarketsOrderingAndPropagation(t *testing.T) {
	messages := fixtureMessages(t, "market_cache_messages.jsonl")
	cache := newTestMarketCache()
	applyMessages(t, cache, messages...)

	candidates, err := EmitAllQuoteCandidates(cache, freshQuoteOptions())
	if err != nil {
		t.Fatalf("emit all candidates: %v", err)
	}
	if len(candidates) != 8 {
		t.Fatalf("candidate count=%d want 8", len(candidates))
	}
	gotOrder := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		gotOrder = append(gotOrder, candidate.MarketID+"|"+string(candidate.ExchangeSide)+"|"+candidate.RunnerName)
	}
	wantOrder := []string{
		"1.111|back|Home FC",
		"1.111|lay|Home FC",
		"1.111|back|Away FC",
		"1.111|lay|Away FC",
		"1.222|back|Over 2.5",
		"1.222|lay|Over 2.5",
		"1.333|back|Draw",
		"1.333|lay|Draw",
	}
	if !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Fatalf("candidate order=%v want %v", gotOrder, wantOrder)
	}

	suspended := requireCandidate(t, candidates, "1.111", 101, 0, ExchangeSideBack)
	if suspended.MarketStatus != "SUSPENDED" {
		t.Fatalf("market status=%q want SUSPENDED", suspended.MarketStatus)
	}

	inPlay := requireCandidate(t, candidates, "1.333", 404, 0, ExchangeSideBack)
	if inPlay.InPlay == nil || !*inPlay.InPlay {
		t.Fatalf("in-play candidate flag=%v want true", inPlay.InPlay)
	}
	if inPlay.MarketStatus != "OPEN" {
		t.Fatalf("in-play market status=%q want OPEN", inPlay.MarketStatus)
	}
}

func freshQuoteOptions() QuoteEmitOptions {
	return QuoteEmitOptions{
		Now:        time.UnixMilli(1715000000000).UTC().Add(time.Minute),
		FreshAfter: 2 * time.Minute,
		AgingAfter: 5 * time.Minute,
	}
}

func validManualSnapshot() MarketSnapshot {
	inPlay := false
	return MarketSnapshot{
		MarketID:          "manual-market",
		Status:            "OPEN",
		EventID:           "manual-event",
		EventName:         "Manual Home v Manual Away",
		Name:              "Match Odds",
		InPlay:            &inPlay,
		MarketTime:        "2026-05-07T12:00:00Z",
		SourcePublishTime: time.UnixMilli(1715000000000).UTC(),
		LocalUpdatedAt:    testCacheTime(),
		Runners: []RunnerSnapshot{{
			SelectionID:     1,
			Name:            "Manual Home",
			BestBackPrice:   float64Ptr(2),
			BestBackSize:    float64Ptr(10),
			BestLayPrice:    float64Ptr(2.1),
			BestLaySize:     float64Ptr(11),
			LastTradedPrice: float64Ptr(2.05),
			TradedVolume:    float64Ptr(100),
		}},
	}
}

func requireCandidate(t *testing.T, candidates []QuoteCandidate, marketID string, selectionID int64, handicap float64, side ExchangeSide) QuoteCandidate {
	t.Helper()
	for _, candidate := range candidates {
		if candidate.MarketID == marketID && candidate.SelectionID == selectionID && candidate.Handicap == handicap && candidate.ExchangeSide == side {
			return candidate
		}
	}
	t.Fatalf("candidate not found market=%q selection=%d handicap=%v side=%q in %+v", marketID, selectionID, handicap, side, candidates)
	return QuoteCandidate{}
}

func candidateKeyHashes(candidates []QuoteCandidate) []string {
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.QuoteKey+"|"+candidate.QuoteHash)
	}
	return out
}

func float64Ptr(v float64) *float64 {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}

func sameInt64Ptr(got, want *int64) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	return *got == *want
}
