package betfair

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMarketCacheAppliesInitialImage(t *testing.T) {
	messages := fixtureMessages(t, "market_cache_messages.jsonl")
	cache := newTestMarketCache()

	applyMessages(t, cache, messages[:2]...)

	snapshot, ok := cache.SnapshotMarket("1.111")
	if !ok {
		t.Fatal("market snapshot missing")
	}
	if snapshot.Status != "OPEN" {
		t.Fatalf("market status=%q want OPEN", snapshot.Status)
	}
	if snapshot.EventID != "event-1" || snapshot.EventTypeID != "1" {
		t.Fatalf("event ids=%q/%q", snapshot.EventID, snapshot.EventTypeID)
	}
	if snapshot.Name != "Match Odds" {
		t.Fatalf("market name=%q", snapshot.Name)
	}
	if snapshot.MarketTime != "2026-05-07T12:00:00.000Z" {
		t.Fatalf("market time=%q", snapshot.MarketTime)
	}
	if snapshot.InPlay == nil || *snapshot.InPlay {
		t.Fatalf("in-play=%v want false", snapshot.InPlay)
	}
	if snapshot.SourcePublishTimeMS == nil || *snapshot.SourcePublishTimeMS != 1715000000000 {
		t.Fatalf("publish ms=%v", snapshot.SourcePublishTimeMS)
	}
	if !snapshot.SourcePublishTime.Equal(time.UnixMilli(1715000000000).UTC()) {
		t.Fatalf("publish time=%v", snapshot.SourcePublishTime)
	}
	if !snapshot.LocalUpdatedAt.Equal(testCacheTime()) {
		t.Fatalf("local updated at=%v", snapshot.LocalUpdatedAt)
	}
	if len(snapshot.Runners) != 2 {
		t.Fatalf("runner count=%d want 2", len(snapshot.Runners))
	}
	if _, ok := snapshot.UnknownFields["unknownMarketField"]; !ok {
		t.Fatal("market unknown field was not preserved")
	}
	if _, ok := snapshot.UnknownFields["definitionExtra"]; !ok {
		t.Fatal("market definition unknown field was not preserved")
	}

	runner := requireRunner(t, snapshot, 101, 0)
	requireFloatPtr(t, "ltp", runner.LastTradedPrice, 2.1)
	requireFloatPtr(t, "traded volume", runner.TradedVolume, 100)
	requireFloatPtr(t, "best back price", runner.BestBackPrice, 2.08)
	requireFloatPtr(t, "best back size", runner.BestBackSize, 50)
	requireFloatPtr(t, "best lay price", runner.BestLayPrice, 2.12)
	requireFloatPtr(t, "best lay size", runner.BestLaySize, 60)
	if len(runner.BestAvailableToBack) != 2 {
		t.Fatalf("batb len=%d want 2", len(runner.BestAvailableToBack))
	}
	if len(runner.AvailableToBack) != 2 || runner.AvailableToBack[0].Price != 2.08 {
		t.Fatalf("available to back=%+v", runner.AvailableToBack)
	}
	if len(runner.Traded) != 1 || runner.Traded[0].Price != 2 {
		t.Fatalf("traded ladder=%+v", runner.Traded)
	}
	if _, ok := runner.UnknownFields["runnerExtra"]; !ok {
		t.Fatal("runner-change unknown field was not preserved")
	}
	if _, ok := runner.UnknownFields["runnerDefinitionExtra"]; !ok {
		t.Fatal("runner-definition unknown field was not preserved")
	}
}

func TestMarketCacheAppliesDeltaAndPreservesOmittedFields(t *testing.T) {
	messages := fixtureMessages(t, "market_cache_messages.jsonl")
	cache := newTestMarketCache()
	applyMessages(t, cache, messages[:2]...)

	beforeHeartbeat, ok := cache.BestPrices("1.111", 101, 0)
	if !ok {
		t.Fatal("best prices missing before heartbeat")
	}
	applyMessages(t, cache, messages[2])
	afterHeartbeat, ok := cache.BestPrices("1.111", 101, 0)
	if !ok {
		t.Fatal("best prices missing after heartbeat")
	}
	if !reflect.DeepEqual(beforeHeartbeat, afterHeartbeat) {
		t.Fatalf("heartbeat mutated prices:\nbefore=%+v\nafter=%+v", beforeHeartbeat, afterHeartbeat)
	}

	applyMessages(t, cache, messages[3])

	deltaSnapshot, ok := cache.SnapshotMarket("1.111")
	if !ok {
		t.Fatal("market snapshot missing after delta")
	}
	runner := requireRunner(t, deltaSnapshot, 101, 0)
	requireFloatPtr(t, "ltp after delta", runner.LastTradedPrice, 2.12)
	requireFloatPtr(t, "tv preserved after delta", runner.TradedVolume, 100)
	requireFloatPtr(t, "best back after delta", runner.BestBackPrice, 2.1)
	requireFloatPtr(t, "best back size after delta", runner.BestBackSize, 70)
	requireFloatPtr(t, "best lay after level removal", runner.BestLayPrice, 2.16)
	requireFloatPtr(t, "best lay size after level removal", runner.BestLaySize, 30)
	if len(runner.BestAvailableToBack) != 2 {
		t.Fatalf("batb after delta=%+v", runner.BestAvailableToBack)
	}
	if runner.BestAvailableToBack[1].Level != 2 || runner.BestAvailableToBack[1].Price != 2.04 {
		t.Fatalf("removed level was not deleted and new level inserted: %+v", runner.BestAvailableToBack)
	}
	if len(runner.AvailableToBack) != 2 || runner.AvailableToBack[0].Price != 2.12 || runner.AvailableToBack[1].Price != 2.06 {
		t.Fatalf("available to back after delta=%+v", runner.AvailableToBack)
	}
	if len(runner.AvailableToLay) != 1 || runner.AvailableToLay[0].Price != 2.18 {
		t.Fatalf("available to lay after delta=%+v", runner.AvailableToLay)
	}
	if len(runner.Traded) != 1 || runner.Traded[0].Price != 2.2 {
		t.Fatalf("traded after delta=%+v", runner.Traded)
	}

	applyMessages(t, cache, messages[4])

	omittedSnapshot, ok := cache.SnapshotMarket("1.111")
	if !ok {
		t.Fatal("market snapshot missing after omitted-field delta")
	}
	if omittedSnapshot.Status != "SUSPENDED" {
		t.Fatalf("status=%q want SUSPENDED", omittedSnapshot.Status)
	}
	runner = requireRunner(t, omittedSnapshot, 101, 0)
	requireFloatPtr(t, "ltp preserved after omitted-field delta", runner.LastTradedPrice, 2.12)
	requireFloatPtr(t, "tv updated after omitted-field delta", runner.TradedVolume, 150)
	requireFloatPtr(t, "best back preserved after omitted-field delta", runner.BestBackPrice, 2.1)
	requireFloatPtr(t, "best lay preserved after omitted-field delta", runner.BestLayPrice, 2.16)
}

func TestMarketCacheImageReplacementReplacesPreviousState(t *testing.T) {
	messages := fixtureMessages(t, "market_cache_image_replace.jsonl")
	cache := newTestMarketCache()

	applyMessages(t, cache, messages...)

	snapshot, ok := cache.SnapshotMarket("1.444")
	if !ok {
		t.Fatal("market snapshot missing")
	}
	if snapshot.InPlay == nil || !*snapshot.InPlay {
		t.Fatalf("in-play=%v want true", snapshot.InPlay)
	}
	if len(snapshot.Runners) != 1 {
		t.Fatalf("runner count after image replacement=%d want 1", len(snapshot.Runners))
	}
	if _, ok := cache.BestPrices("1.444", 502, 0); ok {
		t.Fatal("runner removed by image replacement still has prices")
	}
	runner := requireRunner(t, snapshot, 501, 0)
	if runner.Name != "Replacement Home" {
		t.Fatalf("runner name=%q", runner.Name)
	}
	if runner.LastTradedPrice != nil {
		t.Fatalf("last traded price after replacement=%v want nil", *runner.LastTradedPrice)
	}
	if runner.TradedVolume != nil {
		t.Fatalf("traded volume after replacement=%v want nil", *runner.TradedVolume)
	}
	if len(runner.AvailableToBack) != 0 || len(runner.AvailableToLay) != 0 {
		t.Fatalf("price-point ladders after replacement atb=%+v atl=%+v", runner.AvailableToBack, runner.AvailableToLay)
	}
	requireFloatPtr(t, "replacement best back", runner.BestBackPrice, 1.5)
	requireFloatPtr(t, "replacement best lay", runner.BestLayPrice, 1.6)
}

func TestMarketCacheMultipleMarketsSnapshotsAndDeterministicReplay(t *testing.T) {
	messages := fixtureMessages(t, "market_cache_messages.jsonl")
	cache := newTestMarketCache()
	applyMessages(t, cache, messages...)

	snapshots := cache.SnapshotAll()
	if len(snapshots) != 3 {
		t.Fatalf("snapshot count=%d want 3", len(snapshots))
	}
	gotIDs := []string{snapshots[0].MarketID, snapshots[1].MarketID, snapshots[2].MarketID}
	wantIDs := []string{"1.111", "1.222", "1.333"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("snapshot ids=%v want %v", gotIDs, wantIDs)
	}
	best, ok := cache.BestPrices("1.222", 303, 2.5)
	if !ok {
		t.Fatal("handicap runner best prices missing")
	}
	requireFloatPtr(t, "handicap best back", best.BestBackPrice, 1.7)
	requireFloatPtr(t, "handicap best lay", best.BestLayPrice, 1.74)
	if best.InPlay == nil || *best.InPlay {
		t.Fatalf("handicap market in-play=%v want false", best.InPlay)
	}

	again := newTestMarketCache()
	applyMessages(t, again, messages...)
	if !reflect.DeepEqual(cache.SnapshotAll(), again.SnapshotAll()) {
		t.Fatal("fixture replay was not deterministic")
	}
}

func TestMarketCacheMalformedLadderFixtureFailsClearly(t *testing.T) {
	_, err := readFixtureMessages("market_cache_malformed_ladder.jsonl")
	if err == nil {
		t.Fatal("malformed ladder fixture decoded successfully")
	}
	if !strings.Contains(err.Error(), "expected [level,price,size]") {
		t.Fatalf("malformed ladder error=%v", err)
	}
}

func fixtureMessages(t *testing.T, name string) []Message {
	t.Helper()
	messages, err := readFixtureMessages(name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return messages
}

func readFixtureMessages(name string) ([]Message, error) {
	path := filepath.Join("..", "..", "..", "testdata", "betfair", name)
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	stream := strings.Join(lines, "\r\n") + "\r\n"
	return ReadAll(context.Background(), strings.NewReader(stream), 65536)
}

func newTestMarketCache() *MarketCache {
	return NewMarketCache(WithCacheClock(testCacheTime))
}

func testCacheTime() time.Time {
	return time.Date(2026, 5, 7, 1, 2, 3, 0, time.UTC)
}

func applyMessages(t *testing.T, cache *MarketCache, messages ...Message) {
	t.Helper()
	for _, message := range messages {
		if err := cache.ApplyMessage(message); err != nil {
			t.Fatalf("apply message %q: %v", message.Op, err)
		}
	}
}

func requireRunner(t *testing.T, snapshot MarketSnapshot, selectionID int64, handicap float64) RunnerSnapshot {
	t.Helper()
	for _, runner := range snapshot.Runners {
		if runner.SelectionID == selectionID && runner.Handicap == handicap {
			return runner
		}
	}
	t.Fatalf("runner %d/%v not found in %+v", selectionID, handicap, snapshot.Runners)
	return RunnerSnapshot{}
}

func requireFloatPtr(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s=nil want %v", name, want)
	}
	if *got != want {
		t.Fatalf("%s=%v want %v", name, *got, want)
	}
}
