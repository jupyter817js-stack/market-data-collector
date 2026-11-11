package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
)

type fakeTargetedGamma struct {
	calls  int
	event  polymarket.Event
	err    error
	wantID string
}

func (f *fakeTargetedGamma) GetEvent(ctx context.Context, id string) (polymarket.Event, error) {
	f.calls++
	if f.wantID != "" && id != f.wantID {
		return polymarket.Event{}, fmt.Errorf("unexpected event id=%q want %q", id, f.wantID)
	}
	if f.err != nil {
		return polymarket.Event{}, f.err
	}
	return f.event, nil
}

type fakeTargetedStore struct {
	upsertEvents   []model.PolymarketEvent
	upsertMarkets  []model.PolymarketMarket
	upsertTokens   []model.PolymarketToken
	insertedPrices []model.PolymarketPriceSnapshot
}

func (s *fakeTargetedStore) UpsertPolymarketEvents(_ context.Context, events []model.PolymarketEvent) (int, error) {
	s.upsertEvents = append(s.upsertEvents, events...)
	return len(events), nil
}

func (s *fakeTargetedStore) UpsertPolymarketMarkets(_ context.Context, markets []model.PolymarketMarket) (int, error) {
	s.upsertMarkets = append(s.upsertMarkets, markets...)
	return len(markets), nil
}

func (s *fakeTargetedStore) UpsertPolymarketTokens(_ context.Context, tokens []model.PolymarketToken) (int, error) {
	s.upsertTokens = append(s.upsertTokens, tokens...)
	return len(tokens), nil
}

func (s *fakeTargetedStore) InsertPolymarketPriceSnapshots(_ context.Context, snapshots []model.PolymarketPriceSnapshot) (int, error) {
	s.insertedPrices = append(s.insertedPrices, snapshots...)
	return len(snapshots), nil
}

type fakeTargetedPriceFetcher struct {
	calls int32
	bid   float64
	ask   float64
}

func (f *fakeTargetedPriceFetcher) FetchSnapshot(_ context.Context, token model.PolymarketToken) (model.PolymarketPriceSnapshot, error) {
	atomic.AddInt32(&f.calls, 1)
	bid := f.bid
	ask := f.ask
	mid := (bid + ask) / 2
	return model.PolymarketPriceSnapshot{
		TokenID:            token.TokenID,
		ConditionID:        token.ConditionID,
		PolymarketMarketID: token.PolymarketMarketID,
		BestBid:            &bid,
		BestAsk:            &ask,
		Midpoint:           &mid,
		Source:             model.SourcePolymarket,
		ReceivedAt:         time.Now().UTC(),
	}, nil
}

func sampleTargetedEvent() polymarket.Event {
	yes := true
	no := false
	startTime := time.Date(2026, 5, 9, 19, 10, 0, 0, time.UTC)
	endTime := startTime.Add(3 * time.Hour)
	return polymarket.Event{
		ID:        "446691",
		Slug:      "pistons-vs-cavaliers",
		Title:     "Pistons vs. Cavaliers",
		Active:    &yes,
		Closed:    &no,
		Archived:  &no,
		StartTime: &startTime,
		EndTime:   &endTime,
		Markets: []polymarket.Market{
			{
				ID:          "mkt-cavs-win",
				ConditionID: "cond-cavs-win",
				Question:    "Will the Cavaliers beat the Pistons?",
				Slug:        "cavs-pistons",
				Active:      &yes,
				Closed:      &no,
				Archived:    &no,
				EndTime:     &endTime,
				Tokens: []polymarket.Token{
					{TokenID: "tok-yes", OutcomeName: "Yes", OutcomeIndex: 0},
					{TokenID: "tok-no", OutcomeName: "No", OutcomeIndex: 1},
				},
			},
		},
	}
}

func TestShouldRunTargetedEventDetectsBothFlags(t *testing.T) {
	cases := [][]string{
		{"--target-event-id", "446691"},
		{"--target-event-id=446691"},
		{"--event-id", "446691"},
		{"--event-id=446691"},
		{"--dry-run", "--target-event-id", "446691"},
	}
	for i, args := range cases {
		if !shouldRunTargetedEvent(args) {
			t.Fatalf("case %d args=%v expected detection", i, args)
		}
	}
	for _, args := range [][]string{nil, {"--diagnose-coverage"}, {"--limit", "100"}} {
		if shouldRunTargetedEvent(args) {
			t.Fatalf("args=%v should not trigger targeted mode", args)
		}
	}
}

func TestParseTargetedConfigValidation(t *testing.T) {
	if _, err := parseTargetedConfig(nil, &bytes.Buffer{}); err == nil {
		t.Fatal("missing event id must error")
	}
	if _, err := parseTargetedConfig([]string{"--target-event-id", "446691", "--max-price-tokens", "9999"}, &bytes.Buffer{}); err == nil {
		t.Fatal("oversized --max-price-tokens must error")
	}
	if _, err := parseTargetedConfig([]string{"--target-event-id", "446691", "--price-concurrency", "0"}, &bytes.Buffer{}); err == nil {
		t.Fatal("price-concurrency=0 must error")
	}
	if _, err := parseTargetedConfig([]string{"--target-event-id", "446691", "--output", "xml"}, &bytes.Buffer{}); err == nil {
		t.Fatal("invalid --output must error")
	}
	if _, err := parseTargetedConfig([]string{"--target-event-id", "446691", "--now", "not-a-time"}, &bytes.Buffer{}); err == nil {
		t.Fatal("invalid --now must error")
	}
	cfg, err := parseTargetedConfig([]string{"--event-id", "446691"}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("alias --event-id should be accepted: %v", err)
	}
	if cfg.EventID != "446691" {
		t.Fatalf("alias did not parse; cfg=%+v", cfg)
	}
	if !cfg.SyncPrices || cfg.DryRun {
		t.Fatalf("defaults should be SyncPrices=true DryRun=false; cfg=%+v", cfg)
	}
}

func TestRunTargetedEventUpsertsEventMarketsTokensAndRefreshesPrices(t *testing.T) {
	gamma := &fakeTargetedGamma{event: sampleTargetedEvent(), wantID: "446691"}
	st := &fakeTargetedStore{}
	prices := &fakeTargetedPriceFetcher{bid: 0.42, ask: 0.58}
	cfg := targetedConfig{
		EventID:          "446691",
		SyncPrices:       true,
		MaxPriceTokens:   10,
		PriceConcurrency: 2,
		HTTPTimeout:      time.Second,
		GammaBaseURL:     "https://gamma.example",
		Output:           "json",
		Now:              time.Date(2026, 5, 9, 18, 0, 0, 0, time.UTC),
	}
	var out bytes.Buffer
	if err := runTargetedEvent(context.Background(), cfg, gamma, st, prices, &out); err != nil {
		t.Fatalf("runTargetedEvent: %v", err)
	}

	if gamma.calls != 1 {
		t.Fatalf("Gamma GetEvent called %d times want 1", gamma.calls)
	}
	if len(st.upsertEvents) != 1 || st.upsertEvents[0].PolymarketEventID != "446691" {
		t.Fatalf("upsertEvents=%+v", st.upsertEvents)
	}
	if len(st.upsertMarkets) != 1 || st.upsertMarkets[0].PolymarketMarketID != "mkt-cavs-win" {
		t.Fatalf("upsertMarkets=%+v", st.upsertMarkets)
	}
	if len(st.upsertTokens) != 2 {
		t.Fatalf("upsertTokens count=%d want 2", len(st.upsertTokens))
	}
	if atomic.LoadInt32(&prices.calls) != 2 {
		t.Fatalf("price fetch calls=%d want 2", prices.calls)
	}
	if len(st.insertedPrices) != 2 {
		t.Fatalf("insertedPrices count=%d want 2", len(st.insertedPrices))
	}

	var report targetedReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, out.String())
	}
	if report.UpsertCounts.EventsUpserted != 1 || report.UpsertCounts.MarketsUpserted != 1 || report.UpsertCounts.TokensUpserted != 2 {
		t.Fatalf("upsert counts=%+v", report.UpsertCounts)
	}
	if report.PriceSnapshotCounts.Inserted != 2 {
		t.Fatalf("price counts=%+v", report.PriceSnapshotCounts)
	}
	// next-actions must instruct manual verification, never auto-mapping.
	joined := strings.ToLower(strings.Join(report.OperatorNextActions, " | "))
	if !strings.Contains(joined, "manually inspect") {
		t.Fatalf("missing manual-inspection instruction: %v", report.OperatorNextActions)
	}
	if strings.Contains(joined, "auto-map") || strings.Contains(joined, "auto-approve") {
		t.Fatalf("operator next actions must not encourage auto behavior: %v", report.OperatorNextActions)
	}
}

func TestRunTargetedEventDryRunSkipsAllWrites(t *testing.T) {
	gamma := &fakeTargetedGamma{event: sampleTargetedEvent()}
	prices := &fakeTargetedPriceFetcher{bid: 0.42, ask: 0.58}
	cfg := targetedConfig{
		EventID:          "446691",
		DryRun:           true,
		SyncPrices:       true,
		MaxPriceTokens:   10,
		PriceConcurrency: 2,
		HTTPTimeout:      time.Second,
		GammaBaseURL:     "https://gamma.example",
		Output:           "json",
	}
	var out bytes.Buffer
	if err := runTargetedEvent(context.Background(), cfg, gamma, nil, prices, &out); err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if atomic.LoadInt32(&prices.calls) != 0 {
		t.Fatalf("dry-run must not fetch prices; calls=%d", prices.calls)
	}
	var report targetedReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if !report.DryRun {
		t.Fatal("report.DryRun should be true")
	}
	// Reported counts reflect what WOULD be upserted but no DB write happened.
	if report.UpsertCounts.TokensUpserted != 2 {
		t.Fatalf("dry-run should still report tokens-that-would-upsert=2; got %+v", report.UpsertCounts)
	}
	if report.PriceSnapshotCounts.Inserted != 0 {
		t.Fatalf("dry-run must not insert prices; got %+v", report.PriceSnapshotCounts)
	}
	notes := strings.Join(report.Notes, " | ")
	if !strings.Contains(notes, "--dry-run is on; no rows were written") {
		t.Fatalf("dry-run note missing: %v", report.Notes)
	}
	if !strings.Contains(notes, "--dry-run is on; no CLOB price snapshots") {
		t.Fatalf("dry-run price note missing: %v", report.Notes)
	}
}

func TestRunTargetedEventSafelyHandlesClosedEvent(t *testing.T) {
	yes := true
	closedEvent := sampleTargetedEvent()
	closedEvent.Closed = &yes
	closedEvent.Active = nil

	gamma := &fakeTargetedGamma{event: closedEvent}
	st := &fakeTargetedStore{}
	cfg := targetedConfig{
		EventID:     "446691",
		SyncPrices:  false,
		Output:      "json",
		HTTPTimeout: time.Second,
	}
	var out bytes.Buffer
	if err := runTargetedEvent(context.Background(), cfg, gamma, st, nil, &out); err != nil {
		t.Fatalf("runTargetedEvent: %v", err)
	}
	var report targetedReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode: %v", err)
	}
	joined := strings.ToLower(strings.Join(report.OperatorNextActions, " | "))
	if !strings.Contains(joined, "historical validation") {
		t.Fatalf("closed event must surface historical-validation hint: %v", report.OperatorNextActions)
	}
}

func TestRunTargetedEventErrorsWhenMissingStoreAndNotDryRun(t *testing.T) {
	gamma := &fakeTargetedGamma{event: sampleTargetedEvent()}
	cfg := targetedConfig{EventID: "446691", Output: "json", HTTPTimeout: time.Second}
	err := runTargetedEvent(context.Background(), cfg, gamma, nil, nil, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "requires a store") {
		t.Fatalf("expected store-required error, got %v", err)
	}
}

func TestRunTargetedEventReportsGammaFailureClearly(t *testing.T) {
	gamma := &fakeTargetedGamma{err: errors.New("gamma 404 not found")}
	cfg := targetedConfig{EventID: "999999", Output: "json", HTTPTimeout: time.Second}
	err := runTargetedEvent(context.Background(), cfg, gamma, &fakeTargetedStore{}, nil, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "fetch polymarket event 999999") {
		t.Fatalf("expected fetch error wrapper, got %v", err)
	}
}

func TestRunTargetedEventOutputContainsNoTradingTerminology(t *testing.T) {
	gamma := &fakeTargetedGamma{event: sampleTargetedEvent()}
	st := &fakeTargetedStore{}
	prices := &fakeTargetedPriceFetcher{bid: 0.42, ask: 0.58}
	cfg := targetedConfig{
		EventID:          "446691",
		SyncPrices:       true,
		MaxPriceTokens:   10,
		PriceConcurrency: 2,
		HTTPTimeout:      time.Second,
		Output:           "json",
	}
	var out bytes.Buffer
	if err := runTargetedEvent(context.Background(), cfg, gamma, st, prices, &out); err != nil {
		t.Fatalf("runTargetedEvent: %v", err)
	}
	body := strings.ToLower(out.String())
	for _, forbidden := range []string{
		"emits a trading signal",
		"emits a buy signal",
		"emits a sell signal",
		"places an order",
		"submits an order",
		"automatic execution",
		"recommended trade",
		"auto-mapped",
		"approved alias",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("output contains forbidden phrase %q:\n%s", forbidden, body)
		}
	}
}

func TestRunTargetedEventTableOutputIncludesNextActions(t *testing.T) {
	gamma := &fakeTargetedGamma{event: sampleTargetedEvent()}
	st := &fakeTargetedStore{}
	prices := &fakeTargetedPriceFetcher{bid: 0.42, ask: 0.58}
	cfg := targetedConfig{
		EventID:          "446691",
		SyncPrices:       true,
		MaxPriceTokens:   10,
		PriceConcurrency: 2,
		HTTPTimeout:      time.Second,
		Output:           "table",
		GammaBaseURL:     "https://gamma.example",
	}
	var out bytes.Buffer
	if err := runTargetedEvent(context.Background(), cfg, gamma, st, prices, &out); err != nil {
		t.Fatalf("runTargetedEvent: %v", err)
	}
	output := out.String()
	for _, want := range []string{
		"event_id=446691",
		"https://gamma.example/events/446691",
		"upsert_counts events=1 markets=1 tokens=2",
		"price_snapshots attempted=2 inserted=2",
		"market mkt-cavs-win",
		"next_action: this command does NOT approve a Polymarket alias",
		"next_action: manually inspect",
		"note: targeted sync is read-only",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("table output missing %q:\n%s", want, output)
		}
	}
}
