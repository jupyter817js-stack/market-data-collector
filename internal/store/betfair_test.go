package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

func TestBetfairPersistenceMigrationShape(t *testing.T) {
	sql := readMigration(t, "012_betfair_persistence.sql")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS betfair_raw_stream_messages",
		"CREATE TABLE IF NOT EXISTS betfair_exchange_quote_history",
		"CREATE TABLE IF NOT EXISTS betfair_latest_exchange_quotes",
		"CREATE OR REPLACE VIEW betfair_latest_exchange_quotes_view",
		"exchange_side IN ('back', 'lay')",
		"price_decimal > 1.0",
		"available_size > 0",
		"stale_state IN ('fresh', 'aging', 'stale', 'unknown')",
		"betfair_raw_stream_messages_market_received_idx",
		"betfair_exchange_quote_history_key_received_idx",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("betfair migration missing %q:\n%s", want, sql)
		}
	}
}

func TestBetfairQuoteHistorySQLIsChangeOnly(t *testing.T) {
	if strings.Contains(insertBetfairExchangeQuoteHistorySQL, "ON CONFLICT") {
		t.Fatalf("betfair quote history insert must remain append-only:\n%s", insertBetfairExchangeQuoteHistorySQL)
	}
	if !strings.Contains(upsertBetfairLatestExchangeQuoteSQL, "ON CONFLICT (quote_key)") {
		t.Fatalf("latest betfair quote upsert must key on quote_key:\n%s", upsertBetfairLatestExchangeQuoteSQL)
	}
}

func TestBetfairQuoteCandidateValidation(t *testing.T) {
	valid := betfairCandidateFixture("validation-key", "validation-hash", 2.1, 20)
	if _, ok := normalizeBetfairQuoteCandidate(valid); !ok {
		t.Fatal("valid betfair candidate was rejected")
	}

	tests := []struct {
		name   string
		mutate func(*model.BetfairExchangeQuoteCandidate)
	}{
		{"missing market id", func(c *model.BetfairExchangeQuoteCandidate) { c.MarketID = "" }},
		{"missing selection id", func(c *model.BetfairExchangeQuoteCandidate) { c.SelectionID = 0 }},
		{"unknown side", func(c *model.BetfairExchangeQuoteCandidate) { c.ExchangeSide = "mid" }},
		{"price too low", func(c *model.BetfairExchangeQuoteCandidate) { c.PriceDecimal = 1 }},
		{"zero size", func(c *model.BetfairExchangeQuoteCandidate) { c.AvailableSize = 0 }},
		{"negative size", func(c *model.BetfairExchangeQuoteCandidate) { c.AvailableSize = -1 }},
		{"empty quote key", func(c *model.BetfairExchangeQuoteCandidate) { c.QuoteKey = "" }},
		{"empty quote hash", func(c *model.BetfairExchangeQuoteCandidate) { c.QuoteHash = "" }},
		{"invalid stale state", func(c *model.BetfairExchangeQuoteCandidate) { c.StaleState = "fresh-ish" }},
		{"invalid confidence", func(c *model.BetfairExchangeQuoteCandidate) { c.ConfidenceScore = 101 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := valid
			tc.mutate(&candidate)
			if _, ok := normalizeBetfairQuoteCandidate(candidate); ok {
				t.Fatalf("invalid candidate accepted: %+v", candidate)
			}
		})
	}
}

func TestBetfairRawMessagePayloadHashIsDeterministic(t *testing.T) {
	raw := `{"op":"mcm","id":1}`
	first := betfairPayloadHash(raw)
	second := betfairPayloadHash(raw)
	if first == "" || first != second {
		t.Fatalf("payload hash first=%q second=%q", first, second)
	}
	if first == betfairPayloadHash(`{"op":"heartbeat"}`) {
		t.Fatal("different payloads produced the same fixture hash")
	}
}

func betfairCandidateFixture(key, hash string, price, size float64) model.BetfairExchangeQuoteCandidate {
	inPlay := false
	lastTraded := 2.05
	tradedVolume := 100.0
	sourcePublishMS := int64(1715000000000)
	sourcePublishTime := time.UnixMilli(sourcePublishMS).UTC()
	reducerUpdatedAt := sourcePublishTime.Add(time.Second)
	ageMS := int64(60000)
	return model.BetfairExchangeQuoteCandidate{
		Source:              model.SourceBetfair,
		QuoteKey:            key,
		QuoteHash:           hash,
		MarketID:            "1.234",
		SelectionID:         101,
		Handicap:            0,
		RunnerName:          "Home FC",
		MarketName:          "Match Odds",
		EventID:             "event-1",
		EventName:           "Home FC v Away FC",
		MarketFamily:        "match_odds",
		ExchangeSide:        "back",
		PriceDecimal:        price,
		AvailableSize:       size,
		LastTradedPrice:     &lastTraded,
		TradedVolume:        &tradedVolume,
		MarketStatus:        "OPEN",
		InPlay:              &inPlay,
		MarketTime:          "2026-05-07T12:00:00Z",
		SourcePublishTimeMS: &sourcePublishMS,
		SourcePublishTime:   &sourcePublishTime,
		ReducerUpdatedAt:    &reducerUpdatedAt,
		ReceivedAt:          sourcePublishTime.Add(time.Minute),
		AgeMS:               &ageMS,
		StaleState:          "fresh",
		ConfidenceScore:     80,
		RawMetadata:         json.RawMessage(`{"fixture":"betfair-candidate"}`),
	}
}
