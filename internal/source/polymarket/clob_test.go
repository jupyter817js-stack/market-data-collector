package polymarket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCLOBGetPrice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertNoAuthHeaders(t, r)
		if r.URL.Path != "/price" {
			t.Errorf("path=%q want /price", r.URL.Path)
		}
		if r.URL.Query().Get("token_id") != "tok-1" {
			t.Errorf("token_id=%q want tok-1", r.URL.Query().Get("token_id"))
		}
		if r.URL.Query().Get("side") != "BUY" {
			t.Errorf("side=%q want BUY", r.URL.Query().Get("side"))
		}
		_, _ = w.Write([]byte(`{"price":0.45}`))
	}))
	defer server.Close()

	client := newTestCLOBClient(server.URL, 0)
	price, err := client.GetPrice(context.Background(), " tok-1 ", Side("buy"))
	if err != nil {
		t.Fatalf("GetPrice returned error: %v", err)
	}
	if price.Price != 0.45 {
		t.Fatalf("Price=%v want 0.45", price.Price)
	}
	if len(price.Raw) == 0 {
		t.Fatal("Raw is empty")
	}
}

func TestCLOBGetPrices(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertNoAuthHeaders(t, r)
		if r.URL.Path != "/prices" {
			t.Errorf("path=%q want /prices", r.URL.Path)
		}
		if r.URL.Query().Get("token_ids") != "tok-1,tok-2" {
			t.Errorf("token_ids=%q want tok-1,tok-2", r.URL.Query().Get("token_ids"))
		}
		if r.URL.Query().Get("sides") != "BUY,SELL" {
			t.Errorf("sides=%q want BUY,SELL", r.URL.Query().Get("sides"))
		}
		_, _ = w.Write([]byte(`{
			"tok-1": {"BUY": 0.45},
			"tok-2": {"SELL": 0.52}
		}`))
	}))
	defer server.Close()

	client := newTestCLOBClient(server.URL, 0)
	prices, err := client.GetPrices(context.Background(), []CLOBPriceRequest{
		{TokenID: "tok-1", Side: SideBuy},
		{TokenID: "tok-2", Side: SideSell},
	})
	if err != nil {
		t.Fatalf("GetPrices returned error: %v", err)
	}
	if prices.Prices["tok-1"][SideBuy] != 0.45 {
		t.Fatalf("tok-1 BUY=%v want 0.45", prices.Prices["tok-1"][SideBuy])
	}
	if prices.Prices["tok-2"][SideSell] != 0.52 {
		t.Fatalf("tok-2 SELL=%v want 0.52", prices.Prices["tok-2"][SideSell])
	}
	if len(prices.Raw) == 0 {
		t.Fatal("Raw is empty")
	}
}

func TestCLOBGetOrderBook(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertNoAuthHeaders(t, r)
		if r.URL.Path != "/book" {
			t.Errorf("path=%q want /book", r.URL.Path)
		}
		if r.URL.Query().Get("token_id") != "tok-1" {
			t.Errorf("token_id=%q want tok-1", r.URL.Query().Get("token_id"))
		}
		_, _ = w.Write([]byte(`{
			"market": "cond-1",
			"asset_id": "tok-1",
			"timestamp": "1234567890",
			"hash": "hash-1",
			"bids": [{"price": "0.45", "size": "100"}],
			"asks": [{"price": "0.46", "size": "150"}],
			"min_order_size": "1",
			"tick_size": "0.01",
			"neg_risk": false,
			"last_trade_price": "0.45"
		}`))
	}))
	defer server.Close()

	client := newTestCLOBClient(server.URL, 0)
	book, err := client.GetOrderBook(context.Background(), "tok-1")
	if err != nil {
		t.Fatalf("GetOrderBook returned error: %v", err)
	}
	if book.Market != "cond-1" || book.AssetID != "tok-1" || book.Hash != "hash-1" {
		t.Fatalf("book=%+v", book)
	}
	if len(book.Bids) != 1 || book.Bids[0].Price != "0.45" || book.Bids[0].Size != "100" {
		t.Fatalf("Bids=%+v", book.Bids)
	}
	if len(book.Asks) != 1 || book.Asks[0].Price != "0.46" || book.Asks[0].Size != "150" {
		t.Fatalf("Asks=%+v", book.Asks)
	}
	if book.MinOrderSize != "1" || book.TickSize != "0.01" || book.LastTradePrice != "0.45" {
		t.Fatalf("book sizing fields=%+v", book)
	}
	if len(book.Raw) == 0 || len(book.Bids[0].Raw) == 0 {
		t.Fatal("raw JSON should be preserved")
	}
}

func TestCLOBGetMidpointAndSpread(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertNoAuthHeaders(t, r)
		if r.URL.Query().Get("token_id") != "tok-1" {
			t.Errorf("token_id=%q want tok-1", r.URL.Query().Get("token_id"))
		}
		switch r.URL.Path {
		case "/midpoint":
			_, _ = w.Write([]byte(`{"mid_price":"0.45"}`))
		case "/spread":
			_, _ = w.Write([]byte(`{"spread":"0.02"}`))
		default:
			t.Errorf("unexpected path=%q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestCLOBClient(server.URL, 0)
	midpoint, err := client.GetMidpoint(context.Background(), "tok-1")
	if err != nil {
		t.Fatalf("GetMidpoint returned error: %v", err)
	}
	if midpoint.MidPrice != "0.45" || len(midpoint.Raw) == 0 {
		t.Fatalf("midpoint=%+v", midpoint)
	}

	spread, err := client.GetSpread(context.Background(), "tok-1")
	if err != nil {
		t.Fatalf("GetSpread returned error: %v", err)
	}
	if spread.Spread != "0.02" || len(spread.Raw) == 0 {
		t.Fatalf("spread=%+v", spread)
	}
}

func TestCLOBOrderBookParsesNumericLevelFields(t *testing.T) {
	var book CLOBOrderBook
	if err := json.Unmarshal([]byte(`{
		"market": "cond-1",
		"asset_id": "tok-1",
		"timestamp": 1715000000000,
		"bids": [{"price": 0.44, "size": 25}],
		"asks": [{"price": 0.47, "size": 30}],
		"lastTradePrice": 0.46
	}`), &book); err != nil {
		t.Fatalf("unmarshal book: %v", err)
	}
	if book.Timestamp != "1715000000000" {
		t.Fatalf("Timestamp=%q want 1715000000000", book.Timestamp)
	}
	if book.Bids[0].Price != "0.44" || book.Bids[0].Size != "25" {
		t.Fatalf("bid level=%+v", book.Bids[0])
	}
	if book.Asks[0].Price != "0.47" || book.Asks[0].Size != "30" {
		t.Fatalf("ask level=%+v", book.Asks[0])
	}
	if book.LastTradePrice != "0.46" {
		t.Fatalf("LastTradePrice=%q want 0.46", book.LastTradePrice)
	}
}

func TestCLOBSpreadParsesNumericField(t *testing.T) {
	var spread CLOBSpread
	if err := json.Unmarshal([]byte(`{"spread":0.03}`), &spread); err != nil {
		t.Fatalf("unmarshal spread: %v", err)
	}
	if spread.Spread != "0.03" {
		t.Fatalf("Spread=%q want 0.03", spread.Spread)
	}
}

func TestCLOBRetryOn500ThenSuccess(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			http.Error(w, "temporary", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"spread":"0.02"}`))
	}))
	defer server.Close()

	client := newTestCLOBClient(server.URL, 1)
	spread, err := client.GetSpread(context.Background(), "tok-1")
	if err != nil {
		t.Fatalf("GetSpread returned error: %v", err)
	}
	if spread.Spread != "0.02" {
		t.Fatalf("Spread=%q want 0.02", spread.Spread)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

func TestCLOBNoRetryOn400(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, "invalid token", http.StatusBadRequest)
	}))
	defer server.Close()

	client := newTestCLOBClient(server.URL, 3)
	_, err := client.GetMidpoint(context.Background(), "tok-1")
	if err == nil {
		t.Fatal("GetMidpoint returned nil error")
	}
	if !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("error=%q want status 400", err.Error())
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
}

func TestCLOBContextCancellationStopsRetrying(t *testing.T) {
	attempts := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		cancel()
		http.Error(w, "temporary", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := newTestCLOBClient(server.URL, 3)
	_, err := client.GetOrderBook(ctx, "tok-1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context.Canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
}

func TestCLOBValidation(t *testing.T) {
	client := newTestCLOBClient("http://127.0.0.1:1", 0)

	if _, err := client.GetPrice(context.Background(), "", SideBuy); err == nil {
		t.Fatal("GetPrice with empty token returned nil error")
	}
	if _, err := client.GetPrice(context.Background(), "tok-1", Side("HOLD")); err == nil {
		t.Fatal("GetPrice with invalid side returned nil error")
	}
	if _, err := client.GetPrices(context.Background(), nil); err == nil {
		t.Fatal("GetPrices with nil requests returned nil error")
	}
}

func newTestCLOBClient(baseURL string, maxRetries int) *CLOBClient {
	return NewCLOBClientWithRetry(baseURL, time.Second, RetryConfig{
		MaxRetries: maxRetries,
		BaseDelay:  time.Nanosecond,
		MaxDelay:   time.Nanosecond,
	})
}

func assertNoAuthHeaders(t *testing.T, r *http.Request) {
	t.Helper()
	if r.Header.Get("Authorization") != "" {
		t.Fatalf("Authorization header should not be set")
	}
	for key := range r.Header {
		if strings.HasPrefix(strings.ToUpper(key), "POLY_") {
			t.Fatalf("unexpected auth header %q", key)
		}
	}
}
