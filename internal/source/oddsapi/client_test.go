package oddsapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testAPIKey = "test-secret-key"

func TestFetchOddsRetriesOn500ThenSuccess(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "temporary", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client := newTestClient(server.URL, RetryConfig{
		MaxRetries: 2,
		BaseDelay:  time.Millisecond,
		MaxDelay:   time.Millisecond,
	})

	batch, err := client.FetchOdds(context.Background(), "basketball_nba")
	if err != nil {
		t.Fatalf("FetchOdds returned error: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts=%d want 2", got)
	}
	if batch.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want %d", batch.StatusCode, http.StatusOK)
	}
}

func TestFetchOddsRetriesOn429ThenSuccess(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "10")
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client := newTestClient(server.URL, RetryConfig{
		MaxRetries: 2,
		BaseDelay:  time.Millisecond,
		MaxDelay:   time.Millisecond,
	})

	if _, err := client.FetchOdds(context.Background(), "basketball_nba"); err != nil {
		t.Fatalf("FetchOdds returned error: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts=%d want 2", got)
	}
}

func TestFetchOddsDoesNotRetryOn401(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	client := newTestClient(server.URL, RetryConfig{
		MaxRetries: 3,
		BaseDelay:  time.Millisecond,
		MaxDelay:   time.Millisecond,
	})

	batch, err := client.FetchOdds(context.Background(), "basketball_nba")
	if err == nil {
		t.Fatal("FetchOdds returned nil error")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts=%d want 1", got)
	}
	assertNoAPIKey(t, batch.RequestURL)
	assertNoAPIKey(t, err.Error())
}

func TestFetchOddsContextCancellationStopsRetrying(t *testing.T) {
	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"temporary"}`))
		cancel()
	}))
	defer server.Close()

	client := newTestClient(server.URL, RetryConfig{
		MaxRetries: 3,
		BaseDelay:  50 * time.Millisecond,
		MaxDelay:   50 * time.Millisecond,
	})

	_, err := client.FetchOdds(ctx, "basketball_nba")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context.Canceled", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts=%d want 1", got)
	}
}

func TestFetchOddsRedactsAPIKeyInRequestURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("apiKey"); got != testAPIKey {
			t.Fatalf("request apiKey=%q want test key", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client := newTestClient(server.URL, RetryConfig{
		MaxRetries: 0,
		BaseDelay:  time.Millisecond,
		MaxDelay:   time.Millisecond,
	})

	batch, err := client.FetchOdds(context.Background(), "basketball_nba")
	if err != nil {
		t.Fatalf("FetchOdds returned error: %v", err)
	}
	assertNoAPIKey(t, batch.RequestURL)

	u, err := url.Parse(batch.RequestURL)
	if err != nil {
		t.Fatalf("parse RequestURL: %v", err)
	}
	if got := u.Query().Get("apiKey"); got != "redacted" {
		t.Fatalf("redacted apiKey=%q want redacted", got)
	}
}

func TestFetchOddsDecodesFixtureResponse(t *testing.T) {
	fixture := readOddsAPIFixture(t)
	var sawRequest atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest.Store(true)
		if got := r.URL.Query().Get("apiKey"); got != testAPIKey {
			t.Fatalf("request apiKey=%q want test key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fixture)
	}))
	defer server.Close()

	client := newTestClient(server.URL, RetryConfig{
		MaxRetries: 0,
		BaseDelay:  time.Millisecond,
		MaxDelay:   time.Millisecond,
	})

	batch, err := client.FetchOdds(context.Background(), "basketball_nba")
	if err != nil {
		t.Fatalf("FetchOdds returned error: %v", err)
	}
	if !sawRequest.Load() {
		t.Fatal("test server did not receive request")
	}
	if len(batch.Events) != 2 {
		t.Fatalf("events len=%d want 2", len(batch.Events))
	}
	if batch.Events[0].ID != "fixture-event-prematch" {
		t.Fatalf("first event ID=%q", batch.Events[0].ID)
	}
	if got := batch.Events[0].Bookmakers[0].Markets[0].Key; got != "h2h" {
		t.Fatalf("first market key=%q want h2h", got)
	}
	assertNoAPIKey(t, batch.RequestURL)
}

func newTestClient(baseURL string, retry RetryConfig) *Client {
	return NewClientWithRetry(testAPIKey, baseURL, "us", "h2h", "", time.Second, retry)
}

func readOddsAPIFixture(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "..", "testdata", "oddsapi", "sample_odds_response.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return body
}

func assertNoAPIKey(t *testing.T, value string) {
	t.Helper()
	if strings.Contains(value, testAPIKey) {
		t.Fatalf("value exposed API key: %q", value)
	}
}
