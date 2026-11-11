package polymarket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestListSportsDecodesArrayAndPagination(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sports" {
			t.Errorf("path=%q want /sports", r.URL.Path)
		}
		if r.URL.Query().Get("limit") != "2" {
			t.Errorf("limit=%q want 2", r.URL.Query().Get("limit"))
		}
		if r.URL.Query().Get("offset") != "1" {
			t.Errorf("offset=%q want 1", r.URL.Query().Get("offset"))
		}
		if r.Header.Get("Authorization") != "" {
			t.Errorf("Authorization header should not be set")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{
				"id": 12,
				"slug": "nba",
				"title": "NBA",
				"category": "basketball",
				"tags": [{"id": "tag-1", "slug": "sports", "label": "Sports"}]
			}
		]`))
	}))
	defer server.Close()

	client := newTestClient(server.URL, 0)
	sports, err := client.ListSports(context.Background(), ListOptions{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("ListSports returned error: %v", err)
	}
	if len(sports) != 1 {
		t.Fatalf("len(sports)=%d want 1", len(sports))
	}
	if sports[0].ID != "12" || sports[0].Slug != "nba" || sports[0].Title != "NBA" {
		t.Fatalf("sport=%+v", sports[0])
	}
	if sports[0].Category != "basketball" {
		t.Fatalf("Category=%q want basketball", sports[0].Category)
	}
	if len(sports[0].Tags) != 1 || sports[0].Tags[0].ID != "tag-1" {
		t.Fatalf("Tags=%+v", sports[0].Tags)
	}
	if len(sports[0].Raw) == 0 {
		t.Fatal("Raw is empty")
	}
}

func TestListEventsDecodesEnvelopeAndNestedMarkets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"events": [
				{
					"id": "evt-1",
					"slug": "nba-finals",
					"title": "NBA Finals",
					"startDate": "2026-06-01T00:00:00Z",
					"endDate": "2026-06-02T00:00:00Z",
					"active": true,
					"closed": false,
					"archived": false,
					"sport": "basketball",
					"category": "Sports",
					"tags": [{"id": "nba", "label": "NBA"}],
					"markets": [
						{
							"id": "mkt-1",
							"conditionId": "cond-1",
							"slug": "celtics-title",
							"question": "Will Celtics win?",
							"active": true,
							"closed": false,
							"clobTokenIds": "[\"tok_yes\",\"tok_no\"]",
							"outcomes": "[\"Yes\",\"No\"]"
						}
					]
				}
			]
		}`))
	}))
	defer server.Close()

	client := newTestClient(server.URL, 0)
	events, err := client.ListEvents(context.Background(), ListOptions{})
	if err != nil {
		t.Fatalf("ListEvents returned error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events)=%d want 1", len(events))
	}
	event := events[0]
	if event.ID != "evt-1" || event.Slug != "nba-finals" || event.Title != "NBA Finals" {
		t.Fatalf("event=%+v", event)
	}
	if event.StartTime == nil || !event.StartTime.Equal(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("StartTime=%v", event.StartTime)
	}
	if event.EndTime == nil || !event.EndTime.Equal(time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("EndTime=%v", event.EndTime)
	}
	if event.Active == nil || !*event.Active || event.Closed == nil || *event.Closed || event.Archived == nil || *event.Archived {
		t.Fatalf("event status active=%v closed=%v archived=%v", event.Active, event.Closed, event.Archived)
	}
	if len(event.Markets) != 1 {
		t.Fatalf("len(Markets)=%d want 1", len(event.Markets))
	}
	market := event.Markets[0]
	if market.ID != "mkt-1" || market.ConditionID != "cond-1" || market.Question != "Will Celtics win?" {
		t.Fatalf("market=%+v", market)
	}
	if market.Active == nil || !*market.Active || market.Closed == nil || *market.Closed {
		t.Fatalf("market status active=%v closed=%v", market.Active, market.Closed)
	}
	if len(market.Tokens) != 2 {
		t.Fatalf("len(Tokens)=%d want 2", len(market.Tokens))
	}
	if market.Tokens[0].TokenID != "tok_yes" || market.Tokens[0].OutcomeName != "Yes" {
		t.Fatalf("Tokens[0]=%+v", market.Tokens[0])
	}
	if market.Tokens[1].TokenID != "tok_no" || market.Tokens[1].OutcomeName != "No" {
		t.Fatalf("Tokens[1]=%+v", market.Tokens[1])
	}
	if len(event.Raw) == 0 || len(market.Raw) == 0 {
		t.Fatal("raw JSON should be preserved")
	}
}

func TestListMarketsDecodesDataEnvelopeAndTokens(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"data": [
				{
					"id": "mkt-2",
					"condition_id": "cond-2",
					"title": "NBA champion",
					"startTime": "2026-06-02T03:00:00Z",
					"tokens": [
						{"token_id": "tok_a", "outcome_id": "out_a", "outcome": "Team A", "outcomeIndex": 0},
						{"tokenId": "tok_b", "outcomeId": "out_b", "outcomeName": "Team B", "index": 1}
					]
				}
			]
		}`))
	}))
	defer server.Close()

	client := newTestClient(server.URL, 0)
	markets, err := client.ListMarkets(context.Background(), ListOptions{})
	if err != nil {
		t.Fatalf("ListMarkets returned error: %v", err)
	}
	if len(markets) != 1 {
		t.Fatalf("len(markets)=%d want 1", len(markets))
	}
	market := markets[0]
	if market.ID != "mkt-2" || market.ConditionID != "cond-2" || market.Title != "NBA champion" {
		t.Fatalf("market=%+v", market)
	}
	if market.StartTime == nil || !market.StartTime.Equal(time.Date(2026, 6, 2, 3, 0, 0, 0, time.UTC)) {
		t.Fatalf("StartTime=%v", market.StartTime)
	}
	if len(market.Tokens) != 2 {
		t.Fatalf("len(Tokens)=%d want 2", len(market.Tokens))
	}
	if market.Tokens[1].TokenID != "tok_b" || market.Tokens[1].OutcomeID != "out_b" || market.Tokens[1].OutcomeIndex != 1 {
		t.Fatalf("Tokens[1]=%+v", market.Tokens[1])
	}
}

func TestRetryOn500ThenSuccess(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			http.Error(w, "temporary", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client := newTestClient(server.URL, 1)
	markets, err := client.ListMarkets(context.Background(), ListOptions{})
	if err != nil {
		t.Fatalf("ListMarkets returned error: %v", err)
	}
	if len(markets) != 0 {
		t.Fatalf("len(markets)=%d want 0", len(markets))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

func TestRetryOn429ThenSuccess(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client := newTestClient(server.URL, 1)
	events, err := client.ListEvents(context.Background(), ListOptions{})
	if err != nil {
		t.Fatalf("ListEvents returned error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("len(events)=%d want 0", len(events))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

func TestNoRetryOn404(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer server.Close()

	client := newTestClient(server.URL, 3)
	_, err := client.ListSports(context.Background(), ListOptions{})
	if err == nil {
		t.Fatal("ListSports returned nil error")
	}
	if !strings.Contains(err.Error(), "status 404") {
		t.Fatalf("error=%q want status 404", err.Error())
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
}

func TestContextCancellationStopsRetrying(t *testing.T) {
	attempts := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		cancel()
		http.Error(w, "temporary", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := newTestClient(server.URL, 3)
	_, err := client.ListEvents(ctx, ListOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context.Canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
}

func newTestClient(baseURL string, maxRetries int) *Client {
	return NewClientWithRetry(baseURL, time.Second, RetryConfig{
		MaxRetries: maxRetries,
		BaseDelay:  time.Nanosecond,
		MaxDelay:   time.Nanosecond,
	})
}

// TestEndpointForwardsLifecycleAndOrderFiltersToGamma pins the Phase 12E
// extension that closed/active/archived/order/ascending/tag_slug/tag_id/
// category are forwarded as query parameters to the Gamma list endpoints.
// Before this change the Gamma client only sent limit+offset, so default
// pagination returned the oldest historical markets first and current sports
// markets were never reached within the bounded page budget.
func TestEndpointForwardsLifecycleAndOrderFiltersToGamma(t *testing.T) {
	closedFalse := false
	asc := false
	activeTrue := true
	client := newTestClient("https://gamma.example", 0)
	got, err := client.endpoint("/events", ListOptions{
		Limit:     50,
		Offset:    100,
		Active:    &activeTrue,
		Closed:    &closedFalse,
		Order:     "endDate",
		Ascending: &asc,
		TagSlug:   "nba",
		Category:  "sports",
	})
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	for _, want := range []string{
		"limit=50",
		"offset=100",
		"active=true",
		"closed=false",
		"order=endDate",
		"ascending=false",
		"tag_slug=nba",
		"category=sports",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("endpoint=%q missing %q", got, want)
		}
	}
}

// TestGetEventDecodesBareObjectArrayAndEnvelope pins that GetEvent accepts
// the three Gamma response shapes a /events/{id} endpoint has been observed
// returning: a bare event object, a single-element array, or an
// {"event": {...}} envelope. The Phase 12F targeted sync depends on this
// tolerance so the operator does not have to know which Gamma deployment is
// in use before invoking --target-event-id.
func TestGetEventDecodesBareObjectArrayAndEnvelope(t *testing.T) {
	body := `{
        "id": "446691",
        "slug": "pistons-vs-cavaliers",
        "title": "Pistons vs. Cavaliers",
        "active": true,
        "closed": false,
        "archived": false,
        "startTime": "2026-04-01 07:32:39+00",
        "endTime": "2026-04-01 10:00:00+00:00",
        "markets": [
            {"id": "mkt-1", "conditionId": "cond-1", "question": "Cavaliers beat Pistons?", "outcomes": ["Yes","No"], "clobTokenIds": ["a","b"]}
        ]
    }`
	cases := []struct {
		name string
		resp string
	}{
		{name: "bare_object", resp: body},
		{name: "single_element_array", resp: "[" + body + "]"},
		{name: "envelope_event_key", resp: `{"event": ` + body + `}`},
		{name: "envelope_data_key", resp: `{"data": ` + body + `}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var receivedPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedPath = r.URL.Path
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.resp))
			}))
			defer server.Close()
			client := newTestClient(server.URL, 0)
			event, err := client.GetEvent(context.Background(), "446691")
			if err != nil {
				t.Fatalf("GetEvent: %v", err)
			}
			if receivedPath != "/events/446691" {
				t.Fatalf("server saw path=%q want /events/446691", receivedPath)
			}
			if event.ID != "446691" || event.Title != "Pistons vs. Cavaliers" {
				t.Fatalf("decoded event=%+v", event)
			}
			if len(event.Markets) != 1 || event.Markets[0].ConditionID != "cond-1" {
				t.Fatalf("decoded markets=%+v", event.Markets)
			}
		})
	}
}

func TestGetEventValidatesEmptyID(t *testing.T) {
	client := newTestClient("http://invalid.example", 0)
	if _, err := client.GetEvent(context.Background(), ""); err == nil {
		t.Fatal("empty event id must error before any HTTP call")
	}
}

func TestGetEventReturnsClearErrorOnNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	}))
	defer server.Close()
	client := newTestClient(server.URL, 1)
	_, err := client.GetEvent(context.Background(), "999999")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err=%v want a not-found error", err)
	}
}

// TestEndpointDefaultsKeepBackwardCompatibleQueryString proves the existing
// (pre-12E) call sites are not silently re-routed: with a zero-value
// ListOptions the only query string contributions remain limit/offset.
func TestEndpointDefaultsKeepBackwardCompatibleQueryString(t *testing.T) {
	client := newTestClient("https://gamma.example", 0)
	got, err := client.endpoint("/markets", ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	for _, forbidden := range []string{
		"closed=", "active=", "archived=", "order=", "ascending=", "tag_slug=", "tag_id=", "category=",
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("default endpoint must not include %q; got %q", forbidden, got)
		}
	}
}
