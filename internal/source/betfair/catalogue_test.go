package betfair

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestCatalogueClientListMarketCatalogueRequestAndParse(t *testing.T) {
	appKey := "CATALOGUE_APP_SECRET"
	sessionToken := "CATALOGUE_SESSION_SECRET"
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%s want POST", r.Method)
		}
		if r.Header.Get("X-Application") != appKey || r.Header.Get("X-Authentication") != sessionToken {
			t.Fatalf("auth headers app=%q session=%q", r.Header.Get("X-Application"), r.Header.Get("X-Authentication"))
		}
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		_, _ = w.Write([]byte(`{
			"jsonrpc":"2.0",
			"id":1,
			"result":[{
				"marketId":"1.257744548",
				"marketName":"Match Odds",
				"marketStartTime":"2026-05-09T12:00:00.000Z",
				"event":{"id":"35561874","name":"Home FC v Away FC"},
				"eventType":{"id":"1","name":"Soccer"},
				"competition":{"id":"99","name":"Test League"},
				"runners":[
					{"selectionId":10475264,"runnerName":"Home FC","handicap":0,"sortPriority":1,"metadata":{"runnerId":"10475264"}},
					{"selectionId":8839001,"runnerName":"Away FC","sortPriority":2},
					{"selectionId":58805,"runnerName":"The Draw","sortPriority":3}
				]
			}]
		}`))
	}))
	defer server.Close()

	client, err := NewCatalogueClient(CatalogueClientOptions{
		Endpoint:     server.URL,
		AppKey:       appKey,
		SessionToken: sessionToken,
		HTTPClient:   server.Client(),
	})
	if err != nil {
		t.Fatalf("new catalogue client: %v", err)
	}
	markets, err := client.ListMarketCatalogue(context.Background(), CatalogueLookupOptions{
		MarketIDs: []string{"1.257744548"},
	})
	if err != nil {
		t.Fatalf("list catalogue: %v", err)
	}
	if requestBody["method"] != catalogueMethodListMarketCatalogue {
		t.Fatalf("request method=%+v", requestBody["method"])
	}
	params, ok := requestBody["params"].(map[string]any)
	if !ok {
		t.Fatalf("request params=%+v", requestBody["params"])
	}
	filter, ok := params["filter"].(map[string]any)
	if !ok {
		t.Fatalf("request filter=%+v", params["filter"])
	}
	if _, ok := filter["market_ids"]; ok {
		t.Fatalf("request used snake_case market_ids: %+v", filter)
	}
	marketIDs, ok := filter["marketIds"].([]any)
	if !ok || len(marketIDs) != 1 || marketIDs[0] != "1.257744548" {
		t.Fatalf("request marketIds=%+v", filter["marketIds"])
	}
	if _, ok := filter["eventIds"]; ok {
		t.Fatalf("request included broad event filter: %+v", filter)
	}
	if _, ok := requestBody["orderProjection"]; ok {
		t.Fatalf("request included order projection: %+v", requestBody)
	}
	projections, ok := params["marketProjection"].([]any)
	if !ok || len(projections) != len(DefaultCatalogueMarketProjection()) {
		t.Fatalf("request marketProjection=%+v", params["marketProjection"])
	}
	for _, required := range DefaultCatalogueMarketProjection() {
		found := false
		for _, projection := range projections {
			if projection == required {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("request projections=%+v missing %s", projections, required)
		}
	}
	if len(markets) != 1 {
		t.Fatalf("markets=%d want 1", len(markets))
	}
	market := markets[0]
	if market.MarketID != "1.257744548" || market.MarketName != "Match Odds" || market.Event.ID != "35561874" || market.Event.Name != "Home FC v Away FC" {
		t.Fatalf("market parsed=%+v", market)
	}
	if len(market.Runners) != 3 || market.Runners[0].SelectionID != 10475264 || market.Runners[0].RunnerName != "Home FC" {
		t.Fatalf("runners parsed=%+v", market.Runners)
	}
}

func TestCatalogueClientRedactsSecretsInErrors(t *testing.T) {
	appKey := "CATALOGUE_APP_SECRET"
	sessionToken := "CATALOGUE_SESSION_SECRET"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad "+appKey+" "+sessionToken, http.StatusForbidden)
	}))
	defer server.Close()

	client, err := NewCatalogueClient(CatalogueClientOptions{
		Endpoint:     server.URL,
		AppKey:       appKey,
		SessionToken: sessionToken,
		HTTPClient:   server.Client(),
	})
	if err != nil {
		t.Fatalf("new catalogue client: %v", err)
	}
	_, err = client.ListMarketCatalogue(context.Background(), CatalogueLookupOptions{MarketIDs: []string{"1.111"}})
	if err == nil {
		t.Fatal("list catalogue err=nil want error")
	}
	text := err.Error()
	if strings.Contains(text, appKey) || strings.Contains(text, sessionToken) {
		t.Fatalf("catalogue error leaked secrets: %s", text)
	}
	if !strings.Contains(text, redactedAppKey) || !strings.Contains(text, redactedSessionToken) {
		t.Fatalf("catalogue error missing redaction markers: %s", text)
	}
}

func TestValidateExplicitMarketIDs(t *testing.T) {
	got, err := ValidateExplicitMarketIDs([]string{" 1.1 ", "1.1", "1.2"}, 5)
	if err != nil {
		t.Fatalf("validate explicit market IDs: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"1.1", "1.2"}) {
		t.Fatalf("market IDs=%v", got)
	}
	tests := []struct {
		name string
		ids  []string
		want string
	}{
		{name: "missing", ids: nil, want: "at least one"},
		{name: "wildcard", ids: []string{"*"}, want: "wildcard"},
		{name: "too many", ids: []string{"1.1", "1.2", "1.3", "1.4", "1.5", "1.6"}, want: "too many"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateExplicitMarketIDs(tc.ids, 5)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want containing %q", err, tc.want)
			}
		})
	}
}
