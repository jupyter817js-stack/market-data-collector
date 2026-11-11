package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCatalogueLookupCommandJSONOutput(t *testing.T) {
	appKey := "LOOKUP_APP_SECRET"
	sessionToken := "LOOKUP_SESSION_SECRET"
	t.Setenv("LOOKUP_APP_KEY", appKey)
	t.Setenv("LOOKUP_SESSION_TOKEN", sessionToken)
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Application") != appKey || r.Header.Get("X-Authentication") != sessionToken {
			t.Fatalf("headers app=%q session=%q", r.Header.Get("X-Application"), r.Header.Get("X-Authentication"))
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
					{"selectionId":10475264,"runnerName":"Home FC","handicap":0,"sortPriority":1},
					{"selectionId":8839001,"runnerName":"Away FC","sortPriority":2},
					{"selectionId":58805,"runnerName":"The Draw","sortPriority":3}
				]
			}]
		}`))
	}))
	defer server.Close()

	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--app-key-env", "LOOKUP_APP_KEY",
		"--session-token-env", "LOOKUP_SESSION_TOKEN",
		"--endpoint", server.URL,
		"--market-id", "1.257744548",
		"--output", "json",
	}, &out, server.Client())
	if err != nil {
		t.Fatalf("lookup command: %v output=%s", err, out.String())
	}
	var report lookupReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode lookup report %s: %v", out.String(), err)
	}
	if !report.ReadOnly || report.CollectorIntegration || report.AliasAutoWrite {
		t.Fatalf("boundary flags=%+v", report)
	}
	if report.MarketIDsRequested != 1 || len(report.Markets) != 1 {
		t.Fatalf("report counts=%+v", report)
	}
	market := report.Markets[0]
	if market.BetfairEventID != "35561874" || market.BetfairEventName != "Home FC v Away FC" ||
		market.BetfairMarketID != "1.257744548" || market.BetfairMarketName != "Match Odds" {
		t.Fatalf("market report=%+v", market)
	}
	if len(market.Runners) != 3 || market.Runners[0].SelectionID != 10475264 || market.Runners[0].RunnerName != "Home FC" {
		t.Fatalf("runner report=%+v", market.Runners)
	}
	params := requestBody["params"].(map[string]any)
	filter := params["filter"].(map[string]any)
	marketIDs := filter["marketIds"].([]any)
	if len(marketIDs) != 1 || marketIDs[0] != "1.257744548" {
		t.Fatalf("request marketIds=%+v", filter["marketIds"])
	}
	assertLookupNoSecrets(t, out.String(), appKey, sessionToken)
	if strings.Contains(out.String(), "auto-approve") || strings.Contains(out.String(), "auto-write") {
		t.Fatalf("lookup output suggested auto-mapping: %s", out.String())
	}
}

func TestCatalogueLookupCommandTextOutput(t *testing.T) {
	t.Setenv("LOOKUP_APP_KEY", "LOOKUP_APP_SECRET")
	t.Setenv("LOOKUP_SESSION_TOKEN", "LOOKUP_SESSION_SECRET")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":[{"marketId":"1.111","marketName":"Match Odds","event":{"id":"event-1","name":"Team A v Team B"},"runners":[{"selectionId":101,"runnerName":"Team A","sortPriority":1}]}]}`))
	}))
	defer server.Close()

	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--app-key-env", "LOOKUP_APP_KEY",
		"--session-token-env", "LOOKUP_SESSION_TOKEN",
		"--endpoint", server.URL,
		"--market-id", "1.111",
	}, &out, server.Client())
	if err != nil {
		t.Fatalf("lookup text command: %v output=%s", err, out.String())
	}
	text := out.String()
	if !strings.Contains(text, "betfair_event_id") || !strings.Contains(text, "Team A v Team B") || !strings.Contains(text, "next_step=") {
		t.Fatalf("text output=%s", text)
	}
}

func TestCatalogueLookupValidation(t *testing.T) {
	t.Setenv("LOOKUP_APP_KEY", "LOOKUP_APP_SECRET")
	t.Setenv("LOOKUP_SESSION_TOKEN", "LOOKUP_SESSION_SECRET")
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing app env", args: []string{"--session-token-env", "LOOKUP_SESSION_TOKEN", "--market-id", "1.111"}, want: "app-key-env"},
		{name: "missing session env", args: []string{"--app-key-env", "LOOKUP_APP_KEY", "--market-id", "1.111"}, want: "session-token-env"},
		{name: "missing market", args: []string{"--app-key-env", "LOOKUP_APP_KEY", "--session-token-env", "LOOKUP_SESSION_TOKEN"}, want: "market-id"},
		{name: "wildcard", args: []string{"--app-key-env", "LOOKUP_APP_KEY", "--session-token-env", "LOOKUP_SESSION_TOKEN", "--market-id", "*"}, want: "wildcard"},
		{name: "too many", args: []string{"--app-key-env", "LOOKUP_APP_KEY", "--session-token-env", "LOOKUP_SESSION_TOKEN", "--market-id", "1.1,1.2,1.3,1.4,1.5,1.6"}, want: "too many"},
		{name: "bad output", args: []string{"--app-key-env", "LOOKUP_APP_KEY", "--session-token-env", "LOOKUP_SESSION_TOKEN", "--market-id", "1.111", "--output", "xml"}, want: "output"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := run(context.Background(), tc.args, &out, http.DefaultClient)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want containing %q output=%s", err, tc.want, out.String())
			}
		})
	}
}

func TestCatalogueLookupCommandRedactsErrors(t *testing.T) {
	appKey := "LOOKUP_APP_SECRET"
	sessionToken := "LOOKUP_SESSION_SECRET"
	t.Setenv("LOOKUP_APP_KEY", appKey)
	t.Setenv("LOOKUP_SESSION_TOKEN", sessionToken)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad "+appKey+" "+sessionToken, http.StatusForbidden)
	}))
	defer server.Close()

	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--app-key-env", "LOOKUP_APP_KEY",
		"--session-token-env", "LOOKUP_SESSION_TOKEN",
		"--endpoint", server.URL,
		"--market-id", "1.111",
	}, &out, server.Client())
	if err == nil {
		t.Fatal("lookup error=nil want error")
	}
	assertLookupNoSecrets(t, err.Error(), appKey, sessionToken)
}

func assertLookupNoSecrets(t *testing.T, text string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		if strings.Contains(text, secret) {
			t.Fatalf("text leaked secret %q: %s", secret, text)
		}
	}
}
