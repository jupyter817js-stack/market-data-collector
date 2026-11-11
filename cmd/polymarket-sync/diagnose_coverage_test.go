package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
)

// fakeGammaServer returns an httptest server that serves /events and /markets
// with the supplied payloads keyed by query string offset. Tests use this in
// place of the real Polymarket Gamma endpoint to keep automated runs offline.
func newFakeGammaServer(t *testing.T, eventsByOffset, marketsByOffset map[int]string, requestSink *atomic.Int64, lastQuery *atomic.Value) *httptest.Server {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestSink != nil {
			requestSink.Add(1)
		}
		if lastQuery != nil {
			lastQuery.Store(r.URL.RawQuery)
		}
		offset := 0
		if v := r.URL.Query().Get("offset"); v != "" {
			fmt.Sscanf(v, "%d", &offset)
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/events"):
			body, ok := eventsByOffset[offset]
			if !ok {
				body = `[]`
			}
			_, _ = w.Write([]byte(body))
		case strings.HasSuffix(r.URL.Path, "/markets"):
			body, ok := marketsByOffset[offset]
			if !ok {
				body = `[]`
			}
			_, _ = w.Write([]byte(body))
		default:
			http.NotFound(w, r)
		}
	})
	return httptest.NewServer(handler)
}

func TestShouldRunDiagnoseCoverageStrips(t *testing.T) {
	rest, ok := shouldRunDiagnoseCoverage([]string{"--limit", "100", "--diagnose-coverage", "--endpoint", "events"})
	if !ok {
		t.Fatal("expected shouldRunDiagnoseCoverage=true")
	}
	if got := strings.Join(rest, " "); got != "--limit 100 --endpoint events" {
		t.Fatalf("remaining args=%q want '--limit 100 --endpoint events'", got)
	}
	if _, ok := shouldRunDiagnoseCoverage([]string{"--noop"}); ok {
		t.Fatal("did not expect diagnose mode")
	}
	if rest, ok := shouldRunDiagnoseCoverage([]string{"--diagnose-coverage=false"}); ok {
		t.Fatalf("=false should disable; rest=%v", rest)
	}
}

func TestParseDiagnoseConfigDefaultsAndValidation(t *testing.T) {
	cfg, err := parseDiagnoseConfig(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("default parse: %v", err)
	}
	if cfg.Endpoint != "events" || cfg.PageSize != diagnoseDefaultPageSize || cfg.MaxPages != diagnoseDefaultMaxPages {
		t.Fatalf("defaults=%+v", cfg)
	}
	if cfg.IncludeClosed || cfg.IncludeArchived || cfg.IncludeInactive {
		t.Fatalf("default lifecycle filters should exclude closed/archived/inactive: %+v", cfg)
	}
	if cfg.Output != "json" {
		t.Fatalf("default output=%q want json", cfg.Output)
	}

	if _, err := parseDiagnoseConfig([]string{"--endpoint", "wrong"}, &bytes.Buffer{}); err == nil {
		t.Fatal("invalid endpoint should error")
	}
	if _, err := parseDiagnoseConfig([]string{"--page-size", "0"}, &bytes.Buffer{}); err == nil {
		t.Fatal("page-size=0 should error")
	}
	if _, err := parseDiagnoseConfig([]string{"--max-pages", "9999"}, &bytes.Buffer{}); err == nil {
		t.Fatal("max-pages too large should error")
	}
	if _, err := parseDiagnoseConfig([]string{"--ascending", "maybe"}, &bytes.Buffer{}); err == nil {
		t.Fatal("ascending=maybe should error")
	}
	if _, err := parseDiagnoseConfig([]string{"--now", "not-a-time"}, &bytes.Buffer{}); err == nil {
		t.Fatal("invalid --now should error")
	}
	if _, err := parseDiagnoseConfig([]string{"--output", "xml"}, &bytes.Buffer{}); err == nil {
		t.Fatal("invalid --output should error")
	}
}

func TestRunDiagnoseCoverageActiveCandidateIsActivePlausible(t *testing.T) {
	commenceFuture := time.Date(2026, 5, 9, 19, 10, 0, 0, time.UTC)
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	body := `[
        {
            "id": "evt-current",
            "slug": "cavaliers-vs-pistons-may-9",
            "title": "NBA: Cleveland Cavaliers vs Detroit Pistons - May 9 2026",
            "active": true,
            "closed": false,
            "archived": false,
            "endTime": "` + commenceFuture.Add(3*time.Hour).Format(time.RFC3339) + `",
            "category": "sports",
            "tags": [{"id": "1", "slug": "nba", "label": "NBA"}],
            "markets": [
                {
                    "id": "mkt-1",
                    "conditionId": "cond-1",
                    "question": "Will the Cavaliers beat the Pistons?",
                    "active": true,
                    "closed": false,
                    "archived": false,
                    "outcomes": ["Cavaliers", "Pistons"],
                    "clobTokenIds": ["tok-yes", "tok-no"]
                }
            ]
        }
    ]`
	var requests atomic.Int64
	var lastQuery atomic.Value
	server := newFakeGammaServer(t, map[int]string{0: body}, nil, &requests, &lastQuery)
	defer server.Close()

	client := polymarket.NewClient(server.URL, time.Second)
	cfg := diagnoseConfig{
		Endpoint:     "events",
		PageSize:     50,
		MaxPages:     2,
		HomeTeam:     "Cleveland Cavaliers",
		AwayTeam:     "Detroit Pistons",
		Now:          now,
		Output:       "json",
		HTTPTimeout:  time.Second,
		GammaBaseURL: server.URL,
	}
	var out bytes.Buffer
	if err := runDiagnoseCoverage(context.Background(), cfg, client, &out); err != nil {
		t.Fatalf("runDiagnoseCoverage: %v", err)
	}
	if requests.Load() == 0 {
		t.Fatal("fake server received zero requests")
	}
	q, _ := lastQuery.Load().(string)
	for _, want := range []string{"closed=false", "active=true", "archived=false"} {
		if !strings.Contains(q, want) {
			t.Fatalf("default lifecycle query missing %q: %q", want, q)
		}
	}

	var report diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, out.String())
	}
	if report.SafetyCategoryCounts[candidateActivePlausible] != 1 {
		t.Fatalf("expected one active_plausible candidate, got counts=%+v", report.SafetyCategoryCounts)
	}
	if report.TeamPairMatch.BothTokensMatchCount != 1 {
		t.Fatalf("both tokens should match, got=%+v", report.TeamPairMatch)
	}
	if len(report.SampleMatches) == 0 || report.SampleMatches[0].SafetyCategory != candidateActivePlausible {
		t.Fatalf("sample[0]=%+v", report.SampleMatches)
	}
}

func TestRunDiagnoseCoverageClosedHistoricalIsMarkedUnsafe(t *testing.T) {
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	closedPast := time.Date(2021, 11, 12, 23, 0, 0, 0, time.UTC)
	body := `[
        {
            "id": "evt-2965",
            "slug": "cavaliers-pistons-2021",
            "title": "NBA: Will the Cavaliers beat the Pistons by more than 5.5 points in their November 12th matchup?",
            "active": false,
            "closed": true,
            "archived": false,
            "endTime": "` + closedPast.Format(time.RFC3339) + `",
            "markets": [{"id": "mkt-239376", "conditionId": "cond-old", "question": "Cavaliers vs Pistons over 5.5", "outcomes": ["Yes","No"], "clobTokenIds": ["tk1","tk2"]}]
        }
    ]`
	server := newFakeGammaServer(t, map[int]string{0: body}, nil, nil, nil)
	defer server.Close()
	client := polymarket.NewClient(server.URL, time.Second)
	cfg := diagnoseConfig{
		Endpoint:        "events",
		PageSize:        50,
		MaxPages:        1,
		IncludeClosed:   true,
		IncludeInactive: true,
		HomeTeam:        "Cleveland Cavaliers",
		AwayTeam:        "Detroit Pistons",
		Now:             now,
		Output:          "json",
		HTTPTimeout:     time.Second,
		GammaBaseURL:    server.URL,
	}
	var out bytes.Buffer
	if err := runDiagnoseCoverage(context.Background(), cfg, client, &out); err != nil {
		t.Fatalf("runDiagnoseCoverage: %v", err)
	}
	var report diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if report.SafetyCategoryCounts[candidateClosedHistorical] != 1 {
		t.Fatalf("expected closed_historical=1, got counts=%+v", report.SafetyCategoryCounts)
	}
	if report.SafetyCategoryCounts[candidateActivePlausible] != 0 {
		t.Fatalf("a closed 2021 market must NOT be classified active_plausible: %+v", report.SafetyCategoryCounts)
	}
	if len(report.SampleMatches) == 0 || report.SampleMatches[0].SafetyCategory != candidateClosedHistorical {
		t.Fatalf("first sample should be closed_historical: %+v", report.SampleMatches)
	}
	foundIncludeNote := false
	for _, n := range report.Notes {
		if strings.Contains(n, "--include-closed is on") {
			foundIncludeNote = true
		}
	}
	if !foundIncludeNote {
		t.Fatalf("expected --include-closed safety note; notes=%v", report.Notes)
	}
}

func TestRunDiagnoseCoverageNoMatchAddsNote(t *testing.T) {
	body := `[
        {"id": "evt-other", "title": "Politics: Will candidate X win?", "active": true, "closed": false, "archived": false,
         "markets": [{"id": "mkt-x", "conditionId": "c1", "outcomes": ["Yes","No"], "clobTokenIds": ["a","b"]}]}
    ]`
	server := newFakeGammaServer(t, map[int]string{0: body}, nil, nil, nil)
	defer server.Close()
	client := polymarket.NewClient(server.URL, time.Second)
	cfg := diagnoseConfig{
		Endpoint:     "events",
		PageSize:     50,
		MaxPages:     1,
		HomeTeam:     "Cleveland Cavaliers",
		AwayTeam:     "Detroit Pistons",
		Now:          time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
		Output:       "json",
		HTTPTimeout:  time.Second,
		GammaBaseURL: server.URL,
	}
	var out bytes.Buffer
	if err := runDiagnoseCoverage(context.Background(), cfg, client, &out); err != nil {
		t.Fatalf("runDiagnoseCoverage: %v", err)
	}
	var report diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if report.TeamPairMatch.BothTokensMatchCount != 0 {
		t.Fatalf("no team-pair match expected; got=%+v", report.TeamPairMatch)
	}
	noteFound := false
	for _, n := range report.Notes {
		if strings.Contains(n, "no Polymarket items matched") {
			noteFound = true
		}
	}
	if !noteFound {
		t.Fatalf("expected no-match note; notes=%v", report.Notes)
	}
}

func TestRunDiagnoseCoveragePaginatesAndStopsAtMaxPages(t *testing.T) {
	body := func(offset int) string {
		return fmt.Sprintf(`[{"id":"evt-%d","title":"NBA filler","active":true,"closed":false,"archived":false,"markets":[{"id":"m-%d","conditionId":"c-%d","outcomes":["Yes","No"],"clobTokenIds":["t","u"]}]}]`, offset, offset, offset)
	}
	full := func(offset int) string {
		// Return page-size items so pagination keeps going.
		items := make([]string, 0, 2)
		for i := 0; i < 2; i++ {
			id := offset + i
			items = append(items, fmt.Sprintf(`{"id":"evt-%d","title":"filler","active":true,"closed":false,"archived":false}`, id))
		}
		return "[" + strings.Join(items, ",") + "]"
	}
	_ = body
	pages := map[int]string{
		0:  full(0),
		2:  full(2),
		4:  full(4),
		6:  full(6),
		8:  full(8),
		10: full(10),
	}
	var requests atomic.Int64
	server := newFakeGammaServer(t, pages, nil, &requests, nil)
	defer server.Close()
	client := polymarket.NewClient(server.URL, time.Second)
	cfg := diagnoseConfig{
		Endpoint:     "events",
		PageSize:     2,
		MaxPages:     3,
		Now:          time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
		Output:       "json",
		HTTPTimeout:  time.Second,
		GammaBaseURL: server.URL,
	}
	var out bytes.Buffer
	if err := runDiagnoseCoverage(context.Background(), cfg, client, &out); err != nil {
		t.Fatalf("runDiagnoseCoverage: %v", err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("expected 3 page requests, got %d", got)
	}
	var report diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if !report.Pagination.HitMaxPages {
		t.Fatalf("expected HitMaxPages=true; %+v", report.Pagination)
	}
	if report.Pagination.PagesFetched != 3 || report.Pagination.ItemsFetched != 6 {
		t.Fatalf("pagination=%+v want pages=3 items=6", report.Pagination)
	}
	hitNote := false
	for _, n := range report.Notes {
		if strings.Contains(n, "max-pages cap reached") {
			hitNote = true
		}
	}
	if !hitNote {
		t.Fatalf("expected max-pages note; notes=%v", report.Notes)
	}
}

func TestRunDiagnoseCoverageOutputContainsNoTradingTerminology(t *testing.T) {
	body := `[
        {"id": "evt-1", "title": "NBA Cavaliers vs Pistons", "active": true, "closed": false, "archived": false,
         "markets": [{"id":"m1","conditionId":"c1","outcomes":["Yes","No"],"clobTokenIds":["a","b"]}]}
    ]`
	server := newFakeGammaServer(t, map[int]string{0: body}, nil, nil, nil)
	defer server.Close()
	client := polymarket.NewClient(server.URL, time.Second)
	cfg := diagnoseConfig{
		Endpoint:     "events",
		PageSize:     50,
		MaxPages:     1,
		HomeTeam:     "Cleveland Cavaliers",
		AwayTeam:     "Detroit Pistons",
		Now:          time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
		Output:       "json",
		HTTPTimeout:  time.Second,
		GammaBaseURL: server.URL,
	}
	var out bytes.Buffer
	if err := runDiagnoseCoverage(context.Background(), cfg, client, &out); err != nil {
		t.Fatalf("runDiagnoseCoverage: %v", err)
	}
	body2 := strings.ToLower(out.String())
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
		if strings.Contains(body2, forbidden) {
			t.Fatalf("output contains forbidden phrase %q:\n%s", forbidden, body2)
		}
	}
}

// TestRunDiagnoseCoverageAcceptsGammaSpaceTimestamps is the regression test
// for the production failure:
//
//	parse time "2026-04-01 07:32:39+00": parsing time as RFC3339 cannot parse
//	" 07:32:39+00" as "T"
//
// A real Gamma response with space-separated timestamps must NOT panic the
// events diagnostic. The classifier should bucket the candidate normally.
func TestRunDiagnoseCoverageAcceptsGammaSpaceTimestamps(t *testing.T) {
	body := `[
        {
            "id": "evt-12e",
            "slug": "cavaliers-vs-pistons-april-1-2026",
            "title": "NBA: Cleveland Cavaliers vs Detroit Pistons",
            "active": true,
            "closed": false,
            "archived": false,
            "startTime": "2026-04-01 07:32:39+00",
            "endTime":   "2026-04-01 10:00:00+00:00",
            "markets": [
                {
                    "id": "mkt-1",
                    "conditionId": "cond-1",
                    "question": "Will the Cavaliers beat the Pistons?",
                    "endTime": "2026-04-01 10:30:00+00",
                    "active": true,
                    "closed": false,
                    "archived": false,
                    "outcomes": ["Cavaliers", "Pistons"],
                    "clobTokenIds": ["tok-yes", "tok-no"]
                }
            ]
        }
    ]`
	server := newFakeGammaServer(t, map[int]string{0: body}, nil, nil, nil)
	defer server.Close()
	client := polymarket.NewClient(server.URL, time.Second)

	now := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	cfg := diagnoseConfig{
		Endpoint:     "events",
		PageSize:     50,
		MaxPages:     1,
		HomeTeam:     "Cleveland Cavaliers",
		AwayTeam:     "Detroit Pistons",
		Now:          now,
		Output:       "json",
		HTTPTimeout:  time.Second,
		GammaBaseURL: server.URL,
	}
	var out bytes.Buffer
	if err := runDiagnoseCoverage(context.Background(), cfg, client, &out); err != nil {
		t.Fatalf("runDiagnoseCoverage with Gamma space timestamps: %v", err)
	}
	var report diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, out.String())
	}
	if report.SafetyCategoryCounts[candidateActivePlausible] != 1 {
		t.Fatalf("expected one active_plausible candidate after Gamma timestamp fix; got %+v", report.SafetyCategoryCounts)
	}
	if got := report.TimeBucketCounts["ends_within_7d_future"]; got != 1 {
		t.Fatalf("expected end-time bucket ends_within_7d_future=1, got buckets=%+v", report.TimeBucketCounts)
	}
}

func TestExampleEndpointURLEchoesAppliedFilters(t *testing.T) {
	asc := false
	cfg := diagnoseConfig{
		Endpoint:        "events",
		PageSize:        25,
		IncludeClosed:   false,
		IncludeArchived: false,
		IncludeInactive: false,
		Order:           "endDate",
		Ascending:       &asc,
		TagSlug:         "nba",
		Category:        "sports",
		GammaBaseURL:    "https://gamma.example",
	}
	got := exampleEndpointURL(cfg)
	for _, want := range []string{
		"limit=25",
		"closed=false",
		"active=true",
		"archived=false",
		"order=endDate",
		"ascending=false",
		"tag_slug=nba",
		"category=sports",
		"https://gamma.example/events",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("example URL=%q missing %q", got, want)
		}
	}
}
