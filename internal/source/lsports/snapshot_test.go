package lsports

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSnapshotServerTimeUnitDetection(t *testing.T) {
	cases := []struct {
		name     string
		ts       int64
		wantYear int
	}{
		// 2026-05-28 epoch s ~ 1.78e9
		{"seconds", 1780000000, 2026},
		// epoch ms ~ 1.78e12
		{"milliseconds", 1780000000000, 2026},
		// epoch us ~ 1.78e15
		{"microseconds", 1780000000000000, 2026},
		// zero -> zero time
		{"zero", 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := SnapshotHeader{ServerTimestamp: tc.ts}
			got := h.SnapshotServerTime()
			if tc.ts == 0 {
				if !got.IsZero() {
					t.Fatalf("expected zero time, got %v", got)
				}
				return
			}
			if got.Year() != tc.wantYear {
				t.Fatalf("got year %d, want %d (parsed %v from %d)", got.Year(), tc.wantYear, got, tc.ts)
			}
		})
	}
}

func TestSnapshotFilterOmitsEmpty(t *testing.T) {
	from := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)
	f := SnapshotFilter{
		Fixtures: []int{777},
		FromDate: &from,
	}
	body := f.toBody()
	if _, ok := body["sports"]; ok {
		t.Fatalf("empty sports filter should be omitted, body=%v", body)
	}
	if _, ok := body["leagues"]; ok {
		t.Fatalf("empty leagues filter should be omitted, body=%v", body)
	}
	if got := body["fromDate"]; got == nil {
		t.Fatalf("fromDate should be present, body=%v", body)
	}
	arr, ok := body["fixtures"].([]int)
	if !ok || len(arr) != 1 || arr[0] != 777 {
		t.Fatalf("unexpected fixtures: %v", body["fixtures"])
	}
}

func TestGetScoresParsesSnapshotEnvelope(t *testing.T) {
	var capturedPath string
	var capturedBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &capturedBody)
		_, _ = w.Write([]byte(`{
			"Header":{"Type":36,"MsgGuid":"abc-123","ServerTimestamp":1780000000000},
			"Body":[{"FixtureId":777, "Livescore":{"CurrentPeriod":2}}]
		}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, Credentials{PackageID: 4044, Username: "u", Password: "p"}, 5*time.Second)
	resp, err := c.GetScores(context.Background(), SnapshotFilter{Fixtures: []int{777}})
	if err != nil {
		t.Fatalf("GetScores: %v", err)
	}
	if capturedPath != "/InPlay/GetScores" {
		t.Fatalf("expected /InPlay/GetScores, got %s", capturedPath)
	}
	if resp.Header.Type != 36 || resp.Header.MsgGuid != "abc-123" {
		t.Fatalf("header parse failed: %+v", resp.Header)
	}
	if resp.Header.SnapshotServerTime().Year() != 2026 {
		t.Fatalf("ServerTimestamp parse failed: %v", resp.Header.SnapshotServerTime())
	}
	// Verify request body carries fixtures filter and auth.
	arr, ok := capturedBody["fixtures"].([]any)
	if !ok || len(arr) != 1 || int(arr[0].(float64)) != 777 {
		t.Fatalf("fixtures filter not in request body: %v", capturedBody)
	}
	if capturedBody["packageId"] == nil {
		t.Fatalf("auth packageId missing from request body: %v", capturedBody)
	}
}
