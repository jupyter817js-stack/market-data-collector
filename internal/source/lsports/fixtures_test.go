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

func TestFixtureHomeAwayByPosition(t *testing.T) {
	f := Fixture{
		Participants: []Competitor{
			{ID: 1, Name: "Athletico", Position: 2},
			{ID: 2, Name: "Bayern", Position: 1},
		},
	}
	if got := f.HomeParticipant(); got != "Bayern" {
		t.Fatalf("home: got %q want Bayern", got)
	}
	if got := f.AwayParticipant(); got != "Athletico" {
		t.Fatalf("away: got %q want Athletico", got)
	}
}

func TestFixtureCommenceTimeParsesMultipleLayouts(t *testing.T) {
	cases := []struct {
		raw     string
		wantOK  bool
		wantUTC string
	}{
		{"2026-05-28T19:00:00Z", true, "2026-05-28T19:00:00Z"},
		{"2026-05-28T19:00:00", true, "2026-05-28T19:00:00Z"},
		{"2026-05-28 19:00:00", true, "2026-05-28T19:00:00Z"},
		{"", false, ""},
		{"not-a-date", false, ""},
	}
	for _, tc := range cases {
		f := Fixture{StartDate: tc.raw}
		got, ok := f.CommenceTime()
		if ok != tc.wantOK {
			t.Fatalf("%q: ok mismatch (got %v want %v)", tc.raw, ok, tc.wantOK)
		}
		if !ok {
			continue
		}
		if got.Format(time.RFC3339) != tc.wantUTC {
			t.Fatalf("%q: parsed UTC mismatch (got %s want %s)", tc.raw, got.Format(time.RFC3339), tc.wantUTC)
		}
	}
}

func TestGetFixturesParsesPascalCaseEnvelope(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Fixtures/Get" {
			t.Fatalf("expected /Fixtures/Get, got %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &captured)
		_, _ = w.Write([]byte(`{
			"Header":{"HttpStatusCode":200,"Errors":null},
			"Body":{"Fixtures":[
				{"FixtureId": 777, "StartDate": "2026-05-28T19:00:00Z",
				 "Sport": {"Id":54094, "Name":"Tennis"},
				 "League": {"Id":1, "Name":"ATP"},
				 "Participants":[
				   {"Id":11, "Name":"Sinner", "Position":1},
				   {"Id":12, "Name":"Alcaraz", "Position":2}
				 ]}
			]}
		}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, Credentials{PackageID: 4044, Username: "u", Password: "p"}, 5*time.Second)
	from := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC)
	fixtures, _, err := c.GetFixtures(context.Background(), FixturesFilter{
		SportIDs: []int{54094},
		FromDate: &from,
		ToDate:   &to,
	})
	if err != nil {
		t.Fatalf("GetFixtures: %v", err)
	}
	if len(fixtures) != 1 {
		t.Fatalf("expected 1 fixture, got %d", len(fixtures))
	}
	f := fixtures[0]
	if f.ID != 777 || f.Sport.Name != "Tennis" {
		t.Fatalf("unexpected fixture parse: %+v", f)
	}
	if f.HomeParticipant() != "Sinner" || f.AwayParticipant() != "Alcaraz" {
		t.Fatalf("home/away parse failed: home=%q away=%q", f.HomeParticipant(), f.AwayParticipant())
	}

	// Verify request body forwarded the filter fields as lowerCamel.
	if got := captured["sportIds"]; got == nil {
		t.Fatalf("expected sportIds in body, got nil. body=%v", captured)
	}
	if captured["fromDate"] != "2026-05-28T00:00:00Z" {
		t.Fatalf("fromDate: got %v want 2026-05-28T00:00:00Z", captured["fromDate"])
	}
}
