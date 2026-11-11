package polymarket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestParseGammaTimestampAcceptsRFC3339AndGammaSpaceFormat is the regression
// test for the Phase 12E events-diagnostic failure:
//
//	parse time "2026-04-01 07:32:39+00": parsing time as RFC3339 cannot parse
//	" 07:32:39+00" as "T"
//
// Polymarket Gamma returns at least two distinct timestamp shapes in the wild;
// the parser must accept both and normalize to UTC. Anything that does not
// match a supported layout must still produce a clear error.
func TestParseGammaTimestampAcceptsRFC3339AndGammaSpaceFormat(t *testing.T) {
	expected := time.Date(2026, 4, 1, 7, 32, 39, 0, time.UTC)
	tests := []struct {
		name string
		raw  string
		want time.Time
	}{
		{name: "rfc3339_z", raw: "2026-04-01T07:32:39Z", want: expected},
		{name: "rfc3339_zero_offset_with_colon", raw: "2026-04-01T07:32:39+00:00", want: expected},
		{name: "rfc3339_zero_offset_no_colon", raw: "2026-04-01T07:32:39+0000", want: expected},
		{name: "rfc3339_negative_offset", raw: "2026-04-01T03:32:39-04:00", want: expected},
		{name: "rfc3339_nanos", raw: "2026-04-01T07:32:39.123456789Z", want: time.Date(2026, 4, 1, 7, 32, 39, 123456789, time.UTC)},
		{name: "gamma_space_short_offset", raw: "2026-04-01 07:32:39+00", want: expected},
		{name: "gamma_space_full_colon_offset", raw: "2026-04-01 07:32:39+00:00", want: expected},
		{name: "gamma_space_no_colon_offset", raw: "2026-04-01 07:32:39+0000", want: expected},
		{name: "gamma_space_negative_short_offset", raw: "2026-04-01 03:32:39-04", want: expected},
		{name: "gamma_space_z_suffix", raw: "2026-04-01 07:32:39Z", want: expected},
		{name: "gamma_space_no_offset", raw: "2026-04-01 07:32:39", want: expected},
		{name: "gamma_bare_date", raw: "2026-04-01", want: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseGammaTimestamp(tc.raw)
			if err != nil {
				t.Fatalf("ParseGammaTimestamp(%q): %v", tc.raw, err)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("ParseGammaTimestamp(%q)=%v want %v", tc.raw, got, tc.want)
			}
			if got.Location() != time.UTC {
				t.Fatalf("ParseGammaTimestamp(%q) location=%v want UTC", tc.raw, got.Location())
			}
		})
	}
}

func TestParseGammaTimestampRejectsNonsense(t *testing.T) {
	tests := []string{
		"not-a-time",
		"2026-04-01T07:32:39+0",     // lone +0 is not a valid offset
		"2026/04/01 07:32:39+00:00", // wrong date separators
		"April 1, 2026 07:32:39",
		"2026-13-01T07:32:39Z", // invalid month
	}
	for _, raw := range tests {
		t.Run(raw, func(t *testing.T) {
			ts, err := ParseGammaTimestamp(raw)
			if err == nil {
				t.Fatalf("ParseGammaTimestamp(%q) returned no error; ts=%v", raw, ts)
			}
			if !strings.Contains(err.Error(), raw) {
				t.Fatalf("error %q must include the offending raw string %q", err, raw)
			}
			if !strings.Contains(err.Error(), "no supported layout matched") {
				t.Fatalf("error %q must explain that no supported layout matched", err)
			}
		})
	}
}

func TestParseGammaTimestampRejectsEmpty(t *testing.T) {
	if _, err := ParseGammaTimestamp("   "); err == nil {
		t.Fatal("expected error for empty/whitespace input")
	}
}

func TestParseGammaTimestampNoTradingTerminologyInError(t *testing.T) {
	// The error message is operator-facing diagnostic text only and must not
	// adopt trading/execution language.
	_, err := ParseGammaTimestamp("not-a-time")
	if err == nil {
		t.Fatal("expected error")
	}
	lowered := strings.ToLower(err.Error())
	for _, forbidden := range []string{
		"emits a trading signal",
		"emits a buy signal",
		"emits a sell signal",
		"places an order",
		"submits an order",
		"automatic execution",
		"recommended trade",
	} {
		if strings.Contains(lowered, forbidden) {
			t.Fatalf("error contains forbidden phrase %q: %s", forbidden, err)
		}
	}
}

// TestEventUnmarshalAcceptsGammaSpaceTimestamp wires the new parser through
// the Event JSON unmarshal path so a production-shaped Gamma payload no
// longer panics the events diagnostic.
func TestEventUnmarshalAcceptsGammaSpaceTimestamp(t *testing.T) {
	body := `[
        {
            "id": "evt-12e",
            "title": "NBA: Cavaliers vs Pistons",
            "active": true,
            "closed": false,
            "archived": false,
            "startTime": "2026-04-01 07:32:39+00",
            "endTime": "2026-04-01 10:00:00+00:00",
            "markets": [
                {"id": "mkt-1", "conditionId": "cond-1", "endTime": "2026-04-01 10:30:00+00", "outcomes": ["Yes","No"], "clobTokenIds": ["a","b"]}
            ]
        }
    ]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	client := newTestClient(server.URL, 0)
	events, err := client.ListEvents(context.Background(), ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count=%d want 1", len(events))
	}
	e := events[0]
	if e.StartTime == nil || !e.StartTime.Equal(time.Date(2026, 4, 1, 7, 32, 39, 0, time.UTC)) {
		t.Fatalf("StartTime=%v want 2026-04-01T07:32:39Z", e.StartTime)
	}
	if e.EndTime == nil || !e.EndTime.Equal(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("EndTime=%v want 2026-04-01T10:00:00Z", e.EndTime)
	}
	if len(e.Markets) != 1 || e.Markets[0].EndTime == nil || !e.Markets[0].EndTime.Equal(time.Date(2026, 4, 1, 10, 30, 0, 0, time.UTC)) {
		t.Fatalf("market.EndTime=%v want 2026-04-01T10:30:00Z", e.Markets[0].EndTime)
	}
}
