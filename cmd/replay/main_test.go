package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

func TestParseConfigDefaultsToDryRunReplayShape(t *testing.T) {
	var out bytes.Buffer
	cfg, err := parseConfig([]string{"--id", "42", "--sport-key", "basketball_nba"}, &out)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.Filter.ID != 42 {
		t.Fatalf("id=%d want 42", cfg.Filter.ID)
	}
	if cfg.Filter.Source != model.SourceOddsAPI {
		t.Fatalf("source=%q want %q", cfg.Filter.Source, model.SourceOddsAPI)
	}
	if cfg.Filter.SportKey != "basketball_nba" {
		t.Fatalf("sport key=%q", cfg.Filter.SportKey)
	}
	if cfg.Freshness.FreshAfter != 120*time.Second || cfg.Freshness.AgingAfter != 300*time.Second {
		t.Fatalf("freshness=%v/%v", cfg.Freshness.FreshAfter, cfg.Freshness.AgingAfter)
	}
}

func TestParseConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "negative id", args: []string{"--id", "-1"}, want: "id must be non-negative"},
		{name: "bad from", args: []string{"--from", "not-a-time"}, want: "from must be RFC3339"},
		{name: "to before from", args: []string{"--from", "2026-05-07T00:00:00Z", "--to", "2026-05-06T00:00:00Z"}, want: "to must be >= from"},
		{name: "aging before fresh", args: []string{"--fresh-after-seconds", "300", "--aging-after-seconds", "120"}, want: "aging-after-seconds must be >= fresh-after-seconds"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			_, err := parseConfig(tc.args, &out)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("parseConfig error=%v want containing %q", err, tc.want)
			}
		})
	}
}
