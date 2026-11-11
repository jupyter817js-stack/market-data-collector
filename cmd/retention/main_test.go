package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestParseConfigDefaultsToDryRun(t *testing.T) {
	var out bytes.Buffer
	cfg, err := parseConfig(nil, &out)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.Execute {
		t.Fatal("default retention config should be dry-run")
	}
	if cfg.MaxDeleteRows != defaultMaxDeleteRows {
		t.Fatalf("max delete rows=%d want %d", cfg.MaxDeleteRows, defaultMaxDeleteRows)
	}
	if cfg.RawSnapshotsDays != defaultRawSnapshotsDays ||
		cfg.QuoteHistoryDays != defaultQuoteHistoryDays ||
		cfg.SourceRunsDays != defaultSourceRunsDays ||
		cfg.PolymarketPriceSnapshotsDays != defaultPolymarketPriceSnapshotsDays {
		t.Fatalf("unexpected retention defaults: %+v", cfg)
	}
}

func TestParseConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "max rows", args: []string{"--max-delete-rows", "0"}, want: "max-delete-rows must be positive"},
		{name: "negative raw snapshots", args: []string{"--raw-snapshots-days", "-1"}, want: "raw-snapshots-days must be >= 0"},
		{name: "negative quote history", args: []string{"--quote-history-days", "-1"}, want: "quote-history-days must be >= 0"},
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

func TestRetentionOperationsDryRunPlanning(t *testing.T) {
	cfg := retentionConfig{
		MaxDeleteRows:                100,
		RawSnapshotsDays:             90,
		QuoteHistoryDays:             180,
		SourceRunsDays:               180,
		PolymarketPriceSnapshotsDays: 30,
		Now:                          time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC),
	}
	ops := retentionOperations(cfg)
	if len(ops) != 4 {
		t.Fatalf("operations len=%d want 4", len(ops))
	}
	for _, op := range ops {
		if op.TableName == "" || op.CountSQL == "" || op.DeleteSQL == "" || op.Safety == "" {
			t.Fatalf("operation is incomplete: %+v", op)
		}
	}
}
