package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"strings"
	"testing"
)

func TestParseCommandWithoutArgsPrintsUsage(t *testing.T) {
	var out bytes.Buffer
	_, err := parseCommand(nil, &out)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parse without args error=%v want flag.ErrHelp", err)
	}
	if !strings.Contains(out.String(), "Usage:") {
		t.Fatalf("usage output missing Usage header:\n%s", out.String())
	}
}

func TestParseListCommand(t *testing.T) {
	var out bytes.Buffer
	cfg, err := parseCommand([]string{
		"list-runners",
		"--limit", "25",
		"--event-id", "event-1",
		"--market-id", "1.234",
		"--status", "missing",
	}, &out)
	if err != nil {
		t.Fatalf("parse list command: %v", err)
	}
	if cfg.Kind != commandListRunners {
		t.Fatalf("kind=%q want list-runners", cfg.Kind)
	}
	if cfg.List.Limit != 25 || cfg.List.EventID != "event-1" || cfg.List.MarketID != "1.234" || cfg.List.Status != "missing" {
		t.Fatalf("list options=%+v", cfg.List)
	}
}

func TestParseApproveCommands(t *testing.T) {
	var out bytes.Buffer
	event, err := parseEventDecision("approve-event", []string{
		"--betfair-event-id", "event-1",
		"--betfair-event-name", "Home v Away",
		"--canonical-event-id", "00000000-0000-0000-0000-000000000000",
		"--reviewer", "analyst",
		"--reason", "verified",
		"--confidence", "91.5",
	}, &out, true)
	if err != nil {
		t.Fatalf("parse approve event: %v", err)
	}
	if event.BetfairEventID != "event-1" || event.CanonicalEventID == "" || event.Reviewer != "analyst" || event.ConfidenceScore == nil || *event.ConfidenceScore != 91.5 {
		t.Fatalf("event decision=%+v", event)
	}

	market, err := parseMarketDecision("approve-market", []string{
		"--betfair-market-id", "1.234",
		"--betfair-event-id", "event-1",
		"--canonical-event-id", "00000000-0000-0000-0000-000000000000",
		"--canonical-market-family", "h2h",
		"--line", "2.5",
		"--reviewer", "analyst",
		"--reason", "verified",
	}, &out, true)
	if err != nil {
		t.Fatalf("parse approve market: %v", err)
	}
	if market.BetfairMarketID != "1.234" || market.CanonicalMarketFamily != "h2h" || market.Line == nil || *market.Line != 2.5 {
		t.Fatalf("market decision=%+v", market)
	}

	runner, err := parseRunnerDecision("approve-runner", []string{
		"--betfair-market-id", "1.234",
		"--selection-id", "101",
		"--handicap", "0",
		"--canonical-outcome-name", "Home FC",
		"--canonical-outcome-side", "home",
		"--reviewer", "analyst",
		"--reason", "verified",
	}, &out, true)
	if err != nil {
		t.Fatalf("parse approve runner: %v", err)
	}
	if runner.SelectionID != 101 || runner.CanonicalOutcomeSide != "home" {
		t.Fatalf("runner decision=%+v", runner)
	}
}

func TestParseRejectCommandsRequireExactIDsReviewerAndReason(t *testing.T) {
	var out bytes.Buffer
	if _, err := parseEventDecision("reject-event", []string{"--reviewer", "analyst", "--reason", "bad match"}, &out, false); err == nil || !strings.Contains(err.Error(), "betfair event id") {
		t.Fatalf("reject event missing id error=%v", err)
	}
	if _, err := parseMarketDecision("reject-market", []string{"--betfair-market-id", "1.234", "--reason", "bad match"}, &out, false); err == nil || !strings.Contains(err.Error(), "reviewer") {
		t.Fatalf("reject market missing reviewer error=%v", err)
	}
	if _, err := parseRunnerDecision("reject-runner", []string{"--betfair-market-id", "1.234", "--reviewer", "analyst", "--reason", "bad match"}, &out, false); err == nil || !strings.Contains(err.Error(), "selection id") {
		t.Fatalf("reject runner missing selection error=%v", err)
	}
}

func TestParseRejectRunner(t *testing.T) {
	var out bytes.Buffer
	runner, err := parseRunnerDecision("reject-runner", []string{
		"--betfair-market-id", "1.234",
		"--selection-id", "101",
		"--handicap", "0",
		"--runner-name", "Home FC",
		"--reviewer", "analyst",
		"--reason", "wrong runner",
	}, &out, false)
	if err != nil {
		t.Fatalf("parse reject runner: %v", err)
	}
	if runner.BetfairMarketID != "1.234" || runner.SelectionID != 101 || runner.CanonicalOutcomeName != "" || runner.CanonicalOutcomeSide != "" {
		t.Fatalf("reject runner decision=%+v", runner)
	}
}

func TestParseValidationErrors(t *testing.T) {
	var out bytes.Buffer
	if _, err := parseMarketDecision("approve-market", []string{
		"--betfair-market-id", "1.234",
		"--betfair-event-id", "event-1",
		"--canonical-event-id", "00000000-0000-0000-0000-000000000000",
		"--canonical-market-family", "moneyline-ish",
		"--reviewer", "analyst",
		"--reason", "verified",
	}, &out, true); err == nil || !strings.Contains(err.Error(), "canonical market family") {
		t.Fatalf("invalid market family error=%v", err)
	}
	if _, err := parseRunnerDecision("approve-runner", []string{
		"--betfair-market-id", "1.234",
		"--selection-id", "101",
		"--canonical-outcome-name", "Home FC",
		"--canonical-outcome-side", "winner",
		"--reviewer", "analyst",
		"--reason", "verified",
	}, &out, true); err == nil || !strings.Contains(err.Error(), "canonical outcome side") {
		t.Fatalf("invalid outcome side error=%v", err)
	}
	if _, err := parseEventDecision("approve-event", []string{
		"--betfair-event-id", "event-1",
		"--canonical-event-id", "00000000-0000-0000-0000-000000000000",
		"--reviewer", "analyst",
		"--reason", "verified",
		"--confidence", "101",
	}, &out, true); err == nil || !strings.Contains(err.Error(), "confidence") {
		t.Fatalf("invalid confidence error=%v", err)
	}
}

func TestRunValidationHappensBeforeDatabaseOpen(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://invalid.invalid:5432/nope?sslmode=disable")
	var out bytes.Buffer
	err := run(context.Background(), []string{"approve-event", "--reviewer", "analyst", "--reason", "missing id"}, &out)
	if err == nil {
		t.Fatal("run with missing required args returned nil error")
	}
	if strings.Contains(err.Error(), "open postgres") || strings.Contains(err.Error(), "invalid.invalid") {
		t.Fatalf("validation should happen before database open, got error=%v", err)
	}
}

func TestDatabaseURLDefault(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	if got := databaseURL(); got != defaultDatabaseURL {
		t.Fatalf("databaseURL=%q want default", got)
	}
	if err := os.Setenv("DATABASE_URL", "postgres://example"); err != nil {
		t.Fatalf("set env: %v", err)
	}
	if got := databaseURL(); got != "postgres://example" {
		t.Fatalf("databaseURL=%q want env value", got)
	}
}
