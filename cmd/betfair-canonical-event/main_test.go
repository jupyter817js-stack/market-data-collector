package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

func TestBetfairCanonicalEventDryRunDoesNotOpenDatabase(t *testing.T) {
	var out bytes.Buffer
	called := false
	err := run(context.Background(), baseArgs("--output", "json"), &out, func(context.Context, string) (canonicalStore, error) {
		called = true
		return nil, fmt.Errorf("unexpected database open")
	})
	if err != nil {
		t.Fatalf("dry-run command: %v output=%s", err, out.String())
	}
	if called {
		t.Fatal("dry-run opened database")
	}
	var report report
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode report %s: %v", out.String(), err)
	}
	if !report.DryRun || report.Created || report.CanonicalEventID != "" {
		t.Fatalf("dry-run report=%+v", report)
	}
	if report.CanonicalEvent.SportKey != "soccer" ||
		report.CanonicalEvent.League != "Australian A-League Women" ||
		report.CanonicalEvent.HomeTeam != "Melbourne City (W)" ||
		report.CanonicalEvent.AwayTeam != "Melbourne Victory (W)" ||
		report.CanonicalEvent.CommenceTime != "2026-05-09T04:00:00Z" {
		t.Fatalf("canonical event report=%+v", report.CanonicalEvent)
	}
	if report.Review.Source != sourceBetfairCatalogue || report.Review.SourceEventID != "35561874" || report.Review.SourceMarketID != "1.257744548" {
		t.Fatalf("review context=%+v", report.Review)
	}
	if report.AliasAutoWrite || report.CollectorIntegration {
		t.Fatalf("boundary flags=%+v", report)
	}
}

func TestBetfairCanonicalEventConfirmCreate(t *testing.T) {
	rawURL := "postgres://pol:secret@localhost:5432/pol_odds?sslmode=disable"
	t.Setenv("TEST_DATABASE_URL", rawURL)
	store := &fakeCanonicalStore{}
	var openedURL string
	var out bytes.Buffer
	err := run(context.Background(), append(baseArgs("--confirm-create", "--database-url-env", "TEST_DATABASE_URL", "--output", "json")), &out, func(ctx context.Context, databaseURL string) (canonicalStore, error) {
		openedURL = databaseURL
		return store, nil
	})
	if err != nil {
		t.Fatalf("confirm command: %v output=%s", err, out.String())
	}
	if openedURL != rawURL {
		t.Fatalf("openedURL=%q want explicit env value", openedURL)
	}
	if store.createCount != 1 || store.closeCount != 1 {
		t.Fatalf("store create=%d close=%d", store.createCount, store.closeCount)
	}
	if store.input.SportKey != "soccer" || store.input.League != "Australian A-League Women" ||
		store.input.HomeTeam != "Melbourne City (W)" || store.input.AwayTeam != "Melbourne Victory (W)" {
		t.Fatalf("store input=%+v", store.input)
	}
	var report report
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode confirm report %s: %v", out.String(), err)
	}
	if report.DryRun || !report.Created || report.CanonicalEventID != "canonical-1" {
		t.Fatalf("confirm report=%+v", report)
	}
	if strings.Contains(out.String(), "pol:secret") || strings.Contains(out.String(), rawURL) {
		t.Fatalf("report leaked database URL: %s", out.String())
	}
	if report.AliasAutoWrite || report.CollectorIntegration {
		t.Fatalf("boundary flags=%+v", report)
	}
}

func TestBetfairCanonicalEventValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing source event", args: removeArg(baseArgs(), "--source-event-id"), want: "source-event-id"},
		{name: "missing source market", args: removeArg(baseArgs(), "--source-market-id"), want: "source-market-id"},
		{name: "bad source", args: replaceArg(baseArgs(), "--source", "betfair_runtime"), want: "source must be"},
		{name: "missing sport", args: removeArg(baseArgs(), "--sport-key"), want: "sport-key"},
		{name: "missing league", args: removeArg(baseArgs(), "--league"), want: "league"},
		{name: "missing home", args: removeArg(baseArgs(), "--home-team"), want: "home-team"},
		{name: "same teams", args: replaceArg(baseArgs(), "--away-team", "Melbourne City (W)"), want: "must differ"},
		{name: "missing commence", args: removeArg(baseArgs(), "--commence-time"), want: "commence-time"},
		{name: "missing reviewer", args: removeArg(baseArgs(), "--reviewer"), want: "reviewer"},
		{name: "missing reason", args: removeArg(baseArgs(), "--reason"), want: "reason"},
		{name: "confirm missing database", args: append(baseArgs("--confirm-create"), "--output", "json"), want: "database-url-env"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := run(context.Background(), tc.args, &out, func(context.Context, string) (canonicalStore, error) {
				t.Fatal("database should not open for validation failure")
				return nil, nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want containing %q output=%s", err, tc.want, out.String())
			}
		})
	}
}

type fakeCanonicalStore struct {
	createCount int
	closeCount  int
	input       model.CanonicalEventCreateInput
}

func (s *fakeCanonicalStore) CreateReviewedCanonicalEvent(ctx context.Context, input model.CanonicalEventCreateInput) (model.CanonicalEventCreateResult, error) {
	if err := ctx.Err(); err != nil {
		return model.CanonicalEventCreateResult{}, err
	}
	s.createCount++
	s.input = input
	return model.CanonicalEventCreateResult{
		Created: true,
		Event: model.CanonicalEvent{
			ID:           "canonical-1",
			SportKey:     input.SportKey,
			League:       input.League,
			HomeTeam:     input.HomeTeam,
			AwayTeam:     input.AwayTeam,
			HomeTeamNorm: normalize.EventTeamNorm(input.HomeTeam),
			AwayTeamNorm: normalize.EventTeamNorm(input.AwayTeam),
			CommenceTime: input.CommenceTime,
			Status:       input.Status,
			CreatedAt:    fixedNow(),
			UpdatedAt:    fixedNow(),
		},
	}, nil
}

func (s *fakeCanonicalStore) Close() error {
	s.closeCount++
	return nil
}

func baseArgs(extra ...string) []string {
	args := []string{
		"--source", sourceBetfairCatalogue,
		"--source-event-id", "35561874",
		"--source-market-id", "1.257744548",
		"--sport-key", "soccer",
		"--league", "Australian A-League Women",
		"--home-team", "Melbourne City (W)",
		"--away-team", "Melbourne Victory (W)",
		"--commence-time", "2026-05-09T04:00:00Z",
		"--reviewer", "operator",
		"--reason", "catalogue metadata reviewed",
	}
	return append(args, extra...)
}

func removeArg(args []string, name string) []string {
	out := []string{}
	for i := 0; i < len(args); i++ {
		if args[i] == name {
			i++
			continue
		}
		out = append(out, args[i])
	}
	return out
}

func replaceArg(args []string, name, value string) []string {
	out := append([]string(nil), args...)
	for i := 0; i < len(out)-1; i++ {
		if out[i] == name {
			out[i+1] = value
			return out
		}
	}
	return out
}

func fixedNow() time.Time {
	return time.Date(2026, 5, 9, 4, 0, 0, 0, time.UTC)
}
