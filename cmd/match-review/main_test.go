package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

type fakePolymarketCandidateStore struct {
	report model.PolymarketCandidateReport
	err    error
	calls  int
	opts   model.PolymarketCandidateSearch
}

func (f *fakePolymarketCandidateStore) SearchPolymarketCandidates(_ context.Context, opts model.PolymarketCandidateSearch) (model.PolymarketCandidateReport, error) {
	f.calls++
	f.opts = opts
	if f.err != nil {
		return model.PolymarketCandidateReport{}, f.err
	}
	return f.report, nil
}

func TestRunWithoutArgsPrintsUsage(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), nil, &out)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("run without args error=%v want flag.ErrHelp", err)
	}
	if !strings.Contains(out.String(), "Usage:") {
		t.Fatalf("usage output missing Usage header:\n%s", out.String())
	}
}

func TestDecisionFlagsParseApproveArgs(t *testing.T) {
	fs := flag.NewFlagSet("approve", flag.ContinueOnError)
	decision := decisionFlags(fs)
	confidence := fs.Float64("confidence", -1, "optional confidence score")
	err := fs.Parse([]string{
		"--polymarket-event-id", "pm-event",
		"--polymarket-market-id", "pm-market",
		"--condition-id", "condition",
		"--canonical-event-id", "00000000-0000-0000-0000-000000000000",
		"--reviewer", "analyst",
		"--reason", "verified",
		"--match-reason", "same teams",
		"--confidence", "91.5",
	})
	if err != nil {
		t.Fatalf("parse approve flags: %v", err)
	}
	if decision.PolymarketEventID != "pm-event" {
		t.Fatalf("polymarket event id=%q", decision.PolymarketEventID)
	}
	if decision.PolymarketMarketID != "pm-market" {
		t.Fatalf("polymarket market id=%q", decision.PolymarketMarketID)
	}
	if decision.ConditionID != "condition" {
		t.Fatalf("condition id=%q", decision.ConditionID)
	}
	if decision.CanonicalEventID != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("canonical event id=%q", decision.CanonicalEventID)
	}
	if decision.Reviewer != "analyst" || decision.ReviewReason != "verified" || decision.MatchReason != "same teams" {
		t.Fatalf("decision metadata not parsed: %+v", decision)
	}
	if *confidence != 91.5 {
		t.Fatalf("confidence=%v want 91.5", *confidence)
	}
}

func TestApproveRejectArgumentValidationBeforeDatabaseUse(t *testing.T) {
	var out bytes.Buffer
	err := approve(context.Background(), nil, &out, []string{"--confidence", "101"})
	if err == nil || !strings.Contains(err.Error(), "confidence must be between 0 and 100") {
		t.Fatalf("approve over-confidence error=%v", err)
	}

	err = reject(context.Background(), nil, &out, []string{"--unknown"})
	if err == nil {
		t.Fatal("reject with unknown flag returned nil error")
	}
}

func TestSearchPolymarketRequiresCanonicalEventID(t *testing.T) {
	st := &fakePolymarketCandidateStore{}
	var out bytes.Buffer
	err := searchPolymarket(context.Background(), st, &out, nil)
	if err == nil || !strings.Contains(err.Error(), "--canonical-event-id is required") {
		t.Fatalf("err=%v want missing canonical-event-id", err)
	}
	if st.calls != 0 {
		t.Fatalf("store called %d times for invalid args; want 0", st.calls)
	}
}

func TestSearchPolymarketRejectsInvalidOutput(t *testing.T) {
	st := &fakePolymarketCandidateStore{}
	var out bytes.Buffer
	err := searchPolymarket(context.Background(), st, &out, []string{
		"--canonical-event-id", "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33",
		"--output", "xml",
	})
	if err == nil || !strings.Contains(err.Error(), "--output must be table or json") {
		t.Fatalf("err=%v want output validation", err)
	}
	if st.calls != 0 {
		t.Fatalf("store called %d times for invalid output; want 0", st.calls)
	}
}

func TestSearchPolymarketEmptyResultMentionsSyncSuggestion(t *testing.T) {
	canonicalID := "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33"
	commence := time.Date(2026, 5, 9, 19, 10, 0, 0, time.UTC)
	st := &fakePolymarketCandidateStore{
		report: model.PolymarketCandidateReport{
			CanonicalEvent: model.CanonicalEvent{
				ID:           canonicalID,
				SportKey:     "basketball_nba",
				HomeTeam:     "Cleveland Cavaliers",
				AwayTeam:     "Detroit Pistons",
				CommenceTime: commence,
			},
			HomeTokens: []string{"cavaliers", "cleveland"},
			AwayTokens: []string{"pistons", "detroit"},
		},
	}
	var out bytes.Buffer
	err := searchPolymarket(context.Background(), st, &out, []string{
		"--canonical-event-id", canonicalID,
		"--output", "table",
	})
	if err != nil {
		t.Fatalf("searchPolymarket: %v", err)
	}
	if st.calls != 1 || st.opts.CanonicalEventID != canonicalID {
		t.Fatalf("store opts=%+v calls=%d", st.opts, st.calls)
	}
	output := out.String()
	for _, want := range []string{
		canonicalID,
		"basketball_nba",
		"Cleveland Cavaliers vs Detroit Pistons",
		"home_search_tokens",
		"cavaliers,cleveland",
		"away_search_tokens",
		"pistons,detroit",
		"no local Polymarket candidates found",
		"cmd/polymarket-sync",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("table output missing %q:\n%s", want, output)
		}
	}
}

func TestSearchPolymarketActiveCandidateGetsActionableHint(t *testing.T) {
	canonicalID := "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33"
	commence := time.Date(2026, 5, 9, 19, 10, 0, 0, time.UTC)
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	yes := true
	no := false
	bestBid := 0.42
	bestAsk := 0.58
	mid := 0.50
	st := &fakePolymarketCandidateStore{
		report: model.PolymarketCandidateReport{
			CanonicalEvent: model.CanonicalEvent{
				ID:           canonicalID,
				SportKey:     "basketball_nba",
				HomeTeam:     "Cleveland Cavaliers",
				AwayTeam:     "Detroit Pistons",
				CommenceTime: commence,
			},
			HomeTokens: []string{"cavaliers"},
			AwayTokens: []string{"pistons"},
			Candidates: []model.PolymarketCandidate{{
				PolymarketEventID:    "evt-1",
				PolymarketEventTitle: "NBA: Cavaliers vs Pistons - May 9 2026",
				PolymarketMarketID:   "mkt-1",
				ConditionID:          "cond-1",
				MarketQuestion:       "Will the Cavaliers beat the Pistons on May 9?",
				EventActive:          &yes,
				EventClosed:          &no,
				EventArchived:        &no,
				MarketActive:         &yes,
				MarketClosed:         &no,
				MarketArchived:       &no,
				TokenCount:           2,
				LatestPriceTime:      &freshTime,
				LatestBestBid:        &bestBid,
				LatestBestAsk:        &bestAsk,
				LatestMidpoint:       &mid,
				HasBestBidAndAsk:     true,
				MatchedHomeToken:     "cavaliers",
				MatchedAwayToken:     "pistons",
				MatchedSource:        "both_team_tokens",
			}},
		},
	}
	var out bytes.Buffer
	err := searchPolymarket(context.Background(), st, &out, []string{
		"--canonical-event-id", canonicalID,
		"--output", "json",
	})
	if err != nil {
		t.Fatalf("searchPolymarket: %v", err)
	}
	var report polymarketCandidateReportView
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, out.String())
	}
	if len(report.Candidates) != 1 {
		t.Fatalf("decorated candidate count=%d want 1", len(report.Candidates))
	}
	view := report.Candidates[0]
	if !contains(view.SafetyHints, polymarketHintActiveMarket) {
		t.Fatalf("expected active hint in %v", view.SafetyHints)
	}
	if !strings.Contains(view.NextManualAction, "manually verify") {
		t.Fatalf("active candidate next_manual_action=%q want a manual-verification hint", view.NextManualAction)
	}
	if strings.Contains(strings.ToLower(view.NextManualAction), "auto") {
		t.Fatalf("next_manual_action must not promise auto behavior: %q", view.NextManualAction)
	}
}

func TestSearchPolymarketClosedCandidateIsHistoricalOnly(t *testing.T) {
	canonicalID := "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33"
	commence := time.Date(2026, 5, 9, 19, 10, 0, 0, time.UTC)
	yes := true
	no := false
	st := &fakePolymarketCandidateStore{
		report: model.PolymarketCandidateReport{
			CanonicalEvent: model.CanonicalEvent{
				ID:           canonicalID,
				SportKey:     "basketball_nba",
				HomeTeam:     "Cleveland Cavaliers",
				AwayTeam:     "Detroit Pistons",
				CommenceTime: commence,
			},
			HomeTokens: []string{"cavaliers"},
			AwayTokens: []string{"pistons"},
			Candidates: []model.PolymarketCandidate{{
				PolymarketEventID:    "2965",
				PolymarketEventTitle: "NBA: Will the Cavaliers beat the Pistons by 5.5 points - November 12, 2021",
				PolymarketMarketID:   "old-mkt",
				ConditionID:          "old-cond",
				MarketQuestion:       "Will Cavs beat Pistons by 5.5 on Nov 12 2021?",
				EventClosed:          &yes,
				MarketClosed:         &yes,
				EventActive:          &no,
				MarketActive:         &no,
				TokenCount:           2,
				HasBestBidAndAsk:     false,
				MatchedHomeToken:     "cavaliers",
				MatchedAwayToken:     "pistons",
				MatchedSource:        "both_team_tokens",
			}},
		},
	}
	var out bytes.Buffer
	err := searchPolymarket(context.Background(), st, &out, []string{
		"--canonical-event-id", canonicalID,
		"--output", "json",
	})
	if err != nil {
		t.Fatalf("searchPolymarket: %v", err)
	}
	var report polymarketCandidateReportView
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	view := report.Candidates[0]
	if !contains(view.SafetyHints, polymarketHintClosedOrArchived) {
		t.Fatalf("expected closed_or_archived hint in %v", view.SafetyHints)
	}
	if !strings.Contains(view.NextManualAction, "historical validation") {
		t.Fatalf("closed candidate next_manual_action=%q want historical-validation phrasing", view.NextManualAction)
	}
}

func TestSearchPolymarketMidpointOnlyIsNotExecutable(t *testing.T) {
	canonicalID := "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33"
	commence := time.Date(2026, 5, 9, 19, 10, 0, 0, time.UTC)
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	yes := true
	no := false
	mid := 0.5
	st := &fakePolymarketCandidateStore{
		report: model.PolymarketCandidateReport{
			CanonicalEvent: model.CanonicalEvent{
				ID:           canonicalID,
				SportKey:     "basketball_nba",
				HomeTeam:     "Cleveland Cavaliers",
				AwayTeam:     "Detroit Pistons",
				CommenceTime: commence,
			},
			HomeTokens: []string{"cavaliers"},
			AwayTokens: []string{"pistons"},
			Candidates: []model.PolymarketCandidate{{
				PolymarketEventID:    "evt-mid-only",
				PolymarketEventTitle: "Cavaliers vs Pistons (midpoint-only)",
				PolymarketMarketID:   "mid-mkt",
				ConditionID:          "mid-cond",
				EventActive:          &yes,
				MarketActive:         &yes,
				EventClosed:          &no,
				MarketClosed:         &no,
				EventArchived:        &no,
				MarketArchived:       &no,
				TokenCount:           2,
				LatestPriceTime:      &freshTime,
				LatestMidpoint:       &mid,
				LatestBestBid:        nil,
				LatestBestAsk:        nil,
				HasBestBidAndAsk:     false,
				MatchedHomeToken:     "cavaliers",
				MatchedAwayToken:     "pistons",
				MatchedSource:        "both_team_tokens",
			}},
		},
	}
	var out bytes.Buffer
	err := searchPolymarket(context.Background(), st, &out, []string{
		"--canonical-event-id", canonicalID,
		"--output", "json",
	})
	if err != nil {
		t.Fatalf("searchPolymarket: %v", err)
	}
	var report polymarketCandidateReportView
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	view := report.Candidates[0]
	if !contains(view.SafetyHints, polymarketHintNoExecutablePrice) {
		t.Fatalf("expected no-executable-price hint in %v", view.SafetyHints)
	}
	if !contains(view.SafetyHints, polymarketHintMidpointOnly) {
		t.Fatalf("expected midpoint-only hint in %v", view.SafetyHints)
	}
	if !strings.Contains(view.NextManualAction, "best bid AND best ask") {
		t.Fatalf("midpoint-only next_manual_action=%q want best-bid-AND-best-ask language", view.NextManualAction)
	}
}

func TestSearchPolymarketCandidateOutputContainsNoTradingTerminology(t *testing.T) {
	canonicalID := "5aea2bfb-2b88-4eb4-9bcc-b1a3c4510c33"
	yes := true
	no := false
	st := &fakePolymarketCandidateStore{
		report: model.PolymarketCandidateReport{
			CanonicalEvent: model.CanonicalEvent{
				ID:           canonicalID,
				SportKey:     "basketball_nba",
				HomeTeam:     "Cleveland Cavaliers",
				AwayTeam:     "Detroit Pistons",
				CommenceTime: time.Date(2026, 5, 9, 19, 10, 0, 0, time.UTC),
			},
			HomeTokens: []string{"cavaliers"},
			AwayTokens: []string{"pistons"},
			Candidates: []model.PolymarketCandidate{
				{
					PolymarketEventID: "active-1", PolymarketMarketID: "m1",
					EventActive: &yes, MarketActive: &yes, EventClosed: &no, MarketClosed: &no,
					TokenCount: 2, HasBestBidAndAsk: true,
				},
				{
					PolymarketEventID: "closed-1", PolymarketMarketID: "m2",
					EventClosed: &yes, MarketClosed: &yes, TokenCount: 2,
				},
				{
					PolymarketEventID: "no-tokens", PolymarketMarketID: "m3",
					EventActive: &yes, MarketActive: &yes, TokenCount: 0,
				},
			},
		},
	}
	var out bytes.Buffer
	err := searchPolymarket(context.Background(), st, &out, []string{
		"--canonical-event-id", canonicalID,
		"--output", "json",
	})
	if err != nil {
		t.Fatalf("searchPolymarket: %v", err)
	}
	body := strings.ToLower(out.String())
	for _, forbidden := range []string{
		"emits a trading signal",
		"emits a buy signal",
		"emits a sell signal",
		"places an order",
		"submits an order",
		"automatic execution",
		"recommended trade",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("output contains forbidden trading terminology %q:\n%s", forbidden, body)
		}
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
