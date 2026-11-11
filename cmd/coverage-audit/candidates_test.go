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
)

func TestCandidateScoreWeightsSumToOneHundred(t *testing.T) {
	total := 0
	for _, c := range candidateComponentWeights() {
		total += c.Weight
	}
	if total != maxCandidateScore {
		t.Fatalf("sum of component weights=%d want %d", total, maxCandidateScore)
	}
}

func TestCandidateGapClassification(t *testing.T) {
	yes := true
	no := false
	highLiquidity := 250.0
	lowLiquidity := 0.1
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	staleTime := time.Now().UTC().Add(-30 * time.Minute)
	aliasID := int64(7)
	commence := time.Date(2026, 5, 9, 4, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		row  model.PolymarketComparisonReadinessRow
		want string
	}{
		{
			name: "no external fair value at all",
			row: model.PolymarketComparisonReadinessRow{
				CommenceTime: commence,
			},
			want: candidateGapNoExternalFairValue,
		},
		{
			name: "betfair mapped but not ready (no sportsbook)",
			row: model.PolymarketComparisonReadinessRow{
				BetfairMappedQuoteCount: 6,
				CommenceTime:            commence,
			},
			want: candidateGapBetfairNotReady,
		},
		{
			name: "betfair ready but no sportsbook consensus",
			row: model.PolymarketComparisonReadinessRow{
				BetfairMappedQuoteCount:     6,
				BetfairReadyRunnerBookCount: 3,
				CommenceTime:                commence,
			},
			want: candidateGapNoSportsbookConsensus,
		},
		{
			name: "sportsbook present but no polymarket alias",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				CommenceTime:                    commence,
			},
			want: candidateGapNoConfirmedPolymarketAlias,
		},
		{
			name: "alias present but no market metadata",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				PolymarketAliasID:               &aliasID,
				PolymarketConditionID:           "0xcond",
				CommenceTime:                    commence,
			},
			want: candidateGapNoPolymarketMarketMetadata,
		},
		{
			name: "alias and metadata present but no tokens",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				PolymarketTokenCount:            0,
				CommenceTime:                    commence,
			},
			want: candidateGapNoPolymarketTokens,
		},
		{
			name: "tokens present but no executable price (midpoint only is not enough)",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				PolymarketTokenCount:            2,
				PolymarketPriceSnapshotCount:    2,
				HasBestBid:                      false,
				HasBestAsk:                      false,
				HasDepthOrLiquidity:             true,
				PolymarketLatestPriceTime:       &freshTime,
				CommenceTime:                    commence,
			},
			want: candidateGapNoPolymarketExecutablePrice,
		},
		{
			name: "executable price exists but stale",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				PolymarketTokenCount:            2,
				PolymarketPriceSnapshotCount:    2,
				HasBestBid:                      true,
				HasBestAsk:                      true,
				HasDepthOrLiquidity:             true,
				PolymarketLatestPriceTime:       &staleTime,
				CommenceTime:                    commence,
			},
			want: candidateGapPolymarketPriceStale,
		},
		{
			name: "fresh price but liquidity too thin",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				PolymarketTokenCount:            2,
				PolymarketPriceSnapshotCount:    2,
				HasBestBid:                      true,
				HasBestAsk:                      true,
				HasDepthOrLiquidity:             true,
				PolymarketMaxLiquidity:          &lowLiquidity,
				PolymarketLatestPriceTime:       &freshTime,
				CommenceTime:                    commence,
			},
			want: candidateGapLowPolymarketLiquidity,
		},
		{
			name: "market closed (resolution unverified proxy)",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				PolymarketTokenCount:            2,
				PolymarketPriceSnapshotCount:    2,
				HasBestBid:                      true,
				HasBestAsk:                      true,
				HasDepthOrLiquidity:             true,
				PolymarketMaxLiquidity:          &highLiquidity,
				PolymarketLatestPriceTime:       &freshTime,
				PolymarketMarketClosed:          &yes,
				CommenceTime:                    commence,
			},
			want: candidateGapResolutionUnverified,
		},
		{
			name: "fully covered event",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				BetfairReadyRunnerBookCount:     3,
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				PolymarketTokenCount:            2,
				PolymarketPriceSnapshotCount:    2,
				HasBestBid:                      true,
				HasBestAsk:                      true,
				HasDepthOrLiquidity:             true,
				PolymarketMaxLiquidity:          &highLiquidity,
				PolymarketLatestPriceTime:       &freshTime,
				PolymarketMarketClosed:          &no,
				PolymarketMarketArchived:        &no,
				CommenceTime:                    commence,
			},
			want: candidateGapComparisonReady,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := tc.row
			row.HasSportsbookConsensus = row.SportsbookConsensusOutcomeCount > 0
			row.HasReadyBetfair = row.BetfairReadyRunnerBookCount > 0
			row.HasConfirmedPolymarketAlias = row.PolymarketAliasID != nil
			row.HasPolymarketMarketMetadata = row.PolymarketMarketID != ""
			row.HasPolymarketTokens = row.PolymarketTokenCount > 0
			row.HasBestBidAndAsk = row.HasBestBid && row.HasBestAsk
			if row.PolymarketStaleState == "" {
				row.PolymarketStaleState = computePolymarketStaleState(row.PolymarketLatestPriceTime)
			}
			if row.ResolutionStatus == "" {
				row.ResolutionStatus = computeResolutionStatus(row)
			}
			if got := classifyCandidateGap(row); got != tc.want {
				t.Fatalf("classifyCandidateGap=%q want %q (row=%+v)", got, tc.want, row)
			}
		})
	}
}

func TestScoreCandidatesRanksFullyCoveredHighest(t *testing.T) {
	yes := true
	no := false
	highLiquidity := 250.0
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	aliasID := int64(7)

	rows := []model.PolymarketComparisonReadinessRow{
		// Empty event (worst).
		{CanonicalEventID: "empty-1"},
		// Sportsbook only.
		{
			CanonicalEventID:                "sportsbook-only",
			SportsbookConsensusOutcomeCount: 3,
		},
		// Sportsbook + alias + tokens but no price.
		{
			CanonicalEventID:                "no-price",
			SportsbookConsensusOutcomeCount: 3,
			PolymarketAliasID:               &aliasID,
			PolymarketMarketID:              "0xmarket",
			PolymarketConditionID:           "0xcond",
			PolymarketTokenCount:            2,
		},
		// Fully covered.
		{
			CanonicalEventID:                "ready-1",
			SportsbookConsensusOutcomeCount: 3,
			BetfairReadyRunnerBookCount:     3,
			PolymarketAliasID:               &aliasID,
			PolymarketMarketID:              "0xmarket-ready",
			PolymarketConditionID:           "0xcond-ready",
			PolymarketTokenCount:            2,
			PolymarketPriceSnapshotCount:    2,
			HasBestBid:                      true,
			HasBestAsk:                      true,
			HasDepthOrLiquidity:             true,
			PolymarketMaxLiquidity:          &highLiquidity,
			PolymarketLatestPriceTime:       &freshTime,
			PolymarketMarketClosed:          &no,
			PolymarketMarketArchived:        &no,
		},
		// Closed market (loses resolution_status_open component).
		{
			CanonicalEventID:                "closed-1",
			SportsbookConsensusOutcomeCount: 3,
			PolymarketAliasID:               &aliasID,
			PolymarketMarketID:              "0xmarket-closed",
			PolymarketConditionID:           "0xcond-closed",
			PolymarketTokenCount:            2,
			PolymarketPriceSnapshotCount:    2,
			HasBestBid:                      true,
			HasBestAsk:                      true,
			HasDepthOrLiquidity:             true,
			PolymarketMaxLiquidity:          &highLiquidity,
			PolymarketLatestPriceTime:       &freshTime,
			PolymarketMarketClosed:          &yes,
		},
	}

	scored := scoreCandidates(rows, time.Time{})
	sortCandidatesByReadiness(scored)

	if len(scored) != 5 {
		t.Fatalf("scored count=%d want 5", len(scored))
	}
	if scored[0].CanonicalEventID != "ready-1" {
		t.Fatalf("rank 1=%q want ready-1: %+v", scored[0].CanonicalEventID, scored[0])
	}
	if scored[0].ReadinessScore != maxCandidateScore {
		t.Fatalf("ready event score=%d want %d", scored[0].ReadinessScore, maxCandidateScore)
	}
	if !scored[0].ComparisonReady {
		t.Fatal("fully covered event must be marked comparison_ready")
	}

	last := scored[len(scored)-1]
	if last.CanonicalEventID != "empty-1" {
		t.Fatalf("rank last=%q want empty-1", last.CanonicalEventID)
	}
	if last.ReadinessScore != 0 {
		t.Fatalf("empty event score=%d want 0", last.ReadinessScore)
	}
	if last.PrimaryGapReason != candidateGapNoExternalFairValue {
		t.Fatalf("empty event gap=%q want %q", last.PrimaryGapReason, candidateGapNoExternalFairValue)
	}

	// Net comparison must remain not possible regardless of readiness.
	for _, row := range scored {
		if row.NetComparison {
			t.Fatalf("net comparison must remain not possible: %+v", row)
		}
	}
}

func TestScoreCandidatesEventWithOnlyBetfairMappedQuotes(t *testing.T) {
	// Mirrors the Phase 10K event: betfair mapped quotes only, no readiness, no
	// sportsbook, no Polymarket coverage. Expected primary gap is betfair_not_ready
	// (the row carries Betfair mapping data, just not in a ready state).
	rows := []model.PolymarketComparisonReadinessRow{{
		CanonicalEventID:        "phase-10k",
		BetfairMappedQuoteCount: 6,
	}}
	scored := scoreCandidates(rows, time.Time{})
	if len(scored) != 1 {
		t.Fatalf("scored count=%d want 1", len(scored))
	}
	row := scored[0]
	if row.PrimaryGapReason != candidateGapBetfairNotReady {
		t.Fatalf("primary gap=%q want %q", row.PrimaryGapReason, candidateGapBetfairNotReady)
	}
	if row.ReadinessScore != 0 {
		t.Fatalf("readiness_score=%d want 0", row.ReadinessScore)
	}
	if row.ComparisonReady {
		t.Fatal("comparison_ready must remain false")
	}
}

func TestScoreCandidatesPartialEvents(t *testing.T) {
	aliasID := int64(11)
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	highLiquidity := 250.0
	cases := []struct {
		name             string
		row              model.PolymarketComparisonReadinessRow
		wantPrimaryGap   string
		wantHasComponent string
		wantMissing      string
	}{
		{
			name: "sportsbook but no polymarket alias",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
			},
			wantPrimaryGap:   candidateGapNoConfirmedPolymarketAlias,
			wantHasComponent: componentSportsbookConsensus,
			wantMissing:      componentConfirmedPolymarketAlias,
		},
		{
			name: "polymarket alias but no prices",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				PolymarketTokenCount:            2,
			},
			wantPrimaryGap:   candidateGapNoPolymarketExecutablePrice,
			wantHasComponent: componentPolymarketTokens,
			wantMissing:      componentBestBidAndAsk,
		},
		{
			name: "polymarket midpoint only without bid/ask",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				PolymarketTokenCount:            2,
				PolymarketPriceSnapshotCount:    2,
				HasBestBid:                      false,
				HasBestAsk:                      false,
				HasDepthOrLiquidity:             true,
				PolymarketMaxLiquidity:          &highLiquidity,
				PolymarketLatestPriceTime:       &freshTime,
			},
			wantPrimaryGap:   candidateGapNoPolymarketExecutablePrice,
			wantHasComponent: componentPolymarketPriceFresh,
			wantMissing:      componentBestBidAndAsk,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scored := scoreCandidates([]model.PolymarketComparisonReadinessRow{tc.row}, time.Time{})
			row := scored[0]
			if row.PrimaryGapReason != tc.wantPrimaryGap {
				t.Fatalf("primary_gap=%q want %q", row.PrimaryGapReason, tc.wantPrimaryGap)
			}
			if !contains(row.PresentComponents, tc.wantHasComponent) {
				t.Fatalf("present_components=%v want includes %q", row.PresentComponents, tc.wantHasComponent)
			}
			if !contains(row.MissingComponents, tc.wantMissing) {
				t.Fatalf("missing_components=%v want includes %q", row.MissingComponents, tc.wantMissing)
			}
			if row.ComparisonReady {
				t.Fatalf("comparison_ready must remain false for %s", tc.name)
			}
		})
	}
}

func TestPolymarketCandidatesJSONOutput(t *testing.T) {
	databaseURL := "postgres://pol:pol@localhost/db"
	t.Setenv("CANDIDATE_TEST_DATABASE_URL", databaseURL)
	commence := time.Date(2026, 5, 9, 4, 0, 0, 0, time.UTC)
	st := &fakeCoverageAuditStore{
		polymarketRows: []model.PolymarketComparisonReadinessRow{
			{
				CanonicalEventID:        "phase-10k",
				SportKey:                "soccer",
				HomeTeam:                "Melbourne City (W)",
				AwayTeam:                "Melbourne Victory (W)",
				CommenceTime:            commence,
				BetfairMappedQuoteCount: 6,
			},
		},
	}
	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
		"--audit", "polymarket",
		"--candidates",
		"--sport", "soccer",
		"--now", "2026-05-08T00:00:00Z",
		"--output", "json",
	}, &out, func(ctx context.Context, gotURL string) (coverageAuditStore, error) {
		return st, nil
	})
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if !st.polymarketOpts.Candidates || st.polymarketOpts.SportKey != "soccer" {
		t.Fatalf("polymarket options=%+v", st.polymarketOpts)
	}
	var report candidateReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, out.String())
	}
	if report.Filters.SportKey != "soccer" {
		t.Fatalf("filters.SportKey=%q want soccer", report.Filters.SportKey)
	}
	if report.Summary.RowCount != 1 {
		t.Fatalf("summary.RowCount=%d want 1", report.Summary.RowCount)
	}
	if len(report.Rows) != 1 {
		t.Fatalf("rows=%d want 1", len(report.Rows))
	}
	row := report.Rows[0]
	if row.PrimaryGapReason != candidateGapBetfairNotReady {
		t.Fatalf("primary_gap=%q want %q", row.PrimaryGapReason, candidateGapBetfairNotReady)
	}
	if row.ComparisonReady {
		t.Fatal("comparison_ready must be false for Phase 10K event")
	}
	if row.EventTimingStatus != eventTimingUpcoming {
		t.Fatalf("event_timing_status=%q want %q", row.EventTimingStatus, eventTimingUpcoming)
	}
	if row.MaxReadinessScore != maxCandidateScore {
		t.Fatalf("max_readiness_score=%d want %d", row.MaxReadinessScore, maxCandidateScore)
	}
}

func TestPolymarketCandidatesTableOutput(t *testing.T) {
	t.Setenv("CANDIDATE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	highLiquidity := 250.0
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	no := false
	aliasID := int64(99)
	st := &fakeCoverageAuditStore{
		polymarketRows: []model.PolymarketComparisonReadinessRow{
			{
				CanonicalEventID:                "ready-1",
				SportKey:                        "soccer",
				HomeTeam:                        "Home",
				AwayTeam:                        "Away",
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				SportsbookConsensusOutcomeCount: 3,
				BetfairReadyRunnerBookCount:     3,
				PolymarketTokenCount:            2,
				PolymarketPriceSnapshotCount:    2,
				HasBestBid:                      true,
				HasBestAsk:                      true,
				HasDepthOrLiquidity:             true,
				PolymarketMaxLiquidity:          &highLiquidity,
				PolymarketLatestPriceTime:       &freshTime,
				PolymarketMarketClosed:          &no,
				PolymarketMarketArchived:        &no,
			},
		},
	}
	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
		"--audit", "polymarket",
		"--candidates",
		"--output", "table",
	}, &out, func(ctx context.Context, databaseURL string) (coverageAuditStore, error) {
		return st, nil
	})
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	output := out.String()
	for _, want := range []string{
		"rank",
		"readiness_score",
		"comparison_ready",
		"primary_gap_reason",
		"missing_components",
		"next_manual_action",
		"comparison_ready",
		"ready-1",
		fmt.Sprintf("%d/%d", maxCandidateScore, maxCandidateScore),
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("table output missing %q:\n%s", want, output)
		}
	}
}

func TestCandidatesValidationRequiresPolymarketMode(t *testing.T) {
	t.Setenv("CANDIDATE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "candidates requires polymarket mode",
			args: []string{
				"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
				"--candidates",
			},
			want: "--candidates and --sport require --audit polymarket",
		},
		{
			name: "sport requires polymarket mode",
			args: []string{
				"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
				"--sport", "soccer",
			},
			want: "--candidates and --sport require --audit polymarket",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := run(context.Background(), tc.args, &bytes.Buffer{}, func(ctx context.Context, databaseURL string) (coverageAuditStore, error) {
				t.Fatal("store factory should not be called for invalid config")
				return nil, nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want contains %q", err, tc.want)
			}
		})
	}
}

func TestCandidatesNoTradingTerminology(t *testing.T) {
	yes := true
	no := false
	highLiquidity := 250.0
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	staleTime := time.Now().UTC().Add(-30 * time.Minute)
	aliasID := int64(13)
	rows := []model.PolymarketComparisonReadinessRow{
		{},
		{BetfairMappedQuoteCount: 3},
		{SportsbookConsensusOutcomeCount: 3, BetfairReadyRunnerBookCount: 1},
		{SportsbookConsensusOutcomeCount: 3, PolymarketAliasID: &aliasID, PolymarketMarketID: "m", PolymarketConditionID: "c"},
		{SportsbookConsensusOutcomeCount: 3, PolymarketAliasID: &aliasID, PolymarketMarketID: "m", PolymarketConditionID: "c", PolymarketTokenCount: 2, PolymarketPriceSnapshotCount: 2, HasBestBid: true, HasBestAsk: true, HasDepthOrLiquidity: true, PolymarketLatestPriceTime: &staleTime},
		{SportsbookConsensusOutcomeCount: 3, PolymarketAliasID: &aliasID, PolymarketMarketID: "m", PolymarketConditionID: "c", PolymarketTokenCount: 2, PolymarketPriceSnapshotCount: 2, HasBestBid: true, HasBestAsk: true, HasDepthOrLiquidity: true, PolymarketMaxLiquidity: &highLiquidity, PolymarketLatestPriceTime: &freshTime, PolymarketMarketClosed: &yes},
		{SportsbookConsensusOutcomeCount: 3, PolymarketAliasID: &aliasID, PolymarketMarketID: "m", PolymarketConditionID: "c", PolymarketTokenCount: 2, PolymarketPriceSnapshotCount: 2, HasBestBid: true, HasBestAsk: true, HasDepthOrLiquidity: true, PolymarketMaxLiquidity: &highLiquidity, PolymarketLatestPriceTime: &freshTime, PolymarketMarketClosed: &no, PolymarketMarketArchived: &no},
	}
	scored := scoreCandidates(rows, time.Time{})
	forbidden := []string{
		"trading signal",
		"buy signal",
		"sell signal",
		"execute trade",
		"order placement",
		"private key",
		"wallet",
		"automatic execution",
		"recommended trade",
	}
	for i, row := range scored {
		text := strings.ToLower(row.PrimaryGapReason + " | " + row.NextManualAction + " | " + strings.Join(row.MissingComponents, ",") + " | " + strings.Join(row.PresentComponents, ","))
		for _, term := range forbidden {
			if strings.Contains(text, term) {
				t.Fatalf("row %d contains forbidden trading terminology %q: %s", i, term, text)
			}
		}
	}
}

func contains(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}
