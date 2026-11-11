package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

func TestCoverageClassification(t *testing.T) {
	tests := []struct {
		name string
		row  model.BetfairSportsbookCoverageAuditRow
		want string
	}{
		{
			name: "betfair mapped rows but no sportsbook consensus",
			row: model.BetfairSportsbookCoverageAuditRow{
				CanonicalMarketFamily:           "h2h",
				MappedBetfairQuoteCount:         6,
				ReadyBetfairRunnerBookCount:     0,
				SportsbookConsensusOutcomeCount: 0,
				ReadinessStates:                 []string{"stale", "thin_liquidity"},
			},
			want: "no_sportsbook_consensus",
		},
		{
			name: "betfair mapped rows and sportsbook consensus ready",
			row: model.BetfairSportsbookCoverageAuditRow{
				CanonicalMarketFamily:           "h2h",
				MappedBetfairQuoteCount:         6,
				ReadyBetfairRunnerBookCount:     3,
				SportsbookConsensusOutcomeCount: 3,
				ComparisonRowCount:              3,
				ReadyComparisonRowCount:         3,
			},
			want: "comparison_ready",
		},
		{
			name: "missing betfair mapping",
			row: model.BetfairSportsbookCoverageAuditRow{
				SportsbookConsensusOutcomeCount: 2,
			},
			want: "mapping_missing",
		},
		{
			name: "sportsbook exists but stale betfair is not ready",
			row: model.BetfairSportsbookCoverageAuditRow{
				CanonicalMarketFamily:           "h2h",
				MappedBetfairQuoteCount:         4,
				SportsbookConsensusOutcomeCount: 2,
				ReadinessStates:                 []string{"stale"},
			},
			want: "stale_or_low_confidence",
		},
		{
			name: "sportsbook exists but outcome join is missing",
			row: model.BetfairSportsbookCoverageAuditRow{
				CanonicalMarketFamily:           "h2h",
				MappedBetfairQuoteCount:         4,
				ReadyBetfairRunnerBookCount:     2,
				SportsbookConsensusOutcomeCount: 2,
			},
			want: "no_matching_outcome",
		},
		{
			name: "unsupported market family",
			row: model.BetfairSportsbookCoverageAuditRow{
				CanonicalMarketFamily:           "totals",
				MappedBetfairQuoteCount:         4,
				SportsbookConsensusOutcomeCount: 2,
			},
			want: "unsupported_market_family",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyCoverage(tc.row); got != tc.want {
				t.Fatalf("classifyCoverage=%q want %q", got, tc.want)
			}
		})
	}
}

func TestCoverageAuditJSONOutput(t *testing.T) {
	databaseURL := "postgres://secret-user:secret-pass@localhost/db"
	t.Setenv("COVERAGE_TEST_DATABASE_URL", databaseURL)
	commence := time.Date(2026, 5, 9, 4, 0, 0, 0, time.UTC)
	store := &fakeCoverageAuditStore{
		rows: []model.BetfairSportsbookCoverageAuditRow{{
			CanonicalEventID:                "7a27c64d-d8d1-497a-8826-697aff36cd36",
			SportKey:                        "soccer",
			League:                          "Australian A-League Women",
			HomeTeam:                        "Melbourne City (W)",
			AwayTeam:                        "Melbourne Victory (W)",
			CommenceTime:                    commence,
			BetfairMarketID:                 "1.257744548",
			CanonicalMarketFamily:           "h2h",
			MappedBetfairQuoteCount:         6,
			MappedBetfairRunnerCount:        3,
			BetfairRunnerBookCount:          3,
			ReadyBetfairRunnerBookCount:     0,
			SportsbookConsensusOutcomeCount: 0,
			ComparisonRowCount:              0,
			ReadyComparisonRowCount:         0,
			ReadinessStates:                 []string{"stale", "thin_liquidity"},
		}},
	}
	var out bytes.Buffer

	err := run(context.Background(), []string{
		"--database-url-env", "COVERAGE_TEST_DATABASE_URL",
		"--canonical-event-id", "7a27c64d-d8d1-497a-8826-697aff36cd36",
		"--output", "json",
	}, &out, func(ctx context.Context, gotURL string) (coverageAuditStore, error) {
		if gotURL != databaseURL {
			t.Fatalf("database url=%q want %q", gotURL, databaseURL)
		}
		return store, nil
	})
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if !store.closed {
		t.Fatal("store was not closed")
	}
	if store.opts.CanonicalEventID != "7a27c64d-d8d1-497a-8826-697aff36cd36" {
		t.Fatalf("canonical event filter=%q", store.opts.CanonicalEventID)
	}

	output := out.String()
	if strings.Contains(output, "secret-pass") {
		t.Fatalf("output leaked database URL secret:\n%s", output)
	}
	var report coverageAuditReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, output)
	}
	if report.Summary.RowCount != 1 || report.Summary.ReasonCounts["no_sportsbook_consensus"] != 1 {
		t.Fatalf("unexpected summary: %+v", report.Summary)
	}
	row := report.Rows[0]
	if !row.BetfairMapped || row.SportsbookConsensus || row.ComparisonReady {
		t.Fatalf("unexpected coverage booleans: %+v", row)
	}
	if row.GapReason != "no_sportsbook_consensus" {
		t.Fatalf("reason=%q want no_sportsbook_consensus", row.GapReason)
	}
}

func TestCoverageAuditTableOutput(t *testing.T) {
	t.Setenv("COVERAGE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	var out bytes.Buffer

	err := run(context.Background(), []string{
		"--database-url-env", "COVERAGE_TEST_DATABASE_URL",
		"--betfair-market-id", "1.257744548",
		"--output", "table",
	}, &out, func(ctx context.Context, databaseURL string) (coverageAuditStore, error) {
		return &fakeCoverageAuditStore{rows: []model.BetfairSportsbookCoverageAuditRow{{
			CanonicalEventID:                "event-1",
			HomeTeam:                        "Home",
			AwayTeam:                        "Away",
			BetfairMarketID:                 "1.257744548",
			CanonicalMarketFamily:           "h2h",
			MappedBetfairQuoteCount:         2,
			ReadyBetfairRunnerBookCount:     1,
			SportsbookConsensusOutcomeCount: 2,
			ComparisonRowCount:              1,
			ReadyComparisonRowCount:         1,
		}}}, nil
	})
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	output := out.String()
	for _, want := range []string{
		"canonical_event_id",
		"betfair_market_id",
		"1.257744548",
		"comparison_ready",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("table output missing %q:\n%s", want, output)
		}
	}
}

func TestCoverageAuditValidation(t *testing.T) {
	t.Setenv("COVERAGE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "database env required",
			args: []string{"--recent"},
			want: "database-url-env is required",
		},
		{
			name: "filter required",
			args: []string{"--database-url-env", "COVERAGE_TEST_DATABASE_URL"},
			want: "one of --canonical-event-id",
		},
		{
			name: "output validation",
			args: []string{"--database-url-env", "COVERAGE_TEST_DATABASE_URL", "--recent", "--output", "xml"},
			want: "output must be",
		},
		{
			name: "audit validation",
			args: []string{"--database-url-env", "COVERAGE_TEST_DATABASE_URL", "--recent", "--audit", "trade"},
			want: "audit must be",
		},
		{
			name: "polymarket flags require polymarket audit",
			args: []string{"--database-url-env", "COVERAGE_TEST_DATABASE_URL", "--polymarket-event-id", "evt"},
			want: "require --audit polymarket",
		},
		{
			name: "env value required",
			args: []string{"--database-url-env", "MISSING_COVERAGE_DATABASE_URL", "--recent"},
			want: "is not set or empty",
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

func TestCoverageAuditStoreOpenErrorRedactsDatabaseURL(t *testing.T) {
	databaseURL := "postgres://secret-user:secret-pass@localhost/db"
	t.Setenv("COVERAGE_TEST_DATABASE_URL", databaseURL)
	err := run(context.Background(), []string{
		"--database-url-env", "COVERAGE_TEST_DATABASE_URL",
		"--recent",
	}, &bytes.Buffer{}, func(ctx context.Context, databaseURL string) (coverageAuditStore, error) {
		return nil, errors.New("open failed for " + databaseURL)
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "secret-pass") || !strings.Contains(err.Error(), "<redacted>") {
		t.Fatalf("database URL was not redacted: %v", err)
	}
}

func TestPolymarketReadinessClassification(t *testing.T) {
	yes := true
	no := false
	lowLiquidity := 0.1
	highLiquidity := 250.0

	commence := time.Date(2026, 5, 9, 4, 0, 0, 0, time.UTC)
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	staleTime := time.Now().UTC().Add(-30 * time.Minute)

	aliasID := int64(42)

	tests := []struct {
		name string
		row  model.PolymarketComparisonReadinessRow
		want string
	}{
		{
			name: "no sportsbook and no betfair-ready",
			row:  model.PolymarketComparisonReadinessRow{CommenceTime: commence},
			want: externalFairValueMissingState,
		},
		{
			name: "betfair mapped but no sportsbook consensus",
			row: model.PolymarketComparisonReadinessRow{
				BetfairMappedQuoteCount:     6,
				BetfairReadyRunnerBookCount: 3,
				CommenceTime:                commence,
			},
			want: sportsbookConsensusMissingState,
		},
		{
			name: "sportsbook present but no polymarket alias",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				CommenceTime:                    commence,
			},
			want: polymarketMappingMissingState,
		},
		{
			name: "alias present but tokens missing",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				PolymarketTokenCount:            0,
				CommenceTime:                    commence,
			},
			want: polymarketTokenMissingState,
		},
		{
			name: "alias and tokens present but no price snapshots",
			row: model.PolymarketComparisonReadinessRow{
				SportsbookConsensusOutcomeCount: 3,
				PolymarketAliasID:               &aliasID,
				PolymarketMarketID:              "0xmarket",
				PolymarketConditionID:           "0xcond",
				PolymarketTokenCount:            2,
				PolymarketPriceSnapshotCount:    0,
				CommenceTime:                    commence,
			},
			want: polymarketPriceMissingState,
		},
		{
			name: "price exists but is stale",
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
			want: polymarketPriceStaleState,
		},
		{
			name: "fresh price but liquidity below threshold",
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
			want: polymarketLiquidityLowState,
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
			want: resolutionUnverifiedState,
		},
		{
			name: "fully covered event marked comparison_ready",
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
				PolymarketMarketClosed:          &no,
				PolymarketMarketArchived:        &no,
				CommenceTime:                    commence,
			},
			want: comparisonReadyState,
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
			if got := classifyPolymarketReadiness(row); got != tc.want {
				t.Fatalf("classifyPolymarketReadiness=%q want %q (row=%+v)", got, tc.want, row)
			}
		})
	}
}

func TestPolymarketReadinessNetComparisonNeverPossible(t *testing.T) {
	highLiquidity := 250.0
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	aliasID := int64(7)
	no := false
	rows := []model.PolymarketComparisonReadinessRow{{
		CanonicalEventID:                "event-1",
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
		PolymarketMarketClosed:          &no,
		PolymarketMarketArchived:        &no,
	}}
	decorated := decoratePolymarketRows(rows)
	if !decorated[0].ComparisonReady {
		t.Fatalf("expected comparison_ready, got %+v", decorated[0])
	}
	if decorated[0].NetComparisonPossible {
		t.Fatal("net comparison must remain not possible until a fee/cost model exists")
	}
}

func TestPolymarketReadinessMidpointIsNotExecutable(t *testing.T) {
	// A row where best_ask is missing but a midpoint exists must NOT be flagged
	// as gross_comparison_possible — midpoint is analytical, not executable.
	highLiquidity := 250.0
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	aliasID := int64(9)
	rows := []model.PolymarketComparisonReadinessRow{{
		CanonicalEventID:                "event-2",
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
	}}
	decorated := decoratePolymarketRows(rows)
	if decorated[0].GrossComparisonPossible {
		t.Fatal("gross comparison must require best_ask, not midpoint")
	}
	if decorated[0].ComparisonReady {
		t.Fatal("comparison_ready must not be set when best bid/ask are missing")
	}
	if decorated[0].GapReason != polymarketPriceMissingState {
		t.Fatalf("gap_reason=%q want %q", decorated[0].GapReason, polymarketPriceMissingState)
	}
}

func TestPolymarketReadinessNoTradingTerminology(t *testing.T) {
	highLiquidity := 250.0
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	staleTime := time.Now().UTC().Add(-30 * time.Minute)
	aliasID := int64(11)
	yes := true
	no := false
	cases := []model.PolymarketComparisonReadinessRow{
		{},
		{SportsbookConsensusOutcomeCount: 3, BetfairReadyRunnerBookCount: 1},
		{SportsbookConsensusOutcomeCount: 3, PolymarketAliasID: &aliasID, PolymarketMarketID: "0xmarket", PolymarketConditionID: "0xcond"},
		{SportsbookConsensusOutcomeCount: 3, PolymarketAliasID: &aliasID, PolymarketMarketID: "0xmarket", PolymarketConditionID: "0xcond", PolymarketTokenCount: 2, PolymarketPriceSnapshotCount: 2, HasBestBid: true, HasBestAsk: true, HasDepthOrLiquidity: true, PolymarketLatestPriceTime: &staleTime},
		{SportsbookConsensusOutcomeCount: 3, PolymarketAliasID: &aliasID, PolymarketMarketID: "0xmarket", PolymarketConditionID: "0xcond", PolymarketTokenCount: 2, PolymarketPriceSnapshotCount: 2, HasBestBid: true, HasBestAsk: true, HasDepthOrLiquidity: true, PolymarketMaxLiquidity: &highLiquidity, PolymarketLatestPriceTime: &freshTime, PolymarketMarketClosed: &yes},
		{SportsbookConsensusOutcomeCount: 3, PolymarketAliasID: &aliasID, PolymarketMarketID: "0xmarket", PolymarketConditionID: "0xcond", PolymarketTokenCount: 2, PolymarketPriceSnapshotCount: 2, HasBestBid: true, HasBestAsk: true, HasDepthOrLiquidity: true, PolymarketMaxLiquidity: &highLiquidity, PolymarketLatestPriceTime: &freshTime, PolymarketMarketClosed: &no, PolymarketMarketArchived: &no},
	}
	decorated := decoratePolymarketRows(cases)
	forbidden := []string{
		"trading signal",
		"buy signal",
		"sell signal",
		"execute",
		"order placement",
		"private key",
		"wallet",
		"automatic",
		"recommended trade",
	}
	for i, row := range decorated {
		text := strings.ToLower(row.GapReason + " | " + row.NextManualAction)
		for _, term := range forbidden {
			if strings.Contains(text, term) {
				t.Fatalf("row %d contains forbidden trading terminology %q: %s", i, term, text)
			}
		}
	}
}

func TestPolymarketAuditJSONOutput(t *testing.T) {
	databaseURL := "postgres://pol:pol@localhost/db"
	t.Setenv("COVERAGE_TEST_DATABASE_URL", databaseURL)
	commence := time.Date(2026, 5, 9, 4, 0, 0, 0, time.UTC)
	st := &fakeCoverageAuditStore{
		polymarketRows: []model.PolymarketComparisonReadinessRow{{
			CanonicalEventID:                "7a27c64d-d8d1-497a-8826-697aff36cd36",
			SportKey:                        "soccer",
			League:                          "Australian A-League Women",
			HomeTeam:                        "Melbourne City (W)",
			AwayTeam:                        "Melbourne Victory (W)",
			CommenceTime:                    commence,
			BetfairMappedQuoteCount:         6,
			BetfairReadyRunnerBookCount:     0,
			SportsbookConsensusOutcomeCount: 0,
		}},
	}
	var out bytes.Buffer

	err := run(context.Background(), []string{
		"--database-url-env", "COVERAGE_TEST_DATABASE_URL",
		"--audit", "polymarket",
		"--canonical-event-id", "7a27c64d-d8d1-497a-8826-697aff36cd36",
		"--output", "json",
	}, &out, func(ctx context.Context, gotURL string) (coverageAuditStore, error) {
		return st, nil
	})
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if !st.closed {
		t.Fatal("store was not closed")
	}
	if st.polymarketOpts.CanonicalEventID != "7a27c64d-d8d1-497a-8826-697aff36cd36" {
		t.Fatalf("canonical event filter=%q", st.polymarketOpts.CanonicalEventID)
	}
	var report polymarketReadinessReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, out.String())
	}
	if report.Summary.RowCount != 1 {
		t.Fatalf("unexpected summary: %+v", report.Summary)
	}
	row := report.Rows[0]
	if row.ComparisonReady {
		t.Fatal("expected comparison_ready=false for Phase 10K event with no sportsbook consensus")
	}
	if row.GapReason != externalFairValueMissingState {
		t.Fatalf("gap_reason=%q want %q", row.GapReason, externalFairValueMissingState)
	}
	if row.NetComparisonPossible {
		t.Fatal("net comparison must remain not possible")
	}
	if report.Summary.ReasonCounts[externalFairValueMissingState] != 1 {
		t.Fatalf("reason summary=%+v", report.Summary.ReasonCounts)
	}
}

func TestPolymarketAuditTableOutput(t *testing.T) {
	t.Setenv("COVERAGE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	highLiquidity := 250.0
	freshTime := time.Now().UTC().Add(-30 * time.Second)
	no := false
	aliasID := int64(123)
	st := &fakeCoverageAuditStore{
		polymarketRows: []model.PolymarketComparisonReadinessRow{{
			CanonicalEventID:                "ready-event-1",
			HomeTeam:                        "Home",
			AwayTeam:                        "Away",
			PolymarketMarketID:              "0xmarket",
			PolymarketConditionID:           "0xcond",
			PolymarketAliasID:               &aliasID,
			SportsbookConsensusOutcomeCount: 3,
			PolymarketTokenCount:            2,
			PolymarketPriceSnapshotCount:    2,
			HasBestBid:                      true,
			HasBestAsk:                      true,
			HasDepthOrLiquidity:             true,
			PolymarketMaxLiquidity:          &highLiquidity,
			PolymarketLatestPriceTime:       &freshTime,
			PolymarketMarketClosed:          &no,
			PolymarketMarketArchived:        &no,
		}},
	}
	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--database-url-env", "COVERAGE_TEST_DATABASE_URL",
		"--audit", "polymarket",
		"--recent",
		"--output", "table",
	}, &out, func(ctx context.Context, databaseURL string) (coverageAuditStore, error) {
		return st, nil
	})
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	output := out.String()
	for _, want := range []string{
		"canonical_event_id",
		"polymarket_market_id",
		"comparison_ready",
		"gap_reason",
		"next_manual_action",
		"0xmarket",
		"true",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("table output missing %q:\n%s", want, output)
		}
	}
}

type fakeCoverageAuditStore struct {
	rows           []model.BetfairSportsbookCoverageAuditRow
	opts           model.BetfairSportsbookCoverageAuditOptions
	polymarketRows []model.PolymarketComparisonReadinessRow
	polymarketOpts model.PolymarketComparisonReadinessOptions
	closed         bool
}

func (s *fakeCoverageAuditStore) ListBetfairSportsbookCoverageAudit(_ context.Context, opts model.BetfairSportsbookCoverageAuditOptions) ([]model.BetfairSportsbookCoverageAuditRow, error) {
	s.opts = opts
	return s.rows, nil
}

func (s *fakeCoverageAuditStore) ListPolymarketComparisonReadiness(_ context.Context, opts model.PolymarketComparisonReadinessOptions) ([]model.PolymarketComparisonReadinessRow, error) {
	s.polymarketOpts = opts
	return s.polymarketRows, nil
}

func (s *fakeCoverageAuditStore) Close() error {
	s.closed = true
	return nil
}
