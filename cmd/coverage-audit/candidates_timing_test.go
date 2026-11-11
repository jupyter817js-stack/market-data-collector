package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

func TestClassifyEventTiming(t *testing.T) {
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		commence  time.Time
		want      string
		wantAge   bool
		wantStart bool
	}{
		{
			name:      "upcoming far in the future",
			commence:  now.Add(24 * time.Hour),
			want:      eventTimingUpcoming,
			wantStart: true,
		},
		{
			name:      "starting soon (within window)",
			commence:  now.Add(30 * time.Minute),
			want:      eventTimingStartingSoon,
			wantStart: true,
		},
		{
			name:      "starting soon at edge of window",
			commence:  now.Add(defaultStartingSoonWindow - time.Minute),
			want:      eventTimingStartingSoon,
			wantStart: true,
		},
		{
			name:      "in play / just started",
			commence:  now.Add(-30 * time.Minute),
			want:      eventTimingInPlayOrStarted,
			wantAge:   true,
			wantStart: true,
		},
		{
			name:      "in play at edge of started window",
			commence:  now.Add(-(defaultMaxStartedWindow - time.Minute)),
			want:      eventTimingInPlayOrStarted,
			wantAge:   true,
			wantStart: true,
		},
		{
			name:      "past well beyond max started window",
			commence:  now.Add(-72 * time.Hour),
			want:      eventTimingPast,
			wantAge:   true,
			wantStart: true,
		},
		{
			name:     "unknown commence_time",
			commence: time.Time{},
			want:     eventTimingUnknown,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, ttsSec, ageSec, warning := classifyEventTiming(tc.commence, now)
			if status != tc.want {
				t.Fatalf("status=%q want %q", status, tc.want)
			}
			if tc.wantStart && ttsSec == nil {
				t.Fatal("expected non-nil time_to_start_seconds")
			}
			if !tc.wantStart && ttsSec != nil {
				t.Fatalf("expected nil time_to_start_seconds, got %v", *ttsSec)
			}
			if tc.wantAge && ageSec == nil {
				t.Fatal("expected non-nil event_age_seconds")
			}
			if !tc.wantAge && ageSec != nil {
				t.Fatalf("expected nil event_age_seconds, got %v", *ageSec)
			}
			if status == eventTimingUpcoming && warning != "" {
				t.Fatalf("upcoming should have no warning, got %q", warning)
			}
			if status == eventTimingPast && warning == "" {
				t.Fatal("past event should carry a warning")
			}
		})
	}
}

func TestPolymarketCandidatesDefaultExcludesPastEvents(t *testing.T) {
	t.Setenv("CANDIDATE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	highLiquidity := 250.0
	freshTime := now.Add(-30 * time.Second)
	no := false
	aliasID := int64(7)

	upcoming := model.PolymarketComparisonReadinessRow{
		CanonicalEventID:                "upcoming-1",
		SportKey:                        "soccer",
		HomeTeam:                        "Future Home",
		AwayTeam:                        "Future Away",
		CommenceTime:                    now.Add(48 * time.Hour),
		SportsbookConsensusOutcomeCount: 3,
		BetfairReadyRunnerBookCount:     3,
		PolymarketAliasID:               &aliasID,
		PolymarketMarketID:              "0xfuture",
		PolymarketConditionID:           "0xfuture",
		PolymarketTokenCount:            2,
		PolymarketPriceSnapshotCount:    2,
		HasBestBid:                      true,
		HasBestAsk:                      true,
		HasDepthOrLiquidity:             true,
		PolymarketMaxLiquidity:          &highLiquidity,
		PolymarketLatestPriceTime:       &freshTime,
		PolymarketMarketClosed:          &no,
		PolymarketMarketArchived:        &no,
	}
	past := model.PolymarketComparisonReadinessRow{
		CanonicalEventID:                "past-1",
		SportKey:                        "soccer",
		HomeTeam:                        "Past Home",
		AwayTeam:                        "Past Away",
		CommenceTime:                    now.Add(-72 * time.Hour),
		SportsbookConsensusOutcomeCount: 3,
		BetfairReadyRunnerBookCount:     3,
		PolymarketAliasID:               &aliasID,
		PolymarketMarketID:              "0xpast",
		PolymarketConditionID:           "0xpast",
		PolymarketTokenCount:            2,
		PolymarketPriceSnapshotCount:    2,
		HasBestBid:                      true,
		HasBestAsk:                      true,
		HasDepthOrLiquidity:             true,
		PolymarketMaxLiquidity:          &highLiquidity,
		PolymarketLatestPriceTime:       &freshTime,
		PolymarketMarketClosed:          &no,
		PolymarketMarketArchived:        &no,
	}
	st := &fakeCoverageAuditStore{polymarketRows: []model.PolymarketComparisonReadinessRow{upcoming, past}}

	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
		"--audit", "polymarket",
		"--candidates",
		"--now", "2026-05-09T12:00:00Z",
		"--output", "json",
	}, &out, func(ctx context.Context, _ string) (coverageAuditStore, error) { return st, nil })
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	var report candidateReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, out.String())
	}
	if report.Summary.RowCount != 1 {
		t.Fatalf("summary.RowCount=%d want 1 (past should be excluded)", report.Summary.RowCount)
	}
	if report.Summary.FilteredOutByTimingCount != 1 {
		t.Fatalf("filtered_out_by_timing_count=%d want 1", report.Summary.FilteredOutByTimingCount)
	}
	if report.Rows[0].CanonicalEventID != "upcoming-1" {
		t.Fatalf("kept event=%q want upcoming-1", report.Rows[0].CanonicalEventID)
	}
	if report.Rows[0].EventTimingStatus != eventTimingUpcoming {
		t.Fatalf("event_timing_status=%q want %q", report.Rows[0].EventTimingStatus, eventTimingUpcoming)
	}
	// The store must have received commence_after = now - max_started_window so
	// the SQL fetch already trims deeply-past rows.
	if st.polymarketOpts.CommenceAfter == nil {
		t.Fatal("expected store CommenceAfter to be set under default candidates mode")
	} else if got := st.polymarketOpts.CommenceAfter.UTC(); got != now.Add(-defaultMaxStartedWindow) {
		t.Fatalf("CommenceAfter=%s want %s", got, now.Add(-defaultMaxStartedWindow))
	}
}

func TestPolymarketCandidatesIncludePastIncludesAndMarksPast(t *testing.T) {
	t.Setenv("CANDIDATE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	aliasID := int64(7)
	rows := []model.PolymarketComparisonReadinessRow{
		{
			CanonicalEventID:                "past-1",
			HomeTeam:                        "Past Home",
			AwayTeam:                        "Past Away",
			CommenceTime:                    now.Add(-72 * time.Hour),
			SportsbookConsensusOutcomeCount: 3,
			PolymarketAliasID:               &aliasID,
			PolymarketMarketID:              "0xpast",
			PolymarketConditionID:           "0xpast",
			PolymarketTokenCount:            2,
		},
		{
			CanonicalEventID:                "upcoming-1",
			HomeTeam:                        "Future Home",
			AwayTeam:                        "Future Away",
			CommenceTime:                    now.Add(48 * time.Hour),
			SportsbookConsensusOutcomeCount: 3,
			PolymarketAliasID:               &aliasID,
			PolymarketMarketID:              "0xfuture",
			PolymarketConditionID:           "0xfuture",
			PolymarketTokenCount:            2,
		},
	}
	st := &fakeCoverageAuditStore{polymarketRows: rows}
	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
		"--audit", "polymarket",
		"--candidates",
		"--include-past",
		"--now", "2026-05-09T12:00:00Z",
		"--output", "json",
	}, &out, func(ctx context.Context, _ string) (coverageAuditStore, error) { return st, nil })
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	var report candidateReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, out.String())
	}
	if report.Summary.RowCount != 2 {
		t.Fatalf("summary.RowCount=%d want 2", report.Summary.RowCount)
	}
	if report.Summary.FilteredOutByTimingCount != 0 {
		t.Fatalf("filtered_out_by_timing_count=%d want 0", report.Summary.FilteredOutByTimingCount)
	}
	// Upcoming must rank above past despite identical readiness scores.
	if report.Rows[0].CanonicalEventID != "upcoming-1" {
		t.Fatalf("rank 1=%q want upcoming-1", report.Rows[0].CanonicalEventID)
	}
	if report.Rows[1].CanonicalEventID != "past-1" {
		t.Fatalf("rank 2=%q want past-1", report.Rows[1].CanonicalEventID)
	}
	if report.Rows[1].EventTimingStatus != eventTimingPast {
		t.Fatalf("past row event_timing_status=%q want %q", report.Rows[1].EventTimingStatus, eventTimingPast)
	}
	if report.Rows[1].TimingWarning == "" {
		t.Fatal("past row should carry a timing warning")
	}
	if !strings.Contains(strings.ToLower(report.Rows[1].NextManualAction), "historical validation") {
		t.Fatalf("past row next_manual_action=%q want historical-validation safety message", report.Rows[1].NextManualAction)
	}
	// --include-past must NOT push a commence_after floor down to SQL.
	if st.polymarketOpts.CommenceAfter != nil {
		t.Fatalf("CommenceAfter must remain unset under --include-past, got %v", st.polymarketOpts.CommenceAfter)
	}
}

func TestPolymarketCandidatesUpcomingOnlyExcludesUnknown(t *testing.T) {
	t.Setenv("CANDIDATE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	aliasID := int64(7)
	rows := []model.PolymarketComparisonReadinessRow{
		{
			CanonicalEventID:      "upcoming-1",
			HomeTeam:              "Home",
			AwayTeam:              "Away",
			CommenceTime:          now.Add(36 * time.Hour),
			PolymarketAliasID:     &aliasID,
			PolymarketMarketID:    "0xmarket",
			PolymarketConditionID: "0xcond",
			PolymarketTokenCount:  2,
		},
		{
			// CommenceTime intentionally zero -> classified as "unknown".
			CanonicalEventID:      "unknown-1",
			HomeTeam:              "Mystery Home",
			AwayTeam:              "Mystery Away",
			PolymarketAliasID:     &aliasID,
			PolymarketMarketID:    "0xmarket-unknown",
			PolymarketConditionID: "0xcond-unknown",
			PolymarketTokenCount:  2,
		},
	}
	st := &fakeCoverageAuditStore{polymarketRows: rows}

	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
		"--audit", "polymarket",
		"--candidates",
		"--upcoming-only",
		"--now", "2026-05-09T12:00:00Z",
		"--output", "json",
	}, &out, func(ctx context.Context, _ string) (coverageAuditStore, error) { return st, nil })
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	var report candidateReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, out.String())
	}
	if report.Summary.RowCount != 1 {
		t.Fatalf("summary.RowCount=%d want 1 (unknown should be excluded under --upcoming-only)", report.Summary.RowCount)
	}
	if report.Rows[0].CanonicalEventID != "upcoming-1" {
		t.Fatalf("kept event=%q want upcoming-1", report.Rows[0].CanonicalEventID)
	}
	// Under --upcoming-only the SQL floor pushes commence_after = now (not now - max_started).
	if st.polymarketOpts.CommenceAfter == nil || !st.polymarketOpts.CommenceAfter.Equal(now) {
		t.Fatalf("CommenceAfter=%v want %v", st.polymarketOpts.CommenceAfter, now)
	}
}

func TestPolymarketCandidatesFromTimeAndToTimeFilters(t *testing.T) {
	t.Setenv("CANDIDATE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	st := &fakeCoverageAuditStore{}
	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
		"--audit", "polymarket",
		"--candidates",
		"--from-time", "2026-05-10T00:00:00Z",
		"--to-time", "2026-05-12T00:00:00Z",
		"--output", "json",
	}, &out, func(ctx context.Context, _ string) (coverageAuditStore, error) { return st, nil })
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if st.polymarketOpts.CommenceAfter == nil ||
		!st.polymarketOpts.CommenceAfter.Equal(time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("CommenceAfter=%v want 2026-05-10", st.polymarketOpts.CommenceAfter)
	}
	if st.polymarketOpts.CommenceBefore == nil ||
		!st.polymarketOpts.CommenceBefore.Equal(time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("CommenceBefore=%v want 2026-05-12", st.polymarketOpts.CommenceBefore)
	}
}

func TestPolymarketCandidatesDeterministicNow(t *testing.T) {
	t.Setenv("CANDIDATE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	aliasID := int64(7)
	st := &fakeCoverageAuditStore{
		polymarketRows: []model.PolymarketComparisonReadinessRow{{
			CanonicalEventID:      "evt-1",
			CommenceTime:          now.Add(2*time.Hour - time.Minute),
			PolymarketAliasID:     &aliasID,
			PolymarketMarketID:    "0xmarket",
			PolymarketConditionID: "0xcond",
		}},
	}
	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
		"--audit", "polymarket",
		"--candidates",
		"--now", "2026-05-09T12:00:00Z",
		"--output", "json",
	}, &out, func(ctx context.Context, _ string) (coverageAuditStore, error) { return st, nil })
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	var report candidateReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, out.String())
	}
	if report.Filters.Now != "2026-05-09T12:00:00Z" {
		t.Fatalf("filters.now=%q want 2026-05-09T12:00:00Z", report.Filters.Now)
	}
	row := report.Rows[0]
	if row.EventTimingStatus != eventTimingStartingSoon {
		t.Fatalf("event_timing_status=%q want %q", row.EventTimingStatus, eventTimingStartingSoon)
	}
	if row.TimeToStartSeconds == nil || *row.TimeToStartSeconds <= 0 {
		t.Fatalf("time_to_start_seconds=%v want positive", row.TimeToStartSeconds)
	}
}

func TestPolymarketCandidatesTableIncludesTimingColumns(t *testing.T) {
	t.Setenv("CANDIDATE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	aliasID := int64(7)
	st := &fakeCoverageAuditStore{
		polymarketRows: []model.PolymarketComparisonReadinessRow{
			{
				CanonicalEventID:      "upcoming-1",
				HomeTeam:              "Home",
				AwayTeam:              "Away",
				CommenceTime:          now.Add(48 * time.Hour),
				PolymarketAliasID:     &aliasID,
				PolymarketMarketID:    "0xmarket",
				PolymarketConditionID: "0xcond",
				PolymarketTokenCount:  2,
			},
			{
				CanonicalEventID:      "past-1",
				HomeTeam:              "Past",
				AwayTeam:              "Done",
				CommenceTime:          now.Add(-72 * time.Hour),
				PolymarketAliasID:     &aliasID,
				PolymarketMarketID:    "0xpast",
				PolymarketConditionID: "0xpast",
				PolymarketTokenCount:  2,
			},
		},
	}
	var out bytes.Buffer
	err := run(context.Background(), []string{
		"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
		"--audit", "polymarket",
		"--candidates",
		"--include-past",
		"--now", "2026-05-09T12:00:00Z",
		"--output", "table",
	}, &out, func(ctx context.Context, _ string) (coverageAuditStore, error) { return st, nil })
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	output := out.String()
	for _, want := range []string{
		"event_timing_status",
		"time_to_start_seconds",
		"timing_warning",
		eventTimingUpcoming,
		eventTimingPast,
		"historical validation",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("table output missing %q:\n%s", want, output)
		}
	}
}

func TestPolymarketCandidatesPastEventNextActionIsSafe(t *testing.T) {
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	aliasID := int64(7)
	rows := []model.PolymarketComparisonReadinessRow{{
		CanonicalEventID:      "past-1",
		CommenceTime:          now.Add(-72 * time.Hour),
		PolymarketAliasID:     &aliasID,
		PolymarketMarketID:    "0xmarket",
		PolymarketConditionID: "0xcond",
		PolymarketTokenCount:  2,
	}}
	scored := scoreCandidates(rows, now)
	if len(scored) != 1 {
		t.Fatalf("scored count=%d want 1", len(scored))
	}
	row := scored[0]
	if row.EventTimingStatus != eventTimingPast {
		t.Fatalf("event_timing_status=%q want %q", row.EventTimingStatus, eventTimingPast)
	}
	if !strings.Contains(strings.ToLower(row.NextManualAction), "historical validation") {
		t.Fatalf("past next_manual_action=%q want historical validation phrasing", row.NextManualAction)
	}
	for _, forbidden := range []string{
		"manually map alias",
		"manually map and confirm",
		"trading signal",
		"buy signal",
		"sell signal",
		"execute trade",
		"order placement",
		"private key",
		"wallet",
		"automatic execution",
		"recommended trade",
	} {
		if strings.Contains(strings.ToLower(row.NextManualAction), forbidden) {
			t.Fatalf("past next_manual_action contains forbidden phrase %q: %s", forbidden, row.NextManualAction)
		}
	}
}

func TestPolymarketCandidatesValidationRejectsConflictingTimingFlags(t *testing.T) {
	t.Setenv("CANDIDATE_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "upcoming-only and include-past mutually exclusive",
			args: []string{
				"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
				"--audit", "polymarket",
				"--candidates",
				"--upcoming-only",
				"--include-past",
			},
			want: "mutually exclusive",
		},
		{
			name: "from-time after to-time rejected",
			args: []string{
				"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
				"--audit", "polymarket",
				"--candidates",
				"--from-time", "2026-05-12T00:00:00Z",
				"--to-time", "2026-05-10T00:00:00Z",
			},
			want: "must be strictly before",
		},
		{
			name: "timing flags require candidates mode",
			args: []string{
				"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
				"--audit", "polymarket",
				"--recent",
				"--upcoming-only",
			},
			want: "require --audit polymarket --candidates",
		},
		{
			name: "invalid now rejected",
			args: []string{
				"--database-url-env", "CANDIDATE_TEST_DATABASE_URL",
				"--audit", "polymarket",
				"--candidates",
				"--now", "not-a-time",
			},
			want: "--now must be RFC3339 UTC",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := run(context.Background(), tc.args, &bytes.Buffer{}, func(ctx context.Context, _ string) (coverageAuditStore, error) {
				t.Fatal("store factory should not be called for invalid config")
				return nil, nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want contains %q", err, tc.want)
			}
		})
	}
}

func TestPolymarketCandidatesNoForbiddenTerminologyAcrossTimingStatuses(t *testing.T) {
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	rows := []model.PolymarketComparisonReadinessRow{
		{CanonicalEventID: "upcoming", CommenceTime: now.Add(48 * time.Hour)},
		{CanonicalEventID: "soon", CommenceTime: now.Add(30 * time.Minute)},
		{CanonicalEventID: "in-play", CommenceTime: now.Add(-30 * time.Minute)},
		{CanonicalEventID: "past", CommenceTime: now.Add(-72 * time.Hour)},
		{CanonicalEventID: "unknown"}, // zero commence
	}
	scored := scoreCandidates(rows, now)
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
		text := strings.ToLower(row.PrimaryGapReason + " | " + row.NextManualAction + " | " + row.TimingWarning)
		for _, term := range forbidden {
			if strings.Contains(text, term) {
				t.Fatalf("row %d (%s) contains forbidden terminology %q: %s", i, row.CanonicalEventID, term, text)
			}
		}
	}
}
