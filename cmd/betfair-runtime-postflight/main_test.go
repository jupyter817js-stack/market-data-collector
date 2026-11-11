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

func TestRuntimePostflightJSONReport(t *testing.T) {
	t.Setenv("POSTFLIGHT_TEST_DATABASE_URL", "postgres://secret-user:secret-pass@localhost/db")
	store := &fakePostflightStore{report: sampleReport()}
	var out bytes.Buffer

	err := run(context.Background(), []string{
		"--database-url-env", "POSTFLIGHT_TEST_DATABASE_URL",
		"--run-id", "7",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (postflightStore, error) {
		if !strings.Contains(databaseURL, "secret-pass") {
			t.Fatalf("database url was not passed from explicit env var")
		}
		return store, nil
	})
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if !store.closed {
		t.Fatal("store was not closed")
	}
	output := out.String()
	if strings.Contains(output, "secret-pass") || strings.Contains(output, "BETFAIR_APP_KEY") || strings.Contains(output, "BETFAIR_SESSION_TOKEN") {
		t.Fatalf("output leaked secret-like value:\n%s", output)
	}

	var decoded model.BetfairRuntimeVerificationReport
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("decode json report: %v\n%s", err, output)
	}
	if decoded.RuntimeRun.RunID != 7 || decoded.RuntimeRun.RunLabel != "phase10k-first-postgres" {
		t.Fatalf("unexpected run summary: %+v", decoded.RuntimeRun)
	}
	if !decoded.Classification.IngestionPass || !decoded.Classification.MappingPass {
		t.Fatalf("expected ingestion/mapping pass: %+v", decoded.Classification)
	}
	if decoded.Classification.ReadinessPass {
		t.Fatalf("readiness should not pass with warning rows: %+v", decoded.Classification)
	}
	if decoded.Classification.ExecutionAllowed {
		t.Fatal("execution_allowed must remain false")
	}
}

func TestRuntimePostflightMarkdownReport(t *testing.T) {
	t.Setenv("POSTFLIGHT_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	var out bytes.Buffer

	err := run(context.Background(), []string{
		"--database-url-env", "POSTFLIGHT_TEST_DATABASE_URL",
		"--run-id", "7",
		"--output", "markdown",
	}, &out, func(ctx context.Context, databaseURL string) (postflightStore, error) {
		return &fakePostflightStore{report: sampleReport()}, nil
	})
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	output := out.String()
	for _, want := range []string{
		"# Betfair Runtime Postflight",
		"## Quote Candidate Summary",
		"## Mapped Quote Summary",
		"## Readiness Summary",
		"## Final Classification",
		"| execution_allowed | false |",
		"stale_quote=3",
		"thin_liquidity=3",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("markdown output missing %q:\n%s", want, output)
		}
	}
}

func TestRuntimePostflightValidation(t *testing.T) {
	t.Setenv("POSTFLIGHT_TEST_DATABASE_URL", "postgres://pol:pol@localhost/db")
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "database env required",
			args: []string{"--run-id", "7", "--output", "json"},
			want: "database-url-env is required",
		},
		{
			name: "positive run id required",
			args: []string{"--database-url-env", "POSTFLIGHT_TEST_DATABASE_URL", "--run-id", "0"},
			want: "run-id must be positive",
		},
		{
			name: "output validation",
			args: []string{"--database-url-env", "POSTFLIGHT_TEST_DATABASE_URL", "--run-id", "7", "--output", "text"},
			want: "output must be",
		},
		{
			name: "env value required",
			args: []string{"--database-url-env", "MISSING_POSTFLIGHT_DATABASE_URL", "--run-id", "7"},
			want: "is not set or empty",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := run(context.Background(), tc.args, &bytes.Buffer{}, func(ctx context.Context, databaseURL string) (postflightStore, error) {
				t.Fatal("store factory should not be called for invalid config")
				return nil, nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want contains %q", err, tc.want)
			}
		})
	}
}

func TestRuntimePostflightStoreOpenErrorRedactsDatabaseURL(t *testing.T) {
	databaseURL := "postgres://secret-user:secret-pass@localhost/db"
	t.Setenv("POSTFLIGHT_TEST_DATABASE_URL", databaseURL)
	err := run(context.Background(), []string{
		"--database-url-env", "POSTFLIGHT_TEST_DATABASE_URL",
		"--run-id", "7",
	}, &bytes.Buffer{}, func(ctx context.Context, databaseURL string) (postflightStore, error) {
		return nil, errors.New("open failed for " + databaseURL)
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "secret-pass") || !strings.Contains(err.Error(), "<redacted>") {
		t.Fatalf("database URL was not redacted: %v", err)
	}
}

func sampleReport() model.BetfairRuntimeVerificationReport {
	startedAt := time.Date(2026, 5, 9, 4, 1, 0, 0, time.UTC)
	stoppedAt := startedAt.Add(30 * time.Second)
	minOdds := 2.1
	maxOdds := 3.4
	minSize := 4.5
	maxSize := 22.0
	return model.BetfairRuntimeVerificationReport{
		RuntimeRun: model.BetfairRuntimeVerificationRunSummary{
			RunID:               7,
			RunLabel:            "phase10k-first-postgres",
			StartedAt:           startedAt,
			StoppedAt:           &stoppedAt,
			Status:              model.BetfairRuntimeRunStatusSuccess,
			SinkType:            "postgres",
			MarketIDs:           []string{"1.257744548"},
			RawMessagesInserted: 53,
			CandidatesInserted:  6,
			SinkErrors:          0,
			FlushErrors:         0,
		},
		QuoteCandidate: model.BetfairRuntimeQuoteCandidateSummary{
			CandidateCount:      6,
			DistinctMarketCount: 1,
			DistinctRunnerCount: 3,
			HasBack:             true,
			HasLay:              true,
			MinOdds:             &minOdds,
			MaxOdds:             &maxOdds,
			MinAvailableSize:    &minSize,
			MaxAvailableSize:    &maxSize,
		},
		MappedQuote: model.BetfairRuntimeMappedQuoteSummary{
			CanonicalEventIDs:       []string{"00000000-0000-0000-0000-000000000001"},
			CanonicalMarketIDs:      []string{"00000000-0000-0000-0000-000000000001:h2h"},
			RunnerAliasStatusCounts: map[string]int64{"confirmed": 3},
			MappedRunnersCount:      3,
			UnmappedRunnersCount:    0,
		},
		Readiness: model.BetfairRuntimeReadinessVerification{
			ReadyRowCount:        0,
			WarningRowCount:      3,
			BlockedRowCount:      0,
			ReadinessStateCounts: map[string]int64{"thin_liquidity": 3},
			AnomalyReasonCounts: map[string]int64{
				"low_confidence": 3,
				"stale_quote":    3,
				"thin_liquidity": 3,
			},
			StaleQuoteCount:    3,
			LowConfidenceCount: 3,
			ThinLiquidityCount: 3,
		},
		Classification: model.BetfairRuntimeVerificationClassification{
			IngestionPass:    true,
			MappingPass:      true,
			ReadinessPass:    false,
			ExecutionAllowed: false,
		},
	}
}

type fakePostflightStore struct {
	report model.BetfairRuntimeVerificationReport
	err    error
	closed bool
}

func (s *fakePostflightStore) GetBetfairRuntimeVerificationReport(context.Context, int64) (model.BetfairRuntimeVerificationReport, error) {
	if s.err != nil {
		return model.BetfairRuntimeVerificationReport{}, s.err
	}
	return s.report, nil
}

func (s *fakePostflightStore) Close() error {
	s.closed = true
	return nil
}
