package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const (
	outputJSON     = "json"
	outputMarkdown = "markdown"
)

type postflightConfig struct {
	DatabaseURLEnv string
	DatabaseURL    string
	RunID          int64
	Output         string
}

type postflightStore interface {
	GetBetfairRuntimeVerificationReport(context.Context, int64) (model.BetfairRuntimeVerificationReport, error)
	Close() error
}

type postflightStoreFactory func(context.Context, string) (postflightStore, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, defaultPostflightStoreFactory); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintf(os.Stderr, "betfair runtime postflight failed: %s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer, factory postflightStoreFactory) error {
	cfg, err := parseConfig(args, out)
	if err != nil {
		return err
	}
	st, err := factory(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open postgres using --database-url-env %s: %s", cfg.DatabaseURLEnv, redactKnownValue(err.Error(), cfg.DatabaseURL))
	}
	defer func() { _ = st.Close() }()

	report, err := st.GetBetfairRuntimeVerificationReport(ctx, cfg.RunID)
	if err != nil {
		return fmt.Errorf("read runtime postflight run_id=%d: %w", cfg.RunID, err)
	}
	return writeReport(out, cfg.Output, report)
}

func defaultPostflightStoreFactory(ctx context.Context, databaseURL string) (postflightStore, error) {
	return store.Open(ctx, databaseURL, false, normalize.FreshnessPolicy{})
}

func parseConfig(args []string, out io.Writer) (postflightConfig, error) {
	cfg := postflightConfig{Output: outputJSON}
	fs := flag.NewFlagSet("betfair-runtime-postflight", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&cfg.DatabaseURLEnv, "database-url-env", "", "environment variable name containing the PostgreSQL URL")
	fs.Int64Var(&cfg.RunID, "run-id", 0, "Betfair runtime run id to summarize")
	fs.StringVar(&cfg.Output, "output", cfg.Output, "output format: json or markdown")
	if err := fs.Parse(args); err != nil {
		return postflightConfig{}, err
	}
	cfg.DatabaseURLEnv = strings.TrimSpace(cfg.DatabaseURLEnv)
	cfg.Output = strings.ToLower(strings.TrimSpace(cfg.Output))
	if cfg.Output != outputJSON && cfg.Output != outputMarkdown {
		return postflightConfig{}, fmt.Errorf("output must be %q or %q", outputJSON, outputMarkdown)
	}
	if cfg.DatabaseURLEnv == "" {
		return postflightConfig{}, fmt.Errorf("database-url-env is required")
	}
	if cfg.RunID <= 0 {
		return postflightConfig{}, fmt.Errorf("run-id must be positive")
	}
	databaseURL, ok := os.LookupEnv(cfg.DatabaseURLEnv)
	if !ok || strings.TrimSpace(databaseURL) == "" {
		return postflightConfig{}, fmt.Errorf("environment variable named by --database-url-env (%s) is not set or empty", cfg.DatabaseURLEnv)
	}
	cfg.DatabaseURL = strings.TrimSpace(databaseURL)
	return cfg, nil
}

func writeReport(out io.Writer, output string, report model.BetfairRuntimeVerificationReport) error {
	if output == outputJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	return writeMarkdownReport(out, report)
}

func writeMarkdownReport(out io.Writer, report model.BetfairRuntimeVerificationReport) error {
	run := report.RuntimeRun
	candidates := report.QuoteCandidate
	mapped := report.MappedQuote
	readiness := report.Readiness
	classification := report.Classification

	_, err := fmt.Fprintf(out, "# Betfair Runtime Postflight\n\n")
	if err != nil {
		return err
	}
	if err := writeMarkdownTable(out, []markdownRow{
		{"run_id", fmt.Sprintf("%d", run.RunID)},
		{"run_label", dash(run.RunLabel)},
		{"started_at", formatTime(run.StartedAt)},
		{"stopped_at", formatTimePtr(run.StoppedAt)},
		{"status", dash(run.Status)},
		{"sink_type", dash(run.SinkType)},
		{"market_ids", strings.Join(run.MarketIDs, ", ")},
		{"raw_messages_inserted", fmt.Sprintf("%d", run.RawMessagesInserted)},
		{"candidates_inserted", fmt.Sprintf("%d", run.CandidatesInserted)},
		{"sink_errors", fmt.Sprintf("%d", run.SinkErrors)},
		{"flush_errors", fmt.Sprintf("%d", run.FlushErrors)},
	}); err != nil {
		return err
	}

	_, _ = fmt.Fprint(out, "\n## Quote Candidate Summary\n\n")
	if err := writeMarkdownTable(out, []markdownRow{
		{"candidate_count", fmt.Sprintf("%d", candidates.CandidateCount)},
		{"distinct_market_count", fmt.Sprintf("%d", candidates.DistinctMarketCount)},
		{"distinct_runner_count", fmt.Sprintf("%d", candidates.DistinctRunnerCount)},
		{"has_back", fmt.Sprintf("%t", candidates.HasBack)},
		{"has_lay", fmt.Sprintf("%t", candidates.HasLay)},
		{"min_odds", formatFloatPtr(candidates.MinOdds)},
		{"max_odds", formatFloatPtr(candidates.MaxOdds)},
		{"min_available_size", formatFloatPtr(candidates.MinAvailableSize)},
		{"max_available_size", formatFloatPtr(candidates.MaxAvailableSize)},
	}); err != nil {
		return err
	}

	_, _ = fmt.Fprint(out, "\n## Mapped Quote Summary\n\n")
	if err := writeMarkdownTable(out, []markdownRow{
		{"canonical_event_ids", strings.Join(mapped.CanonicalEventIDs, ", ")},
		{"canonical_market_ids", strings.Join(mapped.CanonicalMarketIDs, ", ")},
		{"runner_alias_status_counts", formatCountMap(mapped.RunnerAliasStatusCounts)},
		{"mapped_runners_count", fmt.Sprintf("%d", mapped.MappedRunnersCount)},
		{"unmapped_runners_count", fmt.Sprintf("%d", mapped.UnmappedRunnersCount)},
	}); err != nil {
		return err
	}

	_, _ = fmt.Fprint(out, "\n## Readiness Summary\n\n")
	if err := writeMarkdownTable(out, []markdownRow{
		{"ready_row_count", fmt.Sprintf("%d", readiness.ReadyRowCount)},
		{"warning_row_count", fmt.Sprintf("%d", readiness.WarningRowCount)},
		{"blocked_row_count", fmt.Sprintf("%d", readiness.BlockedRowCount)},
		{"readiness_state_counts", formatCountMap(readiness.ReadinessStateCounts)},
		{"anomaly_reason_counts", formatCountMap(readiness.AnomalyReasonCounts)},
		{"stale_quote_count", fmt.Sprintf("%d", readiness.StaleQuoteCount)},
		{"low_confidence_count", fmt.Sprintf("%d", readiness.LowConfidenceCount)},
		{"thin_liquidity_count", fmt.Sprintf("%d", readiness.ThinLiquidityCount)},
	}); err != nil {
		return err
	}

	_, _ = fmt.Fprint(out, "\n## Final Classification\n\n")
	return writeMarkdownTable(out, []markdownRow{
		{"ingestion_pass", fmt.Sprintf("%t", classification.IngestionPass)},
		{"mapping_pass", fmt.Sprintf("%t", classification.MappingPass)},
		{"readiness_pass", fmt.Sprintf("%t", classification.ReadinessPass)},
		{"execution_allowed", fmt.Sprintf("%t", classification.ExecutionAllowed)},
	})
}

type markdownRow struct {
	Field string
	Value string
}

func writeMarkdownTable(out io.Writer, rows []markdownRow) error {
	if _, err := fmt.Fprintln(out, "| Field | Value |"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, "| --- | --- |"); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(out, "| %s | %s |\n", escapeMarkdownCell(row.Field), escapeMarkdownCell(dash(row.Value))); err != nil {
			return err
		}
	}
	return nil
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}

func formatTimePtr(ts *time.Time) string {
	if ts == nil {
		return ""
	}
	return formatTime(*ts)
}

func formatFloatPtr(value *float64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%.8g", *value)
}

func formatCountMap(values map[string]int64) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, values[key]))
	}
	return strings.Join(parts, ", ")
}

func dash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func escapeMarkdownCell(value string) string {
	value = strings.ReplaceAll(value, "|", "\\|")
	value = strings.ReplaceAll(value, "\n", " ")
	return value
}

func redactKnownValue(text, secret string) string {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return text
	}
	return strings.ReplaceAll(text, secret, "<redacted>")
}
