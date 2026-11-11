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
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const (
	outputJSON  = "json"
	outputTable = "table"

	auditBetfair    = "betfair"
	auditPolymarket = "polymarket"
)

type coverageAuditConfig struct {
	DatabaseURLEnv     string
	DatabaseURL        string
	Audit              string
	CanonicalEventID   string
	BetfairMarketID    string
	PolymarketEventID  string
	PolymarketMarketID string
	SportKey           string
	Candidates         bool
	UpcomingOnly       bool
	IncludePast        bool
	FromTime           *time.Time
	ToTime             *time.Time
	Now                time.Time
	Recent             bool
	Limit              int
	Output             string
}

type coverageAuditStore interface {
	ListBetfairSportsbookCoverageAudit(context.Context, model.BetfairSportsbookCoverageAuditOptions) ([]model.BetfairSportsbookCoverageAuditRow, error)
	ListPolymarketComparisonReadiness(context.Context, model.PolymarketComparisonReadinessOptions) ([]model.PolymarketComparisonReadinessRow, error)
	Close() error
}

type coverageAuditStoreFactory func(context.Context, string) (coverageAuditStore, error)

type coverageAuditReport struct {
	Filters coverageAuditFilters                      `json:"filters"`
	Summary coverageAuditSummary                      `json:"summary"`
	Rows    []model.BetfairSportsbookCoverageAuditRow `json:"rows"`
}

type coverageAuditFilters struct {
	CanonicalEventID string `json:"canonical_event_id,omitempty"`
	BetfairMarketID  string `json:"betfair_market_id,omitempty"`
	Recent           bool   `json:"recent,omitempty"`
	Limit            int    `json:"limit"`
}

type coverageAuditSummary struct {
	RowCount                int            `json:"row_count"`
	BetfairMappedRows       int            `json:"betfair_mapped_rows"`
	BetfairReadyRows        int            `json:"betfair_ready_rows"`
	SportsbookConsensusRows int            `json:"sportsbook_consensus_rows"`
	ComparisonReadyRows     int            `json:"comparison_ready_rows"`
	ReasonCounts            map[string]int `json:"reason_counts"`
}

type polymarketReadinessReport struct {
	Filters polymarketReadinessFilters               `json:"filters"`
	Summary polymarketReadinessSummary               `json:"summary"`
	Rows    []model.PolymarketComparisonReadinessRow `json:"rows"`
}

type polymarketReadinessFilters struct {
	CanonicalEventID   string `json:"canonical_event_id,omitempty"`
	BetfairMarketID    string `json:"betfair_market_id,omitempty"`
	PolymarketEventID  string `json:"polymarket_event_id,omitempty"`
	PolymarketMarketID string `json:"polymarket_market_id,omitempty"`
	Recent             bool   `json:"recent,omitempty"`
	Limit              int    `json:"limit"`
}

type polymarketReadinessSummary struct {
	RowCount                     int            `json:"row_count"`
	SportsbookConsensusRows      int            `json:"sportsbook_consensus_rows"`
	BetfairReadyRows             int            `json:"betfair_ready_rows"`
	ConfirmedPolymarketAliasRows int            `json:"confirmed_polymarket_alias_rows"`
	PolymarketTokenRows          int            `json:"polymarket_token_rows"`
	BestBidAndAskRows            int            `json:"best_bid_and_ask_rows"`
	GrossComparisonPossibleRows  int            `json:"gross_comparison_possible_rows"`
	ComparisonReadyRows          int            `json:"comparison_ready_rows"`
	ResolutionVerifiedRows       int            `json:"resolution_verified_rows"`
	ReasonCounts                 map[string]int `json:"reason_counts"`
	StaleStateCounts             map[string]int `json:"stale_state_counts"`
	ResolutionStatusCounts       map[string]int `json:"resolution_status_counts"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, defaultCoverageAuditStoreFactory); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintf(os.Stderr, "coverage audit failed: %s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer, factory coverageAuditStoreFactory) error {
	cfg, err := parseConfig(args, out)
	if err != nil {
		return err
	}

	st, err := factory(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open postgres using --database-url-env %s: %s", cfg.DatabaseURLEnv, redactKnownValue(err.Error(), cfg.DatabaseURL))
	}
	defer func() { _ = st.Close() }()

	switch cfg.Audit {
	case auditPolymarket:
		if cfg.Candidates {
			return runPolymarketCandidates(ctx, st, cfg, out)
		}
		return runPolymarketAudit(ctx, st, cfg, out)
	default:
		return runBetfairAudit(ctx, st, cfg, out)
	}
}

func runBetfairAudit(ctx context.Context, st coverageAuditStore, cfg coverageAuditConfig, out io.Writer) error {
	rows, err := st.ListBetfairSportsbookCoverageAudit(ctx, model.BetfairSportsbookCoverageAuditOptions{
		CanonicalEventID: cfg.CanonicalEventID,
		BetfairMarketID:  cfg.BetfairMarketID,
		Recent:           cfg.Recent,
		Limit:            cfg.Limit,
	})
	if err != nil {
		return fmt.Errorf("read sportsbook/betfair coverage audit: %w", err)
	}

	rows = decorateCoverageRows(rows)
	report := coverageAuditReport{
		Filters: coverageAuditFilters{
			CanonicalEventID: cfg.CanonicalEventID,
			BetfairMarketID:  cfg.BetfairMarketID,
			Recent:           cfg.Recent,
			Limit:            cfg.Limit,
		},
		Summary: summarizeCoverageRows(rows),
		Rows:    rows,
	}
	return writeReport(out, cfg.Output, report)
}

func runPolymarketAudit(ctx context.Context, st coverageAuditStore, cfg coverageAuditConfig, out io.Writer) error {
	rows, err := st.ListPolymarketComparisonReadiness(ctx, model.PolymarketComparisonReadinessOptions{
		CanonicalEventID:   cfg.CanonicalEventID,
		BetfairMarketID:    cfg.BetfairMarketID,
		PolymarketEventID:  cfg.PolymarketEventID,
		PolymarketMarketID: cfg.PolymarketMarketID,
		Recent:             cfg.Recent,
		Limit:              cfg.Limit,
	})
	if err != nil {
		return fmt.Errorf("read polymarket comparison readiness: %w", err)
	}

	rows = decoratePolymarketRows(rows)
	report := polymarketReadinessReport{
		Filters: polymarketReadinessFilters{
			CanonicalEventID:   cfg.CanonicalEventID,
			BetfairMarketID:    cfg.BetfairMarketID,
			PolymarketEventID:  cfg.PolymarketEventID,
			PolymarketMarketID: cfg.PolymarketMarketID,
			Recent:             cfg.Recent,
			Limit:              cfg.Limit,
		},
		Summary: summarizePolymarketRows(rows),
		Rows:    rows,
	}
	return writePolymarketReport(out, cfg.Output, report)
}

func defaultCoverageAuditStoreFactory(ctx context.Context, databaseURL string) (coverageAuditStore, error) {
	return store.Open(ctx, databaseURL, false, normalize.FreshnessPolicy{})
}

func parseConfig(args []string, out io.Writer) (coverageAuditConfig, error) {
	cfg := coverageAuditConfig{Output: outputTable, Limit: 25, Audit: auditBetfair}
	fs := flag.NewFlagSet("coverage-audit", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&cfg.DatabaseURLEnv, "database-url-env", "", "environment variable name containing the PostgreSQL URL")
	fs.StringVar(&cfg.Audit, "audit", cfg.Audit, "audit mode: betfair (sportsbook/Betfair coverage) or polymarket (canonical-vs-Polymarket comparison readiness)")
	fs.StringVar(&cfg.CanonicalEventID, "canonical-event-id", "", "canonical event UUID to audit")
	fs.StringVar(&cfg.BetfairMarketID, "betfair-market-id", "", "Betfair market ID to audit")
	fs.StringVar(&cfg.PolymarketEventID, "polymarket-event-id", "", "Polymarket event ID to audit (polymarket mode only)")
	fs.StringVar(&cfg.PolymarketMarketID, "polymarket-market-id", "", "Polymarket market ID to audit (polymarket mode only)")
	fs.StringVar(&cfg.SportKey, "sport", "", "sport_key to filter on (e.g. soccer); polymarket candidates mode only")
	fs.BoolVar(&cfg.Candidates, "candidates", false, "rank canonical events by Polymarket comparison readiness (polymarket mode only)")
	fs.BoolVar(&cfg.UpcomingOnly, "upcoming-only", false, "candidates mode: only events that have not started (excludes in_play_or_started, past, and unknown timing)")
	fs.BoolVar(&cfg.IncludePast, "include-past", false, "candidates mode: include in_play_or_started and past events; intended for historical validation only")
	fromTimeFlag := fs.String("from-time", "", "candidates mode: lower bound on commence_time (RFC3339 UTC, e.g. 2026-05-09T00:00:00Z)")
	toTimeFlag := fs.String("to-time", "", "candidates mode: upper bound on commence_time (RFC3339 UTC, exclusive)")
	nowFlag := fs.String("now", "", "override the current time for deterministic candidate audits (RFC3339 UTC); defaults to real wall clock")
	fs.BoolVar(&cfg.Recent, "recent", false, "show recent coverage rows")
	fs.IntVar(&cfg.Limit, "limit", cfg.Limit, "maximum rows to return")
	fs.StringVar(&cfg.Output, "output", cfg.Output, "output format: table or json")
	if err := fs.Parse(args); err != nil {
		return coverageAuditConfig{}, err
	}

	cfg.DatabaseURLEnv = strings.TrimSpace(cfg.DatabaseURLEnv)
	cfg.Audit = strings.ToLower(strings.TrimSpace(cfg.Audit))
	cfg.CanonicalEventID = strings.TrimSpace(cfg.CanonicalEventID)
	cfg.BetfairMarketID = strings.TrimSpace(cfg.BetfairMarketID)
	cfg.PolymarketEventID = strings.TrimSpace(cfg.PolymarketEventID)
	cfg.PolymarketMarketID = strings.TrimSpace(cfg.PolymarketMarketID)
	cfg.SportKey = strings.ToLower(strings.TrimSpace(cfg.SportKey))
	cfg.Output = strings.ToLower(strings.TrimSpace(cfg.Output))

	if cfg.Audit != auditBetfair && cfg.Audit != auditPolymarket {
		return coverageAuditConfig{}, fmt.Errorf("audit must be %q or %q", auditBetfair, auditPolymarket)
	}
	if cfg.Output != outputTable && cfg.Output != outputJSON {
		return coverageAuditConfig{}, fmt.Errorf("output must be %q or %q", outputTable, outputJSON)
	}
	if cfg.DatabaseURLEnv == "" {
		return coverageAuditConfig{}, errors.New("database-url-env is required")
	}
	if cfg.Audit == auditBetfair && (cfg.PolymarketEventID != "" || cfg.PolymarketMarketID != "") {
		return coverageAuditConfig{}, errors.New("--polymarket-event-id and --polymarket-market-id require --audit polymarket")
	}
	if cfg.Audit == auditBetfair && (cfg.Candidates || cfg.SportKey != "") {
		return coverageAuditConfig{}, errors.New("--candidates and --sport require --audit polymarket")
	}
	timingFlagsUsed := cfg.UpcomingOnly || cfg.IncludePast ||
		strings.TrimSpace(*fromTimeFlag) != "" || strings.TrimSpace(*toTimeFlag) != "" ||
		strings.TrimSpace(*nowFlag) != ""
	if timingFlagsUsed && (cfg.Audit != auditPolymarket || !cfg.Candidates) {
		return coverageAuditConfig{}, errors.New("--upcoming-only, --include-past, --from-time, --to-time, and --now require --audit polymarket --candidates")
	}
	if cfg.UpcomingOnly && cfg.IncludePast {
		return coverageAuditConfig{}, errors.New("--upcoming-only and --include-past are mutually exclusive")
	}
	if v := strings.TrimSpace(*fromTimeFlag); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return coverageAuditConfig{}, fmt.Errorf("--from-time must be RFC3339 UTC (e.g. 2026-05-09T00:00:00Z): %s", err)
		}
		t = t.UTC()
		cfg.FromTime = &t
	}
	if v := strings.TrimSpace(*toTimeFlag); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return coverageAuditConfig{}, fmt.Errorf("--to-time must be RFC3339 UTC: %s", err)
		}
		t = t.UTC()
		cfg.ToTime = &t
	}
	if cfg.FromTime != nil && cfg.ToTime != nil && !cfg.FromTime.Before(*cfg.ToTime) {
		return coverageAuditConfig{}, errors.New("--from-time must be strictly before --to-time")
	}
	if v := strings.TrimSpace(*nowFlag); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return coverageAuditConfig{}, fmt.Errorf("--now must be RFC3339 UTC: %s", err)
		}
		cfg.Now = t.UTC()
	}
	hasFilter := cfg.CanonicalEventID != "" || cfg.BetfairMarketID != "" || cfg.Recent
	if cfg.Audit == auditPolymarket {
		hasFilter = hasFilter || cfg.PolymarketEventID != "" || cfg.PolymarketMarketID != "" || cfg.Candidates || cfg.SportKey != ""
	}
	if !hasFilter {
		if cfg.Audit == auditPolymarket {
			return coverageAuditConfig{}, errors.New("one of --canonical-event-id, --betfair-market-id, --polymarket-event-id, --polymarket-market-id, --candidates, --sport, or --recent is required")
		}
		return coverageAuditConfig{}, errors.New("one of --canonical-event-id, --betfair-market-id, or --recent is required")
	}
	if cfg.Limit <= 0 {
		return coverageAuditConfig{}, errors.New("limit must be positive")
	}

	databaseURL, ok := os.LookupEnv(cfg.DatabaseURLEnv)
	if !ok || strings.TrimSpace(databaseURL) == "" {
		return coverageAuditConfig{}, fmt.Errorf("environment variable named by --database-url-env (%s) is not set or empty", cfg.DatabaseURLEnv)
	}
	cfg.DatabaseURL = strings.TrimSpace(databaseURL)
	return cfg, nil
}

func writeReport(out io.Writer, output string, report coverageAuditReport) error {
	if output == outputJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	return writeTableReport(out, report)
}

func writeTableReport(out io.Writer, report coverageAuditReport) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "canonical_event_id\tbetfair_market_id\tteams\tcommence_time\tmapped\tready_books\tsportsbook_consensus\tcomparison_ready\tmapped_quotes\tready_book_count\tsportsbook_outcomes\tcomparison_rows\treason\treadiness_states"); err != nil {
		return err
	}
	for _, row := range report.Rows {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%t\t%t\t%t\t%t\t%d\t%d\t%d\t%d\t%s\t%s\n",
			row.CanonicalEventID,
			dash(row.BetfairMarketID),
			row.HomeTeam+" vs "+row.AwayTeam,
			formatTime(row.CommenceTime),
			row.BetfairMapped,
			row.BetfairReadyRunnerBooks,
			row.SportsbookConsensus,
			row.ComparisonReady,
			row.MappedBetfairQuoteCount,
			row.ReadyBetfairRunnerBookCount,
			row.SportsbookConsensusOutcomeCount,
			row.ComparisonRowCount,
			row.GapReason,
			dash(strings.Join(row.ReadinessStates, ",")),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writePolymarketReport(out io.Writer, output string, report polymarketReadinessReport) error {
	if output == outputJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	return writePolymarketTableReport(out, report)
}

func writePolymarketTableReport(out io.Writer, report polymarketReadinessReport) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "canonical_event_id\tteams\tcommence_time\tpolymarket_market_id\talias\tsportsbook\tbetfair_ready\ttokens\tbid\task\tdepth\tstale_state\tresolution\tcomparison_ready\tgap_reason\tnext_manual_action"); err != nil {
		return err
	}
	for _, row := range report.Rows {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%t\t%t\t%t\t%d\t%t\t%t\t%t\t%s\t%s\t%t\t%s\t%s\n",
			row.CanonicalEventID,
			row.HomeTeam+" vs "+row.AwayTeam,
			formatTime(row.CommenceTime),
			dash(row.PolymarketMarketID),
			row.HasConfirmedPolymarketAlias,
			row.HasSportsbookConsensus,
			row.HasReadyBetfair,
			row.PolymarketTokenCount,
			row.HasBestBid,
			row.HasBestAsk,
			row.HasDepthOrLiquidity,
			dash(row.PolymarketStaleState),
			dash(row.ResolutionStatus),
			row.ComparisonReady,
			row.GapReason,
			row.NextManualAction,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func decorateCoverageRows(rows []model.BetfairSportsbookCoverageAuditRow) []model.BetfairSportsbookCoverageAuditRow {
	out := make([]model.BetfairSportsbookCoverageAuditRow, len(rows))
	for i, row := range rows {
		row.BetfairMapped = row.MappedBetfairQuoteCount > 0
		row.BetfairReadyRunnerBooks = row.ReadyBetfairRunnerBookCount > 0
		row.SportsbookConsensus = row.SportsbookConsensusOutcomeCount > 0
		row.ComparisonReady = row.ReadyComparisonRowCount > 0
		row.GapReason = classifyCoverage(row)
		out[i] = row
	}
	return out
}

func summarizeCoverageRows(rows []model.BetfairSportsbookCoverageAuditRow) coverageAuditSummary {
	summary := coverageAuditSummary{RowCount: len(rows), ReasonCounts: map[string]int{}}
	for _, row := range rows {
		if row.BetfairMapped {
			summary.BetfairMappedRows++
		}
		if row.BetfairReadyRunnerBooks {
			summary.BetfairReadyRows++
		}
		if row.SportsbookConsensus {
			summary.SportsbookConsensusRows++
		}
		if row.ComparisonReady {
			summary.ComparisonReadyRows++
		}
		summary.ReasonCounts[row.GapReason]++
	}
	return summary
}

func decoratePolymarketRows(rows []model.PolymarketComparisonReadinessRow) []model.PolymarketComparisonReadinessRow {
	out := make([]model.PolymarketComparisonReadinessRow, len(rows))
	for i, row := range rows {
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
		row.GrossComparisonPossible = row.HasSportsbookConsensus &&
			row.HasConfirmedPolymarketAlias &&
			row.HasPolymarketTokens &&
			row.HasBestAsk
		row.NetComparisonPossible = false
		row.GapReason = classifyPolymarketReadiness(row)
		row.NextManualAction = polymarketNextAction(row)
		row.ComparisonReady = row.GapReason == comparisonReadyState
		out[i] = row
	}
	return out
}

func summarizePolymarketRows(rows []model.PolymarketComparisonReadinessRow) polymarketReadinessSummary {
	summary := polymarketReadinessSummary{
		RowCount:               len(rows),
		ReasonCounts:           map[string]int{},
		StaleStateCounts:       map[string]int{},
		ResolutionStatusCounts: map[string]int{},
	}
	for _, row := range rows {
		if row.HasSportsbookConsensus {
			summary.SportsbookConsensusRows++
		}
		if row.HasReadyBetfair {
			summary.BetfairReadyRows++
		}
		if row.HasConfirmedPolymarketAlias {
			summary.ConfirmedPolymarketAliasRows++
		}
		if row.HasPolymarketTokens {
			summary.PolymarketTokenRows++
		}
		if row.HasBestBidAndAsk {
			summary.BestBidAndAskRows++
		}
		if row.GrossComparisonPossible {
			summary.GrossComparisonPossibleRows++
		}
		if row.ComparisonReady {
			summary.ComparisonReadyRows++
		}
		if row.ResolutionVerified {
			summary.ResolutionVerifiedRows++
		}
		summary.ReasonCounts[row.GapReason]++
		if row.PolymarketStaleState != "" {
			summary.StaleStateCounts[row.PolymarketStaleState]++
		}
		if row.ResolutionStatus != "" {
			summary.ResolutionStatusCounts[row.ResolutionStatus]++
		}
	}
	return summary
}

const (
	externalFairValueMissingState   = "external_fair_value_missing"
	sportsbookConsensusMissingState = "sportsbook_consensus_missing"
	betfairNotReadyState            = "betfair_not_ready"
	polymarketMappingMissingState   = "polymarket_mapping_missing"
	polymarketMetadataMissingState  = "polymarket_metadata_missing"
	polymarketTokenMissingState     = "polymarket_token_missing"
	polymarketPriceMissingState     = "polymarket_price_missing"
	polymarketPriceStaleState       = "polymarket_price_stale"
	polymarketLiquidityLowState     = "polymarket_liquidity_low"
	resolutionUnverifiedState       = "resolution_unverified"
	netEdgeModelMissingState        = "net_edge_model_missing"
	comparisonReadyState            = "comparison_ready"
)

const (
	polymarketStaleAfter   = 15 * time.Minute
	polymarketAgingAfter   = 5 * time.Minute
	polymarketMinLiquidity = 1.0
)

func classifyCoverage(row model.BetfairSportsbookCoverageAuditRow) string {
	switch {
	case row.MappedBetfairQuoteCount == 0:
		return "mapping_missing"
	case row.CanonicalMarketFamily != "h2h":
		return "unsupported_market_family"
	case row.SportsbookConsensusOutcomeCount == 0:
		return "no_sportsbook_consensus"
	case row.ReadyBetfairRunnerBookCount == 0:
		if hasAnyReadinessState(row.ReadinessStates, "aging", "low_confidence", "stale", "thin_liquidity", "wide_spread") {
			return "stale_or_low_confidence"
		}
		return "betfair_not_ready"
	case row.ComparisonRowCount == 0:
		return "no_matching_outcome"
	case row.ReadyComparisonRowCount == 0:
		return "comparison_missing"
	default:
		return "comparison_ready"
	}
}

func classifyPolymarketReadiness(row model.PolymarketComparisonReadinessRow) string {
	switch {
	case !row.HasSportsbookConsensus && !row.HasReadyBetfair:
		return externalFairValueMissingState
	case !row.HasSportsbookConsensus && row.HasReadyBetfair:
		return sportsbookConsensusMissingState
	case row.HasSportsbookConsensus && !row.HasReadyBetfair && row.BetfairMappedQuoteCount > 0 && !row.HasConfirmedPolymarketAlias:
		// Sportsbook is present and Betfair is mapped but not ready. Surface the
		// Polymarket-side gap so operators see what the next manual step is, while
		// keeping the betfair-not-ready signal in the readiness states.
		return polymarketMappingMissingState
	case !row.HasConfirmedPolymarketAlias:
		return polymarketMappingMissingState
	case !row.HasPolymarketMarketMetadata:
		return polymarketMetadataMissingState
	case !row.HasPolymarketTokens:
		return polymarketTokenMissingState
	case row.PolymarketPriceSnapshotCount == 0 || !row.HasBestBid || !row.HasBestAsk:
		return polymarketPriceMissingState
	case row.PolymarketStaleState == "missing" || row.PolymarketStaleState == "stale":
		return polymarketPriceStaleState
	case !row.HasDepthOrLiquidity || (row.PolymarketMaxLiquidity != nil && *row.PolymarketMaxLiquidity < polymarketMinLiquidity):
		return polymarketLiquidityLowState
	case row.PolymarketMarketArchived != nil && *row.PolymarketMarketArchived:
		return resolutionUnverifiedState
	case row.PolymarketMarketClosed != nil && *row.PolymarketMarketClosed:
		return resolutionUnverifiedState
	case !row.ResolutionVerified && row.ResolutionStatus != "" && row.ResolutionStatus != "open" && row.ResolutionStatus != "unknown":
		return resolutionUnverifiedState
	default:
		return comparisonReadyState
	}
}

func polymarketNextAction(row model.PolymarketComparisonReadinessRow) string {
	switch row.GapReason {
	case externalFairValueMissingState:
		return "add sportsbook moneyline coverage or wait for Betfair to become ready before comparing Polymarket"
	case sportsbookConsensusMissingState:
		return "add sportsbook moneyline coverage for this canonical event"
	case betfairNotReadyState:
		return "review Betfair readiness before treating Polymarket comparison as actionable"
	case polymarketMappingMissingState:
		return "manually map and confirm a Polymarket alias for this canonical event"
	case polymarketMetadataMissingState:
		return "sync Polymarket market metadata for the mapped condition"
	case polymarketTokenMissingState:
		return "sync Polymarket tokens for the mapped market"
	case polymarketPriceMissingState:
		return "run polymarket-sync to capture best bid and best ask snapshots"
	case polymarketPriceStaleState:
		return "refresh Polymarket price snapshots before interpreting comparison"
	case polymarketLiquidityLowState:
		return "inspect Polymarket order book depth before interpreting comparison"
	case resolutionUnverifiedState:
		return "verify settlement and resolution semantics manually before interpreting comparison"
	case comparisonReadyState:
		return "gross comparison inputs are present; net comparison still requires a fee or cost model that is not implemented in this project"
	default:
		return "review the readiness states and consult the operational runbook"
	}
}

func computePolymarketStaleState(latest *time.Time) string {
	if latest == nil {
		return "missing"
	}
	now := time.Now().UTC()
	switch {
	case latest.Before(now.Add(-polymarketStaleAfter)):
		return "stale"
	case latest.Before(now.Add(-polymarketAgingAfter)):
		return "aging"
	default:
		return "fresh"
	}
}

func computeResolutionStatus(row model.PolymarketComparisonReadinessRow) string {
	if row.PolymarketMarketArchived != nil && *row.PolymarketMarketArchived {
		return "archived"
	}
	if row.PolymarketMarketClosed != nil && *row.PolymarketMarketClosed {
		return "closed"
	}
	if row.PolymarketConditionID == "" {
		return "unknown"
	}
	if row.PolymarketMarketActive != nil && !*row.PolymarketMarketActive {
		return "inactive"
	}
	return "open"
}

func hasAnyReadinessState(states []string, targets ...string) bool {
	targetSet := map[string]struct{}{}
	for _, target := range targets {
		targetSet[target] = struct{}{}
	}
	for _, state := range states {
		if _, ok := targetSet[state]; ok {
			return true
		}
	}
	return false
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}

func dash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func redactKnownValue(text, secret string) string {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return text
	}
	return strings.ReplaceAll(text, secret, "<redacted>")
}
