package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const defaultDatabaseURL = "postgres://pol:pol@localhost:5432/pol_odds?sslmode=disable"

type commandKind string

const (
	commandListEvents           commandKind = "list-events"
	commandListMarkets          commandKind = "list-markets"
	commandListRunners          commandKind = "list-runners"
	commandListConflicts        commandKind = "list-conflicts"
	commandListConfirmedEvents  commandKind = "list-confirmed-events"
	commandListConfirmedMarkets commandKind = "list-confirmed-markets"
	commandListConfirmedRunners commandKind = "list-confirmed-runners"
	commandApproveEvent         commandKind = "approve-event"
	commandApproveMarket        commandKind = "approve-market"
	commandApproveRunner        commandKind = "approve-runner"
	commandRejectEvent          commandKind = "reject-event"
	commandRejectMarket         commandKind = "reject-market"
	commandRejectRunner         commandKind = "reject-runner"
)

type commandConfig struct {
	Kind   commandKind
	List   model.BetfairMapListOptions
	Event  model.BetfairEventAliasDecision
	Market model.BetfairMarketAliasDecision
	Runner model.BetfairRunnerAliasDecision
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		slog.Error("betfair map review failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	cfg, err := parseCommand(args, out)
	if err != nil {
		return err
	}

	st, err := store.Open(ctx, databaseURL(), false, normalize.FreshnessPolicy{})
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer func() { _ = st.Close() }()

	switch cfg.Kind {
	case commandListEvents:
		return listEvents(ctx, st, out, cfg.List)
	case commandListMarkets:
		return listMarkets(ctx, st, out, cfg.List)
	case commandListRunners:
		return listRunners(ctx, st, out, cfg.List)
	case commandListConflicts:
		return listConflicts(ctx, st, out, cfg.List)
	case commandListConfirmedEvents:
		return listConfirmedEvents(ctx, st, out, cfg.List)
	case commandListConfirmedMarkets:
		return listConfirmedMarkets(ctx, st, out, cfg.List)
	case commandListConfirmedRunners:
		return listConfirmedRunners(ctx, st, out, cfg.List)
	case commandApproveEvent:
		if err := st.ApproveBetfairEventAlias(ctx, cfg.Event); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "approved betfair_event_id=%s canonical_event_id=%s\n", cfg.Event.BetfairEventID, cfg.Event.CanonicalEventID)
		return nil
	case commandApproveMarket:
		if err := st.ApproveBetfairMarketAlias(ctx, cfg.Market); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "approved betfair_market_id=%s canonical_event_id=%s canonical_market_family=%s\n", cfg.Market.BetfairMarketID, cfg.Market.CanonicalEventID, cfg.Market.CanonicalMarketFamily)
		return nil
	case commandApproveRunner:
		if err := st.ApproveBetfairRunnerAlias(ctx, cfg.Runner); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "approved betfair_market_id=%s selection_id=%d canonical_outcome_side=%s\n", cfg.Runner.BetfairMarketID, cfg.Runner.SelectionID, cfg.Runner.CanonicalOutcomeSide)
		return nil
	case commandRejectEvent:
		if err := st.RejectBetfairEventAlias(ctx, cfg.Event); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "rejected betfair_event_id=%s\n", cfg.Event.BetfairEventID)
		return nil
	case commandRejectMarket:
		if err := st.RejectBetfairMarketAlias(ctx, cfg.Market); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "rejected betfair_market_id=%s\n", cfg.Market.BetfairMarketID)
		return nil
	case commandRejectRunner:
		if err := st.RejectBetfairRunnerAlias(ctx, cfg.Runner); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "rejected betfair_market_id=%s selection_id=%d\n", cfg.Runner.BetfairMarketID, cfg.Runner.SelectionID)
		return nil
	default:
		return fmt.Errorf("unsupported command %q", cfg.Kind)
	}
}

func parseCommand(args []string, out io.Writer) (commandConfig, error) {
	if len(args) == 0 {
		printUsage(out)
		return commandConfig{}, flag.ErrHelp
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		printUsage(out)
		return commandConfig{}, flag.ErrHelp
	}

	kind := commandKind(args[0])
	switch kind {
	case commandListEvents, commandListMarkets, commandListRunners, commandListConflicts, commandListConfirmedEvents, commandListConfirmedMarkets, commandListConfirmedRunners:
		list, err := parseListOptions(string(kind), args[1:], out)
		if err != nil {
			return commandConfig{}, err
		}
		return commandConfig{Kind: kind, List: list}, nil
	case commandApproveEvent:
		event, err := parseEventDecision(string(kind), args[1:], out, true)
		return commandConfig{Kind: kind, Event: event}, err
	case commandApproveMarket:
		market, err := parseMarketDecision(string(kind), args[1:], out, true)
		return commandConfig{Kind: kind, Market: market}, err
	case commandApproveRunner:
		runner, err := parseRunnerDecision(string(kind), args[1:], out, true)
		return commandConfig{Kind: kind, Runner: runner}, err
	case commandRejectEvent:
		event, err := parseEventDecision(string(kind), args[1:], out, false)
		return commandConfig{Kind: kind, Event: event}, err
	case commandRejectMarket:
		market, err := parseMarketDecision(string(kind), args[1:], out, false)
		return commandConfig{Kind: kind, Market: market}, err
	case commandRejectRunner:
		runner, err := parseRunnerDecision(string(kind), args[1:], out, false)
		return commandConfig{Kind: kind, Runner: runner}, err
	default:
		return commandConfig{}, fmt.Errorf("unknown command %q", args[0])
	}
}

func parseListOptions(name string, args []string, out io.Writer) (model.BetfairMapListOptions, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	opts := model.BetfairMapListOptions{}
	fs.IntVar(&opts.Limit, "limit", 50, "maximum rows to print")
	fs.StringVar(&opts.EventID, "event-id", "", "Betfair event ID filter")
	fs.StringVar(&opts.EventID, "betfair-event-id", "", "alias for --event-id")
	fs.StringVar(&opts.MarketID, "market-id", "", "Betfair market ID filter")
	fs.StringVar(&opts.MarketID, "betfair-market-id", "", "alias for --market-id")
	fs.StringVar(&opts.Status, "status", "", "latest alias status filter")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if opts.Limit <= 0 {
		return opts, errors.New("limit must be positive")
	}
	return opts, nil
}

func parseEventDecision(name string, args []string, out io.Writer, approval bool) (model.BetfairEventAliasDecision, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	decision := model.BetfairEventAliasDecision{}
	confidence := fs.Float64("confidence", -1, "optional confidence score between 0 and 100")
	fs.StringVar(&decision.BetfairEventID, "betfair-event-id", "", "exact Betfair event ID")
	fs.StringVar(&decision.BetfairEventID, "event-id", "", "alias for --betfair-event-id")
	fs.StringVar(&decision.BetfairEventName, "betfair-event-name", "", "optional Betfair event name")
	fs.StringVar(&decision.CanonicalEventID, "canonical-event-id", "", "exact canonical event UUID")
	fs.StringVar(&decision.Reviewer, "reviewer", "", "reviewer name or ID")
	fs.StringVar(&decision.ReviewReason, "reason", "", "reviewer reason")
	fs.StringVar(&decision.MatchReason, "match-reason", "manual_review", "mapping reason label")
	if err := fs.Parse(args); err != nil {
		return decision, err
	}
	if err := setConfidence(&decision.ConfidenceScore, *confidence); err != nil {
		return decision, err
	}
	if approval {
		decision.AliasStatus = "confirmed"
	} else {
		decision.AliasStatus = "rejected"
		decision.CanonicalEventID = ""
	}
	if err := validateEventDecision(decision, approval); err != nil {
		return decision, err
	}
	return decision, nil
}

func parseMarketDecision(name string, args []string, out io.Writer, approval bool) (model.BetfairMarketAliasDecision, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	decision := model.BetfairMarketAliasDecision{}
	confidence := fs.Float64("confidence", -1, "optional confidence score between 0 and 100")
	lineText := fs.String("line", "", "optional market line")
	fs.StringVar(&decision.BetfairMarketID, "betfair-market-id", "", "exact Betfair market ID")
	fs.StringVar(&decision.BetfairMarketID, "market-id", "", "alias for --betfair-market-id")
	fs.StringVar(&decision.BetfairEventID, "betfair-event-id", "", "Betfair event ID")
	fs.StringVar(&decision.BetfairEventID, "event-id", "", "alias for --betfair-event-id")
	fs.StringVar(&decision.CanonicalEventID, "canonical-event-id", "", "exact canonical event UUID")
	fs.StringVar(&decision.BetfairMarketType, "betfair-market-type", "", "optional Betfair market type")
	fs.StringVar(&decision.CanonicalMarketFamily, "canonical-market-family", "", "canonical market family: h2h, spreads, totals, other")
	fs.StringVar(&decision.Reviewer, "reviewer", "", "reviewer name or ID")
	fs.StringVar(&decision.ReviewReason, "reason", "", "reviewer reason")
	fs.StringVar(&decision.MatchReason, "match-reason", "manual_review", "mapping reason label")
	if err := fs.Parse(args); err != nil {
		return decision, err
	}
	if err := setConfidence(&decision.ConfidenceScore, *confidence); err != nil {
		return decision, err
	}
	if strings.TrimSpace(*lineText) != "" {
		line, err := strconv.ParseFloat(strings.TrimSpace(*lineText), 64)
		if err != nil {
			return decision, fmt.Errorf("line must be numeric: %w", err)
		}
		decision.Line = &line
	}
	if approval {
		decision.AliasStatus = "confirmed"
	} else {
		decision.AliasStatus = "rejected"
		decision.CanonicalEventID = ""
		decision.CanonicalMarketFamily = ""
	}
	if err := validateMarketDecision(decision, approval); err != nil {
		return decision, err
	}
	return decision, nil
}

func parseRunnerDecision(name string, args []string, out io.Writer, approval bool) (model.BetfairRunnerAliasDecision, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	decision := model.BetfairRunnerAliasDecision{}
	confidence := fs.Float64("confidence", -1, "optional confidence score between 0 and 100")
	fs.StringVar(&decision.BetfairMarketID, "betfair-market-id", "", "exact Betfair market ID")
	fs.StringVar(&decision.BetfairMarketID, "market-id", "", "alias for --betfair-market-id")
	fs.Int64Var(&decision.SelectionID, "selection-id", 0, "exact Betfair runner selection ID")
	fs.Float64Var(&decision.Handicap, "handicap", 0, "Betfair runner handicap")
	fs.StringVar(&decision.RunnerName, "runner-name", "", "optional Betfair runner name")
	fs.StringVar(&decision.CanonicalOutcomeName, "canonical-outcome-name", "", "canonical outcome name")
	fs.StringVar(&decision.CanonicalOutcomeSide, "canonical-outcome-side", "", "canonical outcome side: home, away, draw, over, under, other")
	fs.StringVar(&decision.Reviewer, "reviewer", "", "reviewer name or ID")
	fs.StringVar(&decision.ReviewReason, "reason", "", "reviewer reason")
	fs.StringVar(&decision.MatchReason, "match-reason", "manual_review", "mapping reason label")
	if err := fs.Parse(args); err != nil {
		return decision, err
	}
	if err := setConfidence(&decision.ConfidenceScore, *confidence); err != nil {
		return decision, err
	}
	if approval {
		decision.AliasStatus = "confirmed"
	} else {
		decision.AliasStatus = "rejected"
		decision.CanonicalOutcomeName = ""
		decision.CanonicalOutcomeSide = ""
	}
	if err := validateRunnerDecision(decision, approval); err != nil {
		return decision, err
	}
	return decision, nil
}

func setConfidence(target **float64, value float64) error {
	if value < 0 {
		return nil
	}
	if value > 100 {
		return errors.New("confidence must be between 0 and 100")
	}
	*target = &value
	return nil
}

func validateEventDecision(decision model.BetfairEventAliasDecision, approval bool) error {
	if strings.TrimSpace(decision.BetfairEventID) == "" {
		return errors.New("betfair event id is required")
	}
	if approval && strings.TrimSpace(decision.CanonicalEventID) == "" {
		return errors.New("canonical event id is required")
	}
	return validateReviewMetadata(decision.Reviewer, decision.ReviewReason)
}

func validateMarketDecision(decision model.BetfairMarketAliasDecision, approval bool) error {
	if strings.TrimSpace(decision.BetfairMarketID) == "" {
		return errors.New("betfair market id is required")
	}
	if approval && strings.TrimSpace(decision.BetfairEventID) == "" {
		return errors.New("betfair event id is required")
	}
	if approval && strings.TrimSpace(decision.CanonicalEventID) == "" {
		return errors.New("canonical event id is required")
	}
	if approval && !validMarketFamily(strings.ToLower(strings.TrimSpace(decision.CanonicalMarketFamily))) {
		return fmt.Errorf("canonical market family %q is invalid", decision.CanonicalMarketFamily)
	}
	return validateReviewMetadata(decision.Reviewer, decision.ReviewReason)
}

func validateRunnerDecision(decision model.BetfairRunnerAliasDecision, approval bool) error {
	if strings.TrimSpace(decision.BetfairMarketID) == "" {
		return errors.New("betfair market id is required")
	}
	if decision.SelectionID == 0 {
		return errors.New("selection id is required")
	}
	if approval && strings.TrimSpace(decision.CanonicalOutcomeName) == "" {
		return errors.New("canonical outcome name is required")
	}
	if approval && !validOutcomeSide(strings.ToLower(strings.TrimSpace(decision.CanonicalOutcomeSide))) {
		return fmt.Errorf("canonical outcome side %q is invalid", decision.CanonicalOutcomeSide)
	}
	return validateReviewMetadata(decision.Reviewer, decision.ReviewReason)
}

func validateReviewMetadata(reviewer, reason string) error {
	if strings.TrimSpace(reviewer) == "" {
		return errors.New("reviewer is required")
	}
	if strings.TrimSpace(reason) == "" {
		return errors.New("review reason is required")
	}
	return nil
}

func validMarketFamily(value string) bool {
	switch value {
	case "h2h", "spreads", "totals", "other":
		return true
	default:
		return false
	}
}

func validOutcomeSide(value string) bool {
	switch value {
	case "home", "away", "draw", "over", "under", "other":
		return true
	default:
		return false
	}
}

func databaseURL() string {
	v := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if v == "" {
		return defaultDatabaseURL
	}
	return v
}

func listEvents(ctx context.Context, st *store.PostgresStore, out io.Writer, opts model.BetfairMapListOptions) error {
	rows, err := st.ListBetfairUnmappedEvents(ctx, opts)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "betfair_event_id\tevent_name\tquote_rows\tmarkets\tlatest_received_at\tlatest_alias_status\tconfidence\tmatch_reason")
	for _, row := range rows {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
			row.BetfairEventID,
			row.BetfairEventName,
			row.QuoteHistoryRows,
			row.MarketCount,
			formatTimePtr(row.LatestReceivedAt),
			row.LatestAliasStatus,
			formatFloat(row.LatestAliasConfidenceScore),
			row.LatestAliasMatchReason,
		)
	}
	return tw.Flush()
}

func listMarkets(ctx context.Context, st *store.PostgresStore, out io.Writer, opts model.BetfairMapListOptions) error {
	rows, err := st.ListBetfairUnmappedMarkets(ctx, opts)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "betfair_market_id\tevent_id\tevent_name\tmarket_name\tmarket_family\tquote_rows\trunners\tlatest_received_at\tlatest_alias_status\talias_family")
	for _, row := range rows {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\n",
			row.BetfairMarketID,
			row.BetfairEventID,
			row.BetfairEventName,
			row.BetfairMarketName,
			row.BetfairMarketFamily,
			row.QuoteHistoryRows,
			row.RunnerCount,
			formatTimePtr(row.LatestReceivedAt),
			row.LatestAliasStatus,
			row.LatestAliasMarketFamily,
		)
	}
	return tw.Flush()
}

func listRunners(ctx context.Context, st *store.PostgresStore, out io.Writer, opts model.BetfairMapListOptions) error {
	rows, err := st.ListBetfairUnmappedRunners(ctx, opts)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "betfair_market_id\tevent_id\tselection_id\thandicap\trunner_name\tquote_rows\tsides\tlatest_received_at\tlatest_alias_status\talias_outcome\talias_side")
	for _, row := range rows {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\t%.4f\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
			row.BetfairMarketID,
			row.BetfairEventID,
			row.SelectionID,
			row.Handicap,
			row.RunnerName,
			row.QuoteHistoryRows,
			row.ExchangeSideCount,
			formatTimePtr(row.LatestReceivedAt),
			row.LatestAliasStatus,
			row.LatestAliasOutcomeName,
			row.LatestAliasOutcomeSide,
		)
	}
	return tw.Flush()
}

func listConflicts(ctx context.Context, st *store.PostgresStore, out io.Writer, opts model.BetfairMapListOptions) error {
	rows, err := st.ListBetfairMappingConflicts(ctx, opts)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "issue_type\tseverity\treason\tbetfair_event_id\tbetfair_market_id\tselection_id\thandicap\tcanonical_event_id\tobserved_at")
	for _, row := range rows {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.IssueType,
			row.Severity,
			row.Reason,
			row.BetfairEventID,
			row.BetfairMarketID,
			formatIntPtr(row.SelectionID),
			formatFloat(row.Handicap),
			row.CanonicalEventID,
			formatTimePtr(row.ObservedAt),
		)
	}
	return tw.Flush()
}

func listConfirmedEvents(ctx context.Context, st *store.PostgresStore, out io.Writer, opts model.BetfairMapListOptions) error {
	rows, err := st.ListConfirmedBetfairEventAliases(ctx, opts)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "alias_id\tbetfair_event_id\tevent_name\tcanonical_event_id\tsport_key\thome_team\taway_team\tcommence_time\tconfidence\treviewer\treason")
	for _, row := range rows {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.AliasID,
			row.BetfairEventID,
			row.BetfairEventName,
			row.CanonicalEventID,
			row.SportKey,
			row.HomeTeam,
			row.AwayTeam,
			row.CommenceTime.UTC().Format(time.RFC3339),
			formatFloat(row.ConfidenceScore),
			row.Reviewer,
			row.ReviewReason,
		)
	}
	return tw.Flush()
}

func listConfirmedMarkets(ctx context.Context, st *store.PostgresStore, out io.Writer, opts model.BetfairMapListOptions) error {
	rows, err := st.ListConfirmedBetfairMarketAliases(ctx, opts)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "alias_id\tbetfair_market_id\tbetfair_event_id\tcanonical_event_id\tmarket_type\tmarket_family\tline\tconfidence\treviewer\treason")
	for _, row := range rows {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.AliasID,
			row.BetfairMarketID,
			row.BetfairEventID,
			row.CanonicalEventID,
			row.BetfairMarketType,
			row.CanonicalMarketFamily,
			formatFloat(row.Line),
			formatFloat(row.ConfidenceScore),
			row.Reviewer,
			row.ReviewReason,
		)
	}
	return tw.Flush()
}

func listConfirmedRunners(ctx context.Context, st *store.PostgresStore, out io.Writer, opts model.BetfairMapListOptions) error {
	rows, err := st.ListConfirmedBetfairRunnerAliases(ctx, opts)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "alias_id\tbetfair_market_id\tselection_id\thandicap\trunner_name\tcanonical_outcome_name\tcanonical_outcome_side\tconfidence\treviewer\treason")
	for _, row := range rows {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%d\t%.4f\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.AliasID,
			row.BetfairMarketID,
			row.SelectionID,
			row.Handicap,
			row.RunnerName,
			row.CanonicalOutcomeName,
			row.CanonicalOutcomeSide,
			formatFloat(row.ConfidenceScore),
			row.Reviewer,
			row.ReviewReason,
		)
	}
	return tw.Flush()
}

func printUsage(out io.Writer) {
	_, _ = fmt.Fprintln(out, `Usage:
  go run ./cmd/betfair-map-review list-events [--limit 50] [--event-id <id>] [--status missing|rejected|ambiguous|unresolved]
  go run ./cmd/betfair-map-review list-markets [--limit 50] [--event-id <id>] [--market-id <id>] [--status missing|rejected|ambiguous|unresolved]
  go run ./cmd/betfair-map-review list-runners [--limit 50] [--event-id <id>] [--market-id <id>] [--status missing|rejected|ambiguous|unresolved]
  go run ./cmd/betfair-map-review list-conflicts [--limit 50] [--event-id <id>] [--market-id <id>]
  go run ./cmd/betfair-map-review list-confirmed-events [--limit 50] [--event-id <id>]
  go run ./cmd/betfair-map-review list-confirmed-markets [--limit 50] [--event-id <id>] [--market-id <id>]
  go run ./cmd/betfair-map-review list-confirmed-runners [--limit 50] [--market-id <id>]

  go run ./cmd/betfair-map-review approve-event --betfair-event-id <id> --canonical-event-id <uuid> --reviewer <name> --reason <text> [--betfair-event-name <name>] [--confidence <score>]
  go run ./cmd/betfair-map-review approve-market --betfair-market-id <id> --betfair-event-id <id> --canonical-event-id <uuid> --canonical-market-family h2h --reviewer <name> --reason <text> [--line <value>] [--betfair-market-type <type>] [--confidence <score>]
  go run ./cmd/betfair-map-review approve-runner --betfair-market-id <id> --selection-id <id> --canonical-outcome-name <name> --canonical-outcome-side home --reviewer <name> --reason <text> [--handicap <value>] [--runner-name <name>] [--confidence <score>]

  go run ./cmd/betfair-map-review reject-event --betfair-event-id <id> --reviewer <name> --reason <text>
  go run ./cmd/betfair-map-review reject-market --betfair-market-id <id> --reviewer <name> --reason <text> [--betfair-event-id <id>]
  go run ./cmd/betfair-map-review reject-runner --betfair-market-id <id> --selection-id <id> --reviewer <name> --reason <text> [--handicap <value>]

Approvals require exact IDs and do not perform fuzzy auto-approval. Mapped Betfair quote rows require confirmed event, market, and runner aliases.`)
}

func formatFloat(v *float64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatFloat(*v, 'f', 2, 64)
}

func formatIntPtr(v *int64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatInt(*v, 10)
}

func formatTimePtr(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
