package main

import (
	"context"
	"encoding/json"
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
	"github.com/your-org/pol-odds-collector/internal/store"
)

const defaultDatabaseURL = "postgres://pol:pol@localhost:5432/pol_odds?sslmode=disable"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		slog.Error("match review failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		printUsage(out)
		return flag.ErrHelp
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		printUsage(out)
		return flag.ErrHelp
	}

	st, err := store.OpenPolymarket(ctx, databaseURL())
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer func() { _ = st.Close() }()

	switch args[0] {
	case "unresolved", "list-unresolved":
		return listReviews(ctx, st, out, "unresolved", args[1:])
	case "ambiguous", "list-ambiguous":
		return listReviews(ctx, st, out, "ambiguous", args[1:])
	case "confirmed", "list-confirmed":
		return listConfirmed(ctx, st, out, args[1:])
	case "approve":
		return approve(ctx, st, out, args[1:])
	case "reject":
		return reject(ctx, st, out, args[1:])
	case "search-polymarket":
		return searchPolymarket(ctx, st, out, args[1:])
	case "approve-polymarket-token":
		return approvePolymarketToken(ctx, st, out, args[1:])
	case "reject-polymarket-token":
		return rejectPolymarketToken(ctx, st, out, args[1:])
	case "list-polymarket-token-aliases":
		return listPolymarketTokenAliases(ctx, st, out, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func databaseURL() string {
	v := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if v == "" {
		return defaultDatabaseURL
	}
	return v
}

func listReviews(ctx context.Context, st *store.PostgresStore, out io.Writer, status string, args []string) error {
	fs := flag.NewFlagSet(status, flag.ContinueOnError)
	fs.SetOutput(out)
	limit := fs.Int("limit", 50, "maximum rows to print")
	if err := fs.Parse(args); err != nil {
		return err
	}

	reviews, err := st.ListPolymarketMatchReviews(ctx, status, *limit)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "review_id\tpolymarket_event_id\tmarket_id\tcondition_id\tstatus\tconfidence\tstart_time\ttitle\tcandidate_canonical_event_ids\tmatch_reason")
	for _, review := range reviews {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			review.ReviewID,
			review.PolymarketEventID,
			review.PolymarketMarketID,
			review.ConditionID,
			review.MatchStatus,
			formatFloat(review.ConfidenceScore),
			formatTimePtr(review.RawStartTime),
			review.Title,
			review.CandidateCanonicalEventIDs,
			review.MatchReason,
		)
	}
	return tw.Flush()
}

func listConfirmed(ctx context.Context, st *store.PostgresStore, out io.Writer, args []string) error {
	fs := flag.NewFlagSet("confirmed", flag.ContinueOnError)
	fs.SetOutput(out)
	limit := fs.Int("limit", 50, "maximum rows to print")
	if err := fs.Parse(args); err != nil {
		return err
	}

	aliases, err := st.ListConfirmedPolymarketAliases(ctx, *limit)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "alias_id\tpolymarket_event_id\tmarket_id\tcondition_id\tcanonical_event_id\tsport_key\thome_team\taway_team\tcommence_time\tconfidence\treviewer\treview_reason\tmatch_reason")
	for _, alias := range aliases {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			alias.AliasID,
			alias.PolymarketEventID,
			alias.PolymarketMarketID,
			alias.ConditionID,
			alias.CanonicalEventID,
			alias.SportKey,
			alias.HomeTeam,
			alias.AwayTeam,
			alias.CommenceTime.UTC().Format(time.RFC3339),
			formatFloat(alias.ConfidenceScore),
			alias.Reviewer,
			alias.ReviewReason,
			alias.MatchReason,
		)
	}
	return tw.Flush()
}

func approve(ctx context.Context, st *store.PostgresStore, out io.Writer, args []string) error {
	fs := flag.NewFlagSet("approve", flag.ContinueOnError)
	fs.SetOutput(out)
	decision := decisionFlags(fs)
	confidence := fs.Float64("confidence", -1, "optional confidence score")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *confidence >= 0 {
		if *confidence > 100 {
			return errors.New("confidence must be between 0 and 100")
		}
		decision.ConfidenceScore = confidence
	}
	if err := st.ApprovePolymarketMatch(ctx, *decision); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "approved polymarket_event_id=%s canonical_event_id=%s\n", decision.PolymarketEventID, decision.CanonicalEventID)
	return nil
}

func reject(ctx context.Context, st *store.PostgresStore, out io.Writer, args []string) error {
	fs := flag.NewFlagSet("reject", flag.ContinueOnError)
	fs.SetOutput(out)
	decision := decisionFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := st.RejectPolymarketMatch(ctx, *decision); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "rejected polymarket_event_id=%s\n", decision.PolymarketEventID)
	return nil
}

func decisionFlags(fs *flag.FlagSet) *model.PolymarketMatchDecision {
	decision := &model.PolymarketMatchDecision{}
	fs.StringVar(&decision.PolymarketEventID, "polymarket-event-id", "", "exact Polymarket event ID")
	fs.StringVar(&decision.PolymarketEventID, "event-id", "", "alias for --polymarket-event-id")
	fs.StringVar(&decision.PolymarketMarketID, "polymarket-market-id", "", "optional exact Polymarket market ID")
	fs.StringVar(&decision.PolymarketMarketID, "market-id", "", "alias for --polymarket-market-id")
	fs.StringVar(&decision.ConditionID, "condition-id", "", "optional exact Polymarket condition ID")
	fs.StringVar(&decision.CanonicalEventID, "canonical-event-id", "", "exact canonical event UUID, required for approve")
	fs.StringVar(&decision.Reviewer, "reviewer", "", "optional reviewer name or ID")
	fs.StringVar(&decision.DecisionSource, "source", "", "optional decision source label, default manual")
	fs.StringVar(&decision.ReviewReason, "reason", "", "reviewer reason")
	fs.StringVar(&decision.MatchReason, "match-reason", "", "matching reason to preserve with the decision")
	return decision
}

func printUsage(out io.Writer) {
	_, _ = fmt.Fprintln(out, `Usage:
  go run ./cmd/match-review unresolved [--limit 50]
  go run ./cmd/match-review ambiguous [--limit 50]
  go run ./cmd/match-review confirmed [--limit 50]
  go run ./cmd/match-review approve --polymarket-event-id <id> --canonical-event-id <uuid> [--condition-id <id>] [--polymarket-market-id <id>] [--reviewer <name>] [--reason <text>] [--confidence <score>] [--match-reason <text>]
  go run ./cmd/match-review reject --polymarket-event-id <id> --reason <text> [--condition-id <id>] [--polymarket-market-id <id>] [--reviewer <name>] [--match-reason <text>]
  go run ./cmd/match-review search-polymarket --canonical-event-id <uuid> [--limit 25] [--output table|json]
  go run ./cmd/match-review approve-polymarket-token --token-id <id> --canonical-event-id <uuid> --canonical-outcome-name <name> --canonical-outcome-side <home|away|draw|over|under|other> --reviewer <name> --reason <text> [--condition-id <id>] [--polymarket-market-id <id>] [--confidence <score>]
  go run ./cmd/match-review reject-polymarket-token --token-id <id> --reviewer <name> --reason <text> [--canonical-event-id <uuid>] [--canonical-outcome-name <name>] [--canonical-outcome-side <side>] [--condition-id <id>] [--polymarket-market-id <id>]
  go run ./cmd/match-review list-polymarket-token-aliases [--canonical-event-id <uuid>] [--token-id <id>] [--status confirmed|rejected|unresolved|ambiguous] [--limit 50] [--output table|json]

Approval requires exact IDs and never performs fuzzy auto-approval. Rejection
records a reviewer decision and does not create an alias. search-polymarket is
read-only: it lists local Polymarket event/market candidates that may match a
canonical sports event so an operator can decide whether to manually review.
search-polymarket never writes aliases, never auto-maps, and does not call any
external API. Polymarket midpoint is reported only as analytical metadata; best
bid and best ask remain separate. Use cmd/polymarket-sync to refresh local
metadata and price snapshots if no candidates are found.

approve-polymarket-token / reject-polymarket-token / list-polymarket-token-aliases
are the Phase 12H manual token-to-canonical-outcome workflow. Confirmed token
aliases require an exact token_id, exact canonical_event_id, an explicit
canonical_outcome_name AND canonical_outcome_side, plus reviewer and reason.
The store rejects any second-confirmation that conflicts with an existing
confirmed alias on the same token, the same canonical event side, or the same
canonical event with a different mapping. There is no auto-mapping, no
auto-approval, no inference from title order, and no path to any order or
account endpoint.`)
}

type polymarketTokenAliasStore interface {
	ApprovePolymarketTokenAlias(context.Context, model.PolymarketTokenAliasDecision) error
	RejectPolymarketTokenAlias(context.Context, model.PolymarketTokenAliasDecision) error
	ListPolymarketTokenAliases(context.Context, model.PolymarketTokenAliasListOptions) ([]model.PolymarketTokenAlias, error)
}

func tokenAliasDecisionFlags(fs *flag.FlagSet) (*model.PolymarketTokenAliasDecision, *float64) {
	decision := &model.PolymarketTokenAliasDecision{}
	fs.StringVar(&decision.TokenID, "token-id", "", "exact Polymarket token ID (required)")
	fs.StringVar(&decision.ConditionID, "condition-id", "", "optional condition_id (must match the token's stored condition_id if supplied)")
	fs.StringVar(&decision.PolymarketMarketID, "polymarket-market-id", "", "optional polymarket_market_id (must match the token's stored market_id if supplied)")
	fs.StringVar(&decision.PolymarketMarketID, "market-id", "", "alias for --polymarket-market-id")
	fs.StringVar(&decision.CanonicalEventID, "canonical-event-id", "", "exact canonical event UUID")
	fs.StringVar(&decision.CanonicalOutcomeName, "canonical-outcome-name", "", "canonical outcome name as the operator confirms it (e.g. \"Cleveland Cavaliers\")")
	fs.StringVar(&decision.CanonicalOutcomeSide, "canonical-outcome-side", "", "canonical outcome side: home, away, draw, over, under, other")
	fs.StringVar(&decision.Reviewer, "reviewer", "", "reviewer name or ID (required)")
	fs.StringVar(&decision.Reason, "reason", "", "reviewer reason (required)")
	fs.StringVar(&decision.DecisionSource, "source", "", "optional decision_source label, default 'manual'")
	confidence := fs.Float64("confidence", -1, "optional confidence score in [0, 100]")
	return decision, confidence
}

func approvePolymarketToken(ctx context.Context, st polymarketTokenAliasStore, out io.Writer, args []string) error {
	fs := flag.NewFlagSet("approve-polymarket-token", flag.ContinueOnError)
	fs.SetOutput(out)
	decision, confidence := tokenAliasDecisionFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *confidence >= 0 {
		if *confidence > 100 {
			return errors.New("confidence must be between 0 and 100")
		}
		decision.ConfidenceScore = confidence
	}
	if err := st.ApprovePolymarketTokenAlias(ctx, *decision); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "approved polymarket token_id=%s canonical_event_id=%s outcome_side=%s outcome_name=%q\n",
		decision.TokenID, decision.CanonicalEventID, strings.ToLower(decision.CanonicalOutcomeSide), decision.CanonicalOutcomeName)
	return nil
}

func rejectPolymarketToken(ctx context.Context, st polymarketTokenAliasStore, out io.Writer, args []string) error {
	fs := flag.NewFlagSet("reject-polymarket-token", flag.ContinueOnError)
	fs.SetOutput(out)
	decision, confidence := tokenAliasDecisionFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *confidence >= 0 {
		if *confidence > 100 {
			return errors.New("confidence must be between 0 and 100")
		}
		decision.ConfidenceScore = confidence
	}
	if err := st.RejectPolymarketTokenAlias(ctx, *decision); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "rejected polymarket token_id=%s reason=%q\n", decision.TokenID, decision.Reason)
	return nil
}

func listPolymarketTokenAliases(ctx context.Context, st polymarketTokenAliasStore, out io.Writer, args []string) error {
	fs := flag.NewFlagSet("list-polymarket-token-aliases", flag.ContinueOnError)
	fs.SetOutput(out)
	canonicalID := fs.String("canonical-event-id", "", "filter by canonical event UUID")
	tokenID := fs.String("token-id", "", "filter by Polymarket token ID")
	status := fs.String("status", "", "filter by status: confirmed, rejected, unresolved, ambiguous")
	limit := fs.Int("limit", 50, "maximum rows to print")
	output := fs.String("output", "table", "output format: table or json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(*output)) {
	case "table", "json":
	default:
		return fmt.Errorf("--output must be table or json")
	}
	aliases, err := st.ListPolymarketTokenAliases(ctx, model.PolymarketTokenAliasListOptions{
		CanonicalEventID: *canonicalID,
		TokenID:          *tokenID,
		Status:           *status,
		Limit:            *limit,
	})
	if err != nil {
		return err
	}
	if strings.EqualFold(strings.TrimSpace(*output), "json") {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(aliases)
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "alias_id\ttoken_id\tcanonical_event_id\tsport\tteams\toutcome_side\toutcome_name\tpm_outcome_name\tstatus\tconfidence\treviewer\treason")
	for _, alias := range aliases {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			alias.AliasID,
			alias.TokenID,
			alias.CanonicalEventID,
			dashStr(alias.SportKey),
			dashStr(alias.HomeTeam+" vs "+alias.AwayTeam),
			alias.CanonicalOutcomeSide,
			alias.CanonicalOutcomeName,
			dashStr(alias.PolymarketOutcomeName),
			alias.Status,
			formatFloat(alias.ConfidenceScore),
			alias.Reviewer,
			alias.Reason,
		)
	}
	return tw.Flush()
}

type polymarketCandidateStore interface {
	SearchPolymarketCandidates(context.Context, model.PolymarketCandidateSearch) (model.PolymarketCandidateReport, error)
}

func searchPolymarket(ctx context.Context, st polymarketCandidateStore, out io.Writer, args []string) error {
	fs := flag.NewFlagSet("search-polymarket", flag.ContinueOnError)
	fs.SetOutput(out)
	canonicalEventID := fs.String("canonical-event-id", "", "canonical event UUID to search Polymarket candidates for (required)")
	limit := fs.Int("limit", 25, "maximum candidate rows to return")
	output := fs.String("output", "table", "output format: table or json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*canonicalEventID) == "" {
		return errors.New("--canonical-event-id is required")
	}
	switch strings.ToLower(strings.TrimSpace(*output)) {
	case "table", "json":
	default:
		return fmt.Errorf("--output must be table or json")
	}
	report, err := st.SearchPolymarketCandidates(ctx, model.PolymarketCandidateSearch{
		CanonicalEventID: *canonicalEventID,
		Limit:            *limit,
	})
	if err != nil {
		return fmt.Errorf("search polymarket candidates: %w", err)
	}
	enriched := decoratePolymarketCandidates(report)
	if strings.EqualFold(*output, "json") {
		return writePolymarketCandidateReportJSON(out, enriched)
	}
	return writePolymarketCandidateReportTable(out, enriched)
}

// polymarketCandidateView is the operator-facing rendering of a candidate row.
// It carries safety hints derived from the candidate's lifecycle/timing/price
// state. The hints are diagnostic only and never authorize an alias.
type polymarketCandidateView struct {
	Candidate        model.PolymarketCandidate `json:"candidate"`
	SafetyHints      []string                  `json:"safety_hints"`
	NextManualAction string                    `json:"next_manual_action"`
}

type polymarketCandidateReportView struct {
	Report     model.PolymarketCandidateReport `json:"report"`
	Candidates []polymarketCandidateView       `json:"decorated_candidates"`
}

const (
	polymarketHintActiveMarket            = "active_event_and_market"
	polymarketHintClosedOrArchived        = "candidate_event_or_market_closed_or_archived"
	polymarketHintZeroTokens              = "polymarket_tokens_missing"
	polymarketHintNoExecutablePrice       = "polymarket_executable_price_missing"
	polymarketHintMidpointOnly            = "midpoint_only_not_executable"
	polymarketHintMarketEndPast           = "polymarket_market_end_time_past"
	polymarketHintConfirmedAliasExists    = "canonical_event_already_has_confirmed_polymarket_alias"
	polymarketHintCandidateAliasConfirmed = "candidate_event_already_confirmed_for_other_canonical_event"
	polymarketHintHasUnresolvedReview     = "candidate_event_has_unresolved_match_review"
	polymarketHintHasAmbiguousReview      = "candidate_event_has_ambiguous_match_review"
	polymarketHintTokensMatchedSingleSide = "only_one_team_token_matched"
)

func decoratePolymarketCandidates(report model.PolymarketCandidateReport) polymarketCandidateReportView {
	views := make([]polymarketCandidateView, 0, len(report.Candidates))
	now := time.Now().UTC()
	for _, c := range report.Candidates {
		hints := []string{}
		if isPolymarketActive(c) {
			hints = append(hints, polymarketHintActiveMarket)
		} else {
			hints = append(hints, polymarketHintClosedOrArchived)
		}
		if c.TokenCount == 0 {
			hints = append(hints, polymarketHintZeroTokens)
		}
		if !c.HasBestBidAndAsk {
			hints = append(hints, polymarketHintNoExecutablePrice)
		}
		if c.LatestMidpoint != nil && !c.HasBestBidAndAsk {
			hints = append(hints, polymarketHintMidpointOnly)
		}
		if c.MarketEndTime != nil && c.MarketEndTime.Before(now) {
			hints = append(hints, polymarketHintMarketEndPast)
		}
		if report.HasConfirmedAlias {
			hints = append(hints, polymarketHintConfirmedAliasExists)
		}
		if c.HasConfirmedAlias && !report.HasConfirmedAlias {
			hints = append(hints, polymarketHintCandidateAliasConfirmed)
		}
		if c.HasUnresolvedReview {
			hints = append(hints, polymarketHintHasUnresolvedReview)
		}
		if c.HasAmbiguousReview {
			hints = append(hints, polymarketHintHasAmbiguousReview)
		}
		if c.MatchedSource == "single_team_token" {
			hints = append(hints, polymarketHintTokensMatchedSingleSide)
		}

		view := polymarketCandidateView{
			Candidate:        c,
			SafetyHints:      hints,
			NextManualAction: polymarketCandidateNextManualAction(report, c, hints),
		}
		views = append(views, view)
	}
	return polymarketCandidateReportView{Report: report, Candidates: views}
}

func isPolymarketActive(c model.PolymarketCandidate) bool {
	if c.EventActive != nil && !*c.EventActive {
		return false
	}
	if c.MarketActive != nil && !*c.MarketActive {
		return false
	}
	if c.EventClosed != nil && *c.EventClosed {
		return false
	}
	if c.MarketClosed != nil && *c.MarketClosed {
		return false
	}
	if c.EventArchived != nil && *c.EventArchived {
		return false
	}
	if c.MarketArchived != nil && *c.MarketArchived {
		return false
	}
	return true
}

func polymarketCandidateNextManualAction(report model.PolymarketCandidateReport, c model.PolymarketCandidate, hints []string) string {
	hintSet := map[string]struct{}{}
	for _, h := range hints {
		hintSet[h] = struct{}{}
	}
	if _, ok := hintSet[polymarketHintConfirmedAliasExists]; ok {
		return "canonical event already has a confirmed Polymarket alias; no new mapping needed unless intentionally replacing it"
	}
	if _, ok := hintSet[polymarketHintCandidateAliasConfirmed]; ok {
		return "this Polymarket event is already confirmed against a different canonical event; do not re-map without explicit reviewer reason"
	}
	if _, ok := hintSet[polymarketHintClosedOrArchived]; ok {
		return "candidate market is closed or archived; use only for historical validation, not for current comparison"
	}
	if _, ok := hintSet[polymarketHintMarketEndPast]; ok {
		return "candidate market end_time is in the past; treat as historical only"
	}
	if _, ok := hintSet[polymarketHintZeroTokens]; ok {
		return "candidate has no Polymarket tokens locally; run cmd/polymarket-sync to refresh metadata before mapping"
	}
	if _, ok := hintSet[polymarketHintNoExecutablePrice]; ok {
		return "candidate has no recent best bid AND best ask snapshot; run cmd/polymarket-sync to refresh prices before treating as comparison-ready"
	}
	if _, ok := hintSet[polymarketHintTokensMatchedSingleSide]; ok {
		return "only one team token matched; manually verify event before approving alias"
	}
	if _, ok := hintSet[polymarketHintHasAmbiguousReview]; ok {
		return "candidate event has an ambiguous match review; resolve via cmd/match-review before approving an alias"
	}
	if _, ok := hintSet[polymarketHintHasUnresolvedReview]; ok {
		return "candidate event has an unresolved match review; resolve via cmd/match-review before approving an alias"
	}
	if c.PolymarketEventID == "" {
		return "candidate has no Polymarket event id; verify metadata before mapping"
	}
	if !report.HasConfirmedAlias {
		return "candidate looks plausible; manually verify event/market settlement semantics before approving an alias via 'match-review approve'"
	}
	return "review candidate manually before any alias decision"
}

func writePolymarketCandidateReportJSON(out io.Writer, view polymarketCandidateReportView) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(view)
}

func writePolymarketCandidateReportTable(out io.Writer, view polymarketCandidateReportView) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	canonical := view.Report.CanonicalEvent
	_, _ = fmt.Fprintf(tw, "canonical_event_id\t%s\n", canonical.ID)
	_, _ = fmt.Fprintf(tw, "sport_key\t%s\n", canonical.SportKey)
	_, _ = fmt.Fprintf(tw, "teams\t%s vs %s\n", canonical.HomeTeam, canonical.AwayTeam)
	_, _ = fmt.Fprintf(tw, "commence_time\t%s\n", formatTime(canonical.CommenceTime))
	_, _ = fmt.Fprintf(tw, "home_search_tokens\t%s\n", strings.Join(view.Report.HomeTokens, ","))
	_, _ = fmt.Fprintf(tw, "away_search_tokens\t%s\n", strings.Join(view.Report.AwayTokens, ","))
	_, _ = fmt.Fprintf(tw, "has_confirmed_alias_for_canonical_event\t%t\n", view.Report.HasConfirmedAlias)
	if len(view.Report.UnresolvedReviewIDs) > 0 {
		ids := make([]string, len(view.Report.UnresolvedReviewIDs))
		for i, id := range view.Report.UnresolvedReviewIDs {
			ids[i] = strconv.FormatInt(id, 10)
		}
		_, _ = fmt.Fprintf(tw, "unresolved_review_ids\t%s\n", strings.Join(ids, ","))
	}
	_, _ = fmt.Fprintf(tw, "candidate_count\t%d\n", len(view.Candidates))
	_ = tw.Flush()
	if len(view.Candidates) == 0 {
		_, _ = fmt.Fprintln(out, "\nno local Polymarket candidates found; run cmd/polymarket-sync to refresh metadata, then re-run search-polymarket")
		return nil
	}
	_, _ = fmt.Fprintln(out)
	tw = tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "rank\tpolymarket_event_id\tpolymarket_market_id\tactive\tclosed\tarchived\ttokens\tbid\task\tmidpoint\tlatest_price_at\ttitle\tquestion\tnext_manual_action\tsafety_hints")
	for i, view := range view.Candidates {
		c := view.Candidate
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			i+1,
			dashStr(c.PolymarketEventID),
			dashStr(c.PolymarketMarketID),
			formatTriBool(c.MarketActive),
			formatTriBool(c.MarketClosed),
			formatTriBool(c.MarketArchived),
			c.TokenCount,
			formatNullableFloat(c.LatestBestBid),
			formatNullableFloat(c.LatestBestAsk),
			formatNullableFloat(c.LatestMidpoint),
			formatNullableTime(c.LatestPriceTime),
			truncateField(c.PolymarketEventTitle, 60),
			truncateField(c.MarketQuestion, 60),
			view.NextManualAction,
			strings.Join(view.SafetyHints, ","),
		)
	}
	return tw.Flush()
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func formatNullableTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func formatTriBool(v *bool) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatBool(*v)
}

func formatNullableFloat(v *float64) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatFloat(*v, 'f', 4, 64)
}

func dashStr(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

func truncateField(s string, max int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	if len(s) > max {
		return s[:max-1] + "…"
	}
	return s
}

func formatFloat(v *float64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatFloat(*v, 'f', 2, 64)
}

func formatTimePtr(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
