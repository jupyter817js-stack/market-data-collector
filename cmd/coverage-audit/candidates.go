package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

// Candidate gap categories. These are the named gaps surfaced to operators in
// `--audit polymarket --candidates` mode. They are read-only diagnostics and
// must not be interpreted as trading signals.
const (
	candidateGapNoExternalFairValue         = "no_external_fair_value"
	candidateGapNoSportsbookConsensus       = "no_sportsbook_consensus"
	candidateGapBetfairNotReady             = "betfair_not_ready"
	candidateGapNoConfirmedPolymarketAlias  = "no_confirmed_polymarket_alias"
	candidateGapNoPolymarketMarketMetadata  = "no_polymarket_market_metadata"
	candidateGapNoPolymarketTokens          = "no_polymarket_tokens"
	candidateGapNoPolymarketExecutablePrice = "no_polymarket_executable_price"
	candidateGapPolymarketPriceStale        = "polymarket_price_stale"
	candidateGapLowPolymarketLiquidity      = "low_polymarket_liquidity"
	candidateGapResolutionUnverified        = "resolution_unverified"
	candidateGapComparisonReady             = "comparison_ready"
)

// Candidate component names. Each component carries an explicit weight so the
// total readiness score is transparent and capped at maxCandidateScore.
const (
	componentSportsbookConsensus      = "sportsbook_consensus"
	componentBetfairReadyRunnerBooks  = "betfair_ready_runner_books"
	componentConfirmedPolymarketAlias = "confirmed_polymarket_alias"
	componentPolymarketMarketMetadata = "polymarket_market_metadata"
	componentPolymarketTokens         = "polymarket_tokens"
	componentBestBidAndAsk            = "best_bid_and_ask"
	componentPolymarketPriceFresh     = "polymarket_price_fresh"
	componentDepthOrLiquidity         = "depth_or_liquidity"
	componentResolutionStatusOpen     = "resolution_status_open"
)

// Maximum readiness score equals the sum of component weights below.
const maxCandidateScore = 100

// candidateComponentWeights returns the ordered component weights used to
// compute the readiness score. Sum equals maxCandidateScore.
func candidateComponentWeights() []candidateComponent {
	return []candidateComponent{
		{Name: componentSportsbookConsensus, Weight: 15},
		{Name: componentBetfairReadyRunnerBooks, Weight: 5},
		{Name: componentConfirmedPolymarketAlias, Weight: 20},
		{Name: componentPolymarketMarketMetadata, Weight: 5},
		{Name: componentPolymarketTokens, Weight: 10},
		{Name: componentBestBidAndAsk, Weight: 20},
		{Name: componentPolymarketPriceFresh, Weight: 10},
		{Name: componentDepthOrLiquidity, Weight: 10},
		{Name: componentResolutionStatusOpen, Weight: 5},
	}
}

type candidateComponent struct {
	Name    string `json:"name"`
	Weight  int    `json:"weight"`
	Present bool   `json:"present"`
}

type candidateRow struct {
	CanonicalEventID     string                                 `json:"canonical_event_id"`
	SportKey             string                                 `json:"sport_key,omitempty"`
	League               string                                 `json:"league,omitempty"`
	HomeTeam             string                                 `json:"home_team,omitempty"`
	AwayTeam             string                                 `json:"away_team,omitempty"`
	CommenceTime         string                                 `json:"commence_time,omitempty"`
	ReadinessScore       int                                    `json:"readiness_score"`
	MaxReadinessScore    int                                    `json:"max_readiness_score"`
	Components           []candidateComponent                   `json:"components"`
	PresentComponents    []string                               `json:"present_components"`
	MissingComponents    []string                               `json:"missing_components"`
	PrimaryGapReason     string                                 `json:"primary_gap_reason"`
	NextManualAction     string                                 `json:"next_manual_action"`
	ComparisonReady      bool                                   `json:"comparison_ready"`
	GrossComparison      bool                                   `json:"gross_comparison_possible"`
	NetComparison        bool                                   `json:"net_comparison_possible"`
	PolymarketStaleState string                                 `json:"polymarket_stale_state,omitempty"`
	ResolutionStatus     string                                 `json:"resolution_status,omitempty"`
	EventTimingStatus    string                                 `json:"event_timing_status,omitempty"`
	TimeToStartSeconds   *int64                                 `json:"time_to_start_seconds,omitempty"`
	EventAgeSeconds      *int64                                 `json:"event_age_seconds,omitempty"`
	TimingWarning        string                                 `json:"timing_warning,omitempty"`
	Source               model.PolymarketComparisonReadinessRow `json:"source"`
}

type candidateReport struct {
	Filters candidateFilters `json:"filters"`
	Summary candidateSummary `json:"summary"`
	Rows    []candidateRow   `json:"rows"`
}

type candidateFilters struct {
	SportKey                  string `json:"sport_key,omitempty"`
	UpcomingOnly              bool   `json:"upcoming_only"`
	IncludePast               bool   `json:"include_past"`
	FromTime                  string `json:"from_time,omitempty"`
	ToTime                    string `json:"to_time,omitempty"`
	Now                       string `json:"now,omitempty"`
	StartingSoonWindowSeconds int64  `json:"starting_soon_window_seconds"`
	MaxStartedWindowSeconds   int64  `json:"max_started_window_seconds"`
	Limit                     int    `json:"limit"`
}

type candidateSummary struct {
	RowCount                    int            `json:"row_count"`
	FilteredOutByTimingCount    int            `json:"filtered_out_by_timing_count"`
	ComparisonReadyRows         int            `json:"comparison_ready_rows"`
	GrossComparisonPossibleRows int            `json:"gross_comparison_possible_rows"`
	GapReasonCounts             map[string]int `json:"gap_reason_counts"`
	TimingStatusCounts          map[string]int `json:"timing_status_counts"`
	AverageReadinessScore       int            `json:"average_readiness_score"`
	TopReadinessScore           int            `json:"top_readiness_score"`
}

// candidateFetchLimit is the upper bound on how many rows the candidates path
// pulls from the store before scoring/sorting in Go. The CLI --limit then
// trims the final sorted output.
const candidateFetchLimit = 500

// Phase 12C event timing classification constants. These are operator-safety
// hints; they are not trading signals.
const (
	eventTimingUpcoming        = "upcoming"
	eventTimingStartingSoon    = "starting_soon"
	eventTimingInPlayOrStarted = "in_play_or_started"
	eventTimingPast            = "past"
	eventTimingUnknown         = "unknown"
)

const (
	defaultStartingSoonWindow = 2 * time.Hour
	defaultMaxStartedWindow   = 4 * time.Hour
)

func runPolymarketCandidates(ctx context.Context, st coverageAuditStore, cfg coverageAuditConfig, out io.Writer) error {
	now := cfg.Now
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	commenceAfter := cfg.FromTime
	commenceBefore := cfg.ToTime
	// When timing filters are not set explicitly, push a sensible commence_time
	// floor down to SQL so the bounded fetch is more likely to surface
	// upcoming/pre-match events instead of recently-priced past events.
	if commenceAfter == nil && !cfg.IncludePast {
		floor := now.Add(-defaultMaxStartedWindow)
		if cfg.UpcomingOnly {
			floor = now
		}
		commenceAfter = &floor
	}

	rows, err := st.ListPolymarketComparisonReadiness(ctx, model.PolymarketComparisonReadinessOptions{
		SportKey:       cfg.SportKey,
		Candidates:     true,
		CommenceAfter:  commenceAfter,
		CommenceBefore: commenceBefore,
		Limit:          candidateFetchLimit,
	})
	if err != nil {
		return fmt.Errorf("read polymarket comparison readiness: %w", err)
	}

	scored := scoreCandidates(rows, now)
	totalScored := len(scored)
	scored = filterCandidatesByTiming(scored, cfg)
	sortCandidatesByReadiness(scored)
	if cfg.Limit > 0 && len(scored) > cfg.Limit {
		scored = scored[:cfg.Limit]
	}
	filters := candidateFilters{
		SportKey:                  cfg.SportKey,
		UpcomingOnly:              cfg.UpcomingOnly,
		IncludePast:               cfg.IncludePast,
		Now:                       formatTime(now),
		StartingSoonWindowSeconds: int64(defaultStartingSoonWindow.Seconds()),
		MaxStartedWindowSeconds:   int64(defaultMaxStartedWindow.Seconds()),
		Limit:                     cfg.Limit,
	}
	if cfg.FromTime != nil {
		filters.FromTime = formatTime(*cfg.FromTime)
	}
	if cfg.ToTime != nil {
		filters.ToTime = formatTime(*cfg.ToTime)
	}
	summary := summarizeCandidates(scored)
	summary.FilteredOutByTimingCount = totalScored - len(scored)
	if summary.FilteredOutByTimingCount < 0 {
		summary.FilteredOutByTimingCount = 0
	}
	report := candidateReport{
		Filters: filters,
		Summary: summary,
		Rows:    scored,
	}
	return writeCandidateReport(out, cfg.Output, report)
}

func scoreCandidates(rows []model.PolymarketComparisonReadinessRow, now time.Time) []candidateRow {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	out := make([]candidateRow, 0, len(rows))
	for _, row := range rows {
		decorated := decoratePolymarketRows([]model.PolymarketComparisonReadinessRow{row})[0]
		components := evaluateCandidateComponents(decorated)
		score := 0
		present := []string{}
		missing := []string{}
		for _, c := range components {
			if c.Present {
				score += c.Weight
				present = append(present, c.Name)
			} else {
				missing = append(missing, c.Name)
			}
		}
		timingStatus, timeToStart, eventAge, timingWarning := classifyEventTiming(decorated.CommenceTime, now)
		nextAction := candidateNextAction(decorated)
		if timingStatus == eventTimingPast || timingStatus == eventTimingInPlayOrStarted {
			nextAction = "event is past; use only for historical validation unless intentionally reviewing stale data"
		}
		out = append(out, candidateRow{
			CanonicalEventID:     decorated.CanonicalEventID,
			SportKey:             decorated.SportKey,
			League:               decorated.League,
			HomeTeam:             decorated.HomeTeam,
			AwayTeam:             decorated.AwayTeam,
			CommenceTime:         formatTime(decorated.CommenceTime),
			ReadinessScore:       score,
			MaxReadinessScore:    maxCandidateScore,
			Components:           components,
			PresentComponents:    present,
			MissingComponents:    missing,
			PrimaryGapReason:     classifyCandidateGap(decorated),
			NextManualAction:     nextAction,
			ComparisonReady:      decorated.ComparisonReady,
			GrossComparison:      decorated.GrossComparisonPossible,
			NetComparison:        decorated.NetComparisonPossible,
			PolymarketStaleState: decorated.PolymarketStaleState,
			ResolutionStatus:     decorated.ResolutionStatus,
			EventTimingStatus:    timingStatus,
			TimeToStartSeconds:   timeToStart,
			EventAgeSeconds:      eventAge,
			TimingWarning:        timingWarning,
			Source:               decorated,
		})
	}
	return out
}

// classifyEventTiming returns (status, time_to_start_seconds, event_age_seconds, warning).
// time_to_start_seconds is positive for future events and negative for past events
// (zero for unknown). event_age_seconds is non-nil only for past/in_play_or_started.
func classifyEventTiming(commence time.Time, now time.Time) (string, *int64, *int64, string) {
	if commence.IsZero() {
		return eventTimingUnknown, nil, nil, "commence_time is unknown; verify event metadata before mapping"
	}
	delta := commence.Sub(now)
	deltaSec := int64(delta.Round(time.Second).Seconds())
	switch {
	case delta > defaultStartingSoonWindow:
		return eventTimingUpcoming, &deltaSec, nil, ""
	case delta > 0:
		return eventTimingStartingSoon, &deltaSec, nil, "event starts within the operator starting-soon window"
	case -delta <= defaultMaxStartedWindow:
		ageSec := int64((-delta).Round(time.Second).Seconds())
		return eventTimingInPlayOrStarted, &deltaSec, &ageSec, "event has already started; comparison would be in-play or post-start"
	default:
		ageSec := int64((-delta).Round(time.Second).Seconds())
		return eventTimingPast, &deltaSec, &ageSec, "event is past; use only for historical validation unless intentionally reviewing stale data"
	}
}

// filterCandidatesByTiming applies the operator-safety timing filters.
// Default behavior excludes past and in_play_or_started; --upcoming-only also
// excludes unknown; --include-past disables exclusions and surfaces every row
// with its timing label.
func filterCandidatesByTiming(rows []candidateRow, cfg coverageAuditConfig) []candidateRow {
	if cfg.IncludePast {
		return rows
	}
	out := rows[:0]
	for _, row := range rows {
		switch row.EventTimingStatus {
		case eventTimingPast, eventTimingInPlayOrStarted:
			continue
		case eventTimingUnknown:
			if cfg.UpcomingOnly {
				continue
			}
		}
		out = append(out, row)
	}
	return out
}

func evaluateCandidateComponents(row model.PolymarketComparisonReadinessRow) []candidateComponent {
	weights := candidateComponentWeights()
	out := make([]candidateComponent, 0, len(weights))
	for _, c := range weights {
		c.Present = candidateComponentPresent(c.Name, row)
		out = append(out, c)
	}
	return out
}

func candidateComponentPresent(name string, row model.PolymarketComparisonReadinessRow) bool {
	switch name {
	case componentSportsbookConsensus:
		return row.HasSportsbookConsensus
	case componentBetfairReadyRunnerBooks:
		return row.HasReadyBetfair
	case componentConfirmedPolymarketAlias:
		return row.HasConfirmedPolymarketAlias
	case componentPolymarketMarketMetadata:
		return row.HasPolymarketMarketMetadata
	case componentPolymarketTokens:
		return row.HasPolymarketTokens
	case componentBestBidAndAsk:
		return row.HasBestBidAndAsk
	case componentPolymarketPriceFresh:
		return row.PolymarketStaleState == "fresh"
	case componentDepthOrLiquidity:
		if !row.HasDepthOrLiquidity {
			return false
		}
		if row.PolymarketMaxLiquidity != nil && *row.PolymarketMaxLiquidity < polymarketMinLiquidity {
			return false
		}
		return true
	case componentResolutionStatusOpen:
		// Only award this component when there is a real Polymarket market to
		// inspect; a row with no metadata cannot have an open resolution status.
		if !row.HasPolymarketMarketMetadata {
			return false
		}
		if row.PolymarketMarketArchived != nil && *row.PolymarketMarketArchived {
			return false
		}
		if row.PolymarketMarketClosed != nil && *row.PolymarketMarketClosed {
			return false
		}
		switch row.ResolutionStatus {
		case "open":
			return true
		case "":
			return true
		default:
			return false
		}
	default:
		return false
	}
}

// classifyCandidateGap maps the row to the spec-listed Phase 12B candidate
// gap category, in priority order from the most upstream blocker downward.
func classifyCandidateGap(row model.PolymarketComparisonReadinessRow) string {
	hasSportsbook := row.HasSportsbookConsensus
	hasReadyBetfair := row.HasReadyBetfair
	hasBetfairData := row.BetfairMappedQuoteCount > 0

	switch {
	case !hasSportsbook && !hasReadyBetfair && !hasBetfairData:
		return candidateGapNoExternalFairValue
	case !hasSportsbook && !hasReadyBetfair && hasBetfairData:
		return candidateGapBetfairNotReady
	case !hasSportsbook && hasReadyBetfair:
		return candidateGapNoSportsbookConsensus
	case !row.HasConfirmedPolymarketAlias:
		return candidateGapNoConfirmedPolymarketAlias
	case !row.HasPolymarketMarketMetadata:
		return candidateGapNoPolymarketMarketMetadata
	case !row.HasPolymarketTokens:
		return candidateGapNoPolymarketTokens
	case row.PolymarketPriceSnapshotCount == 0 || !row.HasBestBid || !row.HasBestAsk:
		return candidateGapNoPolymarketExecutablePrice
	case row.PolymarketStaleState == "missing" || row.PolymarketStaleState == "stale":
		return candidateGapPolymarketPriceStale
	case !candidateComponentPresent(componentDepthOrLiquidity, row):
		return candidateGapLowPolymarketLiquidity
	case !candidateComponentPresent(componentResolutionStatusOpen, row):
		return candidateGapResolutionUnverified
	default:
		return candidateGapComparisonReady
	}
}

func candidateNextAction(row model.PolymarketComparisonReadinessRow) string {
	switch classifyCandidateGap(row) {
	case candidateGapNoExternalFairValue:
		return "add sportsbook moneyline coverage and consider a manual Betfair canonical event mapping"
	case candidateGapBetfairNotReady:
		return "review Betfair runner book readiness or add sportsbook coverage so external fair value is available"
	case candidateGapNoSportsbookConsensus:
		return "add sportsbook moneyline coverage so the external fair value is not Betfair-only"
	case candidateGapNoConfirmedPolymarketAlias:
		return "manually map and confirm a Polymarket alias for this canonical event"
	case candidateGapNoPolymarketMarketMetadata:
		return "sync Polymarket market metadata for the mapped condition"
	case candidateGapNoPolymarketTokens:
		return "sync Polymarket tokens for the mapped market"
	case candidateGapNoPolymarketExecutablePrice:
		return "run polymarket-sync to capture best bid and best ask snapshots; midpoint alone is analytical, not executable"
	case candidateGapPolymarketPriceStale:
		return "refresh Polymarket price snapshots before interpreting comparison"
	case candidateGapLowPolymarketLiquidity:
		return "inspect Polymarket order book depth before interpreting comparison"
	case candidateGapResolutionUnverified:
		return "verify settlement and resolution semantics manually before interpreting comparison"
	case candidateGapComparisonReady:
		return "gross comparison inputs are present; net comparison still requires a fee or cost model that is not implemented in this project"
	default:
		return "review the readiness components and consult the operational runbook"
	}
}

// timingTier maps an event timing status to a sort priority. Lower tier ranks
// higher: upcoming/starting_soon (0) above unknown (1) above in_play_or_started
// (2) above past (3). This keeps stale events at the bottom even when
// --include-past is supplied so operators see actionable candidates first.
func timingTier(status string) int {
	switch status {
	case eventTimingUpcoming, eventTimingStartingSoon:
		return 0
	case eventTimingUnknown:
		return 1
	case eventTimingInPlayOrStarted:
		return 2
	case eventTimingPast:
		return 3
	default:
		return 1
	}
}

func sortCandidatesByReadiness(rows []candidateRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		ti, tj := timingTier(rows[i].EventTimingStatus), timingTier(rows[j].EventTimingStatus)
		if ti != tj {
			return ti < tj
		}
		if rows[i].ReadinessScore != rows[j].ReadinessScore {
			return rows[i].ReadinessScore > rows[j].ReadinessScore
		}
		if rows[i].CommenceTime != rows[j].CommenceTime {
			return rows[i].CommenceTime < rows[j].CommenceTime
		}
		return rows[i].CanonicalEventID < rows[j].CanonicalEventID
	})
}

func summarizeCandidates(rows []candidateRow) candidateSummary {
	summary := candidateSummary{
		RowCount:           len(rows),
		GapReasonCounts:    map[string]int{},
		TimingStatusCounts: map[string]int{},
	}
	if len(rows) == 0 {
		return summary
	}
	totalScore := 0
	for _, row := range rows {
		summary.GapReasonCounts[row.PrimaryGapReason]++
		if row.EventTimingStatus != "" {
			summary.TimingStatusCounts[row.EventTimingStatus]++
		}
		totalScore += row.ReadinessScore
		if row.ReadinessScore > summary.TopReadinessScore {
			summary.TopReadinessScore = row.ReadinessScore
		}
		if row.ComparisonReady {
			summary.ComparisonReadyRows++
		}
		if row.GrossComparison {
			summary.GrossComparisonPossibleRows++
		}
	}
	summary.AverageReadinessScore = totalScore / len(rows)
	return summary
}

func writeCandidateReport(out io.Writer, output string, report candidateReport) error {
	if output == outputJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	return writeCandidateTableReport(out, report)
}

func writeCandidateTableReport(out io.Writer, report candidateReport) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "rank\treadiness_score\tcomparison_ready\tcanonical_event_id\tsport\tteams\tcommence_time\tevent_timing_status\ttime_to_start_seconds\tprimary_gap_reason\tmissing_components\ttiming_warning\tnext_manual_action"); err != nil {
		return err
	}
	for i, row := range report.Rows {
		if _, err := fmt.Fprintf(tw, "%d\t%d/%d\t%t\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			i+1,
			row.ReadinessScore,
			row.MaxReadinessScore,
			row.ComparisonReady,
			row.CanonicalEventID,
			dash(row.SportKey),
			row.HomeTeam+" vs "+row.AwayTeam,
			dash(row.CommenceTime),
			dash(row.EventTimingStatus),
			formatNullableInt64(row.TimeToStartSeconds),
			row.PrimaryGapReason,
			dash(strings.Join(row.MissingComponents, ",")),
			dash(row.TimingWarning),
			row.NextManualAction,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatNullableInt64(v *int64) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *v)
}
