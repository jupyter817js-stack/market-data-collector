package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
)

// Phase 12E discovery diagnostic. cmd/polymarket-sync --diagnose-coverage runs
// a read-only Gamma walk and classifies what was returned. It never writes to
// PostgreSQL, never auto-maps Polymarket markets to canonical events, never
// approves aliases, and never produces a trading signal. The wider sync uses
// the same source client; the goal here is to expose WHY a normal sync only
// returns historical 2021 markets so an operator can pick the right Gamma
// query parameters before retrying a normal sync.

const (
	diagnoseDefaultPageSize = 100
	diagnoseDefaultMaxPages = 5
	diagnoseDefaultTimeout  = 30 * time.Second
)

const (
	candidateActivePlausible  = "active_plausible"
	candidateClosedHistorical = "closed_historical"
	candidateMissingTokens    = "missing_tokens"
	candidateMissingPrice     = "missing_price"
	candidateNoMatch          = "no_match"
)

type diagnoseConfig struct {
	GammaBaseURL    string
	HTTPTimeout     time.Duration
	Endpoint        string
	PageSize        int
	MaxPages        int
	IncludeClosed   bool
	IncludeArchived bool
	IncludeInactive bool
	Order           string
	Ascending       *bool
	TagSlug         string
	TagID           string
	Category        string
	Query           string
	HomeTeam        string
	AwayTeam        string
	Now             time.Time
	Output          string
}

type diagnoseEventLister interface {
	ListEvents(context.Context, polymarket.ListOptions) ([]polymarket.Event, error)
}

type diagnoseMarketLister interface {
	ListMarkets(context.Context, polymarket.ListOptions) ([]polymarket.Market, error)
}

// diagnoseGammaClient is the slice of polymarket.Client used by the
// diagnostic. The interface lets tests inject a fake client backed by an
// httptest server without making real Polymarket calls.
type diagnoseGammaClient interface {
	diagnoseEventLister
	diagnoseMarketLister
}

type diagnoseReport struct {
	Filters              diagnoseFilters           `json:"filters"`
	EndpointURLExample   string                    `json:"endpoint_url_example"`
	Pagination           diagnosePaginationStats   `json:"pagination"`
	LifecycleCounts      map[string]int            `json:"lifecycle_counts"`
	CategoryCounts       map[string]int            `json:"category_counts,omitempty"`
	TagCounts            map[string]int            `json:"tag_counts,omitempty"`
	TimeBucketCounts     map[string]int            `json:"time_bucket_counts"`
	TokensCounts         diagnoseTokenStats        `json:"token_stats"`
	TeamPairMatch        diagnoseTeamPairStats     `json:"team_pair_match"`
	SafetyCategoryCounts map[string]int            `json:"safety_category_counts"`
	SampleMatches        []diagnoseCandidateSample `json:"sample_matches"`
	Notes                []string                  `json:"notes"`
}

type diagnoseFilters struct {
	Endpoint        string `json:"endpoint"`
	PageSize        int    `json:"page_size"`
	MaxPages        int    `json:"max_pages"`
	IncludeClosed   bool   `json:"include_closed"`
	IncludeArchived bool   `json:"include_archived"`
	IncludeInactive bool   `json:"include_inactive"`
	Order           string `json:"order,omitempty"`
	Ascending       *bool  `json:"ascending,omitempty"`
	TagSlug         string `json:"tag_slug,omitempty"`
	TagID           string `json:"tag_id,omitempty"`
	Category        string `json:"category,omitempty"`
	Query           string `json:"query,omitempty"`
	HomeTeam        string `json:"home_team,omitempty"`
	AwayTeam        string `json:"away_team,omitempty"`
	Now             string `json:"now,omitempty"`
}

type diagnosePaginationStats struct {
	PagesFetched      int  `json:"pages_fetched"`
	ItemsFetched      int  `json:"items_fetched"`
	UniqueItemsByID   int  `json:"unique_items_by_id"`
	LastPageItemCount int  `json:"last_page_item_count"`
	HitMaxPages       bool `json:"hit_max_pages"`
}

type diagnoseTokenStats struct {
	ItemsWithTokens    int `json:"items_with_tokens"`
	ItemsWithoutTokens int `json:"items_without_tokens"`
	TokenCountSum      int `json:"token_count_sum"`
}

type diagnoseTeamPairStats struct {
	HomeTokens           []string `json:"home_tokens,omitempty"`
	AwayTokens           []string `json:"away_tokens,omitempty"`
	HomeTokenMatchCount  int      `json:"home_token_match_count"`
	AwayTokenMatchCount  int      `json:"away_token_match_count"`
	BothTokensMatchCount int      `json:"both_tokens_match_count"`
	QueryMatchCount      int      `json:"query_match_count,omitempty"`
}

type diagnoseCandidateSample struct {
	ID             string   `json:"id"`
	Slug           string   `json:"slug,omitempty"`
	Title          string   `json:"title,omitempty"`
	Question       string   `json:"question,omitempty"`
	StartTime      string   `json:"start_time,omitempty"`
	EndTime        string   `json:"end_time,omitempty"`
	Active         *bool    `json:"active,omitempty"`
	Closed         *bool    `json:"closed,omitempty"`
	Archived       *bool    `json:"archived,omitempty"`
	TokenCount     int      `json:"token_count"`
	MatchedHomeTok string   `json:"matched_home_token,omitempty"`
	MatchedAwayTok string   `json:"matched_away_token,omitempty"`
	SafetyCategory string   `json:"safety_category"`
	WhyExcluded    []string `json:"why_excluded,omitempty"`
}

func parseDiagnoseConfig(args []string, out io.Writer) (diagnoseConfig, error) {
	fs := flag.NewFlagSet("diagnose-coverage", flag.ContinueOnError)
	fs.SetOutput(out)
	cfg := diagnoseConfig{
		Endpoint:    "events",
		PageSize:    diagnoseDefaultPageSize,
		MaxPages:    diagnoseDefaultMaxPages,
		HTTPTimeout: diagnoseDefaultTimeout,
		Output:      "json",
	}
	endpoint := fs.String("endpoint", cfg.Endpoint, "Polymarket Gamma list endpoint to walk: events or markets")
	pageSize := fs.Int("page-size", cfg.PageSize, "page size per Gamma request")
	maxPages := fs.Int("max-pages", cfg.MaxPages, "maximum number of pages to fetch")
	includeClosed := fs.Bool("include-closed", false, "include markets where closed=true (default excludes them)")
	includeArchived := fs.Bool("include-archived", false, "include markets where archived=true")
	includeInactive := fs.Bool("include-inactive", false, "include markets where active=false")
	order := fs.String("order", "", "Gamma order parameter (e.g. endDate, startDate, id)")
	ascendingFlag := fs.String("ascending", "", "Gamma ascending parameter: \"true\" or \"false\"")
	tagSlug := fs.String("tag", "", "Gamma tag_slug filter (e.g. nba, sports)")
	tagID := fs.String("tag-id", "", "Gamma tag_id filter")
	category := fs.String("category", "", "Gamma category filter")
	query := fs.String("query", "", "free-text fuzzy substring matched in title/slug/question after fetch")
	homeTeam := fs.String("home-team", "", "canonical home team string for token-pair matching")
	awayTeam := fs.String("away-team", "", "canonical away team string for token-pair matching")
	now := fs.String("now", "", "override current time for deterministic audits (RFC3339 UTC)")
	output := fs.String("output", cfg.Output, "output format: json or table")
	httpTimeout := fs.Duration("http-timeout", cfg.HTTPTimeout, "HTTP timeout per Gamma request")

	if err := fs.Parse(args); err != nil {
		return diagnoseConfig{}, err
	}

	cfg.Endpoint = strings.ToLower(strings.TrimSpace(*endpoint))
	if cfg.Endpoint != "events" && cfg.Endpoint != "markets" {
		return diagnoseConfig{}, fmt.Errorf("--endpoint must be 'events' or 'markets'")
	}
	if *pageSize <= 0 || *pageSize > 500 {
		return diagnoseConfig{}, fmt.Errorf("--page-size must be in (0, 500]")
	}
	cfg.PageSize = *pageSize
	if *maxPages <= 0 || *maxPages > 50 {
		return diagnoseConfig{}, fmt.Errorf("--max-pages must be in (0, 50]")
	}
	cfg.MaxPages = *maxPages
	cfg.IncludeClosed = *includeClosed
	cfg.IncludeArchived = *includeArchived
	cfg.IncludeInactive = *includeInactive
	cfg.Order = strings.TrimSpace(*order)
	if v := strings.TrimSpace(*ascendingFlag); v != "" {
		switch strings.ToLower(v) {
		case "true":
			t := true
			cfg.Ascending = &t
		case "false":
			f := false
			cfg.Ascending = &f
		default:
			return diagnoseConfig{}, fmt.Errorf("--ascending must be 'true' or 'false'")
		}
	}
	cfg.TagSlug = strings.TrimSpace(*tagSlug)
	cfg.TagID = strings.TrimSpace(*tagID)
	cfg.Category = strings.TrimSpace(*category)
	cfg.Query = strings.TrimSpace(*query)
	cfg.HomeTeam = strings.TrimSpace(*homeTeam)
	cfg.AwayTeam = strings.TrimSpace(*awayTeam)
	cfg.Output = strings.ToLower(strings.TrimSpace(*output))
	if cfg.Output != "json" && cfg.Output != "table" {
		return diagnoseConfig{}, fmt.Errorf("--output must be 'json' or 'table'")
	}
	if v := strings.TrimSpace(*now); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return diagnoseConfig{}, fmt.Errorf("--now must be RFC3339 UTC: %w", err)
		}
		cfg.Now = t.UTC()
	}
	if *httpTimeout <= 0 {
		return diagnoseConfig{}, fmt.Errorf("--http-timeout must be positive")
	}
	cfg.HTTPTimeout = *httpTimeout
	cfg.GammaBaseURL = strings.TrimSpace(envDefault("POLYMARKET_GAMMA_BASE_URL", polymarket.DefaultGammaBaseURL))
	return cfg, nil
}

func runDiagnoseCoverage(ctx context.Context, cfg diagnoseConfig, client diagnoseGammaClient, out io.Writer) error {
	if cfg.Now.IsZero() {
		cfg.Now = time.Now().UTC()
	}
	report := diagnoseReport{
		Filters: diagnoseFilters{
			Endpoint:        cfg.Endpoint,
			PageSize:        cfg.PageSize,
			MaxPages:        cfg.MaxPages,
			IncludeClosed:   cfg.IncludeClosed,
			IncludeArchived: cfg.IncludeArchived,
			IncludeInactive: cfg.IncludeInactive,
			Order:           cfg.Order,
			Ascending:       cfg.Ascending,
			TagSlug:         cfg.TagSlug,
			TagID:           cfg.TagID,
			Category:        cfg.Category,
			Query:           cfg.Query,
			HomeTeam:        cfg.HomeTeam,
			AwayTeam:        cfg.AwayTeam,
			Now:             cfg.Now.Format(time.RFC3339),
		},
		LifecycleCounts:      map[string]int{},
		CategoryCounts:       map[string]int{},
		TagCounts:            map[string]int{},
		TimeBucketCounts:     map[string]int{},
		SafetyCategoryCounts: map[string]int{},
		Notes:                []string{},
	}
	report.EndpointURLExample = exampleEndpointURL(cfg)

	listOpts := func(offset int, limit int) polymarket.ListOptions {
		opts := polymarket.ListOptions{
			Limit:     limit,
			Offset:    offset,
			Order:     cfg.Order,
			Ascending: cfg.Ascending,
			TagSlug:   cfg.TagSlug,
			TagID:     cfg.TagID,
			Category:  cfg.Category,
		}
		if !cfg.IncludeClosed {
			f := false
			opts.Closed = &f
		}
		if !cfg.IncludeArchived {
			f := false
			opts.Archived = &f
		}
		if !cfg.IncludeInactive {
			t := true
			opts.Active = &t
		}
		return opts
	}

	homeTokens := teamSearchTokens(cfg.HomeTeam)
	awayTokens := teamSearchTokens(cfg.AwayTeam)
	report.TeamPairMatch.HomeTokens = homeTokens
	report.TeamPairMatch.AwayTokens = awayTokens
	queryLower := strings.ToLower(cfg.Query)

	type unit struct {
		ID        string
		Slug      string
		Title     string
		Question  string
		StartTime *time.Time
		EndTime   *time.Time
		Active    *bool
		Closed    *bool
		Archived  *bool
		Category  string
		Tags      []polymarket.Tag
		TokenLen  int
	}

	uniqueIDs := map[string]struct{}{}
	samples := []diagnoseCandidateSample{}
	const sampleCap = 20
	totalItems := 0
	lastPageItems := 0
	pages := 0
	hitCap := false

	for pages < cfg.MaxPages {
		opts := listOpts(pages*cfg.PageSize, cfg.PageSize)
		var items []unit
		if cfg.Endpoint == "events" {
			events, err := client.ListEvents(ctx, opts)
			if err != nil {
				return fmt.Errorf("list polymarket events page %d: %w", pages+1, err)
			}
			for _, e := range events {
				items = append(items, unit{
					ID:        e.ID,
					Slug:      e.Slug,
					Title:     e.Title,
					StartTime: e.StartTime,
					EndTime:   e.EndTime,
					Active:    e.Active,
					Closed:    e.Closed,
					Archived:  e.Archived,
					Category:  e.Category,
					Tags:      e.Tags,
					TokenLen:  countEventTokens(e),
				})
			}
		} else {
			markets, err := client.ListMarkets(ctx, opts)
			if err != nil {
				return fmt.Errorf("list polymarket markets page %d: %w", pages+1, err)
			}
			for _, m := range markets {
				items = append(items, unit{
					ID:        m.ID,
					Slug:      m.Slug,
					Title:     m.Title,
					Question:  m.Question,
					StartTime: m.StartTime,
					EndTime:   m.EndTime,
					Active:    m.Active,
					Closed:    m.Closed,
					Archived:  m.Archived,
					Category:  m.Category,
					TokenLen:  len(m.Tokens),
				})
			}
		}
		pages++
		lastPageItems = len(items)
		totalItems += len(items)
		for _, it := range items {
			if it.ID != "" {
				uniqueIDs[it.ID] = struct{}{}
			}
			report.LifecycleCounts[lifecycleBucket(it.Active, it.Closed, it.Archived)]++
			if it.Category != "" {
				report.CategoryCounts[it.Category]++
			}
			for _, tag := range it.Tags {
				if tag.Slug != "" {
					report.TagCounts[tag.Slug]++
				} else if tag.Label != "" {
					report.TagCounts[tag.Label]++
				}
			}
			report.TimeBucketCounts[timeBucket(it.EndTime, cfg.Now)]++
			if it.TokenLen > 0 {
				report.TokensCounts.ItemsWithTokens++
				report.TokensCounts.TokenCountSum += it.TokenLen
			} else {
				report.TokensCounts.ItemsWithoutTokens++
			}
			haystack := strings.ToLower(it.Title + " " + it.Slug + " " + it.Question)
			matchedHome := firstTokenMatch(homeTokens, haystack)
			matchedAway := firstTokenMatch(awayTokens, haystack)
			if matchedHome != "" {
				report.TeamPairMatch.HomeTokenMatchCount++
			}
			if matchedAway != "" {
				report.TeamPairMatch.AwayTokenMatchCount++
			}
			if matchedHome != "" && matchedAway != "" {
				report.TeamPairMatch.BothTokensMatchCount++
			}
			if queryLower != "" && strings.Contains(haystack, queryLower) {
				report.TeamPairMatch.QueryMatchCount++
			}

			isQueryMatch := queryLower != "" && strings.Contains(haystack, queryLower)
			isPairMatch := matchedHome != "" && matchedAway != ""
			if isQueryMatch || isPairMatch {
				safety, why := classifyCandidateSafety(it.Active, it.Closed, it.Archived, it.EndTime, it.TokenLen, cfg.Now, isPairMatch, matchedHome, matchedAway)
				report.SafetyCategoryCounts[safety]++
				if len(samples) < sampleCap {
					samples = append(samples, diagnoseCandidateSample{
						ID:             it.ID,
						Slug:           it.Slug,
						Title:          it.Title,
						Question:       it.Question,
						StartTime:      formatNullableRFC3339(it.StartTime),
						EndTime:        formatNullableRFC3339(it.EndTime),
						Active:         it.Active,
						Closed:         it.Closed,
						Archived:       it.Archived,
						TokenCount:     it.TokenLen,
						MatchedHomeTok: matchedHome,
						MatchedAwayTok: matchedAway,
						SafetyCategory: safety,
						WhyExcluded:    why,
					})
				}
			}
		}
		if len(items) == 0 {
			break
		}
		if len(items) < cfg.PageSize {
			break
		}
	}
	if pages >= cfg.MaxPages {
		hitCap = true
	}

	report.Pagination = diagnosePaginationStats{
		PagesFetched:      pages,
		ItemsFetched:      totalItems,
		UniqueItemsByID:   len(uniqueIDs),
		LastPageItemCount: lastPageItems,
		HitMaxPages:       hitCap,
	}
	report.SampleMatches = samples
	if homeTokens == nil && awayTokens == nil && queryLower == "" {
		report.SafetyCategoryCounts[candidateNoMatch] = totalItems
	}
	if report.SafetyCategoryCounts == nil {
		report.SafetyCategoryCounts = map[string]int{}
	}
	if (homeTokens != nil || awayTokens != nil || queryLower != "") && report.TeamPairMatch.BothTokensMatchCount == 0 && report.TeamPairMatch.QueryMatchCount == 0 {
		report.Notes = append(report.Notes,
			"no Polymarket items matched the supplied query/team-pair tokens within the page budget; consider --order endDate --ascending false, a tag/category filter, or expanding --max-pages")
	}
	if hitCap {
		report.Notes = append(report.Notes,
			"max-pages cap reached; the diagnostic stopped before exhausting Gamma. If you suspect missing coverage, raise --max-pages with care or narrow filters")
	}
	if !cfg.IncludeClosed && report.LifecycleCounts["closed"] > 0 {
		report.Notes = append(report.Notes,
			"closed=false was sent to Gamma but some returned items still report closed=true; rely on the lifecycle counts not the request filter")
	}
	if cfg.IncludeClosed {
		report.Notes = append(report.Notes,
			"--include-closed is on; results include closed historical markets which must NOT be mapped to current canonical events")
	}
	report.Notes = append(report.Notes,
		"diagnostic is read-only; nothing was written to PostgreSQL and no aliases were approved",
		"midpoint, last trade price, and spread are analytical metadata only; this output is not a trading signal")
	return writeDiagnoseReport(out, cfg.Output, report)
}

func writeDiagnoseReport(out io.Writer, outputFormat string, report diagnoseReport) error {
	if outputFormat == "json" {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	return writeDiagnoseTable(out, report)
}

func writeDiagnoseTable(out io.Writer, report diagnoseReport) error {
	lines := []string{
		fmt.Sprintf("endpoint=%s page_size=%d max_pages=%d include_closed=%t include_archived=%t include_inactive=%t",
			report.Filters.Endpoint, report.Filters.PageSize, report.Filters.MaxPages,
			report.Filters.IncludeClosed, report.Filters.IncludeArchived, report.Filters.IncludeInactive),
		fmt.Sprintf("endpoint_url_example=%s", report.EndpointURLExample),
		fmt.Sprintf("pagination pages=%d items=%d unique=%d last_page=%d hit_max_pages=%t",
			report.Pagination.PagesFetched, report.Pagination.ItemsFetched, report.Pagination.UniqueItemsByID,
			report.Pagination.LastPageItemCount, report.Pagination.HitMaxPages),
		fmt.Sprintf("lifecycle_counts %s", sortedMap(report.LifecycleCounts)),
		fmt.Sprintf("time_bucket_counts %s", sortedMap(report.TimeBucketCounts)),
		fmt.Sprintf("token_stats with_tokens=%d without_tokens=%d token_sum=%d",
			report.TokensCounts.ItemsWithTokens, report.TokensCounts.ItemsWithoutTokens, report.TokensCounts.TokenCountSum),
		fmt.Sprintf("team_pair_match home_tokens=%v away_tokens=%v home_match=%d away_match=%d both_match=%d query_match=%d",
			report.TeamPairMatch.HomeTokens, report.TeamPairMatch.AwayTokens,
			report.TeamPairMatch.HomeTokenMatchCount, report.TeamPairMatch.AwayTokenMatchCount,
			report.TeamPairMatch.BothTokensMatchCount, report.TeamPairMatch.QueryMatchCount),
		fmt.Sprintf("safety_category_counts %s", sortedMap(report.SafetyCategoryCounts)),
		fmt.Sprintf("sample_matches=%d", len(report.SampleMatches)),
	}
	for _, line := range lines {
		if _, err := fmt.Fprintln(out, line); err != nil {
			return err
		}
	}
	for _, s := range report.SampleMatches {
		if _, err := fmt.Fprintf(out, "  - %s | active=%s closed=%s archived=%s tokens=%d safety=%s title=%q\n",
			s.ID, formatTriBoolStr(s.Active), formatTriBoolStr(s.Closed), formatTriBoolStr(s.Archived),
			s.TokenCount, s.SafetyCategory, truncate(s.Title, 80)); err != nil {
			return err
		}
	}
	for _, note := range report.Notes {
		if _, err := fmt.Fprintf(out, "note: %s\n", note); err != nil {
			return err
		}
	}
	return nil
}

func teamSearchTokens(team string) []string {
	team = strings.ToLower(strings.TrimSpace(team))
	if team == "" {
		return nil
	}
	parts := strings.Fields(team)
	seen := map[string]struct{}{}
	out := []string{}
	add := func(t string) {
		t = strings.TrimSpace(t)
		if len(t) < 4 {
			return
		}
		if _, ok := seen[t]; ok {
			return
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(parts) == 0 {
		return nil
	}
	add(parts[len(parts)-1])
	for i, part := range parts {
		if i == len(parts)-1 {
			continue
		}
		add(part)
	}
	return out
}

func firstTokenMatch(tokens []string, haystack string) string {
	for _, t := range tokens {
		if t == "" {
			continue
		}
		if strings.Contains(haystack, t) {
			return t
		}
	}
	return ""
}

func lifecycleBucket(active, closed, archived *bool) string {
	if archived != nil && *archived {
		return "archived"
	}
	if closed != nil && *closed {
		return "closed"
	}
	if active != nil && !*active {
		return "inactive"
	}
	if active != nil && *active {
		return "active"
	}
	return "unknown_lifecycle"
}

func timeBucket(end *time.Time, now time.Time) string {
	if end == nil {
		return "no_end_time"
	}
	delta := end.Sub(now)
	switch {
	case delta > 30*24*time.Hour:
		return "ends_more_than_30d_future"
	case delta > 7*24*time.Hour:
		return "ends_within_30d_future"
	case delta > 0:
		return "ends_within_7d_future"
	case -delta < 7*24*time.Hour:
		return "ended_within_last_7d"
	case -delta < 30*24*time.Hour:
		return "ended_within_last_30d"
	default:
		return "ended_more_than_30d_ago"
	}
}

func classifyCandidateSafety(active, closed, archived *bool, end *time.Time, tokens int, now time.Time, isPairMatch bool, home, away string) (string, []string) {
	why := []string{}
	if archived != nil && *archived {
		why = append(why, "candidate is archived")
		return candidateClosedHistorical, why
	}
	if closed != nil && *closed {
		why = append(why, "candidate is closed")
		return candidateClosedHistorical, why
	}
	if active != nil && !*active {
		why = append(why, "candidate is inactive")
		return candidateClosedHistorical, why
	}
	if end != nil && end.Before(now) {
		why = append(why, "candidate end_time is in the past")
		return candidateClosedHistorical, why
	}
	if tokens == 0 {
		why = append(why, "candidate has zero CLOB tokens locally")
		return candidateMissingTokens, why
	}
	if !isPairMatch && (home == "" && away == "") {
		why = append(why, "no team-pair tokens were configured")
		return candidateNoMatch, why
	}
	if !isPairMatch {
		why = append(why, "only one team-side token matched; manually verify event")
	}
	return candidateActivePlausible, why
}

func countEventTokens(e polymarket.Event) int {
	total := 0
	for _, m := range e.Markets {
		total += len(m.Tokens)
	}
	return total
}

func formatNullableRFC3339(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func formatTriBoolStr(v *bool) string {
	if v == nil {
		return "?"
	}
	if *v {
		return "true"
	}
	return "false"
}

func sortedMap(m map[string]int) string {
	if len(m) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func truncate(s string, max int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > max {
		return s[:max-1] + "…"
	}
	return s
}

// exampleEndpointURL renders one example URL the diagnostic would call against
// Gamma so the operator can paste it into curl for cross-verification. It uses
// offset=0 so it represents the first page only.
func exampleEndpointURL(cfg diagnoseConfig) string {
	base := strings.TrimRight(cfg.GammaBaseURL, "/")
	if base == "" {
		base = polymarket.DefaultGammaBaseURL
	}
	u, err := url.Parse(base + "/" + cfg.Endpoint)
	if err != nil {
		return ""
	}
	q := u.Query()
	q.Set("limit", fmt.Sprintf("%d", cfg.PageSize))
	q.Set("offset", "0")
	if !cfg.IncludeClosed {
		q.Set("closed", "false")
	}
	if !cfg.IncludeArchived {
		q.Set("archived", "false")
	}
	if !cfg.IncludeInactive {
		q.Set("active", "true")
	}
	if cfg.Order != "" {
		q.Set("order", cfg.Order)
	}
	if cfg.Ascending != nil {
		q.Set("ascending", fmt.Sprintf("%t", *cfg.Ascending))
	}
	if cfg.TagSlug != "" {
		q.Set("tag_slug", cfg.TagSlug)
	}
	if cfg.TagID != "" {
		q.Set("tag_id", cfg.TagID)
	}
	if cfg.Category != "" {
		q.Set("category", cfg.Category)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// shouldRunDiagnoseCoverage returns true if the operator passed
// --diagnose-coverage on the command line. The discriminator is checked before
// loadConfig() so the diagnostic runs without opening PostgreSQL.
func shouldRunDiagnoseCoverage(args []string) ([]string, bool) {
	for i, arg := range args {
		switch arg {
		case "--diagnose-coverage":
			return append(append([]string{}, args[:i]...), args[i+1:]...), true
		case "--diagnose-coverage=true":
			return append(append([]string{}, args[:i]...), args[i+1:]...), true
		case "--diagnose-coverage=false":
			return append(append([]string{}, args[:i]...), args[i+1:]...), false
		}
	}
	return args, false
}

// runDiagnoseCoverageCLI is the entrypoint invoked from main() when
// --diagnose-coverage is present on the CLI. It builds a real Gamma client
// and writes the diagnostic to stdout. Tests should call runDiagnoseCoverage
// directly with a fake client backed by an httptest server.
func runDiagnoseCoverageCLI(ctx context.Context, args []string) error {
	cfg, err := parseDiagnoseConfig(args, os.Stderr)
	if err != nil {
		return err
	}
	client := polymarket.NewClientWithRetry(cfg.GammaBaseURL, cfg.HTTPTimeout, polymarket.DefaultRetryConfig())
	return runDiagnoseCoverage(ctx, cfg, client, os.Stdout)
}
