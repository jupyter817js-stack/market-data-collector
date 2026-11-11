package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const (
	outputText             = "text"
	outputJSON             = "json"
	sourceBetfairCatalogue = "betfair_catalogue"
)

type canonicalStore interface {
	CreateReviewedCanonicalEvent(context.Context, model.CanonicalEventCreateInput) (model.CanonicalEventCreateResult, error)
	Close() error
}

type storeFactory func(context.Context, string) (canonicalStore, error)

type config struct {
	ConfirmCreate  bool
	DatabaseURLEnv string
	DatabaseURL    string
	Output         string
	Source         string
	SourceEventID  string
	SourceMarketID string
	SportKey       string
	League         string
	HomeTeam       string
	AwayTeam       string
	CommenceTime   time.Time
	Status         string
	Reviewer       string
	Reason         string
}

type report struct {
	DryRun               bool                 `json:"dry_run"`
	Created              bool                 `json:"created"`
	CanonicalEventID     string               `json:"canonical_event_id,omitempty"`
	CanonicalEvent       canonicalEventReport `json:"canonical_event"`
	Review               reviewContextReport  `json:"review"`
	DatabaseURLRedacted  string               `json:"database_url_redacted,omitempty"`
	AliasAutoWrite       bool                 `json:"alias_auto_write"`
	CollectorIntegration bool                 `json:"collector_integration"`
	NextAllowedAction    string               `json:"next_allowed_action"`
}

type canonicalEventReport struct {
	SportKey     string `json:"sport_key"`
	League       string `json:"league"`
	HomeTeam     string `json:"home_team"`
	AwayTeam     string `json:"away_team"`
	HomeTeamNorm string `json:"home_team_norm"`
	AwayTeamNorm string `json:"away_team_norm"`
	CommenceTime string `json:"commence_time"`
	Status       string `json:"status"`
}

type reviewContextReport struct {
	Source         string `json:"source"`
	SourceEventID  string `json:"source_event_id"`
	SourceMarketID string `json:"source_market_id"`
	Reviewer       string `json:"reviewer"`
	Reason         string `json:"reason"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, defaultStoreFactory); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintf(os.Stderr, "betfair canonical event failed: %s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer, factory storeFactory) error {
	cfg, err := parseConfig(args, out)
	if err != nil {
		return err
	}
	if !cfg.ConfirmCreate {
		return writeReport(out, cfg.Output, reportFromConfig(cfg, model.CanonicalEventCreateResult{}, true))
	}
	st, err := factory(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer func() { _ = st.Close() }()

	result, err := st.CreateReviewedCanonicalEvent(ctx, model.CanonicalEventCreateInput{
		SportKey:     cfg.SportKey,
		League:       cfg.League,
		HomeTeam:     cfg.HomeTeam,
		AwayTeam:     cfg.AwayTeam,
		CommenceTime: cfg.CommenceTime,
		Status:       cfg.Status,
	})
	if err != nil {
		return err
	}
	return writeReport(out, cfg.Output, reportFromConfig(cfg, result, false))
}

func defaultStoreFactory(ctx context.Context, databaseURL string) (canonicalStore, error) {
	return store.Open(ctx, databaseURL, false, normalize.FreshnessPolicy{})
}

func parseConfig(args []string, out io.Writer) (config, error) {
	cfg := config{
		Output: outputText,
		Source: sourceBetfairCatalogue,
		Status: "scheduled",
	}
	fs := flag.NewFlagSet("betfair-canonical-event", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.BoolVar(&cfg.ConfirmCreate, "confirm-create", false, "write the reviewed canonical event to canonical_events")
	fs.StringVar(&cfg.DatabaseURLEnv, "database-url-env", "", "environment variable name containing the PostgreSQL URL; required with --confirm-create")
	fs.StringVar(&cfg.Output, "output", cfg.Output, "output format: text or json")
	fs.StringVar(&cfg.Source, "source", cfg.Source, "review source label; expected betfair_catalogue")
	fs.StringVar(&cfg.SourceEventID, "source-event-id", "", "Betfair catalogue event ID used as review context")
	fs.StringVar(&cfg.SourceMarketID, "source-market-id", "", "Betfair market ID used as review context")
	fs.StringVar(&cfg.SportKey, "sport-key", "", "canonical sport key")
	fs.StringVar(&cfg.League, "league", "", "canonical league")
	fs.StringVar(&cfg.HomeTeam, "home-team", "", "canonical home team")
	fs.StringVar(&cfg.AwayTeam, "away-team", "", "canonical away team")
	commence := fs.String("commence-time", "", "canonical commence time in RFC3339 UTC")
	fs.StringVar(&cfg.Status, "status", cfg.Status, "canonical event status")
	fs.StringVar(&cfg.Reviewer, "reviewer", "", "reviewer name or ID")
	fs.StringVar(&cfg.Reason, "reason", "", "review reason")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	cfg.Output = strings.ToLower(strings.TrimSpace(cfg.Output))
	if cfg.Output != outputText && cfg.Output != outputJSON {
		return config{}, fmt.Errorf("output must be %q or %q", outputText, outputJSON)
	}
	var err error
	cfg.CommenceTime, err = parseRequiredTime(*commence)
	if err != nil {
		return config{}, err
	}
	cfg.Source = strings.TrimSpace(cfg.Source)
	cfg.SourceEventID = strings.TrimSpace(cfg.SourceEventID)
	cfg.SourceMarketID = strings.TrimSpace(cfg.SourceMarketID)
	cfg.SportKey = strings.TrimSpace(cfg.SportKey)
	cfg.League = strings.TrimSpace(cfg.League)
	cfg.HomeTeam = strings.TrimSpace(cfg.HomeTeam)
	cfg.AwayTeam = strings.TrimSpace(cfg.AwayTeam)
	cfg.Status = strings.TrimSpace(cfg.Status)
	cfg.Reviewer = strings.TrimSpace(cfg.Reviewer)
	cfg.Reason = strings.TrimSpace(cfg.Reason)
	if cfg.Source != sourceBetfairCatalogue {
		return config{}, fmt.Errorf("source must be %q", sourceBetfairCatalogue)
	}
	if cfg.SourceEventID == "" {
		return config{}, fmt.Errorf("source-event-id is required")
	}
	if cfg.SourceMarketID == "" {
		return config{}, fmt.Errorf("source-market-id is required")
	}
	if cfg.Reviewer == "" {
		return config{}, fmt.Errorf("reviewer is required")
	}
	if cfg.Reason == "" {
		return config{}, fmt.Errorf("reason is required")
	}
	if _, err := normalizeCreateInput(cfg); err != nil {
		return config{}, err
	}
	if cfg.ConfirmCreate {
		if cfg.DatabaseURLEnv == "" {
			return config{}, fmt.Errorf("database-url-env is required with --confirm-create")
		}
		databaseURL, ok := os.LookupEnv(cfg.DatabaseURLEnv)
		if !ok || strings.TrimSpace(databaseURL) == "" {
			return config{}, fmt.Errorf("environment variable named by --database-url-env (%s) is not set or empty", cfg.DatabaseURLEnv)
		}
		cfg.DatabaseURL = strings.TrimSpace(databaseURL)
	}
	return cfg, nil
}

func normalizeCreateInput(cfg config) (model.CanonicalEventCreateInput, error) {
	input := model.CanonicalEventCreateInput{
		SportKey:     cfg.SportKey,
		League:       cfg.League,
		HomeTeam:     cfg.HomeTeam,
		AwayTeam:     cfg.AwayTeam,
		CommenceTime: cfg.CommenceTime,
		Status:       cfg.Status,
	}
	if input.SportKey == "" {
		return input, fmt.Errorf("sport-key is required")
	}
	if input.League == "" {
		return input, fmt.Errorf("league is required")
	}
	if input.HomeTeam == "" {
		return input, fmt.Errorf("home-team is required")
	}
	if input.AwayTeam == "" {
		return input, fmt.Errorf("away-team is required")
	}
	if input.HomeTeam == input.AwayTeam {
		return input, fmt.Errorf("home-team and away-team must differ")
	}
	if normalize.EventTeamNorm(input.HomeTeam) == "" || normalize.EventTeamNorm(input.AwayTeam) == "" {
		return input, fmt.Errorf("home-team and away-team must normalize to non-empty values")
	}
	return input, nil
}

func parseRequiredTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("commence-time is required")
	}
	ts, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("commence-time must be RFC3339: %w", err)
	}
	return ts.UTC(), nil
}

func reportFromConfig(cfg config, result model.CanonicalEventCreateResult, dryRun bool) report {
	input, _ := normalizeCreateInput(cfg)
	event := result.Event
	if dryRun || event.ID == "" {
		event = model.CanonicalEvent{
			SportKey:     input.SportKey,
			League:       input.League,
			HomeTeam:     input.HomeTeam,
			AwayTeam:     input.AwayTeam,
			HomeTeamNorm: normalize.EventTeamNorm(input.HomeTeam),
			AwayTeamNorm: normalize.EventTeamNorm(input.AwayTeam),
			CommenceTime: input.CommenceTime,
			Status:       input.Status,
		}
	}
	return report{
		DryRun:           dryRun,
		Created:          result.Created,
		CanonicalEventID: event.ID,
		CanonicalEvent: canonicalEventReport{
			SportKey:     event.SportKey,
			League:       event.League,
			HomeTeam:     event.HomeTeam,
			AwayTeam:     event.AwayTeam,
			HomeTeamNorm: event.HomeTeamNorm,
			AwayTeamNorm: event.AwayTeamNorm,
			CommenceTime: event.CommenceTime.UTC().Format(time.RFC3339),
			Status:       event.Status,
		},
		Review: reviewContextReport{
			Source:         cfg.Source,
			SourceEventID:  cfg.SourceEventID,
			SourceMarketID: cfg.SourceMarketID,
			Reviewer:       cfg.Reviewer,
			Reason:         cfg.Reason,
		},
		DatabaseURLRedacted:  redactDatabaseURL(cfg.DatabaseURL),
		AliasAutoWrite:       false,
		CollectorIntegration: false,
		NextAllowedAction:    "Approve Betfair event, market, and runner aliases manually with cmd/betfair-map-review only after verifying this canonical event.",
	}
}

func writeReport(out io.Writer, output string, report report) error {
	if output == outputJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	_, err := fmt.Fprintf(out, "canonical_event dry_run=%t created=%t canonical_event_id=%s sport_key=%s league=%s home_team=%s away_team=%s commence_time=%s source=%s source_event_id=%s source_market_id=%s alias_auto_write=false collector_integration=false next_allowed_action=%q\n",
		report.DryRun,
		report.Created,
		report.CanonicalEventID,
		report.CanonicalEvent.SportKey,
		report.CanonicalEvent.League,
		report.CanonicalEvent.HomeTeam,
		report.CanonicalEvent.AwayTeam,
		report.CanonicalEvent.CommenceTime,
		report.Review.Source,
		report.Review.SourceEventID,
		report.Review.SourceMarketID,
		report.NextAllowedAction,
	)
	return err
}

func redactDatabaseURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "[REDACTED_DATABASE_URL]"
	}
	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			parsed.User = url.UserPassword("redacted", "redacted")
		} else {
			parsed.User = url.User("redacted")
		}
	}
	return parsed.String()
}
