package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/your-org/pol-odds-collector/internal/source/betfair"
)

const (
	outputText = "text"
	outputJSON = "json"

	maxLookupMarketIDs = 5
)

type lookupConfig struct {
	AppKeyEnv       string
	SessionTokenEnv string
	AppKey          string
	SessionToken    string
	Endpoint        string
	Output          string
	MarketIDs       []string
}

type marketIDFlag struct {
	items []string
}

type lookupReport struct {
	ReadOnly             bool                 `json:"read_only"`
	CollectorIntegration bool                 `json:"collector_integration"`
	AliasAutoWrite       bool                 `json:"alias_auto_write"`
	MarketIDsRequested   int                  `json:"market_ids_requested"`
	Markets              []lookupMarketReport `json:"markets"`
	SuggestedNextStep    string               `json:"suggested_next_step"`
}

type lookupMarketReport struct {
	BetfairEventID       string               `json:"betfair_event_id,omitempty"`
	BetfairEventName     string               `json:"betfair_event_name,omitempty"`
	BetfairEventTypeID   string               `json:"betfair_event_type_id,omitempty"`
	BetfairEventTypeName string               `json:"betfair_event_type_name,omitempty"`
	CompetitionID        string               `json:"competition_id,omitempty"`
	CompetitionName      string               `json:"competition_name,omitempty"`
	BetfairMarketID      string               `json:"betfair_market_id"`
	BetfairMarketName    string               `json:"betfair_market_name,omitempty"`
	MarketStartTime      string               `json:"market_start_time,omitempty"`
	Runners              []lookupRunnerReport `json:"runners"`
}

type lookupRunnerReport struct {
	SelectionID  int64             `json:"selection_id"`
	RunnerName   string            `json:"runner_name,omitempty"`
	Handicap     *float64          `json:"handicap,omitempty"`
	SortPriority *int              `json:"sort_priority,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, http.DefaultClient); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintf(os.Stderr, "betfair catalogue lookup failed: %s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer, httpClient *http.Client) error {
	cfg, err := parseConfig(args, out)
	if err != nil {
		return err
	}
	client, err := betfair.NewCatalogueClient(betfair.CatalogueClientOptions{
		Endpoint:     cfg.Endpoint,
		AppKey:       cfg.AppKey,
		SessionToken: cfg.SessionToken,
		HTTPClient:   httpClient,
	})
	if err != nil {
		return err
	}
	markets, err := client.ListMarketCatalogue(ctx, betfair.CatalogueLookupOptions{MarketIDs: cfg.MarketIDs})
	if err != nil {
		return err
	}
	report := reportFromCatalogue(cfg, markets)
	return writeReport(out, cfg.Output, report)
}

func parseConfig(args []string, out io.Writer) (lookupConfig, error) {
	var marketIDs marketIDFlag
	cfg := lookupConfig{Endpoint: betfair.DefaultCatalogueEndpoint, Output: outputText}
	fs := flag.NewFlagSet("betfair-catalogue-lookup", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&cfg.AppKeyEnv, "app-key-env", "", "environment variable name containing the Betfair app key")
	fs.StringVar(&cfg.SessionTokenEnv, "session-token-env", "", "environment variable name containing the Betfair session token")
	fs.StringVar(&cfg.Endpoint, "endpoint", cfg.Endpoint, "Betfair Betting API JSON-RPC endpoint")
	fs.StringVar(&cfg.Output, "output", cfg.Output, "output format: text or json")
	fs.Var(&marketIDs, "market-id", "explicit Betfair market ID; may be repeated or comma-separated")
	if err := fs.Parse(args); err != nil {
		return lookupConfig{}, err
	}
	cfg.Output = strings.ToLower(strings.TrimSpace(cfg.Output))
	if cfg.Output != outputText && cfg.Output != outputJSON {
		return lookupConfig{}, fmt.Errorf("output must be %q or %q", outputText, outputJSON)
	}
	if strings.TrimSpace(cfg.AppKeyEnv) == "" {
		return lookupConfig{}, fmt.Errorf("app-key-env is required")
	}
	if strings.TrimSpace(cfg.SessionTokenEnv) == "" {
		return lookupConfig{}, fmt.Errorf("session-token-env is required")
	}
	cfg.MarketIDs = marketIDs.values()
	if _, err := betfair.ValidateExplicitMarketIDs(cfg.MarketIDs, maxLookupMarketIDs); err != nil {
		return lookupConfig{}, err
	}
	appKey, ok := os.LookupEnv(cfg.AppKeyEnv)
	if !ok || strings.TrimSpace(appKey) == "" {
		return lookupConfig{}, fmt.Errorf("environment variable named by --app-key-env (%s) is not set or empty", cfg.AppKeyEnv)
	}
	sessionToken, ok := os.LookupEnv(cfg.SessionTokenEnv)
	if !ok || strings.TrimSpace(sessionToken) == "" {
		return lookupConfig{}, fmt.Errorf("environment variable named by --session-token-env (%s) is not set or empty", cfg.SessionTokenEnv)
	}
	cfg.AppKey = strings.TrimSpace(appKey)
	cfg.SessionToken = strings.TrimSpace(sessionToken)
	return cfg, nil
}

func (m *marketIDFlag) String() string {
	return strings.Join(m.values(), ",")
}

func (m *marketIDFlag) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == "*" || strings.Contains(part, "*") {
			return fmt.Errorf("wildcard market IDs are not allowed")
		}
		m.items = append(m.items, part)
	}
	return nil
}

func (m *marketIDFlag) values() []string {
	if m == nil {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(m.items))
	for _, item := range m.items {
		item = strings.TrimSpace(item)
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	return out
}

func reportFromCatalogue(cfg lookupConfig, markets []betfair.CatalogueMarket) lookupReport {
	report := lookupReport{
		ReadOnly:             true,
		CollectorIntegration: false,
		AliasAutoWrite:       false,
		MarketIDsRequested:   len(cfg.MarketIDs),
		Markets:              make([]lookupMarketReport, 0, len(markets)),
		SuggestedNextStep:    "Compare catalogue metadata with canonical_events, then use cmd/betfair-map-review only for manual confirmed aliases.",
	}
	for _, market := range markets {
		item := lookupMarketReport{
			BetfairEventID:       strings.TrimSpace(market.Event.ID),
			BetfairEventName:     strings.TrimSpace(market.Event.Name),
			BetfairEventTypeID:   strings.TrimSpace(market.EventType.ID),
			BetfairEventTypeName: strings.TrimSpace(market.EventType.Name),
			CompetitionID:        strings.TrimSpace(market.Competition.ID),
			CompetitionName:      strings.TrimSpace(market.Competition.Name),
			BetfairMarketID:      strings.TrimSpace(market.MarketID),
			BetfairMarketName:    strings.TrimSpace(market.MarketName),
			MarketStartTime:      strings.TrimSpace(market.MarketStartTime),
			Runners:              make([]lookupRunnerReport, 0, len(market.Runners)),
		}
		for _, runner := range market.Runners {
			item.Runners = append(item.Runners, lookupRunnerReport{
				SelectionID:  runner.SelectionID,
				RunnerName:   strings.TrimSpace(runner.RunnerName),
				Handicap:     runner.Handicap,
				SortPriority: runner.SortPriority,
				Metadata:     runner.Metadata,
			})
		}
		report.Markets = append(report.Markets, item)
	}
	return report
}

func writeReport(out io.Writer, output string, report lookupReport) error {
	if output == outputJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "betfair_event_id\tevent_name\tbetfair_market_id\tmarket_name\tmarket_start_time\tselection_id\trunner_name\thandicap\tsort_priority")
	for _, market := range report.Markets {
		if len(market.Runners) == 0 {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t\t\t\t\n",
				market.BetfairEventID,
				market.BetfairEventName,
				market.BetfairMarketID,
				market.BetfairMarketName,
				market.MarketStartTime,
			)
			continue
		}
		for _, runner := range market.Runners {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
				market.BetfairEventID,
				market.BetfairEventName,
				market.BetfairMarketID,
				market.BetfairMarketName,
				market.MarketStartTime,
				runner.SelectionID,
				runner.RunnerName,
				floatPtrString(runner.Handicap),
				intPtrString(runner.SortPriority),
			)
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(out, "next_step=%s\n", report.SuggestedNextStep)
	return err
}

func floatPtrString(value *float64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%.4f", *value)
}

func intPtrString(value *int) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%d", *value)
}
