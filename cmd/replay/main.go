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
	"github.com/your-org/pol-odds-collector/internal/replay"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const (
	defaultDatabaseURL    = "postgres://pol:pol@localhost:5432/pol_odds?sslmode=disable"
	defaultFreshAfterSec  = 120
	defaultAgingAfterSec  = 300
	defaultSnapshotLimit  = 20
	defaultReplayExitCode = 1
)

type replayConfig struct {
	DatabaseURL string
	Filter      replay.SnapshotFilter
	Freshness   normalize.FreshnessPolicy
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
		slog.Error("replay failed", "error", err)
		os.Exit(defaultReplayExitCode)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	cfg, err := parseConfig(args, out)
	if err != nil {
		return err
	}
	if cfg.Filter.Source != model.SourceOddsAPI {
		return fmt.Errorf("unsupported replay source %q: only %s snapshots are decoded today", cfg.Filter.Source, model.SourceOddsAPI)
	}

	st, err := store.Open(ctx, cfg.DatabaseURL, false, cfg.Freshness)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer func() { _ = st.Close() }()

	snapshots, err := st.ListReplayRawSnapshots(ctx, cfg.Filter)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(out, "replay dry_run=true write_mode=false source=%s snapshots=%d\n", cfg.Filter.Source, len(snapshots))
	if len(snapshots) == 0 {
		return nil
	}

	for _, snapshot := range snapshots {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := replaySnapshot(ctx, st, out, cfg, snapshot); err != nil {
			return err
		}
	}
	return nil
}

func parseConfig(args []string, out io.Writer) (replayConfig, error) {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(out)

	var fromRaw, toRaw string
	filter := replay.SnapshotFilter{}
	fs.Int64Var(&filter.ID, "id", 0, "raw_snapshots.id to replay")
	fs.StringVar(&filter.Source, "source", model.SourceOddsAPI, "source name to replay")
	fs.StringVar(&fromRaw, "from", "", "inclusive received_at lower bound in RFC3339 format")
	fs.StringVar(&toRaw, "to", "", "inclusive received_at upper bound in RFC3339 format")
	fs.StringVar(&filter.SportKey, "sport-key", "", "optional sport key filter after decoding")
	fs.IntVar(&filter.Limit, "limit", defaultSnapshotLimit, "maximum snapshots to replay when --id is not provided")
	databaseURL := fs.String("database-url", envDefault("DATABASE_URL", defaultDatabaseURL), "PostgreSQL connection string")
	freshAfterSec := fs.Int("fresh-after-seconds", positiveIntEnvDefault("FRESH_AFTER_SECONDS", defaultFreshAfterSec), "freshness threshold in seconds")
	agingAfterSec := fs.Int("aging-after-seconds", positiveIntEnvDefault("AGING_AFTER_SECONDS", defaultAgingAfterSec), "aging threshold in seconds")

	if err := fs.Parse(args); err != nil {
		return replayConfig{}, err
	}
	if filter.ID < 0 {
		return replayConfig{}, errors.New("id must be non-negative")
	}
	filter.Source = strings.TrimSpace(filter.Source)
	if filter.Source == "" {
		filter.Source = model.SourceOddsAPI
	}
	filter.SportKey = strings.TrimSpace(filter.SportKey)
	if *freshAfterSec <= 0 {
		return replayConfig{}, errors.New("fresh-after-seconds must be positive")
	}
	if *agingAfterSec <= 0 {
		return replayConfig{}, errors.New("aging-after-seconds must be positive")
	}
	if *agingAfterSec < *freshAfterSec {
		return replayConfig{}, errors.New("aging-after-seconds must be >= fresh-after-seconds")
	}

	from, err := optionalRFC3339(fromRaw, "from")
	if err != nil {
		return replayConfig{}, err
	}
	to, err := optionalRFC3339(toRaw, "to")
	if err != nil {
		return replayConfig{}, err
	}
	if from != nil && to != nil && to.Before(*from) {
		return replayConfig{}, errors.New("to must be >= from")
	}
	filter.From = from
	filter.To = to

	dbURL := strings.TrimSpace(*databaseURL)
	if dbURL == "" {
		dbURL = defaultDatabaseURL
	}

	return replayConfig{
		DatabaseURL: dbURL,
		Filter:      filter,
		Freshness: normalize.FreshnessPolicy{
			FreshAfter: time.Duration(*freshAfterSec) * time.Second,
			AgingAfter: time.Duration(*agingAfterSec) * time.Second,
		},
	}, nil
}

func replaySnapshot(ctx context.Context, st *store.PostgresStore, out io.Writer, cfg replayConfig, snapshot replay.RawSnapshot) error {
	if snapshot.Source != model.SourceOddsAPI {
		return fmt.Errorf("raw snapshot %d has unsupported source %q: only %s snapshots are decoded today", snapshot.ID, snapshot.Source, model.SourceOddsAPI)
	}

	initial, err := replay.ReplayOddsAPISnapshot(snapshot, cfg.Freshness, cfg.Filter.SportKey, nil, nil)
	if err != nil {
		printSnapshotWarning(out, initial)
		return nil
	}
	if initial.SkippedBySportFilter {
		printSnapshotSummary(out, initial)
		return nil
	}

	history, err := st.ReplayQuoteHistory(ctx, snapshot.ID)
	if err != nil {
		return err
	}
	latest, err := st.ReplayLatestQuoteHashes(ctx, replay.QuoteKeys(initial))
	if err != nil {
		return err
	}

	report, err := replay.ReplayOddsAPISnapshot(snapshot, cfg.Freshness, cfg.Filter.SportKey, history, latest)
	if err != nil {
		printSnapshotWarning(out, report)
		return nil
	}
	printSnapshotSummary(out, report)
	return nil
}

func printSnapshotWarning(out io.Writer, report replay.SnapshotReport) {
	_, _ = fmt.Fprintf(out, "\nsnapshot_id=%d source=%s status_code=%d received_at=%s warning=%q\n",
		report.SnapshotID,
		report.Source,
		report.StatusCode,
		formatTime(report.ReceivedAt),
		report.DecodeOrReplayWarning,
	)
}

func printSnapshotSummary(out io.Writer, report replay.SnapshotReport) {
	_, _ = fmt.Fprintf(out, "\nsnapshot_id=%d source=%s sport_key=%s status_code=%d received_at=%s events=%d normalized_quotes=%d\n",
		report.SnapshotID,
		report.Source,
		report.SportKey,
		report.StatusCode,
		formatTime(report.ReceivedAt),
		report.EventCount,
		report.NormalizedQuoteCount,
	)
	if report.SkippedBySportFilter {
		_, _ = fmt.Fprintln(out, "skipped=true reason=sport_filter")
		return
	}
	_, _ = fmt.Fprintf(out, "history matches=%d hash_diffs=%d missing=%d stored_only=%d\n",
		report.HistoryMatches,
		report.HistoryHashDiffs,
		report.MissingFromHistory,
		report.StoredHistoryOnlyCnt,
	)
	_, _ = fmt.Fprintf(out, "latest matches=%d hash_diffs=%d missing=%d\n",
		report.LatestMatches,
		report.LatestHashDiffs,
		report.MissingFromLatest,
	)

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "quote_key\tquote_hash\tstored_history_hash\tlatest_quote_hash\tbookmaker\tmarket\toutcome\tside\thistory_status\tlatest_status")
	for _, quote := range report.Quotes {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			quote.QuoteKey,
			quote.QuoteHash,
			quote.StoredHistoryHash,
			quote.LatestQuoteHash,
			quote.BookmakerKey,
			quote.MarketKey,
			quote.OutcomeName,
			quote.OutcomeSide,
			quote.HistoryStatus,
			quote.LatestStatus,
		)
	}
	_ = tw.Flush()

	if len(report.StoredHistoryOnly) > 0 {
		_, _ = fmt.Fprintln(out, "stored_history_only:")
		tw = tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "quote_key\tquote_hash")
		for _, quote := range report.StoredHistoryOnly {
			_, _ = fmt.Fprintf(tw, "%s\t%s\n", quote.QuoteKey, quote.QuoteHash)
		}
		_ = tw.Flush()
	}
}

func optionalRFC3339(raw string, name string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	ts, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be RFC3339, for example 2026-05-06T12:00:00Z", name)
	}
	u := ts.UTC()
	return &u, nil
}

func positiveIntEnvDefault(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func envDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}
