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
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/betfair"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const (
	defaultDatabaseURL       = "postgres://pol:pol@localhost:5432/pol_odds?sslmode=disable"
	defaultFreshAfterSec     = 120
	defaultAgingAfterSec     = 300
	defaultTimeoutSeconds    = 60
	defaultBetfairReplayCode = 1
)

type replayConfig struct {
	Input          string
	Format         betfair.ReplayFormat
	StreamID       string
	Write          bool
	DatabaseURL    string
	Timeout        time.Duration
	FreshAfter     time.Duration
	AgingAfter     time.Duration
	MaxMessageSize int
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
		slog.Error("betfair replay failed", "error", err)
		os.Exit(defaultBetfairReplayCode)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	cfg, err := parseConfig(args, out)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	file, err := os.Open(cfg.Input)
	if err != nil {
		return fmt.Errorf("open betfair replay input %q: %w", cfg.Input, err)
	}
	defer func() { _ = file.Close() }()

	receivedAt := time.Now().UTC()
	result, err := betfair.ReplayStream(ctx, file, betfair.ReplayOptions{
		Format:         cfg.Format,
		StreamID:       cfg.StreamID,
		ReceivedAt:     receivedAt,
		MaxMessageSize: cfg.MaxMessageSize,
		QuoteOptions: betfair.QuoteEmitOptions{
			Now:        receivedAt,
			FreshAfter: cfg.FreshAfter,
			AgingAfter: cfg.AgingAfter,
		},
	})
	if err != nil {
		return err
	}
	result.Stats.DryRun = !cfg.Write

	if cfg.Write {
		st, err := store.Open(ctx, cfg.DatabaseURL, false, normalize.FreshnessPolicy{})
		if err != nil {
			return fmt.Errorf("open postgres for betfair replay write: %w", err)
		}
		defer func() { _ = st.Close() }()

		stats, err := st.SaveBetfairBatch(ctx, result.RawMessages, result.PersistenceCandidates)
		if err != nil {
			result.Stats.PersistErrors = 1
			return fmt.Errorf("persist betfair replay batch: %w", err)
		}
		result.Stats.RawMessagesInserted = stats.RawMessagesInserted
		result.Stats.CandidatesInserted = stats.CandidatesInserted
		result.Stats.CandidatesUnchanged = stats.CandidatesUnchanged
		result.Stats.CandidatesSkipped = stats.CandidatesSkipped
		result.Stats.CandidatesValid = stats.CandidatesSeen - stats.CandidatesSkipped
	}

	printReplayResult(out, cfg, result)
	return nil
}

func parseConfig(args []string, out io.Writer) (replayConfig, error) {
	fs := flag.NewFlagSet("betfair-replay", flag.ContinueOnError)
	fs.SetOutput(out)

	input := fs.String("input", "", "local Betfair replay input path")
	format := fs.String("format", string(betfair.ReplayFormatJSONL), "input format: jsonl or crlf")
	streamID := fs.String("stream-id", "", "optional replay/session label stored with raw messages")
	write := fs.Bool("write", false, "persist raw messages and quote candidates")
	persist := fs.Bool("persist", false, "alias for --write")
	databaseURL := fs.String("database-url", envDefault("DATABASE_URL", defaultDatabaseURL), "PostgreSQL connection string used only with --write")
	timeoutSeconds := fs.Int("timeout-seconds", defaultTimeoutSeconds, "command timeout in seconds")
	freshAfterSeconds := fs.Int("fresh-after-seconds", defaultFreshAfterSec, "Betfair replay freshness threshold in seconds")
	agingAfterSeconds := fs.Int("aging-after-seconds", defaultAgingAfterSec, "Betfair replay aging threshold in seconds")
	maxMessageSize := fs.Int("max-message-size", betfair.DefaultMaxMessageSize, "maximum Betfair stream message size in bytes")

	if err := fs.Parse(args); err != nil {
		return replayConfig{}, err
	}

	cfg := replayConfig{
		Input:          strings.TrimSpace(*input),
		Format:         betfair.ReplayFormat(strings.ToLower(strings.TrimSpace(*format))),
		StreamID:       strings.TrimSpace(*streamID),
		Write:          *write || *persist,
		DatabaseURL:    strings.TrimSpace(*databaseURL),
		Timeout:        time.Duration(*timeoutSeconds) * time.Second,
		FreshAfter:     time.Duration(*freshAfterSeconds) * time.Second,
		AgingAfter:     time.Duration(*agingAfterSeconds) * time.Second,
		MaxMessageSize: *maxMessageSize,
	}
	if cfg.Input == "" {
		return replayConfig{}, fmt.Errorf("input path is required")
	}
	if cfg.StreamID == "" {
		cfg.StreamID = filepath.Base(cfg.Input)
	}
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = defaultDatabaseURL
	}
	if cfg.Timeout <= 0 {
		return replayConfig{}, fmt.Errorf("timeout-seconds must be positive")
	}
	if cfg.FreshAfter <= 0 {
		return replayConfig{}, fmt.Errorf("fresh-after-seconds must be positive")
	}
	if cfg.AgingAfter <= 0 {
		return replayConfig{}, fmt.Errorf("aging-after-seconds must be positive")
	}
	if cfg.AgingAfter < cfg.FreshAfter {
		return replayConfig{}, fmt.Errorf("aging-after-seconds must be >= fresh-after-seconds")
	}
	if cfg.MaxMessageSize <= 0 {
		return replayConfig{}, fmt.Errorf("max-message-size must be positive")
	}
	switch cfg.Format {
	case betfair.ReplayFormatJSONL, betfair.ReplayFormatCRLF:
	default:
		return replayConfig{}, fmt.Errorf("unsupported format %q: expected jsonl or crlf", cfg.Format)
	}
	return cfg, nil
}

func printReplayResult(out io.Writer, cfg replayConfig, result betfair.ReplayResult) {
	_, _ = fmt.Fprintf(out, "betfair_replay dry_run=%t write_mode=%t input=%s format=%s stream_id=%s\n",
		!cfg.Write,
		cfg.Write,
		cfg.Input,
		cfg.Format,
		cfg.StreamID,
	)
	_, _ = fmt.Fprintf(out, "messages_read=%d messages_parsed=%d market_change_messages=%d control_messages=%d parse_errors=%d\n",
		result.Stats.MessagesRead,
		result.Stats.MessagesParsed,
		result.Stats.MarketChangeMessages,
		result.Stats.ControlMessages,
		result.Stats.ParseErrors,
	)
	_, _ = fmt.Fprintf(out, "raw_messages_ready=%d raw_messages_inserted=%d markets_seen=%d\n",
		result.Stats.RawMessagesReady,
		result.Stats.RawMessagesInserted,
		result.Stats.MarketsSeen,
	)
	_, _ = fmt.Fprintf(out, "candidates_emitted=%d candidates_valid=%d candidates_skipped=%d candidates_inserted=%d candidates_unchanged=%d persist_errors=%d duration_ms=%d\n",
		result.Stats.CandidatesEmitted,
		result.Stats.CandidatesValid,
		result.Stats.CandidatesSkipped,
		result.Stats.CandidatesInserted,
		result.Stats.CandidatesUnchanged,
		result.Stats.PersistErrors,
		result.Stats.Duration.Milliseconds(),
	)

	if len(result.Candidates) == 0 {
		return
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "quote_key\tquote_hash\tmarket_id\tselection_id\thandicap\tside\tprice_decimal\tavailable_size\tstale_state\tconfidence_score")
	for _, candidate := range result.Candidates {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%.4f\t%s\t%.6f\t%.6f\t%s\t%.2f\n",
			candidate.QuoteKey,
			candidate.QuoteHash,
			candidate.MarketID,
			candidate.SelectionID,
			candidate.Handicap,
			candidate.ExchangeSide,
			candidate.PriceDecimal,
			candidate.AvailableSize,
			candidate.StaleState,
			candidate.ConfidenceScore,
		)
	}
	_ = tw.Flush()
}

func envDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
