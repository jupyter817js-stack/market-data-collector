package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/betfair"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const (
	defaultPilotMaxMessages    = 20
	defaultPilotDuration       = 30 * time.Second
	defaultPilotDialTimeout    = 10 * time.Second
	defaultPilotReadTimeout    = 10 * time.Second
	defaultPilotWriteTimeout   = 10 * time.Second
	defaultPilotDBTimeout      = 10 * time.Second
	defaultPilotFlushTimeout   = 10 * time.Second
	defaultPilotHeartbeatMS    = 5000
	defaultPilotLadderLevels   = 3
	defaultPilotMaxMessageSize = betfair.DefaultMaxMessageSize
	defaultPilotMaxReconnects  = 0
	defaultPilotReconnectBase  = 250 * time.Millisecond
	defaultPilotReconnectMax   = 2 * time.Second
	defaultPilotDatabaseURLEnv = "DATABASE_URL"
	maxPilotMarketIDCount      = 5
	maxPilotDuration           = 2 * time.Minute
	maxPilotMessages           = 500
	pilotOutputText            = "text"
	pilotOutputJSON            = "json"
)

const (
	exitPilotSuccess = iota
)

const (
	exitPilotValidation = 2
	exitPilotCredential = 3
	exitPilotConnect    = 4
	exitPilotRuntime    = 5
	exitPilotCanceled   = 6
	exitPilotDatabase   = 7
)

const (
	pilotStopReasonPreflight      = "preflight_only"
	pilotStopReasonEnableRequired = "enable_live_required"
	pilotStopReasonPersist        = "persist_live_required"
	pilotFailureValidation        = "validation"
	pilotFailureCredential        = "credential"
	pilotFailureDatabase          = "database"
)

var (
	errPilotLiveDisabled = errors.New("betfair live pilot requires --enable-live")
	errPilotPersist      = errors.New("betfair live pilot requires --persist-live")
)

type pilotConfig struct {
	EnableLive      bool
	PersistLive     bool
	PreflightOnly   bool
	PostflightOnly  bool
	Output          string
	AppKeyEnv       string
	SessionTokenEnv string
	DatabaseURLEnv  string
	Address         string
	ServerName      string
	RunID           int64
	RunLabel        string
	StreamID        string
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	DBTimeout       time.Duration
	FlushTimeout    time.Duration
	HeartbeatMS     int
	ConflateMS      int
	MaxMessages     int
	Duration        time.Duration
	MarketIDs       []string
	LadderLevels    int
	MaxMessageSize  int
	MaxReconnects   int
	ReconnectBase   time.Duration
	ReconnectMax    time.Duration
}

type pilotReport struct {
	Enabled               bool     `json:"enabled"`
	PersistLive           bool     `json:"persist_live"`
	PreflightOnly         bool     `json:"preflight_only"`
	PostflightOnly        bool     `json:"postflight_only"`
	RunID                 int64    `json:"run_id"`
	RunLabel              string   `json:"run_label"`
	AddressRedacted       string   `json:"address_redacted"`
	ServerName            string   `json:"server_name"`
	MarketIDsCount        int      `json:"market_ids_count"`
	MaxMessages           int      `json:"max_messages"`
	DurationLimit         string   `json:"duration_limit"`
	Connected             bool     `json:"connected"`
	Authenticated         bool     `json:"authenticated"`
	Subscribed            bool     `json:"subscribed"`
	MessagesRead          int      `json:"messages_read"`
	MarketChangeMessages  int      `json:"market_change_messages"`
	HeartbeatMessages     int      `json:"heartbeat_messages"`
	StatusMessages        int      `json:"status_messages"`
	RawMessagesSeen       int      `json:"raw_messages_seen"`
	RawMessagesInserted   int      `json:"raw_messages_inserted"`
	CandidatesEmitted     int      `json:"candidates_emitted"`
	CandidatesInserted    int      `json:"candidates_inserted"`
	CandidatesUnchanged   int      `json:"candidates_unchanged"`
	CandidatesSkipped     int      `json:"candidates_skipped"`
	MarketsSeen           int      `json:"markets_seen"`
	LastClock             string   `json:"last_clk"`
	LastInitialClock      string   `json:"last_initial_clk"`
	LastMessageTime       string   `json:"last_message_time"`
	HeartbeatState        string   `json:"heartbeat_state"`
	StopReason            string   `json:"stop_reason"`
	FailureReason         string   `json:"failure_reason"`
	ElapsedMS             int64    `json:"elapsed_ms"`
	LinkedRawMessageCount int64    `json:"linked_raw_message_count"`
	QuoteHistoryRows      int64    `json:"quote_history_rows"`
	LatestQuoteCount      int64    `json:"latest_quote_count"`
	MappedQuoteCount      int64    `json:"mapped_quote_count"`
	ReadyRunnerBookCount  int64    `json:"ready_runner_book_count"`
	ReadyComparisonCount  int64    `json:"ready_comparison_count"`
	Warnings              []string `json:"warnings,omitempty"`
}

type pilotEnvLookup func(string) (string, bool)
type pilotTransportFactory func(betfair.TLSLiveTransportOptions) betfair.LiveTransport
type pilotStoreFactory func(context.Context, string) (pilotStore, error)

type pilotStore interface {
	SaveBetfairBatch(context.Context, []model.BetfairRawStreamMessage, []model.BetfairExchangeQuoteCandidate) (model.BetfairSaveStats, error)
	StartBetfairLivePilotRun(context.Context, model.BetfairLivePilotRunStart) (int64, error)
	FinishBetfairLivePilotRun(context.Context, int64, model.BetfairLivePilotRunFinish) error
	GetBetfairLivePilotPostflight(context.Context, int64) (model.BetfairLivePilotPostflight, error)
	Close() error
}

type observedPilotMessage struct {
	Message    betfair.Message
	ReceivedAt time.Time
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	code := runCommand(ctx, os.Args[1:], os.Stdout, os.LookupEnv, defaultTransportFactory, defaultStoreFactory)
	if code != exitPilotSuccess {
		os.Exit(code)
	}
}

func runCommand(ctx context.Context, args []string, out io.Writer, lookup pilotEnvLookup, transportFactory pilotTransportFactory, storeFactory pilotStoreFactory) int {
	cfg, err := parsePilotConfig(args, out)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitPilotSuccess
		}
		report := pilotReport{
			Enabled:        hasFlag(args, "enable-live"),
			PersistLive:    hasFlag(args, "persist-live"),
			PreflightOnly:  hasFlag(args, "preflight-only"),
			PostflightOnly: hasFlag(args, "postflight-only"),
			StopReason:     betfair.LiveStopReasonFailed,
			FailureReason:  redactedPilotError(err, "", "", ""),
		}
		_ = writePilotReport(out, report, outputFromArgs(args))
		return exitPilotValidation
	}

	report, err := runPilot(ctx, cfg, lookup, transportFactory, storeFactory)
	_ = writePilotReport(out, report, cfg.Output)
	return exitCodeForPilotError(err)
}

func runPilot(ctx context.Context, cfg pilotConfig, lookup pilotEnvLookup, transportFactory pilotTransportFactory, storeFactory pilotStoreFactory) (pilotReport, error) {
	startedAt := time.Now().UTC()
	report := newPilotReport(cfg)

	if cfg.PostflightOnly {
		return runPostflight(ctx, cfg, lookup, storeFactory)
	}
	if !cfg.EnableLive {
		report.StopReason = pilotStopReasonEnableRequired
		report.FailureReason = errPilotLiveDisabled.Error()
		return report, errPilotLiveDisabled
	}
	if !cfg.PersistLive {
		report.StopReason = pilotStopReasonPersist
		report.FailureReason = errPilotPersist.Error()
		return report, errPilotPersist
	}
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if transportFactory == nil {
		transportFactory = defaultTransportFactory
	}
	if storeFactory == nil {
		storeFactory = defaultStoreFactory
	}

	appKey, sessionToken, databaseURL, err := readPilotInputs(cfg, lookup)
	if err != nil {
		report.StopReason = betfair.LiveStopReasonFailed
		report.FailureReason = redactedPilotError(err, "", "", "")
		return report, pilotCredentialError{err: err}
	}
	report.AddressRedacted = redactPilotString(report.AddressRedacted, appKey, sessionToken, databaseURL)
	if cfg.PreflightOnly {
		report.StopReason = pilotStopReasonPreflight
		report.ElapsedMS = time.Since(startedAt).Milliseconds()
		return report, nil
	}
	if err := ctx.Err(); err != nil {
		report.StopReason = liveStopReasonFromContext(err)
		return report, pilotCanceledError{err: err}
	}

	openCtx, cancelOpen := context.WithTimeout(ctx, cfg.DBTimeout)
	pilotStore, err := storeFactory(openCtx, databaseURL)
	cancelOpen()
	if err != nil {
		report.StopReason = betfair.LiveStopReasonFailed
		report.FailureReason = redactedPilotError(err, appKey, sessionToken, databaseURL)
		return report, pilotDatabaseError{err: err}
	}
	defer func() { _ = pilotStore.Close() }()

	startInput := model.BetfairLivePilotRunStart{
		RunLabel:             cfg.RunLabel,
		StreamID:             cfg.StreamID,
		StartedAt:            startedAt,
		AddressRedacted:      redactPilotString(cfg.Address, appKey, sessionToken, databaseURL),
		ServerName:           cfg.ServerName,
		MarketIDs:            cfg.MarketIDs,
		DurationLimitSeconds: durationSecondsPtr(cfg.Duration),
		MaxMessages:          intPtr(cfg.MaxMessages),
	}
	startCtx, cancelStart := context.WithTimeout(context.Background(), cfg.FlushTimeout)
	runID, err := pilotStore.StartBetfairLivePilotRun(startCtx, startInput)
	cancelStart()
	if err != nil {
		report.StopReason = betfair.LiveStopReasonFailed
		report.FailureReason = redactedPilotError(err, appKey, sessionToken, databaseURL)
		return report, pilotDatabaseError{err: err}
	}
	report.RunID = runID

	observed := make([]observedPilotMessage, 0, cfg.MaxMessages+2)
	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	transport := transportFactory(betfair.TLSLiveTransportOptions{
		Address:      cfg.Address,
		ServerName:   cfg.ServerName,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
	connector := betfair.NewLiveConnector(transport, betfair.LiveConnectorOptions{
		Enabled:      true,
		AppKey:       appKey,
		SessionToken: sessionToken,
		Subscription: betfair.MarketSubscriptionRequest{
			MarketIDs:    cfg.MarketIDs,
			Fields:       betfair.DefaultMarketDataFields(),
			LadderLevels: cfg.LadderLevels,
			HeartbeatMS:  cfg.HeartbeatMS,
			ConflateMS:   cfg.ConflateMS,
		},
		HeartbeatAgingAfter:  time.Duration(cfg.HeartbeatMS*2) * time.Millisecond,
		HeartbeatStaleAfter:  time.Duration(cfg.HeartbeatMS*3) * time.Millisecond,
		MaxReconnectAttempts: cfg.MaxReconnects,
		ReconnectBaseDelay:   cfg.ReconnectBase,
		ReconnectMaxDelay:    cfg.ReconnectMax,
		MaxMessageSize:       cfg.MaxMessageSize,
		MaxMessages:          cfg.MaxMessages,
		OnMessage: func(msg betfair.Message, receivedAt time.Time) error {
			observed = append(observed, observedPilotMessage{Message: msg, ReceivedAt: receivedAt.UTC()})
			return nil
		},
	})

	result, runErr := connector.Run(ctx)
	report = reportFromLiveResult(cfg, result, time.Since(startedAt))
	report.AddressRedacted = redactPilotString(report.AddressRedacted, appKey, sessionToken, databaseURL)
	report.RunID = runID
	report.RawMessagesSeen = len(observed)

	var returnErr error
	shouldFlush := runErr == nil || errors.Is(runErr, context.DeadlineExceeded) || errors.Is(runErr, context.Canceled)
	if shouldFlush {
		stats, flushErr := flushPilotBatch(cfg, pilotStore, observed, result.Candidates)
		report.RawMessagesInserted = stats.RawMessagesInserted
		report.CandidatesInserted = stats.CandidatesInserted
		report.CandidatesUnchanged = stats.CandidatesUnchanged
		report.CandidatesSkipped = stats.CandidatesSkipped
		if flushErr != nil {
			report.StopReason = betfair.LiveStopReasonFailed
			report.FailureReason = redactedPilotError(flushErr, appKey, sessionToken, databaseURL)
			returnErr = pilotDatabaseError{err: flushErr}
		}
	}

	if returnErr == nil {
		switch {
		case errors.Is(runErr, context.DeadlineExceeded):
			report.StopReason = "duration"
		case errors.Is(runErr, context.Canceled):
			report.StopReason = betfair.LiveStopReasonContextCanceled
			returnErr = pilotCanceledError{err: runErr}
		case runErr != nil:
			report.FailureReason = redactedPilotError(runErr, appKey, sessionToken, databaseURL)
			if isConnectOrHandshakeFailure(runErr) {
				returnErr = pilotConnectError{err: runErr}
			} else {
				returnErr = pilotRuntimeError{err: runErr}
			}
		default:
			returnErr = nil
		}
	}

	finishInput := finishInputFromReport(report, exitCodeForPilotError(returnErr))
	finishCtx, cancelFinish := context.WithTimeout(context.Background(), cfg.FlushTimeout)
	finishErr := pilotStore.FinishBetfairLivePilotRun(finishCtx, runID, finishInput)
	cancelFinish()
	if finishErr != nil {
		report.FailureReason = redactedPilotError(finishErr, appKey, sessionToken, databaseURL)
		return report, pilotDatabaseError{err: finishErr}
	}
	return report, returnErr
}

func runPostflight(ctx context.Context, cfg pilotConfig, lookup pilotEnvLookup, storeFactory pilotStoreFactory) (pilotReport, error) {
	startedAt := time.Now().UTC()
	report := newPilotReport(cfg)
	report.StopReason = "postflight_only"
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if storeFactory == nil {
		storeFactory = defaultStoreFactory
	}
	databaseURL, ok := lookup(cfg.DatabaseURLEnv)
	if !ok || strings.TrimSpace(databaseURL) == "" {
		report.StopReason = betfair.LiveStopReasonFailed
		report.FailureReason = fmt.Sprintf("environment variable named by --database-url-env (%s) is not set or empty", cfg.DatabaseURLEnv)
		return report, pilotCredentialError{err: errors.New(report.FailureReason)}
	}
	if err := ctx.Err(); err != nil {
		report.StopReason = liveStopReasonFromContext(err)
		return report, pilotCanceledError{err: err}
	}

	openCtx, cancelOpen := context.WithTimeout(ctx, cfg.DBTimeout)
	pilotStore, err := storeFactory(openCtx, strings.TrimSpace(databaseURL))
	cancelOpen()
	if err != nil {
		report.StopReason = betfair.LiveStopReasonFailed
		report.FailureReason = redactedPilotError(err, "", "", databaseURL)
		return report, pilotDatabaseError{err: err}
	}
	defer func() { _ = pilotStore.Close() }()

	postflight, err := pilotStore.GetBetfairLivePilotPostflight(ctx, cfg.RunID)
	if err != nil {
		report.StopReason = betfair.LiveStopReasonFailed
		report.FailureReason = redactedPilotError(err, "", "", databaseURL)
		return report, pilotDatabaseError{err: err}
	}
	report = reportFromPostflight(cfg, postflight, time.Since(startedAt))
	return report, nil
}

func parsePilotConfig(args []string, out io.Writer) (pilotConfig, error) {
	fs := flag.NewFlagSet("betfair-live-pilot", flag.ContinueOnError)
	fs.SetOutput(out)

	marketIDs := marketIDFlag{}
	cfg := pilotConfig{}
	fs.BoolVar(&cfg.EnableLive, "enable-live", false, "explicitly enable the manual Betfair live persistence pilot")
	fs.BoolVar(&cfg.PersistLive, "persist-live", false, "explicitly enable Betfair live persistence for this bounded pilot")
	fs.BoolVar(&cfg.PreflightOnly, "preflight-only", false, "validate pilot configuration and required env values without connecting or persisting")
	fs.BoolVar(&cfg.PostflightOnly, "postflight-only", false, "read a completed pilot run audit report without connecting to Betfair")
	fs.StringVar(&cfg.Output, "output", pilotOutputText, "output format: text or json")
	fs.StringVar(&cfg.AppKeyEnv, "app-key-env", "", "environment variable name containing the Betfair app key")
	fs.StringVar(&cfg.SessionTokenEnv, "session-token-env", "", "environment variable name containing the Betfair session token")
	fs.StringVar(&cfg.DatabaseURLEnv, "database-url-env", defaultPilotDatabaseURLEnv, "environment variable name containing the PostgreSQL database URL")
	fs.StringVar(&cfg.Address, "address", "", "Betfair stream address as host:port; required when live persistence is enabled")
	fs.StringVar(&cfg.ServerName, "server-name", "", "TLS server name; required when live persistence is enabled")
	fs.Int64Var(&cfg.RunID, "run-id", 0, "Betfair live pilot run ID for postflight audit")
	fs.StringVar(&cfg.RunLabel, "run-label", "", "optional operator label for the live pilot run")
	fs.StringVar(&cfg.StreamID, "stream-id", "", "optional stream/session label stored with raw Betfair messages")
	fs.DurationVar(&cfg.DialTimeout, "dial-timeout", defaultPilotDialTimeout, "TLS dial timeout")
	fs.DurationVar(&cfg.ReadTimeout, "read-timeout", defaultPilotReadTimeout, "per-read timeout")
	fs.DurationVar(&cfg.WriteTimeout, "write-timeout", defaultPilotWriteTimeout, "per-write timeout")
	fs.DurationVar(&cfg.DBTimeout, "db-timeout", defaultPilotDBTimeout, "database open timeout")
	fs.DurationVar(&cfg.FlushTimeout, "flush-timeout", defaultPilotFlushTimeout, "final persistence flush timeout")
	fs.IntVar(&cfg.HeartbeatMS, "heartbeat-ms", defaultPilotHeartbeatMS, "Betfair stream heartbeat interval in milliseconds")
	fs.IntVar(&cfg.ConflateMS, "conflate-ms", 0, "Betfair stream conflate interval in milliseconds")
	fs.IntVar(&cfg.MaxMessages, "max-messages", defaultPilotMaxMessages, "maximum post-subscription stream messages to read")
	fs.DurationVar(&cfg.Duration, "duration", defaultPilotDuration, "maximum pilot command duration")
	fs.Var(&marketIDs, "market-id", "explicit Betfair market ID; may be repeated or comma-separated")
	fs.IntVar(&cfg.LadderLevels, "ladder-levels", defaultPilotLadderLevels, "Betfair price ladder depth")
	fs.IntVar(&cfg.MaxMessageSize, "max-message-size", defaultPilotMaxMessageSize, "maximum Betfair stream message size in bytes")
	fs.IntVar(&cfg.MaxReconnects, "max-reconnects", defaultPilotMaxReconnects, "maximum reconnect attempts for the manual pilot run")
	fs.DurationVar(&cfg.ReconnectBase, "reconnect-base-delay", defaultPilotReconnectBase, "base reconnect delay")
	fs.DurationVar(&cfg.ReconnectMax, "reconnect-max-delay", defaultPilotReconnectMax, "maximum reconnect delay")

	if err := fs.Parse(args); err != nil {
		return pilotConfig{}, err
	}

	cfg.Output = strings.ToLower(strings.TrimSpace(cfg.Output))
	cfg.AppKeyEnv = strings.TrimSpace(cfg.AppKeyEnv)
	cfg.SessionTokenEnv = strings.TrimSpace(cfg.SessionTokenEnv)
	cfg.DatabaseURLEnv = strings.TrimSpace(cfg.DatabaseURLEnv)
	cfg.Address = strings.TrimSpace(cfg.Address)
	cfg.ServerName = strings.TrimSpace(cfg.ServerName)
	cfg.RunLabel = strings.TrimSpace(cfg.RunLabel)
	cfg.StreamID = strings.TrimSpace(cfg.StreamID)
	cfg.MarketIDs = marketIDs.values()

	if cfg.Output == "" {
		cfg.Output = pilotOutputText
	}
	if cfg.Output != pilotOutputText && cfg.Output != pilotOutputJSON {
		return pilotConfig{}, fmt.Errorf("output must be text or json")
	}
	if cfg.RunLabel != "" {
		if err := validateRunLabel(cfg.RunLabel); err != nil {
			return pilotConfig{}, err
		}
	}
	if cfg.PostflightOnly {
		if cfg.DatabaseURLEnv == "" {
			return pilotConfig{}, fmt.Errorf("database-url-env is required when --postflight-only is set")
		}
		if cfg.RunID <= 0 {
			return pilotConfig{}, fmt.Errorf("run-id is required when --postflight-only is set")
		}
		return cfg, nil
	}
	if !cfg.EnableLive || !cfg.PersistLive {
		return cfg, nil
	}
	if cfg.AppKeyEnv == "" {
		return pilotConfig{}, fmt.Errorf("app-key-env is required when --enable-live and --persist-live are set")
	}
	if cfg.SessionTokenEnv == "" {
		return pilotConfig{}, fmt.Errorf("session-token-env is required when --enable-live and --persist-live are set")
	}
	if cfg.DatabaseURLEnv == "" {
		return pilotConfig{}, fmt.Errorf("database-url-env is required when --enable-live and --persist-live are set")
	}
	if cfg.Address == "" {
		return pilotConfig{}, fmt.Errorf("address is required when --enable-live and --persist-live are set")
	}
	if cfg.ServerName == "" {
		return pilotConfig{}, fmt.Errorf("server-name is required when --enable-live and --persist-live are set")
	}
	if len(cfg.MarketIDs) == 0 {
		return pilotConfig{}, fmt.Errorf("at least one market-id is required when --enable-live and --persist-live are set")
	}
	if len(cfg.MarketIDs) > maxPilotMarketIDCount {
		return pilotConfig{}, fmt.Errorf("too many market IDs for live pilot: got %d max %d", len(cfg.MarketIDs), maxPilotMarketIDCount)
	}
	if cfg.DialTimeout <= 0 || cfg.ReadTimeout <= 0 || cfg.WriteTimeout <= 0 {
		return pilotConfig{}, fmt.Errorf("dial-timeout, read-timeout, and write-timeout must be positive")
	}
	if cfg.DBTimeout <= 0 || cfg.FlushTimeout <= 0 {
		return pilotConfig{}, fmt.Errorf("db-timeout and flush-timeout must be positive")
	}
	if cfg.HeartbeatMS <= 0 {
		return pilotConfig{}, fmt.Errorf("heartbeat-ms must be positive")
	}
	if cfg.ConflateMS < 0 {
		return pilotConfig{}, fmt.Errorf("conflate-ms must be non-negative")
	}
	if cfg.MaxMessages <= 0 {
		return pilotConfig{}, fmt.Errorf("max-messages must be positive")
	}
	if cfg.MaxMessages > maxPilotMessages {
		return pilotConfig{}, fmt.Errorf("max-messages exceeds pilot limit: got %d max %d", cfg.MaxMessages, maxPilotMessages)
	}
	if cfg.Duration <= 0 {
		return pilotConfig{}, fmt.Errorf("duration must be positive")
	}
	if cfg.Duration > maxPilotDuration {
		return pilotConfig{}, fmt.Errorf("duration exceeds pilot limit: got %s max %s", cfg.Duration, maxPilotDuration)
	}
	if cfg.LadderLevels < 0 {
		return pilotConfig{}, fmt.Errorf("ladder-levels must be non-negative")
	}
	if cfg.MaxMessageSize <= 0 {
		return pilotConfig{}, fmt.Errorf("max-message-size must be positive")
	}
	if cfg.MaxReconnects < 0 {
		return pilotConfig{}, fmt.Errorf("max-reconnects must be non-negative")
	}
	if cfg.ReconnectBase <= 0 || cfg.ReconnectMax <= 0 {
		return pilotConfig{}, fmt.Errorf("reconnect delays must be positive")
	}
	if cfg.ReconnectMax < cfg.ReconnectBase {
		return pilotConfig{}, fmt.Errorf("reconnect-max-delay must be >= reconnect-base-delay")
	}
	if cfg.StreamID == "" {
		cfg.StreamID = fmt.Sprintf("betfair-live-pilot-%s", time.Now().UTC().Format("20060102T150405Z"))
	}
	return cfg, nil
}

func readPilotInputs(cfg pilotConfig, lookup pilotEnvLookup) (string, string, string, error) {
	appKey, ok := lookup(cfg.AppKeyEnv)
	if !ok || strings.TrimSpace(appKey) == "" {
		return "", "", "", fmt.Errorf("environment variable named by --app-key-env (%s) is not set or empty", cfg.AppKeyEnv)
	}
	sessionToken, ok := lookup(cfg.SessionTokenEnv)
	if !ok || strings.TrimSpace(sessionToken) == "" {
		return "", "", "", fmt.Errorf("environment variable named by --session-token-env (%s) is not set or empty", cfg.SessionTokenEnv)
	}
	databaseURL, ok := lookup(cfg.DatabaseURLEnv)
	if !ok || strings.TrimSpace(databaseURL) == "" {
		return "", "", "", fmt.Errorf("environment variable named by --database-url-env (%s) is not set or empty", cfg.DatabaseURLEnv)
	}
	return strings.TrimSpace(appKey), strings.TrimSpace(sessionToken), strings.TrimSpace(databaseURL), nil
}

func validateRunLabel(label string) error {
	if len(label) > 80 {
		return fmt.Errorf("run-label must be 80 characters or fewer")
	}
	for _, r := range label {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' ||
			r == '_' ||
			r == '.' {
			continue
		}
		return fmt.Errorf("run-label may contain only letters, numbers, dot, dash, and underscore")
	}
	return nil
}

func flushPilotBatch(cfg pilotConfig, saver pilotStore, observed []observedPilotMessage, candidates []betfair.QuoteCandidate) (model.BetfairSaveStats, error) {
	stats := model.BetfairSaveStats{}
	if saver == nil {
		return stats, fmt.Errorf("betfair live pilot store is nil")
	}
	receivedAt := time.Now().UTC()
	raw := make([]model.BetfairRawStreamMessage, 0, len(observed))
	for _, item := range observed {
		raw = append(raw, item.Message.RawStreamMessage(cfg.StreamID, item.ReceivedAt))
	}
	persistenceCandidates := make([]model.BetfairExchangeQuoteCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		persistenceCandidates = append(persistenceCandidates, candidate.PersistenceModel(receivedAt))
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), cfg.FlushTimeout)
	defer cancel()
	return saver.SaveBetfairBatch(flushCtx, raw, persistenceCandidates)
}

func durationSecondsPtr(d time.Duration) *int {
	if d <= 0 {
		return nil
	}
	seconds := int(d.Seconds())
	if seconds <= 0 {
		seconds = 1
	}
	return &seconds
}

func intPtr(v int) *int {
	return &v
}

func newPilotReport(cfg pilotConfig) pilotReport {
	return pilotReport{
		Enabled:         cfg.EnableLive,
		PersistLive:     cfg.PersistLive,
		PreflightOnly:   cfg.PreflightOnly,
		PostflightOnly:  cfg.PostflightOnly,
		RunID:           cfg.RunID,
		RunLabel:        cfg.RunLabel,
		AddressRedacted: cfg.Address,
		ServerName:      cfg.ServerName,
		MarketIDsCount:  len(cfg.MarketIDs),
		MaxMessages:     cfg.MaxMessages,
		DurationLimit:   cfg.Duration.String(),
	}
}

func reportFromLiveResult(cfg pilotConfig, result betfair.LiveRunResult, elapsed time.Duration) pilotReport {
	report := newPilotReport(cfg)
	report.Connected = result.ConnectionAttempts > 0
	report.Authenticated = result.MessagesParsed >= 1 && result.State != betfair.LiveStateDisabled && result.State != betfair.LiveStateFailed
	report.Subscribed = result.MessagesParsed >= 2 && result.State != betfair.LiveStateDisabled && result.State != betfair.LiveStateFailed
	report.MessagesRead = result.MessagesParsed
	report.MarketChangeMessages = result.MarketChangeMessages
	report.HeartbeatMessages = result.HeartbeatMessages
	report.StatusMessages = result.ControlMessages
	report.CandidatesEmitted = len(result.Candidates)
	report.MarketsSeen = result.MarketsSeen
	report.LastClock = result.LatestClock
	report.LastInitialClock = result.InitialClock
	if result.LastMessageTime != nil {
		report.LastMessageTime = result.LastMessageTime.UTC().Format(time.RFC3339)
	}
	report.HeartbeatState = string(result.HeartbeatState)
	report.StopReason = strings.TrimSpace(result.StopReason)
	if report.StopReason == "" {
		report.StopReason = "completed"
	}
	report.ElapsedMS = elapsed.Milliseconds()
	return report
}

func reportFromPostflight(cfg pilotConfig, postflight model.BetfairLivePilotPostflight, elapsed time.Duration) pilotReport {
	run := postflight.Run
	report := newPilotReport(cfg)
	report.PostflightOnly = true
	report.RunID = run.ID
	report.RunLabel = run.RunLabel
	report.AddressRedacted = run.AddressRedacted
	report.ServerName = run.ServerName
	report.MarketIDsCount = run.MarketIDsCount
	if run.MaxMessages != nil {
		report.MaxMessages = *run.MaxMessages
	}
	if run.DurationLimitSeconds != nil {
		report.DurationLimit = (time.Duration(*run.DurationLimitSeconds) * time.Second).String()
	}
	report.MessagesRead = run.MessagesRead
	report.MarketChangeMessages = run.MarketChangeMessages
	report.HeartbeatMessages = run.HeartbeatMessages
	report.StatusMessages = run.StatusMessages
	report.RawMessagesSeen = run.RawMessagesSeen
	report.RawMessagesInserted = run.RawMessagesInserted
	report.CandidatesEmitted = run.CandidatesEmitted
	report.CandidatesInserted = run.CandidatesInserted
	report.CandidatesUnchanged = run.CandidatesUnchanged
	report.CandidatesSkipped = run.CandidatesSkipped
	report.MarketsSeen = run.MarketsSeen
	report.LastClock = run.LastClock
	report.LastInitialClock = run.LastInitialClock
	if run.LastMessageTime != nil {
		report.LastMessageTime = run.LastMessageTime.UTC().Format(time.RFC3339)
	}
	report.HeartbeatState = run.HeartbeatState
	report.StopReason = "postflight_only"
	report.FailureReason = run.FailureReason
	report.ElapsedMS = elapsed.Milliseconds()
	report.LinkedRawMessageCount = postflight.LinkedRawMessageCount
	report.QuoteHistoryRows = postflight.QuoteHistoryRows
	report.LatestQuoteCount = postflight.LatestQuoteCount
	report.MappedQuoteCount = postflight.MappedQuoteCount
	report.ReadyRunnerBookCount = postflight.ReadyRunnerBookCount
	report.ReadyComparisonCount = postflight.ReadyComparisonCount
	report.Warnings = append([]string(nil), postflight.Warnings...)
	return report
}

func finishInputFromReport(report pilotReport, exitCode int) model.BetfairLivePilotRunFinish {
	status := model.BetfairLivePilotRunStatusSuccess
	if exitCode == exitPilotCanceled {
		status = model.BetfairLivePilotRunStatusCanceled
	} else if exitCode != exitPilotSuccess {
		status = model.BetfairLivePilotRunStatusFailed
	}
	var lastMessageTime *time.Time
	if strings.TrimSpace(report.LastMessageTime) != "" {
		if ts, err := time.Parse(time.RFC3339, report.LastMessageTime); err == nil {
			ts = ts.UTC()
			lastMessageTime = &ts
		}
	}
	return model.BetfairLivePilotRunFinish{
		FinishedAt:           time.Now().UTC(),
		Status:               status,
		StopReason:           report.StopReason,
		FailureReason:        report.FailureReason,
		MessagesRead:         report.MessagesRead,
		MarketChangeMessages: report.MarketChangeMessages,
		HeartbeatMessages:    report.HeartbeatMessages,
		StatusMessages:       report.StatusMessages,
		RawMessagesSeen:      report.RawMessagesSeen,
		RawMessagesInserted:  report.RawMessagesInserted,
		CandidatesEmitted:    report.CandidatesEmitted,
		CandidatesInserted:   report.CandidatesInserted,
		CandidatesUnchanged:  report.CandidatesUnchanged,
		CandidatesSkipped:    report.CandidatesSkipped,
		MarketsSeen:          report.MarketsSeen,
		LastClock:            report.LastClock,
		LastInitialClock:     report.LastInitialClock,
		LastMessageTime:      lastMessageTime,
		HeartbeatState:       report.HeartbeatState,
		ExitCode:             exitCode,
		ElapsedMS:            report.ElapsedMS,
	}
}

func writePilotReport(out io.Writer, report pilotReport, format string) error {
	switch format {
	case pilotOutputJSON:
		payload, err := json.Marshal(report)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(payload))
		return err
	default:
		return writeTextPilotReport(out, report)
	}
}

func writeTextPilotReport(out io.Writer, report pilotReport) error {
	_, err := fmt.Fprintf(out, "betfair_live_pilot enabled=%t persist_live=%t preflight_only=%t postflight_only=%t persistence=%t collector_integration=false market_stream_only=true run_id=%d run_label=%s address=%s server_name=%s market_ids_count=%d\n",
		report.Enabled,
		report.PersistLive,
		report.PreflightOnly,
		report.PostflightOnly,
		report.PersistLive,
		report.RunID,
		report.RunLabel,
		report.AddressRedacted,
		report.ServerName,
		report.MarketIDsCount,
	)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "max_messages=%d duration_limit=%s connected=%t authenticated=%t subscribed=%t stop_reason=%s failure_reason=%s\n",
		report.MaxMessages,
		report.DurationLimit,
		report.Connected,
		report.Authenticated,
		report.Subscribed,
		report.StopReason,
		report.FailureReason,
	)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "messages_read=%d market_change_messages=%d heartbeat_messages=%d status_messages=%d raw_messages_seen=%d raw_messages_inserted=%d markets_seen=%d\n",
		report.MessagesRead,
		report.MarketChangeMessages,
		report.HeartbeatMessages,
		report.StatusMessages,
		report.RawMessagesSeen,
		report.RawMessagesInserted,
		report.MarketsSeen,
	)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "candidates_emitted=%d candidates_inserted=%d candidates_unchanged=%d candidates_skipped=%d last_clk=%s last_initial_clk=%s last_message_time=%s heartbeat_state=%s elapsed_ms=%d\n",
		report.CandidatesEmitted,
		report.CandidatesInserted,
		report.CandidatesUnchanged,
		report.CandidatesSkipped,
		report.LastClock,
		report.LastInitialClock,
		report.LastMessageTime,
		report.HeartbeatState,
		report.ElapsedMS,
	)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "linked_raw_message_count=%d quote_history_rows=%d latest_quote_count=%d mapped_quote_count=%d ready_runner_book_count=%d ready_comparison_count=%d warnings=%s\n",
		report.LinkedRawMessageCount,
		report.QuoteHistoryRows,
		report.LatestQuoteCount,
		report.MappedQuoteCount,
		report.ReadyRunnerBookCount,
		report.ReadyComparisonCount,
		strings.Join(report.Warnings, "|"),
	)
	return err
}

func defaultTransportFactory(options betfair.TLSLiveTransportOptions) betfair.LiveTransport {
	return betfair.NewTLSLiveTransport(options)
}

func defaultStoreFactory(ctx context.Context, databaseURL string) (pilotStore, error) {
	return store.Open(ctx, databaseURL, true, normalize.FreshnessPolicy{})
}

func exitCodeForPilotError(err error) int {
	if err == nil {
		return exitPilotSuccess
	}
	if errors.Is(err, errPilotLiveDisabled) || errors.Is(err, errPilotPersist) {
		return exitPilotValidation
	}
	var credential pilotCredentialError
	if errors.As(err, &credential) {
		return exitPilotCredential
	}
	var connect pilotConnectError
	if errors.As(err, &connect) {
		return exitPilotConnect
	}
	var runtime pilotRuntimeError
	if errors.As(err, &runtime) {
		return exitPilotRuntime
	}
	var canceled pilotCanceledError
	if errors.As(err, &canceled) || errors.Is(err, context.Canceled) {
		return exitPilotCanceled
	}
	var database pilotDatabaseError
	if errors.As(err, &database) {
		return exitPilotDatabase
	}
	return exitPilotRuntime
}

func isConnectOrHandshakeFailure(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "connect") ||
		strings.Contains(text, "handshake") ||
		strings.Contains(text, "authentication failed") ||
		strings.Contains(text, "subscription failed") ||
		strings.Contains(text, "max reconnect")
}

func redactedPilotError(err error, appKey, sessionToken, databaseURL string) string {
	if err == nil {
		return ""
	}
	return redactPilotString(err.Error(), appKey, sessionToken, databaseURL)
}

func redactPilotString(s, appKey, sessionToken, databaseURL string) string {
	out := betfair.RedactLiveString(s, appKey, sessionToken)
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		return out
	}
	out = strings.ReplaceAll(out, databaseURL, "[REDACTED_DATABASE_URL]")
	if parsed, err := url.Parse(databaseURL); err == nil {
		if parsed.User != nil {
			if username := parsed.User.Username(); username != "" {
				out = strings.ReplaceAll(out, username, "[REDACTED_DATABASE_USER]")
			}
			if password, ok := parsed.User.Password(); ok && password != "" {
				out = strings.ReplaceAll(out, password, "[REDACTED_DATABASE_PASSWORD]")
			}
		}
	}
	return out
}

func outputFromArgs(args []string) string {
	for i, arg := range args {
		if arg == "--output" && i+1 < len(args) {
			value := strings.ToLower(strings.TrimSpace(args[i+1]))
			if value == pilotOutputJSON {
				return pilotOutputJSON
			}
		}
		if strings.HasPrefix(arg, "--output=") {
			value := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(arg, "--output=")))
			if value == pilotOutputJSON {
				return pilotOutputJSON
			}
		}
	}
	return pilotOutputText
}

func hasFlag(args []string, name string) bool {
	long := "--" + name
	prefix := long + "="
	for _, arg := range args {
		if arg == long || strings.HasPrefix(arg, prefix) {
			return true
		}
	}
	return false
}

func liveStopReasonFromContext(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return betfair.LiveStopReasonDeadlineExceeded
	}
	if errors.Is(err, context.Canceled) {
		return betfair.LiveStopReasonContextCanceled
	}
	return betfair.LiveStopReasonFailed
}

type pilotCredentialError struct{ err error }

func (e pilotCredentialError) Error() string { return e.err.Error() }
func (e pilotCredentialError) Unwrap() error { return e.err }

type pilotConnectError struct{ err error }

func (e pilotConnectError) Error() string { return e.err.Error() }
func (e pilotConnectError) Unwrap() error { return e.err }

type pilotRuntimeError struct{ err error }

func (e pilotRuntimeError) Error() string { return e.err.Error() }
func (e pilotRuntimeError) Unwrap() error { return e.err }

type pilotCanceledError struct{ err error }

func (e pilotCanceledError) Error() string { return e.err.Error() }
func (e pilotCanceledError) Unwrap() error { return e.err }

type pilotDatabaseError struct{ err error }

func (e pilotDatabaseError) Error() string { return e.err.Error() }
func (e pilotDatabaseError) Unwrap() error { return e.err }

type marketIDFlag struct {
	items []string
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
		if part == "*" {
			return fmt.Errorf("wildcard market subscriptions are not allowed")
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
	values := make([]string, 0, len(m.items))
	for _, value := range m.items {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}
