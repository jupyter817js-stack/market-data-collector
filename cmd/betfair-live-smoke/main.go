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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/source/betfair"
)

const (
	defaultSmokeMaxMessages       = 20
	defaultSmokeDuration          = 30 * time.Second
	defaultSmokeDialTimeout       = 10 * time.Second
	defaultSmokeReadTimeout       = 10 * time.Second
	defaultSmokeWriteTimeout      = 10 * time.Second
	defaultSmokeHeartbeatMS       = 5000
	defaultSmokeLadderLevels      = 3
	defaultSmokeMaxMessageSize    = betfair.DefaultMaxMessageSize
	defaultSmokeMaxReconnects     = 0
	defaultSmokeReconnectBase     = 250 * time.Millisecond
	defaultSmokeReconnectMax      = 2 * time.Second
	maxSmokeMarketIDCount         = 5
	maxSmokeDuration              = 2 * time.Minute
	maxSmokeMessages              = 500
	smokeOutputText               = "text"
	smokeOutputJSON               = "json"
	smokeStopReasonPreflight      = "preflight_only"
	smokeStopReasonEnableRequired = "enable_live_required"
	smokeFailureValidation        = "validation"
	smokeFailureCredential        = "credential"
	smokeFailureRuntime           = "runtime"
)

const (
	exitSmokeSuccess = iota
)

const (
	exitSmokeValidation = 2
	exitSmokeCredential = 3
	exitSmokeConnect    = 4
	exitSmokeRuntime    = 5
	exitSmokeCanceled   = 6
)

var errSmokeLiveDisabled = errors.New("betfair live smoke requires --enable-live")

type smokeConfig struct {
	EnableLive      bool
	PreflightOnly   bool
	Output          string
	AppKeyEnv       string
	SessionTokenEnv string
	Address         string
	ServerName      string
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
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

type smokeReport struct {
	Enabled              bool   `json:"enabled"`
	PreflightOnly        bool   `json:"preflight_only"`
	Address              string `json:"address"`
	ServerName           string `json:"server_name"`
	MarketIDsCount       int    `json:"market_ids_count"`
	MaxMessages          int    `json:"max_messages"`
	DurationLimit        string `json:"duration_limit"`
	Connected            bool   `json:"connected"`
	Authenticated        bool   `json:"authenticated"`
	Subscribed           bool   `json:"subscribed"`
	MessagesRead         int    `json:"messages_read"`
	MarketChangeMessages int    `json:"market_change_messages"`
	HeartbeatMessages    int    `json:"heartbeat_messages"`
	StatusMessages       int    `json:"status_messages"`
	MarketsSeen          int    `json:"markets_seen"`
	CandidatesEmitted    int    `json:"candidates_emitted"`
	LastClock            string `json:"last_clk"`
	LastInitialClock     string `json:"last_initial_clk"`
	LastMessageTime      string `json:"last_message_time"`
	HeartbeatState       string `json:"heartbeat_state"`
	StopReason           string `json:"stop_reason"`
	FailureReason        string `json:"failure_reason"`
	ElapsedMS            int64  `json:"elapsed_ms"`
}

type envLookup func(string) (string, bool)
type smokeTransportFactory func(betfair.TLSLiveTransportOptions) betfair.LiveTransport

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	code := runCommand(ctx, os.Args[1:], os.Stdout, os.LookupEnv, defaultTransportFactory)
	if code != exitSmokeSuccess {
		os.Exit(code)
	}
}

func runCommand(ctx context.Context, args []string, out io.Writer, lookup envLookup, factory smokeTransportFactory) int {
	cfg, err := parseSmokeConfig(args, out)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitSmokeSuccess
		}
		report := smokeReport{
			Enabled:       hasFlag(args, "enable-live"),
			PreflightOnly: hasFlag(args, "preflight-only"),
			StopReason:    betfair.LiveStopReasonFailed,
			FailureReason: redactedError(err),
		}
		_ = writeSmokeReport(out, report, outputFromArgs(args))
		return exitSmokeValidation
	}

	report, err := runSmoke(ctx, cfg, lookup, factory)
	_ = writeSmokeReport(out, report, cfg.Output)
	return exitCodeForSmokeError(err)
}

func runSmoke(ctx context.Context, cfg smokeConfig, lookup envLookup, factory smokeTransportFactory) (smokeReport, error) {
	startedAt := time.Now().UTC()
	report := newSmokeReport(cfg)

	if !cfg.EnableLive {
		report.StopReason = smokeStopReasonEnableRequired
		report.FailureReason = errSmokeLiveDisabled.Error()
		return report, errSmokeLiveDisabled
	}
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if factory == nil {
		factory = defaultTransportFactory
	}

	appKey, sessionToken, err := readSmokeCredentials(cfg, lookup)
	if err != nil {
		report.StopReason = betfair.LiveStopReasonFailed
		report.FailureReason = redactedError(err)
		return report, smokeCredentialError{err: err}
	}

	if cfg.PreflightOnly {
		report.StopReason = smokeStopReasonPreflight
		report.ElapsedMS = time.Since(startedAt).Milliseconds()
		return report, nil
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	transport := factory(betfair.TLSLiveTransportOptions{
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
	})

	result, runErr := connector.Run(ctx)
	report = reportFromLiveResult(cfg, result, time.Since(startedAt))
	switch {
	case errors.Is(runErr, context.DeadlineExceeded):
		report.StopReason = "duration"
		return report, nil
	case errors.Is(runErr, context.Canceled):
		report.StopReason = betfair.LiveStopReasonContextCanceled
		return report, smokeCanceledError{err: runErr}
	case runErr != nil:
		report.FailureReason = betfair.RedactLiveString(runErr.Error(), appKey, sessionToken)
		if isConnectOrHandshakeFailure(runErr) {
			return report, smokeConnectError{err: runErr}
		}
		return report, smokeRuntimeError{err: runErr}
	default:
		return report, nil
	}
}

func parseSmokeConfig(args []string, out io.Writer) (smokeConfig, error) {
	fs := flag.NewFlagSet("betfair-live-smoke", flag.ContinueOnError)
	fs.SetOutput(out)

	marketIDs := marketIDFlag{}
	cfg := smokeConfig{}
	fs.BoolVar(&cfg.EnableLive, "enable-live", false, "explicitly enable the manual read-only Betfair live smoke command")
	fs.BoolVar(&cfg.PreflightOnly, "preflight-only", false, "validate smoke configuration and credentials without connecting")
	fs.StringVar(&cfg.Output, "output", smokeOutputText, "output format: text or json")
	fs.StringVar(&cfg.AppKeyEnv, "app-key-env", "", "environment variable name containing the Betfair app key")
	fs.StringVar(&cfg.SessionTokenEnv, "session-token-env", "", "environment variable name containing the Betfair session token")
	fs.StringVar(&cfg.Address, "address", "", "Betfair stream address as host:port; required when --enable-live is set")
	fs.StringVar(&cfg.ServerName, "server-name", "", "TLS server name; required when --enable-live is set")
	fs.DurationVar(&cfg.DialTimeout, "dial-timeout", defaultSmokeDialTimeout, "TLS dial timeout")
	fs.DurationVar(&cfg.ReadTimeout, "read-timeout", defaultSmokeReadTimeout, "per-read timeout")
	fs.DurationVar(&cfg.WriteTimeout, "write-timeout", defaultSmokeWriteTimeout, "per-write timeout")
	fs.IntVar(&cfg.HeartbeatMS, "heartbeat-ms", defaultSmokeHeartbeatMS, "Betfair stream heartbeat interval in milliseconds")
	fs.IntVar(&cfg.ConflateMS, "conflate-ms", 0, "Betfair stream conflate interval in milliseconds")
	fs.IntVar(&cfg.MaxMessages, "max-messages", defaultSmokeMaxMessages, "maximum post-subscription stream messages to read")
	fs.DurationVar(&cfg.Duration, "duration", defaultSmokeDuration, "maximum smoke command duration")
	fs.Var(&marketIDs, "market-id", "explicit Betfair market ID; may be repeated or comma-separated")
	fs.IntVar(&cfg.LadderLevels, "ladder-levels", defaultSmokeLadderLevels, "Betfair price ladder depth")
	fs.IntVar(&cfg.MaxMessageSize, "max-message-size", defaultSmokeMaxMessageSize, "maximum Betfair stream message size in bytes")
	fs.IntVar(&cfg.MaxReconnects, "max-reconnects", defaultSmokeMaxReconnects, "maximum reconnect attempts for the manual smoke run")
	fs.DurationVar(&cfg.ReconnectBase, "reconnect-base-delay", defaultSmokeReconnectBase, "base reconnect delay")
	fs.DurationVar(&cfg.ReconnectMax, "reconnect-max-delay", defaultSmokeReconnectMax, "maximum reconnect delay")

	if err := fs.Parse(args); err != nil {
		return smokeConfig{}, err
	}

	cfg.Output = strings.ToLower(strings.TrimSpace(cfg.Output))
	cfg.AppKeyEnv = strings.TrimSpace(cfg.AppKeyEnv)
	cfg.SessionTokenEnv = strings.TrimSpace(cfg.SessionTokenEnv)
	cfg.Address = strings.TrimSpace(cfg.Address)
	cfg.ServerName = strings.TrimSpace(cfg.ServerName)
	cfg.MarketIDs = marketIDs.values()

	if cfg.Output == "" {
		cfg.Output = smokeOutputText
	}
	if cfg.Output != smokeOutputText && cfg.Output != smokeOutputJSON {
		return smokeConfig{}, fmt.Errorf("output must be text or json")
	}
	if !cfg.EnableLive {
		return cfg, nil
	}
	if cfg.AppKeyEnv == "" {
		return smokeConfig{}, fmt.Errorf("app-key-env is required when --enable-live is set")
	}
	if cfg.SessionTokenEnv == "" {
		return smokeConfig{}, fmt.Errorf("session-token-env is required when --enable-live is set")
	}
	if cfg.Address == "" {
		return smokeConfig{}, fmt.Errorf("address is required when --enable-live is set")
	}
	if cfg.ServerName == "" {
		return smokeConfig{}, fmt.Errorf("server-name is required when --enable-live is set")
	}
	if len(cfg.MarketIDs) == 0 {
		return smokeConfig{}, fmt.Errorf("at least one market-id is required when --enable-live is set")
	}
	if len(cfg.MarketIDs) > maxSmokeMarketIDCount {
		return smokeConfig{}, fmt.Errorf("too many market IDs for smoke run: got %d max %d", len(cfg.MarketIDs), maxSmokeMarketIDCount)
	}
	if cfg.DialTimeout <= 0 || cfg.ReadTimeout <= 0 || cfg.WriteTimeout <= 0 {
		return smokeConfig{}, fmt.Errorf("dial-timeout, read-timeout, and write-timeout must be positive")
	}
	if cfg.HeartbeatMS <= 0 {
		return smokeConfig{}, fmt.Errorf("heartbeat-ms must be positive")
	}
	if cfg.ConflateMS < 0 {
		return smokeConfig{}, fmt.Errorf("conflate-ms must be non-negative")
	}
	if cfg.MaxMessages <= 0 {
		return smokeConfig{}, fmt.Errorf("max-messages must be positive")
	}
	if cfg.MaxMessages > maxSmokeMessages {
		return smokeConfig{}, fmt.Errorf("max-messages exceeds smoke limit: got %d max %d", cfg.MaxMessages, maxSmokeMessages)
	}
	if cfg.Duration <= 0 {
		return smokeConfig{}, fmt.Errorf("duration must be positive")
	}
	if cfg.Duration > maxSmokeDuration {
		return smokeConfig{}, fmt.Errorf("duration exceeds smoke limit: got %s max %s", cfg.Duration, maxSmokeDuration)
	}
	if cfg.LadderLevels < 0 {
		return smokeConfig{}, fmt.Errorf("ladder-levels must be non-negative")
	}
	if cfg.MaxMessageSize <= 0 {
		return smokeConfig{}, fmt.Errorf("max-message-size must be positive")
	}
	if cfg.MaxReconnects < 0 {
		return smokeConfig{}, fmt.Errorf("max-reconnects must be non-negative")
	}
	if cfg.ReconnectBase <= 0 || cfg.ReconnectMax <= 0 {
		return smokeConfig{}, fmt.Errorf("reconnect delays must be positive")
	}
	if cfg.ReconnectMax < cfg.ReconnectBase {
		return smokeConfig{}, fmt.Errorf("reconnect-max-delay must be >= reconnect-base-delay")
	}
	return cfg, nil
}

func readSmokeCredentials(cfg smokeConfig, lookup envLookup) (string, string, error) {
	appKey, ok := lookup(cfg.AppKeyEnv)
	if !ok || strings.TrimSpace(appKey) == "" {
		return "", "", fmt.Errorf("environment variable named by --app-key-env (%s) is not set or empty", cfg.AppKeyEnv)
	}
	sessionToken, ok := lookup(cfg.SessionTokenEnv)
	if !ok || strings.TrimSpace(sessionToken) == "" {
		return "", "", fmt.Errorf("environment variable named by --session-token-env (%s) is not set or empty", cfg.SessionTokenEnv)
	}
	return strings.TrimSpace(appKey), strings.TrimSpace(sessionToken), nil
}

func newSmokeReport(cfg smokeConfig) smokeReport {
	return smokeReport{
		Enabled:        cfg.EnableLive,
		PreflightOnly:  cfg.PreflightOnly,
		Address:        cfg.Address,
		ServerName:     cfg.ServerName,
		MarketIDsCount: len(cfg.MarketIDs),
		MaxMessages:    cfg.MaxMessages,
		DurationLimit:  cfg.Duration.String(),
	}
}

func reportFromLiveResult(cfg smokeConfig, result betfair.LiveRunResult, elapsed time.Duration) smokeReport {
	report := newSmokeReport(cfg)
	report.Connected = result.ConnectionAttempts > 0
	report.Authenticated = result.MessagesParsed >= 1 && result.State != betfair.LiveStateDisabled && result.State != betfair.LiveStateFailed
	report.Subscribed = result.MessagesParsed >= 2 && result.State != betfair.LiveStateDisabled && result.State != betfair.LiveStateFailed
	report.MessagesRead = result.MessagesParsed
	report.MarketChangeMessages = result.MarketChangeMessages
	report.HeartbeatMessages = result.HeartbeatMessages
	report.StatusMessages = result.ControlMessages
	report.MarketsSeen = result.MarketsSeen
	report.CandidatesEmitted = len(result.Candidates)
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

func writeSmokeReport(out io.Writer, report smokeReport, format string) error {
	switch format {
	case smokeOutputJSON:
		payload, err := json.Marshal(report)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(payload))
		return err
	default:
		return writeTextSmokeReport(out, report)
	}
}

func writeTextSmokeReport(out io.Writer, report smokeReport) error {
	_, err := fmt.Fprintf(out, "betfair_live_smoke enabled=%t preflight_only=%t read_only=true persistence=false collector_integration=false market_stream_only=true address=%s server_name=%s market_ids_count=%d\n",
		report.Enabled,
		report.PreflightOnly,
		report.Address,
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
	_, err = fmt.Fprintf(out, "messages_read=%d market_change_messages=%d heartbeat_messages=%d status_messages=%d markets_seen=%d candidates_emitted=%d\n",
		report.MessagesRead,
		report.MarketChangeMessages,
		report.HeartbeatMessages,
		report.StatusMessages,
		report.MarketsSeen,
		report.CandidatesEmitted,
	)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "last_clk=%s last_initial_clk=%s last_message_time=%s heartbeat_state=%s elapsed_ms=%d\n",
		report.LastClock,
		report.LastInitialClock,
		report.LastMessageTime,
		report.HeartbeatState,
		report.ElapsedMS,
	)
	return err
}

func defaultTransportFactory(options betfair.TLSLiveTransportOptions) betfair.LiveTransport {
	return betfair.NewTLSLiveTransport(options)
}

func exitCodeForSmokeError(err error) int {
	if err == nil {
		return exitSmokeSuccess
	}
	if errors.Is(err, errSmokeLiveDisabled) {
		return exitSmokeValidation
	}
	var credential smokeCredentialError
	if errors.As(err, &credential) {
		return exitSmokeCredential
	}
	var connect smokeConnectError
	if errors.As(err, &connect) {
		return exitSmokeConnect
	}
	var canceled smokeCanceledError
	if errors.As(err, &canceled) || errors.Is(err, context.Canceled) {
		return exitSmokeCanceled
	}
	return exitSmokeRuntime
}

func isConnectOrHandshakeFailure(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "connect") ||
		strings.Contains(text, "handshake") ||
		strings.Contains(text, "authentication failed") ||
		strings.Contains(text, "subscription failed") ||
		strings.Contains(text, "max reconnect")
}

func redactedError(err error) string {
	if err == nil {
		return ""
	}
	return betfair.RedactLiveString(err.Error())
}

func outputFromArgs(args []string) string {
	for i, arg := range args {
		if arg == "--output" && i+1 < len(args) {
			value := strings.ToLower(strings.TrimSpace(args[i+1]))
			if value == smokeOutputJSON {
				return smokeOutputJSON
			}
		}
		if strings.HasPrefix(arg, "--output=") {
			value := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(arg, "--output=")))
			if value == smokeOutputJSON {
				return smokeOutputJSON
			}
		}
	}
	return smokeOutputText
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

type smokeCredentialError struct{ err error }

func (e smokeCredentialError) Error() string { return e.err.Error() }
func (e smokeCredentialError) Unwrap() error { return e.err }

type smokeConnectError struct{ err error }

func (e smokeConnectError) Error() string { return e.err.Error() }
func (e smokeConnectError) Unwrap() error { return e.err }

type smokeRuntimeError struct{ err error }

func (e smokeRuntimeError) Error() string { return e.err.Error() }
func (e smokeRuntimeError) Unwrap() error { return e.err }

type smokeCanceledError struct{ err error }

func (e smokeCanceledError) Error() string { return e.err.Error() }
func (e smokeCanceledError) Unwrap() error { return e.err }

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
