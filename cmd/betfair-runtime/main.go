package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/betfair"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const (
	runtimeTransportFake = "fake"
	runtimeTransportTLS  = "tls"
	runtimeOutputText    = "text"
	runtimeOutputJSON    = "json"
	runtimeModeFake      = "fake"
	runtimeModeRealPilot = "real_pilot"
)

const (
	runtimePostflightPass    = "pass"
	runtimePostflightWarning = "warning"
	runtimePostflightFail    = "fail"
)

const (
	runtimeSinkMemory   = "memory"
	runtimeSinkPostgres = "postgres"
)

const (
	runtimeStatusDisabled = "disabled"
	runtimeStatusStarting = "starting"
	runtimeStatusRunning  = "running"
	runtimeStatusStopping = "stopping"
	runtimeStatusStopped  = "stopped"
	runtimeStatusFailed   = "failed"
)

const (
	runtimeReadinessDisabled = "disabled"
	runtimeReadinessStarting = "starting"
	runtimeReadinessReady    = "ready"
	runtimeReadinessDegraded = "degraded"
	runtimeReadinessStale    = "stale"
	runtimeReadinessFailed   = "failed"
	runtimeReadinessStopped  = "stopped"
)

const (
	defaultRuntimeMaxMessages       = 20
	defaultRuntimeDuration          = 30 * time.Second
	defaultRuntimeDialTimeout       = 10 * time.Second
	defaultRuntimeReadTimeout       = 10 * time.Second
	defaultRuntimeWriteTimeout      = 10 * time.Second
	defaultRuntimeHeartbeatMS       = 500
	defaultRuntimeLadderLevels      = 3
	defaultRuntimeMaxMessageSize    = betfair.DefaultMaxMessageSize
	defaultRuntimeReconnectBase     = 250 * time.Millisecond
	defaultRuntimeReconnectMax      = 2 * time.Second
	defaultRuntimeCandidateBatch    = 50
	defaultRuntimeRawMessageBatch   = 50
	defaultRuntimeFlushInterval     = 30 * time.Second
	defaultRuntimeFinalFlushTimeout = 500 * time.Millisecond
	maxRuntimeBatchSize             = 500
	maxRuntimeMarketIDCount         = 5
	maxRuntimeMessages              = 500
	maxRuntimeDuration              = 2 * time.Minute
	maxRuntimeSoakRuns              = 10
	maxRuntimeSoakDelay             = 30 * time.Second
	maxRuntimeDiagnosticSampleLimit = 10
	runtimeStopReasonEnableRequired = "runtime_disabled"
	runtimeStopReasonDuration       = "duration"
	runtimeStopReasonPostflightOnly = "postflight_only"
	runtimeStopReasonValidation     = "validation"
	runtimeStreamIDPrefix           = "runtime-"
)

const (
	runtimeFakeAppKey       = "FAKE_RUNTIME_APP_KEY"
	runtimeFakeSessionToken = "FAKE_RUNTIME_SESSION_TOKEN"
)

const (
	exitRuntimeSuccess = iota
)

const (
	exitRuntimeValidation = 2
	exitRuntimeRuntime    = 5
	exitRuntimeCanceled   = 6
)

type runtimeConfig struct {
	EnableRuntime         bool
	RealRuntimePilot      bool
	PostflightOnly        bool
	PostflightChecklist   bool
	Output                string
	TransportMode         string
	SinkType              string
	DatabaseURLEnv        string
	DatabaseURL           string
	DatabaseURLRedacted   string
	AppKeyEnv             string
	SessionTokenEnv       string
	AppKey                string
	SessionToken          string
	Address               string
	ServerName            string
	RunID                 int64
	RunLabel              string
	SoakRuns              int
	SoakDelay             time.Duration
	SoakRunLabelPrefix    string
	DiagnoseStream        bool
	DiagnosticSampleLimit int
	MaxMessages           int
	DurationLimit         time.Duration
	DialTimeout           time.Duration
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	HeartbeatMS           int
	ConflateMS            int
	MarketIDs             []string
	LadderLevels          int
	MaxMessageSize        int
	MaxReconnectAttempts  int
	ReconnectBase         time.Duration
	ReconnectMax          time.Duration
}

type RuntimeStreamGroup struct {
	ID        string
	MarketIDs []string
}

type RuntimeOptions struct {
	Enabled               bool
	RealRuntimePilot      bool
	TransportMode         string
	MarketIDs             []string
	MaxMessages           int
	DurationLimit         time.Duration
	HeartbeatAgingAfter   time.Duration
	HeartbeatStaleAfter   time.Duration
	MaxReconnectAttempts  int
	ReconnectBaseDelay    time.Duration
	ReconnectMaxDelay     time.Duration
	HeartbeatMS           int
	ConflateMS            int
	AppKey                string
	SessionToken          string
	LadderLevels          int
	MaxMessageSize        int
	Now                   func() time.Time
	Sink                  RuntimeSink
	SinkType              string
	DatabaseEnabled       bool
	DatabaseURLRedacted   string
	TransportFactory      func(RuntimeStreamGroup) betfair.LiveTransport
	CandidateBatchSize    int
	RawMessageBatchSize   int
	FlushInterval         time.Duration
	DiagnoseStream        bool
	DiagnosticSampleLimit int
}

type RuntimeStatus struct {
	Enabled                  bool                        `json:"enabled"`
	RealRuntimePilot         bool                        `json:"real_runtime_pilot"`
	PostflightOnly           bool                        `json:"postflight_only"`
	Running                  bool                        `json:"running"`
	RunID                    int64                       `json:"run_id"`
	RunLabel                 string                      `json:"run_label"`
	Status                   string                      `json:"status"`
	Readiness                string                      `json:"readiness"`
	LifecycleState           string                      `json:"lifecycle_state"`
	ReadinessState           string                      `json:"readiness_state"`
	TransportMode            string                      `json:"transport_mode"`
	SinkType                 string                      `json:"sink_type"`
	FakeTransportOnly        bool                        `json:"fake_transport_only"`
	CollectorIntegration     bool                        `json:"collector_integration"`
	MarketStreamOnly         bool                        `json:"market_stream_only"`
	StreamGroup              string                      `json:"stream_group"`
	MarketIDsCount           int                         `json:"market_ids_count"`
	MaxMessagesLimit         int                         `json:"max_messages_limit"`
	DurationLimitSeconds     int                         `json:"duration_limit_seconds"`
	Connected                bool                        `json:"connected"`
	Authenticated            bool                        `json:"authenticated"`
	Subscribed               bool                        `json:"subscribed"`
	StartedAt                string                      `json:"started_at"`
	StoppedAt                string                      `json:"stopped_at"`
	ElapsedMS                int64                       `json:"elapsed_ms"`
	RawMessagesSeen          int                         `json:"raw_messages_seen"`
	MessagesRead             int                         `json:"messages_read"`
	MarketChangeMessages     int                         `json:"market_change_messages"`
	MarketStreamMessages     int                         `json:"market_stream_messages"`
	MarketPayloadMessages    int                         `json:"market_payload_messages"`
	ServerHeartbeatMessages  int                         `json:"server_heartbeat_messages"`
	HeartbeatMessages        int                         `json:"heartbeat_messages"`
	StatusMessages           int                         `json:"status_messages"`
	CandidatesEmitted        int                         `json:"candidates_emitted"`
	CandidateBatchesSeen     int                         `json:"candidate_batches_seen"`
	CandidateBatchesFlushed  int                         `json:"candidate_batches_flushed"`
	SinkErrors               int                         `json:"sink_errors"`
	RawMessagesBuffered      int                         `json:"raw_messages_buffered"`
	RawMessagesFlushed       int                         `json:"raw_messages_flushed"`
	RawMessagesInserted      int                         `json:"raw_messages_inserted"`
	CandidatesBuffered       int                         `json:"candidates_buffered"`
	CandidatesFlushed        int                         `json:"candidates_flushed"`
	CandidatesInserted       int                         `json:"candidates_inserted"`
	CandidatesUnchanged      int                         `json:"candidates_unchanged"`
	CandidatesSkipped        int                         `json:"candidates_skipped"`
	FlushCount               int                         `json:"flush_count"`
	FlushErrors              int                         `json:"flush_errors"`
	MarketsSeen              int                         `json:"markets_seen"`
	LatestCandidateCount     int                         `json:"latest_candidate_count"`
	ReconnectCount           int                         `json:"reconnect_count"`
	LastClock                string                      `json:"last_clk"`
	LastInitialClock         string                      `json:"last_initial_clk"`
	LastMessageTime          string                      `json:"last_message_time"`
	LastCandidateTime        string                      `json:"last_candidate_time"`
	LastSinkFlushTime        string                      `json:"last_sink_flush_time"`
	LastFlushTime            string                      `json:"last_flush_time"`
	HeartbeatState           string                      `json:"heartbeat_state"`
	StoppedReason            string                      `json:"stopped_reason"`
	FailureReason            string                      `json:"failure_reason"`
	DatabaseEnabled          bool                        `json:"database_enabled"`
	DatabaseURLRedacted      string                      `json:"database_url_redacted"`
	LinkedRawMessageCount    int64                       `json:"linked_raw_message_count"`
	QuoteHistoryRows         int64                       `json:"quote_history_rows"`
	LatestQuoteCount         int64                       `json:"latest_quote_count"`
	MappedQuoteCount         int64                       `json:"mapped_quote_count"`
	ReadyRunnerBookCount     int64                       `json:"ready_runner_book_count"`
	ReadyComparisonCount     int64                       `json:"ready_comparison_count"`
	Warnings                 []string                    `json:"warnings,omitempty"`
	PostflightClassification string                      `json:"postflight_classification,omitempty"`
	PostflightFailReasons    []string                    `json:"postflight_fail_reasons,omitempty"`
	PostflightWarningReasons []string                    `json:"postflight_warning_reasons,omitempty"`
	PostflightChecklist      *RuntimePostflightChecklist `json:"postflight_checklist,omitempty"`
	StreamDiagnostics        *RuntimeStreamDiagnostics   `json:"stream_diagnostics,omitempty"`
}

type RuntimeReport = RuntimeStatus

type RuntimePostflightChecklist struct {
	Enabled                         bool     `json:"enabled"`
	Phase10GFakeLocalSoak           string   `json:"phase_10g_fake_local_soak"`
	RealRuntimePilotStatus          string   `json:"real_runtime_pilot_status"`
	RedactionStatus                 string   `json:"redaction_status"`
	MessageCandidateStatus          string   `json:"message_candidate_status"`
	SinkStatus                      string   `json:"sink_status"`
	HeartbeatStatus                 string   `json:"heartbeat_status"`
	MappedReadinessComparisonStatus string   `json:"mapped_readiness_comparison_status"`
	NextAllowedAction               string   `json:"next_allowed_action"`
	BlockingReasons                 []string `json:"blocking_reasons,omitempty"`
	WarningReasons                  []string `json:"warning_reasons,omitempty"`
}

type RuntimeReadinessStateCount struct {
	ReadinessState string `json:"readiness_state"`
	Count          int    `json:"count"`
}

type RuntimeSoakReport struct {
	Enabled                  bool                         `json:"enabled"`
	Soak                     bool                         `json:"soak"`
	TransportMode            string                       `json:"transport_mode"`
	SinkType                 string                       `json:"sink_type"`
	DatabaseEnabled          bool                         `json:"database_enabled"`
	DatabaseURLRedacted      string                       `json:"database_url_redacted"`
	RunLabelPrefix           string                       `json:"run_label_prefix"`
	SoakRunsRequested        int                          `json:"soak_runs_requested"`
	SoakRunsCompleted        int                          `json:"soak_runs_completed"`
	SoakRunsFailed           int                          `json:"soak_runs_failed"`
	TotalMessagesRead        int                          `json:"total_messages_read"`
	TotalRawMessagesInserted int                          `json:"total_raw_messages_inserted"`
	TotalCandidatesEmitted   int                          `json:"total_candidates_emitted"`
	TotalCandidatesInserted  int                          `json:"total_candidates_inserted"`
	TotalCandidatesUnchanged int                          `json:"total_candidates_unchanged"`
	TotalCandidatesSkipped   int                          `json:"total_candidates_skipped"`
	TotalFlushErrors         int                          `json:"total_flush_errors"`
	TotalSinkErrors          int                          `json:"total_sink_errors"`
	RuntimeRunIDs            []int64                      `json:"runtime_run_ids"`
	FailedRunIDs             []int64                      `json:"failed_run_ids"`
	ReadinessStateCounts     []RuntimeReadinessStateCount `json:"readiness_state_counts"`
	WarningCount             int                          `json:"warning_count"`
	Warnings                 []string                     `json:"warnings,omitempty"`
	ElapsedMS                int64                        `json:"elapsed_ms"`
	FakeTransportOnly        bool                         `json:"fake_transport_only"`
	CollectorIntegration     bool                         `json:"collector_integration"`
	MarketStreamOnly         bool                         `json:"market_stream_only"`
}

type RuntimeSinkStats struct {
	SinkType            string
	RawMessagesBuffered int
	RawMessagesFlushed  int
	RawMessagesInserted int
	CandidatesBuffered  int
	CandidatesFlushed   int
	CandidatesInserted  int
	CandidatesUnchanged int
	CandidatesSkipped   int
	FlushCount          int
	FlushErrors         int
	LastFlushTime       string
	DatabaseEnabled     bool
	DatabaseURLRedacted string
}

type RuntimeSinkStatsReporter interface {
	RuntimeSinkStats() RuntimeSinkStats
}

type RuntimeSink interface {
	OnRawMessage(context.Context, betfair.Message, time.Time) error
	OnQuoteCandidates(context.Context, []betfair.QuoteCandidate, time.Time) error
	OnStatus(context.Context, RuntimeStatus) error
	Flush(context.Context) error
	Close(context.Context) error
}

type InMemoryRuntimeSink struct {
	mu                         sync.Mutex
	rawMessagesSeen            int
	rawMessagesBuffered        int
	rawMessagesFlushed         int
	candidatesBuffered         int
	candidatesFlushed          int
	candidateBatchesSeen       int
	candidateBatchesFlushed    int
	pendingCandidateBatchCount int
	flushCount                 int
	flushErrors                int
	closeCount                 int
	latestCandidates           []betfair.QuoteCandidate
	statuses                   []RuntimeStatus
	onRawMessageError          error
	onQuoteCandidatesError     error
	flushError                 error
	closeError                 error
}

func (s *InMemoryRuntimeSink) OnRawMessage(ctx context.Context, msg betfair.Message, receivedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rawMessagesSeen++
	s.rawMessagesBuffered++
	if s.onRawMessageError != nil {
		return s.onRawMessageError
	}
	return nil
}

func (s *InMemoryRuntimeSink) OnQuoteCandidates(ctx context.Context, candidates []betfair.QuoteCandidate, emittedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.candidateBatchesSeen++
	s.pendingCandidateBatchCount++
	s.candidatesBuffered += len(candidates)
	s.latestCandidates = append([]betfair.QuoteCandidate(nil), candidates...)
	if s.onQuoteCandidatesError != nil {
		return s.onQuoteCandidatesError
	}
	return nil
}

func (s *InMemoryRuntimeSink) OnStatus(ctx context.Context, status RuntimeStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses = appendBoundedRuntimeStatuses(s.statuses, status, 20)
	return nil
}

func (s *InMemoryRuntimeSink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushCount++
	if s.rawMessagesBuffered > 0 {
		s.rawMessagesFlushed += s.rawMessagesBuffered
		s.rawMessagesBuffered = 0
	}
	if s.pendingCandidateBatchCount > 0 {
		s.candidateBatchesFlushed += s.pendingCandidateBatchCount
		s.pendingCandidateBatchCount = 0
	}
	if s.candidatesBuffered > 0 {
		s.candidatesFlushed += s.candidatesBuffered
		s.candidatesBuffered = 0
	}
	if s.flushError != nil {
		s.flushErrors++
		return s.flushError
	}
	return nil
}

func (s *InMemoryRuntimeSink) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCount++
	if s.closeError != nil {
		return s.closeError
	}
	return nil
}

func (s *InMemoryRuntimeSink) RawMessagesSeen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rawMessagesSeen
}

func (s *InMemoryRuntimeSink) Candidates() []betfair.QuoteCandidate {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]betfair.QuoteCandidate(nil), s.latestCandidates...)
}

func (s *InMemoryRuntimeSink) Statuses() []RuntimeStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]RuntimeStatus(nil), s.statuses...)
}

func (s *InMemoryRuntimeSink) CandidateBatchesSeen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.candidateBatchesSeen
}

func (s *InMemoryRuntimeSink) CandidateBatchesFlushed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.candidateBatchesFlushed
}

func (s *InMemoryRuntimeSink) FlushCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushCount
}

func (s *InMemoryRuntimeSink) CloseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}

func (s *InMemoryRuntimeSink) RuntimeSinkStats() RuntimeSinkStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return RuntimeSinkStats{
		SinkType:            runtimeSinkMemory,
		RawMessagesBuffered: s.rawMessagesBuffered,
		RawMessagesFlushed:  s.rawMessagesFlushed,
		CandidatesBuffered:  s.candidatesBuffered,
		CandidatesFlushed:   s.candidatesFlushed,
		FlushCount:          s.flushCount,
		FlushErrors:         s.flushErrors,
	}
}

func (s *InMemoryRuntimeSink) InjectRawMessageError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onRawMessageError = err
}

func (s *InMemoryRuntimeSink) InjectQuoteCandidatesError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onQuoteCandidatesError = err
}

func (s *InMemoryRuntimeSink) InjectFlushError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushError = err
}

func (s *InMemoryRuntimeSink) InjectCloseError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeError = err
}

type betfairBatchSaver interface {
	SaveBetfairBatch(context.Context, []model.BetfairRawStreamMessage, []model.BetfairExchangeQuoteCandidate) (model.BetfairSaveStats, error)
}

type runtimeBatchStore interface {
	betfairBatchSaver
	io.Closer
}

type runtimeTelemetryStore interface {
	StartBetfairRuntimeRun(context.Context, model.BetfairRuntimeRunStart) (int64, error)
	FinishBetfairRuntimeRun(context.Context, int64, model.BetfairRuntimeRunFinish) error
	GetBetfairRuntimePostflight(context.Context, int64) (model.BetfairRuntimePostflight, error)
}

type runtimeStore interface {
	runtimeBatchStore
	runtimeTelemetryStore
}

type PostgresRuntimeSinkOptions struct {
	Saver               betfairBatchSaver
	Closer              io.Closer
	StreamID            string
	Now                 func() time.Time
	DatabaseURL         string
	DatabaseURLRedacted string
}

type PostgresRuntimeSink struct {
	mu                  sync.Mutex
	saver               betfairBatchSaver
	closer              io.Closer
	streamID            string
	now                 func() time.Time
	redactionValues     []string
	databaseURLRedacted string
	rawBuffer           []model.BetfairRawStreamMessage
	candidateBuffer     []model.BetfairExchangeQuoteCandidate
	stats               RuntimeSinkStats
	closed              bool
}

func NewPostgresRuntimeSink(options PostgresRuntimeSinkOptions) (*PostgresRuntimeSink, error) {
	if options.Saver == nil {
		return nil, fmt.Errorf("betfair runtime postgres sink requires a saver")
	}
	streamID := strings.TrimSpace(options.StreamID)
	if streamID == "" {
		streamID = "fake-group-1"
	}
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	redactedURL := strings.TrimSpace(options.DatabaseURLRedacted)
	if redactedURL == "" {
		redactedURL = redactRuntimeDatabaseURL(options.DatabaseURL)
	}
	return &PostgresRuntimeSink{
		saver:               options.Saver,
		closer:              options.Closer,
		streamID:            streamID,
		now:                 now,
		redactionValues:     []string{options.DatabaseURL},
		databaseURLRedacted: redactedURL,
		stats: RuntimeSinkStats{
			SinkType:            runtimeSinkPostgres,
			DatabaseEnabled:     true,
			DatabaseURLRedacted: redactedURL,
		},
	}, nil
}

func (s *PostgresRuntimeSink) OnRawMessage(ctx context.Context, msg betfair.Message, receivedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.redactError(fmt.Errorf("postgres runtime sink is closed"))
	}
	raw := msg.RawStreamMessage(s.streamID, receivedAt.UTC())
	raw.RawJSON = redactRuntimeRawJSON(raw.RawJSON, s.redactionValues...)
	s.rawBuffer = append(s.rawBuffer, raw)
	s.stats.RawMessagesBuffered = len(s.rawBuffer)
	return nil
}

func (s *PostgresRuntimeSink) OnQuoteCandidates(ctx context.Context, candidates []betfair.QuoteCandidate, emittedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.redactError(fmt.Errorf("postgres runtime sink is closed"))
	}
	for _, candidate := range candidates {
		s.candidateBuffer = append(s.candidateBuffer, candidate.PersistenceModel(emittedAt.UTC()))
	}
	s.stats.CandidatesBuffered = len(s.candidateBuffer)
	return nil
}

func (s *PostgresRuntimeSink) OnStatus(ctx context.Context, status RuntimeStatus) error {
	return ctx.Err()
}

func (s *PostgresRuntimeSink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return s.redactError(fmt.Errorf("postgres runtime sink is closed"))
	}
	raw := append([]model.BetfairRawStreamMessage(nil), s.rawBuffer...)
	candidates := append([]model.BetfairExchangeQuoteCandidate(nil), s.candidateBuffer...)
	s.mu.Unlock()

	saved, err := s.saver.SaveBetfairBatch(ctx, raw, candidates)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.FlushCount++
	if err != nil {
		s.stats.FlushErrors++
		return s.redactError(fmt.Errorf("save betfair runtime batch: %w", err))
	}
	s.rawBuffer = nil
	s.candidateBuffer = nil
	s.stats.RawMessagesBuffered = 0
	s.stats.CandidatesBuffered = 0
	s.stats.RawMessagesFlushed += len(raw)
	s.stats.CandidatesFlushed += len(candidates)
	s.stats.RawMessagesInserted += saved.RawMessagesInserted
	s.stats.CandidatesInserted += saved.CandidatesInserted
	s.stats.CandidatesUnchanged += saved.CandidatesUnchanged
	s.stats.CandidatesSkipped += saved.CandidatesSkipped
	s.stats.LastFlushTime = formatRuntimeTime(s.now())
	return nil
}

func (s *PostgresRuntimeSink) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	closer := s.closer
	s.mu.Unlock()
	if closer == nil {
		return nil
	}
	if err := closer.Close(); err != nil {
		return s.redactError(fmt.Errorf("close postgres runtime sink: %w", err))
	}
	return nil
}

func (s *PostgresRuntimeSink) RuntimeSinkStats() RuntimeSinkStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := s.stats
	stats.RawMessagesBuffered = len(s.rawBuffer)
	stats.CandidatesBuffered = len(s.candidateBuffer)
	stats.DatabaseEnabled = true
	if stats.SinkType == "" {
		stats.SinkType = runtimeSinkPostgres
	}
	if stats.DatabaseURLRedacted == "" {
		stats.DatabaseURLRedacted = s.databaseURLRedacted
	}
	return stats
}

func (s *PostgresRuntimeSink) redactError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(redactedRuntimeErrorWithSecrets(err, s.redactionValues...))
}

func appendBoundedRuntimeStatuses(statuses []RuntimeStatus, status RuntimeStatus, limit int) []RuntimeStatus {
	if limit <= 0 {
		return nil
	}
	statuses = append(statuses, status)
	if len(statuses) <= limit {
		return statuses
	}
	return append([]RuntimeStatus(nil), statuses[len(statuses)-limit:]...)
}

type RuntimeSupervisor struct {
	options RuntimeOptions
}

func NewRuntimeSupervisor(options RuntimeOptions) *RuntimeSupervisor {
	return &RuntimeSupervisor{options: options}
}

func (s *RuntimeSupervisor) Run(ctx context.Context) (RuntimeStatus, error) {
	options := normalizeRuntimeOptions(s.options)
	accounting := &runtimeAccounting{}
	marketDataFields := betfair.DefaultMarketDataFields()
	diagnostics := newRuntimeStreamDiagnosticCollector(options.DiagnoseStream, options.DiagnosticSampleLimit, marketDataFields, options.LadderLevels)
	status := baseRuntimeStatus(options)

	if !options.Enabled {
		status.Status = runtimeStatusDisabled
		status.StoppedReason = runtimeStopReasonEnableRequired
		status.Readiness = RuntimeReadiness(status)
		finalizeRuntimeStatusFields(&status)
		return status, notifyRuntimeStatus(context.Background(), options.Sink, status)
	}

	options, group, err := validateRuntimeOptions(options)
	if err != nil {
		status.Status = runtimeStatusFailed
		status.Readiness = RuntimeReadiness(status)
		status.StoppedReason = runtimeStopReasonValidation
		status.FailureReason = redactedRuntimeErrorWithSecrets(err, options.AppKey, options.SessionToken)
		finalizeRuntimeStatusFields(&status)
		_ = notifyRuntimeStatus(context.Background(), options.Sink, status)
		return status, err
	}

	startedAt := options.Now().UTC()
	status = baseRuntimeStatus(options)
	status.StreamGroup = group.ID
	status.MarketIDsCount = len(group.MarketIDs)
	status.Status = runtimeStatusStarting
	status.StartedAt = formatRuntimeTime(startedAt)
	status.ElapsedMS = runtimeElapsedMS(startedAt, startedAt)
	status.Readiness = RuntimeReadiness(status)
	finalizeRuntimeStatusFields(&status)
	if err := notifyRuntimeStatus(context.Background(), options.Sink, status); err != nil {
		return status, err
	}

	transport := options.TransportFactory(group)
	if transport == nil {
		err := fmt.Errorf("betfair runtime transport factory returned nil transport")
		status.Status = runtimeStatusFailed
		status.StoppedAt = formatRuntimeTime(options.Now())
		status.StoppedReason = betfair.LiveStopReasonFailed
		status.FailureReason = redactedRuntimeErrorWithSecrets(err, options.AppKey, options.SessionToken)
		status.Readiness = RuntimeReadiness(status)
		accounting.applyToStatus(&status, options)
		finalizeRuntimeStatusFields(&status)
		_ = notifyRuntimeStatus(context.Background(), options.Sink, status)
		return status, err
	}

	runCtx := ctx
	cancel := func() {}
	if options.DurationLimit > 0 {
		runCtx, cancel = context.WithTimeout(ctx, options.DurationLimit)
	}
	defer cancel()

	status.Status = runtimeStatusRunning
	status.Running = true
	status.Readiness = RuntimeReadiness(status)
	finalizeRuntimeStatusFields(&status)
	if err := notifyRuntimeStatus(context.Background(), options.Sink, status); err != nil {
		return status, err
	}

	appKey := options.AppKey
	if appKey == "" {
		appKey = runtimeFakeAppKey
	}
	sessionToken := options.SessionToken
	if sessionToken == "" {
		sessionToken = runtimeFakeSessionToken
	}

	connector := betfair.NewLiveConnector(transport, betfair.LiveConnectorOptions{
		Enabled:      true,
		AppKey:       appKey,
		SessionToken: sessionToken,
		Subscription: betfair.MarketSubscriptionRequest{
			MarketIDs:    group.MarketIDs,
			Fields:       marketDataFields,
			LadderLevels: options.LadderLevels,
			HeartbeatMS:  options.HeartbeatMS,
			ConflateMS:   options.ConflateMS,
		},
		HeartbeatAgingAfter:  options.HeartbeatAgingAfter,
		HeartbeatStaleAfter:  options.HeartbeatStaleAfter,
		MaxReconnectAttempts: options.MaxReconnectAttempts,
		ReconnectBaseDelay:   options.ReconnectBaseDelay,
		ReconnectMaxDelay:    options.ReconnectMaxDelay,
		MaxMessageSize:       options.MaxMessageSize,
		MaxMessages:          options.MaxMessages,
		Now:                  options.Now,
		OnMessage: func(msg betfair.Message, receivedAt time.Time) error {
			diagnostics.observeMessage(msg)
			if options.Sink == nil {
				return nil
			}
			receivedAt = receivedAt.UTC()
			accounting.recordRawMessage(receivedAt)
			if err := options.Sink.OnRawMessage(runCtx, msg, receivedAt); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				accounting.recordSinkError()
				return runtimeSinkError("raw message", err, options.AppKey, options.SessionToken)
			}
			if accounting.shouldFlushRaw(options.RawMessageBatchSize) || accounting.shouldFlushInterval(options.Now, options.FlushInterval) {
				if err := flushRuntimeSink(runCtx, options, accounting); err != nil {
					return err
				}
			}
			return nil
		},
		OnStatusMessage: func(phase string, msg betfair.Message, receivedAt time.Time) {
			diagnostics.observeStatusPhase(phase, msg)
		},
		OnSubscription: func(requestID int, req betfair.MarketSubscriptionRequest, requestedAt time.Time) {
			diagnostics.observeSubscription(requestID, req)
		},
		OnCacheApply: func(msg betfair.Message, err error, receivedAt time.Time) {
			diagnostics.observeCacheApply(msg, err)
		},
	})

	result, runErr := connector.Run(runCtx)

	status = statusFromLiveResult(options, group, result, startedAt)
	status.Status = runtimeStatusStopping
	status.Running = false
	status.StoppedAt = formatRuntimeTime(options.Now())
	diagnostics.applyToStatus(&status, result)
	accounting.applyToStatus(&status, options)
	status.Readiness = RuntimeReadiness(status)
	finalizeRuntimeStatusFields(&status)
	_ = notifyRuntimeStatus(context.Background(), options.Sink, status)

	if len(result.Candidates) > 0 && options.Sink != nil {
		emittedAt := options.Now().UTC()
		accounting.recordCandidateBatch(len(result.Candidates), emittedAt)
		if err := options.Sink.OnQuoteCandidates(context.Background(), result.Candidates, emittedAt); err != nil {
			accounting.recordSinkError()
			if runErr == nil {
				runErr = runtimeSinkError("quote candidates", err, options.AppKey, options.SessionToken)
			}
		}
		if runErr == nil && accounting.shouldFlushCandidates(options.CandidateBatchSize) {
			if err := flushRuntimeSink(context.Background(), options, accounting); err != nil {
				runErr = err
			}
		}
	}

	if options.Sink != nil {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), defaultRuntimeFinalFlushTimeout)
		if err := flushRuntimeSink(flushCtx, options, accounting); err != nil && runErr == nil {
			runErr = err
		}
		flushCancel()

		closeCtx, closeCancel := context.WithTimeout(context.Background(), defaultRuntimeFinalFlushTimeout)
		if err := options.Sink.Close(closeCtx); err != nil {
			accounting.recordSinkError()
			if runErr == nil {
				runErr = runtimeSinkError("sink close", err, options.AppKey, options.SessionToken)
			}
		}
		closeCancel()
	}

	status = statusFromLiveResult(options, group, result, startedAt)
	status.StoppedAt = formatRuntimeTime(options.Now())
	status.Running = false
	diagnostics.applyToStatus(&status, result)
	accounting.applyToStatus(&status, options)
	switch {
	case errors.Is(runErr, context.DeadlineExceeded):
		status.Status = runtimeStatusStopped
		status.StoppedReason = runtimeStopReasonDuration
		runErr = nil
	case errors.Is(runErr, context.Canceled):
		status.Status = runtimeStatusStopped
		status.StoppedReason = betfair.LiveStopReasonContextCanceled
	case runErr != nil:
		status.Status = runtimeStatusFailed
		status.StoppedReason = betfair.LiveStopReasonFailed
		status.FailureReason = redactedRuntimeErrorWithSecrets(runErr, options.AppKey, options.SessionToken)
		runErr = errors.New(status.FailureReason)
	default:
		status.Status = runtimeStatusStopped
		if strings.TrimSpace(status.StoppedReason) == "" {
			status.StoppedReason = betfair.LiveStopReasonEOF
		}
	}
	status.Readiness = RuntimeReadiness(status)
	finalizeRuntimeStatusFields(&status)
	_ = notifyRuntimeStatus(context.Background(), options.Sink, status)

	return status, runErr
}

type runtimeAccounting struct {
	rawMessagesSeen         int
	rawMessagesSinceFlush   int
	candidateBatchesSeen    int
	candidateBatchesFlushed int
	pendingCandidateBatches int
	sinkErrors              int
	latestCandidateCount    int
	lastMessageTime         time.Time
	lastCandidateTime       time.Time
	lastSinkFlushTime       time.Time
}

func (a *runtimeAccounting) recordRawMessage(receivedAt time.Time) {
	a.rawMessagesSeen++
	a.rawMessagesSinceFlush++
	a.lastMessageTime = receivedAt.UTC()
}

func (a *runtimeAccounting) recordCandidateBatch(count int, emittedAt time.Time) {
	a.candidateBatchesSeen++
	a.pendingCandidateBatches++
	a.latestCandidateCount = count
	a.lastCandidateTime = emittedAt.UTC()
}

func (a *runtimeAccounting) recordFlush(flushedAt time.Time) {
	a.rawMessagesSinceFlush = 0
	if a.pendingCandidateBatches > 0 {
		a.candidateBatchesFlushed += a.pendingCandidateBatches
		a.pendingCandidateBatches = 0
	}
	a.lastSinkFlushTime = flushedAt.UTC()
}

func (a *runtimeAccounting) recordSinkError() {
	a.sinkErrors++
}

func (a *runtimeAccounting) shouldFlushRaw(batchSize int) bool {
	return batchSize > 0 && a.rawMessagesSinceFlush >= batchSize
}

func (a *runtimeAccounting) shouldFlushCandidates(batchSize int) bool {
	return batchSize > 0 && a.latestCandidateCount >= batchSize
}

func (a *runtimeAccounting) shouldFlushInterval(now func() time.Time, interval time.Duration) bool {
	if interval <= 0 || a.lastSinkFlushTime.IsZero() || now == nil {
		return false
	}
	return !now().UTC().Before(a.lastSinkFlushTime.Add(interval))
}

func (a *runtimeAccounting) applyToStatus(status *RuntimeStatus, options RuntimeOptions) {
	status.RawMessagesSeen = a.rawMessagesSeen
	status.CandidateBatchesSeen = a.candidateBatchesSeen
	status.CandidateBatchesFlushed = a.candidateBatchesFlushed
	status.SinkErrors = a.sinkErrors
	status.LatestCandidateCount = a.latestCandidateCount
	if !a.lastMessageTime.IsZero() {
		status.LastMessageTime = formatRuntimeTime(a.lastMessageTime)
	}
	if !a.lastCandidateTime.IsZero() {
		status.LastCandidateTime = formatRuntimeTime(a.lastCandidateTime)
	}
	if !a.lastSinkFlushTime.IsZero() {
		status.LastSinkFlushTime = formatRuntimeTime(a.lastSinkFlushTime)
	}
	applyRuntimeSinkStats(status, options)
}

func applyRuntimeSinkStats(status *RuntimeStatus, options RuntimeOptions) {
	stats := RuntimeSinkStats{
		SinkType:            runtimeSinkType(options),
		DatabaseEnabled:     options.DatabaseEnabled,
		DatabaseURLRedacted: strings.TrimSpace(options.DatabaseURLRedacted),
	}
	if reporter, ok := options.Sink.(RuntimeSinkStatsReporter); ok {
		reported := reporter.RuntimeSinkStats()
		if reported.SinkType != "" {
			stats.SinkType = reported.SinkType
		}
		stats.RawMessagesBuffered = reported.RawMessagesBuffered
		stats.RawMessagesFlushed = reported.RawMessagesFlushed
		stats.RawMessagesInserted = reported.RawMessagesInserted
		stats.CandidatesBuffered = reported.CandidatesBuffered
		stats.CandidatesFlushed = reported.CandidatesFlushed
		stats.CandidatesInserted = reported.CandidatesInserted
		stats.CandidatesUnchanged = reported.CandidatesUnchanged
		stats.CandidatesSkipped = reported.CandidatesSkipped
		stats.FlushCount = reported.FlushCount
		stats.FlushErrors = reported.FlushErrors
		stats.LastFlushTime = reported.LastFlushTime
		if reported.DatabaseEnabled {
			stats.DatabaseEnabled = true
		}
		if reported.DatabaseURLRedacted != "" {
			stats.DatabaseURLRedacted = reported.DatabaseURLRedacted
		}
	}
	status.SinkType = stats.SinkType
	status.RawMessagesBuffered = stats.RawMessagesBuffered
	status.RawMessagesFlushed = stats.RawMessagesFlushed
	status.RawMessagesInserted = stats.RawMessagesInserted
	status.CandidatesBuffered = stats.CandidatesBuffered
	status.CandidatesFlushed = stats.CandidatesFlushed
	status.CandidatesInserted = stats.CandidatesInserted
	status.CandidatesUnchanged = stats.CandidatesUnchanged
	status.CandidatesSkipped = stats.CandidatesSkipped
	status.FlushCount = stats.FlushCount
	status.FlushErrors = stats.FlushErrors
	status.LastFlushTime = stats.LastFlushTime
	if status.LastSinkFlushTime == "" {
		status.LastSinkFlushTime = stats.LastFlushTime
	}
	status.DatabaseEnabled = stats.DatabaseEnabled
	status.DatabaseURLRedacted = stats.DatabaseURLRedacted
}

func runtimeSinkType(options RuntimeOptions) string {
	sinkType := strings.ToLower(strings.TrimSpace(options.SinkType))
	if sinkType != "" {
		return sinkType
	}
	if reporter, ok := options.Sink.(RuntimeSinkStatsReporter); ok {
		stats := reporter.RuntimeSinkStats()
		if strings.TrimSpace(stats.SinkType) != "" {
			return strings.ToLower(strings.TrimSpace(stats.SinkType))
		}
	}
	return runtimeSinkMemory
}

func flushRuntimeSink(ctx context.Context, options RuntimeOptions, accounting *runtimeAccounting) error {
	if options.Sink == nil {
		return nil
	}
	if err := options.Sink.Flush(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		accounting.recordSinkError()
		return runtimeSinkError("sink flush", err, options.AppKey, options.SessionToken)
	}
	accounting.recordFlush(options.Now())
	return nil
}

func normalizeRuntimeOptions(options RuntimeOptions) RuntimeOptions {
	options.TransportMode = strings.ToLower(strings.TrimSpace(options.TransportMode))
	if options.TransportMode == "" {
		options.TransportMode = runtimeTransportFake
	}
	options.SinkType = strings.ToLower(strings.TrimSpace(options.SinkType))
	if options.SinkType == "" {
		options.SinkType = runtimeSinkType(options)
	}
	options.DatabaseURLRedacted = strings.TrimSpace(options.DatabaseURLRedacted)
	options.MarketIDs = cleanRuntimeMarketIDs(options.MarketIDs)
	if options.MaxMessages == 0 {
		options.MaxMessages = defaultRuntimeMaxMessages
	}
	if options.DurationLimit == 0 {
		options.DurationLimit = defaultRuntimeDuration
	}
	if options.HeartbeatMS == 0 {
		options.HeartbeatMS = defaultRuntimeHeartbeatMS
	}
	if options.HeartbeatAgingAfter == 0 {
		options.HeartbeatAgingAfter = time.Duration(options.HeartbeatMS*2) * time.Millisecond
	}
	if options.HeartbeatStaleAfter == 0 {
		options.HeartbeatStaleAfter = time.Duration(options.HeartbeatMS*3) * time.Millisecond
	}
	if options.LadderLevels == 0 {
		options.LadderLevels = defaultRuntimeLadderLevels
	}
	if options.MaxMessageSize == 0 {
		options.MaxMessageSize = defaultRuntimeMaxMessageSize
	}
	if options.ReconnectBaseDelay == 0 {
		options.ReconnectBaseDelay = defaultRuntimeReconnectBase
	}
	if options.ReconnectMaxDelay == 0 {
		options.ReconnectMaxDelay = defaultRuntimeReconnectMax
	}
	if options.CandidateBatchSize == 0 {
		options.CandidateBatchSize = defaultRuntimeCandidateBatch
	}
	if options.RawMessageBatchSize == 0 {
		options.RawMessageBatchSize = defaultRuntimeRawMessageBatch
	}
	if options.FlushInterval == 0 {
		options.FlushInterval = defaultRuntimeFlushInterval
	}
	if options.DiagnosticSampleLimit < 0 {
		options.DiagnosticSampleLimit = 0
	}
	if options.DiagnosticSampleLimit > 0 {
		options.DiagnoseStream = true
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}
	options.AppKey = strings.TrimSpace(options.AppKey)
	options.SessionToken = strings.TrimSpace(options.SessionToken)
	if options.Sink == nil {
		options.Sink = &InMemoryRuntimeSink{}
	}
	if options.TransportFactory == nil {
		options.TransportFactory = func(group RuntimeStreamGroup) betfair.LiveTransport {
			return newFakeRuntimeTransport(defaultFakeRuntimeMessages(group.MarketIDs, options.Now()))
		}
	}
	return options
}

func validateRuntimeOptions(options RuntimeOptions) (RuntimeOptions, RuntimeStreamGroup, error) {
	if options.TransportMode != runtimeTransportFake && options.TransportMode != runtimeTransportTLS {
		return options, RuntimeStreamGroup{}, fmt.Errorf("betfair runtime supports only fake or gated tls transport, got %q", options.TransportMode)
	}
	if options.TransportMode == runtimeTransportTLS && !options.RealRuntimePilot {
		return options, RuntimeStreamGroup{}, fmt.Errorf("betfair runtime supports only fake transport in normal CLI, got %q", options.TransportMode)
	}
	if options.TransportMode == runtimeTransportFake && options.RealRuntimePilot {
		return options, RuntimeStreamGroup{}, fmt.Errorf("real-runtime pilot requires --transport tls")
	}
	if options.SinkType != runtimeSinkMemory && options.SinkType != runtimeSinkPostgres {
		return options, RuntimeStreamGroup{}, fmt.Errorf("runtime sink must be memory or postgres")
	}
	if options.TransportMode == runtimeTransportTLS {
		if strings.TrimSpace(options.AppKey) == "" {
			return options, RuntimeStreamGroup{}, fmt.Errorf("real-runtime pilot app key is required")
		}
		if strings.TrimSpace(options.SessionToken) == "" {
			return options, RuntimeStreamGroup{}, fmt.Errorf("real-runtime pilot session token is required")
		}
	}
	if len(options.MarketIDs) == 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("at least one market-id is required when --enable-runtime is set")
	}
	if len(options.MarketIDs) > maxRuntimeMarketIDCount {
		return options, RuntimeStreamGroup{}, fmt.Errorf("too many market IDs for runtime skeleton: got %d max %d", len(options.MarketIDs), maxRuntimeMarketIDCount)
	}
	for _, marketID := range options.MarketIDs {
		if marketID == "*" {
			return options, RuntimeStreamGroup{}, fmt.Errorf("wildcard market subscriptions are not allowed")
		}
	}
	if options.MaxMessages <= 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("max-messages must be positive")
	}
	if options.MaxMessages > maxRuntimeMessages {
		return options, RuntimeStreamGroup{}, fmt.Errorf("max-messages exceeds runtime skeleton limit: got %d max %d", options.MaxMessages, maxRuntimeMessages)
	}
	if options.DurationLimit <= 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("duration must be positive")
	}
	if options.DurationLimit > maxRuntimeDuration {
		return options, RuntimeStreamGroup{}, fmt.Errorf("duration exceeds runtime skeleton limit: got %s max %s", options.DurationLimit, maxRuntimeDuration)
	}
	if options.HeartbeatMS <= 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("heartbeat-ms must be positive")
	}
	if options.ConflateMS < 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("conflate-ms must be non-negative")
	}
	if options.HeartbeatAgingAfter <= 0 || options.HeartbeatStaleAfter <= 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("heartbeat thresholds must be positive")
	}
	if options.HeartbeatStaleAfter < options.HeartbeatAgingAfter {
		return options, RuntimeStreamGroup{}, fmt.Errorf("heartbeat stale threshold must be >= aging threshold")
	}
	if options.MaxReconnectAttempts < 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("max reconnect attempts must be non-negative")
	}
	if options.ReconnectBaseDelay <= 0 || options.ReconnectMaxDelay <= 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("reconnect delays must be positive")
	}
	if options.ReconnectMaxDelay < options.ReconnectBaseDelay {
		return options, RuntimeStreamGroup{}, fmt.Errorf("reconnect max delay must be >= base delay")
	}
	if options.LadderLevels < 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("ladder-levels must be non-negative")
	}
	if options.MaxMessageSize <= 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("max-message-size must be positive")
	}
	if options.CandidateBatchSize <= 0 || options.CandidateBatchSize > maxRuntimeBatchSize {
		return options, RuntimeStreamGroup{}, fmt.Errorf("candidate batch size must be between 1 and %d", maxRuntimeBatchSize)
	}
	if options.RawMessageBatchSize <= 0 || options.RawMessageBatchSize > maxRuntimeBatchSize {
		return options, RuntimeStreamGroup{}, fmt.Errorf("raw message batch size must be between 1 and %d", maxRuntimeBatchSize)
	}
	if options.FlushInterval < 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("flush interval must be non-negative")
	}
	if options.DiagnosticSampleLimit < 0 {
		return options, RuntimeStreamGroup{}, fmt.Errorf("diagnostic-sample-limit must be zero or positive")
	}
	if options.DiagnosticSampleLimit > maxRuntimeDiagnosticSampleLimit {
		return options, RuntimeStreamGroup{}, fmt.Errorf("diagnostic-sample-limit must be %d or fewer", maxRuntimeDiagnosticSampleLimit)
	}
	groupID := "fake-group-1"
	if options.TransportMode == runtimeTransportTLS {
		groupID = "real-runtime-pilot-1"
	}
	return options, RuntimeStreamGroup{ID: groupID, MarketIDs: append([]string(nil), options.MarketIDs...)}, nil
}

func RuntimeReadiness(status RuntimeStatus) string {
	if !status.Enabled || status.Status == runtimeStatusDisabled {
		return runtimeReadinessDisabled
	}
	switch status.Status {
	case runtimeStatusStarting:
		return runtimeReadinessStarting
	case runtimeStatusFailed:
		return runtimeReadinessFailed
	case runtimeStatusStopped:
		return runtimeReadinessStopped
	}
	if status.SinkErrors > 0 {
		return runtimeReadinessDegraded
	}
	switch status.HeartbeatState {
	case string(betfair.StreamStateHealthy):
		if status.Running || status.Status == runtimeStatusRunning {
			return runtimeReadinessReady
		}
		return runtimeReadinessDegraded
	case string(betfair.StreamStateStale):
		return runtimeReadinessStale
	case string(betfair.StreamStateAging), string(betfair.StreamStateUnknown), "":
		return runtimeReadinessDegraded
	default:
		return runtimeReadinessDegraded
	}
}

func statusFromLiveResult(options RuntimeOptions, group RuntimeStreamGroup, result betfair.LiveRunResult, startedAt time.Time) RuntimeStatus {
	status := baseRuntimeStatus(options)
	status.StreamGroup = group.ID
	status.MarketIDsCount = len(group.MarketIDs)
	status.Status = string(result.State)
	status.StartedAt = formatRuntimeTime(startedAt)
	status.ElapsedMS = runtimeElapsedMS(startedAt, options.Now())
	status.MessagesRead = result.MessagesParsed
	status.Connected = result.Connected
	status.Authenticated = result.Authenticated
	status.Subscribed = result.Subscribed
	status.MarketChangeMessages = result.MarketChangeMessages
	status.MarketStreamMessages = result.MarketStreamMessages
	status.MarketPayloadMessages = result.MarketPayloadMessages
	status.ServerHeartbeatMessages = result.ServerHeartbeatMessages
	status.HeartbeatMessages = result.HeartbeatMessages
	status.StatusMessages = result.ControlMessages
	status.CandidatesEmitted = len(result.Candidates)
	status.MarketsSeen = result.MarketsSeen
	status.ReconnectCount = result.ReconnectAttempts
	status.LastClock = result.LatestClock
	status.LastInitialClock = result.InitialClock
	if result.LastMessageTime != nil {
		status.LastMessageTime = formatRuntimeTime(*result.LastMessageTime)
	}
	status.HeartbeatState = string(result.HeartbeatState)
	status.StoppedReason = result.StopReason
	status.Readiness = RuntimeReadiness(status)
	finalizeRuntimeStatusFields(&status)
	return status
}

func baseRuntimeStatus(options RuntimeOptions) RuntimeStatus {
	mode := strings.ToLower(strings.TrimSpace(options.TransportMode))
	if mode == "" {
		mode = runtimeTransportFake
	}
	return RuntimeStatus{
		Enabled:              options.Enabled,
		RealRuntimePilot:     options.RealRuntimePilot,
		Status:               runtimeStatusDisabled,
		Readiness:            runtimeReadinessDisabled,
		TransportMode:        mode,
		SinkType:             runtimeSinkType(options),
		FakeTransportOnly:    mode == runtimeTransportFake && !options.RealRuntimePilot,
		CollectorIntegration: false,
		MarketStreamOnly:     true,
		MarketIDsCount:       len(options.MarketIDs),
		MaxMessagesLimit:     options.MaxMessages,
		DurationLimitSeconds: int(options.DurationLimit.Round(time.Second).Seconds()),
		DatabaseEnabled:      options.DatabaseEnabled || runtimeSinkType(options) == runtimeSinkPostgres,
		DatabaseURLRedacted:  strings.TrimSpace(options.DatabaseURLRedacted),
	}
}

func finalizeRuntimeStatusFields(status *RuntimeStatus) {
	status.LifecycleState = status.Status
	status.ReadinessState = status.Readiness
	if strings.TrimSpace(status.SinkType) == "" {
		status.SinkType = runtimeSinkMemory
	}
	status.FakeTransportOnly = status.TransportMode == runtimeTransportFake && !status.RealRuntimePilot
	status.CollectorIntegration = false
	status.MarketStreamOnly = true
	if status.LastFlushTime == "" && status.LastSinkFlushTime != "" {
		status.LastFlushTime = status.LastSinkFlushTime
	}
	if status.LastSinkFlushTime == "" && status.LastFlushTime != "" {
		status.LastSinkFlushTime = status.LastFlushTime
	}
}

func runtimeElapsedMS(startedAt, now time.Time) int64 {
	if startedAt.IsZero() || now.IsZero() {
		return 0
	}
	elapsed := now.UTC().Sub(startedAt.UTC())
	if elapsed < 0 {
		return 0
	}
	return elapsed.Milliseconds()
}

func notifyRuntimeStatus(ctx context.Context, sink RuntimeSink, status RuntimeStatus) error {
	if sink == nil {
		return nil
	}
	return sink.OnStatus(ctx, status)
}

func parseRuntimeConfig(args []string, out io.Writer) (runtimeConfig, error) {
	fs := flag.NewFlagSet("betfair-runtime", flag.ContinueOnError)
	fs.SetOutput(out)

	marketIDs := runtimeMarketIDFlag{}
	cfg := runtimeConfig{}
	fs.BoolVar(&cfg.EnableRuntime, "enable-runtime", false, "explicitly enable the Betfair runtime command")
	fs.BoolVar(&cfg.RealRuntimePilot, "real-runtime-pilot", false, "explicitly enable the manual real Betfair runtime pilot when --transport tls is selected")
	fs.BoolVar(&cfg.PostflightOnly, "postflight-only", false, "read a completed fake/local runtime postgres-sink audit report without starting runtime")
	fs.BoolVar(&cfg.PostflightChecklist, "postflight-checklist", false, "include operator pass/warning/fail checklist fields with --postflight-only")
	fs.StringVar(&cfg.Output, "output", runtimeOutputText, "output format: text or json")
	fs.StringVar(&cfg.TransportMode, "transport", runtimeTransportFake, "runtime transport mode: fake or gated tls pilot")
	fs.StringVar(&cfg.SinkType, "sink", runtimeSinkMemory, "runtime sink: memory or postgres")
	fs.StringVar(&cfg.DatabaseURLEnv, "database-url-env", "", "environment variable name containing a PostgreSQL URL when --sink postgres is explicitly selected")
	fs.StringVar(&cfg.AppKeyEnv, "app-key-env", "", "environment variable name containing the Betfair app key for --real-runtime-pilot")
	fs.StringVar(&cfg.SessionTokenEnv, "session-token-env", "", "environment variable name containing the Betfair session token for --real-runtime-pilot")
	fs.StringVar(&cfg.Address, "address", "", "Betfair stream address as host:port for --real-runtime-pilot")
	fs.StringVar(&cfg.ServerName, "server-name", "", "TLS server name for --real-runtime-pilot")
	fs.Int64Var(&cfg.RunID, "run-id", 0, "runtime run id for --postflight-only")
	fs.StringVar(&cfg.RunLabel, "run-label", "", "optional operator label for a fake/local postgres-sink runtime run")
	fs.IntVar(&cfg.SoakRuns, "soak-runs", 0, "run the bounded fake/local runtime repeatedly for soak validation")
	fs.DurationVar(&cfg.SoakDelay, "soak-delay", 0, "delay between fake/local soak runs")
	fs.StringVar(&cfg.SoakRunLabelPrefix, "soak-run-label-prefix", "", "optional safe label prefix for fake/local soak telemetry runs")
	fs.BoolVar(&cfg.DiagnoseStream, "diagnose-stream", false, "include redacted structured stream diagnostics in the runtime report")
	fs.IntVar(&cfg.DiagnosticSampleLimit, "diagnostic-sample-limit", 0, "include up to this many sanitized market-change diagnostic samples")
	fs.IntVar(&cfg.MaxMessages, "max-messages", defaultRuntimeMaxMessages, "maximum post-subscription stream messages to read")
	fs.DurationVar(&cfg.DurationLimit, "duration", defaultRuntimeDuration, "maximum runtime duration")
	fs.DurationVar(&cfg.DialTimeout, "dial-timeout", defaultRuntimeDialTimeout, "TLS dial timeout for --real-runtime-pilot")
	fs.DurationVar(&cfg.ReadTimeout, "read-timeout", defaultRuntimeReadTimeout, "per-read timeout for --real-runtime-pilot")
	fs.DurationVar(&cfg.WriteTimeout, "write-timeout", defaultRuntimeWriteTimeout, "per-write timeout for --real-runtime-pilot")
	fs.IntVar(&cfg.HeartbeatMS, "heartbeat-ms", defaultRuntimeHeartbeatMS, "Betfair stream heartbeat interval in milliseconds")
	fs.IntVar(&cfg.ConflateMS, "conflate-ms", 0, "Betfair stream conflate interval in milliseconds")
	fs.Var(&marketIDs, "market-id", "explicit Betfair market ID; may be repeated or comma-separated")
	fs.IntVar(&cfg.LadderLevels, "ladder-levels", defaultRuntimeLadderLevels, "fake Betfair price ladder depth")
	fs.IntVar(&cfg.MaxMessageSize, "max-message-size", defaultRuntimeMaxMessageSize, "maximum Betfair stream message size in bytes")
	fs.IntVar(&cfg.MaxReconnectAttempts, "max-reconnects", 0, "maximum reconnect attempts for fake runtime")
	fs.DurationVar(&cfg.ReconnectBase, "reconnect-base-delay", defaultRuntimeReconnectBase, "base reconnect delay")
	fs.DurationVar(&cfg.ReconnectMax, "reconnect-max-delay", defaultRuntimeReconnectMax, "maximum reconnect delay")

	if err := fs.Parse(args); err != nil {
		return runtimeConfig{}, err
	}

	cfg.Output = strings.ToLower(strings.TrimSpace(cfg.Output))
	cfg.TransportMode = strings.ToLower(strings.TrimSpace(cfg.TransportMode))
	cfg.SinkType = strings.ToLower(strings.TrimSpace(cfg.SinkType))
	cfg.DatabaseURLEnv = strings.TrimSpace(cfg.DatabaseURLEnv)
	cfg.AppKeyEnv = strings.TrimSpace(cfg.AppKeyEnv)
	cfg.SessionTokenEnv = strings.TrimSpace(cfg.SessionTokenEnv)
	cfg.Address = strings.TrimSpace(cfg.Address)
	cfg.ServerName = strings.TrimSpace(cfg.ServerName)
	cfg.RunLabel = strings.TrimSpace(cfg.RunLabel)
	cfg.SoakRunLabelPrefix = strings.TrimSpace(cfg.SoakRunLabelPrefix)
	cfg.MarketIDs = marketIDs.values()
	if cfg.DiagnosticSampleLimit > 0 {
		cfg.DiagnoseStream = true
	}
	if cfg.Output == "" {
		cfg.Output = runtimeOutputText
	}
	if cfg.Output != runtimeOutputText && cfg.Output != runtimeOutputJSON {
		return cfg, fmt.Errorf("output must be text or json")
	}
	if cfg.SinkType == "" {
		cfg.SinkType = runtimeSinkMemory
	}
	if cfg.SinkType != runtimeSinkMemory && cfg.SinkType != runtimeSinkPostgres {
		return cfg, fmt.Errorf("runtime sink must be memory or postgres")
	}
	if cfg.DiagnosticSampleLimit < 0 {
		return cfg, fmt.Errorf("diagnostic-sample-limit must be zero or positive")
	}
	if cfg.DiagnosticSampleLimit > maxRuntimeDiagnosticSampleLimit {
		return cfg, fmt.Errorf("diagnostic-sample-limit must be %d or fewer", maxRuntimeDiagnosticSampleLimit)
	}
	if cfg.RunLabel != "" {
		if err := validateRuntimeRunLabel(cfg.RunLabel); err != nil {
			return cfg, err
		}
	}
	if cfg.SoakRunLabelPrefix != "" {
		if err := validateRuntimeRunLabel(cfg.SoakRunLabelPrefix); err != nil {
			return cfg, fmt.Errorf("soak-run-label-prefix invalid: %w", err)
		}
	}
	if err := validateRuntimeSoakConfig(cfg); err != nil {
		return cfg, err
	}
	if err := validateRuntimeRealPilotConfig(&cfg); err != nil {
		return cfg, err
	}
	if cfg.PostflightChecklist && !cfg.PostflightOnly {
		return cfg, fmt.Errorf("--postflight-checklist requires --postflight-only")
	}
	if cfg.PostflightOnly {
		if cfg.TransportMode != "" && cfg.TransportMode != runtimeTransportFake {
			return cfg, fmt.Errorf("--postflight-only supports fake transport metadata only in this phase")
		}
		if cfg.RunID <= 0 {
			return cfg, fmt.Errorf("--run-id is required and must be positive when --postflight-only is used")
		}
		if cfg.DatabaseURLEnv == "" {
			return cfg, fmt.Errorf("--database-url-env is required when --postflight-only is used")
		}
		value, ok := os.LookupEnv(cfg.DatabaseURLEnv)
		if !ok || strings.TrimSpace(value) == "" {
			return cfg, fmt.Errorf("database URL env var %q is not set or is empty", cfg.DatabaseURLEnv)
		}
		cfg.DatabaseURL = strings.TrimSpace(value)
		cfg.DatabaseURLRedacted = redactRuntimeDatabaseURL(cfg.DatabaseURL)
		cfg.SinkType = runtimeSinkPostgres
		return cfg, nil
	}
	if !cfg.EnableRuntime {
		return cfg, nil
	}
	if cfg.SinkType == runtimeSinkPostgres {
		if cfg.DatabaseURLEnv == "" {
			return cfg, fmt.Errorf("--database-url-env is required when --sink postgres is used")
		}
		value, ok := os.LookupEnv(cfg.DatabaseURLEnv)
		if !ok || strings.TrimSpace(value) == "" {
			return cfg, fmt.Errorf("database URL env var %q is not set or is empty", cfg.DatabaseURLEnv)
		}
		cfg.DatabaseURL = strings.TrimSpace(value)
		cfg.DatabaseURLRedacted = redactRuntimeDatabaseURL(cfg.DatabaseURL)
	}
	_, _, err := validateRuntimeOptions(normalizeRuntimeOptions(cfg.toOptions(nil)))
	return cfg, err
}

func (cfg runtimeConfig) toOptions(sink RuntimeSink) RuntimeOptions {
	return RuntimeOptions{
		Enabled:               cfg.EnableRuntime,
		RealRuntimePilot:      cfg.RealRuntimePilot,
		TransportMode:         cfg.TransportMode,
		SinkType:              cfg.SinkType,
		DatabaseEnabled:       cfg.SinkType == runtimeSinkPostgres,
		DatabaseURLRedacted:   cfg.DatabaseURLRedacted,
		MarketIDs:             cfg.MarketIDs,
		MaxMessages:           cfg.MaxMessages,
		DurationLimit:         cfg.DurationLimit,
		HeartbeatAgingAfter:   time.Duration(cfg.HeartbeatMS*2) * time.Millisecond,
		HeartbeatStaleAfter:   time.Duration(cfg.HeartbeatMS*3) * time.Millisecond,
		MaxReconnectAttempts:  cfg.MaxReconnectAttempts,
		ReconnectBaseDelay:    cfg.ReconnectBase,
		ReconnectMaxDelay:     cfg.ReconnectMax,
		HeartbeatMS:           cfg.HeartbeatMS,
		ConflateMS:            cfg.ConflateMS,
		AppKey:                cfg.AppKey,
		SessionToken:          cfg.SessionToken,
		LadderLevels:          cfg.LadderLevels,
		MaxMessageSize:        cfg.MaxMessageSize,
		Sink:                  sink,
		DiagnoseStream:        cfg.DiagnoseStream,
		DiagnosticSampleLimit: cfg.DiagnosticSampleLimit,
	}
}

func runCommand(ctx context.Context, args []string, out io.Writer) int {
	return runCommandWithStoreFactory(ctx, args, out, defaultRuntimeStoreFactory)
}

type runtimeStoreFactory func(context.Context, string) (runtimeStore, error)

func runCommandWithStoreFactory(ctx context.Context, args []string, out io.Writer, storeFactory runtimeStoreFactory) int {
	return runCommandWithFactories(ctx, args, out, storeFactory, nil)
}

func runCommandWithFactories(ctx context.Context, args []string, out io.Writer, storeFactory runtimeStoreFactory, transportFactory func(RuntimeStreamGroup) betfair.LiveTransport) int {
	cfg, err := parseRuntimeConfig(args, out)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitRuntimeSuccess
		}
		status := RuntimeStatus{
			Enabled:             hasRuntimeFlag(args, "enable-runtime"),
			RealRuntimePilot:    hasRuntimeFlag(args, "real-runtime-pilot"),
			PostflightOnly:      hasRuntimeFlag(args, "postflight-only"),
			Status:              runtimeStatusFailed,
			Readiness:           runtimeReadinessFailed,
			TransportMode:       transportModeFromArgs(args),
			SinkType:            sinkTypeFromConfigOrArgs(cfg, args),
			RunID:               runIDFromConfigOrArgs(cfg, args),
			RunLabel:            cfg.RunLabel,
			DatabaseEnabled:     sinkTypeFromConfigOrArgs(cfg, args) == runtimeSinkPostgres,
			DatabaseURLRedacted: cfg.DatabaseURLRedacted,
			StoppedReason:       runtimeStopReasonValidation,
			FailureReason:       redactedRuntimeErrorWithSecrets(err, cfg.DatabaseURL, cfg.AppKey, cfg.SessionToken),
		}
		_ = writeRuntimeStatus(out, status, outputFromArgs(args))
		return exitRuntimeValidation
	}

	if cfg.PostflightOnly {
		status, err := runRuntimePostflight(ctx, cfg, storeFactory)
		_ = writeRuntimeStatus(out, status, cfg.Output)
		return exitCodeForRuntimeError(err)
	}

	if cfg.SoakRuns > 0 {
		report, err := runRuntimeSoak(ctx, cfg, storeFactory, transportFactory)
		_ = writeRuntimeSoakReport(out, report, cfg.Output)
		return exitCodeForRuntimeError(err)
	}

	status, err := runRuntime(ctx, cfg, storeFactory, transportFactory)
	_ = writeRuntimeStatus(out, status, cfg.Output)
	return exitCodeForRuntimeError(err)
}

func runRuntime(ctx context.Context, cfg runtimeConfig, storeFactory runtimeStoreFactory, transportFactory func(RuntimeStreamGroup) betfair.LiveTransport) (RuntimeStatus, error) {
	var st runtimeStore
	var runID int64
	var runStreamID string
	if cfg.SinkType == runtimeSinkPostgres && cfg.EnableRuntime {
		opened, err := openRuntimeStore(ctx, cfg, storeFactory)
		if err != nil {
			return RuntimeStatus{
				Enabled:             cfg.EnableRuntime,
				RealRuntimePilot:    cfg.RealRuntimePilot,
				Status:              runtimeStatusFailed,
				Readiness:           runtimeReadinessFailed,
				TransportMode:       cfg.TransportMode,
				SinkType:            cfg.SinkType,
				RunLabel:            cfg.RunLabel,
				DatabaseEnabled:     true,
				DatabaseURLRedacted: cfg.DatabaseURLRedacted,
				StoppedReason:       runtimeStopReasonValidation,
				FailureReason:       redactedRuntimeErrorWithSecrets(err, cfg.DatabaseURL, cfg.AppKey, cfg.SessionToken),
			}, err
		}
		st = opened
		defer func() { _ = st.Close() }()

		id, err := st.StartBetfairRuntimeRun(ctx, runtimeStartInput(cfg, ""))
		if err != nil {
			return RuntimeStatus{
				Enabled:             cfg.EnableRuntime,
				RealRuntimePilot:    cfg.RealRuntimePilot,
				Status:              runtimeStatusFailed,
				Readiness:           runtimeReadinessFailed,
				TransportMode:       cfg.TransportMode,
				SinkType:            cfg.SinkType,
				RunLabel:            cfg.RunLabel,
				DatabaseEnabled:     true,
				DatabaseURLRedacted: cfg.DatabaseURLRedacted,
				StoppedReason:       runtimeStopReasonValidation,
				FailureReason:       redactedRuntimeErrorWithSecrets(err, cfg.DatabaseURL, cfg.AppKey, cfg.SessionToken),
			}, err
		}
		runID = id
		runStreamID = runtimeStreamID(runID)
	}

	sink, err := runtimeSinkForConfig(cfg, st, runStreamID)
	if err != nil {
		status := RuntimeStatus{
			Enabled:             cfg.EnableRuntime,
			RealRuntimePilot:    cfg.RealRuntimePilot,
			Status:              runtimeStatusFailed,
			Readiness:           runtimeReadinessFailed,
			TransportMode:       cfg.TransportMode,
			SinkType:            cfg.SinkType,
			RunID:               runID,
			RunLabel:            cfg.RunLabel,
			DatabaseEnabled:     cfg.SinkType == runtimeSinkPostgres,
			DatabaseURLRedacted: cfg.DatabaseURLRedacted,
			StoppedReason:       runtimeStopReasonValidation,
			FailureReason:       redactedRuntimeErrorWithSecrets(err, cfg.DatabaseURL, cfg.AppKey, cfg.SessionToken),
		}
		if st != nil && runID > 0 {
			finalizeRuntimeStatusFields(&status)
			_ = st.FinishBetfairRuntimeRun(context.Background(), runID, runtimeFinishInputFromStatus(status, exitRuntimeRuntime, runStreamID))
		}
		return status, err
	}

	options := cfg.toOptions(sink)
	if transportFactory != nil {
		options.TransportFactory = transportFactory
	} else if cfg.TransportMode == runtimeTransportTLS {
		options.TransportFactory = runtimeTLSTransportFactory(cfg)
	}
	status, runErr := NewRuntimeSupervisor(options).Run(ctx)
	status.RunID = runID
	status.RunLabel = cfg.RunLabel
	if runErr == nil && runID > 0 {
		status.StoppedReason = strings.TrimSpace(status.StoppedReason)
	}
	if st != nil && runID > 0 {
		finishErr := st.FinishBetfairRuntimeRun(context.Background(), runID, runtimeFinishInputFromStatus(status, exitCodeForRuntimeError(runErr), runStreamID))
		if finishErr != nil && runErr == nil {
			runErr = fmt.Errorf("finish betfair runtime telemetry: %w", finishErr)
			status.Status = runtimeStatusFailed
			status.Readiness = runtimeReadinessFailed
			status.FailureReason = redactedRuntimeErrorWithSecrets(runErr, cfg.DatabaseURL, cfg.AppKey, cfg.SessionToken)
			finalizeRuntimeStatusFields(&status)
		}
	}
	return status, runErr
}

func runRuntimeSoak(ctx context.Context, cfg runtimeConfig, storeFactory runtimeStoreFactory, transportFactory func(RuntimeStreamGroup) betfair.LiveTransport) (RuntimeSoakReport, error) {
	startedAt := time.Now().UTC()
	report := RuntimeSoakReport{
		Enabled:              cfg.EnableRuntime,
		Soak:                 true,
		TransportMode:        cfg.TransportMode,
		SinkType:             cfg.SinkType,
		DatabaseEnabled:      cfg.SinkType == runtimeSinkPostgres,
		DatabaseURLRedacted:  cfg.DatabaseURLRedacted,
		RunLabelPrefix:       runtimeSoakLabelBase(cfg),
		SoakRunsRequested:    cfg.SoakRuns,
		RuntimeRunIDs:        []int64{},
		FailedRunIDs:         []int64{},
		ReadinessStateCounts: []RuntimeReadinessStateCount{},
		Warnings:             []string{},
		FakeTransportOnly:    true,
		CollectorIntegration: false,
		MarketStreamOnly:     true,
	}
	readinessCounts := map[string]int{}
	var firstErr error

	for i := 0; i < cfg.SoakRuns; i++ {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			report.Warnings = append(report.Warnings, redactedRuntimeErrorWithSecrets(err, cfg.DatabaseURL))
			break
		}

		runCfg := cfg
		runCfg.PostflightOnly = false
		runCfg.RunID = 0
		runCfg.SoakRuns = 0
		runCfg.SoakDelay = 0
		runCfg.RunLabel = runtimeSoakRunLabel(cfg, i+1)

		status, err := runRuntime(ctx, runCfg, storeFactory, transportFactory)
		aggregateRuntimeSoakStatus(&report, readinessCounts, status)
		failed := err != nil || status.Status == runtimeStatusFailed || status.Readiness == runtimeReadinessFailed
		if failed {
			report.SoakRunsFailed++
			if status.RunID > 0 {
				report.FailedRunIDs = append(report.FailedRunIDs, status.RunID)
			}
			if err != nil {
				report.Warnings = append(report.Warnings, fmt.Sprintf("run %d failed: %s", i+1, redactedRuntimeErrorWithSecrets(err, cfg.DatabaseURL)))
				if firstErr == nil {
					firstErr = err
				}
			} else if status.FailureReason != "" {
				report.Warnings = append(report.Warnings, fmt.Sprintf("run %d failed: %s", i+1, status.FailureReason))
				if firstErr == nil {
					firstErr = fmt.Errorf("soak run %d failed: %s", i+1, status.FailureReason)
				}
			} else if firstErr == nil {
				firstErr = fmt.Errorf("soak run %d failed", i+1)
			}
		} else {
			report.SoakRunsCompleted++
		}

		if i < cfg.SoakRuns-1 && cfg.SoakDelay > 0 {
			timer := time.NewTimer(cfg.SoakDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				if firstErr == nil {
					firstErr = ctx.Err()
				}
				report.Warnings = append(report.Warnings, redactedRuntimeErrorWithSecrets(ctx.Err(), cfg.DatabaseURL))
				i = cfg.SoakRuns
			case <-timer.C:
			}
		}
	}

	if cfg.SinkType == runtimeSinkPostgres && len(report.RuntimeRunIDs) > 0 {
		validationWarnings, validationErr := validateRuntimeSoakPostflight(ctx, cfg, storeFactory, report.RuntimeRunIDs)
		report.Warnings = append(report.Warnings, validationWarnings...)
		if validationErr != nil {
			if firstErr == nil {
				firstErr = validationErr
			}
		}
		if report.SoakRunsCompleted > 1 && report.TotalCandidatesEmitted > 0 && report.TotalCandidatesUnchanged == 0 {
			warning := "post-soak validation found no unchanged candidates across repeated fake/local runs"
			report.Warnings = append(report.Warnings, warning)
			if firstErr == nil {
				firstErr = fmt.Errorf("%s", warning)
			}
		}
	}

	report.ElapsedMS = runtimeElapsedMS(startedAt, time.Now().UTC())
	report.WarningCount = len(report.Warnings)
	report.ReadinessStateCounts = runtimeReadinessCounts(readinessCounts)
	if firstErr == nil && report.SoakRunsFailed > 0 {
		firstErr = fmt.Errorf("betfair runtime soak had %d failed runs", report.SoakRunsFailed)
	}
	return report, firstErr
}

func aggregateRuntimeSoakStatus(report *RuntimeSoakReport, readinessCounts map[string]int, status RuntimeStatus) {
	report.TotalMessagesRead += status.MessagesRead
	report.TotalRawMessagesInserted += status.RawMessagesInserted
	report.TotalCandidatesEmitted += status.CandidatesEmitted
	report.TotalCandidatesInserted += status.CandidatesInserted
	report.TotalCandidatesUnchanged += status.CandidatesUnchanged
	report.TotalCandidatesSkipped += status.CandidatesSkipped
	report.TotalFlushErrors += status.FlushErrors
	report.TotalSinkErrors += status.SinkErrors
	if status.RunID > 0 {
		report.RuntimeRunIDs = append(report.RuntimeRunIDs, status.RunID)
	}
	readiness := strings.TrimSpace(status.ReadinessState)
	if readiness == "" {
		readiness = strings.TrimSpace(status.Readiness)
	}
	if readiness == "" {
		readiness = "unknown"
	}
	readinessCounts[readiness]++
}

func validateRuntimeSoakPostflight(ctx context.Context, cfg runtimeConfig, storeFactory runtimeStoreFactory, runIDs []int64) ([]string, error) {
	st, err := openRuntimeStore(ctx, cfg, storeFactory)
	if err != nil {
		return []string{"post-soak validation failed to open store: " + redactedRuntimeErrorWithSecrets(err, cfg.DatabaseURL)}, err
	}
	defer func() { _ = st.Close() }()

	warnings := []string{}
	var firstErr error
	for _, runID := range runIDs {
		postflight, err := st.GetBetfairRuntimePostflight(ctx, runID)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("run %d postflight failed: %s", runID, redactedRuntimeErrorWithSecrets(err, cfg.DatabaseURL)))
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		run := postflight.Run
		if run.Status == model.BetfairRuntimeRunStatusFailed || run.Status == model.BetfairRuntimeRunStatusCanceled {
			warnings = append(warnings, fmt.Sprintf("run %d status=%s", runID, run.Status))
			if firstErr == nil {
				firstErr = fmt.Errorf("runtime soak postflight run %d status=%s", runID, run.Status)
			}
		}
		if run.SinkErrors > 0 || run.FlushErrors > 0 {
			warnings = append(warnings, fmt.Sprintf("run %d sink_errors=%d flush_errors=%d", runID, run.SinkErrors, run.FlushErrors))
			if firstErr == nil {
				firstErr = fmt.Errorf("runtime soak postflight run %d had sink or flush errors", runID)
			}
		}
		if postflight.LinkedRawMessageCount == 0 || run.RawMessagesInserted == 0 {
			warnings = append(warnings, fmt.Sprintf("run %d has no linked raw stream messages", runID))
			if firstErr == nil {
				firstErr = fmt.Errorf("runtime soak postflight run %d has no linked raw stream messages", runID)
			}
		}
		if run.CandidatesEmitted == 0 {
			warnings = append(warnings, fmt.Sprintf("run %d emitted no candidates", runID))
			if firstErr == nil {
				firstErr = fmt.Errorf("runtime soak postflight run %d emitted no candidates", runID)
			}
		}
		if postflight.LatestQuoteCount == 0 {
			warnings = append(warnings, fmt.Sprintf("run %d has no latest Betfair exchange quotes", runID))
			if firstErr == nil {
				firstErr = fmt.Errorf("runtime soak postflight run %d has no latest Betfair exchange quotes", runID)
			}
		}
		for _, warning := range postflight.Warnings {
			warning = strings.TrimSpace(warning)
			if warning == "" {
				continue
			}
			lower := strings.ToLower(warning)
			if strings.Contains(lower, "mapped") || strings.Contains(lower, "ready") || strings.Contains(lower, "comparison") {
				warnings = append(warnings, fmt.Sprintf("run %d postflight warning: %s", runID, warning))
			}
		}
	}
	return warnings, firstErr
}

func openRuntimeStore(ctx context.Context, cfg runtimeConfig, storeFactory runtimeStoreFactory) (runtimeStore, error) {
	if storeFactory == nil {
		storeFactory = defaultRuntimeStoreFactory
	}
	st, err := storeFactory(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("open runtime postgres sink from env var %s: %w", cfg.DatabaseURLEnv, err)
	}
	return st, nil
}

func runtimeSinkForConfig(cfg runtimeConfig, st runtimeStore, streamID string) (RuntimeSink, error) {
	if cfg.SinkType != runtimeSinkPostgres {
		return &InMemoryRuntimeSink{}, nil
	}
	if !cfg.EnableRuntime {
		return &InMemoryRuntimeSink{}, nil
	}
	if st == nil {
		return nil, fmt.Errorf("runtime postgres sink store is nil")
	}
	sink, err := NewPostgresRuntimeSink(PostgresRuntimeSinkOptions{
		Saver:               st,
		StreamID:            streamID,
		DatabaseURL:         cfg.DatabaseURL,
		DatabaseURLRedacted: cfg.DatabaseURLRedacted,
	})
	if err != nil {
		return nil, err
	}
	return sink, nil
}

func runtimeTLSTransportFactory(cfg runtimeConfig) func(RuntimeStreamGroup) betfair.LiveTransport {
	return func(RuntimeStreamGroup) betfair.LiveTransport {
		return betfair.NewTLSLiveTransport(betfair.TLSLiveTransportOptions{
			Address:      cfg.Address,
			ServerName:   cfg.ServerName,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		})
	}
}

func defaultRuntimeStoreFactory(ctx context.Context, databaseURL string) (runtimeStore, error) {
	return store.Open(ctx, databaseURL, true, normalize.FreshnessPolicy{})
}

func runRuntimePostflight(ctx context.Context, cfg runtimeConfig, storeFactory runtimeStoreFactory) (RuntimeStatus, error) {
	st, err := openRuntimeStore(ctx, cfg, storeFactory)
	if err != nil {
		status := RuntimeStatus{
			Enabled:             cfg.EnableRuntime,
			RealRuntimePilot:    cfg.RealRuntimePilot,
			PostflightOnly:      true,
			RunID:               cfg.RunID,
			RunLabel:            cfg.RunLabel,
			Status:              runtimeStatusFailed,
			Readiness:           runtimeReadinessFailed,
			TransportMode:       cfg.TransportMode,
			SinkType:            runtimeSinkPostgres,
			DatabaseEnabled:     true,
			DatabaseURLRedacted: cfg.DatabaseURLRedacted,
			StoppedReason:       runtimeStatusFailed,
			FailureReason:       redactedRuntimeErrorWithSecrets(err, cfg.DatabaseURL, cfg.AppKey, cfg.SessionToken),
		}
		applyRuntimePostflightClassification(&status, cfg.PostflightChecklist, "postflight_store_open_failed")
		return status, err
	}
	defer func() { _ = st.Close() }()

	postflight, err := st.GetBetfairRuntimePostflight(ctx, cfg.RunID)
	if err != nil {
		status := RuntimeStatus{
			Enabled:             cfg.EnableRuntime,
			RealRuntimePilot:    cfg.RealRuntimePilot,
			PostflightOnly:      true,
			RunID:               cfg.RunID,
			RunLabel:            cfg.RunLabel,
			Status:              runtimeStatusFailed,
			Readiness:           runtimeReadinessFailed,
			TransportMode:       cfg.TransportMode,
			SinkType:            runtimeSinkPostgres,
			DatabaseEnabled:     true,
			DatabaseURLRedacted: cfg.DatabaseURLRedacted,
			StoppedReason:       runtimeStatusFailed,
			FailureReason:       redactedRuntimeErrorWithSecrets(err, cfg.DatabaseURL, cfg.AppKey, cfg.SessionToken),
		}
		applyRuntimePostflightClassification(&status, cfg.PostflightChecklist, "runtime_run_not_found_or_unreadable")
		return status, err
	}
	status := runtimeStatusFromPostflight(cfg, postflight)
	applyRuntimePostflightClassification(&status, cfg.PostflightChecklist)
	return status, nil
}

func runtimeStartInput(cfg runtimeConfig, streamID string) model.BetfairRuntimeRunStart {
	runtimeMode := runtimeModeFake
	if cfg.RealRuntimePilot {
		runtimeMode = runtimeModeRealPilot
	}
	return model.BetfairRuntimeRunStart{
		RunLabel:             cfg.RunLabel,
		StreamID:             streamID,
		RuntimeMode:          runtimeMode,
		TransportMode:        cfg.TransportMode,
		SinkType:             cfg.SinkType,
		StartedAt:            time.Now().UTC(),
		MarketIDs:            cfg.MarketIDs,
		DurationLimitSeconds: secondsPtr(cfg.DurationLimit),
		MaxMessages:          intPtr(cfg.MaxMessages),
	}
}

func runtimeFinishInputFromStatus(status RuntimeStatus, exitCode int, streamID string) model.BetfairRuntimeRunFinish {
	telemetryStatus := model.BetfairRuntimeRunStatusSuccess
	if exitCode == exitRuntimeCanceled {
		telemetryStatus = model.BetfairRuntimeRunStatusCanceled
	} else if exitCode != exitRuntimeSuccess || status.Status == runtimeStatusFailed {
		telemetryStatus = model.BetfairRuntimeRunStatusFailed
	}
	return model.BetfairRuntimeRunFinish{
		StreamID:                streamID,
		FinishedAt:              time.Now().UTC(),
		Status:                  telemetryStatus,
		ReadinessState:          status.ReadinessState,
		StoppedReason:           status.StoppedReason,
		FailureReason:           status.FailureReason,
		MessagesRead:            status.MessagesRead,
		MarketChangeMessages:    status.MarketChangeMessages,
		HeartbeatMessages:       status.HeartbeatMessages,
		StatusMessages:          status.StatusMessages,
		RawMessagesSeen:         status.RawMessagesSeen,
		RawMessagesFlushed:      status.RawMessagesFlushed,
		RawMessagesInserted:     status.RawMessagesInserted,
		CandidatesEmitted:       status.CandidatesEmitted,
		CandidateBatchesSeen:    status.CandidateBatchesSeen,
		CandidateBatchesFlushed: status.CandidateBatchesFlushed,
		CandidatesInserted:      status.CandidatesInserted,
		CandidatesUnchanged:     status.CandidatesUnchanged,
		CandidatesSkipped:       status.CandidatesSkipped,
		FlushCount:              status.FlushCount,
		FlushErrors:             status.FlushErrors,
		SinkErrors:              status.SinkErrors,
		MarketsSeen:             status.MarketsSeen,
		ReconnectCount:          status.ReconnectCount,
		LastClock:               status.LastClock,
		LastInitialClock:        status.LastInitialClock,
		LastMessageTime:         parseRuntimeTimePtr(status.LastMessageTime),
		LastCandidateTime:       parseRuntimeTimePtr(status.LastCandidateTime),
		LastSinkFlushTime:       parseRuntimeTimePtr(status.LastSinkFlushTime),
		HeartbeatState:          status.HeartbeatState,
		ElapsedMS:               status.ElapsedMS,
		ExitCode:                exitCode,
	}
}

func runtimeStatusFromPostflight(cfg runtimeConfig, postflight model.BetfairRuntimePostflight) RuntimeStatus {
	run := postflight.Run
	status := RuntimeStatus{
		Enabled:                 cfg.EnableRuntime,
		RealRuntimePilot:        run.RuntimeMode == runtimeModeRealPilot,
		PostflightOnly:          true,
		RunID:                   run.ID,
		RunLabel:                run.RunLabel,
		Status:                  run.Status,
		Readiness:               run.ReadinessState,
		TransportMode:           run.TransportMode,
		SinkType:                run.SinkType,
		StreamGroup:             run.StreamID,
		MarketIDsCount:          run.MarketIDsCount,
		StartedAt:               formatRuntimeTime(run.StartedAt),
		RawMessagesSeen:         run.RawMessagesSeen,
		RawMessagesFlushed:      run.RawMessagesFlushed,
		RawMessagesInserted:     run.RawMessagesInserted,
		MessagesRead:            run.MessagesRead,
		MarketChangeMessages:    run.MarketChangeMessages,
		HeartbeatMessages:       run.HeartbeatMessages,
		StatusMessages:          run.StatusMessages,
		CandidatesEmitted:       run.CandidatesEmitted,
		CandidateBatchesSeen:    run.CandidateBatchesSeen,
		CandidateBatchesFlushed: run.CandidateBatchesFlushed,
		CandidatesInserted:      run.CandidatesInserted,
		CandidatesUnchanged:     run.CandidatesUnchanged,
		CandidatesSkipped:       run.CandidatesSkipped,
		FlushCount:              run.FlushCount,
		FlushErrors:             run.FlushErrors,
		SinkErrors:              run.SinkErrors,
		MarketsSeen:             run.MarketsSeen,
		ReconnectCount:          run.ReconnectCount,
		LastClock:               run.LastClock,
		LastInitialClock:        run.LastInitialClock,
		HeartbeatState:          run.HeartbeatState,
		StoppedReason:           runtimeStopReasonPostflightOnly,
		FailureReason:           run.FailureReason,
		DatabaseEnabled:         true,
		DatabaseURLRedacted:     cfg.DatabaseURLRedacted,
		LinkedRawMessageCount:   postflight.LinkedRawMessageCount,
		QuoteHistoryRows:        postflight.QuoteHistoryRows,
		LatestQuoteCount:        postflight.LatestQuoteCount,
		MappedQuoteCount:        postflight.MappedQuoteCount,
		ReadyRunnerBookCount:    postflight.ReadyRunnerBookCount,
		ReadyComparisonCount:    postflight.ReadyComparisonCount,
		Warnings:                append([]string(nil), postflight.Warnings...),
	}
	if run.MaxMessages != nil {
		status.MaxMessagesLimit = *run.MaxMessages
	}
	if run.DurationLimitSeconds != nil {
		status.DurationLimitSeconds = *run.DurationLimitSeconds
	}
	if run.FinishedAt != nil {
		status.StoppedAt = formatRuntimeTime(*run.FinishedAt)
	}
	if run.ElapsedMS != nil {
		status.ElapsedMS = *run.ElapsedMS
	}
	if run.LastMessageTime != nil {
		status.LastMessageTime = formatRuntimeTime(*run.LastMessageTime)
	}
	if run.LastCandidateTime != nil {
		status.LastCandidateTime = formatRuntimeTime(*run.LastCandidateTime)
	}
	if run.LastSinkFlushTime != nil {
		status.LastSinkFlushTime = formatRuntimeTime(*run.LastSinkFlushTime)
		status.LastFlushTime = status.LastSinkFlushTime
	}
	if status.Readiness == "" {
		status.Readiness = runtimeReadinessStopped
	}
	finalizeRuntimeStatusFields(&status)
	return status
}

func applyRuntimePostflightClassification(status *RuntimeStatus, includeChecklist bool, extraFailReasons ...string) {
	if status == nil {
		return
	}
	classification, failReasons, warningReasons := runtimePostflightClassification(*status)
	for _, reason := range extraFailReasons {
		reason = strings.TrimSpace(reason)
		if reason == "" {
			continue
		}
		failReasons = appendUniqueRuntimeReason(failReasons, reason)
	}
	if len(failReasons) > 0 {
		classification = runtimePostflightFail
	}
	status.PostflightClassification = classification
	status.PostflightFailReasons = failReasons
	status.PostflightWarningReasons = warningReasons
	if includeChecklist {
		checklist := runtimePostflightChecklist(*status)
		status.PostflightChecklist = &checklist
	}
	finalizeRuntimeStatusFields(status)
}

func runtimePostflightClassification(status RuntimeStatus) (string, []string, []string) {
	failReasons := []string{}
	warningReasons := []string{}
	if status.RunID <= 0 {
		failReasons = append(failReasons, "runtime_run_not_found")
	}
	if status.Status == runtimeStatusFailed || status.Status == model.BetfairRuntimeRunStatusFailed {
		failReasons = append(failReasons, "status_failed")
	}
	if strings.TrimSpace(status.FailureReason) != "" {
		failReasons = append(failReasons, "failure_reason_present")
	}
	if status.MessagesRead == 0 {
		failReasons = append(failReasons, "zero_messages_read")
	}
	if status.CandidatesEmitted == 0 {
		failReasons = append(failReasons, "zero_candidates_emitted")
	}
	if status.SinkType == runtimeSinkPostgres {
		if status.RawMessagesInserted == 0 {
			failReasons = append(failReasons, "postgres_zero_raw_messages_inserted")
		}
		if status.CandidatesInserted == 0 && status.CandidatesUnchanged == 0 {
			failReasons = append(failReasons, "postgres_zero_candidates_inserted_or_unchanged")
		}
	}
	if status.HeartbeatState == string(betfair.StreamStateStale) {
		failReasons = append(failReasons, "heartbeat_stale")
	}
	if status.ReadinessState == runtimeReadinessFailed || status.ReadinessState == runtimeReadinessStale {
		failReasons = append(failReasons, "readiness_"+status.ReadinessState)
	}
	if status.Readiness == runtimeReadinessFailed || status.Readiness == runtimeReadinessStale {
		failReasons = appendUniqueRuntimeReason(failReasons, "readiness_"+status.Readiness)
	}
	if status.SinkErrors > 0 {
		failReasons = append(failReasons, "sink_errors")
	}
	if status.FlushErrors > 0 {
		failReasons = append(failReasons, "flush_errors")
	}

	if status.Status == model.BetfairRuntimeRunStatusCanceled {
		warningReasons = append(warningReasons, "status_canceled")
	}
	if status.MessagesRead > 0 && status.MessagesRead < 3 {
		warningReasons = append(warningReasons, "low_message_count")
	}
	if status.LatestQuoteCount == 0 {
		warningReasons = append(warningReasons, "no_latest_quote_rows")
	}
	if status.MappedQuoteCount == 0 {
		warningReasons = append(warningReasons, "no_mapped_rows")
	}
	if status.ReadyRunnerBookCount == 0 {
		warningReasons = append(warningReasons, "no_ready_rows")
	}
	if status.ReadyComparisonCount == 0 {
		warningReasons = append(warningReasons, "no_comparison_rows")
	}
	for _, warning := range status.Warnings {
		warning = strings.TrimSpace(warning)
		if warning == "" {
			continue
		}
		warningReasons = appendUniqueRuntimeReason(warningReasons, warning)
	}

	switch {
	case len(failReasons) > 0:
		return runtimePostflightFail, failReasons, warningReasons
	case len(warningReasons) > 0:
		return runtimePostflightWarning, failReasons, warningReasons
	default:
		return runtimePostflightPass, failReasons, warningReasons
	}
}

func runtimePostflightChecklist(status RuntimeStatus) RuntimePostflightChecklist {
	checklist := RuntimePostflightChecklist{
		Enabled:                         true,
		Phase10GFakeLocalSoak:           "manual_evidence_required",
		RealRuntimePilotStatus:          "not_real_runtime_pilot",
		RedactionStatus:                 "report_redacted",
		MessageCandidateStatus:          "ok",
		SinkStatus:                      "ok",
		HeartbeatStatus:                 "ok",
		MappedReadinessComparisonStatus: "ok",
		NextAllowedAction:               "operator_may_review_next_gate",
		BlockingReasons:                 append([]string(nil), status.PostflightFailReasons...),
		WarningReasons:                  append([]string(nil), status.PostflightWarningReasons...),
	}
	if status.RealRuntimePilot {
		checklist.RealRuntimePilotStatus = "real_runtime_pilot"
	}
	if containsRuntimeReason(status.PostflightFailReasons, "zero_messages_read") ||
		containsRuntimeReason(status.PostflightFailReasons, "zero_candidates_emitted") {
		checklist.MessageCandidateStatus = "blocked"
	} else if containsRuntimeReason(status.PostflightWarningReasons, "low_message_count") {
		checklist.MessageCandidateStatus = "review_required"
	}
	if status.SinkErrors > 0 || status.FlushErrors > 0 ||
		containsRuntimeReason(status.PostflightFailReasons, "postgres_zero_raw_messages_inserted") ||
		containsRuntimeReason(status.PostflightFailReasons, "postgres_zero_candidates_inserted_or_unchanged") {
		checklist.SinkStatus = "blocked"
	}
	if containsRuntimeReason(status.PostflightFailReasons, "heartbeat_stale") {
		checklist.HeartbeatStatus = "blocked"
	}
	if containsRuntimeReason(status.PostflightWarningReasons, "no_latest_quote_rows") ||
		containsRuntimeReason(status.PostflightWarningReasons, "no_mapped_rows") ||
		containsRuntimeReason(status.PostflightWarningReasons, "no_ready_rows") ||
		containsRuntimeReason(status.PostflightWarningReasons, "no_comparison_rows") {
		checklist.MappedReadinessComparisonStatus = "operator_review_required"
	}
	switch status.PostflightClassification {
	case runtimePostflightFail:
		checklist.NextAllowedAction = "stop_and_rollback_before_repeat"
	case runtimePostflightWarning:
		checklist.NextAllowedAction = "operator_review_required_before_repeat"
	case runtimePostflightPass:
		checklist.NextAllowedAction = "operator_may_review_next_gate"
	}
	return checklist
}

func appendUniqueRuntimeReason(reasons []string, reason string) []string {
	if containsRuntimeReason(reasons, reason) {
		return reasons
	}
	return append(reasons, reason)
}

func containsRuntimeReason(reasons []string, reason string) bool {
	for _, item := range reasons {
		if item == reason {
			return true
		}
	}
	return false
}

func writeRuntimeStatus(out io.Writer, status RuntimeStatus, format string) error {
	finalizeRuntimeStatusFields(&status)
	switch format {
	case runtimeOutputJSON:
		payload, err := json.Marshal(status)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(payload))
		return err
	default:
		return writeTextRuntimeStatus(out, status)
	}
}

func writeRuntimeSoakReport(out io.Writer, report RuntimeSoakReport, format string) error {
	finalizeRuntimeSoakReportFields(&report)
	switch format {
	case runtimeOutputJSON:
		payload, err := json.Marshal(report)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(payload))
		return err
	default:
		return writeTextRuntimeSoakReport(out, report)
	}
}

func writeTextRuntimeSoakReport(out io.Writer, report RuntimeSoakReport) error {
	_, err := fmt.Fprintf(out, "betfair_runtime_soak enabled=%t soak=%t transport_mode=%s sink_type=%s fake_transport_only=%t persistence=%t collector_integration=%t market_stream_only=%t run_label_prefix=%s soak_runs_requested=%d soak_runs_completed=%d soak_runs_failed=%d elapsed_ms=%d database_url_redacted=%s\n",
		report.Enabled,
		report.Soak,
		report.TransportMode,
		report.SinkType,
		report.FakeTransportOnly,
		report.DatabaseEnabled,
		report.CollectorIntegration,
		report.MarketStreamOnly,
		report.RunLabelPrefix,
		report.SoakRunsRequested,
		report.SoakRunsCompleted,
		report.SoakRunsFailed,
		report.ElapsedMS,
		report.DatabaseURLRedacted,
	)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "total_messages_read=%d total_raw_messages_inserted=%d total_candidates_emitted=%d total_candidates_inserted=%d total_candidates_unchanged=%d total_candidates_skipped=%d total_flush_errors=%d total_sink_errors=%d\n",
		report.TotalMessagesRead,
		report.TotalRawMessagesInserted,
		report.TotalCandidatesEmitted,
		report.TotalCandidatesInserted,
		report.TotalCandidatesUnchanged,
		report.TotalCandidatesSkipped,
		report.TotalFlushErrors,
		report.TotalSinkErrors,
	)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "runtime_run_ids=%s failed_run_ids=%s readiness_state_counts=%s warning_count=%d warnings=%s\n",
		formatRuntimeInt64List(report.RuntimeRunIDs),
		formatRuntimeInt64List(report.FailedRunIDs),
		formatRuntimeReadinessCounts(report.ReadinessStateCounts),
		report.WarningCount,
		strings.Join(report.Warnings, "|"),
	)
	return err
}

func finalizeRuntimeSoakReportFields(report *RuntimeSoakReport) {
	report.Soak = true
	if strings.TrimSpace(report.TransportMode) == "" {
		report.TransportMode = runtimeTransportFake
	}
	if strings.TrimSpace(report.SinkType) == "" {
		report.SinkType = runtimeSinkMemory
	}
	report.DatabaseEnabled = report.DatabaseEnabled || report.SinkType == runtimeSinkPostgres
	report.WarningCount = len(report.Warnings)
	report.FakeTransportOnly = true
	report.CollectorIntegration = false
	report.MarketStreamOnly = true
	if report.RuntimeRunIDs == nil {
		report.RuntimeRunIDs = []int64{}
	}
	if report.FailedRunIDs == nil {
		report.FailedRunIDs = []int64{}
	}
	if report.ReadinessStateCounts == nil {
		report.ReadinessStateCounts = []RuntimeReadinessStateCount{}
	}
}

func writeTextRuntimeStatus(out io.Writer, status RuntimeStatus) error {
	_, err := fmt.Fprintf(out, "betfair_runtime enabled=%t real_runtime_pilot=%t postflight_only=%t running=%t run_id=%d run_label=%s status=%s readiness=%s lifecycle_state=%s readiness_state=%s transport_mode=%s sink_type=%s fake_transport_only=%t persistence=%t collector_integration=%t market_stream_only=%t market_ids_count=%d max_messages_limit=%d duration_limit_seconds=%d elapsed_ms=%d database_url_redacted=%s\n",
		status.Enabled,
		status.RealRuntimePilot,
		status.PostflightOnly,
		status.Running,
		status.RunID,
		status.RunLabel,
		status.Status,
		status.Readiness,
		status.LifecycleState,
		status.ReadinessState,
		status.TransportMode,
		status.SinkType,
		status.FakeTransportOnly,
		status.DatabaseEnabled,
		status.CollectorIntegration,
		status.MarketStreamOnly,
		status.MarketIDsCount,
		status.MaxMessagesLimit,
		status.DurationLimitSeconds,
		status.ElapsedMS,
		status.DatabaseURLRedacted,
	)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "raw_messages_seen=%d messages_read=%d market_change_messages=%d market_stream_messages=%d market_payload_messages=%d server_heartbeat_messages=%d heartbeat_messages=%d status_messages=%d candidates_emitted=%d candidate_batches_seen=%d candidate_batches_flushed=%d sink_errors=%d latest_candidate_count=%d markets_seen=%d reconnect_count=%d\n",
		status.RawMessagesSeen,
		status.MessagesRead,
		status.MarketChangeMessages,
		status.MarketStreamMessages,
		status.MarketPayloadMessages,
		status.ServerHeartbeatMessages,
		status.HeartbeatMessages,
		status.StatusMessages,
		status.CandidatesEmitted,
		status.CandidateBatchesSeen,
		status.CandidateBatchesFlushed,
		status.SinkErrors,
		status.LatestCandidateCount,
		status.MarketsSeen,
		status.ReconnectCount,
	)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "connected=%t authenticated=%t subscribed=%t\n",
		status.Connected,
		status.Authenticated,
		status.Subscribed,
	)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "raw_messages_buffered=%d raw_messages_flushed=%d raw_messages_inserted=%d candidates_buffered=%d candidates_flushed=%d candidates_inserted=%d candidates_unchanged=%d candidates_skipped=%d flush_count=%d flush_errors=%d last_flush_time=%s\n",
		status.RawMessagesBuffered,
		status.RawMessagesFlushed,
		status.RawMessagesInserted,
		status.CandidatesBuffered,
		status.CandidatesFlushed,
		status.CandidatesInserted,
		status.CandidatesUnchanged,
		status.CandidatesSkipped,
		status.FlushCount,
		status.FlushErrors,
		status.LastFlushTime,
	)
	if err != nil {
		return err
	}
	if status.PostflightOnly || status.LinkedRawMessageCount > 0 || len(status.Warnings) > 0 {
		_, err = fmt.Fprintf(out, "postflight classification=%s linked_raw_message_count=%d quote_history_rows=%d latest_quote_count=%d mapped_quote_count=%d ready_runner_book_count=%d ready_comparison_count=%d warnings=%s fail_reasons=%s warning_reasons=%s\n",
			status.PostflightClassification,
			status.LinkedRawMessageCount,
			status.QuoteHistoryRows,
			status.LatestQuoteCount,
			status.MappedQuoteCount,
			status.ReadyRunnerBookCount,
			status.ReadyComparisonCount,
			strings.Join(status.Warnings, "|"),
			strings.Join(status.PostflightFailReasons, "|"),
			strings.Join(status.PostflightWarningReasons, "|"),
		)
		if err != nil {
			return err
		}
	}
	if status.PostflightChecklist != nil {
		checklist := status.PostflightChecklist
		_, err = fmt.Fprintf(out, "postflight_checklist phase_10g_fake_local_soak=%s real_runtime_pilot_status=%s redaction_status=%s message_candidate_status=%s sink_status=%s heartbeat_status=%s mapped_readiness_comparison_status=%s next_allowed_action=%s blocking_reasons=%s warning_reasons=%s\n",
			checklist.Phase10GFakeLocalSoak,
			checklist.RealRuntimePilotStatus,
			checklist.RedactionStatus,
			checklist.MessageCandidateStatus,
			checklist.SinkStatus,
			checklist.HeartbeatStatus,
			checklist.MappedReadinessComparisonStatus,
			checklist.NextAllowedAction,
			strings.Join(checklist.BlockingReasons, "|"),
			strings.Join(checklist.WarningReasons, "|"),
		)
		if err != nil {
			return err
		}
	}
	if diagnostics := status.StreamDiagnostics; diagnostics != nil {
		_, err = fmt.Fprintf(out, "stream_diagnostics requested_market_data_fields=%s requested_ladder_levels=%d requested_market_ids_count=%d requested_market_id_hashes=%s requested_market_filter_has_market_ids=%t requested_market_filter_market_ids_count=%d requested_market_filter_broad_scope=%t subscription_request_id=%d parsed_messages=%d status_messages=%d status_messages_seen=%d status_success_count=%d status_failure_count=%d status_error_codes=%s status_ids_seen=%s connection_closed_count=%d connection_id_present=%t authentication_status_code=%s authentication_error_code=%s authentication_id=%d subscription_status_code=%s subscription_error_code=%s subscription_id=%d market_change_messages=%d mcm_total=%d mcm_with_mc=%d mcm_without_mc=%d mcm_server_heartbeat_or_control=%d mcm_with_ct_heartbeat=%d mcm_with_ct_sub_image=%d mcm_with_ct_resub_image=%d mcm_with_ct_update=%d mcm_with_unknown_ct=%d market_ids_seen_count=%d market_ids_seen=%s runner_changes_seen=%d runner_changes_with_selection_id=%d runner_changes_with_batb=%d runner_changes_with_batl=%d runner_changes_with_atb=%d runner_changes_with_atl=%d runner_changes_with_ltp=%d runner_changes_with_traded_volume=%d warnings=%s\n",
			strings.Join(diagnostics.RequestedMarketDataFields, ","),
			diagnostics.RequestedLadderLevels,
			diagnostics.RequestedMarketIDsCount,
			strings.Join(diagnostics.RequestedMarketIDHashes, ","),
			diagnostics.RequestedMarketFilterHasMarketIDs,
			diagnostics.RequestedMarketFilterMarketIDsCount,
			diagnostics.RequestedMarketFilterBroadScope,
			diagnostics.SubscriptionRequestID,
			diagnostics.ParsedMessages,
			diagnostics.StatusMessages,
			diagnostics.StatusMessagesSeen,
			diagnostics.StatusSuccessCount,
			diagnostics.StatusFailureCount,
			formatRuntimeStringList(diagnostics.StatusErrorCodes),
			formatRuntimeIntList(diagnostics.StatusIDsSeen),
			diagnostics.ConnectionClosedCount,
			diagnostics.ConnectionIDPresent,
			diagnostics.AuthenticationStatusCode,
			diagnostics.AuthenticationErrorCode,
			diagnostics.AuthenticationID,
			diagnostics.SubscriptionStatusCode,
			diagnostics.SubscriptionErrorCode,
			diagnostics.SubscriptionID,
			diagnostics.MarketChangeMessages,
			diagnostics.MCMTotal,
			diagnostics.MCMWithMC,
			diagnostics.MCMWithoutMC,
			diagnostics.MCMServerHeartbeatOrControl,
			diagnostics.MCMWithCTHeartbeat,
			diagnostics.MCMWithCTSubImage,
			diagnostics.MCMWithCTResubImage,
			diagnostics.MCMWithCTUpdate,
			diagnostics.MCMWithUnknownCT,
			diagnostics.MarketIDsSeenCount,
			strings.Join(diagnostics.MarketIDsSeen, ","),
			diagnostics.RunnerChangesSeen,
			diagnostics.RunnerChangesWithSelectionID,
			diagnostics.RunnerChangesWithBATB,
			diagnostics.RunnerChangesWithBATL,
			diagnostics.RunnerChangesWithATB,
			diagnostics.RunnerChangesWithATL,
			diagnostics.RunnerChangesWithLTP,
			diagnostics.RunnerChangesWithTradedVolume,
			strings.Join(diagnostics.Warnings, "|"),
		)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "stream_diagnostics_cache cache_apply_success=%d cache_apply_errors=%d markets_in_cache_after_apply=%d quote_emit_attempts=%d quote_emit_errors=%d quote_candidates_skipped=%d quote_candidates_skipped_reason_counts=%s status_samples=%s samples=%s\n",
			diagnostics.CacheApplySuccess,
			diagnostics.CacheApplyErrors,
			diagnostics.MarketsInCacheAfterApply,
			diagnostics.QuoteEmitAttempts,
			diagnostics.QuoteEmitErrors,
			diagnostics.QuoteCandidatesSkipped,
			formatRuntimeDiagnosticReasonCounts(diagnostics.QuoteCandidatesSkippedReasonCounts),
			formatRuntimeDiagnosticSamples(diagnostics.StatusSamples),
			formatRuntimeDiagnosticSamples(diagnostics.Samples),
		)
		if err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(out, "last_clk=%s last_initial_clk=%s last_message_time=%s last_candidate_time=%s last_sink_flush_time=%s heartbeat_state=%s stopped_reason=%s failure_reason=%s started_at=%s stopped_at=%s\n",
		status.LastClock,
		status.LastInitialClock,
		status.LastMessageTime,
		status.LastCandidateTime,
		status.LastSinkFlushTime,
		status.HeartbeatState,
		status.StoppedReason,
		status.FailureReason,
		status.StartedAt,
		status.StoppedAt,
	)
	return err
}

func exitCodeForRuntimeError(err error) int {
	if err == nil {
		return exitRuntimeSuccess
	}
	if errors.Is(err, context.Canceled) {
		return exitRuntimeCanceled
	}
	return exitRuntimeRuntime
}

func redactedRuntimeError(err error) string {
	return redactedRuntimeErrorWithSecrets(err)
}

func redactedRuntimeErrorWithSecrets(err error, secrets ...string) string {
	if err == nil {
		return ""
	}
	text := betfair.RedactLiveString(err.Error(), runtimeFakeAppKey, runtimeFakeSessionToken)
	for _, secret := range secrets {
		secret = strings.TrimSpace(secret)
		if secret == "" {
			continue
		}
		text = strings.ReplaceAll(text, secret, "[REDACTED_RUNTIME_SECRET]")
	}
	return text
}

func runtimeSinkError(operation string, err error, secrets ...string) error {
	if err == nil {
		return nil
	}
	operation = strings.TrimSpace(operation)
	if operation == "" {
		operation = "sink"
	}
	return fmt.Errorf("betfair runtime %s sink error: %s", operation, redactedRuntimeErrorWithSecrets(err, secrets...))
}

func redactRuntimeRawJSON(raw []byte, secrets ...string) []byte {
	redacted := string(betfair.RedactLivePayload(raw))
	for _, secret := range secrets {
		secret = strings.TrimSpace(secret)
		if secret == "" {
			continue
		}
		redacted = strings.ReplaceAll(redacted, secret, "[REDACTED_RUNTIME_SECRET]")
	}
	return []byte(redacted)
}

func redactRuntimeDatabaseURL(raw string) string {
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
	query := parsed.Query()
	for key := range query {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "password") || strings.Contains(lower, "secret") || strings.Contains(lower, "token") || strings.Contains(lower, "key") {
			query.Set(key, "[REDACTED]")
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func runtimeStreamID(runID int64) string {
	if runID <= 0 {
		return ""
	}
	return fmt.Sprintf("%s%d", runtimeStreamIDPrefix, runID)
}

func parseRuntimeTimePtr(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	ts, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil
	}
	ts = ts.UTC()
	return &ts
}

func secondsPtr(duration time.Duration) *int {
	if duration <= 0 {
		return nil
	}
	seconds := int(duration.Seconds())
	if seconds <= 0 {
		return nil
	}
	return &seconds
}

func intPtr(value int) *int {
	if value <= 0 {
		return nil
	}
	return &value
}

func validateRuntimeRunLabel(label string) error {
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

func validateRuntimeSoakConfig(cfg runtimeConfig) error {
	if cfg.SoakRuns < 0 {
		return fmt.Errorf("soak-runs must be zero or positive")
	}
	if cfg.SoakRuns > maxRuntimeSoakRuns {
		return fmt.Errorf("soak-runs must be %d or fewer", maxRuntimeSoakRuns)
	}
	if cfg.SoakDelay < 0 {
		return fmt.Errorf("soak-delay must be zero or positive")
	}
	if cfg.SoakDelay > maxRuntimeSoakDelay {
		return fmt.Errorf("soak-delay must be %s or less", maxRuntimeSoakDelay)
	}
	if cfg.SoakRuns == 0 {
		if cfg.SoakDelay > 0 {
			return fmt.Errorf("soak-delay requires --soak-runs")
		}
		if cfg.SoakRunLabelPrefix != "" {
			return fmt.Errorf("soak-run-label-prefix requires --soak-runs")
		}
		return nil
	}
	if cfg.PostflightOnly {
		return fmt.Errorf("soak mode cannot be combined with --postflight-only")
	}
	if !cfg.EnableRuntime {
		return fmt.Errorf("soak mode requires --enable-runtime")
	}
	if cfg.TransportMode != runtimeTransportFake {
		return fmt.Errorf("soak mode supports fake transport only")
	}
	if base := runtimeSoakLabelBase(cfg); base != "" {
		if len(base)+4 > 80 {
			return fmt.Errorf("soak run label prefix must leave room for a -001 suffix")
		}
		if err := validateRuntimeRunLabel(runtimeSoakRunLabel(cfg, 1)); err != nil {
			return err
		}
	}
	return nil
}

func validateRuntimeRealPilotConfig(cfg *runtimeConfig) error {
	if cfg == nil {
		return nil
	}
	if cfg.TransportMode == runtimeTransportTLS && !cfg.RealRuntimePilot {
		return fmt.Errorf("--transport tls requires --real-runtime-pilot")
	}
	if !cfg.RealRuntimePilot {
		return nil
	}
	if cfg.PostflightOnly {
		return fmt.Errorf("--real-runtime-pilot cannot be combined with --postflight-only")
	}
	if !cfg.EnableRuntime {
		return fmt.Errorf("--real-runtime-pilot requires --enable-runtime")
	}
	if cfg.TransportMode != runtimeTransportTLS {
		return fmt.Errorf("--real-runtime-pilot requires --transport tls")
	}
	if cfg.Address == "" {
		return fmt.Errorf("--address is required when --real-runtime-pilot is used")
	}
	if cfg.ServerName == "" {
		return fmt.Errorf("--server-name is required when --real-runtime-pilot is used")
	}
	if cfg.AppKeyEnv == "" {
		return fmt.Errorf("--app-key-env is required when --real-runtime-pilot is used")
	}
	if cfg.SessionTokenEnv == "" {
		return fmt.Errorf("--session-token-env is required when --real-runtime-pilot is used")
	}
	if cfg.DialTimeout <= 0 || cfg.ReadTimeout <= 0 || cfg.WriteTimeout <= 0 {
		return fmt.Errorf("real-runtime pilot TLS timeouts must be positive")
	}
	appKey, ok := os.LookupEnv(cfg.AppKeyEnv)
	if !ok || strings.TrimSpace(appKey) == "" {
		return fmt.Errorf("environment variable named by --app-key-env (%s) is not set or empty", cfg.AppKeyEnv)
	}
	sessionToken, ok := os.LookupEnv(cfg.SessionTokenEnv)
	if !ok || strings.TrimSpace(sessionToken) == "" {
		return fmt.Errorf("environment variable named by --session-token-env (%s) is not set or empty", cfg.SessionTokenEnv)
	}
	cfg.AppKey = strings.TrimSpace(appKey)
	cfg.SessionToken = strings.TrimSpace(sessionToken)
	return nil
}

func runtimeSoakLabelBase(cfg runtimeConfig) string {
	if strings.TrimSpace(cfg.SoakRunLabelPrefix) != "" {
		return strings.TrimSpace(cfg.SoakRunLabelPrefix)
	}
	return strings.TrimSpace(cfg.RunLabel)
}

func runtimeSoakRunLabel(cfg runtimeConfig, index int) string {
	base := runtimeSoakLabelBase(cfg)
	if base == "" {
		return ""
	}
	if index <= 0 {
		index = 1
	}
	return fmt.Sprintf("%s-%03d", base, index)
}

func runtimeReadinessCounts(counts map[string]int) []RuntimeReadinessStateCount {
	ordered := []string{
		runtimeReadinessDisabled,
		runtimeReadinessStarting,
		runtimeReadinessReady,
		runtimeReadinessDegraded,
		runtimeReadinessStale,
		runtimeReadinessFailed,
		runtimeReadinessStopped,
		"unknown",
	}
	seen := map[string]bool{}
	result := []RuntimeReadinessStateCount{}
	for _, state := range ordered {
		if count := counts[state]; count > 0 {
			result = append(result, RuntimeReadinessStateCount{ReadinessState: state, Count: count})
			seen[state] = true
		}
	}
	extra := make([]string, 0)
	for state, count := range counts {
		if count > 0 && !seen[state] {
			extra = append(extra, state)
		}
	}
	sort.Strings(extra)
	for _, state := range extra {
		result = append(result, RuntimeReadinessStateCount{ReadinessState: state, Count: counts[state]})
	}
	return result
}

func formatRuntimeInt64List(values []int64) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	return strings.Join(parts, ",")
}

func formatRuntimeReadinessCounts(counts []RuntimeReadinessStateCount) string {
	if len(counts) == 0 {
		return ""
	}
	parts := make([]string, 0, len(counts))
	for _, count := range counts {
		parts = append(parts, fmt.Sprintf("%s:%d", count.ReadinessState, count.Count))
	}
	return strings.Join(parts, ",")
}

func outputFromArgs(args []string) string {
	for i, arg := range args {
		if arg == "--output" && i+1 < len(args) {
			value := strings.ToLower(strings.TrimSpace(args[i+1]))
			if value == runtimeOutputJSON {
				return runtimeOutputJSON
			}
		}
		if strings.HasPrefix(arg, "--output=") {
			value := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(arg, "--output=")))
			if value == runtimeOutputJSON {
				return runtimeOutputJSON
			}
		}
	}
	return runtimeOutputText
}

func transportModeFromArgs(args []string) string {
	for i, arg := range args {
		if arg == "--transport" && i+1 < len(args) {
			return strings.ToLower(strings.TrimSpace(args[i+1]))
		}
		if strings.HasPrefix(arg, "--transport=") {
			return strings.ToLower(strings.TrimSpace(strings.TrimPrefix(arg, "--transport=")))
		}
	}
	return runtimeTransportFake
}

func sinkTypeFromConfigOrArgs(cfg runtimeConfig, args []string) string {
	if strings.TrimSpace(cfg.SinkType) != "" {
		return strings.ToLower(strings.TrimSpace(cfg.SinkType))
	}
	for i, arg := range args {
		if arg == "--sink" && i+1 < len(args) {
			return strings.ToLower(strings.TrimSpace(args[i+1]))
		}
		if strings.HasPrefix(arg, "--sink=") {
			return strings.ToLower(strings.TrimSpace(strings.TrimPrefix(arg, "--sink=")))
		}
	}
	return runtimeSinkMemory
}

func runIDFromConfigOrArgs(cfg runtimeConfig, args []string) int64 {
	if cfg.RunID > 0 {
		return cfg.RunID
	}
	for i, arg := range args {
		if arg == "--run-id" && i+1 < len(args) {
			if id, err := parsePositiveInt64(args[i+1]); err == nil {
				return id
			}
		}
		if strings.HasPrefix(arg, "--run-id=") {
			if id, err := parsePositiveInt64(strings.TrimPrefix(arg, "--run-id=")); err == nil {
				return id
			}
		}
	}
	return 0
}

func parsePositiveInt64(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty integer")
	}
	var n int64
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid integer")
		}
		n = n*10 + int64(r-'0')
	}
	if n <= 0 {
		return 0, fmt.Errorf("integer must be positive")
	}
	return n, nil
}

func hasRuntimeFlag(args []string, name string) bool {
	long := "--" + name
	prefix := long + "="
	for _, arg := range args {
		if arg == long || strings.HasPrefix(arg, prefix) {
			return true
		}
	}
	return false
}

func cleanRuntimeMarketIDs(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func formatRuntimeTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}

type runtimeMarketIDFlag struct {
	items []string
}

func (m *runtimeMarketIDFlag) String() string {
	return strings.Join(m.values(), ",")
}

func (m *runtimeMarketIDFlag) Set(value string) error {
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

func (m *runtimeMarketIDFlag) values() []string {
	if m == nil {
		return nil
	}
	return cleanRuntimeMarketIDs(m.items)
}

type fakeRuntimeTransport struct {
	mu          sync.Mutex
	conns       []*fakeRuntimeConn
	connectErrs []error
	connects    int
}

func newFakeRuntimeTransport(messages []string) *fakeRuntimeTransport {
	return &fakeRuntimeTransport{conns: []*fakeRuntimeConn{newFakeRuntimeConn(messages...)}}
}

func (t *fakeRuntimeTransport) Connect(ctx context.Context) (betfair.LiveConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connects++
	if len(t.connectErrs) > 0 {
		err := t.connectErrs[0]
		t.connectErrs = t.connectErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if len(t.conns) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	conn := t.conns[0]
	t.conns = t.conns[1:]
	return conn, nil
}

func (t *fakeRuntimeTransport) connectCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.connects
}

type fakeRuntimeConn struct {
	reader io.Reader
	writes [][]byte
	closed bool
}

func newFakeRuntimeConn(messages ...string) *fakeRuntimeConn {
	var stream bytes.Buffer
	for _, message := range messages {
		stream.WriteString(message)
		stream.WriteString("\r\n")
	}
	return &fakeRuntimeConn{reader: bytes.NewReader(stream.Bytes())}
}

func (c *fakeRuntimeConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *fakeRuntimeConn) Write(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writes = append(c.writes, append([]byte(nil), payload...))
	return nil
}

func (c *fakeRuntimeConn) Close() error {
	c.closed = true
	return nil
}

func defaultFakeRuntimeMessages(marketIDs []string, now time.Time) []string {
	messages := []string{
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"runtime-initial"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"runtime-subscription"}`,
		`{"op":"heartbeat","id":3,"clk":"runtime-heartbeat"}`,
	}
	for i, marketID := range marketIDs {
		messages = append(messages, runtimeMarketImageMessage(marketID, i, now))
	}
	return messages
}

func runtimeMarketImageMessage(marketID string, index int, now time.Time) string {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	publishMS := now.UTC().UnixMilli()
	selectionID := int64(101 + index)
	backPrice := 2.10 + float64(index)*0.05
	layPrice := backPrice + 0.04
	msg := map[string]any{
		"op":  "mcm",
		"id":  10 + index,
		"pt":  publishMS,
		"ct":  "SUB_IMAGE",
		"clk": fmt.Sprintf("runtime-market-%d", index+1),
		"mc": []any{map[string]any{
			"id":  marketID,
			"img": true,
			"marketDefinition": map[string]any{
				"status":      "OPEN",
				"eventId":     fmt.Sprintf("fake-event-%d", index+1),
				"eventName":   fmt.Sprintf("Fake Home %d v Fake Away %d", index+1, index+1),
				"eventTypeId": "1",
				"marketTime":  now.UTC().Add(2 * time.Hour).Format("2006-01-02T15:04:05.000Z"),
				"name":        "Match Odds",
				"inPlay":      false,
				"runners": []any{map[string]any{
					"id":     selectionID,
					"name":   fmt.Sprintf("Fake Home %d", index+1),
					"status": "ACTIVE",
				}},
			},
			"rc": []any{map[string]any{
				"id":   selectionID,
				"ltp":  backPrice - 0.02,
				"tv":   100 + index,
				"batb": []any{[]any{0, backPrice, 50 + index}},
				"batl": []any{[]any{0, layPrice, 60 + index}},
			}},
		}},
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	code := runCommand(ctx, os.Args[1:], os.Stdout)
	if code != exitRuntimeSuccess {
		os.Exit(code)
	}
}
