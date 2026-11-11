package betfair

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

var (
	ErrLiveConnectorDisabled = errors.New("betfair live connector disabled")
	ErrLiveMaxReconnects     = errors.New("betfair live connector max reconnect attempts exceeded")
)

const (
	LiveStopReasonDisabled             = "disabled"
	LiveStopReasonEOF                  = "eof"
	LiveStopReasonMaxMessages          = "max_messages"
	LiveStopReasonContextCanceled      = "context_canceled"
	LiveStopReasonDeadlineExceeded     = "deadline_exceeded"
	LiveStopReasonFailed               = "failed"
	LiveStopReasonMaxReconnectAttempts = "max_reconnect_attempts"
)

type LiveTransport interface {
	Connect(ctx context.Context) (LiveConn, error)
}

type LiveConn interface {
	io.Reader
	Write(ctx context.Context, payload []byte) error
	Close() error
}

type LiveConnectorState string

const (
	LiveStateDisabled       LiveConnectorState = "disabled"
	LiveStateConnecting     LiveConnectorState = "connecting"
	LiveStateAuthenticating LiveConnectorState = "authenticating"
	LiveStateSubscribing    LiveConnectorState = "subscribing"
	LiveStateRunning        LiveConnectorState = "running"
	LiveStateReconnecting   LiveConnectorState = "reconnecting"
	LiveStateStopped        LiveConnectorState = "stopped"
	LiveStateFailed         LiveConnectorState = "failed"
)

type LiveConnectorOptions struct {
	Enabled              bool
	AppKey               string
	SessionToken         string
	Subscription         MarketSubscriptionRequest
	HeartbeatAgingAfter  time.Duration
	HeartbeatStaleAfter  time.Duration
	MaxReconnectAttempts int
	ReconnectBaseDelay   time.Duration
	ReconnectMaxDelay    time.Duration
	MaxMessageSize       int
	MaxMessages          int
	Now                  func() time.Time
	OnMessage            func(Message, time.Time) error
	OnStatusMessage      func(string, Message, time.Time)
	OnSubscription       func(int, MarketSubscriptionRequest, time.Time)
	OnCacheApply         func(Message, error, time.Time)
}

type LiveRunResult struct {
	State                   LiveConnectorState
	StopReason              string
	Connected               bool
	Authenticated           bool
	Subscribed              bool
	ReconnectAttempts       int
	ConnectionAttempts      int
	MessagesParsed          int
	StreamMessagesRead      int
	MarketChangeMessages    int
	MarketStreamMessages    int
	MarketPayloadMessages   int
	ServerHeartbeatMessages int
	HeartbeatMessages       int
	ControlMessages         int
	MarketsSeen             int
	Candidates              []QuoteCandidate
	LatestClock             string
	InitialClock            string
	LastMessageTime         *time.Time
	HeartbeatState          StreamState
}

type LiveConnector struct {
	transport  LiveTransport
	options    LiveConnectorOptions
	state      LiveConnectorState
	tracker    *HeartbeatTracker
	cache      *MarketCache
	clock      string
	initial    string
	stopReason string
}

func NewLiveConnector(transport LiveTransport, options LiveConnectorOptions) *LiveConnector {
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
		options.Now = now
	}
	return &LiveConnector{
		transport: transport,
		options:   options,
		state:     LiveStateDisabled,
		cache: NewMarketCache(WithCacheClock(func() time.Time {
			return now().UTC()
		})),
	}
}

func (c *LiveConnector) Run(ctx context.Context) (LiveRunResult, error) {
	if c == nil {
		return LiveRunResult{State: LiveStateFailed}, fmt.Errorf("betfair live connector is nil")
	}
	result := LiveRunResult{State: c.state}
	if err := ctx.Err(); err != nil {
		c.state = LiveStateFailed
		c.stopReason = liveStopReasonFromContext(err)
		return c.result(result), err
	}
	if err := c.validate(); err != nil {
		if errors.Is(err, ErrLiveConnectorDisabled) {
			c.state = LiveStateDisabled
			c.stopReason = LiveStopReasonDisabled
		} else {
			c.state = LiveStateFailed
			c.stopReason = LiveStopReasonFailed
		}
		return c.result(result), err
	}

	tracker, err := NewHeartbeatTracker(StalenessConfig{
		HealthyAfter: c.options.HeartbeatAgingAfter,
		StaleAfter:   c.options.HeartbeatStaleAfter,
	})
	if err != nil {
		c.state = LiveStateFailed
		c.stopReason = LiveStopReasonFailed
		return c.result(result), err
	}
	c.tracker = tracker

	requestID := 1
	reconnectAttempts := 0
	connectionAttempts := 0
	for {
		if err := ctx.Err(); err != nil {
			c.state = LiveStateStopped
			c.stopReason = liveStopReasonFromContext(err)
			result.ReconnectAttempts = reconnectAttempts
			result.ConnectionAttempts = connectionAttempts
			return c.result(result), err
		}

		if connectionAttempts == 0 {
			c.state = LiveStateConnecting
		} else {
			c.state = LiveStateReconnecting
		}
		connectionAttempts++

		conn, err := c.transport.Connect(ctx)
		if err != nil {
			if reconnectAttempts >= c.options.MaxReconnectAttempts {
				c.state = LiveStateFailed
				c.stopReason = LiveStopReasonMaxReconnectAttempts
				result.ReconnectAttempts = reconnectAttempts
				result.ConnectionAttempts = connectionAttempts
				return c.result(result), fmt.Errorf("%w: connect: %s", ErrLiveMaxReconnects, c.safeError(err))
			}
			reconnectAttempts++
			if err := c.waitBeforeReconnect(ctx, reconnectAttempts); err != nil {
				c.state = LiveStateStopped
				c.stopReason = liveStopReasonFromContext(err)
				result.ReconnectAttempts = reconnectAttempts
				result.ConnectionAttempts = connectionAttempts
				return c.result(result), err
			}
			continue
		}
		result.Connected = true

		streamErr := c.runConnection(ctx, conn, &requestID, &result)
		_ = conn.Close()
		if streamErr == nil {
			c.state = LiveStateStopped
			if c.stopReason == "" {
				c.stopReason = LiveStopReasonEOF
			}
			result.ReconnectAttempts = reconnectAttempts
			result.ConnectionAttempts = connectionAttempts
			return c.result(result), nil
		}
		if errors.Is(streamErr, context.Canceled) || errors.Is(streamErr, context.DeadlineExceeded) {
			c.state = LiveStateStopped
			c.stopReason = liveStopReasonFromContext(streamErr)
			result.ReconnectAttempts = reconnectAttempts
			result.ConnectionAttempts = connectionAttempts
			return c.result(result), streamErr
		}
		if !liveErrorCanReconnect(streamErr) {
			c.state = LiveStateFailed
			c.stopReason = LiveStopReasonFailed
			result.ReconnectAttempts = reconnectAttempts
			result.ConnectionAttempts = connectionAttempts
			return c.result(result), streamErr
		}
		if reconnectAttempts >= c.options.MaxReconnectAttempts {
			c.state = LiveStateFailed
			c.stopReason = LiveStopReasonMaxReconnectAttempts
			result.ReconnectAttempts = reconnectAttempts
			result.ConnectionAttempts = connectionAttempts
			return c.result(result), fmt.Errorf("%w: %s", ErrLiveMaxReconnects, c.safeError(streamErr))
		}
		reconnectAttempts++
		if err := c.waitBeforeReconnect(ctx, reconnectAttempts); err != nil {
			c.state = LiveStateStopped
			c.stopReason = liveStopReasonFromContext(err)
			result.ReconnectAttempts = reconnectAttempts
			result.ConnectionAttempts = connectionAttempts
			return c.result(result), err
		}
	}
}

func (c *LiveConnector) validate() error {
	if !c.options.Enabled {
		return ErrLiveConnectorDisabled
	}
	if strings.TrimSpace(c.options.AppKey) == "" {
		return fmt.Errorf("betfair live connector: app key is required")
	}
	if strings.TrimSpace(c.options.SessionToken) == "" {
		return fmt.Errorf("betfair live connector: session token is required")
	}
	if !c.options.Subscription.valid() {
		return fmt.Errorf("betfair live connector: subscription market ids or market types are required")
	}
	if c.transport == nil {
		return fmt.Errorf("betfair live connector: transport is required")
	}
	if c.options.HeartbeatAgingAfter <= 0 {
		return fmt.Errorf("betfair live connector: heartbeat aging threshold must be positive")
	}
	if c.options.HeartbeatStaleAfter <= 0 {
		return fmt.Errorf("betfair live connector: heartbeat stale threshold must be positive")
	}
	if c.options.HeartbeatStaleAfter < c.options.HeartbeatAgingAfter {
		return fmt.Errorf("betfair live connector: heartbeat stale threshold must be >= aging threshold")
	}
	if c.options.MaxReconnectAttempts < 0 {
		return fmt.Errorf("betfair live connector: max reconnect attempts must be non-negative")
	}
	if c.options.MaxMessages < 0 {
		return fmt.Errorf("betfair live connector: max messages must be non-negative")
	}
	return nil
}

func (c *LiveConnector) runConnection(ctx context.Context, conn LiveConn, requestID *int, result *LiveRunResult) error {
	stopCancelWatch := liveCloseOnContext(ctx, conn)
	defer stopCancelWatch()

	parser := NewParser(conn, c.options.MaxMessageSize)

	c.state = LiveStateAuthenticating
	auth, err := BuildAuthenticationMessage(*requestID, c.options.AppKey, c.options.SessionToken)
	if err != nil {
		return err
	}
	*requestID = *requestID + 1
	if err := conn.Write(ctx, auth); err != nil {
		return liveReconnectableError{err: fmt.Errorf("write betfair live authentication request: %s", c.safeError(err))}
	}
	if err := c.readStatus(ctx, parser, "authentication", result); err != nil {
		return err
	}
	result.Authenticated = true

	c.state = LiveStateSubscribing
	subscription := c.options.Subscription.withClocks(c.clock, c.initial)
	if c.options.OnSubscription != nil {
		c.options.OnSubscription(*requestID, subscription, c.now())
	}
	subscribe, err := BuildMarketSubscriptionMessage(*requestID, subscription)
	if err != nil {
		return err
	}
	*requestID = *requestID + 1
	if err := conn.Write(ctx, subscribe); err != nil {
		return liveReconnectableError{err: fmt.Errorf("write betfair live market subscription request: %s", c.safeError(err))}
	}
	if err := c.readStatus(ctx, parser, "subscription", result); err != nil {
		return err
	}
	result.Subscribed = true

	c.state = LiveStateRunning
	for {
		msg, err := parser.Next(ctx)
		if err != nil {
			// Prefer context cancellation/deadline over EOF: when liveCloseOnContext
			// closes the connection in response to ctx cancellation, the blocked read
			// can return io.EOF (e.g. clean TLS close-notify on Linux). The cancellation
			// is the operator-meaningful stop reason, so surface it instead of EOF.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			if liveParserErrorIsMalformed(err) {
				return fmt.Errorf("parse betfair live stream message: %s", c.safeError(err))
			}
			return liveReconnectableError{err: fmt.Errorf("read betfair live stream: %s", c.safeError(err))}
		}
		c.recordMessage(msg, true, result)
		if err := c.observeMessage(msg); err != nil {
			return err
		}
		if msg.Op == OperationStatus && !liveStatusSuccess(msg) {
			return fmt.Errorf("betfair live stream status failure: %s", c.safeStatus(msg))
		}
		if c.maxMessagesReached(result) {
			c.stopReason = LiveStopReasonMaxMessages
			return nil
		}
	}
}

func (c *LiveConnector) readStatus(ctx context.Context, parser *Parser, phase string, result *LiveRunResult) error {
	for {
		msg, err := parser.Next(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if liveParserErrorIsMalformed(err) {
				return fmt.Errorf("parse betfair live %s status: %s", phase, c.safeError(err))
			}
			return liveReconnectableError{err: fmt.Errorf("read betfair live %s status: %s", phase, c.safeError(err))}
		}
		c.recordMessage(msg, false, result)
		if err := c.observeMessage(msg); err != nil {
			return err
		}
		if msg.Op == OperationConnection {
			continue
		}
		if msg.Op != OperationStatus {
			return fmt.Errorf("betfair live %s expected status response, got %q", phase, msg.Op)
		}
		if c.options.OnStatusMessage != nil {
			c.options.OnStatusMessage(phase, msg, c.now())
		}
		if !liveStatusSuccess(msg) {
			return fmt.Errorf("betfair live %s failed: %s", phase, c.safeStatus(msg))
		}
		return nil
	}
}

func (c *LiveConnector) recordMessage(msg Message, streamMessage bool, result *LiveRunResult) {
	result.MessagesParsed++
	if streamMessage {
		result.StreamMessagesRead++
	}
	switch msg.Op {
	case OperationMarketChange:
		result.MarketChangeMessages++
		result.MarketStreamMessages++
		if len(msg.MarketChanges) > 0 {
			result.MarketPayloadMessages++
		} else {
			result.ServerHeartbeatMessages++
		}
	case OperationHeartbeat:
		result.HeartbeatMessages++
		result.ServerHeartbeatMessages++
	default:
		result.ControlMessages++
	}
}

func (c *LiveConnector) maxMessagesReached(result *LiveRunResult) bool {
	return c.options.MaxMessages > 0 && result.StreamMessagesRead >= c.options.MaxMessages
}

func (c *LiveConnector) observeMessage(msg Message) error {
	now := c.now()
	c.updateClocks(msg)
	if c.tracker != nil {
		c.tracker.Observe(msg, now)
	}
	if c.options.OnMessage != nil {
		if err := c.options.OnMessage(msg, now); err != nil {
			return fmt.Errorf("observe betfair live stream message: %w", err)
		}
	}
	applyErr := c.cache.ApplyMessage(msg)
	if c.options.OnCacheApply != nil {
		c.options.OnCacheApply(msg, applyErr, now)
	}
	if applyErr != nil {
		return fmt.Errorf("apply betfair live stream message: %w", applyErr)
	}
	return nil
}

func (c *LiveConnector) updateClocks(msg Message) {
	if strings.TrimSpace(msg.Clock) != "" {
		c.clock = strings.TrimSpace(msg.Clock)
	}
	if strings.TrimSpace(msg.InitialClock) != "" {
		c.initial = strings.TrimSpace(msg.InitialClock)
	}
}

func (c *LiveConnector) result(result LiveRunResult) LiveRunResult {
	result.State = c.state
	result.StopReason = c.stopReason
	result.LatestClock = c.clock
	result.InitialClock = c.initial
	if c.tracker != nil {
		result.HeartbeatState = c.tracker.State(c.now())
		if last, ok := c.tracker.LastMessageTime(); ok {
			last = last.UTC()
			result.LastMessageTime = &last
		}
	}
	result.MarketsSeen = len(c.cache.SnapshotAll())
	candidates, err := EmitAllQuoteCandidates(c.cache, QuoteEmitOptions{
		Now:        c.now(),
		FreshAfter: c.options.HeartbeatAgingAfter,
		AgingAfter: c.options.HeartbeatStaleAfter,
	})
	if err == nil {
		result.Candidates = candidates
	}
	return result
}

func (c *LiveConnector) State() LiveConnectorState {
	if c == nil {
		return LiveStateFailed
	}
	return c.state
}

func (c *LiveConnector) SnapshotAll() []MarketSnapshot {
	if c == nil || c.cache == nil {
		return nil
	}
	return c.cache.SnapshotAll()
}

func (c *LiveConnector) HeartbeatState(now time.Time) StreamState {
	if c == nil || c.tracker == nil {
		return StreamStateUnknown
	}
	return c.tracker.State(now)
}

func (c *LiveConnector) now() time.Time {
	if c.options.Now != nil {
		return c.options.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *LiveConnector) safeError(err error) string {
	if err == nil {
		return ""
	}
	return RedactLiveString(err.Error(), c.options.AppKey, c.options.SessionToken)
}

func (c *LiveConnector) safeStatus(msg Message) string {
	parts := []string{}
	if msg.StatusCode != "" {
		parts = append(parts, "status="+msg.StatusCode)
	}
	if msg.ErrorCode != "" {
		parts = append(parts, "error_code="+msg.ErrorCode)
	}
	if msg.ErrorMessage != "" {
		parts = append(parts, "error_message="+msg.ErrorMessage)
	}
	if len(parts) == 0 {
		parts = append(parts, "status response did not include success")
	}
	return RedactLiveString(strings.Join(parts, " "), c.options.AppKey, c.options.SessionToken)
}

func (c *LiveConnector) waitBeforeReconnect(ctx context.Context, attempt int) error {
	delay := boundedReconnectDelay(attempt, c.options.ReconnectBaseDelay, c.options.ReconnectMaxDelay)
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func liveStatusSuccess(msg Message) bool {
	return strings.EqualFold(strings.TrimSpace(msg.StatusCode), "SUCCESS")
}

type liveReconnectableError struct {
	err error
}

func (e liveReconnectableError) Error() string {
	return e.err.Error()
}

func (e liveReconnectableError) Unwrap() error {
	return e.err
}

func liveErrorCanReconnect(err error) bool {
	var reconnectable liveReconnectableError
	return errors.As(err, &reconnectable)
}

func liveParserErrorIsMalformed(err error) bool {
	if errors.Is(err, ErrMessageTooLarge) {
		return true
	}
	text := err.Error()
	return strings.Contains(text, "decode betfair stream message") ||
		strings.Contains(text, "expected CRLF delimiter") ||
		strings.Contains(text, "empty message") ||
		strings.Contains(text, "unterminated CRLF-delimited message")
}

func boundedReconnectDelay(attempt int, base, max time.Duration) time.Duration {
	if attempt <= 0 || base <= 0 {
		return 0
	}
	delay := base
	for i := 1; i < attempt; i++ {
		delay *= 2
		if max > 0 && delay > max {
			return max
		}
	}
	if max > 0 && delay > max {
		return max
	}
	return delay
}

func liveStopReasonFromContext(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return LiveStopReasonDeadlineExceeded
	}
	if errors.Is(err, context.Canceled) {
		return LiveStopReasonContextCanceled
	}
	return LiveStopReasonFailed
}

func liveCloseOnContext(ctx context.Context, conn LiveConn) func() {
	if ctx.Done() == nil || conn == nil {
		return func() {}
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() {
		close(done)
		<-exited
	}
}
