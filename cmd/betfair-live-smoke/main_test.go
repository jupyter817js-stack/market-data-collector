package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/your-org/pol-odds-collector/internal/source/betfair"
)

const (
	smokeTestAppKey  = "SMOKE-APP-SECRET"
	smokeTestSession = "SMOKE-SESSION-SECRET"
)

func TestRunSmokeDisabledDoesNotConnect(t *testing.T) {
	factory := &recordingSmokeFactory{}
	var out bytes.Buffer
	code := runCommand(context.Background(), nil, &out, smokeEnv(nil), factory.make)
	if code != exitSmokeValidation {
		t.Fatalf("disabled exit code=%d want %d", code, exitSmokeValidation)
	}
	if factory.calls != 0 {
		t.Fatalf("transport factory calls=%d want 0", factory.calls)
	}
	output := out.String()
	assertSmokeNoSecrets(t, output)
	if !strings.Contains(output, "enabled=false") || !strings.Contains(output, "stop_reason=enable_live_required") {
		t.Fatalf("disabled output=%q", output)
	}
}

func TestRunSmokePreflightOnlyDoesNotConnect(t *testing.T) {
	factory := &recordingSmokeFactory{}
	var out bytes.Buffer
	code := runCommand(context.Background(), append(baseSmokeArgs(), "--preflight-only"), &out, smokeEnv(map[string]string{
		"APP_ENV":     smokeTestAppKey,
		"SESSION_ENV": smokeTestSession,
	}), factory.make)
	if code != exitSmokeSuccess {
		t.Fatalf("preflight exit code=%d want success output=%s", code, out.String())
	}
	if factory.calls != 0 {
		t.Fatalf("transport factory calls=%d want 0 for preflight", factory.calls)
	}
	output := out.String()
	assertSmokeNoSecrets(t, output)
	for _, want := range []string{"preflight_only=true", "stop_reason=preflight_only", "connected=false", "persistence=false"} {
		if !strings.Contains(output, want) {
			t.Fatalf("preflight output missing %q:\n%s", want, output)
		}
	}
}

func TestRunSmokePreflightMissingEnvValueIsRedacted(t *testing.T) {
	factory := &recordingSmokeFactory{}
	var out bytes.Buffer
	code := runCommand(context.Background(), append(baseSmokeArgs(), "--preflight-only"), &out, smokeEnv(map[string]string{
		"APP_ENV": smokeTestAppKey,
	}), factory.make)
	if code != exitSmokeCredential {
		t.Fatalf("missing env preflight exit code=%d want credential", code)
	}
	if factory.calls != 0 {
		t.Fatalf("transport factory calls=%d want 0", factory.calls)
	}
	output := out.String()
	assertSmokeNoSecrets(t, output)
	if !strings.Contains(output, "SESSION_ENV") || !strings.Contains(output, "failure_reason=") {
		t.Fatalf("missing env output=%s", output)
	}
}

func TestRunSmokeValidationExitAndRedaction(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing app key env flag",
			args: []string{"--enable-live", "--session-token-env", "SESSION_ENV", "--address", "127.0.0.1:443", "--server-name", "localhost", "--market-id", "1.111"},
			want: "app-key-env is required",
		},
		{
			name: "missing session env flag",
			args: []string{"--enable-live", "--app-key-env", "APP_ENV", "--address", "127.0.0.1:443", "--server-name", "localhost", "--market-id", "1.111"},
			want: "session-token-env is required",
		},
		{
			name: "missing market id",
			args: []string{"--enable-live", "--app-key-env", "APP_ENV", "--session-token-env", "SESSION_ENV", "--address", "127.0.0.1:443", "--server-name", "localhost"},
			want: "market-id is required",
		},
		{
			name: "missing server name",
			args: []string{"--enable-live", "--app-key-env", "APP_ENV", "--session-token-env", "SESSION_ENV", "--address", "127.0.0.1:443", "--market-id", "1.111"},
			want: "server-name is required",
		},
		{
			name: "invalid duration",
			args: append(baseSmokeArgs(), "--duration", "0s"),
			want: "duration must be positive",
		},
		{
			name: "invalid max messages",
			args: append(baseSmokeArgs(), "--max-messages", "0"),
			want: "max-messages must be positive",
		},
		{
			name: "duration limit",
			args: append(baseSmokeArgs(), "--duration", "3m"),
			want: "duration exceeds smoke limit",
		},
		{
			name: "max messages limit",
			args: append(baseSmokeArgs(), "--max-messages", "501"),
			want: "max-messages exceeds smoke limit",
		},
		{
			name: "market id limit",
			args: append(baseSmokeArgs(), "--market-id", "1.2,1.3,1.4,1.5,1.6"),
			want: "too many market IDs",
		},
		{
			name: "wildcard market id",
			args: append(baseSmokeArgs(), "--market-id", "*"),
			want: "wildcard market subscriptions are not allowed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			factory := &recordingSmokeFactory{}
			var out bytes.Buffer
			code := runCommand(context.Background(), tc.args, &out, smokeEnv(map[string]string{
				"APP_ENV":     smokeTestAppKey,
				"SESSION_ENV": smokeTestSession,
			}), factory.make)
			if code != exitSmokeValidation {
				t.Fatalf("validation exit code=%d want %d output=%s", code, exitSmokeValidation, out.String())
			}
			if factory.calls != 0 {
				t.Fatalf("transport factory calls=%d want 0 for validation failure", factory.calls)
			}
			output := out.String()
			assertSmokeNoSecrets(t, output)
			if !strings.Contains(output, tc.want) {
				t.Fatalf("validation output=%s want containing %q", output, tc.want)
			}
		})
	}
}

func TestRunSmokeMissingEnvValueExitAndRedaction(t *testing.T) {
	factory := &recordingSmokeFactory{}
	var out bytes.Buffer
	code := runCommand(context.Background(), baseSmokeArgs(), &out, smokeEnv(map[string]string{
		"APP_ENV": smokeTestAppKey,
	}), factory.make)
	if code != exitSmokeCredential {
		t.Fatalf("missing env exit code=%d want %d", code, exitSmokeCredential)
	}
	if factory.calls != 0 {
		t.Fatalf("transport factory calls=%d want 0", factory.calls)
	}
	output := out.String()
	assertSmokeNoSecrets(t, output)
	if !strings.Contains(output, "SESSION_ENV") {
		t.Fatalf("missing env output=%s", output)
	}
}

func TestRunSmokeJSONReportShapeAndRedaction(t *testing.T) {
	conn := newSmokeFakeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"initial-smoke"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"sub-smoke"}`,
		`{"op":"heartbeat","id":3,"clk":"heartbeat-smoke"}`,
		smokeMarketImageMessage("1.111"),
	)
	factory := &recordingSmokeFactory{transport: &smokeFakeTransport{conns: []betfair.LiveConn{conn}}}
	var out bytes.Buffer
	code := runCommand(context.Background(), append(baseSmokeArgs(), "--output", "json", "--max-messages", "2"), &out, smokeEnv(map[string]string{
		"APP_ENV":     smokeTestAppKey,
		"SESSION_ENV": smokeTestSession,
	}), factory.make)
	if code != exitSmokeSuccess {
		t.Fatalf("json smoke exit code=%d output=%s", code, out.String())
	}
	output := out.String()
	assertSmokeNoSecrets(t, output)
	var report smokeReport
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &report); err != nil {
		t.Fatalf("decode smoke report json: %v output=%s", err, output)
	}
	if !report.Enabled || report.PreflightOnly || !report.Connected || !report.Authenticated || !report.Subscribed {
		t.Fatalf("json report status=%+v", report)
	}
	if report.StopReason != betfair.LiveStopReasonMaxMessages || report.MarketChangeMessages != 1 || report.HeartbeatMessages != 1 {
		t.Fatalf("json report counters=%+v", report)
	}
	if report.CandidatesEmitted != 2 || report.LastClock != "heartbeat-smoke" || report.LastInitialClock != "initial-smoke" {
		t.Fatalf("json report quote/clock fields=%+v", report)
	}
	if fmt.Sprintf("%+v", report) == "" {
		t.Fatal("formatted report unexpectedly empty")
	}
	assertSmokeSubscriptionDataFilter(t, conn.writes)
	assertSmokeNoSecrets(t, fmt.Sprintf("%+v", report))
}

func TestRunSmokeTextReportRedactionAndNoDBRequirement(t *testing.T) {
	t.Setenv("DATABASE_URL", "not-a-valid-database-url")
	conn := newSmokeFakeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"initial-smoke"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"sub-smoke"}`,
		`{"op":"heartbeat","id":3,"clk":"heartbeat-smoke"}`,
		smokeMarketImageMessage("1.111"),
	)
	factory := &recordingSmokeFactory{transport: &smokeFakeTransport{conns: []betfair.LiveConn{conn}}}
	args := append(baseSmokeArgs(), "--max-messages", "2", "--market-id", "1.222,1.111")
	var out bytes.Buffer
	code := runCommand(context.Background(), args, &out, smokeEnv(map[string]string{
		"APP_ENV":     smokeTestAppKey,
		"SESSION_ENV": smokeTestSession,
	}), factory.make)
	if code != exitSmokeSuccess {
		t.Fatalf("text smoke exit code=%d output=%s", code, out.String())
	}
	if factory.calls != 1 || factory.transport.connects != 1 {
		t.Fatalf("factory/connect calls=%d/%d want 1/1", factory.calls, factory.transport.connects)
	}
	output := out.String()
	assertSmokeNoSecrets(t, output)
	for _, want := range []string{
		"read_only=true",
		"persistence=false",
		"collector_integration=false",
		"market_stream_only=true",
		"connected=true",
		"authenticated=true",
		"subscribed=true",
		"stop_reason=max_messages",
		"market_change_messages=1",
		"heartbeat_messages=1",
		"markets_seen=1",
		"candidates_emitted=2",
		"last_clk=heartbeat-smoke",
		"last_initial_clk=initial-smoke",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("smoke output missing %q:\n%s", want, output)
		}
	}
}

func TestRunSmokeStatusFailuresRedactSecretsAndMapExitCode(t *testing.T) {
	tests := []struct {
		name string
		conn betfair.LiveConn
		want string
	}{
		{
			name: "auth failure",
			conn: newSmokeFakeConn(`{"op":"status","id":1,"statusCode":"FAILURE","errorCode":"AUTH","errorMessage":"bad ` + smokeTestAppKey + ` ` + smokeTestSession + `"}`),
			want: "authentication failed",
		},
		{
			name: "subscription failure",
			conn: newSmokeFakeConn(
				`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
				`{"op":"status","id":2,"statusCode":"FAILURE","errorCode":"SUB","errorMessage":"bad `+smokeTestAppKey+` `+smokeTestSession+`"}`,
			),
			want: "subscription failed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			factory := &recordingSmokeFactory{transport: &smokeFakeTransport{conns: []betfair.LiveConn{tc.conn}}}
			var out bytes.Buffer
			code := runCommand(context.Background(), append(baseSmokeArgs(), "--output=json"), &out, smokeEnv(map[string]string{
				"APP_ENV":     smokeTestAppKey,
				"SESSION_ENV": smokeTestSession,
			}), factory.make)
			if code != exitSmokeConnect {
				t.Fatalf("status failure exit code=%d want %d output=%s", code, exitSmokeConnect, out.String())
			}
			output := out.String()
			assertSmokeNoSecrets(t, output)
			if !strings.Contains(output, tc.want) {
				t.Fatalf("status failure output=%s want containing %q", output, tc.want)
			}
		})
	}
}

func TestRunSmokeDurationStopsBlockedRead(t *testing.T) {
	conn := newSmokeBlockingConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
	)
	factory := &recordingSmokeFactory{transport: &smokeFakeTransport{conns: []betfair.LiveConn{conn}}}
	args := append(baseSmokeArgs(), "--duration", "10ms")
	var out bytes.Buffer
	code := runCommand(context.Background(), args, &out, smokeEnv(map[string]string{
		"APP_ENV":     smokeTestAppKey,
		"SESSION_ENV": smokeTestSession,
	}), factory.make)
	if code != exitSmokeSuccess {
		t.Fatalf("duration-limited smoke exit code=%d output=%s", code, out.String())
	}
	output := out.String()
	if !strings.Contains(output, "stop_reason=duration") {
		t.Fatalf("duration output=%s", output)
	}
	assertSmokeNoSecrets(t, output)
}

func TestRunSmokeContextCancellationExitCode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	factory := &recordingSmokeFactory{transport: &smokeFakeTransport{conns: []betfair.LiveConn{newSmokeFakeConn()}}}
	var out bytes.Buffer
	code := runCommand(ctx, baseSmokeArgs(), &out, smokeEnv(map[string]string{
		"APP_ENV":     smokeTestAppKey,
		"SESSION_ENV": smokeTestSession,
	}), factory.make)
	if code != exitSmokeCanceled {
		t.Fatalf("canceled exit code=%d want %d output=%s", code, exitSmokeCanceled, out.String())
	}
	output := out.String()
	assertSmokeNoSecrets(t, output)
	if !strings.Contains(output, "stop_reason=context_canceled") {
		t.Fatalf("canceled output=%s", output)
	}
}

func TestExitCodeMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"success", nil, exitSmokeSuccess},
		{"validation", errSmokeLiveDisabled, exitSmokeValidation},
		{"credential", smokeCredentialError{err: errors.New("missing env")}, exitSmokeCredential},
		{"connect", smokeConnectError{err: errors.New("auth failed")}, exitSmokeConnect},
		{"runtime", smokeRuntimeError{err: errors.New("parse failed")}, exitSmokeRuntime},
		{"canceled", smokeCanceledError{err: context.Canceled}, exitSmokeCanceled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := exitCodeForSmokeError(tc.err); got != tc.want {
				t.Fatalf("exit code=%d want %d", got, tc.want)
			}
		})
	}
}

func baseSmokeArgs() []string {
	return []string{
		"--enable-live",
		"--app-key-env", "APP_ENV",
		"--session-token-env", "SESSION_ENV",
		"--address", "127.0.0.1:443",
		"--server-name", "localhost",
		"--market-id", "1.111",
	}
}

func smokeEnv(values map[string]string) envLookup {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}

type recordingSmokeFactory struct {
	calls     int
	options   []betfair.TLSLiveTransportOptions
	transport *smokeFakeTransport
}

func (f *recordingSmokeFactory) make(options betfair.TLSLiveTransportOptions) betfair.LiveTransport {
	f.calls++
	f.options = append(f.options, options)
	if f.transport == nil {
		f.transport = &smokeFakeTransport{}
	}
	return f.transport
}

type smokeFakeTransport struct {
	conns    []betfair.LiveConn
	connects int
}

func (t *smokeFakeTransport) Connect(ctx context.Context) (betfair.LiveConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.connects++
	if len(t.conns) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	conn := t.conns[0]
	t.conns = t.conns[1:]
	return conn, nil
}

type smokeFakeConn struct {
	reader io.Reader
	writes [][]byte
	closed bool
}

func newSmokeFakeConn(messages ...string) *smokeFakeConn {
	var stream bytes.Buffer
	for _, message := range messages {
		stream.WriteString(message)
		stream.WriteString("\r\n")
	}
	return &smokeFakeConn{reader: bytes.NewReader(stream.Bytes())}
}

func (c *smokeFakeConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *smokeFakeConn) Write(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writes = append(c.writes, append([]byte(nil), payload...))
	return nil
}

func (c *smokeFakeConn) Close() error {
	c.closed = true
	return nil
}

type smokeBlockingConn struct {
	prefix *bytes.Reader
	closed chan struct{}
	once   sync.Once
}

func newSmokeBlockingConn(messages ...string) *smokeBlockingConn {
	var stream bytes.Buffer
	for _, message := range messages {
		stream.WriteString(message)
		stream.WriteString("\r\n")
	}
	return &smokeBlockingConn{
		prefix: bytes.NewReader(stream.Bytes()),
		closed: make(chan struct{}),
	}
}

func (c *smokeBlockingConn) Read(p []byte) (int, error) {
	if c.prefix.Len() > 0 {
		return c.prefix.Read(p)
	}
	<-c.closed
	return 0, io.ErrClosedPipe
}

func (c *smokeBlockingConn) Write(ctx context.Context, payload []byte) error {
	return ctx.Err()
}

func (c *smokeBlockingConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
	})
	return nil
}

func smokeMarketImageMessage(marketID string) string {
	return fmt.Sprintf(`{"op":"mcm","id":10,"pt":1715000000000,"ct":"SUB_IMAGE","mc":[{"id":%q,"img":true,"marketDefinition":{"status":"OPEN","eventId":"event-1","eventName":"Home FC v Away FC","marketTime":"2026-05-07T12:00:00.000Z","name":"Match Odds","inPlay":false,"runners":[{"id":101,"name":"Home FC","status":"ACTIVE"}]},"rc":[{"id":101,"ltp":2.20,"tv":120,"batb":[[0,2.30,75]],"batl":[[0,2.34,80]]}]}]}`, marketID)
}

func assertSmokeSubscriptionDataFilter(t *testing.T, writes [][]byte) {
	t.Helper()
	if len(writes) < 2 {
		t.Fatalf("writes=%d want auth+subscription", len(writes))
	}
	var payload map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(writes[1]), &payload); err != nil {
		t.Fatalf("decode smoke subscription payload: %v", err)
	}
	if payload["op"] != "marketSubscription" {
		t.Fatalf("smoke subscription payload=%+v", payload)
	}
	if _, ok := payload["market_filter"]; ok {
		t.Fatalf("smoke subscription used snake_case market_filter: %+v", payload)
	}
	if _, ok := payload["market_data_filter"]; ok {
		t.Fatalf("smoke subscription used snake_case market_data_filter: %+v", payload)
	}
	marketFilter, ok := payload["marketFilter"].(map[string]any)
	if !ok {
		t.Fatalf("smoke subscription missing marketFilter: %+v", payload)
	}
	marketIDs, ok := marketFilter["marketIds"].([]any)
	if !ok || len(marketIDs) != 1 || marketIDs[0] != "1.111" {
		t.Fatalf("smoke subscription marketIds=%+v", marketFilter["marketIds"])
	}
	if _, ok := marketFilter["marketTypes"]; ok {
		t.Fatalf("smoke subscription included broad marketTypes: %+v", marketFilter)
	}
	filter, ok := payload["marketDataFilter"].(map[string]any)
	if !ok {
		t.Fatalf("smoke subscription missing marketDataFilter: %+v", payload)
	}
	if _, ok := filter["ladder_levels"]; ok {
		t.Fatalf("smoke subscription used snake_case ladder_levels: %+v", filter)
	}
	fieldsAny, ok := filter["fields"].([]any)
	if !ok {
		t.Fatalf("smoke subscription fields=%+v", filter["fields"])
	}
	fields := make([]string, 0, len(fieldsAny))
	for _, value := range fieldsAny {
		field, ok := value.(string)
		if !ok {
			t.Fatalf("smoke subscription field=%+v", value)
		}
		fields = append(fields, field)
	}
	if !reflect.DeepEqual(fields, betfair.DefaultMarketDataFields()) {
		t.Fatalf("smoke subscription fields=%v want %v", fields, betfair.DefaultMarketDataFields())
	}
	if filter["ladderLevels"] != float64(defaultSmokeLadderLevels) {
		t.Fatalf("smoke subscription ladderLevels=%+v want %d", filter["ladderLevels"], defaultSmokeLadderLevels)
	}
	if strings.Contains(fmt.Sprintf("%+v", payload), "order") || strings.Contains(fmt.Sprintf("%+v", payload), "account") {
		t.Fatalf("smoke subscription included order/account scope: %+v", payload)
	}
}

func assertSmokeNoSecrets(t *testing.T, s string) {
	t.Helper()
	if strings.Contains(s, smokeTestAppKey) || strings.Contains(s, smokeTestSession) {
		t.Fatalf("output contains unredacted smoke secret: %s", s)
	}
}
