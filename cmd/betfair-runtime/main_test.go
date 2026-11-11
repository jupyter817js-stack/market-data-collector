package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/betfair"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func TestRuntimeDisabledByDefaultDoesNotConstructTransport(t *testing.T) {
	sink := &InMemoryRuntimeSink{}
	constructed := 0
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Sink: sink,
		TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
			constructed++
			return newFakeRuntimeTransport(nil)
		},
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("disabled runtime error=%v want nil", err)
	}
	if constructed != 0 {
		t.Fatalf("transport constructed %d times while disabled", constructed)
	}
	if status.Status != runtimeStatusDisabled || status.Readiness != runtimeReadinessDisabled {
		t.Fatalf("disabled status=%+v", status)
	}
	statuses := sink.Statuses()
	if len(statuses) != 1 || statuses[0].Status != runtimeStatusDisabled {
		t.Fatalf("disabled sink statuses=%+v", statuses)
	}
}

func TestRuntimeCommandDisabledOutput(t *testing.T) {
	var out bytes.Buffer
	code := runCommand(context.Background(), nil, &out)
	if code != exitRuntimeSuccess {
		t.Fatalf("disabled command exit=%d want 0 output=%s", code, out.String())
	}
	text := out.String()
	if !strings.Contains(text, "status=disabled") || !strings.Contains(text, "readiness=disabled") {
		t.Fatalf("disabled output=%s", text)
	}
	if strings.Contains(text, "appKey") || strings.Contains(text, "session") || strings.Contains(text, `"op"`) {
		t.Fatalf("disabled output leaked payload detail: %s", text)
	}
}

func TestRuntimeUnsupportedTransportModeFailsClearly(t *testing.T) {
	var out bytes.Buffer
	code := runCommand(context.Background(), []string{
		"--enable-runtime",
		"--transport", "tls",
		"--market-id", "1.111",
	}, &out)
	if code != exitRuntimeValidation {
		t.Fatalf("unsupported transport exit=%d want validation output=%s", code, out.String())
	}
	if !strings.Contains(out.String(), "--transport tls requires --real-runtime-pilot") {
		t.Fatalf("unsupported transport output=%s", out.String())
	}
}

func TestRuntimeRealPilotValidationGates(t *testing.T) {
	appKey := "REAL_RUNTIME_APP_SECRET"
	sessionToken := "REAL_RUNTIME_SESSION_SECRET"
	t.Setenv("RUNTIME_TEST_APP_KEY", appKey)
	t.Setenv("RUNTIME_TEST_SESSION_TOKEN", sessionToken)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "tls without real pilot",
			args: []string{"--enable-runtime", "--transport", "tls", "--market-id", "1.111", "--output", "json"},
			want: "--transport tls requires --real-runtime-pilot",
		},
		{
			name: "real pilot without enable",
			args: []string{"--real-runtime-pilot", "--transport", "tls", "--address", "localhost:443", "--server-name", "localhost", "--app-key-env", "RUNTIME_TEST_APP_KEY", "--session-token-env", "RUNTIME_TEST_SESSION_TOKEN", "--market-id", "1.111", "--output", "json"},
			want: "--real-runtime-pilot requires --enable-runtime",
		},
		{
			name: "real pilot without tls",
			args: []string{"--enable-runtime", "--real-runtime-pilot", "--transport", "fake", "--address", "localhost:443", "--server-name", "localhost", "--app-key-env", "RUNTIME_TEST_APP_KEY", "--session-token-env", "RUNTIME_TEST_SESSION_TOKEN", "--market-id", "1.111", "--output", "json"},
			want: "--real-runtime-pilot requires --transport tls",
		},
		{
			name: "missing address",
			args: []string{"--enable-runtime", "--real-runtime-pilot", "--transport", "tls", "--server-name", "localhost", "--app-key-env", "RUNTIME_TEST_APP_KEY", "--session-token-env", "RUNTIME_TEST_SESSION_TOKEN", "--market-id", "1.111", "--output", "json"},
			want: "--address is required",
		},
		{
			name: "missing server name",
			args: []string{"--enable-runtime", "--real-runtime-pilot", "--transport", "tls", "--address", "localhost:443", "--app-key-env", "RUNTIME_TEST_APP_KEY", "--session-token-env", "RUNTIME_TEST_SESSION_TOKEN", "--market-id", "1.111", "--output", "json"},
			want: "--server-name is required",
		},
		{
			name: "missing app key env flag",
			args: []string{"--enable-runtime", "--real-runtime-pilot", "--transport", "tls", "--address", "localhost:443", "--server-name", "localhost", "--session-token-env", "RUNTIME_TEST_SESSION_TOKEN", "--market-id", "1.111", "--output", "json"},
			want: "--app-key-env is required",
		},
		{
			name: "missing session token env flag",
			args: []string{"--enable-runtime", "--real-runtime-pilot", "--transport", "tls", "--address", "localhost:443", "--server-name", "localhost", "--app-key-env", "RUNTIME_TEST_APP_KEY", "--market-id", "1.111", "--output", "json"},
			want: "--session-token-env is required",
		},
		{
			name: "missing app key env value",
			args: []string{"--enable-runtime", "--real-runtime-pilot", "--transport", "tls", "--address", "localhost:443", "--server-name", "localhost", "--app-key-env", "RUNTIME_TEST_MISSING_APP_KEY", "--session-token-env", "RUNTIME_TEST_SESSION_TOKEN", "--market-id", "1.111", "--output", "json"},
			want: "environment variable named by --app-key-env",
		},
		{
			name: "missing session token env value",
			args: []string{"--enable-runtime", "--real-runtime-pilot", "--transport", "tls", "--address", "localhost:443", "--server-name", "localhost", "--app-key-env", "RUNTIME_TEST_APP_KEY", "--session-token-env", "RUNTIME_TEST_MISSING_SESSION_TOKEN", "--market-id", "1.111", "--output", "json"},
			want: "environment variable named by --session-token-env",
		},
		{
			name: "missing market id",
			args: []string{"--enable-runtime", "--real-runtime-pilot", "--transport", "tls", "--address", "localhost:443", "--server-name", "localhost", "--app-key-env", "RUNTIME_TEST_APP_KEY", "--session-token-env", "RUNTIME_TEST_SESSION_TOKEN", "--output", "json"},
			want: "at least one market-id",
		},
		{
			name: "wildcard market id",
			args: []string{"--enable-runtime", "--real-runtime-pilot", "--transport", "tls", "--address", "localhost:443", "--server-name", "localhost", "--app-key-env", "RUNTIME_TEST_APP_KEY", "--session-token-env", "RUNTIME_TEST_SESSION_TOKEN", "--market-id", "*", "--output", "json"},
			want: "wildcard",
		},
		{
			name: "duration guardrail",
			args: []string{"--enable-runtime", "--real-runtime-pilot", "--transport", "tls", "--address", "localhost:443", "--server-name", "localhost", "--app-key-env", "RUNTIME_TEST_APP_KEY", "--session-token-env", "RUNTIME_TEST_SESSION_TOKEN", "--market-id", "1.111", "--duration", "3m", "--output", "json"},
			want: "duration exceeds",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			code := runCommand(context.Background(), tc.args, &out)
			if code != exitRuntimeValidation {
				t.Fatalf("real pilot validation exit=%d want validation output=%s", code, out.String())
			}
			text := out.String()
			if !strings.Contains(text, tc.want) {
				t.Fatalf("real pilot validation output=%s want %q", text, tc.want)
			}
			assertRuntimeNoSecrets(t, text, appKey, sessionToken)
		})
	}
}

func TestRuntimeRealPilotMemorySinkWithInjectedTLS(t *testing.T) {
	now := fixedRuntimeNow()
	appKey := "REAL_RUNTIME_APP_SECRET"
	sessionToken := "REAL_RUNTIME_SESSION_SECRET"
	t.Setenv("RUNTIME_TEST_APP_KEY", appKey)
	t.Setenv("RUNTIME_TEST_SESSION_TOKEN", sessionToken)

	writes := make(chan []byte, 2)
	server := newRuntimeLocalTLSServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		auth, err := reader.ReadBytes('\n')
		if err != nil {
			t.Errorf("read real pilot auth request: %v", err)
			return
		}
		writes <- append([]byte(nil), auth...)
		_, _ = conn.Write([]byte(`{"op":"connection","connectionId":"real-runtime-local"}` + "\r\n"))
		_, _ = conn.Write([]byte(`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"real-runtime-initial"}` + "\r\n"))

		subscription, err := reader.ReadBytes('\n')
		if err != nil {
			t.Errorf("read real pilot subscription request: %v", err)
			return
		}
		writes <- append([]byte(nil), subscription...)
		_, _ = conn.Write([]byte(`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"real-runtime-sub"}` + "\r\n"))
		_, _ = conn.Write([]byte(runtimeMarketImageMessage("1.111", 0, now) + "\r\n"))
		_, _ = conn.Write([]byte(`{"op":"heartbeat","id":3,"clk":"real-runtime-heartbeat"}` + "\r\n"))
	})
	defer server.close()

	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--real-runtime-pilot",
		"--transport", "tls",
		"--sink", "memory",
		"--address", server.address,
		"--server-name", "localhost",
		"--app-key-env", "RUNTIME_TEST_APP_KEY",
		"--session-token-env", "RUNTIME_TEST_SESSION_TOKEN",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, nil, runtimeLocalTLSTransportFactory(server))
	if code != exitRuntimeSuccess {
		t.Fatalf("real pilot memory exit=%d output=%s", code, out.String())
	}
	var status RuntimeStatus
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &status); err != nil {
		t.Fatalf("decode real pilot memory output %s: %v", out.String(), err)
	}
	if !status.RealRuntimePilot || status.TransportMode != runtimeTransportTLS || status.SinkType != runtimeSinkMemory {
		t.Fatalf("real pilot memory status=%+v", status)
	}
	if status.FakeTransportOnly || status.CollectorIntegration || !status.MarketStreamOnly {
		t.Fatalf("real pilot boundary flags=%+v", status)
	}
	if !status.Connected || !status.Authenticated || !status.Subscribed {
		t.Fatalf("real pilot connection flags=%+v", status)
	}
	if status.MessagesRead != 5 || status.CandidatesEmitted != 2 || status.RunID != 0 || status.DatabaseEnabled {
		t.Fatalf("real pilot counters/status=%+v", status)
	}
	if status.LastClock != "real-runtime-heartbeat" || status.LastInitialClock != "real-runtime-initial" {
		t.Fatalf("real pilot clocks=%q/%q", status.LastClock, status.LastInitialClock)
	}

	authPayload := decodeRuntimeTLSWrite(t, readRuntimeTLSWrite(t, writes))
	if authPayload["appKey"] != appKey || authPayload["session"] != sessionToken {
		t.Fatalf("real pilot auth payload=%+v", authPayload)
	}
	subscriptionPayload := decodeRuntimeTLSWrite(t, readRuntimeTLSWrite(t, writes))
	if subscriptionPayload["op"] != "marketSubscription" {
		t.Fatalf("real pilot subscription payload=%+v", subscriptionPayload)
	}
	assertRuntimeSubscriptionDataFilter(t, subscriptionPayload)
	assertRuntimeNoSecrets(t, out.String(), appKey, sessionToken)
}

func TestRuntimeRealPilotPostgresSinkUsesExistingTelemetryPathWithFakeStore(t *testing.T) {
	appKey := "REAL_RUNTIME_APP_SECRET"
	sessionToken := "REAL_RUNTIME_SESSION_SECRET"
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_APP_KEY", appKey)
	t.Setenv("RUNTIME_TEST_SESSION_TOKEN", sessionToken)
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)

	saver := newFakeRuntimeBatchSaver()
	saver.nextRunID = 909
	storeCalls := 0
	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--real-runtime-pilot",
		"--transport", "tls",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--address", "localhost:443",
		"--server-name", "localhost",
		"--app-key-env", "RUNTIME_TEST_APP_KEY",
		"--session-token-env", "RUNTIME_TEST_SESSION_TOKEN",
		"--run-label", "phase10i_test",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		storeCalls++
		if databaseURL != rawDatabaseURL {
			t.Fatalf("databaseURL=%q want explicit env value", databaseURL)
		}
		return saver, nil
	}, func(group RuntimeStreamGroup) betfair.LiveTransport {
		return newFakeRuntimeTransport(defaultFakeRuntimeMessages(group.MarketIDs, fixedRuntimeNow()))
	})
	if code != exitRuntimeSuccess {
		t.Fatalf("real pilot postgres exit=%d output=%s", code, out.String())
	}
	if storeCalls != 1 || saver.startCount() != 1 || saver.finishCount() != 1 {
		t.Fatalf("real pilot postgres storeCalls=%d starts=%d finishes=%d", storeCalls, saver.startCount(), saver.finishCount())
	}
	starts := saver.telemetryStarts()
	if starts[0].RuntimeMode != runtimeModeRealPilot || starts[0].TransportMode != runtimeTransportTLS || starts[0].SinkType != runtimeSinkPostgres {
		t.Fatalf("real pilot telemetry start=%+v", starts[0])
	}
	finishes := saver.telemetryFinishes()
	if finishes[0].runID != 909 || finishes[0].input.Status != model.BetfairRuntimeRunStatusSuccess {
		t.Fatalf("real pilot telemetry finish=%+v", finishes[0])
	}
	var status RuntimeStatus
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &status); err != nil {
		t.Fatalf("decode real pilot postgres output %s: %v", out.String(), err)
	}
	if status.RunID != 909 || !status.RealRuntimePilot || status.RawMessagesInserted != 4 || status.CandidatesInserted != 2 {
		t.Fatalf("real pilot postgres status=%+v", status)
	}
	if !status.Connected || !status.Authenticated || !status.Subscribed {
		t.Fatalf("real pilot postgres connection flags=%+v", status)
	}
	assertRuntimeNoSecrets(t, out.String(), appKey, sessionToken, rawDatabaseURL, "runtime_user:runtime_pass")
}

func TestRuntimeRealPilotAuthFailureRedactsSecrets(t *testing.T) {
	appKey := "REAL_RUNTIME_APP_SECRET"
	sessionToken := "REAL_RUNTIME_SESSION_SECRET"
	t.Setenv("RUNTIME_TEST_APP_KEY", appKey)
	t.Setenv("RUNTIME_TEST_SESSION_TOKEN", sessionToken)

	server := newRuntimeLocalTLSServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read real pilot auth request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(fmt.Sprintf(`{"op":"status","id":1,"statusCode":"FAILURE","errorCode":"AUTH_FAIL","errorMessage":"bad %s %s"}`, appKey, sessionToken) + "\r\n"))
	})
	defer server.close()

	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--real-runtime-pilot",
		"--transport", "tls",
		"--sink", "memory",
		"--address", server.address,
		"--server-name", "localhost",
		"--app-key-env", "RUNTIME_TEST_APP_KEY",
		"--session-token-env", "RUNTIME_TEST_SESSION_TOKEN",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, nil, runtimeLocalTLSTransportFactory(server))
	if code != exitRuntimeRuntime {
		t.Fatalf("real pilot auth failure exit=%d want runtime output=%s", code, out.String())
	}
	text := out.String()
	if !strings.Contains(text, "[REDACTED_BETFAIR_APP_KEY]") || !strings.Contains(text, "[REDACTED_BETFAIR_SESSION_TOKEN]") {
		t.Fatalf("real pilot auth failure output missing redaction markers: %s", text)
	}
	assertRuntimeNoSecrets(t, text, appKey, sessionToken)
}

func TestRuntimeRealPilotContextCancellationStopsBlockedRead(t *testing.T) {
	appKey := "REAL_RUNTIME_APP_SECRET"
	sessionToken := "REAL_RUNTIME_SESSION_SECRET"
	streaming := make(chan struct{})
	server := newRuntimeLocalTLSServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read real pilot auth request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(`{"op":"status","id":1,"statusCode":"SUCCESS"}` + "\r\n"))
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read real pilot subscription request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(`{"op":"status","id":2,"statusCode":"SUCCESS"}` + "\r\n"))
		time.Sleep(25 * time.Millisecond)
		close(streaming)
		_, _ = reader.Peek(1)
	})
	defer server.close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		status RuntimeStatus
		err    error
	}, 1)
	go func() {
		status, err := NewRuntimeSupervisor(RuntimeOptions{
			Enabled:          true,
			RealRuntimePilot: true,
			TransportMode:    runtimeTransportTLS,
			AppKey:           appKey,
			SessionToken:     sessionToken,
			MarketIDs:        []string{"1.111"},
			DurationLimit:    time.Second,
			TransportFactory: runtimeLocalTLSTransportFactory(server),
		}).Run(ctx)
		done <- struct {
			status RuntimeStatus
			err    error
		}{status: status, err: err}
	}()

	select {
	case <-streaming:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for real pilot stream loop")
	}
	cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("real pilot cancellation error=%v want context.Canceled status=%+v", result.err, result.status)
		}
		if result.status.Status != runtimeStatusStopped || result.status.StoppedReason != betfair.LiveStopReasonContextCanceled {
			t.Fatalf("real pilot cancellation status=%+v", result.status)
		}
		if !result.status.RealRuntimePilot || !result.status.Connected || !result.status.Authenticated || !result.status.Subscribed {
			t.Fatalf("real pilot cancellation flags=%+v", result.status)
		}
		assertRuntimeNoSecrets(t, result.status.FailureReason, appKey, sessionToken)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for real pilot runtime cancellation")
	}
}

func TestRuntimeFakeHappyPath(t *testing.T) {
	now := fixedRuntimeNow()
	sink := &InMemoryRuntimeSink{}
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:       true,
		TransportMode: runtimeTransportFake,
		MarketIDs:     []string{"1.111"},
		MaxMessages:   10,
		DurationLimit: time.Second,
		Now:           func() time.Time { return now },
		Sink:          sink,
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("fake runtime happy path: %v status=%+v", err, status)
	}
	if status.Status != runtimeStatusStopped || status.Readiness != runtimeReadinessStopped {
		t.Fatalf("final status=%+v", status)
	}
	if status.MessagesRead != 4 || status.MarketChangeMessages != 1 || status.HeartbeatMessages != 1 || status.StatusMessages != 2 {
		t.Fatalf("message counters=%+v", status)
	}
	if status.MarketStreamMessages != 1 || status.MarketPayloadMessages != 1 || status.ServerHeartbeatMessages != 1 {
		t.Fatalf("classified stream counters=%+v", status)
	}
	if status.CandidatesEmitted != 2 || status.MarketsSeen != 1 {
		t.Fatalf("candidate/market counters=%+v", status)
	}
	if status.LastClock != "runtime-market-1" || status.LastInitialClock != "runtime-initial" {
		t.Fatalf("clocks=%q/%q", status.LastClock, status.LastInitialClock)
	}
	if got := sink.RawMessagesSeen(); got != 4 {
		t.Fatalf("sink raw messages=%d want 4", got)
	}
	candidates := sink.Candidates()
	if len(candidates) != 2 {
		t.Fatalf("sink candidates=%d want 2", len(candidates))
	}
	back := requireRuntimeCandidate(t, candidates, "1.111", betfair.ExchangeSideBack)
	lay := requireRuntimeCandidate(t, candidates, "1.111", betfair.ExchangeSideLay)
	if back.PriceDecimal != 2.10 || lay.PriceDecimal != 2.14 {
		t.Fatalf("candidate prices back=%+v lay=%+v", back, lay)
	}
	if status.RawMessagesSeen != 4 || status.CandidateBatchesSeen != 1 || status.CandidateBatchesFlushed != 1 || status.LatestCandidateCount != 2 {
		t.Fatalf("runtime accounting status=%+v", status)
	}
	if status.SinkType != runtimeSinkMemory || status.RawMessagesFlushed != 4 || status.CandidatesFlushed != 2 || status.FlushCount == 0 {
		t.Fatalf("runtime sink stats status=%+v", status)
	}
	if status.LastCandidateTime == "" || status.LastSinkFlushTime == "" || status.LastFlushTime == "" {
		t.Fatalf("runtime accounting times missing in %+v", status)
	}
}

func TestRuntimeDiagnosticsFakeHappyPath(t *testing.T) {
	now := fixedRuntimeNow()
	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "memory",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--diagnose-stream",
		"--diagnostic-sample-limit", "3",
		"--output", "json",
	}, &out, nil, func(group RuntimeStreamGroup) betfair.LiveTransport {
		return newFakeRuntimeTransport(defaultFakeRuntimeMessages(group.MarketIDs, now))
	})
	if code != exitRuntimeSuccess {
		t.Fatalf("diagnostic fake runtime exit=%d output=%s", code, out.String())
	}
	var status RuntimeStatus
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &status); err != nil {
		t.Fatalf("decode diagnostic fake output %s: %v", out.String(), err)
	}
	diagnostics := status.StreamDiagnostics
	if diagnostics == nil {
		t.Fatalf("missing stream diagnostics in %+v", status)
	}
	if !reflect.DeepEqual(diagnostics.RequestedMarketDataFields, betfair.DefaultMarketDataFields()) || diagnostics.RequestedLadderLevels != defaultRuntimeLadderLevels {
		t.Fatalf("diagnostic requested filter=%+v", diagnostics)
	}
	if diagnostics.RequestedMarketIDsCount != 1 ||
		diagnostics.RequestedMarketFilterMarketIDsCount != 1 ||
		!diagnostics.RequestedMarketFilterHasMarketIDs ||
		diagnostics.RequestedMarketFilterBroadScope ||
		diagnostics.SubscriptionRequestID != 2 {
		t.Fatalf("diagnostic subscription summary=%+v", diagnostics)
	}
	if len(diagnostics.RequestedMarketIDHashes) != 1 || strings.Contains(strings.Join(diagnostics.RequestedMarketIDHashes, ","), "1.111") {
		t.Fatalf("diagnostic market id hashes not redacted=%+v", diagnostics.RequestedMarketIDHashes)
	}
	if diagnostics.StatusMessagesSeen != 2 || diagnostics.StatusSuccessCount != 2 || diagnostics.StatusFailureCount != 0 {
		t.Fatalf("diagnostic status counters=%+v", diagnostics)
	}
	if !reflect.DeepEqual(diagnostics.StatusIDsSeen, []int{1, 2}) ||
		diagnostics.AuthenticationStatusCode != "SUCCESS" ||
		diagnostics.AuthenticationID != 1 ||
		diagnostics.SubscriptionStatusCode != "SUCCESS" ||
		diagnostics.SubscriptionID != 2 {
		t.Fatalf("diagnostic status details=%+v", diagnostics)
	}
	if diagnostics.ParsedMessages != 4 || diagnostics.StatusMessages != 2 || diagnostics.MarketChangeMessages != 1 {
		t.Fatalf("diagnostic message counters=%+v", diagnostics)
	}
	if diagnostics.MCMTotal != 1 || diagnostics.MCMWithMC != 1 || diagnostics.MCMWithoutMC != 0 || diagnostics.MCMWithCTSubImage != 1 {
		t.Fatalf("diagnostic mcm counters=%+v", diagnostics)
	}
	if diagnostics.MarketChangeMessagesWithMC != 1 || diagnostics.MarketChangeMessagesWithoutMC != 0 {
		t.Fatalf("diagnostic market-change counters=%+v", diagnostics)
	}
	if diagnostics.MarketIDsSeenCount != 1 || !reflect.DeepEqual(diagnostics.MarketIDsSeen, []string{"1.111"}) {
		t.Fatalf("diagnostic market ids=%+v", diagnostics)
	}
	if diagnostics.RunnerChangesSeen != 1 ||
		diagnostics.RunnerChangesWithSelectionID != 1 ||
		diagnostics.RunnerChangesWithBATB != 1 ||
		diagnostics.RunnerChangesWithBATL != 1 ||
		diagnostics.RunnerChangesWithLTP != 1 ||
		diagnostics.RunnerChangesWithTradedVolume != 1 {
		t.Fatalf("diagnostic runner counters=%+v", diagnostics)
	}
	if diagnostics.CacheApplySuccess != 1 || diagnostics.CacheApplyErrors != 0 || diagnostics.MarketsInCacheAfterApply != 1 {
		t.Fatalf("diagnostic cache counters=%+v", diagnostics)
	}
	if diagnostics.QuoteEmitAttempts != 1 || diagnostics.QuoteEmitErrors != 0 || diagnostics.QuoteCandidatesSkipped != 0 {
		t.Fatalf("diagnostic quote counters=%+v", diagnostics)
	}
	if len(diagnostics.Samples) != 1 || diagnostics.Samples[0].Op != string(betfair.OperationMarketChange) || diagnostics.Samples[0].MarketID != "1.111" {
		t.Fatalf("diagnostic samples=%+v", diagnostics.Samples)
	}
	if len(diagnostics.StatusSamples) != 2 || diagnostics.StatusSamples[0].StatusCode != "SUCCESS" || diagnostics.StatusSamples[1].StatusCode != "SUCCESS" {
		t.Fatalf("diagnostic status samples=%+v", diagnostics.StatusSamples)
	}
	if strings.Contains(out.String(), "marketDefinition") || strings.Contains(out.String(), `"rc"`) || strings.Contains(out.String(), "appKey") || strings.Contains(out.String(), "session") {
		t.Fatalf("diagnostic output leaked raw payload detail: %s", out.String())
	}
}

func TestRuntimeDiagnosticsMarketChangeWithoutMC(t *testing.T) {
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:               true,
		TransportMode:         runtimeTransportFake,
		MarketIDs:             []string{"1.111"},
		MaxMessages:           10,
		DurationLimit:         time.Second,
		DiagnoseStream:        true,
		DiagnosticSampleLimit: 2,
		TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
			return newFakeRuntimeTransport([]string{
				`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"runtime-initial"}`,
				`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"runtime-subscription"}`,
				`{"op":"mcm","id":2,"ct":"SUB_IMAGE","clk":"empty-sub-image"}`,
				`{"op":"mcm","id":2,"ct":"HEARTBEAT","clk":"empty-market-change"}`,
			})
		},
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("diagnostic empty mcm runtime: %v status=%+v", err, status)
	}
	diagnostics := status.StreamDiagnostics
	if diagnostics == nil {
		t.Fatalf("missing stream diagnostics in %+v", status)
	}
	if diagnostics.MarketChangeMessages != 2 || diagnostics.MarketChangeMessagesWithMC != 0 || diagnostics.MarketChangeMessagesWithoutMC != 2 {
		t.Fatalf("diagnostic empty mcm counters=%+v", diagnostics)
	}
	if diagnostics.MCMTotal != 2 || diagnostics.MCMWithMC != 0 || diagnostics.MCMWithoutMC != 2 ||
		diagnostics.MCMServerHeartbeatOrControl != 2 || diagnostics.MCMWithCTSubImage != 1 || diagnostics.MCMWithCTHeartbeat != 1 {
		t.Fatalf("diagnostic empty mcm classification=%+v", diagnostics)
	}
	if diagnostics.CacheApplySuccess != 0 || diagnostics.CacheApplyErrors != 0 || diagnostics.MarketsInCacheAfterApply != 0 {
		t.Fatalf("diagnostic empty mcm cache counters=%+v", diagnostics)
	}
	if !reflect.DeepEqual(diagnostics.Warnings, []string{runtimeDiagnosticWarningNoMarketPayload, runtimeDiagnosticWarningSubscriptionNoMarketPayload}) {
		t.Fatalf("diagnostic empty mcm warnings=%+v", diagnostics.Warnings)
	}
	var out bytes.Buffer
	if err := writeRuntimeStatus(&out, status, runtimeOutputJSON); err != nil {
		t.Fatalf("write diagnostic empty mcm json: %v", err)
	}
	if !strings.Contains(out.String(), "requested_market_id_hashes") ||
		strings.Contains(out.String(), `"requested_market_ids":["`) ||
		strings.Contains(out.String(), `"1.111"`) {
		t.Fatalf("diagnostic subscription summary not redacted: %s", out.String())
	}
	if !strings.Contains(out.String(), runtimeDiagnosticWarningSubscriptionNoMarketPayload) {
		t.Fatalf("diagnostic subscription warning missing: %s", out.String())
	}
	if status.MarketStreamMessages != 2 || status.MarketPayloadMessages != 0 || status.ServerHeartbeatMessages != 2 {
		t.Fatalf("diagnostic empty mcm top-level counters=%+v", status)
	}
	if status.CandidatesEmitted != 0 || status.MarketsSeen != 0 {
		t.Fatalf("diagnostic empty mcm status=%+v", status)
	}
}

func TestRuntimeDiagnosticsStatusFailureIsSanitized(t *testing.T) {
	appKey := "RUNTIME_DIAG_APP_SECRET"
	sessionToken := "RUNTIME_DIAG_SESSION_SECRET"
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:               true,
		TransportMode:         runtimeTransportFake,
		MarketIDs:             []string{"1.111"},
		MaxMessages:           10,
		DurationLimit:         time.Second,
		AppKey:                appKey,
		SessionToken:          sessionToken,
		DiagnoseStream:        true,
		DiagnosticSampleLimit: 2,
		TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
			return newFakeRuntimeTransport([]string{
				`{"op":"status","id":1,"statusCode":"FAILURE","errorCode":"AUTH_FAIL","errorMessage":"bad ` + appKey + ` ` + sessionToken + `"}`,
			})
		},
	}).Run(context.Background())
	if err == nil {
		t.Fatalf("diagnostic status failure err=nil status=%+v", status)
	}
	diagnostics := status.StreamDiagnostics
	if diagnostics == nil {
		t.Fatalf("missing stream diagnostics in %+v", status)
	}
	if diagnostics.StatusMessagesSeen != 1 || diagnostics.StatusSuccessCount != 0 || diagnostics.StatusFailureCount != 1 {
		t.Fatalf("diagnostic failure status counters=%+v", diagnostics)
	}
	if !reflect.DeepEqual(diagnostics.StatusErrorCodes, []string{"AUTH_FAIL"}) || diagnostics.AuthenticationErrorCode != "AUTH_FAIL" || diagnostics.AuthenticationStatusCode != "FAILURE" {
		t.Fatalf("diagnostic failure status details=%+v", diagnostics)
	}
	if len(diagnostics.StatusSamples) != 1 || diagnostics.StatusSamples[0].ErrorCode != "AUTH_FAIL" || diagnostics.StatusSamples[0].StatusCode != "FAILURE" {
		t.Fatalf("diagnostic failure status samples=%+v", diagnostics.StatusSamples)
	}
	var out bytes.Buffer
	if err := writeRuntimeStatus(&out, status, runtimeOutputJSON); err != nil {
		t.Fatalf("write diagnostic status failure json: %v", err)
	}
	assertRuntimeNoSecrets(t, out.String(), appKey, sessionToken)
	if strings.Contains(out.String(), "errorMessage") {
		t.Fatalf("diagnostic status failure leaked raw status payload field: %s", out.String())
	}
}

func TestRuntimeDiagnosticsRunnerChangesNoLadders(t *testing.T) {
	message := `{"op":"mcm","id":3,"pt":1700000000000,"ct":"SUB_IMAGE","clk":"no-ladders","mc":[{"id":"1.111","img":true,"marketDefinition":{"status":"OPEN","name":"Match Odds","runners":[{"id":101,"name":"Home","status":"ACTIVE"}]},"rc":[{"id":101}]}]}`
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:               true,
		TransportMode:         runtimeTransportFake,
		MarketIDs:             []string{"1.111"},
		MaxMessages:           10,
		DurationLimit:         time.Second,
		DiagnoseStream:        true,
		DiagnosticSampleLimit: 1,
		TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
			return newFakeRuntimeTransport([]string{
				`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"runtime-initial"}`,
				`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"runtime-subscription"}`,
				message,
			})
		},
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("diagnostic no-ladders runtime: %v status=%+v", err, status)
	}
	diagnostics := status.StreamDiagnostics
	if diagnostics == nil {
		t.Fatalf("missing stream diagnostics in %+v", status)
	}
	if diagnostics.RunnerChangesSeen != 1 || diagnostics.RunnerChangesWithSelectionID != 1 {
		t.Fatalf("diagnostic no-ladders runner counters=%+v", diagnostics)
	}
	if diagnostics.RunnerChangesWithBATB != 0 || diagnostics.RunnerChangesWithBATL != 0 || diagnostics.RunnerChangesWithATB != 0 || diagnostics.RunnerChangesWithATL != 0 {
		t.Fatalf("diagnostic no-ladders ladder counters=%+v", diagnostics)
	}
	if diagnostics.CacheApplySuccess != 1 || diagnostics.MarketsInCacheAfterApply != 1 {
		t.Fatalf("diagnostic no-ladders cache counters=%+v", diagnostics)
	}
	if status.CandidatesEmitted != 0 || status.MarketsSeen != 1 {
		t.Fatalf("diagnostic no-ladders status=%+v", status)
	}
	wantReasons := map[string]int{
		runtimeSkipMissingBackPrice: 1,
		runtimeSkipMissingLayPrice:  1,
	}
	if !reflect.DeepEqual(diagnostics.QuoteCandidatesSkippedReasonCounts, wantReasons) || diagnostics.QuoteCandidatesSkipped != 2 {
		t.Fatalf("diagnostic skip reasons=%+v skipped=%d", diagnostics.QuoteCandidatesSkippedReasonCounts, diagnostics.QuoteCandidatesSkipped)
	}
}

func TestRuntimeDiagnosticsTextOutputIsSanitized(t *testing.T) {
	var out bytes.Buffer
	code := runCommand(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "memory",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--diagnostic-sample-limit", "1",
		"--output", "text",
	}, &out)
	if code != exitRuntimeSuccess {
		t.Fatalf("diagnostic text runtime exit=%d output=%s", code, out.String())
	}
	text := out.String()
	if !strings.Contains(text, "stream_diagnostics") || !strings.Contains(text, "samples=op=mcm") {
		t.Fatalf("diagnostic text output missing counters: %s", text)
	}
	if strings.Contains(text, `{"op"`) || strings.Contains(text, "marketDefinition") || strings.Contains(text, "appKey") || strings.Contains(text, "session") {
		t.Fatalf("diagnostic text output leaked raw payload detail: %s", text)
	}
}

func TestRuntimeMarketIDValidation(t *testing.T) {
	tests := []struct {
		name      string
		marketIDs []string
		want      string
	}{
		{name: "missing", marketIDs: nil, want: "at least one market-id"},
		{name: "wildcard", marketIDs: []string{"*"}, want: "wildcard"},
		{name: "too many", marketIDs: []string{"1.1", "1.2", "1.3", "1.4", "1.5", "1.6"}, want: "too many market IDs"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewRuntimeSupervisor(RuntimeOptions{
				Enabled:       true,
				TransportMode: runtimeTransportFake,
				MarketIDs:     tc.marketIDs,
			}).Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation error=%v want containing %q", err, tc.want)
			}
		})
	}
}

func TestRuntimeDurationAndMaxMessageGuardrails(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RuntimeOptions)
		want   string
	}{
		{"negative max messages", func(o *RuntimeOptions) { o.MaxMessages = -1 }, "max-messages must be positive"},
		{"too many messages", func(o *RuntimeOptions) { o.MaxMessages = maxRuntimeMessages + 1 }, "max-messages exceeds"},
		{"negative duration", func(o *RuntimeOptions) { o.DurationLimit = -time.Second }, "duration must be positive"},
		{"too long duration", func(o *RuntimeOptions) { o.DurationLimit = maxRuntimeDuration + time.Second }, "duration exceeds"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := RuntimeOptions{
				Enabled:       true,
				TransportMode: runtimeTransportFake,
				MarketIDs:     []string{"1.111"},
			}
			tc.mutate(&opts)
			_, err := NewRuntimeSupervisor(opts).Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("guardrail error=%v want containing %q", err, tc.want)
			}
		})
	}
}

func TestRuntimeStopsOnMaxMessages(t *testing.T) {
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:       true,
		TransportMode: runtimeTransportFake,
		MarketIDs:     []string{"1.111"},
		MaxMessages:   1,
		DurationLimit: time.Second,
		Now:           fixedRuntimeNowFunc(),
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("max messages run error=%v", err)
	}
	if status.StoppedReason != betfair.LiveStopReasonMaxMessages || status.MessagesRead != 3 {
		t.Fatalf("max message status=%+v", status)
	}
}

func TestRuntimeStopsOnDuration(t *testing.T) {
	conn := newBlockingRuntimeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
	)
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:       true,
		TransportMode: runtimeTransportFake,
		MarketIDs:     []string{"1.111"},
		DurationLimit: 20 * time.Millisecond,
		TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
			return &singleRuntimeTransport{conn: conn}
		},
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("duration run error=%v want nil", err)
	}
	if status.Status != runtimeStatusStopped || status.StoppedReason != runtimeStopReasonDuration {
		t.Fatalf("duration status=%+v", status)
	}
}

func TestRuntimeContextCancellation(t *testing.T) {
	conn := newBlockingRuntimeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
	)
	connected := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		status RuntimeStatus
		err    error
	}, 1)
	go func() {
		status, err := NewRuntimeSupervisor(RuntimeOptions{
			Enabled:       true,
			TransportMode: runtimeTransportFake,
			MarketIDs:     []string{"1.111"},
			DurationLimit: time.Second,
			TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
				connected <- struct{}{}
				return &singleRuntimeTransport{conn: conn}
			},
		}).Run(ctx)
		done <- struct {
			status RuntimeStatus
			err    error
		}{status: status, err: err}
	}()
	<-connected
	conn.waitForWrites(t, 2)
	cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("canceled runtime error=%v want context.Canceled status=%+v", result.err, result.status)
		}
		if result.status.Status != runtimeStatusStopped || result.status.StoppedReason != betfair.LiveStopReasonContextCanceled {
			t.Fatalf("canceled status=%+v", result.status)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for canceled runtime")
	}
}

func TestRuntimeStatusTransitions(t *testing.T) {
	sink := &InMemoryRuntimeSink{}
	_, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:       true,
		TransportMode: runtimeTransportFake,
		MarketIDs:     []string{"1.111"},
		MaxMessages:   10,
		DurationLimit: time.Second,
		Sink:          sink,
		Now:           fixedRuntimeNowFunc(),
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("run runtime: %v", err)
	}
	statuses := sink.Statuses()
	got := make([]string, 0, len(statuses))
	for _, status := range statuses {
		got = append(got, status.Status)
	}
	want := []string{runtimeStatusStarting, runtimeStatusRunning, runtimeStatusStopping, runtimeStatusStopped}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("status transitions=%v want %v full=%+v", got, want, statuses)
	}
}

func TestRuntimeReadinessClassification(t *testing.T) {
	tests := []struct {
		name   string
		status RuntimeStatus
		want   string
	}{
		{name: "disabled", status: RuntimeStatus{Enabled: false, Status: runtimeStatusDisabled}, want: runtimeReadinessDisabled},
		{name: "starting", status: RuntimeStatus{Enabled: true, Status: runtimeStatusStarting}, want: runtimeReadinessStarting},
		{name: "ready", status: RuntimeStatus{Enabled: true, Running: true, Status: runtimeStatusRunning, HeartbeatState: string(betfair.StreamStateHealthy)}, want: runtimeReadinessReady},
		{name: "degraded", status: RuntimeStatus{Enabled: true, Running: true, Status: runtimeStatusRunning, HeartbeatState: string(betfair.StreamStateAging)}, want: runtimeReadinessDegraded},
		{name: "sink error degraded", status: RuntimeStatus{Enabled: true, Running: true, Status: runtimeStatusRunning, HeartbeatState: string(betfair.StreamStateHealthy), SinkErrors: 1}, want: runtimeReadinessDegraded},
		{name: "stale", status: RuntimeStatus{Enabled: true, Running: true, Status: runtimeStatusRunning, HeartbeatState: string(betfair.StreamStateStale)}, want: runtimeReadinessStale},
		{name: "failed", status: RuntimeStatus{Enabled: true, Status: runtimeStatusFailed}, want: runtimeReadinessFailed},
		{name: "stopped", status: RuntimeStatus{Enabled: true, Status: runtimeStatusStopped}, want: runtimeReadinessStopped},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := RuntimeReadiness(tc.status); got != tc.want {
				t.Fatalf("readiness=%q want %q for %+v", got, tc.want, tc.status)
			}
		})
	}
}

func TestRuntimeHeartbeatDoesNotMutatePrices(t *testing.T) {
	now := fixedRuntimeNow()
	messages := []string{
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"initial-one"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"sub-one"}`,
		runtimeMarketImageMessage("1.111", 0, now),
		`{"op":"heartbeat","id":3,"clk":"heartbeat-only"}`,
	}
	sink := &InMemoryRuntimeSink{}
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:       true,
		TransportMode: runtimeTransportFake,
		MarketIDs:     []string{"1.111"},
		MaxMessages:   10,
		DurationLimit: time.Second,
		Now:           func() time.Time { return now },
		Sink:          sink,
		TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
			return newFakeRuntimeTransport(messages)
		},
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("heartbeat run error=%v", err)
	}
	if status.LastClock != "heartbeat-only" || status.HeartbeatMessages != 1 {
		t.Fatalf("heartbeat status=%+v", status)
	}
	back := requireRuntimeCandidate(t, sink.Candidates(), "1.111", betfair.ExchangeSideBack)
	if back.PriceDecimal != 2.10 || back.AvailableSize != 50 {
		t.Fatalf("heartbeat mutated candidate=%+v", back)
	}
}

func TestRuntimeReconnectCount(t *testing.T) {
	first := newFakeRuntimeDisconnectConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"initial-one"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
		runtimeMarketImageMessage("1.111", 0, fixedRuntimeNow()),
	)
	second := newFakeRuntimeConn(
		`{"op":"status","id":3,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":4,"statusCode":"SUCCESS"}`,
	)
	transport := &fakeRuntimeTransport{conns: []*fakeRuntimeConn{first, second}}
	sink := &InMemoryRuntimeSink{}
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:              true,
		TransportMode:        runtimeTransportFake,
		MarketIDs:            []string{"1.111"},
		MaxMessages:          10,
		DurationLimit:        time.Second,
		MaxReconnectAttempts: 1,
		ReconnectBaseDelay:   time.Millisecond,
		ReconnectMaxDelay:    time.Millisecond,
		Now:                  fixedRuntimeNowFunc(),
		Sink:                 sink,
		TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
			return transport
		},
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("reconnect run error=%v status=%+v", err, status)
	}
	if status.ReconnectCount != 1 || transport.connectCount() != 2 {
		t.Fatalf("reconnect counters status=%+v transport=%d", status, transport.connectCount())
	}
	if len(sink.Candidates()) != 2 {
		t.Fatalf("candidates after reconnect=%d want 2", len(sink.Candidates()))
	}
}

func TestRuntimeLocalTLSHappyPath(t *testing.T) {
	now := fixedRuntimeNow()
	writes := make(chan []byte, 2)
	server := newRuntimeLocalTLSServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		auth, err := reader.ReadBytes('\n')
		if err != nil {
			t.Errorf("read local TLS auth request: %v", err)
			return
		}
		writes <- append([]byte(nil), auth...)
		_, _ = conn.Write([]byte(`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"tls-runtime-initial"}` + "\r\n"))

		subscription, err := reader.ReadBytes('\n')
		if err != nil {
			t.Errorf("read local TLS subscription request: %v", err)
			return
		}
		writes <- append([]byte(nil), subscription...)
		_, _ = conn.Write([]byte(`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"tls-runtime-sub"}` + "\r\n"))
		_, _ = conn.Write([]byte(runtimeMarketImageMessage("1.111", 0, now) + "\r\n"))
		_, _ = conn.Write([]byte(`{"op":"heartbeat","id":3,"clk":"tls-runtime-heartbeat"}` + "\r\n"))
	})
	defer server.close()

	sink := &InMemoryRuntimeSink{}
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:          true,
		TransportMode:    runtimeTransportFake,
		MarketIDs:        []string{"1.111"},
		MaxMessages:      10,
		DurationLimit:    time.Second,
		Now:              func() time.Time { return now },
		Sink:             sink,
		TransportFactory: runtimeLocalTLSTransportFactory(server),
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("local TLS runtime happy path: %v status=%+v", err, status)
	}
	if status.Status != runtimeStatusStopped || status.Readiness != runtimeReadinessStopped || status.StoppedReason != betfair.LiveStopReasonEOF {
		t.Fatalf("local TLS final status=%+v", status)
	}
	if status.MessagesRead != 4 || status.MarketChangeMessages != 1 || status.HeartbeatMessages != 1 || status.StatusMessages != 2 {
		t.Fatalf("local TLS counters=%+v", status)
	}
	if status.CandidatesEmitted != 2 || status.MarketsSeen != 1 || status.HeartbeatState != string(betfair.StreamStateHealthy) {
		t.Fatalf("local TLS candidate/heartbeat status=%+v", status)
	}
	if status.LastClock != "tls-runtime-heartbeat" || status.LastInitialClock != "tls-runtime-initial" {
		t.Fatalf("local TLS clocks=%q/%q", status.LastClock, status.LastInitialClock)
	}
	back := requireRuntimeCandidate(t, sink.Candidates(), "1.111", betfair.ExchangeSideBack)
	lay := requireRuntimeCandidate(t, sink.Candidates(), "1.111", betfair.ExchangeSideLay)
	if back.PriceDecimal != 2.10 || lay.PriceDecimal != 2.14 {
		t.Fatalf("local TLS candidates back=%+v lay=%+v", back, lay)
	}

	auth := readRuntimeTLSWrite(t, writes)
	subscription := readRuntimeTLSWrite(t, writes)
	authPayload := decodeRuntimeTLSWrite(t, auth)
	if authPayload["op"] != "authentication" {
		t.Fatalf("auth payload=%+v", authPayload)
	}
	redactedAuth := string(betfair.RedactLivePayload(auth))
	assertRuntimeNoFakeSecrets(t, redactedAuth)
	if !strings.Contains(redactedAuth, "[REDACTED_BETFAIR_APP_KEY]") || !strings.Contains(redactedAuth, "[REDACTED_BETFAIR_SESSION_TOKEN]") {
		t.Fatalf("redacted auth payload=%s", redactedAuth)
	}

	subscriptionPayload := decodeRuntimeTLSWrite(t, subscription)
	if subscriptionPayload["op"] != "marketSubscription" {
		t.Fatalf("subscription payload=%+v", subscriptionPayload)
	}
	assertRuntimeSubscriptionDataFilter(t, subscriptionPayload)
	marketFilter, ok := subscriptionPayload["marketFilter"].(map[string]any)
	if !ok {
		t.Fatalf("subscription marketFilter=%+v", subscriptionPayload["marketFilter"])
	}
	marketIDs, ok := marketFilter["marketIds"].([]any)
	if !ok || len(marketIDs) != 1 || marketIDs[0] != "1.111" {
		t.Fatalf("subscription marketIds=%+v", marketFilter["marketIds"])
	}
}

func TestRuntimeLocalTLSMaxMessageStop(t *testing.T) {
	now := fixedRuntimeNow()
	server := newRuntimeLocalTLSServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read local TLS auth request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"tls-max-initial"}` + "\r\n"))
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read local TLS subscription request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"tls-max-sub"}` + "\r\n"))
		_, _ = conn.Write([]byte(runtimeMarketImageMessage("1.111", 0, now) + "\r\n"))
		_, _ = conn.Write([]byte(`{"op":"heartbeat","id":3,"clk":"should-not-be-required"}` + "\r\n"))
	})
	defer server.close()

	sink := &InMemoryRuntimeSink{}
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:          true,
		TransportMode:    runtimeTransportFake,
		MarketIDs:        []string{"1.111"},
		MaxMessages:      1,
		DurationLimit:    time.Second,
		Now:              func() time.Time { return now },
		Sink:             sink,
		TransportFactory: runtimeLocalTLSTransportFactory(server),
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("local TLS max-message run: %v status=%+v", err, status)
	}
	if status.Status != runtimeStatusStopped || status.StoppedReason != betfair.LiveStopReasonMaxMessages || status.MessagesRead != 3 {
		t.Fatalf("local TLS max-message status=%+v", status)
	}
	if len(sink.Candidates()) != 2 {
		t.Fatalf("local TLS max-message candidates=%d want 2", len(sink.Candidates()))
	}
}

func TestRuntimeLocalTLSDurationStop(t *testing.T) {
	streaming := make(chan struct{})
	server := newRuntimeLocalTLSServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read local TLS auth request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(`{"op":"status","id":1,"statusCode":"SUCCESS"}` + "\r\n"))
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read local TLS subscription request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(`{"op":"status","id":2,"statusCode":"SUCCESS"}` + "\r\n"))
		close(streaming)
		_, _ = reader.Peek(1)
	})
	defer server.close()

	done := make(chan struct {
		status RuntimeStatus
		err    error
	}, 1)
	go func() {
		status, err := NewRuntimeSupervisor(RuntimeOptions{
			Enabled:          true,
			TransportMode:    runtimeTransportFake,
			MarketIDs:        []string{"1.111"},
			DurationLimit:    100 * time.Millisecond,
			TransportFactory: runtimeLocalTLSTransportFactory(server),
		}).Run(context.Background())
		done <- struct {
			status RuntimeStatus
			err    error
		}{status: status, err: err}
	}()

	select {
	case <-streaming:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for local TLS duration stream loop")
	}

	var status RuntimeStatus
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("local TLS duration run error=%v want nil", result.err)
		}
		status = result.status
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for local TLS duration stop")
	}
	if status.Status != runtimeStatusStopped || status.StoppedReason != runtimeStopReasonDuration {
		t.Fatalf("local TLS duration status=%+v", status)
	}
}

func TestRuntimeLocalTLSContextCancellationStopsBlockedRead(t *testing.T) {
	streaming := make(chan struct{})
	server := newRuntimeLocalTLSServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read local TLS auth request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(`{"op":"status","id":1,"statusCode":"SUCCESS"}` + "\r\n"))
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read local TLS subscription request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(`{"op":"status","id":2,"statusCode":"SUCCESS"}` + "\r\n"))
		close(streaming)
		_, _ = reader.Peek(1)
	})
	defer server.close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		status RuntimeStatus
		err    error
	}, 1)
	go func() {
		status, err := NewRuntimeSupervisor(RuntimeOptions{
			Enabled:          true,
			TransportMode:    runtimeTransportFake,
			MarketIDs:        []string{"1.111"},
			DurationLimit:    time.Second,
			TransportFactory: runtimeLocalTLSTransportFactory(server),
		}).Run(ctx)
		done <- struct {
			status RuntimeStatus
			err    error
		}{status: status, err: err}
	}()

	select {
	case <-streaming:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for local TLS stream loop")
	}
	cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("local TLS cancellation error=%v want context.Canceled status=%+v", result.err, result.status)
		}
		if result.status.Status != runtimeStatusStopped || result.status.StoppedReason != betfair.LiveStopReasonContextCanceled {
			t.Fatalf("local TLS cancellation status=%+v", result.status)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for local TLS runtime cancellation")
	}
}

func TestRuntimeLocalTLSAuthFailureRedactsSecrets(t *testing.T) {
	server := newRuntimeLocalTLSServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read local TLS auth request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(fmt.Sprintf(`{"op":"status","id":1,"statusCode":"FAILURE","errorCode":"AUTH_FAIL","errorMessage":"bad %s %s"}`, runtimeFakeAppKey, runtimeFakeSessionToken) + "\r\n"))
	})
	defer server.close()

	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:          true,
		TransportMode:    runtimeTransportFake,
		MarketIDs:        []string{"1.111"},
		DurationLimit:    time.Second,
		TransportFactory: runtimeLocalTLSTransportFactory(server),
	}).Run(context.Background())
	if err == nil {
		t.Fatalf("local TLS auth failure error=nil status=%+v", status)
	}
	assertRuntimeNoFakeSecrets(t, err.Error())
	assertRuntimeNoFakeSecrets(t, status.FailureReason)
	if !strings.Contains(status.FailureReason, "[REDACTED_BETFAIR_APP_KEY]") || !strings.Contains(status.FailureReason, "[REDACTED_BETFAIR_SESSION_TOKEN]") {
		t.Fatalf("auth failure missing redaction markers: %+v", status)
	}
	var out bytes.Buffer
	if err := writeRuntimeStatus(&out, status, runtimeOutputJSON); err != nil {
		t.Fatalf("write auth failure status: %v", err)
	}
	assertRuntimeNoFakeSecrets(t, out.String())
}

func TestRuntimeLocalTLSSubscriptionFailureRedactsSecrets(t *testing.T) {
	server := newRuntimeLocalTLSServer(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read local TLS auth request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(`{"op":"status","id":1,"statusCode":"SUCCESS"}` + "\r\n"))
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Errorf("read local TLS subscription request: %v", err)
			return
		}
		_, _ = conn.Write([]byte(fmt.Sprintf(`{"op":"status","id":2,"statusCode":"FAILURE","errorCode":"SUB_FAIL","errorMessage":"bad %s %s"}`, runtimeFakeAppKey, runtimeFakeSessionToken) + "\r\n"))
	})
	defer server.close()

	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:          true,
		TransportMode:    runtimeTransportFake,
		MarketIDs:        []string{"1.111"},
		DurationLimit:    time.Second,
		TransportFactory: runtimeLocalTLSTransportFactory(server),
	}).Run(context.Background())
	if err == nil {
		t.Fatalf("local TLS subscription failure error=nil status=%+v", status)
	}
	assertRuntimeNoFakeSecrets(t, err.Error())
	assertRuntimeNoFakeSecrets(t, status.FailureReason)
	if !strings.Contains(status.FailureReason, "[REDACTED_BETFAIR_APP_KEY]") || !strings.Contains(status.FailureReason, "[REDACTED_BETFAIR_SESSION_TOKEN]") {
		t.Fatalf("subscription failure missing redaction markers: %+v", status)
	}
	var out bytes.Buffer
	if err := writeRuntimeStatus(&out, status, runtimeOutputText); err != nil {
		t.Fatalf("write subscription failure status: %v", err)
	}
	assertRuntimeNoFakeSecrets(t, out.String())
}

func TestRuntimeSinkFlushOnBatchSize(t *testing.T) {
	sink := &InMemoryRuntimeSink{}
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:             true,
		TransportMode:       runtimeTransportFake,
		MarketIDs:           []string{"1.111"},
		MaxMessages:         10,
		DurationLimit:       time.Second,
		RawMessageBatchSize: 2,
		CandidateBatchSize:  2,
		Now:                 fixedRuntimeNowFunc(),
		Sink:                sink,
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("runtime batch flush: %v status=%+v", err, status)
	}
	if sink.FlushCount() < 3 {
		t.Fatalf("flush count=%d want at least raw batch, candidate batch, final flush", sink.FlushCount())
	}
	if status.CandidateBatchesSeen != 1 || status.CandidateBatchesFlushed != 1 {
		t.Fatalf("candidate batch accounting=%+v", status)
	}
	if status.LastSinkFlushTime == "" {
		t.Fatalf("last sink flush time missing: %+v", status)
	}
}

func TestRuntimeSinkFlushAndCloseOnCleanStop(t *testing.T) {
	sink := &InMemoryRuntimeSink{}
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:       true,
		TransportMode: runtimeTransportFake,
		MarketIDs:     []string{"1.111"},
		MaxMessages:   10,
		DurationLimit: time.Second,
		Now:           fixedRuntimeNowFunc(),
		Sink:          sink,
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("runtime clean stop flush: %v status=%+v", err, status)
	}
	if sink.FlushCount() == 0 || sink.CloseCount() != 1 {
		t.Fatalf("flush/close counts flush=%d close=%d", sink.FlushCount(), sink.CloseCount())
	}
	if status.CandidateBatchesFlushed != 1 {
		t.Fatalf("clean stop candidate flush status=%+v", status)
	}
}

func TestRuntimeSinkFlushOnCancellation(t *testing.T) {
	conn := newBlockingRuntimeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
	)
	sink := &InMemoryRuntimeSink{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		status RuntimeStatus
		err    error
	}, 1)
	go func() {
		status, err := NewRuntimeSupervisor(RuntimeOptions{
			Enabled:       true,
			TransportMode: runtimeTransportFake,
			MarketIDs:     []string{"1.111"},
			DurationLimit: time.Second,
			Sink:          sink,
			TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
				return &singleRuntimeTransport{conn: conn}
			},
		}).Run(ctx)
		done <- struct {
			status RuntimeStatus
			err    error
		}{status: status, err: err}
	}()
	conn.waitForWrites(t, 2)
	cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("canceled runtime error=%v want context.Canceled status=%+v", result.err, result.status)
		}
		if sink.FlushCount() == 0 || sink.CloseCount() != 1 {
			t.Fatalf("canceled flush/close counts flush=%d close=%d status=%+v", sink.FlushCount(), sink.CloseCount(), result.status)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancellation flush")
	}
}

func TestRuntimeSinkRawMessageErrorFailsRedacted(t *testing.T) {
	sink := &InMemoryRuntimeSink{}
	sink.InjectRawMessageError(fmt.Errorf("raw sink failed for %s %s", runtimeFakeAppKey, runtimeFakeSessionToken))
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:       true,
		TransportMode: runtimeTransportFake,
		MarketIDs:     []string{"1.111"},
		MaxMessages:   10,
		DurationLimit: time.Second,
		Sink:          sink,
	}).Run(context.Background())
	if err == nil {
		t.Fatalf("raw sink error=nil status=%+v", status)
	}
	if status.Status != runtimeStatusFailed || status.SinkErrors == 0 {
		t.Fatalf("raw sink failure status=%+v", status)
	}
	assertRuntimeNoFakeSecrets(t, err.Error())
	assertRuntimeNoFakeSecrets(t, status.FailureReason)
}

func TestRuntimeSinkQuoteCandidateErrorFailsRedacted(t *testing.T) {
	sink := &InMemoryRuntimeSink{}
	sink.InjectQuoteCandidatesError(fmt.Errorf("candidate sink failed for %s %s", runtimeFakeAppKey, runtimeFakeSessionToken))
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:       true,
		TransportMode: runtimeTransportFake,
		MarketIDs:     []string{"1.111"},
		MaxMessages:   10,
		DurationLimit: time.Second,
		Sink:          sink,
	}).Run(context.Background())
	if err == nil {
		t.Fatalf("candidate sink error=nil status=%+v", status)
	}
	if status.Status != runtimeStatusFailed || status.SinkErrors == 0 || status.CandidateBatchesSeen != 1 {
		t.Fatalf("candidate sink failure status=%+v", status)
	}
	assertRuntimeNoFakeSecrets(t, err.Error())
	assertRuntimeNoFakeSecrets(t, status.FailureReason)
}

func TestRuntimeSinkFlushErrorFailsRedacted(t *testing.T) {
	sink := &InMemoryRuntimeSink{}
	sink.InjectFlushError(fmt.Errorf("flush failed for %s %s", runtimeFakeAppKey, runtimeFakeSessionToken))
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:       true,
		TransportMode: runtimeTransportFake,
		MarketIDs:     []string{"1.111"},
		MaxMessages:   10,
		DurationLimit: time.Second,
		Sink:          sink,
	}).Run(context.Background())
	if err == nil {
		t.Fatalf("flush sink error=nil status=%+v", status)
	}
	if status.Status != runtimeStatusFailed || status.SinkErrors == 0 {
		t.Fatalf("flush sink failure status=%+v", status)
	}
	assertRuntimeNoFakeSecrets(t, err.Error())
	assertRuntimeNoFakeSecrets(t, status.FailureReason)
}

func TestRuntimeDefaultSinkIsMemory(t *testing.T) {
	cfg, err := parseRuntimeConfig([]string{
		"--enable-runtime",
		"--transport", "fake",
		"--market-id", "1.111",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse runtime config: %v", err)
	}
	if cfg.SinkType != runtimeSinkMemory {
		t.Fatalf("default sink=%q want memory", cfg.SinkType)
	}
}

func TestRuntimePostgresSinkPreflightRequiresDatabaseURLEnv(t *testing.T) {
	var out bytes.Buffer
	code := runCommand(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "postgres",
		"--market-id", "1.111",
	}, &out)
	if code != exitRuntimeValidation {
		t.Fatalf("postgres missing database-url-env exit=%d want validation output=%s", code, out.String())
	}
	if !strings.Contains(out.String(), "--database-url-env is required") {
		t.Fatalf("postgres missing database-url-env output=%s", out.String())
	}
}

func TestRuntimePostgresSinkMissingDatabaseURLValueFails(t *testing.T) {
	t.Setenv("RUNTIME_TEST_DATABASE_URL", "")
	var out bytes.Buffer
	code := runCommand(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--market-id", "1.111",
	}, &out)
	if code != exitRuntimeValidation {
		t.Fatalf("postgres missing db url exit=%d want validation output=%s", code, out.String())
	}
	if !strings.Contains(out.String(), "database URL env var") || strings.Contains(out.String(), "postgres://") {
		t.Fatalf("postgres missing db url output=%s", out.String())
	}
}

func TestRuntimePostgresSinkRejectsUnsupportedTransport(t *testing.T) {
	t.Setenv("RUNTIME_TEST_DATABASE_URL", "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable")
	var out bytes.Buffer
	code := runCommand(context.Background(), []string{
		"--enable-runtime",
		"--transport", "tls",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--market-id", "1.111",
	}, &out)
	if code != exitRuntimeValidation {
		t.Fatalf("postgres tls exit=%d want validation output=%s", code, out.String())
	}
	if !strings.Contains(out.String(), "--transport tls requires --real-runtime-pilot") {
		t.Fatalf("postgres tls output=%s", out.String())
	}
	if strings.Contains(out.String(), "runtime_pass") || strings.Contains(out.String(), "runtime_user:runtime_pass") {
		t.Fatalf("postgres tls output leaked database URL: %s", out.String())
	}
}

func TestRuntimePostgresSinkFakeRuntimeRecordsFlushStats(t *testing.T) {
	now := fixedRuntimeNow()
	saver := newFakeRuntimeBatchSaver()
	sink := newTestPostgresRuntimeSink(t, saver, "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable")
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:             true,
		TransportMode:       runtimeTransportFake,
		SinkType:            runtimeSinkPostgres,
		DatabaseEnabled:     true,
		DatabaseURLRedacted: sink.RuntimeSinkStats().DatabaseURLRedacted,
		MarketIDs:           []string{"1.111"},
		MaxMessages:         10,
		DurationLimit:       time.Second,
		Now:                 func() time.Time { return now },
		Sink:                sink,
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("postgres fake runtime: %v status=%+v", err, status)
	}
	if status.SinkType != runtimeSinkPostgres || !status.DatabaseEnabled {
		t.Fatalf("postgres sink status=%+v", status)
	}
	if status.RawMessagesInserted != 4 || status.RawMessagesFlushed != 4 {
		t.Fatalf("postgres raw stats=%+v", status)
	}
	if status.CandidatesInserted != 2 || status.CandidatesUnchanged != 0 || status.CandidatesSkipped != 0 || status.CandidatesFlushed != 2 {
		t.Fatalf("postgres candidate stats=%+v", status)
	}
	if status.DatabaseURLRedacted == "" || strings.Contains(status.DatabaseURLRedacted, "runtime_pass") || strings.Contains(status.DatabaseURLRedacted, "runtime_user") {
		t.Fatalf("database URL not redacted in status=%+v", status)
	}
	if saver.batchCount() == 0 {
		t.Fatal("postgres fake saver was not flushed")
	}
}

func TestRuntimePostgresSinkChangeOnlyStats(t *testing.T) {
	now := fixedRuntimeNow()
	saver := newFakeRuntimeBatchSaver()
	for i := 0; i < 2; i++ {
		sink := newTestPostgresRuntimeSink(t, saver, "")
		status, err := NewRuntimeSupervisor(RuntimeOptions{
			Enabled:         true,
			TransportMode:   runtimeTransportFake,
			SinkType:        runtimeSinkPostgres,
			DatabaseEnabled: true,
			MarketIDs:       []string{"1.111"},
			MaxMessages:     10,
			DurationLimit:   time.Second,
			Now:             func() time.Time { return now },
			Sink:            sink,
		}).Run(context.Background())
		if err != nil {
			t.Fatalf("postgres fake repeated run %d: %v status=%+v", i+1, err, status)
		}
		if i == 0 && status.CandidatesInserted != 2 {
			t.Fatalf("first repeated run stats=%+v", status)
		}
		if i == 1 && (status.CandidatesInserted != 0 || status.CandidatesUnchanged != 2) {
			t.Fatalf("second repeated run stats=%+v", status)
		}
	}
	if got := saver.rawInserted(); got != 8 {
		t.Fatalf("raw messages inserted=%d want append-only 8", got)
	}
}

func TestRuntimePostgresSinkFlushOnBatchSize(t *testing.T) {
	saver := newFakeRuntimeBatchSaver()
	sink := newTestPostgresRuntimeSink(t, saver, "")
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:             true,
		TransportMode:       runtimeTransportFake,
		SinkType:            runtimeSinkPostgres,
		DatabaseEnabled:     true,
		MarketIDs:           []string{"1.111"},
		MaxMessages:         10,
		DurationLimit:       time.Second,
		RawMessageBatchSize: 2,
		CandidateBatchSize:  2,
		Now:                 fixedRuntimeNowFunc(),
		Sink:                sink,
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("postgres batch flush: %v status=%+v", err, status)
	}
	if status.FlushCount < 3 || status.RawMessagesFlushed != 4 || status.CandidatesFlushed != 2 {
		t.Fatalf("postgres batch flush status=%+v", status)
	}
	if saver.batchCount() < 3 {
		t.Fatalf("postgres saver batches=%d want at least 3", saver.batchCount())
	}
}

func TestRuntimePostgresSinkFlushOnCancellation(t *testing.T) {
	conn := newBlockingRuntimeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
	)
	saver := newFakeRuntimeBatchSaver()
	sink := newTestPostgresRuntimeSink(t, saver, "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		status RuntimeStatus
		err    error
	}, 1)
	go func() {
		status, err := NewRuntimeSupervisor(RuntimeOptions{
			Enabled:             true,
			TransportMode:       runtimeTransportFake,
			SinkType:            runtimeSinkPostgres,
			DatabaseEnabled:     true,
			MarketIDs:           []string{"1.111"},
			DurationLimit:       time.Second,
			RawMessageBatchSize: 1,
			Sink:                sink,
			TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
				return &singleRuntimeTransport{conn: conn}
			},
		}).Run(ctx)
		done <- struct {
			status RuntimeStatus
			err    error
		}{status: status, err: err}
	}()
	conn.waitForWrites(t, 2)
	deadline := time.Now().Add(time.Second)
	for saver.rawInserted() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("postgres canceled runtime error=%v want context.Canceled status=%+v", result.err, result.status)
		}
		if result.status.RawMessagesInserted != 2 || result.status.FlushCount == 0 {
			t.Fatalf("postgres cancellation flush status=%+v", result.status)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for postgres cancellation flush")
	}
}

func TestRuntimePostgresSinkFlushErrorFailsRedacted(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	saver := newFakeRuntimeBatchSaver()
	saver.err = fmt.Errorf("save failed for %s %s %s", rawDatabaseURL, runtimeFakeAppKey, runtimeFakeSessionToken)
	sink := newTestPostgresRuntimeSink(t, saver, rawDatabaseURL)
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:             true,
		TransportMode:       runtimeTransportFake,
		SinkType:            runtimeSinkPostgres,
		DatabaseEnabled:     true,
		DatabaseURLRedacted: sink.RuntimeSinkStats().DatabaseURLRedacted,
		MarketIDs:           []string{"1.111"},
		MaxMessages:         10,
		DurationLimit:       time.Second,
		Now:                 fixedRuntimeNowFunc(),
		Sink:                sink,
	}).Run(context.Background())
	if err == nil {
		t.Fatalf("postgres flush error=nil status=%+v", status)
	}
	if status.Status != runtimeStatusFailed || status.FlushErrors == 0 || status.SinkErrors == 0 {
		t.Fatalf("postgres flush failure status=%+v", status)
	}
	for _, text := range []string{err.Error(), status.FailureReason, status.DatabaseURLRedacted} {
		assertRuntimeNoFakeSecrets(t, text)
		if strings.Contains(text, "runtime_pass") || strings.Contains(text, "runtime_user:runtime_pass") || strings.Contains(text, rawDatabaseURL) {
			t.Fatalf("postgres flush failure leaked database URL: %s", text)
		}
	}
}

func TestRuntimePostgresSinkReportRedactsDatabaseURL(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	redacted := redactRuntimeDatabaseURL(rawDatabaseURL)
	status := RuntimeStatus{
		Enabled:             true,
		Status:              runtimeStatusStopped,
		Readiness:           runtimeReadinessStopped,
		TransportMode:       runtimeTransportFake,
		SinkType:            runtimeSinkPostgres,
		DatabaseEnabled:     true,
		DatabaseURLRedacted: redacted,
	}
	var out bytes.Buffer
	if err := writeRuntimeStatus(&out, status, runtimeOutputJSON); err != nil {
		t.Fatalf("write postgres json status: %v", err)
	}
	text := out.String()
	if strings.Contains(text, "runtime_pass") || strings.Contains(text, "runtime_user:runtime_pass") || strings.Contains(text, rawDatabaseURL) {
		t.Fatalf("postgres json output leaked database URL: %s", text)
	}
	if !strings.Contains(text, "database_url_redacted") {
		t.Fatalf("postgres json output missing database_url_redacted: %s", text)
	}
}

func TestRuntimeCommandPostgresSinkUsesExplicitEnvAndFakeStore(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	saver := newFakeRuntimeBatchSaver()
	storeCalls := 0
	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		storeCalls++
		if databaseURL != rawDatabaseURL {
			t.Fatalf("databaseURL=%q want explicit env value", databaseURL)
		}
		return saver, nil
	}, func(RuntimeStreamGroup) betfair.LiveTransport {
		return newFakeRuntimeTransport(defaultFakeRuntimeMessages([]string{"1.111"}, fixedRuntimeNow()))
	})
	if code != exitRuntimeSuccess {
		t.Fatalf("postgres command exit=%d output=%s", code, out.String())
	}
	if storeCalls != 1 {
		t.Fatalf("store factory calls=%d want 1", storeCalls)
	}
	text := out.String()
	if !strings.Contains(text, `"sink_type":"postgres"`) || !strings.Contains(text, `"raw_messages_inserted":4`) || !strings.Contains(text, `"candidates_inserted":2`) {
		t.Fatalf("postgres command output=%s", text)
	}
	if strings.Contains(text, "runtime_pass") || strings.Contains(text, "runtime_user:runtime_pass") || strings.Contains(text, rawDatabaseURL) {
		t.Fatalf("postgres command leaked database URL: %s", text)
	}
}

func TestRuntimeCommandDisabledPostgresSinkDoesNotOpenStore(t *testing.T) {
	storeCalls := 0
	var out bytes.Buffer
	code := runCommandWithStoreFactory(context.Background(), []string{
		"--sink", "postgres",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		storeCalls++
		return newFakeRuntimeBatchSaver(), nil
	})
	if code != exitRuntimeSuccess {
		t.Fatalf("disabled postgres command exit=%d output=%s", code, out.String())
	}
	if storeCalls != 0 {
		t.Fatalf("disabled postgres command opened store %d times", storeCalls)
	}
	if !strings.Contains(out.String(), `"status":"disabled"`) || !strings.Contains(out.String(), `"sink_type":"postgres"`) {
		t.Fatalf("disabled postgres output=%s", out.String())
	}
}

func TestRuntimeCommandPostgresTelemetryStartsBeforeTransport(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	saver := newFakeRuntimeBatchSaver()
	saver.nextRunID = 202
	constructed := 0
	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--run-label", "phase10f_test",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		if databaseURL != rawDatabaseURL {
			t.Fatalf("databaseURL=%q want explicit env value", databaseURL)
		}
		return saver, nil
	}, func(RuntimeStreamGroup) betfair.LiveTransport {
		constructed++
		return newFakeRuntimeTransport(defaultFakeRuntimeMessages([]string{"1.111"}, fixedRuntimeNow()))
	})
	if code != exitRuntimeSuccess {
		t.Fatalf("postgres telemetry command exit=%d output=%s", code, out.String())
	}
	if saver.startCount() != 1 {
		t.Fatalf("telemetry starts=%d want 1", saver.startCount())
	}
	if constructed != 1 {
		t.Fatalf("transport constructed=%d want 1", constructed)
	}
	finishes := saver.telemetryFinishes()
	if len(finishes) != 1 {
		t.Fatalf("telemetry finishes=%+v want one", finishes)
	}
	if finishes[0].runID != 202 || finishes[0].input.StreamID != "runtime-202" || finishes[0].input.Status != model.BetfairRuntimeRunStatusSuccess {
		t.Fatalf("telemetry finish=%+v", finishes[0])
	}
	for _, batch := range saver.savedBatches() {
		for _, raw := range batch.raw {
			if raw.StreamID != "runtime-202" {
				t.Fatalf("raw stream_id=%q want runtime-202", raw.StreamID)
			}
		}
	}
	text := out.String()
	if !strings.Contains(text, `"run_id":202`) || !strings.Contains(text, `"run_label":"phase10f_test"`) {
		t.Fatalf("postgres telemetry output missing run metadata: %s", text)
	}
	if strings.Contains(text, rawDatabaseURL) || strings.Contains(text, "runtime_user:runtime_pass") {
		t.Fatalf("postgres telemetry output leaked database URL: %s", text)
	}
}

func TestRuntimeCommandPostgresTelemetryStartFailureAbortsBeforeTransport(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	saver := newFakeRuntimeBatchSaver()
	saver.telemetryStartErr = fmt.Errorf("start telemetry failed for %s", rawDatabaseURL)
	constructed := 0
	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		return saver, nil
	}, func(RuntimeStreamGroup) betfair.LiveTransport {
		constructed++
		return newFakeRuntimeTransport(defaultFakeRuntimeMessages([]string{"1.111"}, fixedRuntimeNow()))
	})
	if code != exitRuntimeRuntime {
		t.Fatalf("telemetry start failure exit=%d output=%s", code, out.String())
	}
	if constructed != 0 {
		t.Fatalf("transport constructed after telemetry start failure: %d", constructed)
	}
	if saver.finishCount() != 0 {
		t.Fatalf("telemetry finishes=%d want 0", saver.finishCount())
	}
	text := out.String()
	if !strings.Contains(text, `"status":"failed"`) || !strings.Contains(text, "[REDACTED_RUNTIME_SECRET]") {
		t.Fatalf("telemetry start failure output=%s", text)
	}
	if strings.Contains(text, rawDatabaseURL) || strings.Contains(text, "runtime_user:runtime_pass") {
		t.Fatalf("telemetry start failure leaked database URL: %s", text)
	}
}

func TestRuntimeCommandPostgresTelemetryFinalizesRuntimeFailure(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	saver := newFakeRuntimeBatchSaver()
	saver.nextRunID = 303
	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		return saver, nil
	}, func(RuntimeStreamGroup) betfair.LiveTransport {
		return &fakeRuntimeTransport{connectErrs: []error{fmt.Errorf("connect failed with %s and %s", runtimeFakeAppKey, runtimeFakeSessionToken)}}
	})
	if code != exitRuntimeRuntime {
		t.Fatalf("runtime failure exit=%d output=%s", code, out.String())
	}
	finishes := saver.telemetryFinishes()
	if len(finishes) != 1 {
		t.Fatalf("telemetry finishes=%+v want one", finishes)
	}
	if finishes[0].runID != 303 || finishes[0].input.Status != model.BetfairRuntimeRunStatusFailed {
		t.Fatalf("runtime failure telemetry finish=%+v", finishes[0])
	}
	assertRuntimeNoFakeSecrets(t, finishes[0].input.FailureReason)
	assertRuntimeNoFakeSecrets(t, out.String())
}

func TestRuntimeCommandPostgresTelemetryFinalizesCancellation(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	saver := newFakeRuntimeBatchSaver()
	saver.nextRunID = 404
	conn := newBlockingRuntimeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"runtime-initial"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"runtime-subscription"}`,
		`{"op":"heartbeat","id":3,"clk":"runtime-heartbeat"}`,
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	var out bytes.Buffer
	go func() {
		done <- runCommandWithFactories(ctx, []string{
			"--enable-runtime",
			"--transport", "fake",
			"--sink", "postgres",
			"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
			"--market-id", "1.111",
			"--max-messages", "20",
			"--duration", "30s",
			"--output", "json",
		}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
			return saver, nil
		}, func(RuntimeStreamGroup) betfair.LiveTransport {
			return &singleRuntimeTransport{conn: conn}
		})
	}()
	conn.waitForWrites(t, 2)
	cancel()
	select {
	case code := <-done:
		if code != exitRuntimeCanceled {
			t.Fatalf("canceled runtime exit=%d output=%s", code, out.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled runtime command")
	}
	finishes := saver.telemetryFinishes()
	if len(finishes) != 1 {
		t.Fatalf("telemetry finishes=%+v want one", finishes)
	}
	if finishes[0].runID != 404 || finishes[0].input.Status != model.BetfairRuntimeRunStatusCanceled {
		t.Fatalf("canceled telemetry finish=%+v", finishes[0])
	}
}

func TestRuntimeCommandPostgresTelemetryFinalizesSinkError(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	saver := newFakeRuntimeBatchSaver()
	saver.nextRunID = 505
	saver.err = fmt.Errorf("fake store sink failure with %s", rawDatabaseURL)
	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		return saver, nil
	}, func(RuntimeStreamGroup) betfair.LiveTransport {
		return newFakeRuntimeTransport(defaultFakeRuntimeMessages([]string{"1.111"}, fixedRuntimeNow()))
	})
	if code != exitRuntimeRuntime {
		t.Fatalf("sink error exit=%d output=%s", code, out.String())
	}
	finishes := saver.telemetryFinishes()
	if len(finishes) != 1 {
		t.Fatalf("telemetry finishes=%+v want one", finishes)
	}
	if finishes[0].runID != 505 || finishes[0].input.Status != model.BetfairRuntimeRunStatusFailed || finishes[0].input.SinkErrors == 0 {
		t.Fatalf("sink error telemetry finish=%+v", finishes[0])
	}
	text := out.String()
	if strings.Contains(text, rawDatabaseURL) || strings.Contains(text, "runtime_user:runtime_pass") {
		t.Fatalf("sink error output leaked database URL: %s", text)
	}
}

func TestRuntimeCommandPostflightOnlyReadsTelemetryWithoutTransport(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	saver := newFakeRuntimeBatchSaver()
	finishedAt := fixedRuntimeNow().Add(2 * time.Second)
	elapsed := int64(2000)
	saver.postflight = model.BetfairRuntimePostflight{
		Run: model.BetfairRuntimeRun{
			ID:                      707,
			RunLabel:                "postflight_test",
			StreamID:                "runtime-707",
			RuntimeMode:             runtimeModeFake,
			TransportMode:           runtimeTransportFake,
			SinkType:                runtimeSinkPostgres,
			StartedAt:               fixedRuntimeNow(),
			FinishedAt:              &finishedAt,
			Status:                  model.BetfairRuntimeRunStatusSuccess,
			ReadinessState:          runtimeReadinessStopped,
			MessagesRead:            4,
			RawMessagesSeen:         4,
			RawMessagesFlushed:      4,
			RawMessagesInserted:     4,
			CandidatesEmitted:       2,
			CandidateBatchesSeen:    1,
			CandidateBatchesFlushed: 1,
			CandidatesInserted:      2,
			MarketsSeen:             1,
			ElapsedMS:               &elapsed,
			LastMessageTime:         &finishedAt,
			LastCandidateTime:       &finishedAt,
			LastSinkFlushTime:       &finishedAt,
		},
		LinkedRawMessageCount: 4,
		QuoteHistoryRows:      2,
		LatestQuoteCount:      2,
		MappedQuoteCount:      1,
		ReadyRunnerBookCount:  1,
		ReadyComparisonCount:  1,
		Warnings:              []string{"runtime_no_ready_rows"},
	}
	constructed := 0
	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--postflight-only",
		"--run-id", "707",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		if databaseURL != rawDatabaseURL {
			t.Fatalf("databaseURL=%q want explicit env value", databaseURL)
		}
		return saver, nil
	}, func(RuntimeStreamGroup) betfair.LiveTransport {
		constructed++
		return newFakeRuntimeTransport(defaultFakeRuntimeMessages([]string{"1.111"}, fixedRuntimeNow()))
	})
	if code != exitRuntimeSuccess {
		t.Fatalf("postflight-only exit=%d output=%s", code, out.String())
	}
	if constructed != 0 {
		t.Fatalf("postflight constructed transport %d times", constructed)
	}
	text := out.String()
	if !strings.Contains(text, `"postflight_only":true`) || !strings.Contains(text, `"run_id":707`) || !strings.Contains(text, `"linked_raw_message_count":4`) {
		t.Fatalf("postflight output=%s", text)
	}
	if strings.Contains(text, rawDatabaseURL) || strings.Contains(text, "runtime_user:runtime_pass") {
		t.Fatalf("postflight output leaked database URL: %s", text)
	}
}

func TestRuntimePostflightClassificationPassWarningFail(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	finishedAt := fixedRuntimeNow().Add(2 * time.Second)
	elapsed := int64(2000)
	basePostflight := func() model.BetfairRuntimePostflight {
		return model.BetfairRuntimePostflight{
			Run: model.BetfairRuntimeRun{
				ID:                      808,
				RunLabel:                "real_pilot_postflight",
				StreamID:                "runtime-808",
				RuntimeMode:             runtimeModeRealPilot,
				TransportMode:           runtimeTransportTLS,
				SinkType:                runtimeSinkPostgres,
				StartedAt:               fixedRuntimeNow(),
				FinishedAt:              &finishedAt,
				Status:                  model.BetfairRuntimeRunStatusSuccess,
				ReadinessState:          runtimeReadinessStopped,
				MessagesRead:            8,
				RawMessagesSeen:         8,
				RawMessagesFlushed:      8,
				RawMessagesInserted:     8,
				CandidatesEmitted:       4,
				CandidateBatchesSeen:    2,
				CandidateBatchesFlushed: 2,
				CandidatesInserted:      2,
				CandidatesUnchanged:     2,
				MarketsSeen:             1,
				HeartbeatState:          string(betfair.StreamStateHealthy),
				ElapsedMS:               &elapsed,
				LastMessageTime:         &finishedAt,
				LastCandidateTime:       &finishedAt,
				LastSinkFlushTime:       &finishedAt,
			},
			LinkedRawMessageCount: 8,
			QuoteHistoryRows:      2,
			LatestQuoteCount:      2,
			MappedQuoteCount:      1,
			ReadyRunnerBookCount:  1,
			ReadyComparisonCount:  1,
		}
	}
	tests := []struct {
		name           string
		mutate         func(*model.BetfairRuntimePostflight)
		wantClass      string
		wantReason     string
		wantNextAction string
	}{
		{
			name:           "pass",
			wantClass:      runtimePostflightPass,
			wantNextAction: "operator_may_review_next_gate",
		},
		{
			name: "warning",
			mutate: func(postflight *model.BetfairRuntimePostflight) {
				postflight.LatestQuoteCount = 0
				postflight.MappedQuoteCount = 0
				postflight.ReadyRunnerBookCount = 0
				postflight.ReadyComparisonCount = 0
			},
			wantClass:      runtimePostflightWarning,
			wantReason:     "no_mapped_rows",
			wantNextAction: "operator_review_required_before_repeat",
		},
		{
			name: "fail",
			mutate: func(postflight *model.BetfairRuntimePostflight) {
				postflight.Run.Status = model.BetfairRuntimeRunStatusFailed
				postflight.Run.ReadinessState = runtimeReadinessFailed
				postflight.Run.FailureReason = "redacted runtime failure"
				postflight.Run.MessagesRead = 0
				postflight.Run.RawMessagesInserted = 0
				postflight.Run.CandidatesEmitted = 0
				postflight.Run.CandidatesInserted = 0
				postflight.Run.CandidatesUnchanged = 0
				postflight.Run.SinkErrors = 1
				postflight.Run.FlushErrors = 1
				postflight.Run.HeartbeatState = string(betfair.StreamStateStale)
			},
			wantClass:      runtimePostflightFail,
			wantReason:     "status_failed",
			wantNextAction: "stop_and_rollback_before_repeat",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			saver := newFakeRuntimeBatchSaver()
			postflight := basePostflight()
			if tc.mutate != nil {
				tc.mutate(&postflight)
			}
			saver.postflight = postflight
			constructed := 0
			var out bytes.Buffer
			code := runCommandWithFactories(context.Background(), []string{
				"--postflight-only",
				"--postflight-checklist",
				"--run-id", "808",
				"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
				"--output", "json",
			}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
				if databaseURL != rawDatabaseURL {
					t.Fatalf("databaseURL=%q want explicit env value", databaseURL)
				}
				return saver, nil
			}, func(RuntimeStreamGroup) betfair.LiveTransport {
				constructed++
				return newFakeRuntimeTransport(defaultFakeRuntimeMessages([]string{"1.111"}, fixedRuntimeNow()))
			})
			if code != exitRuntimeSuccess {
				t.Fatalf("postflight classification exit=%d output=%s", code, out.String())
			}
			if constructed != 0 {
				t.Fatalf("postflight constructed transport %d times", constructed)
			}
			var status RuntimeStatus
			if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &status); err != nil {
				t.Fatalf("decode postflight status %s: %v", out.String(), err)
			}
			if status.PostflightClassification != tc.wantClass {
				t.Fatalf("classification=%q want %q status=%+v", status.PostflightClassification, tc.wantClass, status)
			}
			if !status.RealRuntimePilot {
				t.Fatalf("postflight did not identify real runtime pilot: %+v", status)
			}
			if status.PostflightChecklist == nil || status.PostflightChecklist.NextAllowedAction != tc.wantNextAction {
				t.Fatalf("checklist=%+v want next action %q", status.PostflightChecklist, tc.wantNextAction)
			}
			if tc.wantReason != "" && !containsRuntimeReason(append(status.PostflightFailReasons, status.PostflightWarningReasons...), tc.wantReason) {
				t.Fatalf("reasons fail=%+v warning=%+v want %q", status.PostflightFailReasons, status.PostflightWarningReasons, tc.wantReason)
			}
			text := out.String()
			if strings.Contains(text, rawDatabaseURL) || strings.Contains(text, "runtime_user:runtime_pass") {
				t.Fatalf("postflight output leaked database URL: %s", text)
			}
		})
	}
}

func TestRuntimePostflightStoreErrorClassifiesFailAndRedacts(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	saver := newFakeRuntimeBatchSaver()
	saver.postflightErr = fmt.Errorf("missing runtime run for %s", rawDatabaseURL)
	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--postflight-only",
		"--postflight-checklist",
		"--run-id", "909",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		return saver, nil
	}, func(RuntimeStreamGroup) betfair.LiveTransport {
		t.Fatal("postflight constructed transport")
		return nil
	})
	if code != exitRuntimeRuntime {
		t.Fatalf("postflight error exit=%d output=%s", code, out.String())
	}
	var status RuntimeStatus
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &status); err != nil {
		t.Fatalf("decode postflight error status %s: %v", out.String(), err)
	}
	if status.PostflightClassification != runtimePostflightFail || !containsRuntimeReason(status.PostflightFailReasons, "runtime_run_not_found_or_unreadable") {
		t.Fatalf("postflight error status=%+v", status)
	}
	text := out.String()
	if strings.Contains(text, rawDatabaseURL) || strings.Contains(text, "runtime_user:runtime_pass") {
		t.Fatalf("postflight error leaked database URL: %s", text)
	}
}

func TestRuntimePostflightChecklistRequiresPostflightOnly(t *testing.T) {
	var out bytes.Buffer
	code := runCommand(context.Background(), []string{
		"--postflight-checklist",
		"--output", "json",
	}, &out)
	if code != exitRuntimeValidation {
		t.Fatalf("postflight checklist validation exit=%d output=%s", code, out.String())
	}
	if !strings.Contains(out.String(), "--postflight-checklist requires --postflight-only") {
		t.Fatalf("postflight checklist validation output=%s", out.String())
	}
}

func TestRuntimeRealPilotSoakRemainsDeferred(t *testing.T) {
	t.Setenv("RUNTIME_TEST_APP_KEY", "secret-app-key")
	t.Setenv("RUNTIME_TEST_SESSION_TOKEN", "secret-session-token")
	var out bytes.Buffer
	code := runCommand(context.Background(), []string{
		"--enable-runtime",
		"--real-runtime-pilot",
		"--transport", "tls",
		"--sink", "memory",
		"--address", "127.0.0.1:443",
		"--server-name", "localhost",
		"--app-key-env", "RUNTIME_TEST_APP_KEY",
		"--session-token-env", "RUNTIME_TEST_SESSION_TOKEN",
		"--market-id", "1.111",
		"--soak-runs", "2",
		"--output", "json",
	}, &out)
	if code != exitRuntimeValidation {
		t.Fatalf("real pilot soak validation exit=%d output=%s", code, out.String())
	}
	text := out.String()
	if !strings.Contains(text, "soak mode supports fake transport only") {
		t.Fatalf("real pilot soak validation output=%s", text)
	}
	if strings.Contains(text, "secret-app-key") || strings.Contains(text, "secret-session-token") {
		t.Fatalf("real pilot soak validation leaked secret: %s", text)
	}
}

func TestRuntimeRunLabelValidation(t *testing.T) {
	var out bytes.Buffer
	code := runCommand(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "memory",
		"--run-label", "bad label",
		"--market-id", "1.111",
		"--output", "json",
	}, &out)
	if code != exitRuntimeValidation {
		t.Fatalf("invalid run label exit=%d output=%s", code, out.String())
	}
	if !strings.Contains(out.String(), "run-label may contain only") {
		t.Fatalf("invalid run label output=%s", out.String())
	}
	out.Reset()
	code = runCommand(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "memory",
		"--run-label", "phase10f.ok-1",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out)
	if code != exitRuntimeSuccess {
		t.Fatalf("valid run label exit=%d output=%s", code, out.String())
	}
	if !strings.Contains(out.String(), `"run_label":"phase10f.ok-1"`) {
		t.Fatalf("valid run label output=%s", out.String())
	}
}

func TestRuntimeSoakFlagValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "too many runs",
			args: []string{"--enable-runtime", "--transport", "fake", "--sink", "memory", "--soak-runs", "11", "--market-id", "1.111", "--output", "json"},
			want: "soak-runs must be 10 or fewer",
		},
		{
			name: "delay too large",
			args: []string{"--enable-runtime", "--transport", "fake", "--sink", "memory", "--soak-runs", "2", "--soak-delay", "31s", "--market-id", "1.111", "--output", "json"},
			want: "soak-delay must be 30s or less",
		},
		{
			name: "requires enable",
			args: []string{"--transport", "fake", "--sink", "memory", "--soak-runs", "2", "--market-id", "1.111", "--output", "json"},
			want: "soak mode requires --enable-runtime",
		},
		{
			name: "requires fake",
			args: []string{"--enable-runtime", "--transport", "tls", "--sink", "memory", "--soak-runs", "2", "--market-id", "1.111", "--output", "json"},
			want: "soak mode supports fake transport only",
		},
		{
			name: "label prefix safe chars",
			args: []string{"--enable-runtime", "--transport", "fake", "--sink", "memory", "--soak-runs", "2", "--soak-run-label-prefix", "bad label", "--market-id", "1.111", "--output", "json"},
			want: "soak-run-label-prefix invalid",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			code := runCommand(context.Background(), tc.args, &out)
			if code != exitRuntimeValidation {
				t.Fatalf("soak validation exit=%d output=%s", code, out.String())
			}
			if !strings.Contains(out.String(), tc.want) {
				t.Fatalf("soak validation output=%s want %q", out.String(), tc.want)
			}
		})
	}
}

func TestRuntimeSoakMemorySinkDoesNotRequireDB(t *testing.T) {
	storeCalls := 0
	var out bytes.Buffer
	code := runCommandWithStoreFactory(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "memory",
		"--soak-runs", "2",
		"--soak-run-label-prefix", "memory_soak",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		storeCalls++
		return newFakeRuntimeBatchSaver(), nil
	})
	if code != exitRuntimeSuccess {
		t.Fatalf("memory soak exit=%d output=%s", code, out.String())
	}
	if storeCalls != 0 {
		t.Fatalf("memory soak opened store %d times", storeCalls)
	}
	var report RuntimeSoakReport
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &report); err != nil {
		t.Fatalf("decode memory soak report %s: %v", out.String(), err)
	}
	if report.SoakRunsRequested != 2 || report.SoakRunsCompleted != 2 || report.SoakRunsFailed != 0 {
		t.Fatalf("memory soak report=%+v", report)
	}
	if len(report.RuntimeRunIDs) != 0 || report.TotalCandidatesEmitted != 4 {
		t.Fatalf("memory soak report=%+v", report)
	}
	assertRuntimeNoFakeSecrets(t, out.String())
}

func TestRuntimeSoakPostgresSinkCreatesTelemetryRuns(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	saver := newFakeRuntimeBatchSaver()
	saver.nextRunID = 801
	var out bytes.Buffer
	storeCalls := 0
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--soak-runs", "3",
		"--soak-run-label-prefix", "phase10g_soak",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		storeCalls++
		if databaseURL != rawDatabaseURL {
			t.Fatalf("databaseURL=%q want explicit env value", databaseURL)
		}
		return saver, nil
	}, func(group RuntimeStreamGroup) betfair.LiveTransport {
		return newFakeRuntimeTransport(defaultFakeRuntimeMessages(group.MarketIDs, fixedRuntimeNow()))
	})
	if code != exitRuntimeSuccess {
		t.Fatalf("postgres soak exit=%d output=%s", code, out.String())
	}
	if storeCalls != 4 {
		t.Fatalf("postgres soak store calls=%d want 4", storeCalls)
	}
	if saver.startCount() != 3 || saver.finishCount() != 3 {
		t.Fatalf("postgres soak starts/finishes=%d/%d", saver.startCount(), saver.finishCount())
	}
	var report RuntimeSoakReport
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &report); err != nil {
		t.Fatalf("decode postgres soak report %s: %v", out.String(), err)
	}
	if report.SoakRunsRequested != 3 || report.SoakRunsCompleted != 3 || report.SoakRunsFailed != 0 {
		t.Fatalf("postgres soak report=%+v", report)
	}
	if !reflect.DeepEqual(report.RuntimeRunIDs, []int64{801, 802, 803}) {
		t.Fatalf("runtime run ids=%+v", report.RuntimeRunIDs)
	}
	if report.TotalRawMessagesInserted != 12 || report.TotalCandidatesInserted != 2 || report.TotalCandidatesUnchanged != 4 {
		t.Fatalf("postgres soak totals=%+v", report)
	}
	starts := saver.telemetryStarts()
	if got := []string{starts[0].RunLabel, starts[1].RunLabel, starts[2].RunLabel}; !reflect.DeepEqual(got, []string{"phase10g_soak-001", "phase10g_soak-002", "phase10g_soak-003"}) {
		t.Fatalf("soak run labels=%+v", got)
	}
	text := out.String()
	if strings.Contains(text, rawDatabaseURL) || strings.Contains(text, "runtime_user:runtime_pass") {
		t.Fatalf("postgres soak leaked database URL: %s", text)
	}
}

func TestRuntimeSoakFailedRunIsAggregated(t *testing.T) {
	rawDatabaseURL := "postgres://runtime_user:runtime_pass@localhost:5432/runtime_db?sslmode=disable"
	t.Setenv("RUNTIME_TEST_DATABASE_URL", rawDatabaseURL)
	saver := newFakeRuntimeBatchSaver()
	saver.nextRunID = 901
	runNumber := 0
	var out bytes.Buffer
	code := runCommandWithFactories(context.Background(), []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--soak-runs", "3",
		"--soak-run-label-prefix", "phase10g_fail",
		"--market-id", "1.111",
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, func(ctx context.Context, databaseURL string) (runtimeStore, error) {
		return saver, nil
	}, func(RuntimeStreamGroup) betfair.LiveTransport {
		runNumber++
		if runNumber == 2 {
			return &fakeRuntimeTransport{connectErrs: []error{fmt.Errorf("connect failed with %s", runtimeFakeSessionToken)}}
		}
		return newFakeRuntimeTransport(defaultFakeRuntimeMessages([]string{"1.111"}, fixedRuntimeNow()))
	})
	if code != exitRuntimeRuntime {
		t.Fatalf("failed soak exit=%d output=%s", code, out.String())
	}
	var report RuntimeSoakReport
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &report); err != nil {
		t.Fatalf("decode failed soak report %s: %v", out.String(), err)
	}
	if report.SoakRunsRequested != 3 || report.SoakRunsCompleted != 2 || report.SoakRunsFailed != 1 {
		t.Fatalf("failed soak report=%+v", report)
	}
	if !reflect.DeepEqual(report.RuntimeRunIDs, []int64{901, 902, 903}) || !reflect.DeepEqual(report.FailedRunIDs, []int64{902}) {
		t.Fatalf("failed soak run ids report=%+v", report)
	}
	text := out.String()
	assertRuntimeNoFakeSecrets(t, text)
	if !strings.Contains(text, "[REDACTED_BETFAIR_SESSION_TOKEN]") {
		t.Fatalf("failed soak output missing redaction marker: %s", text)
	}
}

func TestRuntimeOutputJSONAndTextAreRedacted(t *testing.T) {
	status := RuntimeStatus{
		Enabled:                 true,
		Status:                  runtimeStatusStopped,
		Readiness:               runtimeReadinessStopped,
		TransportMode:           runtimeTransportFake,
		SinkType:                runtimeSinkMemory,
		MarketIDsCount:          1,
		LastClock:               "clk",
		LastInitialClock:        "initial",
		HeartbeatState:          string(betfair.StreamStateHealthy),
		StoppedReason:           betfair.LiveStopReasonEOF,
		StartedAt:               "2026-05-07T00:00:00Z",
		StoppedAt:               "2026-05-07T00:00:01Z",
		ElapsedMS:               1000,
		RawMessagesSeen:         4,
		MessagesRead:            4,
		CandidatesEmitted:       2,
		CandidateBatchesSeen:    1,
		CandidateBatchesFlushed: 1,
		LatestCandidateCount:    2,
		LastCandidateTime:       "2026-05-07T00:00:00Z",
		LastSinkFlushTime:       "2026-05-07T00:00:01Z",
	}
	var text bytes.Buffer
	if err := writeRuntimeStatus(&text, status, runtimeOutputText); err != nil {
		t.Fatalf("write text status: %v", err)
	}
	textOut := text.String()
	if !strings.Contains(textOut, "betfair_runtime enabled=true") || !strings.Contains(textOut, "sink_type=memory") || !strings.Contains(textOut, "candidate_batches_flushed=1") || strings.Contains(textOut, `"op"`) || strings.Contains(textOut, runtimeFakeAppKey) || strings.Contains(textOut, runtimeFakeSessionToken) {
		t.Fatalf("text output=%s", textOut)
	}
	var jsonOut bytes.Buffer
	if err := writeRuntimeStatus(&jsonOut, status, runtimeOutputJSON); err != nil {
		t.Fatalf("write json status: %v", err)
	}
	var decoded RuntimeStatus
	if err := json.Unmarshal(bytes.TrimSpace(jsonOut.Bytes()), &decoded); err != nil {
		t.Fatalf("decode json output %s: %v", jsonOut.String(), err)
	}
	expected := status
	finalizeRuntimeStatusFields(&expected)
	if !reflect.DeepEqual(decoded, expected) {
		t.Fatalf("json decoded=%+v want %+v", decoded, expected)
	}
}

func TestRuntimeFailureRedactsFakeSecrets(t *testing.T) {
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:       true,
		TransportMode: runtimeTransportFake,
		MarketIDs:     []string{"1.111"},
		TransportFactory: func(RuntimeStreamGroup) betfair.LiveTransport {
			return &fakeRuntimeTransport{connectErrs: []error{fmt.Errorf("connect failed with %s and %s", runtimeFakeAppKey, runtimeFakeSessionToken)}}
		},
	}).Run(context.Background())
	if err == nil {
		t.Fatal("expected connect failure")
	}
	if strings.Contains(status.FailureReason, runtimeFakeAppKey) || strings.Contains(status.FailureReason, runtimeFakeSessionToken) {
		t.Fatalf("failure reason leaked fake secrets: %+v", status)
	}
	if !strings.Contains(status.FailureReason, "[REDACTED_BETFAIR_APP_KEY]") || !strings.Contains(status.FailureReason, "[REDACTED_BETFAIR_SESSION_TOKEN]") {
		t.Fatalf("failure reason missing redaction markers: %+v", status)
	}
}

func TestRuntimeHasNoConfigOrBetfairCredentialEnvDependency(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	text := string(source)
	for _, forbidden := range []string{
		"internal/config",
		"BETFAIR_APP",
		"BETFAIR_SESSION",
		"BETFAIR_RUNTIME",
		"BETFAIR_DATABASE",
		`"DATABASE_URL"`,
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("runtime should not depend on %s", forbidden)
		}
	}
}

func TestRuntimeOptionalDBIntegrationPostgresSinkFakeStream(t *testing.T) {
	if strings.TrimSpace(os.Getenv("RUN_DB_TESTS")) != "1" {
		t.Skip("skipping DB integration test; set RUN_DB_TESTS=1 and TEST_DATABASE_URL to run")
	}
	dsn := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("skipping DB integration test; TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := store.Open(ctx, dsn, true, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open postgres store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres sql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	streamID := fmt.Sprintf("runtime-db-%d", time.Now().UnixNano())
	sink, err := NewPostgresRuntimeSink(PostgresRuntimeSinkOptions{
		Saver:    st,
		StreamID: streamID,
		Now:      fixedRuntimeNowFunc(),
	})
	if err != nil {
		t.Fatalf("new postgres sink: %v", err)
	}
	now := fixedRuntimeNow()
	marketID := fmt.Sprintf("1.%d", time.Now().UnixNano()%1000000000)
	status, err := NewRuntimeSupervisor(RuntimeOptions{
		Enabled:         true,
		TransportMode:   runtimeTransportFake,
		SinkType:        runtimeSinkPostgres,
		DatabaseEnabled: true,
		MarketIDs:       []string{marketID},
		MaxMessages:     10,
		DurationLimit:   time.Second,
		Now:             func() time.Time { return now },
		Sink:            sink,
	}).Run(ctx)
	if err != nil {
		t.Fatalf("runtime postgres DB integration: %v status=%+v", err, status)
	}
	if status.RawMessagesInserted != 4 || status.CandidatesInserted != 2 {
		t.Fatalf("runtime postgres DB integration status=%+v", status)
	}
	if got := scalarRuntimeDBInt(ctx, t, db, `SELECT count(*) FROM betfair_raw_stream_messages WHERE stream_id = $1`, streamID); got != 4 {
		t.Fatalf("raw stream rows=%d want 4", got)
	}
	if got := scalarRuntimeDBInt(ctx, t, db, `SELECT count(*) FROM betfair_latest_exchange_quotes_view WHERE market_id = $1`, marketID); got < 2 {
		t.Fatalf("latest quote rows=%d want at least 2", got)
	}
}

func TestRuntimeOptionalDBIntegrationPostgresSoakFakeStream(t *testing.T) {
	if strings.TrimSpace(os.Getenv("RUN_DB_TESTS")) != "1" {
		t.Skip("skipping DB integration test; set RUN_DB_TESTS=1 and TEST_DATABASE_URL to run")
	}
	dsn := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("skipping DB integration test; TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres sql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	st, err := store.Open(ctx, dsn, true, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open postgres store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	marketID := fmt.Sprintf("1.%d", time.Now().UnixNano()%1000000000)
	labelPrefix := fmt.Sprintf("phase10g_db_%d", time.Now().UnixNano())
	t.Setenv("RUNTIME_TEST_DATABASE_URL", dsn)
	var out bytes.Buffer
	code := runCommandWithFactories(ctx, []string{
		"--enable-runtime",
		"--transport", "fake",
		"--sink", "postgres",
		"--database-url-env", "RUNTIME_TEST_DATABASE_URL",
		"--soak-runs", "3",
		"--soak-run-label-prefix", labelPrefix,
		"--market-id", marketID,
		"--max-messages", "10",
		"--duration", "1s",
		"--output", "json",
	}, &out, nil, func(group RuntimeStreamGroup) betfair.LiveTransport {
		return newFakeRuntimeTransport(defaultFakeRuntimeMessages(group.MarketIDs, fixedRuntimeNow()))
	})
	if code != exitRuntimeSuccess {
		t.Fatalf("postgres soak DB integration exit=%d output=%s", code, out.String())
	}
	var report RuntimeSoakReport
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &report); err != nil {
		t.Fatalf("decode postgres soak DB report %s: %v", out.String(), err)
	}
	if report.SoakRunsRequested != 3 || report.SoakRunsCompleted != 3 || report.SoakRunsFailed != 0 {
		t.Fatalf("postgres soak DB report=%+v", report)
	}
	if len(report.RuntimeRunIDs) != 3 || report.TotalRawMessagesInserted != 12 || report.TotalCandidatesInserted != 2 || report.TotalCandidatesUnchanged != 4 {
		t.Fatalf("postgres soak DB totals=%+v", report)
	}
	if got := scalarRuntimeDBInt(ctx, t, db, `SELECT count(*) FROM betfair_runtime_runs WHERE run_label LIKE $1`, labelPrefix+"-%"); got != 3 {
		t.Fatalf("runtime soak telemetry rows=%d want 3", got)
	}
	for _, runID := range report.RuntimeRunIDs {
		streamID := runtimeStreamID(runID)
		if got := scalarRuntimeDBInt(ctx, t, db, `SELECT count(*) FROM betfair_raw_stream_messages WHERE stream_id = $1`, streamID); got != 4 {
			t.Fatalf("raw stream rows for %s=%d want 4", streamID, got)
		}
		postflight, err := st.GetBetfairRuntimePostflight(ctx, runID)
		if err != nil {
			t.Fatalf("get soak postflight run %d: %v", runID, err)
		}
		if postflight.LinkedRawMessageCount != 4 || postflight.LatestQuoteCount < 2 {
			t.Fatalf("soak postflight run %d=%+v", runID, postflight)
		}
	}
	if got := scalarRuntimeDBInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history WHERE market_id = $1`, marketID); got != 2 {
		t.Fatalf("quote history rows for %s=%d want 2", marketID, got)
	}
	if got := scalarRuntimeDBInt(ctx, t, db, `SELECT count(*) FROM betfair_latest_exchange_quotes_view WHERE market_id = $1`, marketID); got < 2 {
		t.Fatalf("latest quote rows for %s=%d want at least 2", marketID, got)
	}
}

func fixedRuntimeNow() time.Time {
	return time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC)
}

func fixedRuntimeNowFunc() func() time.Time {
	now := fixedRuntimeNow()
	return func() time.Time { return now }
}

func requireRuntimeCandidate(t *testing.T, candidates []betfair.QuoteCandidate, marketID string, side betfair.ExchangeSide) betfair.QuoteCandidate {
	t.Helper()
	for _, candidate := range candidates {
		if candidate.MarketID == marketID && candidate.ExchangeSide == side {
			return candidate
		}
	}
	t.Fatalf("candidate market=%s side=%s not found in %+v", marketID, side, candidates)
	return betfair.QuoteCandidate{}
}

func newTestPostgresRuntimeSink(t *testing.T, saver *fakeRuntimeBatchSaver, databaseURL string) *PostgresRuntimeSink {
	t.Helper()
	sink, err := NewPostgresRuntimeSink(PostgresRuntimeSinkOptions{
		Saver:               saver,
		StreamID:            "fake-group-1",
		Now:                 fixedRuntimeNowFunc(),
		DatabaseURL:         databaseURL,
		DatabaseURLRedacted: redactRuntimeDatabaseURL(databaseURL),
	})
	if err != nil {
		t.Fatalf("new postgres runtime sink: %v", err)
	}
	return sink
}

type fakeRuntimeBatchSaver struct {
	mu                 sync.Mutex
	err                error
	telemetryStartErr  error
	telemetryFinishErr error
	postflightErr      error
	nextRunID          int64
	starts             []model.BetfairRuntimeRunStart
	finishes           []fakeRuntimeTelemetryFinish
	postflight         model.BetfairRuntimePostflight
	batches            []fakeRuntimeSavedBatch
	latest             map[string]string
}

type fakeRuntimeSavedBatch struct {
	raw        []model.BetfairRawStreamMessage
	candidates []model.BetfairExchangeQuoteCandidate
	stats      model.BetfairSaveStats
}

type fakeRuntimeTelemetryFinish struct {
	runID int64
	input model.BetfairRuntimeRunFinish
}

func newFakeRuntimeBatchSaver() *fakeRuntimeBatchSaver {
	return &fakeRuntimeBatchSaver{nextRunID: 101, latest: map[string]string{}}
}

func (s *fakeRuntimeBatchSaver) SaveBetfairBatch(ctx context.Context, raw []model.BetfairRawStreamMessage, candidates []model.BetfairExchangeQuoteCandidate) (model.BetfairSaveStats, error) {
	if err := ctx.Err(); err != nil {
		return model.BetfairSaveStats{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return model.BetfairSaveStats{}, s.err
	}
	stats := model.BetfairSaveStats{RawMessagesInserted: len(raw)}
	for _, candidate := range candidates {
		stats.CandidatesSeen++
		if strings.TrimSpace(candidate.QuoteKey) == "" || strings.TrimSpace(candidate.QuoteHash) == "" {
			stats.CandidatesSkipped++
			continue
		}
		if s.latest[candidate.QuoteKey] == candidate.QuoteHash {
			stats.CandidatesUnchanged++
			continue
		}
		s.latest[candidate.QuoteKey] = candidate.QuoteHash
		stats.CandidatesInserted++
	}
	s.batches = append(s.batches, fakeRuntimeSavedBatch{
		raw:        append([]model.BetfairRawStreamMessage(nil), raw...),
		candidates: append([]model.BetfairExchangeQuoteCandidate(nil), candidates...),
		stats:      stats,
	})
	return stats, nil
}

func (s *fakeRuntimeBatchSaver) Close() error {
	return nil
}

func (s *fakeRuntimeBatchSaver) StartBetfairRuntimeRun(ctx context.Context, input model.BetfairRuntimeRunStart) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.telemetryStartErr != nil {
		return 0, s.telemetryStartErr
	}
	runID := s.nextRunID
	if runID <= 0 {
		runID = 101
	}
	s.nextRunID = runID + 1
	s.starts = append(s.starts, input)
	return runID, nil
}

func (s *fakeRuntimeBatchSaver) FinishBetfairRuntimeRun(ctx context.Context, runID int64, input model.BetfairRuntimeRunFinish) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.telemetryFinishErr != nil {
		return s.telemetryFinishErr
	}
	s.finishes = append(s.finishes, fakeRuntimeTelemetryFinish{runID: runID, input: input})
	return nil
}

func (s *fakeRuntimeBatchSaver) GetBetfairRuntimePostflight(ctx context.Context, runID int64) (model.BetfairRuntimePostflight, error) {
	if err := ctx.Err(); err != nil {
		return model.BetfairRuntimePostflight{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.postflightErr != nil {
		return model.BetfairRuntimePostflight{}, s.postflightErr
	}
	postflight := s.postflight
	if postflight.Run.ID == 0 {
		streamID := runtimeStreamID(runID)
		rawCount := int64(0)
		quoteHistoryRows := int64(0)
		for _, batch := range s.batches {
			for _, raw := range batch.raw {
				if raw.StreamID == streamID {
					rawCount++
				}
			}
			if len(batch.raw) > 0 && batch.raw[0].StreamID == streamID {
				quoteHistoryRows += int64(batch.stats.CandidatesInserted)
			}
		}
		latestQuoteCount := int64(len(s.latest))
		var finish model.BetfairRuntimeRunFinish
		for _, item := range s.finishes {
			if item.runID == runID {
				finish = item.input
			}
		}
		finishedAt := finish.FinishedAt
		elapsed := finish.ElapsedMS
		postflight.Run = model.BetfairRuntimeRun{
			ID:                      runID,
			StreamID:                streamID,
			RuntimeMode:             runtimeModeFake,
			TransportMode:           runtimeTransportFake,
			SinkType:                runtimeSinkPostgres,
			StartedAt:               fixedRuntimeNow(),
			FinishedAt:              &finishedAt,
			Status:                  finish.Status,
			ReadinessState:          finish.ReadinessState,
			MessagesRead:            finish.MessagesRead,
			MarketChangeMessages:    finish.MarketChangeMessages,
			HeartbeatMessages:       finish.HeartbeatMessages,
			StatusMessages:          finish.StatusMessages,
			RawMessagesSeen:         finish.RawMessagesSeen,
			RawMessagesFlushed:      finish.RawMessagesFlushed,
			RawMessagesInserted:     finish.RawMessagesInserted,
			CandidatesEmitted:       finish.CandidatesEmitted,
			CandidateBatchesSeen:    finish.CandidateBatchesSeen,
			CandidateBatchesFlushed: finish.CandidateBatchesFlushed,
			CandidatesInserted:      finish.CandidatesInserted,
			CandidatesUnchanged:     finish.CandidatesUnchanged,
			CandidatesSkipped:       finish.CandidatesSkipped,
			FlushCount:              finish.FlushCount,
			FlushErrors:             finish.FlushErrors,
			SinkErrors:              finish.SinkErrors,
			MarketsSeen:             finish.MarketsSeen,
			ReconnectCount:          finish.ReconnectCount,
			LastClock:               finish.LastClock,
			LastInitialClock:        finish.LastInitialClock,
			LastMessageTime:         finish.LastMessageTime,
			LastCandidateTime:       finish.LastCandidateTime,
			LastSinkFlushTime:       finish.LastSinkFlushTime,
			HeartbeatState:          finish.HeartbeatState,
			ElapsedMS:               &elapsed,
			ExitCode:                &finish.ExitCode,
		}
		if postflight.Run.Status == "" {
			postflight.Run.Status = model.BetfairRuntimeRunStatusSuccess
		}
		if postflight.Run.ReadinessState == "" {
			postflight.Run.ReadinessState = runtimeReadinessStopped
		}
		postflight.LinkedRawMessageCount = rawCount
		postflight.QuoteHistoryRows = quoteHistoryRows
		postflight.LatestQuoteCount = latestQuoteCount
		postflight.MappedQuoteCount = 1
		postflight.ReadyRunnerBookCount = 1
		postflight.ReadyComparisonCount = 1
	}
	return postflight, nil
}

func (s *fakeRuntimeBatchSaver) batchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batches)
}

func (s *fakeRuntimeBatchSaver) savedBatches() []fakeRuntimeSavedBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	batches := make([]fakeRuntimeSavedBatch, 0, len(s.batches))
	for _, batch := range s.batches {
		batches = append(batches, fakeRuntimeSavedBatch{
			raw:        append([]model.BetfairRawStreamMessage(nil), batch.raw...),
			candidates: append([]model.BetfairExchangeQuoteCandidate(nil), batch.candidates...),
			stats:      batch.stats,
		})
	}
	return batches
}

func (s *fakeRuntimeBatchSaver) startCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.starts)
}

func (s *fakeRuntimeBatchSaver) telemetryStarts() []model.BetfairRuntimeRunStart {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.BetfairRuntimeRunStart(nil), s.starts...)
}

func (s *fakeRuntimeBatchSaver) finishCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.finishes)
}

func (s *fakeRuntimeBatchSaver) telemetryFinishes() []fakeRuntimeTelemetryFinish {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fakeRuntimeTelemetryFinish(nil), s.finishes...)
}

func (s *fakeRuntimeBatchSaver) rawInserted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, batch := range s.batches {
		total += batch.stats.RawMessagesInserted
	}
	return total
}

func scalarRuntimeDBInt(ctx context.Context, t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		t.Fatalf("query scalar int: %v", err)
	}
	return count
}

func assertRuntimeNoFakeSecrets(t *testing.T, text string) {
	t.Helper()
	if strings.Contains(text, runtimeFakeAppKey) || strings.Contains(text, runtimeFakeSessionToken) {
		t.Fatalf("text leaked fake secrets: %s", text)
	}
}

func assertRuntimeNoSecrets(t *testing.T, text string, secrets ...string) {
	t.Helper()
	assertRuntimeNoFakeSecrets(t, text)
	for _, secret := range secrets {
		if strings.TrimSpace(secret) == "" {
			continue
		}
		if strings.Contains(text, secret) {
			t.Fatalf("text leaked runtime secret %q: %s", secret, text)
		}
	}
}

func readRuntimeTLSWrite(t *testing.T, writes <-chan []byte) []byte {
	t.Helper()
	select {
	case write := <-writes:
		return write
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for local TLS write")
		return nil
	}
}

func decodeRuntimeTLSWrite(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	if !bytes.HasSuffix(payload, []byte("\r\n")) {
		t.Fatalf("local TLS write missing CRLF suffix: %q", payload)
	}
	var decoded map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(payload), &decoded); err != nil {
		t.Fatalf("decode local TLS write %q: %v", payload, err)
	}
	return decoded
}

func assertRuntimeSubscriptionDataFilter(t *testing.T, payload map[string]any) {
	t.Helper()
	if _, ok := payload["market_filter"]; ok {
		t.Fatalf("subscription used snake_case market_filter: %+v", payload)
	}
	if _, ok := payload["market_data_filter"]; ok {
		t.Fatalf("subscription used snake_case market_data_filter: %+v", payload)
	}
	marketFilter, ok := payload["marketFilter"].(map[string]any)
	if !ok {
		t.Fatalf("subscription missing marketFilter: %+v", payload)
	}
	if _, ok := marketFilter["market_ids"]; ok {
		t.Fatalf("subscription used snake_case market_ids: %+v", marketFilter)
	}
	marketIDs, ok := marketFilter["marketIds"].([]any)
	if !ok || len(marketIDs) == 0 {
		t.Fatalf("subscription marketIds=%+v", marketFilter["marketIds"])
	}
	if _, ok := marketFilter["marketTypes"]; ok {
		t.Fatalf("subscription included broad marketTypes: %+v", marketFilter)
	}
	for _, marketID := range marketIDs {
		if marketID == "*" {
			t.Fatalf("subscription included wildcard market ID: %+v", marketIDs)
		}
	}
	filter, ok := payload["marketDataFilter"].(map[string]any)
	if !ok {
		t.Fatalf("subscription missing marketDataFilter: %+v", payload)
	}
	if _, ok := filter["ladder_levels"]; ok {
		t.Fatalf("subscription used snake_case ladder_levels: %+v", filter)
	}
	fieldsAny, ok := filter["fields"].([]any)
	if !ok {
		t.Fatalf("subscription fields=%+v", filter["fields"])
	}
	fields := make([]string, 0, len(fieldsAny))
	for _, value := range fieldsAny {
		field, ok := value.(string)
		if !ok {
			t.Fatalf("subscription field value=%+v", value)
		}
		fields = append(fields, field)
	}
	if !reflect.DeepEqual(fields, betfair.DefaultMarketDataFields()) {
		t.Fatalf("subscription fields=%v want %v", fields, betfair.DefaultMarketDataFields())
	}
	if filter["ladderLevels"] != float64(defaultRuntimeLadderLevels) {
		t.Fatalf("subscription ladderLevels=%+v want %d", filter["ladderLevels"], defaultRuntimeLadderLevels)
	}
	if strings.Contains(fmt.Sprintf("%+v", payload), "order") || strings.Contains(fmt.Sprintf("%+v", payload), "account") {
		t.Fatalf("subscription payload included order/account scope: %+v", payload)
	}
}

func runtimeLocalTLSTransportFactory(server runtimeLocalTLSServer) func(RuntimeStreamGroup) betfair.LiveTransport {
	return func(RuntimeStreamGroup) betfair.LiveTransport {
		return betfair.NewTLSLiveTransport(betfair.TLSLiveTransportOptions{
			Address:      server.address,
			ServerName:   "localhost",
			TLSConfig:    server.clientTLS,
			DialTimeout:  time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
		})
	}
}

type runtimeLocalTLSServer struct {
	address   string
	clientTLS *tls.Config
	close     func()
}

func newRuntimeLocalTLSServer(t *testing.T, handler func(net.Conn)) runtimeLocalTLSServer {
	t.Helper()
	certPEM, keyPEM := newRuntimeLocalTLSCertificate(t)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load local TLS certificate: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("append local TLS certificate to pool failed")
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("start local TLS listener: %v", err)
	}
	done := make(chan struct{})
	var handlers sync.WaitGroup
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer conn.Close()
				handler(conn)
			}()
		}
	}()
	return runtimeLocalTLSServer{
		address: listener.Addr().String(),
		clientTLS: &tls.Config{
			RootCAs:    pool,
			ServerName: "localhost",
			MinVersion: tls.VersionTLS12,
		},
		close: func() {
			_ = listener.Close()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("timed out closing local TLS listener")
			}
			handlerDone := make(chan struct{})
			go func() {
				handlers.Wait()
				close(handlerDone)
			}()
			select {
			case <-handlerDone:
			case <-time.After(time.Second):
				t.Fatal("timed out closing local TLS handler")
			}
		},
	}
}

func newRuntimeLocalTLSCertificate(t *testing.T) ([]byte, []byte) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate local TLS key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create local TLS certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return certPEM, keyPEM
}

type singleRuntimeTransport struct {
	conn betfair.LiveConn
}

func (t *singleRuntimeTransport) Connect(ctx context.Context) (betfair.LiveConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return t.conn, nil
}

type blockingRuntimeConn struct {
	reader  io.Reader
	closed  chan struct{}
	once    sync.Once
	writes  [][]byte
	writeCh chan int
}

func newBlockingRuntimeConn(messages ...string) *blockingRuntimeConn {
	var stream bytes.Buffer
	for _, message := range messages {
		stream.WriteString(message)
		stream.WriteString("\r\n")
	}
	return &blockingRuntimeConn{
		reader:  bytes.NewReader(stream.Bytes()),
		closed:  make(chan struct{}),
		writeCh: make(chan int, 4),
	}
}

func (c *blockingRuntimeConn) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	if n > 0 {
		return n, nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return n, err
	}
	<-c.closed
	return 0, io.ErrClosedPipe
}

func (c *blockingRuntimeConn) Write(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writes = append(c.writes, append([]byte(nil), payload...))
	select {
	case c.writeCh <- len(c.writes):
	default:
	}
	return nil
}

func (c *blockingRuntimeConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *blockingRuntimeConn) waitForWrites(t *testing.T, count int) {
	t.Helper()
	deadline := time.After(time.Second)
	seen := 0
	for seen < count {
		select {
		case seen = <-c.writeCh:
		case <-deadline:
			t.Fatalf("timed out waiting for %d writes, saw %d", count, seen)
		}
	}
}

var errRuntimeFakeDisconnect = fmt.Errorf("fake runtime disconnect")

func newFakeRuntimeDisconnectConn(messages ...string) *fakeRuntimeConn {
	var stream bytes.Buffer
	for _, message := range messages {
		stream.WriteString(message)
		stream.WriteString("\r\n")
	}
	return &fakeRuntimeConn{reader: &runtimeErrAfterReader{
		reader: bytes.NewReader(stream.Bytes()),
		err:    errRuntimeFakeDisconnect,
	}}
}

type runtimeErrAfterReader struct {
	reader *bytes.Reader
	err    error
}

func (r *runtimeErrAfterReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		return n, nil
	}
	if errors.Is(err, io.EOF) {
		return 0, r.err
	}
	return n, err
}
