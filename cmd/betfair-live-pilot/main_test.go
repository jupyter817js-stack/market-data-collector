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

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/source/betfair"
)

const (
	pilotTestAppKey  = "PILOT-APP-SECRET"
	pilotTestSession = "PILOT-SESSION-SECRET"
	pilotTestDBURL   = "postgres://pilot_user:pilot_password@localhost:5432/pilot_db?sslmode=disable"
)

func TestRunPilotDisabledDoesNotConnectOrOpenDB(t *testing.T) {
	transportFactory := &recordingPilotTransportFactory{}
	storeFactory := &recordingPilotStoreFactory{}
	var out bytes.Buffer
	code := runCommand(context.Background(), nil, &out, pilotEnv(nil), transportFactory.make, storeFactory.make)
	if code != exitPilotValidation {
		t.Fatalf("disabled exit code=%d want %d", code, exitPilotValidation)
	}
	if transportFactory.calls != 0 || storeFactory.calls != 0 {
		t.Fatalf("transport/store calls=%d/%d want 0/0", transportFactory.calls, storeFactory.calls)
	}
	output := out.String()
	assertPilotNoSecrets(t, output)
	if !strings.Contains(output, "enabled=false") || !strings.Contains(output, "stop_reason=enable_live_required") {
		t.Fatalf("disabled output=%q", output)
	}
}

func TestRunPilotRequiresPersistLive(t *testing.T) {
	transportFactory := &recordingPilotTransportFactory{}
	storeFactory := &recordingPilotStoreFactory{}
	var out bytes.Buffer
	code := runCommand(context.Background(), []string{"--enable-live"}, &out, pilotEnv(nil), transportFactory.make, storeFactory.make)
	if code != exitPilotValidation {
		t.Fatalf("missing persist exit code=%d want %d", code, exitPilotValidation)
	}
	if transportFactory.calls != 0 || storeFactory.calls != 0 {
		t.Fatalf("transport/store calls=%d/%d want 0/0", transportFactory.calls, storeFactory.calls)
	}
	output := out.String()
	assertPilotNoSecrets(t, output)
	if !strings.Contains(output, "persist_live_required") {
		t.Fatalf("missing persist output=%s", output)
	}
}

func TestRunPilotPreflightOnlyDoesNotConnectOrOpenDB(t *testing.T) {
	transportFactory := &recordingPilotTransportFactory{}
	storeFactory := &recordingPilotStoreFactory{}
	var out bytes.Buffer
	code := runCommand(context.Background(), append(basePilotArgs(), "--preflight-only"), &out, pilotEnv(map[string]string{
		"APP_ENV":     pilotTestAppKey,
		"SESSION_ENV": pilotTestSession,
		"DB_ENV":      pilotTestDBURL,
	}), transportFactory.make, storeFactory.make)
	if code != exitPilotSuccess {
		t.Fatalf("preflight exit code=%d want success output=%s", code, out.String())
	}
	if transportFactory.calls != 0 || storeFactory.calls != 0 {
		t.Fatalf("transport/store calls=%d/%d want 0/0 for preflight", transportFactory.calls, storeFactory.calls)
	}
	output := out.String()
	assertPilotNoSecrets(t, output)
	for _, want := range []string{"preflight_only=true", "stop_reason=preflight_only", "connected=false", "persist_live=true"} {
		if !strings.Contains(output, want) {
			t.Fatalf("preflight output missing %q:\n%s", want, output)
		}
	}
}

func TestRunPilotPreflightMissingDBEnvValueIsRedacted(t *testing.T) {
	transportFactory := &recordingPilotTransportFactory{}
	storeFactory := &recordingPilotStoreFactory{}
	var out bytes.Buffer
	code := runCommand(context.Background(), append(basePilotArgs(), "--preflight-only"), &out, pilotEnv(map[string]string{
		"APP_ENV":     pilotTestAppKey,
		"SESSION_ENV": pilotTestSession,
	}), transportFactory.make, storeFactory.make)
	if code != exitPilotCredential {
		t.Fatalf("missing DB env exit code=%d want credential", code)
	}
	if transportFactory.calls != 0 || storeFactory.calls != 0 {
		t.Fatalf("transport/store calls=%d/%d want 0/0", transportFactory.calls, storeFactory.calls)
	}
	output := out.String()
	assertPilotNoSecrets(t, output)
	if !strings.Contains(output, "DB_ENV") || !strings.Contains(output, "failure_reason=") {
		t.Fatalf("missing DB env output=%s", output)
	}
}

func TestRunPilotValidationExitAndRedaction(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing app key env flag",
			args: []string{"--enable-live", "--persist-live", "--session-token-env", "SESSION_ENV", "--database-url-env", "DB_ENV", "--address", "127.0.0.1:443", "--server-name", "localhost", "--market-id", "1.111"},
			want: "app-key-env is required",
		},
		{
			name: "missing session env flag",
			args: []string{"--enable-live", "--persist-live", "--app-key-env", "APP_ENV", "--database-url-env", "DB_ENV", "--address", "127.0.0.1:443", "--server-name", "localhost", "--market-id", "1.111"},
			want: "session-token-env is required",
		},
		{
			name: "missing market id",
			args: []string{"--enable-live", "--persist-live", "--app-key-env", "APP_ENV", "--session-token-env", "SESSION_ENV", "--database-url-env", "DB_ENV", "--address", "127.0.0.1:443", "--server-name", "localhost"},
			want: "market-id is required",
		},
		{
			name: "missing address",
			args: []string{"--enable-live", "--persist-live", "--app-key-env", "APP_ENV", "--session-token-env", "SESSION_ENV", "--database-url-env", "DB_ENV", "--server-name", "localhost", "--market-id", "1.111"},
			want: "address is required",
		},
		{
			name: "missing server name",
			args: []string{"--enable-live", "--persist-live", "--app-key-env", "APP_ENV", "--session-token-env", "SESSION_ENV", "--database-url-env", "DB_ENV", "--address", "127.0.0.1:443", "--market-id", "1.111"},
			want: "server-name is required",
		},
		{
			name: "invalid duration",
			args: append(basePilotArgs(), "--duration", "0s"),
			want: "duration must be positive",
		},
		{
			name: "invalid max messages",
			args: append(basePilotArgs(), "--max-messages", "0"),
			want: "max-messages must be positive",
		},
		{
			name: "duration limit",
			args: append(basePilotArgs(), "--duration", "3m"),
			want: "duration exceeds pilot limit",
		},
		{
			name: "max messages limit",
			args: append(basePilotArgs(), "--max-messages", "501"),
			want: "max-messages exceeds pilot limit",
		},
		{
			name: "market id limit",
			args: append(basePilotArgs(), "--market-id", "1.2,1.3,1.4,1.5,1.6"),
			want: "too many market IDs",
		},
		{
			name: "wildcard market id",
			args: append(basePilotArgs(), "--market-id", "*"),
			want: "wildcard market subscriptions are not allowed",
		},
		{
			name: "invalid run label",
			args: append(basePilotArgs(), "--run-label", "bad label"),
			want: "run-label may contain only",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transportFactory := &recordingPilotTransportFactory{}
			storeFactory := &recordingPilotStoreFactory{}
			var out bytes.Buffer
			code := runCommand(context.Background(), tc.args, &out, pilotEnv(map[string]string{
				"APP_ENV":     pilotTestAppKey,
				"SESSION_ENV": pilotTestSession,
				"DB_ENV":      pilotTestDBURL,
			}), transportFactory.make, storeFactory.make)
			if code != exitPilotValidation {
				t.Fatalf("validation exit code=%d want %d output=%s", code, exitPilotValidation, out.String())
			}
			if transportFactory.calls != 0 || storeFactory.calls != 0 {
				t.Fatalf("transport/store calls=%d/%d want 0/0", transportFactory.calls, storeFactory.calls)
			}
			output := out.String()
			assertPilotNoSecrets(t, output)
			if !strings.Contains(output, tc.want) {
				t.Fatalf("validation output=%s want containing %q", output, tc.want)
			}
		})
	}
}

func TestRunPilotDatabaseOpenFailureRedactsURL(t *testing.T) {
	transportFactory := &recordingPilotTransportFactory{}
	storeFactory := &recordingPilotStoreFactory{err: fmt.Errorf("open %s failed for pilot_user", pilotTestDBURL)}
	var out bytes.Buffer
	code := runCommand(context.Background(), basePilotArgs(), &out, pilotEnv(map[string]string{
		"APP_ENV":     pilotTestAppKey,
		"SESSION_ENV": pilotTestSession,
		"DB_ENV":      pilotTestDBURL,
	}), transportFactory.make, storeFactory.make)
	if code != exitPilotDatabase {
		t.Fatalf("database failure exit code=%d want %d output=%s", code, exitPilotDatabase, out.String())
	}
	if transportFactory.calls != 0 || storeFactory.calls != 1 {
		t.Fatalf("transport/store calls=%d/%d want 0/1", transportFactory.calls, storeFactory.calls)
	}
	assertPilotNoSecrets(t, out.String())
	if !strings.Contains(out.String(), "[REDACTED_DATABASE_URL]") {
		t.Fatalf("database failure output missing redaction marker:\n%s", out.String())
	}
}

func TestRunPilotTelemetryStartFailureAbortsBeforeTransport(t *testing.T) {
	transportFactory := &recordingPilotTransportFactory{}
	storeFactory := &recordingPilotStoreFactory{store: &fakePilotStore{startErr: errors.New("telemetry start failed")}}
	var out bytes.Buffer
	code := runCommand(context.Background(), append(basePilotArgs(), "--run-label", "pilot.safe-1"), &out, pilotEnv(map[string]string{
		"APP_ENV":     pilotTestAppKey,
		"SESSION_ENV": pilotTestSession,
		"DB_ENV":      pilotTestDBURL,
	}), transportFactory.make, storeFactory.make)
	if code != exitPilotDatabase {
		t.Fatalf("start telemetry failure exit code=%d want %d output=%s", code, exitPilotDatabase, out.String())
	}
	if transportFactory.calls != 0 || storeFactory.calls != 1 || storeFactory.store.startCalls != 1 {
		t.Fatalf("transport/store/start calls=%d/%d/%d want 0/1/1", transportFactory.calls, storeFactory.calls, storeFactory.store.startCalls)
	}
	assertPilotNoSecrets(t, out.String())
	if !strings.Contains(out.String(), "telemetry start failed") {
		t.Fatalf("start failure output=%s", out.String())
	}
}

func TestRunPilotJSONHappyPathPersistsThroughFakeStore(t *testing.T) {
	conn := newPilotFakeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"initial-pilot"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"sub-pilot"}`,
		`{"op":"heartbeat","id":3,"clk":"heartbeat-pilot"}`,
		pilotMarketImageMessage("1.111"),
	)
	transportFactory := &recordingPilotTransportFactory{transport: &pilotFakeTransport{conns: []betfair.LiveConn{conn}}}
	fakeStore := &fakePilotStore{}
	storeFactory := &recordingPilotStoreFactory{store: fakeStore}
	args := append(basePilotArgs(), "--output", "json", "--max-messages", "2", "--stream-id", "pilot-test-stream")
	var out bytes.Buffer
	code := runCommand(context.Background(), args, &out, pilotEnv(map[string]string{
		"APP_ENV":     pilotTestAppKey,
		"SESSION_ENV": pilotTestSession,
		"DB_ENV":      pilotTestDBURL,
	}), transportFactory.make, storeFactory.make)
	if code != exitPilotSuccess {
		t.Fatalf("pilot exit code=%d output=%s", code, out.String())
	}
	output := out.String()
	assertPilotNoSecrets(t, output)
	var report pilotReport
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &report); err != nil {
		t.Fatalf("decode pilot report json: %v output=%s", err, output)
	}
	if !report.Enabled || !report.PersistLive || report.PreflightOnly || !report.Connected || !report.Authenticated || !report.Subscribed {
		t.Fatalf("pilot report status=%+v", report)
	}
	if report.RawMessagesSeen != 4 || report.RawMessagesInserted != 4 || report.CandidatesEmitted != 2 || report.CandidatesInserted != 2 {
		t.Fatalf("pilot report persistence counters=%+v", report)
	}
	if fakeStore.saveCalls != 1 || len(fakeStore.raw) != 4 || len(fakeStore.candidates) != 2 {
		t.Fatalf("fake store calls/raw/candidates=%d/%d/%d", fakeStore.saveCalls, len(fakeStore.raw), len(fakeStore.candidates))
	}
	if fakeStore.startCalls != 1 || fakeStore.finishCalls != 1 {
		t.Fatalf("telemetry start/finish calls=%d/%d want 1/1", fakeStore.startCalls, fakeStore.finishCalls)
	}
	if fakeStore.finishInput.Status != model.BetfairLivePilotRunStatusSuccess || fakeStore.finishInput.RawMessagesInserted != 4 || fakeStore.finishInput.CandidatesInserted != 2 {
		t.Fatalf("finish telemetry input=%+v", fakeStore.finishInput)
	}
	if fakeStore.raw[0].StreamID != "pilot-test-stream" || fakeStore.raw[3].StreamID != "pilot-test-stream" {
		t.Fatalf("raw stream IDs not preserved: %#v", fakeStore.raw)
	}
	if report.RunID != 42 {
		t.Fatalf("report run id=%d want 42", report.RunID)
	}
	if report.StopReason != betfair.LiveStopReasonMaxMessages || report.LastClock != "heartbeat-pilot" || report.LastInitialClock != "initial-pilot" {
		t.Fatalf("pilot report stop/clocks=%+v", report)
	}
	assertPilotSubscriptionDataFilter(t, conn.writes)
	assertPilotNoSecrets(t, fmt.Sprintf("%+v", report))
}

func TestRunPilotTextReportRedaction(t *testing.T) {
	conn := newPilotFakeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"initial-pilot"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"sub-pilot"}`,
		pilotMarketImageMessage("1.111"),
	)
	transportFactory := &recordingPilotTransportFactory{transport: &pilotFakeTransport{conns: []betfair.LiveConn{conn}}}
	storeFactory := &recordingPilotStoreFactory{store: &fakePilotStore{}}
	var out bytes.Buffer
	code := runCommand(context.Background(), append(basePilotArgs(), "--max-messages", "1"), &out, pilotEnv(map[string]string{
		"APP_ENV":     pilotTestAppKey,
		"SESSION_ENV": pilotTestSession,
		"DB_ENV":      pilotTestDBURL,
	}), transportFactory.make, storeFactory.make)
	if code != exitPilotSuccess {
		t.Fatalf("text pilot exit code=%d output=%s", code, out.String())
	}
	output := out.String()
	assertPilotNoSecrets(t, output)
	for _, want := range []string{
		"persist_live=true",
		"persistence=true",
		"collector_integration=false",
		"market_stream_only=true",
		"raw_messages_inserted=3",
		"candidates_inserted=2",
		"stop_reason=max_messages",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("pilot output missing %q:\n%s", want, output)
		}
	}
}

func TestRunPilotStatusFailuresRedactSecretsAndDoNotPersist(t *testing.T) {
	tests := []struct {
		name string
		conn betfair.LiveConn
		want string
	}{
		{
			name: "auth failure",
			conn: newPilotFakeConn(`{"op":"status","id":1,"statusCode":"FAILURE","errorCode":"AUTH","errorMessage":"bad ` + pilotTestAppKey + ` ` + pilotTestSession + ` ` + pilotTestDBURL + `"}`),
			want: "authentication failed",
		},
		{
			name: "subscription failure",
			conn: newPilotFakeConn(
				`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
				`{"op":"status","id":2,"statusCode":"FAILURE","errorCode":"SUB","errorMessage":"bad `+pilotTestAppKey+` `+pilotTestSession+`"}`,
			),
			want: "subscription failed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transportFactory := &recordingPilotTransportFactory{transport: &pilotFakeTransport{conns: []betfair.LiveConn{tc.conn}}}
			fakeStore := &fakePilotStore{}
			storeFactory := &recordingPilotStoreFactory{store: fakeStore}
			var out bytes.Buffer
			code := runCommand(context.Background(), append(basePilotArgs(), "--output=json"), &out, pilotEnv(map[string]string{
				"APP_ENV":     pilotTestAppKey,
				"SESSION_ENV": pilotTestSession,
				"DB_ENV":      pilotTestDBURL,
			}), transportFactory.make, storeFactory.make)
			if code != exitPilotConnect {
				t.Fatalf("status failure exit code=%d want %d output=%s", code, exitPilotConnect, out.String())
			}
			output := out.String()
			assertPilotNoSecrets(t, output)
			if !strings.Contains(output, tc.want) {
				t.Fatalf("status failure output=%s want containing %q", output, tc.want)
			}
			if fakeStore.saveCalls != 0 {
				t.Fatalf("status failure save calls=%d want 0", fakeStore.saveCalls)
			}
			if fakeStore.startCalls != 1 || fakeStore.finishCalls != 1 {
				t.Fatalf("status failure telemetry start/finish=%d/%d want 1/1", fakeStore.startCalls, fakeStore.finishCalls)
			}
			if fakeStore.finishInput.Status != model.BetfairLivePilotRunStatusFailed {
				t.Fatalf("status failure finish input=%+v", fakeStore.finishInput)
			}
		})
	}
}

func TestRunPilotFinalTelemetryFailureReturnsDatabaseError(t *testing.T) {
	conn := newPilotFakeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"initial-pilot"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"sub-pilot"}`,
		pilotMarketImageMessage("1.111"),
	)
	transportFactory := &recordingPilotTransportFactory{transport: &pilotFakeTransport{conns: []betfair.LiveConn{conn}}}
	fakeStore := &fakePilotStore{finishErr: errors.New("final telemetry update failed")}
	storeFactory := &recordingPilotStoreFactory{store: fakeStore}
	var out bytes.Buffer
	code := runCommand(context.Background(), append(basePilotArgs(), "--max-messages", "1"), &out, pilotEnv(map[string]string{
		"APP_ENV":     pilotTestAppKey,
		"SESSION_ENV": pilotTestSession,
		"DB_ENV":      pilotTestDBURL,
	}), transportFactory.make, storeFactory.make)
	if code != exitPilotDatabase {
		t.Fatalf("final telemetry failure exit code=%d want %d output=%s", code, exitPilotDatabase, out.String())
	}
	if fakeStore.saveCalls != 1 || fakeStore.startCalls != 1 || fakeStore.finishCalls != 1 {
		t.Fatalf("save/start/finish calls=%d/%d/%d want 1/1/1", fakeStore.saveCalls, fakeStore.startCalls, fakeStore.finishCalls)
	}
	assertPilotNoSecrets(t, out.String())
	if !strings.Contains(out.String(), "final telemetry update failed") {
		t.Fatalf("final telemetry failure output=%s", out.String())
	}
}

func TestRunPilotDurationFlushesPendingBatch(t *testing.T) {
	conn := newPilotBlockingConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
		pilotMarketImageMessage("1.111"),
	)
	transportFactory := &recordingPilotTransportFactory{transport: &pilotFakeTransport{conns: []betfair.LiveConn{conn}}}
	fakeStore := &fakePilotStore{}
	storeFactory := &recordingPilotStoreFactory{store: fakeStore}
	var out bytes.Buffer
	code := runCommand(context.Background(), append(basePilotArgs(), "--duration", "10ms"), &out, pilotEnv(map[string]string{
		"APP_ENV":     pilotTestAppKey,
		"SESSION_ENV": pilotTestSession,
		"DB_ENV":      pilotTestDBURL,
	}), transportFactory.make, storeFactory.make)
	if code != exitPilotSuccess {
		t.Fatalf("duration pilot exit code=%d output=%s", code, out.String())
	}
	output := out.String()
	assertPilotNoSecrets(t, output)
	if !strings.Contains(output, "stop_reason=duration") || fakeStore.saveCalls != 1 || len(fakeStore.raw) != 3 {
		t.Fatalf("duration output=%s saveCalls=%d raw=%d", output, fakeStore.saveCalls, len(fakeStore.raw))
	}
	if fakeStore.finishCalls != 1 || fakeStore.finishInput.Status != model.BetfairLivePilotRunStatusSuccess {
		t.Fatalf("duration finish telemetry=%d %+v", fakeStore.finishCalls, fakeStore.finishInput)
	}
}

func TestRunPilotRuntimeCancellationFinalizesTelemetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	conn := newPilotBlockingConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
		pilotMarketImageMessage("1.111"),
	)
	transportFactory := &recordingPilotTransportFactory{transport: &pilotFakeTransport{conns: []betfair.LiveConn{conn}}}
	fakeStore := &fakePilotStore{startSignal: started}
	storeFactory := &recordingPilotStoreFactory{store: fakeStore}
	done := make(chan int, 1)
	var out bytes.Buffer
	go func() {
		done <- runCommand(ctx, basePilotArgs(), &out, pilotEnv(map[string]string{
			"APP_ENV":     pilotTestAppKey,
			"SESSION_ENV": pilotTestSession,
			"DB_ENV":      pilotTestDBURL,
		}), transportFactory.make, storeFactory.make)
	}()
	<-started
	<-conn.blocked
	cancel()
	code := <-done
	if code != exitPilotCanceled {
		t.Fatalf("runtime canceled exit code=%d want %d output=%s", code, exitPilotCanceled, out.String())
	}
	assertPilotNoSecrets(t, out.String())
	if fakeStore.startCalls != 1 || fakeStore.finishCalls != 1 {
		t.Fatalf("runtime canceled telemetry start/finish=%d/%d want 1/1", fakeStore.startCalls, fakeStore.finishCalls)
	}
	if fakeStore.finishInput.Status != model.BetfairLivePilotRunStatusCanceled {
		t.Fatalf("runtime canceled finish input=%+v", fakeStore.finishInput)
	}
}

func TestRunPilotPostflightOnlyDoesNotRequireBetfairCredentialsOrTransport(t *testing.T) {
	transportFactory := &recordingPilotTransportFactory{}
	fakeStore := &fakePilotStore{postflight: model.BetfairLivePilotPostflight{
		Run: model.BetfairLivePilotRun{
			ID:                77,
			RunLabel:          "pilot.label",
			AddressRedacted:   "127.0.0.1:443",
			ServerName:        "localhost",
			MarketIDsCount:    1,
			MessagesRead:      4,
			CandidatesEmitted: 2,
		},
		LinkedRawMessageCount: 4,
		QuoteHistoryRows:      2,
		LatestQuoteCount:      2,
		MappedQuoteCount:      0,
		Warnings:              []string{"no confirmed mapped Betfair exchange quotes found for pilot market scope"},
	}}
	storeFactory := &recordingPilotStoreFactory{store: fakeStore}
	var out bytes.Buffer
	code := runCommand(context.Background(), []string{"--postflight-only", "--run-id", "77", "--database-url-env", "DB_ENV", "--output", "json"}, &out, pilotEnv(map[string]string{
		"DB_ENV": pilotTestDBURL,
	}), transportFactory.make, storeFactory.make)
	if code != exitPilotSuccess {
		t.Fatalf("postflight exit code=%d output=%s", code, out.String())
	}
	if transportFactory.calls != 0 || storeFactory.calls != 1 || fakeStore.postflightCalls != 1 {
		t.Fatalf("transport/store/postflight calls=%d/%d/%d want 0/1/1", transportFactory.calls, storeFactory.calls, fakeStore.postflightCalls)
	}
	assertPilotNoSecrets(t, out.String())
	var report pilotReport
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &report); err != nil {
		t.Fatalf("decode postflight report: %v output=%s", err, out.String())
	}
	if !report.PostflightOnly || report.RunID != 77 || report.LinkedRawMessageCount != 4 || report.QuoteHistoryRows != 2 || len(report.Warnings) != 1 {
		t.Fatalf("postflight report=%+v", report)
	}
}

func TestRunPilotContextCancellationExitCode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	transportFactory := &recordingPilotTransportFactory{}
	storeFactory := &recordingPilotStoreFactory{store: &fakePilotStore{}}
	var out bytes.Buffer
	code := runCommand(ctx, basePilotArgs(), &out, pilotEnv(map[string]string{
		"APP_ENV":     pilotTestAppKey,
		"SESSION_ENV": pilotTestSession,
		"DB_ENV":      pilotTestDBURL,
	}), transportFactory.make, storeFactory.make)
	if code != exitPilotCanceled {
		t.Fatalf("canceled exit code=%d want %d output=%s", code, exitPilotCanceled, out.String())
	}
	if transportFactory.calls != 0 || storeFactory.calls != 0 {
		t.Fatalf("transport/store calls=%d/%d want 0/0", transportFactory.calls, storeFactory.calls)
	}
	output := out.String()
	assertPilotNoSecrets(t, output)
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
		{"success", nil, exitPilotSuccess},
		{"disabled", errPilotLiveDisabled, exitPilotValidation},
		{"persist", errPilotPersist, exitPilotValidation},
		{"credential", pilotCredentialError{err: errors.New("missing env")}, exitPilotCredential},
		{"connect", pilotConnectError{err: errors.New("auth failed")}, exitPilotConnect},
		{"runtime", pilotRuntimeError{err: errors.New("parse failed")}, exitPilotRuntime},
		{"canceled", pilotCanceledError{err: context.Canceled}, exitPilotCanceled},
		{"database", pilotDatabaseError{err: errors.New("db failed")}, exitPilotDatabase},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := exitCodeForPilotError(tc.err); got != tc.want {
				t.Fatalf("exit code=%d want %d", got, tc.want)
			}
		})
	}
}

func basePilotArgs() []string {
	return []string{
		"--enable-live",
		"--persist-live",
		"--app-key-env", "APP_ENV",
		"--session-token-env", "SESSION_ENV",
		"--database-url-env", "DB_ENV",
		"--address", "127.0.0.1:443",
		"--server-name", "localhost",
		"--market-id", "1.111",
	}
}

func pilotEnv(values map[string]string) pilotEnvLookup {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}

type recordingPilotTransportFactory struct {
	calls     int
	options   []betfair.TLSLiveTransportOptions
	transport *pilotFakeTransport
}

func (f *recordingPilotTransportFactory) make(options betfair.TLSLiveTransportOptions) betfair.LiveTransport {
	f.calls++
	f.options = append(f.options, options)
	if f.transport == nil {
		f.transport = &pilotFakeTransport{}
	}
	return f.transport
}

type recordingPilotStoreFactory struct {
	calls int
	urls  []string
	store *fakePilotStore
	err   error
}

func (f *recordingPilotStoreFactory) make(ctx context.Context, databaseURL string) (pilotStore, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.calls++
	f.urls = append(f.urls, databaseURL)
	if f.err != nil {
		return nil, f.err
	}
	if f.store == nil {
		f.store = &fakePilotStore{}
	}
	return f.store, nil
}

type fakePilotStore struct {
	saveCalls       int
	startCalls      int
	finishCalls     int
	postflightCalls int
	raw             []model.BetfairRawStreamMessage
	candidates      []model.BetfairExchangeQuoteCandidate
	startInput      model.BetfairLivePilotRunStart
	finishInput     model.BetfairLivePilotRunFinish
	postflight      model.BetfairLivePilotPostflight
	err             error
	startErr        error
	finishErr       error
	postflightErr   error
	startSignal     chan struct{}
	closed          bool
}

func (s *fakePilotStore) SaveBetfairBatch(ctx context.Context, raw []model.BetfairRawStreamMessage, candidates []model.BetfairExchangeQuoteCandidate) (model.BetfairSaveStats, error) {
	if err := ctx.Err(); err != nil {
		return model.BetfairSaveStats{}, err
	}
	s.saveCalls++
	s.raw = append(s.raw, raw...)
	s.candidates = append(s.candidates, candidates...)
	if s.err != nil {
		return model.BetfairSaveStats{}, s.err
	}
	return model.BetfairSaveStats{
		RawMessagesInserted: len(raw),
		CandidatesSeen:      len(candidates),
		CandidatesInserted:  len(candidates),
	}, nil
}

func (s *fakePilotStore) StartBetfairLivePilotRun(ctx context.Context, input model.BetfairLivePilotRunStart) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.startCalls++
	s.startInput = input
	if s.startSignal != nil {
		close(s.startSignal)
		s.startSignal = nil
	}
	if s.startErr != nil {
		return 0, s.startErr
	}
	return 42, nil
}

func (s *fakePilotStore) FinishBetfairLivePilotRun(ctx context.Context, runID int64, input model.BetfairLivePilotRunFinish) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.finishCalls++
	s.finishInput = input
	if s.finishErr != nil {
		return s.finishErr
	}
	return nil
}

func (s *fakePilotStore) GetBetfairLivePilotPostflight(ctx context.Context, runID int64) (model.BetfairLivePilotPostflight, error) {
	if err := ctx.Err(); err != nil {
		return model.BetfairLivePilotPostflight{}, err
	}
	s.postflightCalls++
	if s.postflightErr != nil {
		return model.BetfairLivePilotPostflight{}, s.postflightErr
	}
	if s.postflight.Run.ID == 0 {
		s.postflight.Run.ID = runID
	}
	return s.postflight, nil
}

func (s *fakePilotStore) Close() error {
	s.closed = true
	return nil
}

type pilotFakeTransport struct {
	conns    []betfair.LiveConn
	connects int
}

func (t *pilotFakeTransport) Connect(ctx context.Context) (betfair.LiveConn, error) {
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

type pilotFakeConn struct {
	reader io.Reader
	writes [][]byte
	closed bool
}

func newPilotFakeConn(messages ...string) *pilotFakeConn {
	var stream bytes.Buffer
	for _, message := range messages {
		stream.WriteString(message)
		stream.WriteString("\r\n")
	}
	return &pilotFakeConn{reader: bytes.NewReader(stream.Bytes())}
}

func (c *pilotFakeConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *pilotFakeConn) Write(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writes = append(c.writes, append([]byte(nil), payload...))
	return nil
}

func (c *pilotFakeConn) Close() error {
	c.closed = true
	return nil
}

func assertPilotSubscriptionDataFilter(t *testing.T, writes [][]byte) {
	t.Helper()
	if len(writes) < 2 {
		t.Fatalf("writes=%d want auth+subscription", len(writes))
	}
	var payload map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(writes[1]), &payload); err != nil {
		t.Fatalf("decode pilot subscription payload: %v", err)
	}
	if payload["op"] != "marketSubscription" {
		t.Fatalf("pilot subscription payload=%+v", payload)
	}
	if _, ok := payload["market_filter"]; ok {
		t.Fatalf("pilot subscription used snake_case market_filter: %+v", payload)
	}
	if _, ok := payload["market_data_filter"]; ok {
		t.Fatalf("pilot subscription used snake_case market_data_filter: %+v", payload)
	}
	marketFilter, ok := payload["marketFilter"].(map[string]any)
	if !ok {
		t.Fatalf("pilot subscription missing marketFilter: %+v", payload)
	}
	marketIDs, ok := marketFilter["marketIds"].([]any)
	if !ok || len(marketIDs) != 1 || marketIDs[0] != "1.111" {
		t.Fatalf("pilot subscription marketIds=%+v", marketFilter["marketIds"])
	}
	if _, ok := marketFilter["marketTypes"]; ok {
		t.Fatalf("pilot subscription included broad marketTypes: %+v", marketFilter)
	}
	filter, ok := payload["marketDataFilter"].(map[string]any)
	if !ok {
		t.Fatalf("pilot subscription missing marketDataFilter: %+v", payload)
	}
	if _, ok := filter["ladder_levels"]; ok {
		t.Fatalf("pilot subscription used snake_case ladder_levels: %+v", filter)
	}
	fieldsAny, ok := filter["fields"].([]any)
	if !ok {
		t.Fatalf("pilot subscription fields=%+v", filter["fields"])
	}
	fields := make([]string, 0, len(fieldsAny))
	for _, value := range fieldsAny {
		field, ok := value.(string)
		if !ok {
			t.Fatalf("pilot subscription field=%+v", value)
		}
		fields = append(fields, field)
	}
	if !reflect.DeepEqual(fields, betfair.DefaultMarketDataFields()) {
		t.Fatalf("pilot subscription fields=%v want %v", fields, betfair.DefaultMarketDataFields())
	}
	if filter["ladderLevels"] != float64(defaultPilotLadderLevels) {
		t.Fatalf("pilot subscription ladderLevels=%+v want %d", filter["ladderLevels"], defaultPilotLadderLevels)
	}
	if strings.Contains(fmt.Sprintf("%+v", payload), "order") || strings.Contains(fmt.Sprintf("%+v", payload), "account") {
		t.Fatalf("pilot subscription included order/account scope: %+v", payload)
	}
}

type pilotBlockingConn struct {
	chunks    [][]byte
	blocked   chan struct{}
	closed    chan struct{}
	once      sync.Once
	blockOnce sync.Once
}

func newPilotBlockingConn(messages ...string) *pilotBlockingConn {
	chunks := make([][]byte, 0, len(messages))
	for _, message := range messages {
		chunks = append(chunks, []byte(message+"\r\n"))
	}
	return &pilotBlockingConn{
		chunks:  chunks,
		blocked: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (c *pilotBlockingConn) Read(p []byte) (int, error) {
	if len(c.chunks) > 0 {
		chunk := c.chunks[0]
		c.chunks = c.chunks[1:]
		n := copy(p, chunk)
		if n < len(chunk) {
			c.chunks = append([][]byte{chunk[n:]}, c.chunks...)
		}
		if len(c.chunks) == 0 {
			c.signalBlocked()
		}
		return n, nil
	}
	c.signalBlocked()
	<-c.closed
	return 0, io.ErrClosedPipe
}

func (c *pilotBlockingConn) Write(ctx context.Context, payload []byte) error {
	return ctx.Err()
}

func (c *pilotBlockingConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
	})
	return nil
}

func (c *pilotBlockingConn) signalBlocked() {
	c.blockOnce.Do(func() {
		close(c.blocked)
	})
}

func pilotMarketImageMessage(marketID string) string {
	return fmt.Sprintf(`{"op":"mcm","id":10,"pt":1715000000000,"ct":"SUB_IMAGE","mc":[{"id":%q,"img":true,"marketDefinition":{"status":"OPEN","eventId":"event-1","eventName":"Home FC v Away FC","marketTime":"2026-05-07T12:00:00.000Z","name":"Match Odds","inPlay":false,"runners":[{"id":101,"name":"Home FC","status":"ACTIVE"}]},"rc":[{"id":101,"ltp":2.20,"tv":120,"batb":[[0,2.30,75]],"batl":[[0,2.34,80]]}]}]}`, marketID)
}

func assertPilotNoSecrets(t *testing.T, s string) {
	t.Helper()
	for _, secret := range []string{pilotTestAppKey, pilotTestSession, pilotTestDBURL, "pilot_user", "pilot_password"} {
		if strings.Contains(s, secret) {
			t.Fatalf("output contains unredacted pilot secret %q: %s", secret, s)
		}
	}
}
