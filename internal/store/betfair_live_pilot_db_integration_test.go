package store

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/betfair"
)

func TestDBIntegrationBetfairLivePilotStyleFakeStreamPersistence(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	st, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	raw, candidates := runBetfairPilotStyleFakeStream(ctx, t, "pilot-db-stream", betfairPilotDBMarketMessage("1.777", 2.30, 75, 2.34, 80))
	stats, err := st.SaveBetfairBatch(ctx, raw, candidates)
	if err != nil {
		t.Fatalf("save first pilot-style batch: %v", err)
	}
	if stats.RawMessagesInserted != 4 || stats.CandidatesInserted != 2 || stats.CandidatesUnchanged != 0 || stats.CandidatesSkipped != 0 {
		t.Fatalf("first pilot-style stats=%+v", stats)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_raw_stream_messages WHERE stream_id = 'pilot-db-stream'`); got != 4 {
		t.Fatalf("raw count after first run=%d want 4", got)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != 2 {
		t.Fatalf("history count after first run=%d want 2", got)
	}

	raw, candidates = runBetfairPilotStyleFakeStream(ctx, t, "pilot-db-stream", betfairPilotDBMarketMessage("1.777", 2.30, 75, 2.34, 80))
	stats, err = st.SaveBetfairBatch(ctx, raw, candidates)
	if err != nil {
		t.Fatalf("save unchanged pilot-style batch: %v", err)
	}
	if stats.RawMessagesInserted != 4 || stats.CandidatesInserted != 0 || stats.CandidatesUnchanged != 2 || stats.CandidatesSkipped != 0 {
		t.Fatalf("unchanged pilot-style stats=%+v", stats)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_raw_stream_messages WHERE stream_id = 'pilot-db-stream'`); got != 8 {
		t.Fatalf("raw count after unchanged run=%d want 8", got)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != 2 {
		t.Fatalf("history count after unchanged run=%d want 2", got)
	}

	raw, candidates = runBetfairPilotStyleFakeStream(ctx, t, "pilot-db-stream-changed", betfairPilotDBMarketMessage("1.777", 2.40, 90, 2.46, 95))
	stats, err = st.SaveBetfairBatch(ctx, raw, candidates)
	if err != nil {
		t.Fatalf("save changed pilot-style batch: %v", err)
	}
	if stats.RawMessagesInserted != 4 || stats.CandidatesInserted != 2 || stats.CandidatesUnchanged != 0 || stats.CandidatesSkipped != 0 {
		t.Fatalf("changed pilot-style stats=%+v", stats)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != 4 {
		t.Fatalf("history count after changed run=%d want 4", got)
	}
	back := findBetfairPilotBackCandidate(t, candidates)
	assertBetfairLatest(ctx, t, db, back.QuoteKey, back.QuoteHash, back.PriceDecimal, back.AvailableSize)
}

func TestDBIntegrationBetfairLivePilotTelemetryAndPostflight(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	st, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	firstRunID := startBetfairPilotTelemetryRun(ctx, t, st, "pilot.telemetry-1", "pilot-telemetry-stream-1")
	firstRaw, firstCandidates := runBetfairPilotStyleFakeStream(ctx, t, "pilot-telemetry-stream-1", betfairPilotDBMarketMessage("1.777", 2.30, 75, 2.34, 80))
	firstStats, err := st.SaveBetfairBatch(ctx, firstRaw, firstCandidates)
	if err != nil {
		t.Fatalf("save first telemetry pilot batch: %v", err)
	}
	if firstStats.RawMessagesInserted != 4 || firstStats.CandidatesInserted != 2 || firstStats.CandidatesUnchanged != 0 {
		t.Fatalf("first telemetry pilot stats=%+v", firstStats)
	}
	finishBetfairPilotTelemetryRun(ctx, t, st, firstRunID, model.BetfairLivePilotRunStatusSuccess, firstStats, 4, 2)

	firstRun, err := st.GetBetfairLivePilotRun(ctx, firstRunID)
	if err != nil {
		t.Fatalf("get first pilot telemetry run: %v", err)
	}
	if firstRun.Status != model.BetfairLivePilotRunStatusSuccess || firstRun.RunLabel != "pilot.telemetry-1" || firstRun.StreamID != "pilot-telemetry-stream-1" {
		t.Fatalf("first pilot run=%+v", firstRun)
	}
	for _, secret := range []string{"test-app-key", "test-session-token", dsn} {
		if containsBetfairPilotRunSecret(firstRun, secret) {
			t.Fatalf("pilot telemetry stored secret %q in run %+v", secret, firstRun)
		}
	}

	firstPostflight, err := st.GetBetfairLivePilotPostflight(ctx, firstRunID)
	if err != nil {
		t.Fatalf("get first pilot postflight: %v", err)
	}
	if firstPostflight.LinkedRawMessageCount != 4 || firstPostflight.LatestQuoteCount != 2 {
		t.Fatalf("first postflight counts=%+v", firstPostflight)
	}
	if len(firstPostflight.Warnings) == 0 {
		t.Fatalf("first postflight should warn before mapping/readiness exists: %+v", firstPostflight)
	}

	secondRunID := startBetfairPilotTelemetryRun(ctx, t, st, "pilot.telemetry-2", "pilot-telemetry-stream-2")
	secondRaw, secondCandidates := runBetfairPilotStyleFakeStream(ctx, t, "pilot-telemetry-stream-2", betfairPilotDBMarketMessage("1.777", 2.30, 75, 2.34, 80))
	secondStats, err := st.SaveBetfairBatch(ctx, secondRaw, secondCandidates)
	if err != nil {
		t.Fatalf("save repeated telemetry pilot batch: %v", err)
	}
	if secondStats.RawMessagesInserted != 4 || secondStats.CandidatesInserted != 0 || secondStats.CandidatesUnchanged != 2 {
		t.Fatalf("repeated telemetry pilot stats=%+v", secondStats)
	}
	finishBetfairPilotTelemetryRun(ctx, t, st, secondRunID, model.BetfairLivePilotRunStatusSuccess, secondStats, 4, 2)
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != 2 {
		t.Fatalf("quote history after repeated telemetry run=%d want 2", got)
	}

	changedRunID := startBetfairPilotTelemetryRun(ctx, t, st, "pilot.telemetry-3", "pilot-telemetry-stream-3")
	changedRaw, changedCandidates := runBetfairPilotStyleFakeStream(ctx, t, "pilot-telemetry-stream-3", betfairPilotDBMarketMessage("1.777", 2.40, 90, 2.46, 95))
	changedStats, err := st.SaveBetfairBatch(ctx, changedRaw, changedCandidates)
	if err != nil {
		t.Fatalf("save changed telemetry pilot batch: %v", err)
	}
	if changedStats.RawMessagesInserted != 4 || changedStats.CandidatesInserted != 2 || changedStats.CandidatesUnchanged != 0 {
		t.Fatalf("changed telemetry pilot stats=%+v", changedStats)
	}
	finishBetfairPilotTelemetryRun(ctx, t, st, changedRunID, model.BetfairLivePilotRunStatusSuccess, changedStats, 4, 2)
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != 4 {
		t.Fatalf("quote history after changed telemetry run=%d want 4", got)
	}
	changedBack := findBetfairPilotBackCandidate(t, changedCandidates)
	assertBetfairLatest(ctx, t, db, changedBack.QuoteKey, changedBack.QuoteHash, changedBack.PriceDecimal, changedBack.AvailableSize)

	changedPostflight, err := st.GetBetfairLivePilotPostflight(ctx, changedRunID)
	if err != nil {
		t.Fatalf("get changed pilot postflight: %v", err)
	}
	if changedPostflight.LinkedRawMessageCount != 4 || changedPostflight.QuoteHistoryRows < 2 || changedPostflight.LatestQuoteCount != 2 {
		t.Fatalf("changed postflight counts=%+v", changedPostflight)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_live_pilot_run_summary`); got != 3 {
		t.Fatalf("pilot run summary count=%d want 3", got)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM data_quality_betfair_live_pilot_runs`); got == 0 {
		t.Fatalf("expected pilot audit diagnostic warnings before mapping/readiness exists")
	}
}

func TestDBIntegrationBetfairRuntimeTelemetryAndPostflight(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	st, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	firstRunID := startBetfairRuntimeTelemetryRun(ctx, t, st, "runtime.telemetry-1", "runtime-telemetry-stream-1")
	firstRaw, firstCandidates := runBetfairPilotStyleFakeStream(ctx, t, "runtime-telemetry-stream-1", betfairPilotDBMarketMessage("1.777", 2.30, 75, 2.34, 80))
	firstStats, err := st.SaveBetfairBatch(ctx, firstRaw, firstCandidates)
	if err != nil {
		t.Fatalf("save first runtime telemetry batch: %v", err)
	}
	if firstStats.RawMessagesInserted != 4 || firstStats.CandidatesInserted != 2 || firstStats.CandidatesUnchanged != 0 {
		t.Fatalf("first runtime telemetry stats=%+v", firstStats)
	}
	finishBetfairRuntimeTelemetryRun(ctx, t, st, firstRunID, "runtime-telemetry-stream-1", model.BetfairRuntimeRunStatusSuccess, firstStats, 4, 2)

	firstRun, err := st.GetBetfairRuntimeRun(ctx, firstRunID)
	if err != nil {
		t.Fatalf("get first runtime telemetry run: %v", err)
	}
	if firstRun.Status != model.BetfairRuntimeRunStatusSuccess || firstRun.RunLabel != "runtime.telemetry-1" || firstRun.StreamID != "runtime-telemetry-stream-1" {
		t.Fatalf("first runtime run=%+v", firstRun)
	}
	for _, secret := range []string{"test-app-key", "test-session-token", dsn} {
		if containsBetfairRuntimeRunSecret(firstRun, secret) {
			t.Fatalf("runtime telemetry stored secret %q in run %+v", secret, firstRun)
		}
	}

	firstPostflight, err := st.GetBetfairRuntimePostflight(ctx, firstRunID)
	if err != nil {
		t.Fatalf("get first runtime postflight: %v", err)
	}
	if firstPostflight.LinkedRawMessageCount != 4 || firstPostflight.LatestQuoteCount != 2 {
		t.Fatalf("first runtime postflight counts=%+v", firstPostflight)
	}
	if len(firstPostflight.Warnings) == 0 {
		t.Fatalf("first runtime postflight should warn before mapping/readiness exists: %+v", firstPostflight)
	}

	secondRunID := startBetfairRuntimeTelemetryRun(ctx, t, st, "runtime.telemetry-2", "runtime-telemetry-stream-2")
	secondRaw, secondCandidates := runBetfairPilotStyleFakeStream(ctx, t, "runtime-telemetry-stream-2", betfairPilotDBMarketMessage("1.777", 2.30, 75, 2.34, 80))
	secondStats, err := st.SaveBetfairBatch(ctx, secondRaw, secondCandidates)
	if err != nil {
		t.Fatalf("save repeated runtime telemetry batch: %v", err)
	}
	if secondStats.RawMessagesInserted != 4 || secondStats.CandidatesInserted != 0 || secondStats.CandidatesUnchanged != 2 {
		t.Fatalf("repeated runtime telemetry stats=%+v", secondStats)
	}
	finishBetfairRuntimeTelemetryRun(ctx, t, st, secondRunID, "runtime-telemetry-stream-2", model.BetfairRuntimeRunStatusSuccess, secondStats, 4, 2)
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != 2 {
		t.Fatalf("quote history after repeated runtime telemetry run=%d want 2", got)
	}

	changedRunID := startBetfairRuntimeTelemetryRun(ctx, t, st, "runtime.telemetry-3", "runtime-telemetry-stream-3")
	changedRaw, changedCandidates := runBetfairPilotStyleFakeStream(ctx, t, "runtime-telemetry-stream-3", betfairPilotDBMarketMessage("1.777", 2.40, 90, 2.46, 95))
	changedStats, err := st.SaveBetfairBatch(ctx, changedRaw, changedCandidates)
	if err != nil {
		t.Fatalf("save changed runtime telemetry batch: %v", err)
	}
	if changedStats.RawMessagesInserted != 4 || changedStats.CandidatesInserted != 2 || changedStats.CandidatesUnchanged != 0 {
		t.Fatalf("changed runtime telemetry stats=%+v", changedStats)
	}
	finishBetfairRuntimeTelemetryRun(ctx, t, st, changedRunID, "runtime-telemetry-stream-3", model.BetfairRuntimeRunStatusSuccess, changedStats, 4, 2)
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_exchange_quote_history`); got != 4 {
		t.Fatalf("quote history after changed runtime telemetry run=%d want 4", got)
	}
	changedBack := findBetfairPilotBackCandidate(t, changedCandidates)
	assertBetfairLatest(ctx, t, db, changedBack.QuoteKey, changedBack.QuoteHash, changedBack.PriceDecimal, changedBack.AvailableSize)

	changedPostflight, err := st.GetBetfairRuntimePostflight(ctx, changedRunID)
	if err != nil {
		t.Fatalf("get changed runtime postflight: %v", err)
	}
	if changedPostflight.LinkedRawMessageCount != 4 || changedPostflight.QuoteHistoryRows < 2 || changedPostflight.LatestQuoteCount != 2 {
		t.Fatalf("changed runtime postflight counts=%+v", changedPostflight)
	}
	runs, err := st.ListBetfairRuntimeRuns(ctx, 10)
	if err != nil {
		t.Fatalf("list runtime telemetry runs: %v", err)
	}
	if len(runs) < 3 {
		t.Fatalf("runtime telemetry list returned %d runs, want at least 3", len(runs))
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM betfair_runtime_run_summary`); got != 3 {
		t.Fatalf("runtime run summary count=%d want 3", got)
	}
	if got := scalarInt(ctx, t, db, `SELECT count(*) FROM data_quality_betfair_runtime_runs`); got == 0 {
		t.Fatalf("expected runtime audit diagnostic warnings before mapping/readiness exists")
	}
}

func runBetfairPilotStyleFakeStream(ctx context.Context, t *testing.T, streamID, marketMessage string) ([]model.BetfairRawStreamMessage, []model.BetfairExchangeQuoteCandidate) {
	t.Helper()
	receivedAt := time.Now().UTC().Truncate(time.Second)
	raw := []model.BetfairRawStreamMessage{}
	connector := betfair.NewLiveConnector(&betfairPilotDBFakeTransport{conn: newBetfairPilotDBFakeConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"initial-pilot-db"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"sub-pilot-db"}`,
		`{"op":"heartbeat","id":3,"clk":"heartbeat-pilot-db"}`,
		marketMessage,
	)}, betfair.LiveConnectorOptions{
		Enabled:      true,
		AppKey:       "test-app-key",
		SessionToken: "test-session-token",
		Subscription: betfair.MarketSubscriptionRequest{
			MarketIDs:    []string{"1.777"},
			Fields:       []string{"EX_BEST_OFFERS", "EX_TRADED"},
			LadderLevels: 3,
			HeartbeatMS:  5000,
		},
		HeartbeatAgingAfter:  10 * time.Second,
		HeartbeatStaleAfter:  15 * time.Second,
		MaxReconnectAttempts: 0,
		MaxMessageSize:       betfair.DefaultMaxMessageSize,
		MaxMessages:          2,
		OnMessage: func(msg betfair.Message, observedAt time.Time) error {
			raw = append(raw, msg.RawStreamMessage(streamID, observedAt))
			return nil
		},
	})
	result, err := connector.Run(ctx)
	if err != nil {
		t.Fatalf("run fake live connector: %v", err)
	}
	if len(raw) != 4 || len(result.Candidates) != 2 {
		t.Fatalf("raw/candidates=%d/%d want 4/2", len(raw), len(result.Candidates))
	}
	candidates := make([]model.BetfairExchangeQuoteCandidate, 0, len(result.Candidates))
	for _, candidate := range result.Candidates {
		candidates = append(candidates, candidate.PersistenceModel(receivedAt))
	}
	return raw, candidates
}

type betfairPilotDBFakeTransport struct {
	conn betfair.LiveConn
}

func (t *betfairPilotDBFakeTransport) Connect(ctx context.Context) (betfair.LiveConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return t.conn, nil
}

type betfairPilotDBFakeConn struct {
	reader io.Reader
}

func newBetfairPilotDBFakeConn(messages ...string) *betfairPilotDBFakeConn {
	var stream bytes.Buffer
	for _, message := range messages {
		stream.WriteString(message)
		stream.WriteString("\r\n")
	}
	return &betfairPilotDBFakeConn{reader: bytes.NewReader(stream.Bytes())}
}

func (c *betfairPilotDBFakeConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *betfairPilotDBFakeConn) Write(ctx context.Context, payload []byte) error {
	return ctx.Err()
}

func (c *betfairPilotDBFakeConn) Close() error {
	return nil
}

func betfairPilotDBMarketMessage(marketID string, backPrice, backSize, layPrice, laySize float64) string {
	return `{"op":"mcm","id":10,"pt":1715000000000,"ct":"SUB_IMAGE","mc":[{"id":"` + marketID + `","img":true,"marketDefinition":{"status":"OPEN","eventId":"event-1","eventName":"Home FC v Away FC","marketTime":"2026-05-07T12:00:00.000Z","name":"Match Odds","inPlay":false,"runners":[{"id":101,"name":"Home FC","status":"ACTIVE"}]},"rc":[{"id":101,"ltp":2.20,"tv":120,"batb":[[0,` + formatPilotFloat(backPrice) + `,` + formatPilotFloat(backSize) + `]],"batl":[[0,` + formatPilotFloat(layPrice) + `,` + formatPilotFloat(laySize) + `]]}]}]}`
}

func formatPilotFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func findBetfairPilotBackCandidate(t *testing.T, candidates []model.BetfairExchangeQuoteCandidate) model.BetfairExchangeQuoteCandidate {
	t.Helper()
	for _, candidate := range candidates {
		if candidate.ExchangeSide == "back" {
			return candidate
		}
	}
	t.Fatalf("back candidate not found in %#v", candidates)
	return model.BetfairExchangeQuoteCandidate{}
}

func startBetfairPilotTelemetryRun(ctx context.Context, t *testing.T, st *PostgresStore, runLabel, streamID string) int64 {
	t.Helper()
	runID, err := st.StartBetfairLivePilotRun(ctx, model.BetfairLivePilotRunStart{
		RunLabel:             runLabel,
		StreamID:             streamID,
		StartedAt:            time.Now().UTC().Add(-time.Second),
		AddressRedacted:      "redacted-betfair-stream:443",
		ServerName:           "redacted-betfair-stream",
		MarketIDs:            []string{"1.777"},
		DurationLimitSeconds: intPtrTest(30),
		MaxMessages:          intPtrTest(20),
	})
	if err != nil {
		t.Fatalf("start pilot telemetry run: %v", err)
	}
	return runID
}

func finishBetfairPilotTelemetryRun(ctx context.Context, t *testing.T, st *PostgresStore, runID int64, status string, stats model.BetfairSaveStats, messagesRead, candidatesEmitted int) {
	t.Helper()
	if err := st.FinishBetfairLivePilotRun(ctx, runID, model.BetfairLivePilotRunFinish{
		FinishedAt:           time.Now().UTC().Add(time.Second),
		Status:               status,
		StopReason:           "max_messages",
		MessagesRead:         messagesRead,
		MarketChangeMessages: 1,
		HeartbeatMessages:    1,
		StatusMessages:       2,
		RawMessagesSeen:      messagesRead,
		RawMessagesInserted:  stats.RawMessagesInserted,
		CandidatesEmitted:    candidatesEmitted,
		CandidatesInserted:   stats.CandidatesInserted,
		CandidatesUnchanged:  stats.CandidatesUnchanged,
		CandidatesSkipped:    stats.CandidatesSkipped,
		MarketsSeen:          1,
		LastClock:            "heartbeat-pilot-db",
		LastInitialClock:     "initial-pilot-db",
		LastMessageTime:      timePtr(time.Now().UTC()),
		HeartbeatState:       "healthy",
		ExitCode:             0,
		ElapsedMS:            100,
	}); err != nil {
		t.Fatalf("finish pilot telemetry run %d: %v", runID, err)
	}
}

func startBetfairRuntimeTelemetryRun(ctx context.Context, t *testing.T, st *PostgresStore, runLabel, streamID string) int64 {
	t.Helper()
	runID, err := st.StartBetfairRuntimeRun(ctx, model.BetfairRuntimeRunStart{
		RunLabel:             runLabel,
		StreamID:             streamID,
		RuntimeMode:          "fake",
		TransportMode:        "fake",
		SinkType:             "postgres",
		StartedAt:            time.Now().UTC().Add(-time.Second),
		MarketIDs:            []string{"1.777"},
		DurationLimitSeconds: intPtrTest(30),
		MaxMessages:          intPtrTest(20),
	})
	if err != nil {
		t.Fatalf("start runtime telemetry run: %v", err)
	}
	return runID
}

func finishBetfairRuntimeTelemetryRun(ctx context.Context, t *testing.T, st *PostgresStore, runID int64, streamID, status string, stats model.BetfairSaveStats, messagesRead, candidatesEmitted int) {
	t.Helper()
	now := time.Now().UTC()
	if err := st.FinishBetfairRuntimeRun(ctx, runID, model.BetfairRuntimeRunFinish{
		StreamID:                streamID,
		FinishedAt:              now.Add(time.Second),
		Status:                  status,
		ReadinessState:          "stopped",
		StoppedReason:           "max_messages",
		MessagesRead:            messagesRead,
		MarketChangeMessages:    1,
		HeartbeatMessages:       1,
		StatusMessages:          2,
		RawMessagesSeen:         messagesRead,
		RawMessagesFlushed:      stats.RawMessagesInserted,
		RawMessagesInserted:     stats.RawMessagesInserted,
		CandidatesEmitted:       candidatesEmitted,
		CandidateBatchesSeen:    1,
		CandidateBatchesFlushed: 1,
		CandidatesInserted:      stats.CandidatesInserted,
		CandidatesUnchanged:     stats.CandidatesUnchanged,
		CandidatesSkipped:       stats.CandidatesSkipped,
		FlushCount:              1,
		MarketsSeen:             1,
		LastClock:               "heartbeat-runtime-db",
		LastInitialClock:        "initial-runtime-db",
		LastMessageTime:         timePtr(now),
		LastCandidateTime:       timePtr(now),
		LastSinkFlushTime:       timePtr(now),
		HeartbeatState:          "healthy",
		ExitCode:                0,
		ElapsedMS:               100,
	}); err != nil {
		t.Fatalf("finish runtime telemetry run %d: %v", runID, err)
	}
}

func containsBetfairRuntimeRunSecret(run model.BetfairRuntimeRun, secret string) bool {
	if secret == "" {
		return false
	}
	values := []string{
		run.RunLabel,
		run.StreamID,
		run.RuntimeMode,
		run.TransportMode,
		run.SinkType,
		run.Status,
		run.ReadinessState,
		run.StoppedReason,
		run.FailureReason,
		run.LastClock,
		run.LastInitialClock,
		run.HeartbeatState,
	}
	for _, value := range values {
		if bytes.Contains([]byte(value), []byte(secret)) {
			return true
		}
	}
	return false
}

func containsBetfairPilotRunSecret(run model.BetfairLivePilotRun, secret string) bool {
	if secret == "" {
		return false
	}
	values := []string{
		run.RunLabel,
		run.StreamID,
		run.Status,
		run.StopReason,
		run.FailureReason,
		run.AddressRedacted,
		run.ServerName,
		run.LastClock,
		run.LastInitialClock,
		run.HeartbeatState,
	}
	for _, value := range values {
		if bytes.Contains([]byte(value), []byte(secret)) {
			return true
		}
	}
	return false
}

func intPtrTest(v int) *int {
	return &v
}

func timePtr(v time.Time) *time.Time {
	return &v
}
