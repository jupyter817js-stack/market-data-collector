package betfair

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParserParsesOneCRLFDelimitedMessage(t *testing.T) {
	parser := NewParser(strings.NewReader(`{"op":"connection","connectionId":"conn-1"}`+"\r\n"), 1024)
	msg, err := parser.Next(context.Background())
	if err != nil {
		t.Fatalf("next message: %v", err)
	}
	if msg.Op != OperationConnection {
		t.Fatalf("op=%q want %q", msg.Op, OperationConnection)
	}
	if msg.ConnectionID != "conn-1" {
		t.Fatalf("connection id=%q", msg.ConnectionID)
	}
	if string(msg.Raw) != `{"op":"connection","connectionId":"conn-1"}` {
		t.Fatalf("raw=%q", string(msg.Raw))
	}
}

func TestParserParsesMultipleCRLFDelimitedMessagesFromFixture(t *testing.T) {
	messages, err := ReadAll(context.Background(), strings.NewReader(crlfFixture(t)), 4096)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(messages) != 3 {
		t.Fatalf("messages len=%d want 3", len(messages))
	}
	if messages[0].Op != OperationConnection || messages[1].Op != OperationHeartbeat || messages[2].Op != OperationMarketChange {
		t.Fatalf("ops=%q/%q/%q", messages[0].Op, messages[1].Op, messages[2].Op)
	}
	if _, ok := messages[0].UnknownFields["extra"]; !ok {
		t.Fatal("connection unknown field was not preserved")
	}
	if _, ok := messages[2].UnknownFields["unknownTopLevel"]; !ok {
		t.Fatal("market-change unknown top-level field was not preserved")
	}
	if len(messages[2].MarketChanges) != 1 {
		t.Fatalf("market changes len=%d want 1", len(messages[2].MarketChanges))
	}
	change := messages[2].MarketChanges[0]
	if change.ID != "1.234" {
		t.Fatalf("market id=%q", change.ID)
	}
	if change.MarketDefinition == nil || change.MarketDefinition.Status != "OPEN" {
		t.Fatalf("market definition=%+v", change.MarketDefinition)
	}
	if _, ok := change.UnknownFields["unexpectedMarketField"]; !ok {
		t.Fatal("market-change unknown field was not preserved")
	}
	if len(change.RunnerChanges) != 2 {
		t.Fatalf("runner changes len=%d want 2", len(change.RunnerChanges))
	}
}

func TestParserMalformedJSONReturnsClearError(t *testing.T) {
	parser := NewParser(strings.NewReader(`{"op":"connection"`+"\r\n"), 1024)
	_, err := parser.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "malformed JSON") {
		t.Fatalf("malformed JSON error=%v", err)
	}
}

func TestParserRejectsOversizedMessage(t *testing.T) {
	parser := NewParser(strings.NewReader(`{"op":"connection","payload":"too-large"}`+"\r\n"), 12)
	_, err := parser.Next(context.Background())
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("oversized message error=%v want ErrMessageTooLarge", err)
	}
}

func TestParserRespectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	parser := NewParser(strings.NewReader(`{"op":"connection"}`+"\r\n"), 1024)
	_, err := parser.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error=%v want context.Canceled", err)
	}
}

func TestParserUnknownMessageTypeIsPreserved(t *testing.T) {
	parser := NewParser(strings.NewReader(`{"op":"future-op","field":123}`+"\r\n"), 1024)
	msg, err := parser.Next(context.Background())
	if err != nil {
		t.Fatalf("next message: %v", err)
	}
	if msg.Op != Operation("future-op") {
		t.Fatalf("op=%q", msg.Op)
	}
	if _, ok := msg.UnknownFields["field"]; !ok {
		t.Fatal("unknown field was not preserved")
	}
	if string(msg.Raw) != `{"op":"future-op","field":123}` {
		t.Fatalf("raw=%q", msg.Raw)
	}
}

func TestParserEOF(t *testing.T) {
	parser := NewParser(strings.NewReader(""), 1024)
	_, err := parser.Next(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("empty reader error=%v want io.EOF", err)
	}
}

func TestHeartbeatTrackerStateTransitions(t *testing.T) {
	tracker, err := NewHeartbeatTracker(StalenessConfig{
		HealthyAfter: time.Second,
		StaleAfter:   3 * time.Second,
	})
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	now := time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC)
	if state := tracker.State(now); state != StreamStateUnknown {
		t.Fatalf("initial state=%q want unknown", state)
	}

	tracker.Observe(Message{Op: OperationHeartbeat}, now)
	if state := tracker.State(now.Add(time.Second)); state != StreamStateHealthy {
		t.Fatalf("healthy state=%q", state)
	}
	if state := tracker.State(now.Add(2 * time.Second)); state != StreamStateAging {
		t.Fatalf("aging state=%q", state)
	}
	if state := tracker.State(now.Add(4 * time.Second)); state != StreamStateStale {
		t.Fatalf("stale state=%q", state)
	}
	if _, ok := tracker.LastControlTime(); !ok {
		t.Fatal("last control time was not tracked")
	}
}

func TestSupervisorRunsInMemoryStream(t *testing.T) {
	var handled []Message
	now := time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC)
	supervisor, err := NewSupervisor(
		ReaderSource{Reader: strings.NewReader(crlfFixture(t))},
		MessageHandlerFunc(func(ctx context.Context, msg Message) error {
			handled = append(handled, msg)
			return nil
		}),
		SupervisorConfig{
			MaxMessageSize: 4096,
			Staleness: StalenessConfig{
				HealthyAfter: time.Second,
				StaleAfter:   3 * time.Second,
			},
			Now: func() time.Time { return now },
		},
	)
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	if err := supervisor.Run(context.Background()); err != nil {
		t.Fatalf("run supervisor: %v", err)
	}
	if len(handled) != 3 {
		t.Fatalf("handled messages=%d want 3", len(handled))
	}
	if state := supervisor.State(now); state != StreamStateHealthy {
		t.Fatalf("supervisor state=%q want healthy", state)
	}
	if _, ok := supervisor.LastMessageTime(); !ok {
		t.Fatal("last message time was not tracked")
	}
	if _, ok := supervisor.LastControlTime(); !ok {
		t.Fatal("last control time was not tracked")
	}
}

func crlfFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "..", "testdata", "betfair", "stream_messages.jsonl")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read betfair fixture: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	return strings.Join(lines, "\r\n") + "\r\n"
}
