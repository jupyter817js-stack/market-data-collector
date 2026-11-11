package betfair

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestJSONLToCRLF(t *testing.T) {
	got := string(JSONLToCRLF([]byte("  {\"op\":\"connection\"}\n\n{\"op\":\"heartbeat\"}\r\n")))
	want := "{\"op\":\"connection\"}\r\n{\"op\":\"heartbeat\"}\r\n"
	if got != want {
		t.Fatalf("jsonl to crlf=%q want %q", got, want)
	}
}

func TestReplayStreamDryRunFromJSONLFixture(t *testing.T) {
	body := readBetfairFixtureBody(t, "market_cache_messages.jsonl")
	result, err := ReplayStream(context.Background(), strings.NewReader(string(body)), replayTestOptions(ReplayFormatJSONL))
	if err != nil {
		t.Fatalf("replay stream: %v", err)
	}

	if result.Stats.MessagesRead != 6 || result.Stats.MessagesParsed != 6 {
		t.Fatalf("messages read/parsed=%d/%d want 6/6", result.Stats.MessagesRead, result.Stats.MessagesParsed)
	}
	if result.Stats.MarketChangeMessages != 4 {
		t.Fatalf("market change messages=%d want 4", result.Stats.MarketChangeMessages)
	}
	if result.Stats.ControlMessages != 2 {
		t.Fatalf("control messages=%d want 2", result.Stats.ControlMessages)
	}
	if result.Stats.RawMessagesReady != 6 || len(result.RawMessages) != 6 {
		t.Fatalf("raw messages ready=%d len=%d want 6", result.Stats.RawMessagesReady, len(result.RawMessages))
	}
	if result.Stats.MarketsSeen != 3 {
		t.Fatalf("markets seen=%d want 3", result.Stats.MarketsSeen)
	}
	if result.Stats.CandidatesEmitted != 8 || result.Stats.CandidatesValid != 8 || len(result.PersistenceCandidates) != 8 {
		t.Fatalf("candidate stats emitted=%d valid=%d persistence=%d want 8", result.Stats.CandidatesEmitted, result.Stats.CandidatesValid, len(result.PersistenceCandidates))
	}
	if result.Stats.ParseErrors != 0 || result.Stats.PersistErrors != 0 {
		t.Fatalf("unexpected errors parse=%d persist=%d", result.Stats.ParseErrors, result.Stats.PersistErrors)
	}
	if result.RawMessages[0].Source != SourceBetfair || result.RawMessages[0].StreamID != "fixture-stream" {
		t.Fatalf("raw DTO source/stream=%q/%q", result.RawMessages[0].Source, result.RawMessages[0].StreamID)
	}
	if result.PersistenceCandidates[0].QuoteKey == "" || result.PersistenceCandidates[0].QuoteHash == "" {
		t.Fatalf("empty persisted candidate key/hash: %+v", result.PersistenceCandidates[0])
	}
}

func TestReplayStreamSupportsCRLFInput(t *testing.T) {
	stream := `{"op":"connection","connectionId":"conn-1"}` + "\r\n" +
		`{"op":"mcm","id":1,"pt":1715000000000,"ct":"SUB_IMAGE","mc":[{"id":"1.777","img":true,"marketDefinition":{"status":"OPEN","name":"Match Odds","runners":[{"id":1,"name":"A"}]},"rc":[{"id":1,"batb":[[0,2,10]],"batl":[[0,2.1,11]]}]}]}` + "\r\n"

	result, err := ReplayStream(context.Background(), strings.NewReader(stream), replayTestOptions(ReplayFormatCRLF))
	if err != nil {
		t.Fatalf("replay crlf: %v", err)
	}
	if result.Stats.MessagesParsed != 2 {
		t.Fatalf("messages parsed=%d want 2", result.Stats.MessagesParsed)
	}
	if result.Stats.CandidatesEmitted != 2 {
		t.Fatalf("candidates emitted=%d want 2", result.Stats.CandidatesEmitted)
	}
}

func TestReplayStreamUnsupportedFormat(t *testing.T) {
	_, err := ReplayStream(context.Background(), strings.NewReader(`{"op":"heartbeat"}`), ReplayOptions{Format: ReplayFormat("xml")})
	if err == nil {
		t.Fatal("unsupported format returned nil error")
	}
	if !strings.Contains(err.Error(), "unsupported betfair replay format") {
		t.Fatalf("unsupported format error=%v", err)
	}
}

func TestReplayStreamMalformedFixture(t *testing.T) {
	result, err := ReplayStream(context.Background(), strings.NewReader(`{"op":"mcm"`+"\n"), replayTestOptions(ReplayFormatJSONL))
	if err == nil {
		t.Fatal("malformed fixture returned nil error")
	}
	if result.Stats.ParseErrors != 1 {
		t.Fatalf("parse errors=%d want 1", result.Stats.ParseErrors)
	}
	if !strings.Contains(err.Error(), "malformed JSON") {
		t.Fatalf("malformed error=%v", err)
	}
}

func TestReplayStreamDeterministicCandidateKeysAndHashes(t *testing.T) {
	body := readBetfairFixtureBody(t, "market_cache_messages.jsonl")
	first, err := ReplayStream(context.Background(), strings.NewReader(string(body)), replayTestOptions(ReplayFormatJSONL))
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	second, err := ReplayStream(context.Background(), strings.NewReader(string(body)), replayTestOptions(ReplayFormatJSONL))
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if !reflect.DeepEqual(replayCandidateKeyHashes(first.Candidates), replayCandidateKeyHashes(second.Candidates)) {
		t.Fatalf("candidate key/hash mismatch:\nfirst=%v\nsecond=%v", replayCandidateKeyHashes(first.Candidates), replayCandidateKeyHashes(second.Candidates))
	}
}

func replayTestOptions(format ReplayFormat) ReplayOptions {
	receivedAt := time.UnixMilli(1715000000000).UTC().Add(time.Minute)
	return ReplayOptions{
		Format:     format,
		StreamID:   "fixture-stream",
		ReceivedAt: receivedAt,
		QuoteOptions: QuoteEmitOptions{
			Now:        receivedAt,
			FreshAfter: 2 * time.Minute,
			AgingAfter: 5 * time.Minute,
		},
	}
}

func readBetfairFixtureBody(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "betfair", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return body
}

func replayCandidateKeyHashes(candidates []QuoteCandidate) []string {
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.QuoteKey+"|"+candidate.QuoteHash)
	}
	return out
}
