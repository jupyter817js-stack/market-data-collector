package betfair

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

type ReplayFormat string

const (
	ReplayFormatJSONL ReplayFormat = "jsonl"
	ReplayFormatCRLF  ReplayFormat = "crlf"
)

type ReplayOptions struct {
	Format         ReplayFormat
	StreamID       string
	ReceivedAt     time.Time
	QuoteOptions   QuoteEmitOptions
	MaxMessageSize int
}

type ReplayStats struct {
	MessagesRead         int
	MessagesParsed       int
	MarketChangeMessages int
	ControlMessages      int
	RawMessagesReady     int
	RawMessagesInserted  int
	MarketsSeen          int
	CandidatesEmitted    int
	CandidatesValid      int
	CandidatesSkipped    int
	CandidatesInserted   int
	CandidatesUnchanged  int
	ParseErrors          int
	PersistErrors        int
	DryRun               bool
	Duration             time.Duration
}

type ReplayResult struct {
	Stats                 ReplayStats
	RawMessages           []model.BetfairRawStreamMessage
	Candidates            []QuoteCandidate
	PersistenceCandidates []model.BetfairExchangeQuoteCandidate
}

func ReplayStream(ctx context.Context, r io.Reader, opts ReplayOptions) (ReplayResult, error) {
	startedAt := time.Now()
	result := ReplayResult{}
	if r == nil {
		return result, fmt.Errorf("betfair replay input reader is nil")
	}
	opts, err := normalizeReplayOptions(opts)
	if err != nil {
		return result, err
	}

	reader, err := replayInputReader(r, opts.Format)
	if err != nil {
		result.Stats.ParseErrors = 1
		return result, err
	}

	cache := NewMarketCache(WithCacheClock(func() time.Time { return opts.ReceivedAt }))
	parser := NewParser(reader, opts.MaxMessageSize)
	for {
		msg, err := parser.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			result.Stats.ParseErrors++
			result.Stats.Duration = time.Since(startedAt)
			return result, fmt.Errorf("parse betfair replay stream: %w", err)
		}

		result.Stats.MessagesRead++
		result.Stats.MessagesParsed++
		if msg.IsControl() {
			result.Stats.ControlMessages++
		}
		if msg.Op == OperationMarketChange {
			result.Stats.MarketChangeMessages++
		}

		result.RawMessages = append(result.RawMessages, msg.RawStreamMessage(opts.StreamID, opts.ReceivedAt))
		result.Stats.RawMessagesReady = len(result.RawMessages)

		if err := cache.ApplyMessage(msg); err != nil {
			result.Stats.Duration = time.Since(startedAt)
			return result, fmt.Errorf("apply betfair replay message %d: %w", result.Stats.MessagesParsed, err)
		}
	}

	snapshots := cache.SnapshotAll()
	result.Stats.MarketsSeen = len(snapshots)

	candidates, err := EmitAllQuoteCandidates(cache, opts.QuoteOptions)
	if err != nil {
		result.Stats.Duration = time.Since(startedAt)
		return result, fmt.Errorf("emit betfair replay candidates: %w", err)
	}
	result.Candidates = candidates
	result.Stats.CandidatesEmitted = len(candidates)
	result.Stats.CandidatesValid = len(candidates)

	result.PersistenceCandidates = make([]model.BetfairExchangeQuoteCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		result.PersistenceCandidates = append(result.PersistenceCandidates, candidate.PersistenceModel(opts.ReceivedAt))
	}
	result.Stats.Duration = time.Since(startedAt)
	return result, nil
}

func JSONLToCRLF(body []byte) []byte {
	body = bytes.ReplaceAll(body, []byte("\r\n"), []byte("\n"))
	body = bytes.ReplaceAll(body, []byte("\r"), []byte("\n"))

	var out bytes.Buffer
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		out.Write(line)
		out.WriteString("\r\n")
	}
	return out.Bytes()
}

func normalizeReplayOptions(opts ReplayOptions) (ReplayOptions, error) {
	if opts.Format == "" {
		opts.Format = ReplayFormatJSONL
	}
	switch opts.Format {
	case ReplayFormatJSONL, ReplayFormatCRLF:
	default:
		return ReplayOptions{}, fmt.Errorf("unsupported betfair replay format %q", opts.Format)
	}
	if opts.ReceivedAt.IsZero() {
		opts.ReceivedAt = time.Now().UTC()
	} else {
		opts.ReceivedAt = opts.ReceivedAt.UTC()
	}
	if opts.QuoteOptions.Now.IsZero() {
		opts.QuoteOptions.Now = opts.ReceivedAt
	}
	if opts.MaxMessageSize <= 0 {
		opts.MaxMessageSize = DefaultMaxMessageSize
	}
	return opts, nil
}

func replayInputReader(r io.Reader, format ReplayFormat) (io.Reader, error) {
	switch format {
	case ReplayFormatCRLF:
		return r, nil
	case ReplayFormatJSONL:
		body, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("read betfair jsonl replay input: %w", err)
		}
		return bytes.NewReader(JSONLToCRLF(body)), nil
	default:
		return nil, fmt.Errorf("unsupported betfair replay format %q", format)
	}
}
