package betfair

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

type StreamSource interface {
	Open(ctx context.Context) (io.ReadCloser, error)
}

type MessageHandler interface {
	HandleMessage(ctx context.Context, msg Message) error
}

type MessageHandlerFunc func(ctx context.Context, msg Message) error

func (f MessageHandlerFunc) HandleMessage(ctx context.Context, msg Message) error {
	return f(ctx, msg)
}

type SupervisorConfig struct {
	MaxMessageSize int
	Staleness      StalenessConfig
	Now            func() time.Time
}

type Supervisor struct {
	source  StreamSource
	handler MessageHandler
	parser  func(io.Reader) *Parser
	tracker *HeartbeatTracker
	now     func() time.Time
}

func NewSupervisor(source StreamSource, handler MessageHandler, config SupervisorConfig) (*Supervisor, error) {
	if source == nil {
		return nil, fmt.Errorf("betfair stream source is required")
	}
	if handler == nil {
		return nil, fmt.Errorf("betfair message handler is required")
	}
	tracker, err := NewHeartbeatTracker(config.Staleness)
	if err != nil {
		return nil, err
	}
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Supervisor{
		source:  source,
		handler: handler,
		parser: func(r io.Reader) *Parser {
			return NewParser(r, config.MaxMessageSize)
		},
		tracker: tracker,
		now:     now,
	}, nil
}

func (s *Supervisor) Run(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stream, err := s.source.Open(ctx)
	if err != nil {
		return fmt.Errorf("open betfair stream source: %w", err)
	}
	defer func() { _ = stream.Close() }()

	parser := s.parser(stream)
	for {
		msg, err := parser.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		s.tracker.Observe(msg, s.now())
		if err := s.handler.HandleMessage(ctx, msg); err != nil {
			return fmt.Errorf("handle betfair stream message: %w", err)
		}
	}
}

func (s *Supervisor) State(now time.Time) StreamState {
	return s.tracker.State(now)
}

func (s *Supervisor) LastMessageTime() (time.Time, bool) {
	return s.tracker.LastMessageTime()
}

func (s *Supervisor) LastControlTime() (time.Time, bool) {
	return s.tracker.LastControlTime()
}

type ReaderSource struct {
	Reader io.Reader
}

func (s ReaderSource) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Reader == nil {
		return nil, fmt.Errorf("betfair reader source is nil")
	}
	if rc, ok := s.Reader.(io.ReadCloser); ok {
		return rc, nil
	}
	return io.NopCloser(s.Reader), nil
}
