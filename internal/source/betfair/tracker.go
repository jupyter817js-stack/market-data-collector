package betfair

import (
	"fmt"
	"time"
)

type StreamState string

const (
	StreamStateUnknown StreamState = "unknown"
	StreamStateHealthy StreamState = "healthy"
	StreamStateAging   StreamState = "aging"
	StreamStateStale   StreamState = "stale"
)

type StalenessConfig struct {
	HealthyAfter time.Duration
	StaleAfter   time.Duration
}

type HeartbeatTracker struct {
	config      StalenessConfig
	lastMessage time.Time
	lastControl time.Time
}

func NewHeartbeatTracker(config StalenessConfig) (*HeartbeatTracker, error) {
	if config.HealthyAfter <= 0 {
		return nil, fmt.Errorf("healthy threshold must be positive")
	}
	if config.StaleAfter <= 0 {
		return nil, fmt.Errorf("stale threshold must be positive")
	}
	if config.StaleAfter < config.HealthyAfter {
		return nil, fmt.Errorf("stale threshold must be >= healthy threshold")
	}
	return &HeartbeatTracker{config: config}, nil
}

func (t *HeartbeatTracker) Observe(msg Message, receivedAt time.Time) {
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}
	receivedAt = receivedAt.UTC()
	t.lastMessage = receivedAt
	if msg.IsControl() {
		t.lastControl = receivedAt
	}
}

func (t *HeartbeatTracker) State(now time.Time) StreamState {
	if t.lastMessage.IsZero() {
		return StreamStateUnknown
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	age := now.UTC().Sub(t.lastMessage)
	if age < 0 {
		age = 0
	}
	switch {
	case age <= t.config.HealthyAfter:
		return StreamStateHealthy
	case age <= t.config.StaleAfter:
		return StreamStateAging
	default:
		return StreamStateStale
	}
}

func (t *HeartbeatTracker) LastMessageTime() (time.Time, bool) {
	if t.lastMessage.IsZero() {
		return time.Time{}, false
	}
	return t.lastMessage, true
}

func (t *HeartbeatTracker) LastControlTime() (time.Time, bool) {
	if t.lastControl.IsZero() {
		return time.Time{}, false
	}
	return t.lastControl, true
}
