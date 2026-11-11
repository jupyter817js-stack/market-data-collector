package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

func TestRunOnceUsesBoundedConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	release := make(chan struct{})
	collector := &fakeCollector{release: release}
	store := &fakeStore{}
	done := make(chan struct{})

	go func() {
		defer close(done)
		runOnce(ctx, collector, store, []string{"a", "b", "c", "d", "e"}, "secret", 2)
	}()

	waitForActive(t, collector, 2)
	if got := collector.maxActiveCount(); got > 2 {
		t.Fatalf("max active fetches=%d want <= 2", got)
	}
	close(release)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("runOnce did not finish")
	}

	if got := collector.fetchCount(); got != 5 {
		t.Fatalf("fetch count=%d want 5", got)
	}
	if got := collector.maxActiveCount(); got != 2 {
		t.Fatalf("max active fetches=%d want 2", got)
	}
}

func TestRunOnceStopsPendingWorkOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	collector := &fakeCollector{
		onFetch: func(string) {
			cancel()
		},
	}
	store := &fakeStore{}

	runOnce(ctx, collector, store, []string{"a", "b", "c"}, "secret", 1)

	if got := collector.fetchCount(); got != 1 {
		t.Fatalf("fetch count=%d want 1", got)
	}
}

func TestRunOnceSportFailureDoesNotStopOtherSports(t *testing.T) {
	collector := &fakeCollector{
		errBySport: map[string]error{
			"b": errors.New("boom"),
		},
	}
	store := &fakeStore{}

	runOnce(context.Background(), collector, store, []string{"a", "b", "c"}, "secret", 2)

	if got := collector.fetchCount(); got != 3 {
		t.Fatalf("fetch count=%d want 3", got)
	}
	if got := store.saveCount(); got != 2 {
		t.Fatalf("save count=%d want 2", got)
	}
	if got := store.runStatusCount(model.SourceRunStatusError); got != 1 {
		t.Fatalf("error source run count=%d want 1", got)
	}
	if got := store.runStatusCount(model.SourceRunStatusSuccess); got != 2 {
		t.Fatalf("success source run count=%d want 2", got)
	}
}

type fakeCollector struct {
	mu         sync.Mutex
	active     int
	maxActive  int
	fetched    []string
	errBySport map[string]error
	release    <-chan struct{}
	onFetch    func(string)
}

func (c *fakeCollector) Name() string { return "fake_source" }

func (c *fakeCollector) FetchOdds(ctx context.Context, sportKey string) (model.FetchBatch, error) {
	if c.onFetch != nil {
		c.onFetch(sportKey)
	}

	c.mu.Lock()
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.fetched = append(c.fetched, sportKey)
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()

	if c.release != nil {
		select {
		case <-c.release:
		case <-ctx.Done():
			return model.FetchBatch{Source: c.Name(), SportKey: sportKey}, ctx.Err()
		}
	}

	if err := c.errBySport[sportKey]; err != nil {
		return model.FetchBatch{Source: c.Name(), SportKey: sportKey, ReceivedAt: time.Now().UTC()}, err
	}
	return model.FetchBatch{
		Source:     c.Name(),
		SportKey:   sportKey,
		StatusCode: 200,
		RequestURL: "https://example.test/odds?apiKey=redacted",
		ReceivedAt: time.Now().UTC(),
		Payload:    []byte("[]"),
	}, nil
}

func (c *fakeCollector) fetchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.fetched)
}

func (c *fakeCollector) activeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active
}

func (c *fakeCollector) maxActiveCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxActive
}

type fakeStore struct {
	mu     sync.Mutex
	saves  []model.FetchBatch
	runs   []model.SourceRun
	saveFn func(context.Context, model.FetchBatch) (model.SaveStats, error)
}

func (s *fakeStore) SaveBatch(ctx context.Context, batch model.FetchBatch) (model.SaveStats, error) {
	if s.saveFn != nil {
		return s.saveFn(ctx, batch)
	}

	s.mu.Lock()
	s.saves = append(s.saves, batch)
	s.mu.Unlock()
	return model.SaveStats{
		RawSnapshotsInserted: 1,
		EventsSeen:           1,
		QuotesSeen:           1,
		QuotesInserted:       1,
	}, nil
}

func (s *fakeStore) InsertSourceRun(_ context.Context, run model.SourceRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs = append(s.runs, run)
	return nil
}

func (s *fakeStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.saves)
}

func (s *fakeStore) runStatusCount(status string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, run := range s.runs {
		if run.Status == status {
			count++
		}
	}
	return count
}

func waitForActive(t *testing.T, collector *fakeCollector, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if collector.activeCount() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("active fetches=%d want %d", collector.activeCount(), want)
}
