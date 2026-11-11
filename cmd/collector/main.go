package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/health"
	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source"
	"github.com/your-org/pol-odds-collector/internal/source/oddsapi"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const maxTelemetryErrorMessage = 1000

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}
	// As of 2026-05-17 this legacy Odds-API collector is retained for
	// optional pre-match analytics only. The live pipeline runs through
	// cmd/pinnacle-collector + cmd/polymarket-sync. Fail loudly here if the
	// Odds-API-specific env vars are missing so users don't run this thinking
	// it's the live path.
	if err := config.MustOddsAPIConfig(cfg); err != nil {
		slog.Error("cmd/collector requires Odds API env (ODDS_API_KEY, SPORT_KEYS, REGIONS or BOOKMAKERS, MARKETS). For live in-play use cmd/pinnacle-collector instead.",
			"error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL, cfg.PrematchOnly, normalize.FreshnessPolicy{
		FreshAfter: cfg.FreshAfter,
		AgingAfter: cfg.AgingAfter,
	})
	if err != nil {
		slog.Error("open postgres", "error", err)
		os.Exit(1)
	}
	defer func() { _ = st.Close() }()

	healthServer, err := startHealthServer(cfg.HealthAddr, st)
	if err != nil {
		slog.Error("start health server", "addr", cfg.HealthAddr, "error", err)
		os.Exit(1)
	}

	collector := oddsapi.NewClientWithRetry(
		cfg.OddsAPIKey,
		cfg.OddsAPIBaseURL,
		cfg.Regions,
		cfg.Markets,
		cfg.Bookmakers,
		cfg.HTTPTimeout,
		oddsapi.RetryConfig{
			MaxRetries: cfg.OddsAPIMaxRetries,
			BaseDelay:  cfg.OddsAPIRetryBase,
			MaxDelay:   cfg.OddsAPIRetryMax,
		},
	)

	slog.Info("collector started",
		"source", collector.Name(),
		"sports", cfg.SportKeys,
		"markets", cfg.Markets,
		"regions", cfg.Regions,
		"prematch_only", cfg.PrematchOnly,
		"poll_interval", cfg.PollInterval.String(),
		"max_concurrent_sports", cfg.MaxConcurrentSports,
		"health_addr", cfg.HealthAddr,
	)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		runOnce(ctx, collector, st, cfg.SportKeys, cfg.OddsAPIKey, cfg.MaxConcurrentSports)

		select {
		case <-ctx.Done():
			slog.Info("collector stopped")
			shutdownHealthServer(healthServer)
			return
		case <-ticker.C:
		}
	}
}

type batchStore interface {
	SaveBatch(ctx context.Context, batch model.FetchBatch) (model.SaveStats, error)
	InsertSourceRun(ctx context.Context, run model.SourceRun) error
}

func startHealthServer(addr string, st *store.PostgresStore) (*http.Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	server := &http.Server{
		Handler:           health.NewHandler(st, time.Second),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("health server started", "addr", listener.Addr().String())
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("health server stopped unexpectedly", "error", err)
		}
	}()

	return server, nil
}

func shutdownHealthServer(server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("shutdown health server", "error", err)
		return
	}
	slog.Info("health server stopped")
}

func runOnce(ctx context.Context, collector source.Collector, st batchStore, sportKeys []string, oddsAPIKey string, maxConcurrentSports int) {
	if maxConcurrentSports < 1 {
		maxConcurrentSports = 1
	}

	sem := make(chan struct{}, maxConcurrentSports)
	var wg sync.WaitGroup

loop:
	for _, sportKey := range sportKeys {
		if ctx.Err() != nil {
			break
		}

		select {
		case sem <- struct{}{}:
			if ctx.Err() != nil {
				<-sem
				break loop
			}
		case <-ctx.Done():
			break loop
		}

		sportKey := sportKey
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			runSport(ctx, collector, st, sportKey, oddsAPIKey)
		}()
	}

	wg.Wait()
}

func runSport(ctx context.Context, collector source.Collector, st batchStore, sportKey string, oddsAPIKey string) {
	startedAt := time.Now().UTC()
	run := model.SourceRun{
		Source:    collector.Name(),
		SportKey:  sportKey,
		StartedAt: startedAt,
		Status:    model.SourceRunStatusSuccess,
	}

	batch, err := collector.FetchOdds(ctx, sportKey)
	if err != nil {
		run.StatusCode = statusCodePtr(batch.StatusCode)
		run.RequestURL = redactSecret(batch.RequestURL, oddsAPIKey)
		run.ResponseHeaders = batch.Headers
		telemetryErr := err

		if len(batch.Payload) > 0 {
			stats, saveErr := st.SaveBatch(ctx, batch)
			run.Stats = stats
			if saveErr != nil {
				telemetryErr = joinErrors(err, saveErr)
				slog.Error("save failed api response", "sport", sportKey, "fetch_error", err, "save_error", saveErr)
			}
		}
		run.FinishedAt = time.Now().UTC()
		run.Status = sourceRunStatus(ctx, telemetryErr)
		run.ErrorMessage = safeTelemetryError(telemetryErr, oddsAPIKey)
		recordSourceRun(st, run)
		if !errors.Is(err, context.Canceled) {
			slog.Error("fetch odds", "source", collector.Name(), "sport", sportKey, "error", err)
		}
		return
	}

	stats, err := st.SaveBatch(ctx, batch)
	run.FinishedAt = time.Now().UTC()
	run.StatusCode = statusCodePtr(batch.StatusCode)
	run.RequestURL = redactSecret(batch.RequestURL, oddsAPIKey)
	run.ResponseHeaders = batch.Headers
	run.Stats = stats
	if err != nil {
		run.Status = sourceRunStatus(ctx, err)
		run.ErrorMessage = safeTelemetryError(err, oddsAPIKey)
		recordSourceRun(st, run)
		slog.Error("save batch", "source", collector.Name(), "sport", sportKey, "error", err)
		return
	}

	recordSourceRun(st, run)
	slog.Info("batch saved",
		"source", collector.Name(),
		"sport", sportKey,
		"events_seen", stats.EventsSeen,
		"events_skipped_live", stats.EventsSkippedLive,
		"quotes_seen", stats.QuotesSeen,
		"quotes_inserted", stats.QuotesInserted,
		"quotes_unchanged", stats.QuotesUnchanged,
		"raw_snapshots_inserted", stats.RawSnapshotsInserted,
	)
}

func recordSourceRun(st batchStore, run model.SourceRun) {
	if run.FinishedAt.IsZero() {
		run.FinishedAt = time.Now().UTC()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := st.InsertSourceRun(ctx, run); err != nil {
		slog.Error("insert source run telemetry", "source", run.Source, "sport", run.SportKey, "status", run.Status, "error", err)
	}
}

func sourceRunStatus(ctx context.Context, err error) string {
	if err == nil {
		return model.SourceRunStatusSuccess
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		return model.SourceRunStatusCanceled
	}
	return model.SourceRunStatusError
}

func statusCodePtr(statusCode int) *int {
	if statusCode == 0 {
		return nil
	}
	return &statusCode
}

func safeTelemetryError(err error, secret string) string {
	if err == nil {
		return ""
	}
	return truncateRunError(redactSecret(err.Error(), secret))
}

func joinErrors(primary, secondary error) error {
	if secondary == nil {
		return primary
	}
	if primary == nil {
		return secondary
	}
	return errors.Join(primary, secondary)
}

func redactSecret(s, secret string) string {
	if secret == "" {
		return s
	}
	s = strings.ReplaceAll(s, secret, "redacted")
	s = strings.ReplaceAll(s, url.QueryEscape(secret), "redacted")
	s = strings.ReplaceAll(s, url.PathEscape(secret), "redacted")
	return s
}

func truncateRunError(s string) string {
	runes := []rune(s)
	if len(runes) <= maxTelemetryErrorMessage {
		return s
	}
	return string(runes[:maxTelemetryErrorMessage])
}
