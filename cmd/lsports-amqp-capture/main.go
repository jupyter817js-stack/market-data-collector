// cmd/lsports-amqp-capture is the Phase D push consumer for LSPORTS
// TRADE360 in-play. It connects to RabbitMQ (stm-inplay.lsports.eu:5672,
// vhost StmInPlay, queue "_<PackageID>_"), pulls messages, and persists
// each one to lsports_inplay_observations with source='amqp_push'.
//
// Compared to cmd/lsports-inplay-capture (REST snapshot polling), this is
// the latency-side comparator: by writing both 'snapshot' and 'amqp_push'
// rows for the same fixture, we can later compute how much faster the
// push channel was than polling. See migration 049 view
// lsports_source_comparison.
//
// Reconnect policy: on disconnection, exponential backoff (1s → 30s) and
// retry indefinitely. SIGINT/SIGTERM triggers graceful shutdown after the
// in-flight message is ack'd.
//
// Usage:
//
//	go run ./cmd/lsports-amqp-capture
//	go run ./cmd/lsports-amqp-capture --fixture-id 19012395  # filter by fixture
//
// Required env:
//
//	LSPORTS_USERNAME, LSPORTS_PASSWORD
//	LSPORTS_INPLAY_PACKAGE_ID    (default 4044)
//	LSPORTS_AMQP_HOST            (default stm-inplay.lsports.eu)
//	LSPORTS_AMQP_PORT            (default 5672)
//	LSPORTS_AMQP_VHOST           (default StmInPlay)
//	DATABASE_URL
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/lsports"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	var (
		fixtureFilter = flag.Int64("fixture-id", 0, "if >0, only persist messages for this fixture; others are ack'd and dropped")
		consumerTag   = flag.String("consumer-tag", "pol-odds-collector-amqp", "AMQP consumer tag")
		minBackoff    = flag.Duration("min-backoff", 1*time.Second, "minimum reconnect backoff")
		maxBackoff    = flag.Duration("max-backoff", 30*time.Second, "maximum reconnect backoff")
		heartbeat     = flag.Duration("heartbeat", 10*time.Second, "AMQP heartbeat interval (0 = library default)")
		probeOnly     = flag.Bool("probe-only", false, "attempt ONE connection and exit; for credential testing (no message consumption, no DB writes)")
	)
	flag.Parse()

	_, _ = config.Load()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	user := strings.TrimSpace(os.Getenv("LSPORTS_USERNAME"))
	pw := os.Getenv("LSPORTS_PASSWORD")
	pkgStr := envOr("LSPORTS_INPLAY_PACKAGE_ID", "4044")
	host := envOr("LSPORTS_AMQP_HOST", lsports.DefaultAMQPHost)
	portStr := envOr("LSPORTS_AMQP_PORT", strconv.Itoa(lsports.DefaultAMQPPort))
	vhost := envOr("LSPORTS_AMQP_VHOST", lsports.DefaultAMQPVHost)
	dbURL := envOr("DATABASE_URL", "")

	pkg, err := strconv.Atoi(pkgStr)
	if err != nil {
		fail("LSPORTS_INPLAY_PACKAGE_ID must be integer: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		fail("LSPORTS_AMQP_PORT must be integer: %v", err)
	}
	if user == "" || pw == "" {
		fail("LSPORTS_USERNAME and LSPORTS_PASSWORD required")
	}
	if dbURL == "" {
		fail("DATABASE_URL required")
	}

	queue := fmt.Sprintf("_%d_", pkg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := lsports.AMQPConfig{
		Host:      host,
		Port:      port,
		Username:  user,
		Password:  pw,
		VHost:     vhost,
		Queue:     queue,
		Heartbeat: *heartbeat,
	}

	if *probeOnly {
		slog.Info("amqp probe starting",
			"host", cfg.Host, "port", cfg.Port, "vhost", cfg.VHost,
			"queue", cfg.Queue, "user", "<redacted>",
		)
		consumer := lsports.NewAMQPConsumer(cfg)
		if err := consumer.Connect(ctx); err != nil {
			slog.Error("amqp connect FAILED", "error", err)
			os.Exit(2)
		}
		slog.Info("amqp connect OK — credentials accepted")
		_ = consumer.Close()
		return
	}

	st, err := store.Open(ctx, dbURL, false, normalize.FreshnessPolicy{})
	if err != nil {
		fail("open postgres: %v", err)
	}
	defer func() { _ = st.Close() }()

	slog.Info("lsports-amqp-capture starting",
		"host", cfg.Host, "port", cfg.Port, "vhost", cfg.VHost, "queue", cfg.Queue,
		"fixture_filter", *fixtureFilter,
	)

	backoff := *minBackoff
	for {
		if err := runOnce(ctx, st, cfg, *consumerTag, *fixtureFilter); err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("ctx canceled; exiting")
				return
			}
			slog.Error("consumer loop exited", "error", err, "next_backoff", backoff.String())
		} else {
			slog.Info("consumer loop ended cleanly; reconnecting")
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > *maxBackoff {
			backoff = *maxBackoff
		}
	}
}

func runOnce(ctx context.Context, st *store.PostgresStore, cfg lsports.AMQPConfig, consumerTag string, fixtureFilter int64) error {
	consumer := lsports.NewAMQPConsumer(cfg)
	if err := consumer.Connect(ctx); err != nil {
		return err
	}
	defer func() { _ = consumer.Close() }()
	slog.Info("amqp connected", "queue", cfg.Queue)

	deliveries, err := consumer.Consume(ctx, consumerTag)
	if err != nil {
		return fmt.Errorf("start consume: %w", err)
	}
	closeNotify := consumer.CloseNotify()

	persistedCount := 0
	skippedCount := 0
	noFidLogged := 0
	const noFidLogLimit = 5
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case amqpErr := <-closeNotify:
			if amqpErr == nil {
				return errors.New("connection closed (nil error)")
			}
			return fmt.Errorf("connection closed: %s", amqpErr.Error())
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			msg, perr := lsports.ParseAMQPMessage(d)
			if perr != nil {
				slog.Warn("parse failed; nack+requeue", "error", perr)
				_ = d.Nack(false, true)
				continue
			}

			fid := lsports.FixtureIDFromMessage(d.Body)
			if fixtureFilter > 0 && fid != fixtureFilter {
				skippedCount++
				_ = d.Ack(false)
				if skippedCount%100 == 0 {
					slog.Info("filtered out", "skipped_total", skippedCount, "filter", fixtureFilter)
				}
				continue
			}

			// Skip keepalive/heartbeat persistence — high volume, low value.
			// We still ack them.
			if msg.ObservationKind == "keepalive" || msg.ObservationKind == "heartbeat" {
				_ = d.Ack(false)
				continue
			}

			// Persist with fid (may be 0 if extraction failed). The schema
			// requires fixture_id > 0 so we skip rows we can't attribute.
			if fid == 0 {
				// CRITICAL DIAGNOSTIC: log the actual raw body for the first
				// few occurrences so the operator can see the real shape and
				// update FixtureIDFromMessage. After noFidLogLimit suppress
				// to avoid log spam.
				if noFidLogged < noFidLogLimit {
					body := string(d.Body)
					if len(body) > 1500 {
						body = body[:1500] + "...[truncated]"
					}
					slog.Warn("no fixture id extracted; sample raw body",
						"kind", msg.ObservationKind, "type", msg.Header.Type,
						"raw", body,
					)
					noFidLogged++
				} else {
					if noFidLogged == noFidLogLimit {
						slog.Warn("suppressing further no-fixture-id warnings",
							"already_logged", noFidLogged,
						)
						noFidLogged++
					}
				}
				_ = d.Ack(false)
				continue
			}

			obs := model.LSportsInplayObservation{
				LSportsFixtureID: fid,
				Source:           "amqp_push",
				ObservationKind:  msg.ObservationKind,
				ReceivedAt:       &msg.ReceivedAt,
				RawPayload:       msg.RawPayload,
			}
			if t := msg.Header.SourcePublishTime(); !t.IsZero() {
				obs.SourcePublishTime = &t
			}

			if _, ierr := st.InsertLSportsInplayObservation(ctx, obs); ierr != nil {
				slog.Error("persist failed; nack+requeue", "error", ierr,
					"fixture_id", fid, "kind", msg.ObservationKind)
				_ = d.Nack(false, true)
				continue
			}
			_ = d.Ack(false)
			persistedCount++
			if persistedCount%50 == 0 {
				slog.Info("persisted",
					"persisted_total", persistedCount, "skipped_total", skippedCount,
					"last_fixture", fid, "last_kind", msg.ObservationKind,
				)
			}
		}
	}
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
