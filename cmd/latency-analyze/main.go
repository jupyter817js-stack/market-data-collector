// cmd/latency-analyze is a one-shot CLI that drives the LSPORTS→Polymarket
// latency measurement analysis. It does NOT poll any external API and does
// NOT consume LSPORTS quota — it reads what cmd/lsports-inplay-capture and
// cmd/polymarket-latency-capture have already written to PostgreSQL.
//
// Modes (one per invocation):
//
//	--mode inspect-raw     Pretty-print the most recent raw_payload for a
//	                       fixture+kind. Use this RIGHT AFTER the match
//	                       goes InProgress to discover the actual JSON
//	                       shape of LSPORTS livescore, so we can refine
//	                       the upstream parser.
//
//	--mode detect-events   Scan lsports_inplay_observations (livescore)
//	                       for the fixture, insert inplay_decisive_events
//	                       at every score/period transition. Idempotent.
//
//	--mode compute-latency Join decisive events with PM price-change
//	                       observations within --window seconds, populate
//	                       latency_observations. Idempotent.
//
//	--mode report          Print the latency_summary view (median, p95,
//	                       p99, mean, stddev of Δt across all observed
//	                       events).
//
// Usage:
//
//	go run ./cmd/latency-analyze --mode inspect-raw --fixture-id 19012395 --kind livescore
//	go run ./cmd/latency-analyze --mode detect-events --fixture-id 19012395
//	go run ./cmd/latency-analyze --mode compute-latency --fixture-id 19012395 --window 300s
//	go run ./cmd/latency-analyze --mode report
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	var (
		mode      = flag.String("mode", "report", "inspect-raw | detect-events | compute-latency | report | source-comparison | backfill-scores")
		fixtureID = flag.Int64("fixture-id", 0, "LSPORTS fixture id (required except for --mode report and source-comparison without filter)")
		kind      = flag.String("kind", "livescore", "observation kind for inspect-raw: livescore | market | settlement")
		window    = flag.Duration("window", 300*time.Second, "search window for the FIRST PM reaction after each event")
		homePath  = flag.String("home-path", "", "backfill: comma-separated JSONB path to home score (e.g. Body,0,Livescore,Scoreboard,HomeScore)")
		awayPath  = flag.String("away-path", "", "backfill: comma-separated JSONB path to away score")
		periodPath = flag.String("period-path", "", "backfill: comma-separated JSONB path to current period")
		dedupWindow = flag.Duration("dedup-window", 5*time.Second, "detect-events: drop transitions within this duration of a previous transition (0 = disable). 5s catches LSPORTS data-revision flickers")
	)
	flag.Parse()

	_, _ = config.Load()

	dbURL := envOr("DATABASE_URL", "")
	if dbURL == "" {
		fail("DATABASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	st, err := store.Open(ctx, dbURL, false, normalize.FreshnessPolicy{})
	if err != nil {
		fail("open postgres: %v", err)
	}
	defer func() { _ = st.Close() }()

	switch *mode {
	case "inspect-raw":
		if *fixtureID == 0 {
			fail("--fixture-id required for inspect-raw")
		}
		runInspectRaw(ctx, st, *fixtureID, *kind)
	case "detect-events":
		if *fixtureID == 0 {
			fail("--fixture-id required for detect-events")
		}
		runDetectEvents(ctx, st, *fixtureID, *dedupWindow)
	case "compute-latency":
		if *fixtureID == 0 {
			fail("--fixture-id required for compute-latency")
		}
		runComputeLatency(ctx, st, *fixtureID, *window)
	case "report":
		runReport(ctx, st)
	case "source-comparison":
		runSourceComparison(ctx, st, *fixtureID)
	case "backfill-scores":
		if *fixtureID == 0 {
			fail("--fixture-id required for backfill-scores")
		}
		runBackfillScores(ctx, st, *fixtureID, *homePath, *awayPath, *periodPath)
	default:
		fail("--mode must be one of: inspect-raw | detect-events | compute-latency | report | source-comparison | backfill-scores (got %q)", *mode)
	}
}

func runInspectRaw(ctx context.Context, st *store.PostgresStore, fixtureID int64, kind string) {
	pretty, err := st.InspectRawObservation(ctx, fixtureID, kind)
	if err != nil {
		fail("inspect raw: %v", err)
	}
	fmt.Printf("Most recent %s raw_payload for fixture %d:\n\n", kind, fixtureID)
	fmt.Println(pretty)
}

func runDetectEvents(ctx context.Context, st *store.PostgresStore, fixtureID int64, dedupWindow time.Duration) {
	stats, err := st.DetectDecisiveEvents(ctx, fixtureID, dedupWindow)
	if err != nil {
		fail("detect events: %v", err)
	}
	fmt.Printf("fixture %d: livescore observations seen=%d, decisive events inserted (new this run)=%d (dedup_window=%s)\n",
		fixtureID, stats.ObservationsSeen, stats.Inserted, dedupWindow)
	if stats.ObservationsSeen > 0 && stats.Inserted == 0 {
		fmt.Println("note: 0 new events. Either all transitions already recorded, OR score/period columns")
		fmt.Println("      are not yet populated by the upstream parser. Run --mode inspect-raw to verify schema.")
	}
}

func runComputeLatency(ctx context.Context, st *store.PostgresStore, fixtureID int64, window time.Duration) {
	stats, err := st.ComputeLatencyObservations(ctx, fixtureID, window)
	if err != nil {
		fail("compute latency: %v", err)
	}
	fmt.Printf("fixture %d: decisive events known=%d, latency rows inserted (new)=%d, events with no PM reaction in %s window=%d\n",
		fixtureID, stats.EventsSeen, stats.Inserted, window, stats.NoReaction)
}

func runReport(ctx context.Context, st *store.PostgresStore) {
	rows, err := st.LatencySummary(ctx)
	if err != nil {
		fail("query summary: %v", err)
	}
	if len(rows) == 0 {
		fmt.Println("latency_summary is empty — no latency_observations populated yet.")
		fmt.Println("Run --mode detect-events then --mode compute-latency first.")
		return
	}
	fmt.Printf("%-22s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s\n",
		"method", "observed", "w_react", "w/o_react", "median_s", "p95_s", "p99_s", "mean_s", "stddev_s")
	fmt.Println(strings.Repeat("-", 130))
	for _, r := range rows {
		fmt.Printf("%-22s  %-10d  %-10d  %-10d  %-10s  %-10s  %-10s  %-10s  %-10s\n",
			r.Method,
			r.ObservationCount,
			r.WithReaction,
			r.WithoutReaction,
			fmtP(r.MedianLagSec),
			fmtP(r.P95LagSec),
			fmtP(r.P99LagSec),
			fmtP(r.MeanLagSec),
			fmtP(r.StddevLagSec),
		)
	}
}

func runSourceComparison(ctx context.Context, st *store.PostgresStore, fixtureID int64) {
	rows, err := st.SourceComparison(ctx, fixtureID)
	if err != nil {
		fail("source comparison: %v", err)
	}
	if len(rows) == 0 {
		fmt.Println("lsports_source_comparison: no rows. Both daemons must be writing for this view to populate.")
		return
	}
	fmt.Printf("%-12s  %-14s  %-9s  %-9s  %-14s  %-14s  %-14s  %-14s\n",
		"fixture", "kind", "snap_n", "amqp_n",
		"snap_avg_sec", "amqp_avg_sec", "snap_med_sec", "amqp_med_sec")
	fmt.Println(strings.Repeat("-", 130))
	for _, r := range rows {
		fmt.Printf("%-12d  %-14s  %-9d  %-9d  %-14s  %-14s  %-14s  %-14s\n",
			r.LSportsFixtureID, r.ObservationKind,
			r.SnapshotCount, r.AmqpCount,
			fmtP(r.SnapshotAvgPubToRecvSec), fmtP(r.AmqpAvgPubToRecvSec),
			fmtP(r.SnapshotMedPubToRecvSec), fmtP(r.AmqpMedPubToRecvSec),
		)
	}
	fmt.Println()
	fmt.Println("snap_avg_sec / amqp_avg_sec = LSPORTS publish_time → our local receive_at gap (smaller = faster path).")
	fmt.Println("If amqp < snap by a meaningful margin: AMQP push beats snapshot polling on LSPORTS-side latency.")
}

func runBackfillScores(ctx context.Context, st *store.PostgresStore, fixtureID int64, homePath, awayPath, periodPath string) {
	stats, err := st.BackfillScoresFromRaw(ctx, fixtureID, homePath, awayPath, periodPath)
	if err != nil {
		fail("backfill: %v", err)
	}
	fmt.Printf("fixture %d: livescore rows scanned=%d, updated=%d\n",
		fixtureID, stats.Scanned, stats.Updated)
	fmt.Println("Now re-run --mode detect-events to populate inplay_decisive_events from the newly-filled columns.")
}

func fmtP(p *float64) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprintf("%.3f", *p)
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
