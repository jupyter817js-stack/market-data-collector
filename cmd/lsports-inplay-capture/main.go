// cmd/lsports-inplay-capture polls the LSPORTS Snapshot API for every
// fixture currently open in the lsports_subscriptions ledger and appends
// one row per response to lsports_inplay_observations. It is the LSPORTS
// half of the latency-measurement infrastructure (see
// memory/latency_measurement_thesis.md).
//
// Polling cadence:
//   Default 1 s per endpoint per fixture, respecting the documented
//   LSPORTS rate limit "Filtered Requests... once per second" with a
//   global "1 call per second for all types combined". The daemon
//   serializes calls to stay under both caps. For multiple fixtures the
//   effective cadence per fixture degrades to (interval × call_count).
//
// What we poll per tick:
//   /InPlay/GetScores         (livescore: scores, period, status)
//   /InPlay/GetFixtureMarkets (markets / bets / prices)
//
// What we do NOT poll:
//   /InPlay/GetFixtures (static metadata; once per match is enough)
//   /InPlay/GetEvents   (overlaps Scores + FixtureMarkets)
//
// Quota cost: 0. The daemon only reads what is already subscribed.
//
// Usage:
//
//	# Continuous loop (default; SIGINT to stop):
//	go run ./cmd/lsports-inplay-capture
//
//	# Single tick then exit (useful for cron / smoke test):
//	go run ./cmd/lsports-inplay-capture --once
//
//	# Limit to one fixture (test mode):
//	go run ./cmd/lsports-inplay-capture --fixture-id 12345 --once
//
//	# Tune cadence:
//	go run ./cmd/lsports-inplay-capture --interval 2s
package main

import (
	"context"
	"encoding/json"
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
		intervalFlag  = flag.Duration("interval", 1*time.Second, "per-call sleep (LSPORTS filtered-request cap = 1/s)")
		once          = flag.Bool("once", false, "exit after one tick instead of looping")
		fixtureFilter = flag.Int64("fixture-id", 0, "if >0, poll only this fixture (must be in the open ledger)")
		captureKinds  = flag.String("kinds", "scores,markets", "comma-list of kinds to fetch: scores | markets")
	)
	flag.Parse()

	_, _ = config.Load()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	base := envOr("LSPORTS_BASE_URL", lsports.DefaultBaseURL)
	snapshotBase := envOr("LSPORTS_SNAPSHOT_BASE_URL", lsports.DefaultSnapshotBaseURL)
	user := strings.TrimSpace(os.Getenv("LSPORTS_USERNAME"))
	pw := os.Getenv("LSPORTS_PASSWORD")
	pkgStr := envOr("LSPORTS_INPLAY_PACKAGE_ID", "4044")
	dbURL := envOr("DATABASE_URL", "")

	pkg, err := strconv.Atoi(pkgStr)
	if err != nil {
		fail("LSPORTS_INPLAY_PACKAGE_ID must be integer: %v", err)
	}
	if user == "" || pw == "" {
		fail("LSPORTS_USERNAME and LSPORTS_PASSWORD are required")
	}
	if dbURL == "" {
		fail("DATABASE_URL is required (capture writes lsports_inplay_observations)")
	}

	kinds := parseKinds(*captureKinds)
	if len(kinds) == 0 {
		fail("--kinds resolved to empty set; must contain at least one of: scores, markets")
	}

	// metadataClient is used for /Fixtures/Get type calls if we ever extend;
	// the daemon currently only needs the snapshot host.
	_ = base
	snapshotClient := lsports.NewClient(snapshotBase, lsports.Credentials{
		PackageID: pkg,
		Username:  user,
		Password:  pw,
	}, 30*time.Second)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := store.Open(ctx, dbURL, false, normalize.FreshnessPolicy{})
	if err != nil {
		fail("open postgres: %v", err)
	}
	defer func() { _ = st.Close() }()

	// Diagnostic: print which database we actually connected to so we can
	// tell when DATABASE_URL points at the wrong place.
	if dbName, derr := store.CurrentDatabaseName(ctx, st); derr == nil {
		slog.Info("postgres connection", "database", dbName, "url_redacted", redactDBURL(dbURL))
	} else {
		slog.Warn("could not query current_database()", "error", derr)
	}

	slog.Info("lsports-inplay-capture started",
		"snapshot_base", snapshotBase,
		"package_id", pkg,
		"interval", intervalFlag.String(),
		"kinds", kinds,
		"once", *once,
		"fixture_filter", *fixtureFilter,
	)

	for {
		tickStart := time.Now()
		ids, err := st.ListOpenLSportsSubscriptionFixtureIDs(ctx)
		if err != nil {
			slog.Error("list open subscriptions", "error", err)
		} else if *fixtureFilter > 0 {
			ids = filterOnly(ids, *fixtureFilter)
		}

		if len(ids) == 0 {
			slog.Info("no open subscriptions; idling", "filter", *fixtureFilter)
		} else {
			runTick(ctx, snapshotClient, st, ids, kinds)
		}

		if *once {
			slog.Info("--once supplied; exiting after one tick", "elapsed", time.Since(tickStart).String())
			return
		}
		// Sleep until next tick boundary, respecting interval but bounded.
		elapsed := time.Since(tickStart)
		sleep := *intervalFlag - elapsed
		if sleep < 100*time.Millisecond {
			sleep = 100 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			slog.Info("ctx canceled; exiting")
			return
		case <-time.After(sleep):
		}
	}
}

func runTick(ctx context.Context, c *lsports.Client, st *store.PostgresStore, fixtureIDs []int64, kinds []string) {
	for _, fid := range fixtureIDs {
		for _, kind := range kinds {
			if err := ctx.Err(); err != nil {
				return
			}
			switch kind {
			case "scores":
				if err := captureScores(ctx, c, st, fid); err != nil {
					slog.Error("scores capture", "fixture_id", fid, "error", err)
				}
			case "markets":
				if err := captureMarkets(ctx, c, st, fid); err != nil {
					slog.Error("markets capture", "fixture_id", fid, "error", err)
				}
			}
			// Serialize all snapshot calls — LSPORTS global cap is 1/s.
			select {
			case <-ctx.Done():
				return
			case <-time.After(1100 * time.Millisecond):
			}
		}
	}
}

func captureScores(ctx context.Context, c *lsports.Client, st *store.PostgresStore, fid int64) error {
	resp, err := c.GetScores(ctx, lsports.SnapshotFilter{Fixtures: []int{int(fid)}})
	if err != nil {
		return err
	}
	receivedAt := time.Now().UTC()
	srcPub := resp.Header.SnapshotServerTime()
	obs := model.LSportsInplayObservation{
		LSportsFixtureID: fid,
		Source:           "snapshot",
		ObservationKind:  "livescore",
		ReceivedAt:       &receivedAt,
		RawPayload:       resp.Raw,
	}
	if !srcPub.IsZero() {
		obs.SourcePublishTime = &srcPub
	}
	// Best-effort score/period extraction. The snapshot Body shape is not in
	// public docs; we probe for common field names. Even if we miss them the
	// raw_payload still allows post-hoc analysis.
	extractLivescoreFacets(resp.Raw, &obs)
	if _, err := st.InsertLSportsInplayObservation(ctx, obs); err != nil {
		return err
	}
	slog.Info("captured livescore",
		"fixture_id", fid,
		"home", optInt(obs.HomeScore), "away", optInt(obs.AwayScore),
		"period", optInt(obs.CurrentPeriod),
		"server_time", formatTimePtr(obs.SourcePublishTime),
	)
	return nil
}

func captureMarkets(ctx context.Context, c *lsports.Client, st *store.PostgresStore, fid int64) error {
	resp, err := c.GetFixtureMarkets(ctx, lsports.SnapshotFilter{Fixtures: []int{int(fid)}})
	if err != nil {
		return err
	}
	receivedAt := time.Now().UTC()
	srcPub := resp.Header.SnapshotServerTime()
	obs := model.LSportsInplayObservation{
		LSportsFixtureID: fid,
		Source:           "snapshot",
		ObservationKind:  "market",
		ReceivedAt:       &receivedAt,
		RawPayload:       resp.Raw,
	}
	if !srcPub.IsZero() {
		obs.SourcePublishTime = &srcPub
	}
	if _, err := st.InsertLSportsInplayObservation(ctx, obs); err != nil {
		return err
	}
	slog.Info("captured market", "fixture_id", fid, "server_time", formatTimePtr(obs.SourcePublishTime))
	return nil
}

// liveScoreItem models the actual LSPORTS livescore event shape observed
// from a live AMQP capture on 2026-05-28:
//
//	{
//	  "FixtureId": 19033944,
//	  "Livescore": {
//	    "Scoreboard": {
//	      "Status": 3,
//	      "CurrentPeriod": 100,
//	      "Time": "5640",
//	      "Results": [
//	        {"Position":"1","Value":"1"},   // home score
//	        {"Position":"2","Value":"0"}    // away score
//	      ]
//	    },
//	    "Periods": [ ... ]
//	  }
//	}
//
// Scoreboard.Results is the canonical scorecard; Position 1 = home, 2 = away.
type liveScoreItem struct {
	FixtureID int64 `json:"FixtureId"`
	Livescore *struct {
		Scoreboard *struct {
			Status        int    `json:"Status"`
			CurrentPeriod int    `json:"CurrentPeriod"`
			Time          string `json:"Time"`
			Results       []struct {
				Position string `json:"Position"`
				Value    string `json:"Value"`
			} `json:"Results"`
		} `json:"Scoreboard"`
	} `json:"Livescore"`
}

// extractLivescoreFacets extracts home/away score, period, and status from
// the LSPORTS livescore payload's actual on-the-wire shape. Handles the
// three observed envelope variants:
//
//	{"Body": [{...}]}                       <- Snapshot REST: array directly
//	{"Body": {"Events": [{...}]}}           <- AMQP push: Events wrapper
//	{"Body": {...}}                         <- single-object fallback
//
// Score values use the Position/Value discriminator pattern, where
// Position "1" is home and Position "2" is away. Failure is non-fatal —
// raw_payload always carries the full data for post-hoc backfill via
// cmd/latency-analyze --mode backfill-scores.
func extractLivescoreFacets(raw json.RawMessage, obs *model.LSportsInplayObservation) {
	var envelope struct {
		Body json.RawMessage `json:"Body"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return
	}
	if len(envelope.Body) == 0 {
		return
	}

	// Variant 1: AMQP push — {"Body": {"Events": [{...}]}}
	var amqpShape struct {
		Events []liveScoreItem `json:"Events"`
	}
	if err := json.Unmarshal(envelope.Body, &amqpShape); err == nil && len(amqpShape.Events) > 0 {
		applyLiveScoreItem(amqpShape.Events[0], obs)
		return
	}

	// Variant 2: Snapshot REST — {"Body": [{...}]}
	var arrShape []liveScoreItem
	if err := json.Unmarshal(envelope.Body, &arrShape); err == nil && len(arrShape) > 0 {
		applyLiveScoreItem(arrShape[0], obs)
		return
	}

	// Variant 3: Single object — {"Body": {...}}
	var oneShape liveScoreItem
	if err := json.Unmarshal(envelope.Body, &oneShape); err == nil && oneShape.Livescore != nil {
		applyLiveScoreItem(oneShape, obs)
	}
}

// applyLiveScoreItem mutates obs to reflect a parsed livescore item. Score
// values are string-encoded ("1", "0") and use the Position discriminator.
func applyLiveScoreItem(item liveScoreItem, obs *model.LSportsInplayObservation) {
	if item.Livescore == nil || item.Livescore.Scoreboard == nil {
		return
	}
	sb := item.Livescore.Scoreboard

	for _, r := range sb.Results {
		v, err := strconv.Atoi(strings.TrimSpace(r.Value))
		if err != nil {
			continue
		}
		switch strings.TrimSpace(r.Position) {
		case "1":
			obs.HomeScore = &v
		case "2":
			obs.AwayScore = &v
		}
	}
	if sb.CurrentPeriod > 0 {
		cp := sb.CurrentPeriod
		obs.CurrentPeriod = &cp
	}
	if sb.Status > 0 {
		s := sb.Status
		obs.MatchStatus = &s
	}
}

func parseKinds(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, p := range parts {
		k := strings.ToLower(strings.TrimSpace(p))
		if k == "" || seen[k] {
			continue
		}
		switch k {
		case "scores", "markets":
			out = append(out, k)
			seen[k] = true
		default:
			fmt.Fprintf(os.Stderr, "warning: unknown --kinds entry %q ignored (allowed: scores, markets)\n", k)
		}
	}
	return out
}

func filterOnly(ids []int64, only int64) []int64 {
	for _, id := range ids {
		if id == only {
			return []int64{only}
		}
	}
	return nil
}

// redactDBURL strips the password from a postgres:// URL so we can log the
// host/db without leaking credentials.
func redactDBURL(u string) string {
	at := strings.LastIndex(u, "@")
	if at < 0 {
		return u
	}
	colon := strings.Index(u[:at], "//")
	if colon < 0 {
		return u
	}
	colon += 2
	for i := colon; i < at; i++ {
		if u[i] == ':' {
			return u[:i+1] + "***@" + u[at+1:]
		}
	}
	return u
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func optInt(p *int) int {
	if p == nil {
		return -1
	}
	return *p
}

func formatTimePtr(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.Format(time.RFC3339Nano)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
