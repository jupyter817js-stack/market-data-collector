// Package auto implements LSPORTS-side fixture discovery + Polymarket
// event matching for LSPORTS-driven paper baseball trading. It calls
// LSPORTS InPlay/GetEvents on a polling cadence, parses live baseball
// fixtures, matches each one to a Polymarket Gamma event by hardcoded
// MLB team-name → PM slug short-code mapping, and (when the LSPORTS
// Subscribe + Trial-access verify succeeds) notifies a caller-supplied
// callback so the paper-baseball daemon can begin emitting signals.
//
// This package does NOT execute trades, does NOT handle wallets, and
// does NOT call Polymarket order endpoints.
package auto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/source/lsports"
	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
	"github.com/your-org/pol-odds-collector/internal/store"
)

const BaseballSportID = 154

type DiscoveryConfig struct {
	LSPORTSMeta       *lsports.Client // metadata + subscribe host (stm-api)
	LSPORTSSnapshot   *lsports.Client // snapshot host (stm-snapshot)
	PMGamma           *polymarket.Client
	DB                *store.PostgresStore
	PollInterval      time.Duration // default 5m
	TrialVerifyWindow time.Duration // default 60s — must see AMQP livescore within
	BaseballSportID   int           // default 154
	Logger            *slog.Logger
}

// DiscoveredFixture is the fully-resolved fixture handed off to the
// paper-baseball consumer to begin emitting signals.
type DiscoveredFixture struct {
	FixtureID     int64
	HomeTeam      string
	AwayTeam      string
	GameDate      string // YYYY-MM-DD UTC
	PMSlug        string
	PMEventID     string
	PMConditionID string
	PMMarketID    string
	PMTokenIDs    [2]string
}

// AddCallback is invoked when a new fixture passes both Gamma matching
// and Trial-access verification. The paper-baseball daemon supplies a
// callback that mutates its in-memory consumer map under a mutex.
type AddCallback func(d DiscoveredFixture)

// Run blocks until ctx is canceled. It polls LSPORTS InPlay/GetEvents
// every cfg.PollInterval, and for each newly seen baseball fixture
// attempts the full match-and-verify pipeline. The `known` map tracks
// fixtures we've considered (whether accepted or skipped) so we don't
// retry them every tick.
func Run(ctx context.Context, cfg DiscoveryConfig, addCb AddCallback) error {
	if cfg.LSPORTSSnapshot == nil || cfg.LSPORTSMeta == nil || cfg.PMGamma == nil || cfg.DB == nil {
		return errors.New("auto.Run: LSPORTSMeta, LSPORTSSnapshot, PMGamma, DB required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Minute
	}
	if cfg.TrialVerifyWindow <= 0 {
		cfg.TrialVerifyWindow = 60 * time.Second
	}
	if cfg.BaseballSportID == 0 {
		cfg.BaseballSportID = BaseballSportID
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	known := map[int64]string{} // fid -> outcome (accepted | skipped:<reason>)
	tick := time.NewTicker(cfg.PollInterval)
	defer tick.Stop()
	// run once immediately
	pollOnce(ctx, cfg, known, addCb)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			pollOnce(ctx, cfg, known, addCb)
		}
	}
}

func pollOnce(ctx context.Context, cfg DiscoveryConfig, known map[int64]string, addCb AddCallback) {
	// Sports filter (per LSPORTS docs) drops the rate limit from
	// 1/15s (unfiltered) to 1/s (filtered) and also trims the response
	// to baseball only.
	resp, err := cfg.LSPORTSSnapshot.GetInPlayEvents(ctx, lsports.SnapshotFilter{
		Sports: []int{cfg.BaseballSportID},
	})
	if err != nil {
		cfg.Logger.Warn("auto-discover: InPlay/GetEvents failed", "error", err)
		return
	}
	fixtures := parseInPlayEvents(resp.Raw, cfg.BaseballSportID)
	cfg.Logger.Info("auto-discover: poll result",
		"baseball_fixtures_seen", len(fixtures),
		"response_bytes", len(resp.Raw),
	)
	if len(fixtures) == 0 {
		return
	}

	for _, f := range fixtures {
		if _, seen := known[f.FixtureID]; seen {
			continue
		}
		cfg.Logger.Info("auto-discover: new baseball fixture seen",
			"fixture_id", f.FixtureID, "home", f.HomeTeam, "away", f.AwayTeam,
		)

		outcome := tryFixture(ctx, cfg, &f, addCb)
		known[f.FixtureID] = outcome
		cfg.Logger.Info("auto-discover: fixture decision",
			"fixture_id", f.FixtureID, "outcome", outcome,
		)
	}
}

// tryFixture runs the full match-and-verify pipeline for one fixture.
// Returns a short string outcome for the `known` map so subsequent polls
// don't retry.
func tryFixture(ctx context.Context, cfg DiscoveryConfig, f *DiscoveredFixture, addCb AddCallback) string {
	// 1. Build PM slug from team names.
	gameDate := time.Now().UTC().Format("2006-01-02")
	f.GameDate = gameDate
	slug := polymarket.BuildMLBEventSlug(f.HomeTeam, f.AwayTeam, gameDate)
	if slug == "" {
		// team mapping unknown — try yesterday's date too (game crossed midnight UTC)
		altDate := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
		slug = polymarket.BuildMLBEventSlug(f.HomeTeam, f.AwayTeam, altDate)
		if slug == "" {
			return "skipped:team_mapping_unknown"
		}
		f.GameDate = altDate
	}
	f.PMSlug = slug

	// 2. Gamma fetch + winner-market pick.
	ev, ok, err := cfg.PMGamma.GetEventBySlug(ctx, slug)
	if err != nil {
		return "skipped:gamma_error:" + truncErr(err)
	}
	if !ok {
		// Try date-swap (PM may post slugs by Eastern Time date, not UTC).
		altDate := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
		if altDate != f.GameDate {
			altSlug := polymarket.BuildMLBEventSlug(f.HomeTeam, f.AwayTeam, altDate)
			ev, ok, _ = cfg.PMGamma.GetEventBySlug(ctx, altSlug)
			if ok {
				f.GameDate = altDate
				f.PMSlug = altSlug
				slug = altSlug
			}
		}
	}
	if !ok {
		return "skipped:gamma_not_found:" + slug
	}
	mkt, ok := polymarket.PickWinnerMarket(ev, f.HomeTeam, f.AwayTeam)
	if !ok {
		return "skipped:winner_market_not_found"
	}
	f.PMEventID = ev.ID
	f.PMConditionID = mkt.ConditionID
	f.PMMarketID = mkt.ID
	if len(mkt.Tokens) >= 2 {
		f.PMTokenIDs[0] = mkt.Tokens[0].TokenID
		f.PMTokenIDs[1] = mkt.Tokens[1].TokenID
	}

	// 3. Trial-access verify: if we are already receiving AMQP livescore
	// for this fixture, Trial OK. Otherwise Subscribe + wait. (We assume
	// amqp-capture is the lone AMQP consumer providing the persistence
	// so the DB query is authoritative.)
	if cnt, _ := cfg.DB.CountRecentAMQPLivescore(ctx, f.FixtureID, cfg.TrialVerifyWindow); cnt > 0 {
		// already subscribed and receiving data — no need to Subscribe again
		cfg.Logger.Info("auto-discover: fixture already producing AMQP livescore",
			"fixture_id", f.FixtureID, "recent_count", cnt,
		)
	} else {
		// Attempt subscribe. If it fails (already subscribed, quota, etc.) log and skip.
		if _, err := cfg.LSPORTSMeta.Subscribe(ctx, int(f.FixtureID)); err != nil {
			// "already subscribed" is OK; everything else is fatal for this fixture.
			if !strings.Contains(strings.ToLower(err.Error()), "already") {
				return "skipped:subscribe_failed:" + truncErr(err)
			}
		}
		cfg.Logger.Info("auto-discover: subscribed; waiting for AMQP livescore",
			"fixture_id", f.FixtureID, "window", cfg.TrialVerifyWindow,
		)
		verified := waitForAMQPLivescore(ctx, cfg, f.FixtureID)
		if !verified {
			// Trial-blocked: unsubscribe. May lose quota if fixture went InProgress.
			if _, err := cfg.LSPORTSMeta.Unsubscribe(ctx, int(f.FixtureID)); err != nil {
				cfg.Logger.Warn("auto-discover: unsubscribe failed (Trial cleanup)",
					"fixture_id", f.FixtureID, "error", err,
				)
			}
			return "skipped:trial_blocked"
		}
	}

	// 4. polymarket-sync subprocess to enrich tokens (required by paper
	// signal pipeline). Run sync only if our DB doesn't already have
	// these tokens — quick lookup.
	if _, err := cfg.DB.LookupPolymarketTokenByOutcome(ctx, f.PMConditionID, f.HomeTeam); err != nil {
		// missing — run sync subprocess (best-effort; failure logged but
		// we still proceed since the paper-baseball lookup will surface
		// the error in audit rows).
		if perr := runPolymarketSync(ctx, ev.ID, cfg.Logger); perr != nil {
			cfg.Logger.Warn("auto-discover: polymarket-sync failed (proceeding anyway)",
				"event_id", ev.ID, "error", perr,
			)
		}
	}

	// 5. Hand off to paper-baseball consumer.
	addCb(*f)
	return "accepted"
}

// waitForAMQPLivescore polls the DB every 5s up to TrialVerifyWindow.
// Returns true if any livescore amqp_push row arrived for the fixture.
func waitForAMQPLivescore(ctx context.Context, cfg DiscoveryConfig, fid int64) bool {
	deadline := time.Now().Add(cfg.TrialVerifyWindow)
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		if cnt, _ := cfg.DB.CountRecentAMQPLivescore(ctx, fid, cfg.TrialVerifyWindow); cnt > 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-tick.C:
			continue
		}
	}
}

// runPolymarketSync invokes `go run ./cmd/polymarket-sync --target-event-id <ID> --no-prices`
// as a subprocess to enrich the polymarket_tokens table. This mirrors the
// operator's manual sync workflow.
func runPolymarketSync(ctx context.Context, eventID string, logger *slog.Logger) error {
	subCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(subCtx, "go", "run", "./cmd/polymarket-sync",
		"--target-event-id", eventID, "--no-prices")
	out, err := cmd.CombinedOutput()
	if err != nil {
		logger.Warn("polymarket-sync subprocess failed",
			"event_id", eventID, "error", err, "tail", lastLines(string(out), 4))
		return err
	}
	logger.Info("polymarket-sync subprocess OK", "event_id", eventID, "bytes", len(out))
	return nil
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

func truncErr(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > 100 {
		return s[:100] + "..."
	}
	return s
}

// parseInPlayEvents extracts (fixture_id, home_team, away_team) tuples
// from the raw /InPlay/GetEvents response. The body shape observed in
// production LSPORTS replies is:
//
//	{ "Header": {...}, "Body": [
//	    {"FixtureId": 12345,
//	     "Fixture": {
//	         "Sport": {"Id": 154, "Name": "Baseball"},
//	         "Participants": [
//	             {"Name": "Cincinnati Reds", "Position": 1},
//	             {"Name": "Atlanta Braves",  "Position": 2}
//	         ]
//	     },
//	     ...
//	    }
//	]}
//
// We're defensive about field presence and skip items missing Sport.Id
// or Participants. Position: 1=home, 2=away per LSPORTS convention.
func parseInPlayEvents(raw []byte, baseballSportID int) []DiscoveredFixture {
	var env struct {
		Body []json.RawMessage `json:"Body"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil
	}
	out := []DiscoveredFixture{}
	for _, item := range env.Body {
		var ev struct {
			FixtureID int64 `json:"FixtureId"`
			Fixture   struct {
				Sport struct {
					ID int `json:"Id"`
				} `json:"Sport"`
				Participants []struct {
					Name     string `json:"Name"`
					Position int    `json:"Position"`
				} `json:"Participants"`
			} `json:"Fixture"`
		}
		if err := json.Unmarshal(item, &ev); err != nil {
			continue
		}
		if ev.Fixture.Sport.ID != baseballSportID || ev.FixtureID == 0 {
			continue
		}
		d := DiscoveredFixture{FixtureID: ev.FixtureID}
		for _, p := range ev.Fixture.Participants {
			if p.Position == 1 {
				d.HomeTeam = p.Name
			}
			if p.Position == 2 {
				d.AwayTeam = p.Name
			}
		}
		if d.HomeTeam == "" || d.AwayTeam == "" {
			continue
		}
		out = append(out, d)
	}
	return out
}

// FormatFixtureLog returns a short human-readable label.
func FormatFixtureLog(d DiscoveredFixture) string {
	return fmt.Sprintf("%d %s vs %s slug=%s cond=%s tokens=[%s,%s]",
		d.FixtureID, d.HomeTeam, d.AwayTeam, d.PMSlug, d.PMConditionID,
		short(d.PMTokenIDs[0]), short(d.PMTokenIDs[1]),
	)
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12] + "..."
	}
	return s
}

// EventIDInt parses the PM event ID string (returned by Gamma as a number
// or numeric string) into int64. Tolerates non-numeric values by
// returning 0.
func EventIDInt(s string) int64 {
	n, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n
}
