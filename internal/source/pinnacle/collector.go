package pinnacle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Sink receives raw API outputs from the collector. The concrete
// implementation in internal/store/pinnacle.go writes to Postgres; tests use
// fakes. All methods must be safe to call from multiple goroutines.
type Sink interface {
	UpsertLeagues(ctx context.Context, sportID int, leagues []League) error
	UpsertMatchups(ctx context.Context, leagueID int, matchups []Matchup) (newCount int, err error)
	SaveMarketSnapshot(ctx context.Context, m Market) (inserted bool, err error)
	StartRun(ctx context.Context, sportID int) (runID int64, err error)
	FinishRun(ctx context.Context, runID int64, summary RunSummary) error
	// SweepStaleLiveFlags clears last_is_live on matchups for a sport that
	// haven't been re-seen by the matchup-refresh loop in `maxAge`.
	SweepStaleLiveFlags(ctx context.Context, sportID int, maxAge time.Duration) (int64, error)
}

// RunSummary is the per-cycle telemetry record.
type RunSummary struct {
	LeaguesSeen      int
	MatchupsSeen     int
	MarketsSeen      int
	MarketsInserted  int
	MarketsUnchanged int
	HTTPErrors       int
	Status           string // success | error | canceled
	ErrorMessage     string
}

// Options configures the collector.
type Options struct {
	SportIDs            []int    // e.g. [12] for E Sports
	LeagueNameIncludes  []string // case-insensitive substrings; empty = all
	LeagueNameExcludes  []string // case-insensitive substrings; non-empty match drops the league even if includes also matched
	LeaguePollInterval  time.Duration
	MatchupPollInterval time.Duration
	MarketsPollInterval time.Duration
	MarketsConcurrency  int
	OnlyLiveOrSoon      time.Duration // poll markets only when within this window of start (or live). 0 = always poll matchups with hasMarkets.
	Logger              *slog.Logger
}

// Collector orchestrates the three polling loops:
//
//  1. League poll (every LeaguePollInterval): refresh active league set per sport.
//  2. Matchup poll (every MatchupPollInterval): refresh matchups per active league.
//  3. Markets poll (every MarketsPollInterval): refresh markets per active matchup.
//
// All discovered data is pushed through the Sink. The collector keeps
// in-memory state of "which matchups are worth polling for markets" so the
// markets-poll loop has a bounded work queue.
type Collector struct {
	client *Client
	sink   Sink
	opts   Options
	log    *slog.Logger

	mu             sync.Mutex
	activeLeagues  map[int]League // league id -> league
	activeMatchups map[int64]liveMatchup
}

type liveMatchup struct {
	matchup  Matchup
	leagueID int
	lastPoll time.Time
}

// NewCollector returns a configured collector. Defaults are applied for
// missing intervals.
func NewCollector(client *Client, sink Sink, opts Options) *Collector {
	if opts.LeaguePollInterval == 0 {
		opts.LeaguePollInterval = 60 * time.Second
	}
	if opts.MatchupPollInterval == 0 {
		// 15s default (was 30s) — sport-global matchups call is one
		// request per sport regardless of league count, so we can afford
		// to discover newly-live matches faster. KICK-OFF latency drops
		// from up to 30s to up to 15s.
		opts.MatchupPollInterval = 15 * time.Second
	}
	if opts.MarketsPollInterval == 0 {
		opts.MarketsPollInterval = 2500 * time.Millisecond
	}
	if opts.MarketsConcurrency <= 0 {
		opts.MarketsConcurrency = 3
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Collector{
		client:         client,
		sink:           sink,
		opts:           opts,
		log:            log,
		activeLeagues:  map[int]League{},
		activeMatchups: map[int64]liveMatchup{},
	}
}

// Run blocks until ctx is canceled, driving the three loops in parallel.
func (c *Collector) Run(ctx context.Context) error {
	if len(c.opts.SportIDs) == 0 {
		return errors.New("pinnacle: at least one sport id required")
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); c.leagueLoop(ctx) }()
	go func() { defer wg.Done(); c.matchupLoop(ctx) }()
	go func() { defer wg.Done(); c.marketsLoop(ctx) }()
	wg.Wait()
	return ctx.Err()
}

// ---- league loop ----

func (c *Collector) leagueLoop(ctx context.Context) {
	c.refreshLeagues(ctx) // fire one immediately
	t := time.NewTicker(c.opts.LeaguePollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.refreshLeagues(ctx)
		}
	}
}

func (c *Collector) refreshLeagues(ctx context.Context) {
	for _, sportID := range c.opts.SportIDs {
		lgs, err := c.client.Leagues(ctx, sportID, false)
		if err != nil {
			c.log.Warn("pinnacle: leagues fetch failed", "sport_id", sportID, "err", err)
			continue
		}
		filtered := filterLeagues(lgs, c.opts.LeagueNameIncludes, c.opts.LeagueNameExcludes)
		active := filtered[:0]
		for _, l := range filtered {
			if l.MatchupCount > 0 {
				active = append(active, l)
			}
		}
		if err := c.sink.UpsertLeagues(ctx, sportID, active); err != nil {
			c.log.Warn("pinnacle: upsert leagues failed", "sport_id", sportID, "err", err)
		}
		c.mu.Lock()
		for _, l := range active {
			c.activeLeagues[l.ID] = l
		}
		// Drop leagues that are no longer active (matchupCount became 0 or filter no longer matches).
		seen := map[int]bool{}
		for _, l := range active {
			seen[l.ID] = true
		}
		for id := range c.activeLeagues {
			if !seen[id] {
				delete(c.activeLeagues, id)
			}
		}
		c.mu.Unlock()
		c.log.Info("pinnacle: leagues refreshed", "sport_id", sportID, "active", len(active))
	}
}

// ---- matchup loop ----

func (c *Collector) matchupLoop(ctx context.Context) {
	// Wait briefly so the first league poll has populated activeLeagues.
	select {
	case <-ctx.Done():
		return
	case <-time.After(2 * time.Second):
	}
	c.refreshMatchups(ctx)
	t := time.NewTicker(c.opts.MatchupPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.refreshMatchups(ctx)
		}
	}
}

// refreshMatchups fetches the global sport-matchups endpoint per sport (a
// single call that returns every matchup across every league for that
// sport — including the live ones in obscure leagues we'd otherwise miss
// via the per-league fanout). It also opportunistically upserts any
// previously-unknown leagues discovered in the matchup payload, so the
// edge view's league join keeps resolving.
func (c *Collector) refreshMatchups(ctx context.Context) {
	for _, sportID := range c.opts.SportIDs {
		ms, err := c.client.SportMatchups(ctx, sportID)
		if err != nil {
			c.log.Warn("pinnacle: sport matchups fetch failed", "sport_id", sportID, "err", err)
			continue
		}

		// Apply league-name filter (if any) and partition by league.
		filtered := filterMatchupsByLeagueName(ms, c.opts.LeagueNameIncludes, c.opts.LeagueNameExcludes)

		// Extract distinct leagues + per-league matchup counts so we upsert
		// even leagues that weren't in the active list yet (FK-safe).
		byLeague := map[int][]Matchup{}
		for _, m := range filtered {
			byLeague[m.League.ID] = append(byLeague[m.League.ID], m)
		}
		leagues := make([]League, 0, len(byLeague))
		for id, list := range byLeague {
			// Use the first matchup's nested league record for metadata.
			meta := list[0].League
			leagues = append(leagues, League{
				ID:           id,
				Name:         meta.Name,
				Group:        meta.Group,
				MatchupCount: len(list),
				Sport: struct {
					ID int `json:"id"`
				}{ID: sportID},
			})
		}
		if err := c.sink.UpsertLeagues(ctx, sportID, leagues); err != nil {
			c.log.Warn("pinnacle: upsert leagues from matchups failed", "sport_id", sportID, "err", err)
		}

		// Upsert matchups by league. Update the in-memory active set per league.
		for leagueID, list := range byLeague {
			if _, err := c.sink.UpsertMatchups(ctx, leagueID, list); err != nil {
				c.log.Warn("pinnacle: upsert matchups failed", "league_id", leagueID, "err", err)
			}
			c.updateActiveMatchups(leagueID, list)
		}

		// Also drop active matchups for leagues that no longer appear in
		// this sport's response at all (e.g. their tournament ended).
		c.mu.Lock()
		stillKnown := map[int]bool{}
		for id := range byLeague {
			stillKnown[id] = true
		}
		for mid, am := range c.activeMatchups {
			lg, ok := c.activeLeagues[am.leagueID]
			if !ok {
				continue
			}
			if lg.Sport.ID == sportID && !stillKnown[am.leagueID] {
				delete(c.activeMatchups, mid)
			}
		}
		c.mu.Unlock()

		// Clear last_is_live=true on matchups that didn't appear in this
		// sport's response (Pinnacle removes finished matches instead of
		// flipping isLive=false). Use the matchup-poll interval + 2x as
		// the staleness cutoff so transient blips don't clear live flags.
		swept, sweepErr := c.sink.SweepStaleLiveFlags(ctx, sportID, c.opts.MatchupPollInterval*2)
		if sweepErr != nil {
			c.log.Warn("pinnacle: sweep stale live flags failed", "sport_id", sportID, "err", sweepErr)
		}

		c.log.Info("pinnacle: matchups refreshed (sport-global)",
			"sport_id", sportID, "total_returned", len(ms),
			"after_filter", len(filtered), "leagues", len(byLeague),
			"swept_stale_live", swept)
	}
}

// filterMatchupsByLeagueName drops matchups whose league name doesn't
// contain any of the configured substrings. Empty filters keeps all.
func filterMatchupsByLeagueName(in []Matchup, includes, excludes []string) []Matchup {
	if len(includes) == 0 && len(excludes) == 0 {
		return in
	}
	out := make([]Matchup, 0, len(in))
	for _, m := range in {
		n := strings.ToLower(m.League.Name)
		if !leagueNamePasses(n, includes, excludes) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// leagueNamePasses returns true if n is allowed by the include + exclude
// pair. Empty includes = pass any name; non-empty = require at least one
// substring hit. Non-empty excludes = drop n if any substring hits, even
// when includes matched. Both arrays are matched case-insensitively;
// callers should lowercase n before calling.
func leagueNamePasses(n string, includes, excludes []string) bool {
	if len(includes) > 0 {
		matched := false
		for _, f := range includes {
			if strings.Contains(n, f) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, f := range excludes {
		if strings.Contains(n, f) {
			return false
		}
	}
	return true
}

// updateActiveMatchups rebuilds the in-memory set of matchups that the
// markets loop should poll. Includes only type="matchup" with hasMarkets and
// (live OR starts within OnlyLiveOrSoon window).
func (c *Collector) updateActiveMatchups(leagueID int, matchups []Matchup) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	// Drop matchups from this league that no longer appear.
	keep := map[int64]bool{}
	for _, m := range matchups {
		if m.Type != "matchup" || !m.HasMarkets {
			continue
		}
		if !c.matchupWorthPolling(m, now) {
			continue
		}
		keep[m.ID] = true
		existing := c.activeMatchups[m.ID]
		existing.matchup = m
		existing.leagueID = leagueID
		c.activeMatchups[m.ID] = existing
	}
	for id, am := range c.activeMatchups {
		if am.leagueID != leagueID {
			continue
		}
		if !keep[id] {
			delete(c.activeMatchups, id)
		}
	}
}

func (c *Collector) matchupWorthPolling(m Matchup, now time.Time) bool {
	if m.IsLive {
		return true
	}
	if c.opts.OnlyLiveOrSoon == 0 {
		return true
	}
	if m.StartTime.IsZero() {
		return false
	}
	delta := m.StartTime.Sub(now)
	return delta > 0 && delta <= c.opts.OnlyLiveOrSoon
}

// ---- markets loop ----

func (c *Collector) marketsLoop(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(3 * time.Second):
	}
	t := time.NewTicker(c.opts.MarketsPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.tickMarkets(ctx)
		}
	}
}

func (c *Collector) tickMarkets(ctx context.Context) {
	c.mu.Lock()
	ids := make([]int64, 0, len(c.activeMatchups))
	for id := range c.activeMatchups {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	if len(ids) == 0 {
		return
	}

	// Start a run for this tick.
	runID, err := c.sink.StartRun(ctx, c.opts.SportIDs[0])
	if err != nil {
		c.log.Warn("pinnacle: StartRun failed", "err", err)
	}
	summary := RunSummary{Status: "success"}
	var (
		seen, inserted, unchanged, httpErrs int64
	)

	sem := make(chan struct{}, c.opts.MarketsConcurrency)
	var wg sync.WaitGroup
	for _, id := range ids {
		select {
		case <-ctx.Done():
			break
		default:
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(matchupID int64) {
			defer wg.Done()
			defer func() { <-sem }()
			mkts, err := c.client.Markets(ctx, matchupID)
			if err != nil {
				atomic.AddInt64(&httpErrs, 1)
				if IsRateLimited(err) {
					// jittered backoff
					time.Sleep(time.Duration(500+rand.Intn(1500)) * time.Millisecond)
				}
				c.log.Debug("pinnacle: markets fetch failed", "matchup_id", matchupID, "err", err)
				return
			}
			// Empty response = Pinnacle no longer offers markets for this
			// matchup (game ended, market closed, etc). Drop from active
			// set so we stop wasting calls. The next sport-wide matchup
			// refresh will re-add it if it really comes back.
			if len(mkts) == 0 {
				c.mu.Lock()
				delete(c.activeMatchups, matchupID)
				c.mu.Unlock()
				return
			}
			for _, m := range mkts {
				// markets/related/straight returns related prop matchup markets
				// too (each has its own matchupId). We only persist the main
				// matchup's markets here; props can be modeled separately later.
				if m.MatchupID != matchupID {
					continue
				}
				atomic.AddInt64(&seen, 1)
				ins, err := c.sink.SaveMarketSnapshot(ctx, m)
				if err != nil {
					c.log.Warn("pinnacle: save market failed", "matchup_id", matchupID, "key", m.Key, "version", m.Version, "err", err)
					continue
				}
				if ins {
					atomic.AddInt64(&inserted, 1)
				} else {
					atomic.AddInt64(&unchanged, 1)
				}
			}
		}(id)
	}
	wg.Wait()

	summary.MatchupsSeen = len(ids)
	summary.MarketsSeen = int(seen)
	summary.MarketsInserted = int(inserted)
	summary.MarketsUnchanged = int(unchanged)
	summary.HTTPErrors = int(httpErrs)
	if ctx.Err() != nil {
		summary.Status = "canceled"
	} else if httpErrs > 0 && inserted == 0 {
		summary.Status = "error"
		summary.ErrorMessage = fmt.Sprintf("%d http errors, 0 inserts", httpErrs)
	}
	if runID > 0 {
		if err := c.sink.FinishRun(ctx, runID, summary); err != nil {
			c.log.Warn("pinnacle: FinishRun failed", "err", err)
		}
	}
	c.log.Info("pinnacle: markets tick",
		"matchups", len(ids), "markets_seen", seen,
		"inserted", inserted, "unchanged", unchanged, "http_errors", httpErrs)
}

// filterLeagues keeps leagues whose lowercase name contains any of the
// substrings (also lowercase). Empty filters returns the input unchanged.
func filterLeagues(in []League, includes, excludes []string) []League {
	if len(includes) == 0 && len(excludes) == 0 {
		return in
	}
	out := make([]League, 0, len(in))
	for _, l := range in {
		if !leagueNamePasses(strings.ToLower(l.Name), includes, excludes) {
			continue
		}
		out = append(out, l)
	}
	return out
}
