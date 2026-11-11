// cmd/lsports-paper-baseball is a LSPORTS-driven paper trading signal
// emitter. It listens to LSPORTS AMQP push messages for a configured set
// of baseball fixtures, detects score-change events in real time,
// snapshots the matching Polymarket CLOB token book, and inserts a
// synthetic "BUY_CANDIDATE" signal into the existing signals table so
// the cmd/clob-auto-submit daemon (run with -really-submit=false) picks
// it up and writes a hypothetical order payload to proposed_orders.
//
// It does NOT execute trades, does NOT handle wallets or private keys,
// and is NOT a trade-execution surface. The companion cmd/clob-auto-
// submit, only when explicitly run with -really-submit=true (which is
// not the paper-trading workflow), would send orders to Polymarket.
//
// Two operating modes:
//
//   - STATIC:    --map fixture_id:condition_id:home_team:away_team ...
//   - AUTO:      --auto-discover   (polls LSPORTS InPlay/GetEvents for
//                                   new baseball fixtures, matches to
//                                   PM via hardcoded MLB team map +
//                                   Gamma slug lookup, verifies Trial
//                                   access, then dynamically adds to
//                                   the consumer map.)
//
// The two modes can be combined.
//
// Required env:
//
//	LSPORTS_USERNAME, LSPORTS_PASSWORD, LSPORTS_INPLAY_PACKAGE_ID
//	LSPORTS_AMQP_HOST, LSPORTS_AMQP_PORT, LSPORTS_AMQP_VHOST
//	DATABASE_URL
//	POLYMARKET_CLOB_URL                (default https://clob.polymarket.com)
//	POLYMARKET_GAMMA_URL               (default https://gamma-api.polymarket.com)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/auto"
	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/lsports"
	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
	"github.com/your-org/pol-odds-collector/internal/store"
	"github.com/your-org/pol-odds-collector/internal/trade"
)

type fixtureMap struct {
	HomeTeam  string
	AwayTeam  string
	Condition string // 0x... PM condition_id of the winner market
}

type fixtureState struct {
	mu             sync.Mutex
	homeScore      int
	awayScore      int
	haveBaseline   bool
	lastScoreEvent time.Time // for 5s dedup
}

// consumerRegistry is the concurrent-safe set of fixtures being watched.
// Both --map static entries and --auto-discover dynamic entries land in
// the same registry. Reads happen on the AMQP delivery hot path; writes
// only when --auto-discover commits a new fixture.
type consumerRegistry struct {
	mu      sync.RWMutex
	entries map[int64]fixtureMap
	state   map[int64]*fixtureState
}

func newRegistry() *consumerRegistry {
	return &consumerRegistry{
		entries: map[int64]fixtureMap{},
		state:   map[int64]*fixtureState{},
	}
}

func (r *consumerRegistry) Add(fid int64, fm fixtureMap) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[fid] = fm
	if _, ok := r.state[fid]; !ok {
		r.state[fid] = &fixtureState{}
	}
}

func (r *consumerRegistry) Get(fid int64) (fixtureMap, *fixtureState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fm, ok := r.entries[fid]
	if !ok {
		return fixtureMap{}, nil, false
	}
	return fm, r.state[fid], true
}

func (r *consumerRegistry) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

type mapsFlag struct {
	entries map[int64]fixtureMap
}

func (m *mapsFlag) String() string { return fmt.Sprintf("%v", m.entries) }
func (m *mapsFlag) Set(v string) error {
	if m.entries == nil {
		m.entries = make(map[int64]fixtureMap)
	}
	parts := strings.SplitN(v, ":", 4)
	if len(parts) != 4 {
		return fmt.Errorf("--map must be fixture_id:condition_id:home_team:away_team (got %q)", v)
	}
	fid, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil || fid <= 0 {
		return fmt.Errorf("--map fixture_id must be positive int: %q", parts[0])
	}
	cond := strings.TrimSpace(parts[1])
	if !strings.HasPrefix(cond, "0x") {
		return fmt.Errorf("--map condition_id must be 0x-prefixed hex: %q", parts[1])
	}
	home := strings.TrimSpace(parts[2])
	away := strings.TrimSpace(parts[3])
	if home == "" || away == "" {
		return fmt.Errorf("--map home_team and away_team required")
	}
	m.entries[fid] = fixtureMap{HomeTeam: home, AwayTeam: away, Condition: cond}
	return nil
}

const dedupWindow = 5 * time.Second

// Package-level live trading config + state, populated by main() once
// the user passes --live-trade. handleLivescore reads them under the
// state mutex when deciding whether to call internal/trade.PlaceOrder.
var (
	liveCfg     liveTradeConfig
	liveSt      = newLiveTradeState()
)

// liveTradeConfig holds the daemon-wide settings for posting real orders
// after each emitted paper signal. Zero-value Enabled=false skips all
// live-trade work.
type liveTradeConfig struct {
	Enabled            bool
	SizeShares         float64
	PerEventCapUSDC    float64
	DailyCapUSDC       float64
	MaxMatchups        int           // 0 = unlimited; otherwise distinct matchup count per day
	CooldownPerMatchup time.Duration
	ServerURL          string // persistent Node trade server (default http://127.0.0.1:9876)
	OrderType          string // "FAK" default

	// Dual-trigger exit settings. HoldDuration > 0 enables it.
	HoldDuration  time.Duration // 5m default — auto-sell after this if no counter-score
	ExitOrderType string        // "FAK" for SELL — almost always FAK
}

// positionMgr is the dual-trigger exit manager. Populated by main() when
// --live-trade is set; nil otherwise. close-on-counter-score and
// timer-based auto-sell both go through here.
var positionMgr *positionManager

// priceCaptureMgr writes a 1.5s-cadence book snapshot stream into
// polymarket_latency_observations for every watched fixture's two
// tokens. Populated unconditionally (paper or live) at main() so
// post-match analysis always has the data.
var priceCaptureMgr *priceCaptureManager

type liveTradeState struct {
	mu           sync.Mutex
	lastTradeAt  map[int64]time.Time // matchup_id -> last live order at
	perEventUSDC map[int64]float64   // matchup_id -> cumulative live notional
	dailyUSDC    float64             // total live notional today (UTC)
	dayStartUTC  time.Time           // 00:00 UTC of current day
}

func newLiveTradeState() *liveTradeState {
	return &liveTradeState{
		lastTradeAt:  map[int64]time.Time{},
		perEventUSDC: map[int64]float64{},
		dayStartUTC:  time.Now().UTC().Truncate(24 * time.Hour),
	}
}

// allow returns "" if the trade may proceed, else a short reason string.
// On allow, the caller is responsible for calling commit() after a
// successful POST so the per-event and daily counters track notional.
func (l *liveTradeState) allow(matchupID int64, notionalUSDC float64, cfg liveTradeConfig) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now().UTC()
	if today := now.Truncate(24 * time.Hour); today.After(l.dayStartUTC) {
		l.dayStartUTC = today
		l.dailyUSDC = 0
		// reset perEventUSDC so previous-day matchups don't carry over
		for k := range l.perEventUSDC {
			delete(l.perEventUSDC, k)
		}
	}
	// Max matchups gate: count distinct matchups we've already touched today.
	// A new matchup (first trade for that fixture today) only allowed if we
	// have room.
	if cfg.MaxMatchups > 0 {
		if _, alreadyTouched := l.perEventUSDC[matchupID]; !alreadyTouched {
			if len(l.perEventUSDC) >= cfg.MaxMatchups {
				return fmt.Sprintf("max_matchups (already %d / %d)", len(l.perEventUSDC), cfg.MaxMatchups)
			}
		}
	}
	if cfg.DailyCapUSDC > 0 && l.dailyUSDC+notionalUSDC > cfg.DailyCapUSDC {
		return fmt.Sprintf("daily_cap (would be %.2f / %.2f)", l.dailyUSDC+notionalUSDC, cfg.DailyCapUSDC)
	}
	if cfg.PerEventCapUSDC > 0 && l.perEventUSDC[matchupID]+notionalUSDC > cfg.PerEventCapUSDC {
		return fmt.Sprintf("per_event_cap (would be %.2f / %.2f)", l.perEventUSDC[matchupID]+notionalUSDC, cfg.PerEventCapUSDC)
	}
	if cfg.CooldownPerMatchup > 0 {
		if last, ok := l.lastTradeAt[matchupID]; ok {
			if now.Sub(last) < cfg.CooldownPerMatchup {
				return fmt.Sprintf("cooldown (%.0fs remaining)", (cfg.CooldownPerMatchup - now.Sub(last)).Seconds())
			}
		}
	}
	return ""
}

func (l *liveTradeState) commit(matchupID int64, notionalUSDC float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastTradeAt[matchupID] = time.Now().UTC()
	l.perEventUSDC[matchupID] += notionalUSDC
	l.dailyUSDC += notionalUSDC
}

func main() {
	var (
		maps              mapsFlag
		autoDiscover      = flag.Bool("auto-discover", false, "poll LSPORTS InPlay/GetEvents and auto-add new baseball fixtures")
		pollInterval      = flag.Duration("auto-poll-interval", 5*time.Minute, "auto-discover polling interval")
		trialVerifyWindow = flag.Duration("auto-trial-verify", 60*time.Second, "auto-discover Trial verify window — must see AMQP livescore within")
		consumerTag       = flag.String("consumer-tag", "pol-paper-baseball", "AMQP consumer tag (must differ from amqp-capture)")
		clobBase          = flag.String("clob-base", "", "PM CLOB base URL (default env POLYMARKET_CLOB_URL or clob.polymarket.com)")
		gammaBase         = flag.String("gamma-base", "", "PM Gamma base URL (default env POLYMARKET_GAMMA_URL or gamma-api.polymarket.com)")
		lsportsMetaBase   = flag.String("lsports-meta-base", "", "LSPORTS metadata base URL (default stm-api)")
		lsportsSnapBase   = flag.String("lsports-snapshot-base", "", "LSPORTS snapshot base URL (default stm-snapshot)")
		clobTimeout       = flag.Duration("clob-timeout", 5*time.Second, "PM CLOB HTTP timeout for book snapshot")
		minBackoff        = flag.Duration("min-backoff", 1*time.Second, "minimum reconnect backoff")
		maxBackoff        = flag.Duration("max-backoff", 30*time.Second, "maximum reconnect backoff")
		heartbeat         = flag.Duration("heartbeat", 10*time.Second, "AMQP heartbeat interval")

		// Live trading flags. Default OFF — leaving these unset keeps the
		// daemon in pure paper mode.
		liveTrade           = flag.Bool("live-trade", false, "place real PM orders after each emitted signal (uses Node subprocess)")
		liveSizeShares      = flag.Float64("live-size-shares", 11, "live: order size in SHARES (~$5 at ask 0.45)")
		livePerEventCap     = flag.Float64("live-per-event-cap-usd", 5, "live: max notional USD per matchup (0=unlimited)")
		liveDailyCap        = flag.Float64("live-daily-cap-usd", 20, "live: max notional USD across all matchups today (0=unlimited)")
		liveMaxMatchups     = flag.Int("live-max-matchups", 0, "live: max distinct matchups to trade today (0=unlimited)")
		liveCooldown        = flag.Duration("live-cooldown", 5*time.Minute, "live: minimum gap between trades for the same matchup")
		liveOrderType       = flag.String("live-order-type", "FAK", "live: order type GTC|FAK|FOK")
		liveServerURL       = flag.String("live-server-url", trade.DefaultServerURL, "live: persistent Node trade server URL")
		liveHoldDuration    = flag.Duration("live-hold-duration", 5*time.Minute, "live: dual-trigger hold — auto-sell after this if no counter-score (0=disabled, hold to settlement)")
		liveExitOrderType   = flag.String("live-exit-order-type", "FAK", "live: SELL order type for exit (FAK | GTC)")
	)
	flag.Var(&maps, "map", "fixture_id:condition_id:home_team:away_team (repeatable; required unless --auto-discover)")
	flag.Parse()

	_, _ = config.Load()

	// Populate package-level live trading config from flags. Default
	// disabled — only enabled when operator explicitly passes --live-trade.
	if *liveTrade {
		serverURL := strings.TrimSpace(*liveServerURL)
		if serverURL == "" {
			serverURL = trade.DefaultServerURL
		}
		// Best-effort health probe (logged only). Server may take a few
		// seconds to start; we don't block daemon launch on this.
		healthCtx, healthCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if info, herr := trade.Health(healthCtx, serverURL); herr != nil {
			slog.Warn("live-trade: trade server health probe failed",
				"url", serverURL, "err", herr,
				"hint", "start with `node scripts/clob-trade/server.mjs` first")
		} else {
			slog.Info("live-trade: trade server health OK", "url", serverURL, "info", info)
		}
		healthCancel()
		liveCfg = liveTradeConfig{
			Enabled:            true,
			SizeShares:         *liveSizeShares,
			PerEventCapUSDC:    *livePerEventCap,
			DailyCapUSDC:       *liveDailyCap,
			MaxMatchups:        *liveMaxMatchups,
			CooldownPerMatchup: *liveCooldown,
			ServerURL:          serverURL,
			OrderType:          strings.ToUpper(strings.TrimSpace(*liveOrderType)),
			HoldDuration:       *liveHoldDuration,
			ExitOrderType:      strings.ToUpper(strings.TrimSpace(*liveExitOrderType)),
		}
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Sweep stale kill switch file (24h+ age) before accepting any
	// traffic. Operators set this intentionally as an emergency stop
	// but old test detritus has neutralized live sessions before.
	killSwitchSweepStale()

	if len(maps.entries) == 0 && !*autoDiscover {
		fail("at least one --map required, or pass --auto-discover")
	}

	user := strings.TrimSpace(os.Getenv("LSPORTS_USERNAME"))
	pw := os.Getenv("LSPORTS_PASSWORD")
	pkgStr := envOr("LSPORTS_INPLAY_PACKAGE_ID", "4044")
	host := envOr("LSPORTS_AMQP_HOST", lsports.DefaultAMQPHost)
	portStr := envOr("LSPORTS_AMQP_PORT", strconv.Itoa(lsports.DefaultAMQPPort))
	vhost := envOr("LSPORTS_AMQP_VHOST", lsports.DefaultAMQPVHost)
	dbURL := envOr("DATABASE_URL", "")
	clobURL := strings.TrimSpace(*clobBase)
	if clobURL == "" {
		clobURL = envOr("POLYMARKET_CLOB_URL", polymarket.DefaultCLOBBaseURL)
	}
	gammaURL := strings.TrimSpace(*gammaBase)
	if gammaURL == "" {
		gammaURL = envOr("POLYMARKET_GAMMA_URL", polymarket.DefaultGammaBaseURL)
	}
	lspMetaURL := strings.TrimSpace(*lsportsMetaBase)
	if lspMetaURL == "" {
		lspMetaURL = lsports.DefaultBaseURL
	}
	lspSnapURL := strings.TrimSpace(*lsportsSnapBase)
	if lspSnapURL == "" {
		lspSnapURL = lsports.DefaultSnapshotBaseURL
	}

	if user == "" || pw == "" {
		fail("LSPORTS_USERNAME and LSPORTS_PASSWORD required")
	}
	if dbURL == "" {
		fail("DATABASE_URL required")
	}
	pkg, err := strconv.Atoi(pkgStr)
	if err != nil {
		fail("LSPORTS_INPLAY_PACKAGE_ID must be integer: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		fail("LSPORTS_AMQP_PORT must be integer: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := store.Open(ctx, dbURL, false, normalize.FreshnessPolicy{})
	if err != nil {
		fail("open postgres: %v", err)
	}
	defer func() { _ = st.Close() }()

	// On graceful shutdown, flatten any open positions so we don't leave
	// SDK-tracked positions hanging (they'd hold to settlement, which is
	// fine, but operators expect explicit exits).
	defer func() {
		if positionMgr != nil {
			positionMgr.CloseAll(context.Background(), "shutdown")
		}
	}()

	clob := polymarket.NewCLOBClient(clobURL, *clobTimeout)

	// Price capture goroutines run regardless of paper/live so analysis
	// data is always recorded.
	priceCaptureMgr = newPriceCaptureManager(clob, st, 1500*time.Millisecond)
	defer priceCaptureMgr.StopAll("shutdown")

	// Initialize position manager when live trade enabled AND hold duration > 0.
	// HoldDuration=0 means "hold to settlement" (no managed exit).
	if liveCfg.Enabled && liveCfg.HoldDuration > 0 {
		positionMgr = newPositionManager(
			liveCfg.HoldDuration,
			liveCfg.ServerURL,
			liveCfg.ExitOrderType,
			clob,
		)
		slog.Info("dual-trigger exit ENABLED",
			"hold_duration", liveCfg.HoldDuration,
			"exit_order_type", liveCfg.ExitOrderType,
			"counter_score_trigger", "on opposite-side score event same fixture",
		)
	}

	cfg := lsports.AMQPConfig{
		Host:      host,
		Port:      port,
		Username:  user,
		Password:  pw,
		VHost:     vhost,
		Queue:     fmt.Sprintf("_%d_", pkg),
		Heartbeat: *heartbeat,
	}

	registry := newRegistry()
	for fid, m := range maps.entries {
		registry.Add(fid, m)
		slog.Info("static fixture mapped",
			"fixture_id", fid, "home", m.HomeTeam, "away", m.AwayTeam,
			"condition_id", m.Condition,
		)
		priceCaptureMgr.Start(ctx, fid, m.Condition)
	}

	slog.Info("lsports-paper-baseball starting",
		"static_fixtures", len(maps.entries),
		"auto_discover", *autoDiscover,
		"queue", cfg.Queue, "clob", clobURL, "gamma", gammaURL,
		"consumer_tag", *consumerTag,
	)

	// Optional auto-discover goroutine.
	if *autoDiscover {
		creds := lsports.Credentials{PackageID: pkg, Username: user, Password: pw}
		lspMeta := lsports.NewClient(lspMetaURL, creds, 15*time.Second)
		lspSnap := lsports.NewClient(lspSnapURL, creds, 15*time.Second)
		gamma := polymarket.NewClient(gammaURL, 15*time.Second)
		discoveryCfg := auto.DiscoveryConfig{
			LSPORTSMeta:       lspMeta,
			LSPORTSSnapshot:   lspSnap,
			PMGamma:           gamma,
			DB:                st,
			PollInterval:      *pollInterval,
			TrialVerifyWindow: *trialVerifyWindow,
			BaseballSportID:   auto.BaseballSportID,
			Logger:            slog.Default(),
		}
		go func() {
			err := auto.Run(ctx, discoveryCfg, func(d auto.DiscoveredFixture) {
				if d.PMConditionID == "" {
					slog.Warn("auto-discover: empty condition_id; skip add", "fixture_id", d.FixtureID)
					return
				}
				registry.Add(d.FixtureID, fixtureMap{
					HomeTeam:  d.HomeTeam,
					AwayTeam:  d.AwayTeam,
					Condition: d.PMConditionID,
				})
				slog.Info("auto-discover: ADDED to consumer map",
					"summary", auto.FormatFixtureLog(d),
					"registry_size", registry.Size(),
				)
				priceCaptureMgr.Start(ctx, d.FixtureID, d.PMConditionID)
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("auto-discover: exited with error", "error", err)
			}
		}()
	}

	backoff := *minBackoff
	for {
		if err := runOnce(ctx, st, clob, cfg, *consumerTag, registry); err != nil {
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

func runOnce(
	ctx context.Context,
	st *store.PostgresStore,
	clob *polymarket.CLOBClient,
	cfg lsports.AMQPConfig,
	consumerTag string,
	registry *consumerRegistry,
) error {
	consumer := lsports.NewAMQPConsumer(cfg)
	if err := consumer.Connect(ctx); err != nil {
		return err
	}
	defer func() { _ = consumer.Close() }()
	slog.Info("amqp connected", "queue", cfg.Queue, "tag", consumerTag)

	deliveries, err := consumer.Consume(ctx, consumerTag)
	if err != nil {
		return fmt.Errorf("start consume: %w", err)
	}
	closeNotify := consumer.CloseNotify()

	signals := 0
	skips := 0
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
				_ = d.Nack(false, true)
				continue
			}
			// We care only about livescore. Ack everything else fast.
			if msg.ObservationKind != "livescore" {
				_ = d.Ack(false)
				continue
			}
			fid := lsports.FixtureIDFromMessage(d.Body)
			fmap, fst, watched := registry.Get(fid)
			if !watched {
				_ = d.Ack(false)
				continue
			}

			handleLivescore(ctx, st, clob, fid, fmap, fst, msg, d.Body, &signals, &skips)
			_ = d.Ack(false)

			if (signals+skips)%25 == 0 && signals+skips > 0 {
				slog.Info("paper-baseball stats", "signals", signals, "skips", skips,
					"registry_size", registry.Size())
			}
		}
	}
}

// scoreboardProj captures only the fields we care about. We try both
// `Body.Events[0].Livescore.Scoreboard.*` and `Body[0].Livescore.Scoreboard.*`
// shapes because LSPORTS push messages observed in the wild use both.
type scoreboardProj struct {
	Status        int
	CurrentPeriod int
	HomeScore     int
	AwayScore     int
	Found         bool
}

func parseScoreboard(body []byte) scoreboardProj {
	var out scoreboardProj
	var canonical struct {
		Body struct {
			Events []struct {
				Livescore struct {
					Scoreboard struct {
						Status        int `json:"Status"`
						CurrentPeriod int `json:"CurrentPeriod"`
						Results       []struct {
							Value json.Number `json:"Value"`
						} `json:"Results"`
					} `json:"Scoreboard"`
				} `json:"Livescore"`
			} `json:"Events"`
		} `json:"Body"`
	}
	if err := json.Unmarshal(body, &canonical); err == nil && len(canonical.Body.Events) > 0 {
		sb := canonical.Body.Events[0].Livescore.Scoreboard
		out.Status = sb.Status
		out.CurrentPeriod = sb.CurrentPeriod
		if len(sb.Results) >= 2 {
			out.HomeScore = numInt(sb.Results[0].Value)
			out.AwayScore = numInt(sb.Results[1].Value)
			out.Found = true
			return out
		}
	}
	var alt struct {
		Body []struct {
			Livescore struct {
				Scoreboard struct {
					Status        int `json:"Status"`
					CurrentPeriod int `json:"CurrentPeriod"`
					Results       []struct {
						Value json.Number `json:"Value"`
					} `json:"Results"`
				} `json:"Scoreboard"`
			} `json:"Livescore"`
		} `json:"Body"`
	}
	if err := json.Unmarshal(body, &alt); err == nil && len(alt.Body) > 0 {
		sb := alt.Body[0].Livescore.Scoreboard
		out.Status = sb.Status
		out.CurrentPeriod = sb.CurrentPeriod
		if len(sb.Results) >= 2 {
			out.HomeScore = numInt(sb.Results[0].Value)
			out.AwayScore = numInt(sb.Results[1].Value)
			out.Found = true
		}
	}
	return out
}

func numInt(n json.Number) int {
	if n == "" {
		return 0
	}
	if i, err := n.Int64(); err == nil {
		return int(i)
	}
	if f, err := n.Float64(); err == nil {
		return int(f)
	}
	return 0
}

func handleLivescore(
	ctx context.Context,
	st *store.PostgresStore,
	clob *polymarket.CLOBClient,
	fid int64,
	fmap fixtureMap,
	fst *fixtureState,
	msg lsports.AMQPMessage,
	body []byte,
	signals, skips *int,
) {
	stage0 := msg.ReceivedAt
	if stage0.IsZero() {
		stage0 = time.Now().UTC()
	}

	sb := parseScoreboard(body)
	if !sb.Found {
		return
	}
	if sb.Status != 2 {
		// Match no longer InProgress (status=3 Finished is the common one
		// here). Tear down the price-capture goroutine so we don't keep
		// polling a settled book — and so settled rows don't dilute hold-P&L
		// analyses.
		if priceCaptureMgr != nil {
			priceCaptureMgr.Stop(fid, fmt.Sprintf("match_status=%d", sb.Status))
		}
		return
	}

	fst.mu.Lock()
	prevHome, prevAway, hadBaseline := fst.homeScore, fst.awayScore, fst.haveBaseline
	lastEvent := fst.lastScoreEvent
	if !hadBaseline {
		fst.homeScore, fst.awayScore, fst.haveBaseline = sb.HomeScore, sb.AwayScore, true
		fst.mu.Unlock()
		return
	}
	if sb.HomeScore == prevHome && sb.AwayScore == prevAway {
		fst.mu.Unlock()
		return
	}
	if time.Since(lastEvent) < dedupWindow {
		fst.mu.Unlock()
		*skips++
		return
	}
	scoringSide := "home"
	if sb.AwayScore > prevAway {
		scoringSide = "away"
	} else if sb.HomeScore <= prevHome {
		fst.mu.Unlock()
		*skips++
		return
	}
	fst.homeScore = sb.HomeScore
	fst.awayScore = sb.AwayScore
	fst.lastScoreEvent = time.Now().UTC()
	fst.mu.Unlock()

	// Dual-trigger exit: counter-score check BEFORE we open a new position
	// for this same event. If we already hold a position on the opposite
	// side of this fixture, sell it immediately — our LSPORTS push gives
	// us ~21s edge over PM, so we exit before PM repriges.
	if positionMgr != nil {
		positionMgr.CheckCounterScore(ctx, fid, scoringSide)
	}

	teamName := fmap.HomeTeam
	if scoringSide == "away" {
		teamName = fmap.AwayTeam
	}
	scoreBefore := fmt.Sprintf("%d-%d", prevHome, prevAway)
	scoreAfter := fmt.Sprintf("%d-%d", sb.HomeScore, sb.AwayScore)

	stage1 := time.Now().UTC()
	lsportsToDaemonMS := int(stage1.Sub(stage0).Milliseconds())

	publishedAt := msg.Header.SourcePublishTime()
	audit := model.PaperBaseballSignal{
		LSportsFixtureID:            fid,
		LSportsEventAt:              stage0,
		ScoreBefore:                 scoreBefore,
		ScoreAfter:                  scoreAfter,
		ScoringSide:                 scoringSide,
		HomeTeamName:                fmap.HomeTeam,
		AwayTeamName:                fmap.AwayTeam,
		TargetPolymarketConditionID: fmap.Condition,
		RawPayload:                  json.RawMessage(body),
		LSportsToDaemonMS:           lsportsToDaemonMS,
	}
	if !publishedAt.IsZero() {
		audit.LSportsSourcePublishAt = &publishedAt
	}

	tok, err := st.LookupPolymarketTokenByOutcome(ctx, fmap.Condition, teamName)
	stage2 := time.Now().UTC()
	audit.DaemonToTokenLookupMS = int(stage2.Sub(stage1).Milliseconds())
	if err != nil {
		audit.SignalSkipReason = fmt.Sprintf("token_lookup_failed: %v", err)
		audit.TargetOutcomeName = teamName
		audit.PMSnapshotAt = stage2
		audit.TotalSignalLatencyMS = int(stage2.Sub(stage0).Milliseconds())
		if _, ierr := st.InsertPaperBaseballSignal(ctx, audit); ierr != nil {
			slog.Error("audit insert failed", "error", ierr, "fixture", fid)
		}
		slog.Warn("token lookup failed",
			"fixture", fid, "scoring_side", scoringSide, "team", teamName, "err", err)
		*skips++
		return
	}
	audit.TargetPolymarketMarketID = tok.PolymarketMarketID
	audit.TargetPolymarketTokenID = tok.TokenID
	audit.TargetOutcomeName = tok.OutcomeName
	audit.TargetOutcomeSide = tok.Side

	bookCtx, bookCancel := context.WithTimeout(ctx, 5*time.Second)
	book, berr := clob.GetOrderBook(bookCtx, tok.TokenID)
	bookCancel()
	stage3 := time.Now().UTC()
	audit.TokenLookupToPMSnapshotMS = int(stage3.Sub(stage2).Milliseconds())
	audit.PMSnapshotAt = stage3

	bestBid, _ := topLevel(book.Bids, true)
	bestAsk, _ := topLevel(book.Asks, false)
	var midpoint, spread *float64
	if bestBid != nil && bestAsk != nil {
		m := (*bestBid + *bestAsk) / 2.0
		s := *bestAsk - *bestBid
		midpoint = &m
		spread = &s
	}
	if berr != nil {
		audit.SignalSkipReason = fmt.Sprintf("clob_book_failed: %v", berr)
		audit.TotalSignalLatencyMS = int(stage3.Sub(stage0).Milliseconds())
		if _, ierr := st.InsertPaperBaseballSignal(ctx, audit); ierr != nil {
			slog.Error("audit insert failed", "error", ierr, "fixture", fid)
		}
		slog.Warn("CLOB book failed", "fixture", fid, "token", tok.TokenID, "err", berr)
		*skips++
		return
	}
	audit.PMBestAsk = bestAsk
	audit.PMBestBid = bestBid
	audit.PMMidpoint = midpoint
	audit.PMSpread = spread

	if bestAsk == nil || *bestAsk <= 0 {
		audit.SignalSkipReason = "no_ask"
		audit.TotalSignalLatencyMS = int(time.Now().UTC().Sub(stage0).Milliseconds())
		if _, ierr := st.InsertPaperBaseballSignal(ctx, audit); ierr != nil {
			slog.Error("audit insert failed", "error", ierr, "fixture", fid)
		}
		*skips++
		return
	}

	edgeSnap, _ := json.Marshal(map[string]any{
		"source":             "lsports_paper_baseball",
		"lsports_fixture_id": fid,
		"lsports_event_at":   stage0.Format(time.RFC3339Nano),
		"scoring_side":       scoringSide,
		"score_before":       scoreBefore,
		"score_after":        scoreAfter,
		"team_scored":        teamName,
	})

	syn := model.SyntheticBuySignal{
		MatchupID:             fid,
		PolymarketConditionID: fmap.Condition,
		PolymarketTokenID:     tok.TokenID,
		OutcomeSide:           tok.Side,
		OutcomeName:           tok.OutcomeName,
		HomeName:              fmap.HomeTeam,
		AwayName:              fmap.AwayTeam,
		LeagueName:            "MLB",
		PMAsk:                 bestAsk,
		PMBid:                 bestBid,
		PMMid:                 midpoint,
		PMSpread:              spread,
		PMAgeSec:              time.Since(stage3).Seconds(),
		EdgeSnapshot:          edgeSnap,
	}

	signalID, serr := st.InsertSyntheticBuySignal(ctx, syn)
	stage4 := time.Now().UTC()
	audit.PMSnapshotToSignalInsertMS = int(stage4.Sub(stage3).Milliseconds())
	audit.TotalSignalLatencyMS = int(stage4.Sub(stage0).Milliseconds())
	if serr != nil {
		audit.SignalSkipReason = fmt.Sprintf("signal_insert_failed: %v", serr)
		if _, ierr := st.InsertPaperBaseballSignal(ctx, audit); ierr != nil {
			slog.Error("audit insert failed", "error", ierr, "fixture", fid)
		}
		slog.Error("synthetic signal insert failed", "fixture", fid, "err", serr)
		*skips++
		return
	}
	audit.SignalID = &signalID

	if _, ierr := st.InsertPaperBaseballSignal(ctx, audit); ierr != nil {
		slog.Error("audit insert failed", "error", ierr, "fixture", fid)
		return
	}
	*signals++

	slog.Info("PAPER SIGNAL emitted",
		"fixture", fid,
		"scored", teamName,
		"score", scoreAfter,
		"token", tok.TokenID[:16]+"...",
		"ask", fmtPtr(bestAsk),
		"mid", fmtPtr(midpoint),
		"signal_id", signalID,
		"total_ms", audit.TotalSignalLatencyMS,
	)

	// Live trade hook. Off by default; only fires when --live-trade is set.
	if liveCfg.Enabled {
		absDiff := sb.HomeScore - sb.AwayScore
		if absDiff < 0 {
			absDiff = -absDiff
		}
		maybeLiveTrade(ctx, fid, tok.TokenID, bestAsk, fmap, scoringSide, teamName, scoreAfter, absDiff)
	}
}

// killSwitchPath is the sentinel file that, when present, blocks all
// new BUY orders. Existing positions still close via timer/counter-score
// (exit is never gated — operators want stuck positions to flatten).
// Operator command: New-Item ./var/clob-kill-switch -ItemType File -Force
const killSwitchPath = "./var/clob-kill-switch"

// killSwitchStaleAfter is the cutoff past which a kill switch is treated
// as forgotten test detritus rather than a live operator command. We
// learned this the hard way on 2026-06-01 when a 2026-05-17 sentinel
// silently blocked the entire CHC-STL match's BUYs.
const killSwitchStaleAfter = 24 * time.Hour

func killSwitchActive() bool {
	_, err := os.Stat(killSwitchPath)
	return err == nil
}

// killSwitchSweepStale removes the sentinel file if it predates
// killSwitchStaleAfter — runs once at daemon startup so an old file
// can't silently neutralize a fresh trading session.
func killSwitchSweepStale() {
	info, err := os.Stat(killSwitchPath)
	if err != nil {
		return
	}
	age := time.Since(info.ModTime())
	if age <= killSwitchStaleAfter {
		slog.Warn("kill switch ACTIVE at startup — recent operator file",
			"path", killSwitchPath, "age", age.Round(time.Second))
		return
	}
	if rerr := os.Remove(killSwitchPath); rerr != nil {
		slog.Error("kill switch is stale but auto-remove failed",
			"path", killSwitchPath, "age", age.Round(time.Second), "err", rerr)
		return
	}
	slog.Warn("kill switch was STALE — auto-removed at startup",
		"path", killSwitchPath, "age", age.Round(time.Second),
		"cutoff", killSwitchStaleAfter)
}

// pmMinNotionalUSD is Polymarket's minimum marketable order size. Orders
// below this notional are rejected with "invalid amount … min size: $1".
// Discovered on 2026-06-01 CHC-STL: an 11-share BUY at $0.05 ask =
// $0.55 notional bounced. We compensate by upsizing toward the floor.
const pmMinNotionalUSD = 1.0

// blowoutScoreDiff is the post-event |home - away| run gap at which we
// stop opening new positions. PM has already priced the winner near $1
// and the underdog near $0; further score deltas move tokens only
// single-digit cents — insufficient against our per-event cap +
// cooldown overhead. Confirmed on 2026-06-01 CHC-STL (5-0: STL $0.93,
// CHC $0.05; 5-1: CHC up to $0.05 then back).
const blowoutScoreDiff = 4

// maybeLiveTrade applies risk gates and, if allowed, invokes the Node SDK
// subprocess to place one real order on Polymarket. Logs the outcome and
// updates the live state counters. Never returns an error — failures are
// logged so paper-trading audit isn't disrupted.
func maybeLiveTrade(
	ctx context.Context,
	fid int64,
	tokenID string,
	bestAsk *float64,
	fmap fixtureMap,
	scoringSide, teamName, scoreAfter string,
	absScoreDiff int,
) {
	if killSwitchActive() {
		slog.Warn("live-trade: KILL SWITCH ACTIVE — BUY blocked",
			"fixture", fid, "path", killSwitchPath)
		return
	}
	if bestAsk == nil || *bestAsk <= 0 || *bestAsk >= 1 {
		slog.Warn("live-trade skipped: no usable ask", "fixture", fid)
		return
	}
	// Blowout skip: when the winning side leads by > blowoutScoreDiff
	// runs, PM has already priced the underdog at <$0.05 and the
	// favorite at >$0.95. Further score deltas yield only single-digit
	// cents of movement on either token — not enough to clear our
	// per-event cap + cooldown overhead. (Confirmed on 2026-06-01
	// CHC-STL: 5-0 STL token already at $0.93, 5-1 CHC at $0.05.)
	if absScoreDiff >= blowoutScoreDiff {
		slog.Info("live-trade skipped: blowout — winner already priced in",
			"fixture", fid, "score_diff", absScoreDiff,
			"threshold", blowoutScoreDiff, "ask", *bestAsk,
			"score", scoreAfter)
		return
	}

	// Adaptive sizing: configured size (e.g. 11 shares) at a low-priced
	// underdog token can fall below PM's $1 minimum. Bump shares up so
	// notional clears the floor, but cap at the per-event cap so we
	// never silently double exposure on a deep underdog (e.g. ask $0.02
	// would otherwise demand 50 shares = $1).
	sizeShares := liveCfg.SizeShares
	notional := (*bestAsk) * sizeShares
	if notional < pmMinNotionalUSD {
		needed := pmMinNotionalUSD / (*bestAsk)
		// Round up to next whole share.
		upSize := float64(int(needed))
		if upSize < needed {
			upSize++
		}
		// Cap by per-event cap (if configured) so a $0.01 token can't
		// blow past $5; otherwise just trust the floor.
		if liveCfg.PerEventCapUSDC > 0 {
			maxAffordable := liveCfg.PerEventCapUSDC / (*bestAsk)
			if upSize > maxAffordable {
				slog.Warn("live-trade skipped: underdog ask too low to reach $1 within per-event cap",
					"fixture", fid, "ask", *bestAsk,
					"needed_shares", upSize, "cap_allowed_shares", maxAffordable,
					"per_event_cap_usd", liveCfg.PerEventCapUSDC)
				return
			}
		}
		slog.Info("live-trade: upsized for PM min-notional floor",
			"fixture", fid, "ask", *bestAsk,
			"orig_shares", liveCfg.SizeShares, "upsized_shares", upSize,
			"new_notional_usd", (*bestAsk)*upSize)
		sizeShares = upSize
		notional = (*bestAsk) * sizeShares
	}

	if reason := liveSt.allow(fid, notional, liveCfg); reason != "" {
		slog.Info("live-trade gated",
			"fixture", fid, "reason", reason, "notional_usd", notional)
		return
	}

	tctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	t0 := time.Now()
	resp, err := trade.PlaceOrder(tctx, liveCfg.ServerURL, trade.PlaceOrderRequest{
		TokenID:   tokenID,
		Price:     *bestAsk,
		SizeShare: sizeShares,
		Side:      "BUY",
		OrderType: liveCfg.OrderType,
	})
	wallMS := time.Since(t0).Milliseconds()
	if err != nil {
		slog.Error("live-trade subprocess failed",
			"fixture", fid, "err", err, "wall_ms", wallMS)
		return
	}
	// PM sometimes returns success=true alongside a non-empty errorMsg
	// (e.g. "invalid amount … min size: $1" on 2026-06-01 — order_id was
	// empty but the success flag was still asserted). Treat any
	// non-empty errorMsg OR empty orderID as failure regardless of the
	// success flag — those are the actual signals that nothing was
	// accepted.
	if !resp.Success || strings.TrimSpace(resp.ErrorMsg) != "" || strings.TrimSpace(resp.OrderID) == "" {
		slog.Warn("live-trade PM rejected",
			"fixture", fid, "success_flag", resp.Success,
			"error_msg", resp.ErrorMsg, "order_id", resp.OrderID, "wall_ms", wallMS)
		// Even on PM reject, commit notional to per-event/daily caps so
		// repeated rejected attempts are throttled.
		liveSt.commit(fid, notional)
		return
	}
	liveSt.commit(fid, notional)
	slog.Info("LIVE ORDER POSTED",
		"fixture", fid,
		"home", fmap.HomeTeam, "away", fmap.AwayTeam,
		"scored", teamName, "score", scoreAfter, "scoring_side", scoringSide,
		"price", *bestAsk, "size_shares", sizeShares, "notional_usd", notional,
		"order_id", resp.OrderID,
		"error_msg", resp.ErrorMsg,
		"wall_ms", wallMS,
	)

	// Register position for dual-trigger exit. We only add when the FAK
	// actually filled something — takingAmount empty / "0" means PM killed
	// it with no match, so nothing to exit. Use root context (context.Background())
	// so the 5-min timer outlives this AMQP delivery handler.
	if positionMgr != nil && filledShares(resp) > 0 {
		positionMgr.Add(context.Background(), &openPosition{
			fixtureID:   fid,
			tokenID:     tokenID,
			scoringSide: scoringSide,
			sizeShares:  filledShares(resp),
			entryPrice:  *bestAsk,
			enteredAt:   time.Now().UTC(),
			buyOrderID:  resp.OrderID,
		})
	}
}

// filledShares parses PM's makingAmount (BUY = USDC out, shares in = takingAmount).
// For a BUY at price p, shares received = takingAmount / 1e6 (USDC is 6dp...
// actually clob-client returns takingAmount in SHARES not USDC for BUY).
// Empirically PM returns takingAmount as decimal share count. Fall back to 0
// on parse error — caller treats that as "FAK killed, no position".
func filledShares(resp trade.PlaceOrderResponse) float64 {
	s := strings.TrimSpace(resp.TakingAmount)
	if s == "" || s == "0" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return 0
	}
	return v
}

func topLevel(levels []polymarket.CLOBBookLevel, bid bool) (price, size *float64) {
	if len(levels) == 0 {
		return nil, nil
	}
	bestPrice := 0.0
	bestSize := 0.0
	bestIdx := -1
	for i, lv := range levels {
		p, err := strconv.ParseFloat(strings.TrimSpace(lv.Price), 64)
		if err != nil {
			continue
		}
		if bestIdx < 0 {
			bestPrice = p
			s, _ := strconv.ParseFloat(strings.TrimSpace(lv.Size), 64)
			bestSize = s
			bestIdx = i
			continue
		}
		if bid && p > bestPrice {
			bestPrice = p
			s, _ := strconv.ParseFloat(strings.TrimSpace(lv.Size), 64)
			bestSize = s
		}
		if !bid && p < bestPrice {
			bestPrice = p
			s, _ := strconv.ParseFloat(strings.TrimSpace(lv.Size), 64)
			bestSize = s
		}
	}
	if bestIdx < 0 {
		return nil, nil
	}
	return &bestPrice, &bestSize
}

func fmtPtr(p *float64) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("%.4f", *p)
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
