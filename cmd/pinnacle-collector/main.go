// Command pinnacle-collector is the production live-odds polling loop
// against Pinnacle's public guest API. It writes raw league/matchup/market
// state to PostgreSQL (pinnacle_* tables, migration 021). It is read-only
// against Pinnacle — no bets are placed.
//
//	go run ./cmd/pinnacle-collector
//
// Configure via .env (see PINNACLE_* and DATABASE_URL keys).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/pinnacle"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	logLevel := flag.String("log-level", "info", "debug | info | warn | error")
	dryRun := flag.Bool("dry-run", false, "do not connect to Postgres; print activity to stdout only")
	onlySoon := flag.Duration("only-soon", 0, "only poll markets for matchups that are live or start within this duration (default 0 = poll all hasMarkets matchups)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(*logLevel)}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(2)
	}
	if len(cfg.PinnacleSportIDs) == 0 {
		slog.Error("PINNACLE_SPORT_IDS must list at least one sport (e.g. 12 for E Sports)")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := pinnacle.NewClient(
		cfg.PinnacleBaseURL,
		cfg.PinnacleAPIKey,
		cfg.HTTPTimeout,
	)

	var sink pinnacle.Sink
	if *dryRun {
		sink = &stdoutSink{log: slog.Default()}
		slog.Info("pinnacle-collector: dry-run mode, no Postgres writes")
	} else {
		pg, err := store.Open(ctx, cfg.DatabaseURL, false, normalize.FreshnessPolicy{
			FreshAfter: cfg.FreshAfter,
			AgingAfter: cfg.AgingAfter,
		})
		if err != nil {
			slog.Error("open postgres", "err", err)
			os.Exit(1)
		}
		defer func() { _ = pg.Close() }()
		sink = store.NewPinnacleStore(pg)
	}

	collector := pinnacle.NewCollector(client, sink, pinnacle.Options{
		SportIDs:            cfg.PinnacleSportIDs,
		LeagueNameIncludes:  cfg.PinnacleLeagueNameIncludes,
		LeagueNameExcludes:  cfg.PinnacleLeagueNameExcludes,
		LeaguePollInterval:  cfg.PinnacleLeaguePollInterval,
		MatchupPollInterval: cfg.PinnacleMatchupPollInterval,
		MarketsPollInterval: cfg.PinnacleMarketsPollInterval,
		MarketsConcurrency:  cfg.PinnacleMarketsConcurrency,
		OnlyLiveOrSoon:      *onlySoon,
		Logger:              slog.Default(),
	})

	slog.Info("pinnacle-collector starting",
		"sport_ids", cfg.PinnacleSportIDs,
		"league_filter", cfg.PinnacleLeagueNameIncludes,
		"league_excludes", cfg.PinnacleLeagueNameExcludes,
		"league_poll", cfg.PinnacleLeaguePollInterval,
		"matchup_poll", cfg.PinnacleMatchupPollInterval,
		"markets_poll", cfg.PinnacleMarketsPollInterval,
		"markets_concurrency", cfg.PinnacleMarketsConcurrency,
		"only_soon", *onlySoon,
		"dry_run", *dryRun,
	)

	if err := collector.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("collector exited", "err", err)
		os.Exit(1)
	}
	slog.Info("pinnacle-collector stopped")
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// stdoutSink implements pinnacle.Sink as a no-op tracer for dry-run mode.
type stdoutSink struct{ log *slog.Logger }

func (s *stdoutSink) UpsertLeagues(_ context.Context, sportID int, leagues []pinnacle.League) error {
	s.log.Info("upsertLeagues", "sport_id", sportID, "count", len(leagues))
	for _, l := range leagues {
		s.log.Debug("  league", "id", l.ID, "name", l.Name, "matchups", l.MatchupCount)
	}
	return nil
}

func (s *stdoutSink) UpsertMatchups(_ context.Context, leagueID int, matchups []pinnacle.Matchup) (int, error) {
	main := 0
	live := 0
	for _, m := range matchups {
		if m.Type == "matchup" {
			main++
		}
		if m.IsLive {
			live++
		}
	}
	s.log.Info("upsertMatchups", "league_id", leagueID, "total", len(matchups), "main", main, "live", live)
	return main, nil
}

func (s *stdoutSink) SaveMarketSnapshot(_ context.Context, m pinnacle.Market) (bool, error) {
	if m.Type != "moneyline" || m.IsAlternate || m.Period != 0 {
		return true, nil
	}
	nv := pinnacle.NoVigTwoWay(m.Prices)
	pn := func(i int) string {
		if i < len(m.Prices) {
			return m.Prices[i].Designation
		}
		return ""
	}
	var nvStr string
	if len(nv) == 2 {
		nvStr = fmt.Sprintf("no-vig %s=%.1f%% %s=%.1f%%", pn(0), nv[0]*100, pn(1), nv[1]*100)
	}
	s.log.Info("ML(period=0)",
		"matchup_id", m.MatchupID, "status", m.Status, "version", m.Version, "novig", nvStr)
	return true, nil
}

func (s *stdoutSink) SweepStaleLiveFlags(_ context.Context, sportID int, maxAge time.Duration) (int64, error) {
	return 0, nil
}

func (s *stdoutSink) StartRun(_ context.Context, sportID int) (int64, error) {
	return time.Now().UnixNano(), nil
}

func (s *stdoutSink) FinishRun(_ context.Context, _ int64, r pinnacle.RunSummary) error {
	s.log.Info("tick summary",
		"matchups", r.MatchupsSeen, "markets_seen", r.MarketsSeen,
		"inserted", r.MarketsInserted, "unchanged", r.MarketsUnchanged,
		"http_errs", r.HTTPErrors, "status", r.Status)
	return nil
}
