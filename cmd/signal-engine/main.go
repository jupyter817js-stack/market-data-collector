// Command signal-engine is the dry-run signal layer of the live pipeline.
// It polls pinnacle_polymarket_cs2_moneyline_edges, classifies each row via
// internal/signal.Classify, and appends one row per (matchup, outcome) per
// tick to the signals table. It does NOT place orders — Step 5 introduces
// the executor that reads BUY_CANDIDATE rows from this audit trail.
//
// Modes:
//
//	go run ./cmd/signal-engine                        # one tick, exit
//	go run ./cmd/signal-engine -loop                  # continuous (interval from SIGNAL_POLL_INTERVAL_MS)
//	go run ./cmd/signal-engine -mode list             # recent signals (default 20)
//	go run ./cmd/signal-engine -mode list -state BUY_CANDIDATE -limit 50
//
// All flags can be combined with -dry-run-stdout to skip Postgres writes
// and just print classifications (useful before applying migration 023).
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	sigpkg "github.com/your-org/pol-odds-collector/internal/signal"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	mode := flag.String("mode", "tick", "tick | list")
	loop := flag.Bool("loop", false, "tick mode: continuous loop using SIGNAL_POLL_INTERVAL_MS")
	stateFilter := flag.String("state", "", "list mode: filter by state (e.g. BUY_CANDIDATE)")
	limit := flag.Int("limit", 20, "list mode: max rows")
	dryStdout := flag.Bool("dry-run-stdout", false, "skip Postgres writes; print classifications to stdout")
	logLevel := flag.String("log-level", "info", "debug | info | warn | error")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(*logLevel)}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(2)
	}

	engineCfg := sigpkg.Config{
		ProposedSizeUSDC:     cfg.SignalProposedSizeUSDC,
		MinNetEdgeBuy:        cfg.SignalMinNetEdgeBuy,
		MinGrossEdgeSell:     cfg.SignalMinGrossEdgeSell,
		MaxPinnacleAgeSec:    cfg.SignalMaxPinnacleAgeSec,
		MaxPMAgeSec:          cfg.SignalMaxPMAgeSec,
		MinPMLiquidityUSDC:   cfg.SignalMinPMLiquidityUSDC,
		SpreadCostMultiplier: cfg.SignalSpreadCostMultiplier,
		SlippageBaseBps:      cfg.SignalSlippageBaseBps,
		SlippageVsLiqBps:     cfg.SignalSlippageVsLiqBps,
		LatencyBufferPerSec:  cfg.SignalLatencyBufferPerSec,
		OverroundShare:       cfg.SignalOverroundShare,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pg, err := store.Open(ctx, cfg.DatabaseURL, false, normalize.FreshnessPolicy{
		FreshAfter: cfg.FreshAfter,
		AgingAfter: cfg.AgingAfter,
	})
	if err != nil {
		slog.Error("open postgres", "err", err)
		os.Exit(1)
	}
	defer func() { _ = pg.Close() }()
	ss := store.NewSignalStore(pg)

	switch *mode {
	case "tick":
		runTick(ctx, ss, engineCfg, cfg.SignalPollInterval, *loop, *dryStdout)
	case "list":
		runList(ctx, ss, *stateFilter, *limit)
	default:
		slog.Error("unknown -mode", "mode", *mode)
		os.Exit(2)
	}
}

func runTick(ctx context.Context, ss *store.SignalStore, cfg sigpkg.Config, interval time.Duration, loop, dryStdout bool) {
	doOne := func() {
		runID := int64(0)
		if !dryStdout {
			id, err := ss.StartRun(ctx)
			if err != nil {
				slog.Warn("StartRun failed", "err", err)
			}
			runID = id
		}

		edges, err := ss.ReadEdges(ctx, cfg.ProposedSizeUSDC)
		if err != nil {
			slog.Error("ReadEdges failed", "err", err)
			if runID > 0 {
				_ = ss.FinishRun(ctx, runID, 0, 0, nil, "error", err.Error())
			}
			return
		}

		sigs := make([]sigpkg.Signal, 0, len(edges))
		for _, e := range edges {
			sigs = append(sigs, sigpkg.Classify(e, cfg))
		}
		// Promote EDGE_TOO_SMALL on side B to BUY_CANDIDATE when side A on the
		// same 2-way matchup is OVERVALUED. We can't sell A from a flat book,
		// but buying B captures the same binary-market divergence.
		sigs = sigpkg.PromoteFadePairs(sigs)

		emitted := 0
		if dryStdout {
			printSignals(sigs)
			emitted = len(sigs)
		} else {
			for _, s := range sigs {
				if err := ss.InsertSignal(ctx, s); err != nil {
					slog.Warn("InsertSignal failed", "matchup_id", s.MatchupID, "err", err)
					continue
				}
				emitted++
			}
		}

		counts := sigpkg.CountByState(sigs)
		slog.Info("signal tick",
			"edges_seen", len(edges),
			"emitted", emitted,
			"states", counts,
		)

		if runID > 0 {
			_ = ss.FinishRun(ctx, runID, len(edges), emitted, counts, "success", "")
		}
	}

	if !loop {
		doOne()
		return
	}

	if interval <= 0 {
		interval = 2 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	doOne()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			doOne()
		}
	}
}

func runList(ctx context.Context, ss *store.SignalStore, state string, limit int) {
	sigs, err := ss.ListRecentSignals(ctx, state, limit)
	if err != nil {
		slog.Error("ListRecentSignals failed", "err", err)
		os.Exit(1)
	}
	if len(sigs) == 0 {
		fmt.Println("(no signals)")
		return
	}
	// stable order by net edge desc within the result page
	sort.SliceStable(sigs, func(i, j int) bool { return sigs[i].NetEdgeBuy > sigs[j].NetEdgeBuy })
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "STATE\tMATCHUP\tLEAGUE\tHOME\tAWAY\tOUT\tFAIR\tASK\tBID\tGROSS\tNET\tPIN_AGE\tPM_AGE")
	for _, s := range sigs {
		fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\t%s\t%.3f\t%s\t%s\t%+.3f\t%+.3f\t%.1f\t%.1f\n",
			s.State, s.MatchupID, truncate(s.LeagueName, 22),
			truncate(s.HomeName, 14), truncate(s.AwayName, 14),
			s.OutcomeSide,
			s.PinnacleFairProb,
			nullF(s.PMAsk), nullF(s.PMBid),
			s.GrossEdgeToBuy, s.NetEdgeBuy,
			s.PinnacleAgeSec, s.PMAgeSec,
		)
	}
	w.Flush()
}

func printSignals(sigs []sigpkg.Signal) {
	for _, s := range sigs {
		j, _ := json.Marshal(map[string]any{
			"matchup":     s.MatchupID,
			"league":      s.LeagueName,
			"home":        s.HomeName,
			"away":        s.AwayName,
			"side":        s.OutcomeSide,
			"state":       s.State,
			"fair":        round4(s.PinnacleFairProb),
			"ask":         nullF(s.PMAsk),
			"bid":         nullF(s.PMBid),
			"gross_buy":   round4(s.GrossEdgeToBuy),
			"gross_sell":  round4(s.GrossEdgeToSell),
			"spread_cost": round4(s.SpreadCost),
			"slip":        round4(s.SlippageBuffer),
			"latency":     round4(s.LatencyBuffer),
			"overround":   round4(s.OverroundBuffer),
			"net_buy":     round4(s.NetEdgeBuy),
			"pin_age":     round1(s.PinnacleAgeSec),
			"pm_age":      round1(s.PMAgeSec),
			"reasons":     s.Reasons,
		})
		fmt.Println(string(j))
	}
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

func nullF(v sql.NullFloat64) string {
	if !v.Valid {
		return "-"
	}
	return fmt.Sprintf("%.3f", v.Float64)
}

func round4(f float64) float64 { return float64(int64(f*10000+0.5)) / 10000 }
func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
