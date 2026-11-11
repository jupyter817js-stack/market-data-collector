// Command paper-pnl simulates virtual trades from the signals audit trail.
// v0 is mark-to-market only — realized PnL requires Polymarket resolution
// data we don't yet ingest. This tool answers "if I had bought every
// BUY_CANDIDATE at its recorded ask, how does my position mark right now?"
//
//	go run ./cmd/paper-pnl -since 24h -mode summary
//	go run ./cmd/paper-pnl -since 6h  -mode trades -limit 50
//	go run ./cmd/paper-pnl -since 24h -mode summary -write-run -notes "post-IEM Atlanta"
//	go run ./cmd/paper-pnl -since 24h -override-size 10  (sensitivity check)
//	go run ./cmd/paper-pnl -since 24h -no-dedupe         (allow re-fires)
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/paper"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	since := flag.Duration("since", 24*time.Hour, "look-back window")
	mode := flag.String("mode", "summary", "summary | trades")
	limit := flag.Int("limit", 50, "trades mode: max rows printed")
	overrideSize := flag.Float64("override-size", 0, "override every signal's proposed_size_usdc")
	noDedupe := flag.Bool("no-dedupe", false, "disable first-per-(matchup,outcome) dedupe")
	writeRun := flag.Bool("write-run", false, "record summary to paper_pnl_runs")
	notes := flag.String("notes", "", "free-text annotation when -write-run")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pg, err := store.Open(ctx, cfg.DatabaseURL, false, normalize.FreshnessPolicy{
		FreshAfter: cfg.FreshAfter,
		AgingAfter: cfg.AgingAfter,
	})
	if err != nil {
		slog.Error("open postgres", "err", err)
		os.Exit(1)
	}
	defer func() { _ = pg.Close() }()
	ps := store.NewPaperStore(pg)

	end := time.Now().UTC()
	start := end.Add(-*since)

	sigs, err := ps.ReadBuyCandidates(ctx, start, end)
	if err != nil {
		slog.Error("ReadBuyCandidates", "err", err)
		os.Exit(1)
	}
	prices, err := ps.LatestPolymarketPrices(ctx)
	if err != nil {
		slog.Error("LatestPolymarketPrices", "err", err)
		os.Exit(1)
	}
	resolutions, err := ps.LatestResolutions(ctx)
	if err != nil {
		slog.Warn("LatestResolutions (continuing without realized PnL)", "err", err)
		resolutions = nil
	}

	simCfg := paper.Config{
		DedupePerMatchupOutcome: !*noDedupe,
		OverrideSizeUSDC:        *overrideSize,
	}
	trades, summary := paper.Simulate(sigs, prices, resolutions, simCfg)

	runID := int64(0)
	if *writeRun {
		size := *overrideSize
		if size == 0 && len(sigs) > 0 {
			// Record the actual size used (first signal's proposed size is the
			// canonical config; they're all the same engine config in practice).
			size = sigs[0].ProposedSizeUSDC
		}
		if size == 0 {
			size = cfg.SignalProposedSizeUSDC
		}
		id, err := ps.StartRun(ctx, start, end, size, !*noDedupe)
		if err != nil {
			slog.Warn("StartRun", "err", err)
		}
		runID = id
		if runID > 0 {
			if err := ps.FinishRun(ctx, runID, summary, *notes, "success"); err != nil {
				slog.Warn("FinishRun", "err", err)
			}
		}
	}

	switch *mode {
	case "summary":
		printSummary(start, end, summary, simCfg, runID)
	case "trades":
		printTrades(trades, *limit)
		fmt.Println()
		printSummary(start, end, summary, simCfg, runID)
	case "buckets":
		rows := paper.BucketByEdge(trades, paper.DefaultEdgeBuckets())
		fmt.Println("==== paper-pnl bucket study ====")
		fmt.Printf("window: %s  →  %s  (%s)\n",
			start.Format(time.RFC3339), end.Format(time.RFC3339), end.Sub(start))
		fmt.Println(paper.FormatBuckets(rows))
		fmt.Println()
		printSummary(start, end, summary, simCfg, runID)
	default:
		slog.Error("unknown -mode", "mode", *mode)
		os.Exit(2)
	}
}

func printSummary(start, end time.Time, s paper.Summary, cfg paper.Config, runID int64) {
	fmt.Println("==== paper-pnl summary ====")
	fmt.Printf("window           : %s  →  %s  (%s)\n",
		start.Format(time.RFC3339), end.Format(time.RFC3339), end.Sub(start))
	fmt.Printf("dedupe per m+side: %v\n", cfg.DedupePerMatchupOutcome)
	if cfg.OverrideSizeUSDC > 0 {
		fmt.Printf("override size    : $%.2f\n", cfg.OverrideSizeUSDC)
	}
	fmt.Printf("candidates seen  : %d\n", s.CandidatesSeen)
	fmt.Printf("trades simulated : %d  (with MTM: %d, unpriced: %d)\n",
		s.TradesSimulated, s.TradesWithMTM, s.TradesUnpriced)
	fmt.Printf("total size       : $%.2f\n", s.TotalSizeUSDC)
	fmt.Printf("total MTM PnL    : $%+.4f\n", s.TotalMTMPnLUSDC)
	if s.TotalSizeUSDC > 0 {
		fmt.Printf("MTM PnL / size   : %+.4f%%\n", 100*s.TotalMTMPnLUSDC/s.TotalSizeUSDC)
	}
	fmt.Printf("avg claimed net  : %+.4fpp\n", s.AvgClaimedNetEdgePP)
	fmt.Printf("avg MTM edge     : %+.4fpp  (gap from claim: %+.4fpp)\n",
		s.AvgMTMEdgePP, s.AvgMTMEdgePP-s.AvgClaimedNetEdgePP)
	if s.TradesResolved > 0 {
		fmt.Printf("---- realized ----\n")
		fmt.Printf("resolved trades  : %d / %d (%.1f%% of simulated)\n",
			s.TradesResolved, s.TradesSimulated,
			100*float64(s.TradesResolved)/float64(s.TradesSimulated))
		fmt.Printf("wins             : %d  (hit rate %.1f%%)\n", s.TradesWon, s.HitRate*100)
		fmt.Printf("realized PnL     : $%+.4f\n", s.TotalRealizedPnLUSDC)
		if s.TotalSizeUSDC > 0 {
			fmt.Printf("realized / size  : %+.4f%% (resolved trades only)\n",
				100*s.TotalRealizedPnLUSDC/float64(s.TradesResolved)*float64(s.TradesResolved)/s.TotalSizeUSDC)
		}
	} else {
		fmt.Printf("(no resolved trades in window — realized PnL pending)\n")
	}
	if runID > 0 {
		fmt.Printf("recorded as paper_pnl_runs id=%d\n", runID)
	}
}

func printTrades(trades []paper.Trade, limit int) {
	if len(trades) == 0 {
		fmt.Println("(no trades)")
		return
	}
	// Most negative PnL first so we surface the bad ones in the first page.
	sorted := make([]paper.Trade, len(trades))
	copy(sorted, trades)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].MTMPnLUSDC < sorted[j].MTMPnLUSDC })
	if len(sorted) > limit {
		sorted = sorted[:limit]
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "OPENED\tMATCHUP\tLEAGUE\tHOME\tAWAY\tSIDE\tASK\tMID\tSIZE\tMTM\tREAL\tRES\tCLAIM_PP")
	for _, t := range trades_top(sorted, limit) {
		mid := "-"
		mtm := "-"
		if t.HasMTM {
			mid = fmt.Sprintf("%.3f", t.MarkPrice)
			mtm = fmt.Sprintf("%+.2f", t.MTMPnLUSDC)
		}
		real := "-"
		resTag := "-"
		if t.IsResolved {
			real = fmt.Sprintf("%+.2f", t.RealizedPnLUSDC)
			if t.Won {
				resTag = "WIN"
			} else {
				resTag = "LOSS"
			}
		}
		fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\t%s\t%.3f\t%s\t%.0f\t%s\t%s\t%s\t%+.2f\n",
			t.OpenedAt.Format("01-02 15:04:05"),
			t.MatchupID, truncate(t.LeagueName, 22),
			truncate(t.HomeName, 14), truncate(t.AwayName, 14),
			t.OutcomeSide,
			t.EntryAsk, mid,
			t.SizeUSDC, mtm, real, resTag,
			t.ClaimedNetEdge*100,
		)
	}
	w.Flush()
}

func trades_top(in []paper.Trade, limit int) []paper.Trade {
	if len(in) <= limit {
		return in
	}
	return in[:limit]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
