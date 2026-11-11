// Command executor-plan is the planner side of the Polymarket CLOB
// executor (Step 5). It reads signals_buy_candidates_fresh, builds the
// exact order body that WOULD be POSTed to
// https://clob.polymarket.com/order, and appends a row to proposed_orders
// for human review.
//
// This command performs NO wallet IO, NO EIP-712 signing, and NO HTTP
// submission. Real on-chain execution lives in a future cmd/executor
// per docs/step5_executor_plan.md.
//
//	go run ./cmd/executor-plan -mode plan -maker 0xabc...
//	go run ./cmd/executor-plan -mode plan -maker 0xabc... -override-size 10
//	go run ./cmd/executor-plan -mode list -since 1h
//	go run ./cmd/executor-plan -mode list -status planned -limit 100
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"text/tabwriter"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/executor"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	mode := flag.String("mode", "plan", "plan | list")
	makerAddr := flag.String("maker", "", "Polygon EOA address that will sign + pay (required for plan)")
	orderType := flag.String("order-type", "FAK", "GTC | FOK | GTD | FAK")
	expirationFromNow := flag.Duration("expiration", 0, "expiration relative to now (0 = no expiry)")
	overrideSize := flag.Float64("override-size", 0, "override every signal's proposed_size_usdc")
	notes := flag.String("notes", "", "free-text annotation attached to each row")
	since := flag.Duration("since", time.Hour, "list mode: look-back window")
	statusFilter := flag.String("status", "", "list mode: filter by status (planned|sent|rejected|canceled|filled)")
	limit := flag.Int("limit", 50, "list mode: max rows")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	es := store.NewExecutorStore(pg)

	switch *mode {
	case "plan":
		if *makerAddr == "" {
			slog.Error("-maker is required for plan mode")
			os.Exit(2)
		}
		runPlan(ctx, es, executor.BuildConfig{
			MakerAddress:      *makerAddr,
			OrderType:         executor.OrderType(*orderType),
			ExpirationFromNow: *expirationFromNow,
			SizeOverrideUSDC:  *overrideSize,
		}, *notes)
	case "list":
		runList(ctx, es, *since, *statusFilter, *limit)
	default:
		slog.Error("unknown -mode", "mode", *mode)
		os.Exit(2)
	}
}

func runPlan(ctx context.Context, es *store.ExecutorStore, bcfg executor.BuildConfig, notes string) {
	sigs, err := es.FreshBuyCandidates(ctx)
	if err != nil {
		slog.Error("FreshBuyCandidates", "err", err)
		os.Exit(1)
	}
	if len(sigs) == 0 {
		fmt.Println("(no fresh BUY_CANDIDATE signals)")
		return
	}
	planned, deduped, failed := 0, 0, 0
	for _, s := range sigs {
		spec, err := executor.Build(s, bcfg)
		if err != nil {
			slog.Warn("Build skipped", "signal_id", s.ID, "matchup", s.MatchupID, "err", err)
			failed++
			continue
		}
		if err := es.InsertProposedOrder(ctx, spec, notes); err != nil {
			if errors.Is(err, store.ErrDuplicate) {
				deduped++
				continue
			}
			slog.Warn("InsertProposedOrder", "signal_id", s.ID, "matchup", s.MatchupID, "err", err)
			failed++
			continue
		}
		planned++
		slog.Info("planned",
			"signal_id", s.ID, "matchup", s.MatchupID, "side", spec.OutcomeSide,
			"token", trunc(spec.TokenID, 14), "price", spec.Price, "size_usdc", spec.SizeUSDC,
			"maker", spec.MakerAmount.String(), "taker", spec.TakerAmount.String())
	}
	fmt.Printf("\nplanned=%d  deduped=%d  failed=%d  (from %d fresh BUY candidates)\n",
		planned, deduped, failed, len(sigs))
	fmt.Println("review with: go run ./cmd/executor-plan -mode list")
}

func runList(ctx context.Context, es *store.ExecutorStore, since time.Duration, status string, limit int) {
	rows, err := es.ListProposedOrders(ctx, since, status, limit)
	if err != nil {
		slog.Error("ListProposedOrders", "err", err)
		os.Exit(1)
	}
	if len(rows) == 0 {
		fmt.Println("(no proposed orders)")
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCREATED\tSTATUS\tMATCHUP\tLEAGUE\tHOME\tAWAY\tSIDE\tSIZE\tPRICE\tMAKER\tTAKER\tCLAIM_PP")
	for _, r := range rows {
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t$%.2f\t%.3f\t%d\t%d\t%+.2f\n",
			r.ID, r.CreatedAt.Format("01-02 15:04:05"), r.Status, r.MatchupID,
			trunc(r.LeagueName, 22), trunc(r.HomeName, 14), trunc(r.AwayName, 14),
			r.Side, r.SizeUSDC, r.Price,
			r.MakerAmount6Dec, r.TakerAmount6Dec, r.ClaimedNetEdge*100)
	}
	w.Flush()
	fmt.Printf("\n%d row(s)\n", len(rows))
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
