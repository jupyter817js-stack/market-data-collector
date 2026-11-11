// Command clob-exit-monitor evaluates every open position against the
// exit thresholds and writes one exit_signals row per tick per position.
// It does NOT place SELL orders today — execution is gated on Step 7
// just like clob-auto-submit. Once the operator funds the wallet and is
// ready to trade, an exit-side submitter daemon can read the latest
// EXIT_* state from exit_signals_latest and POST the SELL / merge order.
//
// Read-only against the network. Reads positions + position_exit_routes
// + (optionally) polymarket_resolved_markets to detect settled markets.
// Writes only into exit_signals.
//
// Usage:
//
//	clob-exit-monitor                              # one tick, exit
//	clob-exit-monitor -loop 30s                    # daemon
//	clob-exit-monitor -profit 0.05 -stop 0.30 -max-hold 6h
package main

import (
	"context"
	"database/sql"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/exit"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	loop := flag.Duration("loop", 0, "tick interval (e.g. 30s); zero = one tick then exit")
	profit := flag.Float64("profit", 0.05, "EXIT_PROFIT when unrealized PnL fraction >= this (0.05 = +5%)")
	stop := flag.Float64("stop", 0.30, "EXIT_STOP_LOSS when unrealized PnL fraction <= -this (0.30 = -30%)")
	maxHold := flag.Duration("max-hold", 6*time.Hour, "EXIT_TIME after this much elapsed since opened_at")
	minBid := flag.Float64("min-bid", 0.05, "EXIT_LIQUIDITY_RISK when held bid drops below this; 0 disables")
	logLevel := flag.String("log-level", "info", "debug | info | warn | error")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(*logLevel)})))

	cfg, err := config.Load()
	if err != nil {
		die("config: " + err.Error())
	}
	exitCfg := exit.Config{
		ProfitTargetFrac:  *profit,
		StopLossFrac:      *stop,
		MaxHoldDuration:   *maxHold,
		MinBidForLiveExit: *minBid,
	}
	slog.Info("clob-exit-monitor: starting",
		"loop", *loop,
		"profit_target_frac", exitCfg.ProfitTargetFrac,
		"stop_loss_frac", exitCfg.StopLossFrac,
		"max_hold", exitCfg.MaxHoldDuration,
		"min_bid", exitCfg.MinBidForLiveExit)

	ctx, cancel := contextFor(*loop)
	defer cancel()

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		die("open db: " + err.Error())
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		die("ping db: " + err.Error())
	}

	pg, err := store.Open(ctx, cfg.DatabaseURL, false, normalize.FreshnessPolicy{
		FreshAfter: cfg.FreshAfter,
		AgingAfter: cfg.AgingAfter,
	})
	if err != nil {
		die("open postgres store: " + err.Error())
	}
	defer func() { _ = pg.Close() }()
	pos := store.NewPositionStore(pg)

	tick := func() {
		open, err := pos.ListOpenExitRoutes(ctx)
		if err != nil {
			slog.Error("list open positions", "err", err)
			return
		}
		if len(open) == 0 {
			slog.Info("exit-monitor tick: no open positions")
			return
		}
		settled := loadSettledFlags(ctx, db, open)
		now := time.Now().UTC()
		states := map[exit.State]int{}
		emitted := 0
		for _, op := range open {
			p := exit.Position{
				ID:                    op.PositionID,
				PolymarketTokenID:     op.PolymarketTokenID,
				PolymarketConditionID: op.PolymarketConditionID,
				CostUSDC:              op.CostUSDC,
				Shares:                op.SharesFilled,
				HeldBid:               nullFloat(op.HeldBid),
				HeldAsk:               nullFloat(op.HeldAsk),
				SumOppositeAsk:        nullFloat(op.SumOppositeAsk),
				DirectSellValue:       op.DirectSellValue,
				SyntheticSellValue:    op.SyntheticSellValue,
				BestExitValue:         op.BestExitValue,
				RecommendedRoute:      op.RecommendedExit,
			}
			if s, ok := settled[op.PolymarketConditionID]; ok {
				p.MarketSettled = true
				p.WinnerOnSettle = s.winningTokenID == op.PolymarketTokenID
			}
			// We don't have OpenedAt on OpenPosition projection (lives on
			// positions row). Fetch it lazily.
			openedAt, perr := openedAtFor(ctx, db, op.PositionID)
			if perr != nil {
				slog.Warn("opened_at lookup", "position_id", op.PositionID, "err", perr)
				continue
			}
			p.OpenedAt = openedAt
			sig := exit.Classify(p, exitCfg, now)
			if _, perr := pos.InsertExitSignal(ctx, sig); perr != nil {
				slog.Error("InsertExitSignal", "position_id", op.PositionID, "err", perr)
				continue
			}
			emitted++
			states[sig.State]++
			if sig.State != exit.StateHold {
				slog.Info("exit candidate",
					"position_id", op.PositionID, "state", sig.State,
					"route", sig.RecommendedRoute, "unrealized_pct", sig.UnrealizedPnLPct,
					"unrealized_usdc", sig.UnrealizedPnLUSDC, "reason", sig.Reasons[0])
			}
		}
		slog.Info("exit-monitor tick done",
			"open_positions", len(open), "emitted", emitted, "states", states)
	}

	if *loop <= 0 {
		tick()
		return
	}
	t := time.NewTicker(*loop)
	defer t.Stop()
	tick()
	for {
		select {
		case <-ctx.Done():
			slog.Info("clob-exit-monitor: stopping")
			return
		case <-t.C:
			tick()
		}
	}
}

type settledMarket struct {
	winningTokenID string
}

// loadSettledFlags returns a map of condition_id -> settled outcome for
// every open position whose market is in polymarket_resolved_markets.
// Markets still in 'proposed' UMA state appear here too (migration 038
// surfaces them).
func loadSettledFlags(ctx context.Context, db *sql.DB, open []store.OpenPosition) map[string]settledMarket {
	out := map[string]settledMarket{}
	if len(open) == 0 {
		return out
	}
	ids := make([]string, 0, len(open))
	seen := map[string]bool{}
	for _, op := range open {
		if op.PolymarketConditionID == "" || seen[op.PolymarketConditionID] {
			continue
		}
		seen[op.PolymarketConditionID] = true
		ids = append(ids, op.PolymarketConditionID)
	}
	if len(ids) == 0 {
		return out
	}
	rows, err := db.QueryContext(ctx, `
        SELECT condition_id, COALESCE(winning_token_id, '')
          FROM polymarket_resolved_markets
         WHERE condition_id = ANY($1)
    `, ids)
	if err != nil {
		slog.Warn("loadSettledFlags", "err", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var cid, winner string
		if err := rows.Scan(&cid, &winner); err == nil && cid != "" {
			out[cid] = settledMarket{winningTokenID: winner}
		}
	}
	return out
}

func openedAtFor(ctx context.Context, db *sql.DB, positionID int64) (time.Time, error) {
	var t time.Time
	err := db.QueryRowContext(ctx,
		`SELECT opened_at FROM positions WHERE id = $1`, positionID).Scan(&t)
	return t, err
}

func nullFloat(n sql.NullFloat64) float64 {
	if n.Valid {
		return n.Float64
	}
	return 0
}

func contextFor(loop time.Duration) (context.Context, context.CancelFunc) {
	if loop > 0 {
		return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	}
	return context.WithTimeout(context.Background(), 30*time.Second)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
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

func die(msg string) {
	slog.Error(msg)
	os.Exit(2)
}
