// Command clob-fill-monitor polls Polymarket CLOB for the terminal
// status of orders that were POSTed via cmd/clob-executor -mode submit
// -really-submit and recorded as 'sent' in proposed_orders. When a
// poll returns a terminal status (filled / unmatched / canceled), the
// row is advanced and re-checks stop.
//
// Read-only against the CLOB API (GET /data/order/{id}). Uses the same
// L2 HMAC creds the submitter wrote with cmd/clob-executor -mode
// bootstrap -really-bootstrap. NO order placement, ever — this binary
// does not import internal/clob.Sign or executor.Build.
//
// Usage:
//
//	clob-fill-monitor                         # one tick
//	clob-fill-monitor -loop 10s               # daemon, poll every 10s
//	clob-fill-monitor -loop 10s -stale 30s    # only re-check rows >30s since last check
//	clob-fill-monitor -limit 25 -base-url https://clob.polymarket.com
//
// SAFETY: -really-poll is required to actually issue HTTP requests.
// Without it, the binary lists in-flight rows and exits — useful for
// auditing what's currently in flight without burning rate-limit budget.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/pol-odds-collector/internal/clob"
	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	loop := flag.Duration("loop", 0, "poll interval (e.g. 10s); zero = one tick then exit")
	stale := flag.Duration("stale", 15*time.Second, "only re-check rows whose last_status_check_at is older than this")
	limit := flag.Int("limit", 50, "max rows per tick (rate-limit budget)")
	credsFile := flag.String("creds-file", "", "path to L2 creds JSON (default: user config dir)")
	baseURL := flag.String("base-url", clob.DefaultBaseURL, "CLOB API origin")
	reallyPoll := flag.Bool("really-poll", false, "required to actually GET /data/order/{id}; default = list-only dry-run")
	logLevel := flag.String("log-level", "info", "debug | info | warn | error")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(*logLevel)})))

	cfg, err := config.Load()
	if err != nil {
		die("config: " + err.Error())
	}
	credsPath := strings.TrimSpace(*credsFile)
	if credsPath == "" {
		credsPath = clob.DefaultCredsPath()
	}

	var creds clob.L2Credentials
	if *reallyPoll {
		f, err := clob.LoadCreds(credsPath)
		if err != nil {
			die("creds: " + err.Error())
		}
		creds = f.As()
		slog.Info("fill-monitor: armed", "base_url", *baseURL, "creds_address", f.Address, "api_key", f.APIKey)
	} else {
		slog.Info("fill-monitor: DRY-RUN (no -really-poll); will list in-flight rows only")
	}

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if *loop > 0 {
		ctx, cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	}
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
	ps := store.NewProposedOrderStore(pg)
	pos := store.NewPositionStore(pg)

	client := clob.NewClient(*baseURL, 10*time.Second)

	tick := func() {
		inflight, err := ps.ListInFlight(ctx, *stale, *limit)
		if err != nil {
			slog.Error("list in-flight", "err", err)
			return
		}
		if len(inflight) == 0 {
			slog.Info("fill-monitor: no in-flight orders due for re-check")
			return
		}
		slog.Info("fill-monitor: in-flight orders due", "count", len(inflight))
		if !*reallyPoll {
			for _, r := range inflight {
				slog.Info("would poll",
					"row_id", r.ID, "clob_order_id", r.ClobOrderID,
					"signal_id", r.SignalID, "created_at", r.CreatedAt.UTC().Format(time.RFC3339),
					"last_check", lastCheckLog(r.LastCheckedAt))
			}
			return
		}
		terminal := 0
		nonTerminal := 0
		errCount := 0
		for _, r := range inflight {
			info, err := client.GetOrder(ctx, r.ClobOrderID, creds)
			if err != nil {
				if errors.Is(err, clob.ErrOrderNotFound) {
					// 404 right after POST = the order may not be propagated yet;
					// bump check time and try again next tick. If it stays 404
					// for a while (>5min), promote to rejected.
					age := time.Since(r.CreatedAt)
					if age > 5*time.Minute {
						_ = ps.MarkTerminal(ctx, r.ID, "rejected", fmt.Sprintf("404 from CLOB for >5min (age=%s)", age))
						terminal++
						continue
					}
					_ = ps.BumpStatusCheck(ctx, r.ID, fmt.Sprintf("404 (age=%s; will retry)", age))
					nonTerminal++
					continue
				}
				slog.Warn("poll error", "row_id", r.ID, "clob_order_id", r.ClobOrderID, "err", err)
				_ = ps.BumpStatusCheck(ctx, r.ID, "poll error: "+err.Error())
				errCount++
				continue
			}
			note := fmt.Sprintf("poll: status=%s size_matched=%s/%s avg_price=%s",
				info.Status, info.SizeMatched, info.OriginalSize, info.AveragePrice)
			if info.Status.IsTerminal() {
				dst := mapTerminalStatus(info.Status)
				if err := ps.MarkTerminal(ctx, r.ID, dst, note); err != nil {
					slog.Error("mark terminal", "row_id", r.ID, "err", err)
					errCount++
					continue
				}
				// Any positive size_matched is a real position, even on a
				// 'canceled' terminal — FAK orders book the cross part and
				// cancel the rest, so partial fills survive the cancel path.
				if shares, cost, ok := parseFillFromInfo(info); ok {
					if _, perr := pos.UpsertOpenFromFill(ctx, store.Fill{
						PolymarketTokenID:     r.PolymarketTokenID,
						PolymarketConditionID: r.PolymarketConditionID,
						MatchupID:             r.MatchupID,
						OutcomeSide:           r.OutcomeSide,
						Side:                  r.Side,
						Shares:                shares,
						CostUSDC:              cost,
						FilledAt:              time.Now().UTC(),
						SourceProposedOrderID: r.ID,
					}); perr != nil {
						slog.Error("upsert position from fill", "row_id", r.ID, "err", perr)
						errCount++
					}
				}
				slog.Info("terminal", "row_id", r.ID, "status", dst, "clob_status", info.Status,
					"filled", info.SizeMatched, "of", info.OriginalSize, "avg_price", info.AveragePrice)
				terminal++
				continue
			}
			_ = ps.BumpStatusCheck(ctx, r.ID, note)
			slog.Debug("non-terminal", "row_id", r.ID, "status", info.Status)
			nonTerminal++
		}
		slog.Info("fill-monitor tick done",
			"polled", len(inflight), "terminal", terminal, "non_terminal", nonTerminal, "errors", errCount)
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
			return
		case <-t.C:
			tick()
		}
	}
}

// mapTerminalStatus normalises CLOB-side terminal codes into the
// proposed_orders status enum. filled/canceled flow through 1:1;
// unmatched (FAK with no liquidity) is recorded as 'canceled' rather
// than 'rejected' because the order was accepted, just nothing crossed.
// parseFillFromInfo derives (shares, cost_usdc) from a CLOB OrderInfo
// for use by the position writer. Polymarket exposes shares as decimal
// strings (numeric over the wire) and average_price as a probability
// 0-1; cost = shares × avg_price. Returns ok=false when size_matched is
// zero, non-numeric, or the average price is missing — i.e. nothing
// crossed and there's no position to record.
func parseFillFromInfo(info clob.OrderInfo) (shares, cost float64, ok bool) {
	shares, err := strconv.ParseFloat(strings.TrimSpace(info.SizeMatched), 64)
	if err != nil || shares <= 0 {
		return 0, 0, false
	}
	avg, err := strconv.ParseFloat(strings.TrimSpace(info.AveragePrice), 64)
	if err != nil || avg <= 0 {
		return 0, 0, false
	}
	return shares, shares * avg, true
}

func mapTerminalStatus(s clob.OrderStatus) string {
	switch s {
	case clob.OrderStatusFilled, clob.OrderStatusMatched:
		return "filled"
	case clob.OrderStatusCanceled, clob.OrderStatusUnmatched:
		return "canceled"
	default:
		return "canceled"
	}
}

func lastCheckLog(t sql.NullTime) string {
	if !t.Valid {
		return "never"
	}
	return t.Time.UTC().Format(time.RFC3339)
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

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
