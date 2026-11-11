// cmd/clob-trade-live places one live order on Polymarket CLOB v2 via
// the Node SDK subprocess bridge. This is the Go entry point that
// production daemons (paper-baseball, auto-discover) can call once
// mode B (proxy + sigType=3) trading is confirmed working.
//
// Default order type is FAK with auto-kill so an un-matchable bid
// (e.g. $0.02 BUY on a market with best_ask >> $0.02) returns an
// orderID and self-cancels without locking funds — useful for live
// connectivity / authentication tests.
//
// Usage:
//
//	# Connectivity test (FAK $0.02, $1.2 notional, auto-killed):
//	go run ./cmd/clob-trade-live \
//	    --token 1485...840 --price 0.02 --size 60
//
//	# Real GTC maker quote (caller MUST manage exit):
//	go run ./cmd/clob-trade-live \
//	    --token 1485...840 --price 0.45 --size 11 --order-type GTC
//
// Exits 0 on PM success=true (even if FAK kills with no match —
// orderID still returned). Exits 1 on subprocess / network failure.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/trade"
)

func main() {
	var (
		token     = flag.String("token", "", "decimal token id (required)")
		price     = flag.Float64("price", 0, "limit price (0 < p < 1) (required)")
		sizeShare = flag.Float64("size", 0, "size in SHARES (required)")
		side      = flag.String("side", "BUY", "BUY | SELL")
		orderType = flag.String("order-type", "FAK", "GTC | FAK | FOK | GTD")
		serverURL = flag.String("server-url", trade.DefaultServerURL, "persistent Node trade server URL")
		timeout   = flag.Duration("timeout", 15*time.Second, "HTTP deadline")
	)
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if _, err := config.Load(); err != nil {
		die("config.Load: " + err.Error())
	}

	if *token == "" {
		die("--token required")
	}
	if *price <= 0 || *price >= 1 {
		die(fmt.Sprintf("--price out of range: %v", *price))
	}
	if *sizeShare <= 0 {
		die("--size must be > 0")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	slog.Info("placing live order",
		"token", trunc(*token, 16),
		"price", *price, "size_shares", *sizeShare,
		"side", *side, "order_type", *orderType,
		"server", *serverURL,
	)
	notional := (*price) * (*sizeShare)
	slog.Info("notional", "usdc", notional)

	resp, err := trade.PlaceOrder(ctx, *serverURL, trade.PlaceOrderRequest{
		TokenID:   *token,
		Price:     *price,
		SizeShare: *sizeShare,
		Side:      *side,
		OrderType: *orderType,
	})
	if err != nil {
		slog.Error("HTTP failure (server down?)", "err", err, "client_wall_ms", resp.ClientWallMS)
		os.Exit(1)
	}
	pretty, _ := json.MarshalIndent(resp, "", "  ")
	fmt.Println(string(pretty))
	if !resp.Success {
		slog.Warn("PM returned success=false", "error_msg", resp.ErrorMsg, "client_wall_ms", resp.ClientWallMS, "server_wall_ms", resp.ServerWallMS)
		os.Exit(1)
	}
	slog.Info("OK", "order_id", resp.OrderID, "client_wall_ms", resp.ClientWallMS, "server_wall_ms", resp.ServerWallMS)
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
