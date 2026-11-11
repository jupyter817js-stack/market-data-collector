// Package trade bridges Go signals to the Polymarket CLOB v2 Node SDK
// via a persistent HTTP server (scripts/clob-trade/server.mjs). The
// server keeps one initialized ClobClient in memory; the Go daemon
// makes one HTTP round-trip per signal — wall time drops from ~6s
// (subprocess per signal) to ~200-500ms.
//
// Why a Node server? Our Go reimplementation of the V2 + POLY_1271 wire
// format has subtle byte-level mismatches PM rejects (the official
// py-clob-client-v2 / clob-client-v2 / rs-clob-client all have the same
// failure mode in issues #64/#65/#70/#71 — only the actual JS SDK
// produces a wire body PM accepts). We isolate the SDK in a process and
// drive it via JSON-over-HTTP.
package trade

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// DefaultServerURL is where the Node server listens by default. Override
// via the CLOB_TRADE_SERVER_URL env var or the daemon's --live-server-url flag.
const DefaultServerURL = "http://127.0.0.1:9876"

// PlaceOrderRequest is the minimal set of inputs needed to post one
// signed order via the Node SDK server.
type PlaceOrderRequest struct {
	TokenID   string
	Price     float64 // 0 < p < 1
	SizeShare float64 // SHARES (not USDC)
	Side      string  // "BUY" | "SELL"
	OrderType string  // "GTC" | "FAK" | "FOK" | "GTD"; default "FAK"
	TickSize  string  // optional; default "0.01"
}

// PlaceOrderResponse is the parsed server output. Success means PM
// returned 2xx with success=true. OrderID may still be present on
// errorMsg cases like FAK no-match (PM assigns an id even when killing).
type PlaceOrderResponse struct {
	OK           bool   `json:"ok"`
	Error        string `json:"error"`
	OrderID      string `json:"orderID"`
	ErrorMsg     string `json:"errorMsg"`
	Status       string `json:"status"`
	TakingAmount string `json:"takingAmount"`
	MakingAmount string `json:"makingAmount"`
	Success      bool   `json:"success"`

	// Latency tracing (from server side).
	ServerWallMS int64 `json:"server_wall_ms"`
	MSBuild      int64 `json:"ms_build"`
	MSPost       int64 `json:"ms_post"`

	// Set by the Go side after the HTTP round-trip.
	ClientWallMS int64 `json:"-"`
}

// PlaceOrder posts a JSON request to the persistent Node server and
// returns the parsed response. Non-nil error means the HTTP transport
// itself failed (server down, timeout); PM-side rejections are signaled
// via resp.Success=false + resp.ErrorMsg.
func PlaceOrder(ctx context.Context, serverURL string, req PlaceOrderRequest) (PlaceOrderResponse, error) {
	if req.TokenID == "" {
		return PlaceOrderResponse{}, fmt.Errorf("PlaceOrder: TokenID required")
	}
	if req.Price <= 0 || req.Price >= 1 {
		return PlaceOrderResponse{}, fmt.Errorf("PlaceOrder: Price out of range: %v", req.Price)
	}
	if req.SizeShare <= 0 {
		return PlaceOrderResponse{}, fmt.Errorf("PlaceOrder: SizeShare must be > 0")
	}
	side := strings.ToUpper(strings.TrimSpace(req.Side))
	if side != "BUY" && side != "SELL" {
		side = "BUY"
	}
	ot := strings.ToUpper(strings.TrimSpace(req.OrderType))
	if ot == "" {
		ot = "FAK"
	}
	tickSize := strings.TrimSpace(req.TickSize)
	if tickSize == "" {
		tickSize = "0.01"
	}

	bodyMap := map[string]any{
		"token":     req.TokenID,
		"price":     req.Price,
		"size":      req.SizeShare,
		"side":      side,
		"orderType": ot,
		"tickSize":  tickSize,
	}
	bodyBytes, err := json.Marshal(bodyMap)
	if err != nil {
		return PlaceOrderResponse{}, fmt.Errorf("PlaceOrder: marshal: %w", err)
	}

	url := strings.TrimRight(serverURL, "/") + "/place-order"
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return PlaceOrderResponse{}, fmt.Errorf("PlaceOrder: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	t0 := time.Now()
	resp, err := http.DefaultClient.Do(httpReq)
	clientWallMS := time.Since(t0).Milliseconds()
	if err != nil {
		return PlaceOrderResponse{ClientWallMS: clientWallMS}, fmt.Errorf("PlaceOrder: http: %w", err)
	}
	defer resp.Body.Close()

	var parsed PlaceOrderResponse
	if derr := json.NewDecoder(resp.Body).Decode(&parsed); derr != nil {
		return PlaceOrderResponse{ClientWallMS: clientWallMS}, fmt.Errorf("PlaceOrder: decode (status=%d): %w", resp.StatusCode, derr)
	}
	parsed.ClientWallMS = clientWallMS
	return parsed, nil
}

// Health pings the server's /health endpoint to confirm it's alive and
// to surface its derived EOA/funder/sigType for sanity logging.
func Health(ctx context.Context, serverURL string) (map[string]any, error) {
	url := strings.TrimRight(serverURL, "/") + "/health"
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("Health: build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("Health: http: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Health: status %d", resp.StatusCode)
	}
	var out map[string]any
	if derr := json.NewDecoder(resp.Body).Decode(&out); derr != nil {
		return nil, fmt.Errorf("Health: decode: %w", derr)
	}
	return out, nil
}
