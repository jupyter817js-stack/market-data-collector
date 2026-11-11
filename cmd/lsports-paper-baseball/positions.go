package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
	"github.com/your-org/pol-odds-collector/internal/trade"
)

// openPosition is a single live BUY that needs an exit. We never compose
// positions — same fixture + same scoring side just produces a second
// independent row (per-event cap usually limits this to one anyway).
type openPosition struct {
	fixtureID   int64
	tokenID     string
	scoringSide string  // "home" | "away" — the side we bet ON
	sizeShares  float64 // shares originally requested at BUY
	entryPrice  float64 // ask we sent (≈ filled price for FAK)
	enteredAt   time.Time
	buyOrderID  string

	timer  *time.Timer
	closed bool // protected by positionManager.mu
}

// positionManager runs the dual-trigger exit logic: either a hold-duration
// timer fires (e.g. 5 minutes), or a counter-side score event arrives on
// the same fixture — whichever happens first. Both paths fan into close(),
// which is idempotent.
type positionManager struct {
	mu        sync.Mutex
	positions map[int64][]*openPosition // by fixtureID — append-only within a fixture's life

	holdDur   time.Duration
	serverURL string
	orderType string // exit order type (FAK default)
	clob      *polymarket.CLOBClient
}

func newPositionManager(
	holdDur time.Duration,
	serverURL, orderType string,
	clob *polymarket.CLOBClient,
) *positionManager {
	return &positionManager{
		positions: map[int64][]*openPosition{},
		holdDur:   holdDur,
		serverURL: serverURL,
		orderType: orderType,
		clob:      clob,
	}
}

// Add registers a freshly-opened position and arms its hold-duration
// auto-sell timer. The base context governs the eventual SELL HTTP call;
// pass the daemon's root context so timer-fired closes survive the
// AMQP-delivery scope that spawned the BUY.
func (pm *positionManager) Add(baseCtx context.Context, p *openPosition) {
	pm.mu.Lock()
	pm.positions[p.fixtureID] = append(pm.positions[p.fixtureID], p)
	pm.mu.Unlock()

	p.timer = time.AfterFunc(pm.holdDur, func() {
		pm.close(baseCtx, p, "hold_timer_expired")
	})

	slog.Info("POSITION OPENED",
		"fixture", p.fixtureID,
		"scoring_side", p.scoringSide,
		"size_shares", p.sizeShares,
		"entry_price", p.entryPrice,
		"buy_order_id", p.buyOrderID,
		"hold_until", p.enteredAt.Add(pm.holdDur).Format(time.RFC3339),
	)
}

// CheckCounterScore is called by the livescore handler whenever a NEW
// score event has been detected on a watched fixture. Any open position
// on that fixture whose scoring side is the OPPOSITE of the new event is
// closed immediately. Same-side score events leave positions untouched
// (they typically push our side's price up further).
func (pm *positionManager) CheckCounterScore(
	baseCtx context.Context,
	fixtureID int64,
	newScoringSide string,
) {
	if newScoringSide != "home" && newScoringSide != "away" {
		return
	}
	pm.mu.Lock()
	var toClose []*openPosition
	for _, p := range pm.positions[fixtureID] {
		if p.closed {
			continue
		}
		if p.scoringSide != newScoringSide {
			toClose = append(toClose, p)
		}
	}
	pm.mu.Unlock()

	for _, p := range toClose {
		if p.timer != nil {
			p.timer.Stop()
		}
		go pm.close(baseCtx, p, "counter_score")
	}
}

// CloseAll is invoked on graceful shutdown so we don't leave naked
// positions waiting for an auto-redeem at settlement. Best-effort: each
// close runs sequentially so the operator log shows the final P&L.
func (pm *positionManager) CloseAll(baseCtx context.Context, reason string) {
	pm.mu.Lock()
	var toClose []*openPosition
	for _, list := range pm.positions {
		for _, p := range list {
			if !p.closed {
				toClose = append(toClose, p)
			}
		}
	}
	pm.mu.Unlock()

	for _, p := range toClose {
		if p.timer != nil {
			p.timer.Stop()
		}
		pm.close(baseCtx, p, reason)
	}
}

// close fetches the current best_bid for the held token and submits a
// SELL FAK to flatten. Idempotent — concurrent timer / counter triggers
// race here and the loser exits early.
func (pm *positionManager) close(baseCtx context.Context, p *openPosition, reason string) {
	pm.mu.Lock()
	if p.closed {
		pm.mu.Unlock()
		return
	}
	p.closed = true
	pm.mu.Unlock()

	bookCtx, bookCancel := context.WithTimeout(baseCtx, 5*time.Second)
	book, err := pm.clob.GetOrderBook(bookCtx, p.tokenID)
	bookCancel()
	if err != nil {
		slog.Error("POSITION CLOSE: book fetch failed",
			"fixture", p.fixtureID, "reason", reason, "err", err,
			"size_shares", p.sizeShares, "entry_price", p.entryPrice)
		return
	}
	bestBid, _ := topLevel(book.Bids, true)
	if bestBid == nil || *bestBid <= 0 {
		slog.Warn("POSITION CLOSE: no bid available",
			"fixture", p.fixtureID, "reason", reason,
			"size_shares", p.sizeShares, "entry_price", p.entryPrice)
		return
	}

	tctx, cancel := context.WithTimeout(baseCtx, 15*time.Second)
	defer cancel()
	t0 := time.Now()
	resp, err := trade.PlaceOrder(tctx, pm.serverURL, trade.PlaceOrderRequest{
		TokenID:   p.tokenID,
		Price:     *bestBid,
		SizeShare: p.sizeShares,
		Side:      "SELL",
		OrderType: pm.orderType,
	})
	wallMS := time.Since(t0).Milliseconds()
	if err != nil {
		slog.Error("POSITION CLOSE: SELL HTTP failed",
			"fixture", p.fixtureID, "reason", reason, "err", err, "wall_ms", wallMS)
		return
	}

	pnlPerShare := *bestBid - p.entryPrice
	pnlTotalUSD := pnlPerShare * p.sizeShares

	slog.Info("POSITION CLOSED",
		"fixture", p.fixtureID,
		"reason", reason,
		"scoring_side", p.scoringSide,
		"entry_price", p.entryPrice,
		"exit_price", *bestBid,
		"size_shares", p.sizeShares,
		"pnl_per_share_c", pnlPerShare*100,
		"pnl_total_usd", pnlTotalUSD,
		"hold_dur_s", time.Since(p.enteredAt).Seconds(),
		"sell_order_id", resp.OrderID,
		"sell_success", resp.Success,
		"sell_error_msg", resp.ErrorMsg,
		"wall_ms", wallMS,
	)
}
