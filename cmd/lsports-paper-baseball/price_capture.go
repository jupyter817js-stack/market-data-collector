package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/source/polymarket"
	"github.com/your-org/pol-odds-collector/internal/store"
)

// priceCaptureManager polls the Polymarket CLOB book for every watched
// fixture's two tokens at a fixed cadence and writes ticks to
// polymarket_latency_observations. This is the in-daemon equivalent of
// running cmd/polymarket-latency-capture separately — we built it
// because the standalone capture daemon only watches tokens listed on
// its command line at startup, so newly added fixtures (from
// auto-discover or --map at restart) silently lacked 5-min hold P&L
// data. (See 2026-06-01 CHC-STL post-mortem.)
//
// Cadence default 1.5s per token. Per-fixture goroutines run
// independently and exit on match status=3 (Finished) or daemon
// shutdown.
type priceCaptureManager struct {
	mu     sync.Mutex
	active map[int64]*captureSlot

	clob     *polymarket.CLOBClient
	db       *store.PostgresStore
	interval time.Duration
}

type captureSlot struct {
	cancel context.CancelFunc
	tokens []string // up to 2 (home + away for winner market)
}

func newPriceCaptureManager(clob *polymarket.CLOBClient, db *store.PostgresStore, interval time.Duration) *priceCaptureManager {
	if interval <= 0 {
		interval = 1500 * time.Millisecond
	}
	return &priceCaptureManager{
		active:   map[int64]*captureSlot{},
		clob:     clob,
		db:       db,
		interval: interval,
	}
}

// Start launches a polling goroutine for fixtureID's two winner-market
// tokens. Idempotent — calling twice for the same fixture is a no-op.
// Token resolution is best-effort: if polymarket_tokens has no rows for
// the condition, we log and skip.
func (pm *priceCaptureManager) Start(baseCtx context.Context, fixtureID int64, conditionID string) {
	if conditionID == "" {
		return
	}
	pm.mu.Lock()
	if _, ok := pm.active[fixtureID]; ok {
		pm.mu.Unlock()
		return
	}
	pm.mu.Unlock()

	tokens, err := pm.db.LookupPolymarketTokensByCondition(baseCtx, conditionID)
	if err != nil || len(tokens) == 0 {
		slog.Warn("price-capture: cannot start — token lookup failed or empty",
			"fixture", fixtureID, "condition", conditionID, "err", err, "n", len(tokens))
		return
	}
	tokenIDs := make([]string, 0, len(tokens))
	for _, t := range tokens {
		tokenIDs = append(tokenIDs, t.TokenID)
	}

	ctx, cancel := context.WithCancel(baseCtx)
	pm.mu.Lock()
	pm.active[fixtureID] = &captureSlot{cancel: cancel, tokens: tokenIDs}
	pm.mu.Unlock()

	slog.Info("price-capture: STARTED",
		"fixture", fixtureID, "tokens", len(tokenIDs),
		"interval", pm.interval)
	go pm.loop(ctx, fixtureID, tokenIDs)
}

// Stop cancels the goroutine for fixtureID and removes its slot.
// Idempotent.
func (pm *priceCaptureManager) Stop(fixtureID int64, reason string) {
	pm.mu.Lock()
	slot, ok := pm.active[fixtureID]
	if ok {
		delete(pm.active, fixtureID)
	}
	pm.mu.Unlock()
	if !ok {
		return
	}
	slot.cancel()
	slog.Info("price-capture: STOPPED", "fixture", fixtureID, "reason", reason)
}

// StopAll cancels every active capture goroutine. Used at shutdown.
func (pm *priceCaptureManager) StopAll(reason string) {
	pm.mu.Lock()
	ids := make([]int64, 0, len(pm.active))
	for id := range pm.active {
		ids = append(ids, id)
	}
	pm.mu.Unlock()
	for _, id := range ids {
		pm.Stop(id, reason)
	}
}

func (pm *priceCaptureManager) loop(ctx context.Context, fixtureID int64, tokens []string) {
	ticker := time.NewTicker(pm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, tok := range tokens {
			if err := ctx.Err(); err != nil {
				return
			}
			pm.captureOne(ctx, fixtureID, tok)
		}
	}
}

// captureOne writes a single observation for one token. We keep it flat
// (no shared prior state) — consumers of the observations rely on the
// received_at timeline, not on the price_changed_from_prior flag.
func (pm *priceCaptureManager) captureOne(ctx context.Context, fixtureID int64, tokenID string) {
	subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	book, err := pm.clob.GetOrderBook(subCtx, tokenID)
	if err != nil {
		return
	}
	now := time.Now().UTC()

	bestBid, _ := bestBookSide(book.Bids, true)
	bestAsk, _ := bestBookSide(book.Asks, false)
	var bidPtr, askPtr, midPtr, spreadPtr *float64
	if bestBid != nil && bestAsk != nil {
		b, a := *bestBid, *bestAsk
		bidPtr, askPtr = &b, &a
		mid := (b + a) / 2
		midPtr = &mid
		sp := a - b
		spreadPtr = &sp
	}
	var lastPx *float64
	if lp, perr := strconv.ParseFloat(strings.TrimSpace(book.LastTradePrice), 64); perr == nil && book.LastTradePrice != "" {
		lastPx = &lp
	}

	obs := model.PolymarketLatencyObservation{
		LSportsFixtureID:  fixtureID,
		PolymarketTokenID: tokenID,
		ReceivedAt:        &now,
		BestBid:           bidPtr,
		BestAsk:           askPtr,
		Midpoint:          midPtr,
		LastPrice:         lastPx,
		Spread:            spreadPtr,
		RawPayload:        json.RawMessage(book.Raw),
	}
	_, _ = pm.db.InsertPolymarketLatencyObservation(ctx, obs)
}

// bestBookSide picks the highest bid (bid=true) or lowest ask (bid=false)
// price from a list of book levels. Returns nil pointers when empty.
func bestBookSide(levels []polymarket.CLOBBookLevel, bid bool) (price, size *float64) {
	if len(levels) == 0 {
		return nil, nil
	}
	var bestPx, bestSz float64
	have := false
	for _, lv := range levels {
		p, err := strconv.ParseFloat(strings.TrimSpace(lv.Price), 64)
		if err != nil {
			continue
		}
		if !have || (bid && p > bestPx) || (!bid && p < bestPx) {
			bestPx = p
			s, _ := strconv.ParseFloat(strings.TrimSpace(lv.Size), 64)
			bestSz = s
			have = true
		}
	}
	if !have {
		return nil, nil
	}
	return &bestPx, &bestSz
}
