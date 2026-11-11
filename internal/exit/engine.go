// Package exit classifies an open position into HOLD / EXIT_* states
// based on its current best exit value, unrealized PnL, age, and
// liquidity. Pure: given a Position + Config + clock, return a Signal.
// Persistence lives in internal/store; daemon lives in cmd/clob-exit-monitor.
//
// The exit engine deliberately mirrors internal/signal's shape (Classify
// is pure, Signal carries State + Reasons) so the dashboard, audit, and
// future fade strategies treat entry and exit decisions uniformly.
package exit

import (
	"fmt"
	"time"
)

// State is the verdict for a single position at one moment.
type State string

const (
	StateHold              State = "HOLD"
	StateExitProfit        State = "EXIT_PROFIT"
	StateExitStopLoss      State = "EXIT_STOP_LOSS"
	StateExitTime          State = "EXIT_TIME"
	StateExitLiquidityRisk State = "EXIT_LIQUIDITY_RISK"
	StateExitMarketSettled State = "EXIT_MARKET_SETTLED"
)

// Position is the input projection — values come from the
// position_exit_routes view computed by migration 040.
type Position struct {
	ID                    int64
	PolymarketTokenID     string
	PolymarketConditionID string
	CostUSDC              float64
	Shares                float64
	OpenedAt              time.Time

	HeldBid            float64 // 0 if no fresh PM snapshot for the held token
	HeldAsk            float64
	SumOppositeAsk     float64
	DirectSellValue    float64
	SyntheticSellValue float64
	BestExitValue      float64
	RecommendedRoute   string // direct_sell | synthetic_sell | merge

	// MarketSettled means the held token has its outcomePrice = 0 or 1
	// (UMA proposed or fully closed). The caller flags this from the
	// resolved_markets view; we don't recompute it here.
	MarketSettled bool
	// Winner true means this token is the winning side (will pay $1).
	WinnerOnSettle bool
}

// Config is the threshold surface. All fractions; durations in Go units.
type Config struct {
	// ProfitTargetFrac is the unrealized-PnL fraction (over cost) that
	// triggers EXIT_PROFIT. 0.05 = "exit when I'm up 5%".
	ProfitTargetFrac float64
	// StopLossFrac is the loss fraction that triggers EXIT_STOP_LOSS.
	// 0.30 = "exit when I'm down 30%".
	StopLossFrac float64
	// MaxHoldDuration triggers EXIT_TIME independent of PnL.
	MaxHoldDuration time.Duration
	// MinBidForLiveExit: below this held bid the market is treated as
	// effectively settled and EXIT_LIQUIDITY_RISK fires regardless of
	// MarketSettled. 0 to disable.
	MinBidForLiveExit float64
}

// Signal is the classified output, persisted by the daemon.
type Signal struct {
	PositionID           int64
	State                State
	RecommendedRoute     string
	ExpectedProceedsUSDC float64
	UnrealizedPnLUSDC    float64
	UnrealizedPnLPct     float64 // fraction (0.05 = +5%)
	HeldBid              float64
	HeldAsk              float64
	SumOppositeAsk       float64
	PositionAgeSeconds   int64
	Reasons              []string
}

// Classify is pure: same inputs → same Signal. Order of checks matters:
//  1. MARKET_SETTLED — if the market resolved, route to merge (winner)
//     or expired_settled (loser).
//  2. LIQUIDITY_RISK — bid collapsed; close before settlement risk.
//  3. STOP_LOSS — bigger losses before profit-takes to avoid masking.
//  4. PROFIT — target hit.
//  5. TIME — age-based exit (last so it's only a fallback).
//  6. HOLD — none of the above.
func Classify(pos Position, cfg Config, now time.Time) Signal {
	age := now.Sub(pos.OpenedAt)
	if age < 0 {
		age = 0
	}
	unrealized := pos.BestExitValue - pos.CostUSDC
	pct := 0.0
	if pos.CostUSDC > 0 {
		pct = unrealized / pos.CostUSDC
	}

	sig := Signal{
		PositionID:           pos.ID,
		RecommendedRoute:     pos.RecommendedRoute,
		ExpectedProceedsUSDC: pos.BestExitValue,
		UnrealizedPnLUSDC:    unrealized,
		UnrealizedPnLPct:     pct,
		HeldBid:              pos.HeldBid,
		HeldAsk:              pos.HeldAsk,
		SumOppositeAsk:       pos.SumOppositeAsk,
		PositionAgeSeconds:   int64(age.Seconds()),
	}

	if pos.MarketSettled {
		sig.State = StateExitMarketSettled
		if pos.WinnerOnSettle {
			sig.RecommendedRoute = "merge"
			sig.Reasons = []string{"market settled, this side won; redeem complete set via merge for $1/share"}
		} else {
			sig.RecommendedRoute = "expired_settled"
			sig.Reasons = []string{"market settled, this side lost; close at near-zero proceeds"}
		}
		return sig
	}

	if cfg.MinBidForLiveExit > 0 && pos.HeldBid > 0 && pos.HeldBid < cfg.MinBidForLiveExit {
		sig.State = StateExitLiquidityRisk
		sig.Reasons = []string{fmt.Sprintf(
			"held_bid %.4f < %.4f; book likely about to settle, close now",
			pos.HeldBid, cfg.MinBidForLiveExit)}
		return sig
	}

	if cfg.StopLossFrac > 0 && pct <= -cfg.StopLossFrac {
		sig.State = StateExitStopLoss
		sig.Reasons = []string{fmt.Sprintf(
			"unrealized %+.2f%% <= -stop_loss %+.2f%% (via %s, %.4f USDC < cost %.4f)",
			pct*100, cfg.StopLossFrac*100, sig.RecommendedRoute, pos.BestExitValue, pos.CostUSDC)}
		return sig
	}

	if cfg.ProfitTargetFrac > 0 && pct >= cfg.ProfitTargetFrac {
		sig.State = StateExitProfit
		sig.Reasons = []string{fmt.Sprintf(
			"unrealized %+.2f%% >= target %+.2f%% (via %s, +%.4f USDC over cost)",
			pct*100, cfg.ProfitTargetFrac*100, sig.RecommendedRoute, unrealized)}
		return sig
	}

	if cfg.MaxHoldDuration > 0 && age >= cfg.MaxHoldDuration {
		sig.State = StateExitTime
		sig.Reasons = []string{fmt.Sprintf(
			"held %s >= max %s; unrealized %+.2f%% (close at recommendation %s)",
			age.Truncate(time.Second), cfg.MaxHoldDuration, pct*100, sig.RecommendedRoute)}
		return sig
	}

	sig.State = StateHold
	sig.Reasons = []string{fmt.Sprintf(
		"unrealized %+.2f%% within hold band [-%v / +%v]; held %s of max %s",
		pct*100, cfg.StopLossFrac, cfg.ProfitTargetFrac,
		age.Truncate(time.Second), cfg.MaxHoldDuration)}
	return sig
}
