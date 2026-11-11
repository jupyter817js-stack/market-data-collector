package exit

import (
	"strings"
	"testing"
	"time"
)

func baseCfg() Config {
	return Config{
		ProfitTargetFrac:  0.05,
		StopLossFrac:      0.30,
		MaxHoldDuration:   6 * time.Hour,
		MinBidForLiveExit: 0.05,
	}
}

func basePos(now time.Time) Position {
	return Position{
		ID:                 1,
		PolymarketTokenID:  "tok",
		CostUSDC:           20,
		Shares:             40,
		OpenedAt:           now.Add(-1 * time.Hour),
		HeldBid:            0.55,
		HeldAsk:            0.60,
		SumOppositeAsk:     0.43,
		DirectSellValue:    22.0, // 40 × 0.55
		SyntheticSellValue: 22.8, // 40 × (1 − 0.43)
		BestExitValue:      22.8,
		RecommendedRoute:   "synthetic_sell",
	}
}

func TestClassify_HoldWhenWithinBand(t *testing.T) {
	now := time.Now().UTC()
	pos := basePos(now)
	// +14% from 20 → 22.8 (above zero, below 5% no wait, 14% > 5%) — actually 14% > 5% target → would EXIT_PROFIT.
	// Lower best to 20.5 so it's within band.
	pos.BestExitValue = 20.5
	pos.SyntheticSellValue = 20.5
	sig := Classify(pos, baseCfg(), now)
	if sig.State != StateHold {
		t.Fatalf("state=%s want HOLD (pct=%.2f%%)", sig.State, sig.UnrealizedPnLPct*100)
	}
}

func TestClassify_ProfitWhenAboveTarget(t *testing.T) {
	now := time.Now().UTC()
	pos := basePos(now) // best 22.8 from cost 20 → +14% > 5% target
	sig := Classify(pos, baseCfg(), now)
	if sig.State != StateExitProfit {
		t.Fatalf("state=%s want EXIT_PROFIT", sig.State)
	}
	if sig.RecommendedRoute != "synthetic_sell" {
		t.Fatalf("route=%s want synthetic_sell (best path)", sig.RecommendedRoute)
	}
	if sig.UnrealizedPnLUSDC < 2.79 || sig.UnrealizedPnLUSDC > 2.81 {
		t.Fatalf("pnl=%v want ~2.80", sig.UnrealizedPnLUSDC)
	}
}

func TestClassify_StopLossBeatsProfitWhenBothImpossible(t *testing.T) {
	now := time.Now().UTC()
	pos := basePos(now)
	pos.BestExitValue = 13.0 // -35% from 20 → stop loss at 30%
	pos.SyntheticSellValue = 13.0
	sig := Classify(pos, baseCfg(), now)
	if sig.State != StateExitStopLoss {
		t.Fatalf("state=%s want EXIT_STOP_LOSS", sig.State)
	}
}

func TestClassify_TimeExitAfterMaxHoldEvenInBand(t *testing.T) {
	now := time.Now().UTC()
	pos := basePos(now)
	pos.OpenedAt = now.Add(-7 * time.Hour) // > 6h max
	pos.BestExitValue = 20.5               // within profit/loss band
	pos.SyntheticSellValue = 20.5
	sig := Classify(pos, baseCfg(), now)
	if sig.State != StateExitTime {
		t.Fatalf("state=%s want EXIT_TIME", sig.State)
	}
}

func TestClassify_LiquidityRiskBeatsHold(t *testing.T) {
	now := time.Now().UTC()
	pos := basePos(now)
	pos.HeldBid = 0.02 // < 0.05 floor
	pos.BestExitValue = 20.5
	pos.SyntheticSellValue = 20.5
	sig := Classify(pos, baseCfg(), now)
	if sig.State != StateExitLiquidityRisk {
		t.Fatalf("state=%s want EXIT_LIQUIDITY_RISK", sig.State)
	}
}

func TestClassify_MarketSettledWinnerRoutesToMerge(t *testing.T) {
	now := time.Now().UTC()
	pos := basePos(now)
	pos.MarketSettled = true
	pos.WinnerOnSettle = true
	sig := Classify(pos, baseCfg(), now)
	if sig.State != StateExitMarketSettled {
		t.Fatalf("state=%s want EXIT_MARKET_SETTLED", sig.State)
	}
	if sig.RecommendedRoute != "merge" {
		t.Fatalf("route=%s want merge (winner takes $1/share via redemption)", sig.RecommendedRoute)
	}
	if !strings.Contains(sig.Reasons[0], "won") {
		t.Fatalf("reason=%q want 'won' mention", sig.Reasons[0])
	}
}

func TestClassify_MarketSettledLoserRoutesToExpiredSettled(t *testing.T) {
	now := time.Now().UTC()
	pos := basePos(now)
	pos.MarketSettled = true
	pos.WinnerOnSettle = false
	sig := Classify(pos, baseCfg(), now)
	if sig.State != StateExitMarketSettled {
		t.Fatalf("state=%s want EXIT_MARKET_SETTLED", sig.State)
	}
	if sig.RecommendedRoute != "expired_settled" {
		t.Fatalf("route=%s want expired_settled", sig.RecommendedRoute)
	}
}

func TestClassify_MarketSettledOverridesEverything(t *testing.T) {
	now := time.Now().UTC()
	pos := basePos(now)
	pos.MarketSettled = true
	pos.WinnerOnSettle = true
	pos.BestExitValue = 0 // even with disastrous "current" value
	pos.HeldBid = 0
	pos.OpenedAt = now.Add(-10 * time.Hour) // past max-hold
	sig := Classify(pos, baseCfg(), now)
	if sig.State != StateExitMarketSettled {
		t.Fatalf("state=%s; MarketSettled must take precedence over every other trigger", sig.State)
	}
}

func TestClassify_HoldReasonIncludesAge(t *testing.T) {
	now := time.Now().UTC()
	pos := basePos(now)
	pos.BestExitValue = 20.5 // hold band
	pos.SyntheticSellValue = 20.5
	sig := Classify(pos, baseCfg(), now)
	if sig.State != StateHold {
		t.Fatalf("state=%s want HOLD", sig.State)
	}
	if sig.PositionAgeSeconds < 3500 || sig.PositionAgeSeconds > 3700 {
		t.Fatalf("age=%d want ~3600", sig.PositionAgeSeconds)
	}
}
