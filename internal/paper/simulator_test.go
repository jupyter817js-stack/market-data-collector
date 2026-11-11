package paper

import (
	"math"
	"testing"
	"time"

	sigpkg "github.com/your-org/pol-odds-collector/internal/signal"
)

func ts(min int) time.Time {
	return time.Date(2026, 5, 17, 12, min, 0, 0, time.UTC)
}

func buyCandidate(id int64, t time.Time, matchupID int64, side string, tokenID string, ask, claimedNet float64) SignalRow {
	return SignalRow{
		ID:                    id,
		CreatedAt:             t,
		MatchupID:             matchupID,
		OutcomeSide:           side,
		PolymarketConditionID: "0xc",
		PolymarketTokenID:     tokenID,
		HomeName:              "A", AwayName: "B", LeagueName: "CS2 - X",
		State:            sigpkg.StateBuyCandidate,
		PMAsk:            ask,
		NetEdgeBuy:       claimedNet,
		ProposedSizeUSDC: 20,
	}
}

func TestSimulate_DedupesAcrossPinnacleParallelMatchupIDs(t *testing.T) {
	// Real-world regression: Pinnacle returns the same Udinese vs Cremonese
	// game under two matchup_ids (1630948179 and 1630948181). Auto-confirm
	// aliases both to the SAME PM condition + token. The old dedupe key was
	// (matchup_id, outcome_side), so the simulator counted that one trade
	// twice and inflated the loss to -$40 instead of -$20. The fix is to
	// dedupe on polymarket_token_id when available — the token is the
	// actual tradeable unit and is stable across Pinnacle's parallel
	// matchup_id quirk.
	sigs := []SignalRow{
		buyCandidate(1, ts(0), 1630948179, "home", "udineseHomeToken", 0.26, 0.05),
		buyCandidate(2, ts(1), 1630948181, "home", "udineseHomeToken", 0.26, 0.04), // same game, parallel Pinnacle id
	}
	prices := map[string]PriceRow{
		"udineseHomeToken": {PolymarketTokenID: "udineseHomeToken", Ask: 0.001},
	}
	trades, sum := Simulate(sigs, prices, nil, Config{DedupePerMatchupOutcome: true})
	if len(trades) != 1 {
		t.Fatalf("trades=%d want 1 (parallel matchup_id should not duplicate)", len(trades))
	}
	if sum.TradesSimulated != 1 {
		t.Fatalf("TradesSimulated=%d want 1", sum.TradesSimulated)
	}
	// First sig wins under sort-by-CreatedAt-asc.
	if trades[0].SignalID != 1 {
		t.Fatalf("kept SignalID=%d want 1 (first by CreatedAt)", trades[0].SignalID)
	}
}

func TestSimulate_FallsBackToMatchupKeyWhenTokenMissing(t *testing.T) {
	// Old behavior: pre-token-alias signals had empty PolymarketTokenID and
	// the simulator had to rely on (matchup_id, side). Make sure removing
	// the token doesn't lose that fallback path.
	sig := buyCandidate(1, ts(0), 100, "home", "", 0.30, 0.05)
	sig2 := buyCandidate(2, ts(1), 100, "home", "", 0.31, 0.04) // dupe by matchup+side
	trades, _ := Simulate([]SignalRow{sig, sig2}, nil, nil, Config{DedupePerMatchupOutcome: true})
	if len(trades) != 1 {
		t.Fatalf("trades=%d want 1 (matchup+side fallback)", len(trades))
	}
}

func TestSimulate_DedupeKeepsFirstPerMatchupOutcome(t *testing.T) {
	sigs := []SignalRow{
		buyCandidate(1, ts(0), 100, "home", "tokA", 0.30, 0.05), // first → kept
		buyCandidate(2, ts(1), 100, "home", "tokA", 0.31, 0.04), // dupe → dropped
		buyCandidate(3, ts(2), 100, "away", "tokB", 0.65, 0.04), // different side → kept
		buyCandidate(4, ts(3), 101, "home", "tokC", 0.40, 0.06), // different matchup → kept
	}
	prices := map[string]PriceRow{
		"tokA": {PolymarketTokenID: "tokA", Mid: 0.33},
		"tokB": {PolymarketTokenID: "tokB", Mid: 0.66},
		"tokC": {PolymarketTokenID: "tokC", Mid: 0.42},
	}
	trades, sum := Simulate(sigs, prices, nil, Config{DedupePerMatchupOutcome: true})
	if len(trades) != 3 {
		t.Fatalf("trades=%d want 3", len(trades))
	}
	if sum.CandidatesSeen != 4 || sum.TradesSimulated != 3 || sum.TradesWithMTM != 3 {
		t.Fatalf("summary=%+v", sum)
	}
}

func TestSimulate_DedupeOffKeepsAll(t *testing.T) {
	sigs := []SignalRow{
		buyCandidate(1, ts(0), 100, "home", "tokA", 0.30, 0.05),
		buyCandidate(2, ts(1), 100, "home", "tokA", 0.31, 0.04),
	}
	prices := map[string]PriceRow{"tokA": {PolymarketTokenID: "tokA", Mid: 0.33}}
	trades, _ := Simulate(sigs, prices, nil, Config{DedupePerMatchupOutcome: false})
	if len(trades) != 2 {
		t.Fatalf("trades=%d want 2", len(trades))
	}
}

func TestSimulate_MTMArithmeticIsCorrect(t *testing.T) {
	sigs := []SignalRow{
		buyCandidate(1, ts(0), 100, "home", "tokA", 0.30, 0.05),
	}
	prices := map[string]PriceRow{
		"tokA": {PolymarketTokenID: "tokA", Mid: 0.35, UpdatedAt: ts(5)},
	}
	trades, sum := Simulate(sigs, prices, nil, Config{DedupePerMatchupOutcome: true})
	if len(trades) != 1 {
		t.Fatalf("trades=%d", len(trades))
	}
	tr := trades[0]
	// shares = 20 / 0.30 ≈ 66.667
	if math.Abs(tr.Shares-66.6666667) > 1e-4 {
		t.Fatalf("shares=%.5f", tr.Shares)
	}
	// PnL = (0.35/0.30 - 1) * 20 = 0.166667*20 ≈ 3.333
	wantPnL := (0.35/0.30 - 1.0) * 20.0
	if math.Abs(tr.MTMPnLUSDC-wantPnL) > 1e-9 {
		t.Fatalf("pnl=%.6f want %.6f", tr.MTMPnLUSDC, wantPnL)
	}
	if math.Abs(tr.MTMEdgeFrac-(0.35-0.30)) > 1e-9 {
		t.Fatalf("mtm edge frac=%.6f", tr.MTMEdgeFrac)
	}
	if math.Abs(sum.TotalMTMPnLUSDC-wantPnL) > 1e-9 {
		t.Fatalf("summary pnl=%.6f", sum.TotalMTMPnLUSDC)
	}
}

func TestSimulate_SkipsBadAsk(t *testing.T) {
	sigs := []SignalRow{
		buyCandidate(1, ts(0), 100, "home", "tokA", 0, 0.05),    // ask 0 → skip
		buyCandidate(2, ts(1), 101, "home", "tokB", -0.1, 0.05), // negative → skip
		buyCandidate(3, ts(2), 102, "home", "tokC", 0.30, 0.05), // good
	}
	trades, _ := Simulate(sigs, nil, nil, Config{DedupePerMatchupOutcome: true})
	if len(trades) != 1 {
		t.Fatalf("trades=%d want 1", len(trades))
	}
}

func TestSimulate_MarkPriceFallback(t *testing.T) {
	cases := []struct {
		name      string
		row       PriceRow
		wantMark  float64
		wantHas   bool
		wantPnLOf func(entry, size float64) float64
	}{
		{
			name:     "mid preferred when set",
			row:      PriceRow{Mid: 0.40, Bid: 0.10, Ask: 0.50},
			wantMark: 0.40, wantHas: true,
		},
		{
			name:     "bid+ask midpoint when mid missing",
			row:      PriceRow{Bid: 0.20, Ask: 0.30},
			wantMark: 0.25, wantHas: true,
		},
		{
			name:     "ask-only when bid is null too (settled token)",
			row:      PriceRow{Ask: 0.001},
			wantMark: 0.001, wantHas: true,
		},
		{
			name:     "bid-only as last resort",
			row:      PriceRow{Bid: 0.42},
			wantMark: 0.42, wantHas: true,
		},
		{
			name:     "all zero stays unpriced",
			row:      PriceRow{},
			wantMark: 0, wantHas: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := markPriceFromPriceRow(tc.row)
			if got != tc.wantMark {
				t.Fatalf("mark=%v want %v", got, tc.wantMark)
			}
			sigs := []SignalRow{buyCandidate(1, ts(0), 100, "home", "tok", 0.30, 0.05)}
			prices := map[string]PriceRow{"tok": tc.row}
			trades, sum := Simulate(sigs, prices, nil, Config{DedupePerMatchupOutcome: true})
			if len(trades) != 1 {
				t.Fatalf("trades=%d want 1", len(trades))
			}
			if trades[0].HasMTM != tc.wantHas {
				t.Fatalf("HasMTM=%v want %v", trades[0].HasMTM, tc.wantHas)
			}
			if tc.wantHas && sum.TradesWithMTM != 1 {
				t.Fatalf("TradesWithMTM=%d want 1", sum.TradesWithMTM)
			}
			if !tc.wantHas && sum.TradesUnpriced != 1 {
				t.Fatalf("TradesUnpriced=%d want 1", sum.TradesUnpriced)
			}
		})
	}
}

func TestSimulate_SettledTokenProducesNearTotalLoss(t *testing.T) {
	// Entry ask 0.35 on a "home" token; market settled with home losing, so
	// ask drops to 0.001 (the settled marker PM exposes). Mid is null because
	// no two-sided book remains. The fallback should pick ask=0.001 and
	// compute the MTM loss honestly (~$19.94 on a $20 size), not silently
	// drop the trade into TradesUnpriced.
	sigs := []SignalRow{buyCandidate(1, ts(0), 100, "home", "tokSettled", 0.35, 0.05)}
	prices := map[string]PriceRow{"tokSettled": {Ask: 0.001, UpdatedAt: ts(5)}}
	trades, sum := Simulate(sigs, prices, nil, Config{DedupePerMatchupOutcome: true})
	if !trades[0].HasMTM {
		t.Fatalf("HasMTM=false; expected fallback to ask")
	}
	wantPnL := (0.001/0.35 - 1.0) * 20
	if got := trades[0].MTMPnLUSDC; got < wantPnL-0.01 || got > wantPnL+0.01 {
		t.Fatalf("MTMPnL=%v want ~%v", got, wantPnL)
	}
	if sum.TradesWithMTM != 1 || sum.TradesUnpriced != 0 {
		t.Fatalf("summary=%+v", sum)
	}
}

func TestSimulate_UnpricedTradeStillCountsInTotals(t *testing.T) {
	sigs := []SignalRow{
		buyCandidate(1, ts(0), 100, "home", "tokMissing", 0.30, 0.05),
	}
	trades, sum := Simulate(sigs, nil, nil, Config{DedupePerMatchupOutcome: true}) // empty price map
	if len(trades) != 1 {
		t.Fatalf("trades=%d want 1", len(trades))
	}
	if trades[0].HasMTM {
		t.Fatalf("HasMTM=true want false")
	}
	if sum.TradesUnpriced != 1 || sum.TradesWithMTM != 0 {
		t.Fatalf("summary=%+v", sum)
	}
	if sum.TotalSizeUSDC != 20 || sum.TotalMTMPnLUSDC != 0 {
		t.Fatalf("summary money=%+v", sum)
	}
}

func TestSimulate_OverrideSizeApplied(t *testing.T) {
	sigs := []SignalRow{buyCandidate(1, ts(0), 100, "home", "tokA", 0.30, 0.05)}
	prices := map[string]PriceRow{"tokA": {PolymarketTokenID: "tokA", Mid: 0.33}}
	trades, _ := Simulate(sigs, prices, nil, Config{DedupePerMatchupOutcome: true, OverrideSizeUSDC: 50})
	if trades[0].SizeUSDC != 50 {
		t.Fatalf("size=%.2f want 50", trades[0].SizeUSDC)
	}
}

func TestSimulate_IgnoresNonBuyCandidate(t *testing.T) {
	sigs := []SignalRow{
		{ID: 1, State: sigpkg.StateNoEdge, MatchupID: 100, OutcomeSide: "home", PMAsk: 0.30, ProposedSizeUSDC: 20, CreatedAt: ts(0)},
		{ID: 2, State: sigpkg.StateOvervalued, MatchupID: 100, OutcomeSide: "home", PMAsk: 0.30, ProposedSizeUSDC: 20, CreatedAt: ts(1)},
		buyCandidate(3, ts(2), 100, "home", "tokA", 0.30, 0.05),
	}
	trades, sum := Simulate(sigs, nil, nil, Config{DedupePerMatchupOutcome: true})
	if len(trades) != 1 || sum.CandidatesSeen != 1 {
		t.Fatalf("trades=%d summary=%+v", len(trades), sum)
	}
}

// --- Realized PnL (resolved markets) ---

func TestSimulate_RealizedWin_PaysOneMinusEntry(t *testing.T) {
	sigs := []SignalRow{buyCandidate(1, ts(0), 100, "home", "tokA", 0.30, 0.05)}
	prices := map[string]PriceRow{"tokA": {PolymarketTokenID: "tokA", Mid: 0.40}}
	res := map[string]Resolution{
		"0xc": {ConditionID: "0xc", WinningTokenID: "tokA", ClosedAt: ts(60)},
	}
	trades, sum := Simulate(sigs, prices, res, Config{DedupePerMatchupOutcome: true})
	if !trades[0].IsResolved || !trades[0].Won {
		t.Fatalf("expected resolved win, got %+v", trades[0])
	}
	// shares = 20/0.30 = 66.67; payoff $1 each = $66.67; realized = 66.67 - 20 = 46.67
	wantRealized := (20.0/0.30)*1.0 - 20.0
	if math.Abs(trades[0].RealizedPnLUSDC-wantRealized) > 1e-6 {
		t.Fatalf("realized=%.4f want %.4f", trades[0].RealizedPnLUSDC, wantRealized)
	}
	if sum.TradesResolved != 1 || sum.TradesWon != 1 {
		t.Fatalf("summary=%+v", sum)
	}
	if math.Abs(sum.HitRate-1.0) > 1e-9 {
		t.Fatalf("hit_rate=%.4f want 1", sum.HitRate)
	}
	if math.Abs(sum.TotalRealizedPnLUSDC-wantRealized) > 1e-6 {
		t.Fatalf("summary realized=%.4f", sum.TotalRealizedPnLUSDC)
	}
}

func TestSimulate_RealizedLoss_TokenDidNotWin(t *testing.T) {
	sigs := []SignalRow{buyCandidate(1, ts(0), 100, "home", "tokA", 0.30, 0.05)}
	res := map[string]Resolution{
		"0xc": {ConditionID: "0xc", WinningTokenID: "tokOther", ClosedAt: ts(60)},
	}
	trades, sum := Simulate(sigs, nil, res, Config{DedupePerMatchupOutcome: true})
	if !trades[0].IsResolved || trades[0].Won {
		t.Fatalf("expected resolved loss, got %+v", trades[0])
	}
	if math.Abs(trades[0].RealizedPnLUSDC-(-20.0)) > 1e-9 {
		t.Fatalf("realized=%.4f want -20", trades[0].RealizedPnLUSDC)
	}
	if sum.TradesResolved != 1 || sum.TradesWon != 0 {
		t.Fatalf("summary=%+v", sum)
	}
	if sum.HitRate != 0 {
		t.Fatalf("hit_rate=%.4f want 0", sum.HitRate)
	}
}

func TestSimulate_Unresolved_NoRealizedFields(t *testing.T) {
	sigs := []SignalRow{buyCandidate(1, ts(0), 100, "home", "tokA", 0.30, 0.05)}
	prices := map[string]PriceRow{"tokA": {PolymarketTokenID: "tokA", Mid: 0.33}}
	trades, sum := Simulate(sigs, prices, nil, Config{DedupePerMatchupOutcome: true})
	if trades[0].IsResolved {
		t.Fatalf("IsResolved=true want false (no resolutions)")
	}
	if sum.TradesResolved != 0 || sum.HitRate != 0 {
		t.Fatalf("summary=%+v", sum)
	}
}

func TestSimulate_MixedResolution_HitRateAndTotals(t *testing.T) {
	// 4 trades: 2 wins, 1 loss, 1 unresolved.
	sigs := []SignalRow{
		{ID: 1, CreatedAt: ts(0), MatchupID: 1, OutcomeSide: "home", PolymarketConditionID: "0x1", PolymarketTokenID: "tokA1", State: sigpkg.StateBuyCandidate, PMAsk: 0.25, ProposedSizeUSDC: 20},
		{ID: 2, CreatedAt: ts(1), MatchupID: 2, OutcomeSide: "home", PolymarketConditionID: "0x2", PolymarketTokenID: "tokB1", State: sigpkg.StateBuyCandidate, PMAsk: 0.40, ProposedSizeUSDC: 20},
		{ID: 3, CreatedAt: ts(2), MatchupID: 3, OutcomeSide: "home", PolymarketConditionID: "0x3", PolymarketTokenID: "tokC1", State: sigpkg.StateBuyCandidate, PMAsk: 0.50, ProposedSizeUSDC: 20},
		{ID: 4, CreatedAt: ts(3), MatchupID: 4, OutcomeSide: "home", PolymarketConditionID: "0x4", PolymarketTokenID: "tokD1", State: sigpkg.StateBuyCandidate, PMAsk: 0.60, ProposedSizeUSDC: 20},
	}
	res := map[string]Resolution{
		"0x1": {ConditionID: "0x1", WinningTokenID: "tokA1"},    // win
		"0x2": {ConditionID: "0x2", WinningTokenID: "tokB1"},    // win
		"0x3": {ConditionID: "0x3", WinningTokenID: "tokOther"}, // loss
		// 0x4: not present → unresolved
	}
	_, sum := Simulate(sigs, nil, res, Config{DedupePerMatchupOutcome: true})
	if sum.TradesSimulated != 4 || sum.TradesResolved != 3 || sum.TradesWon != 2 {
		t.Fatalf("summary=%+v", sum)
	}
	if math.Abs(sum.HitRate-(2.0/3.0)) > 1e-9 {
		t.Fatalf("hit_rate=%.6f want 0.666...", sum.HitRate)
	}
	// realized = (80 - 20) + (50 - 20) + (-20) = 60+30-20 = 70
	if math.Abs(sum.TotalRealizedPnLUSDC-70.0) > 1e-6 {
		t.Fatalf("realized=%.4f want 70", sum.TotalRealizedPnLUSDC)
	}
}
