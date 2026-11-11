package signal

import (
	"database/sql"
	"math"
	"testing"
)

// baseConfig matches the .env defaults: $20 trade, 3pp net-edge threshold,
// 10s freshness, $100 min PM liquidity, half-spread cost, 50bps base slip
// + 200bps×(size/liq), 0.1pp/s latency, 50% of overround as safety.
func baseConfig() Config {
	return Config{
		ProposedSizeUSDC:     20,
		MinNetEdgeBuy:        0.03,
		MinGrossEdgeSell:     0.03,
		MaxPinnacleAgeSec:    10,
		MaxPMAgeSec:          10,
		MinPMLiquidityUSDC:   100,
		SpreadCostMultiplier: 0.5,
		SlippageBaseBps:      50,
		SlippageVsLiqBps:     200,
		LatencyBufferPerSec:  0.001,
		OverroundShare:       0.5,
	}
}

// healthyRow is a generic well-formed row: home outcome aligned, market
// open, fresh, liquid, with adjustable gross/sell edges via mutators.
func healthyRow(grossBuy, grossSell float64) EdgeRow {
	return EdgeRow{
		MatchupID:             1,
		PolymarketConditionID: "0xabc",
		PolymarketTokenID:     "tok1",
		OutcomeSide:           "home",
		OutcomeName:           "MOUZ",
		HomeName:              "MOUZ",
		AwayName:              "magic",
		LeagueName:            "CS2 - PGL Astana",
		PinnacleFairProb:      0.74,
		PMAsk:                 sql.NullFloat64{Float64: 0.70, Valid: true},
		PMBid:                 sql.NullFloat64{Float64: 0.69, Valid: true},
		PMMid:                 sql.NullFloat64{Float64: 0.695, Valid: true},
		PMSpread:              sql.NullFloat64{Float64: 0.01, Valid: true},
		PMLiquidity:           sql.NullFloat64{Float64: 5000, Valid: true},
		PinnacleMarketStatus:  "open",
		PinnacleMarketVersion: 100,
		PinnacleOverround:     0.04,
		GrossEdgeToBuy:        grossBuy,
		GrossEdgeToSell:       grossSell,
		PinnacleAgeSec:        1,
		PMAgeSec:              1,
	}
}

func TestClassify_BuyCandidate_WhenNetEdgeAboveThreshold(t *testing.T) {
	// gross 4pp, costs ~ spread 0.5pp + slip 0.5pp + latency 0.1pp + overround 2pp = 3.1pp
	// → net ≈ 0.9pp < 3pp default → NOT a buy. Bump gross to 8pp.
	row := healthyRow(0.08, -0.01)
	got := Classify(row, baseConfig())
	if got.State != StateBuyCandidate {
		t.Fatalf("state=%s want BUY_CANDIDATE; net=%.4f reasons=%v", got.State, got.NetEdgeBuy, got.Reasons)
	}
	if got.NetEdgeBuy < 0.03 {
		t.Fatalf("net=%.4f want >= 0.03", got.NetEdgeBuy)
	}
}

func TestClassify_EdgeTooSmall_WhenGrossPositiveButCostsEatIt(t *testing.T) {
	// gross 2pp → costs ~3pp → net negative → EDGE_TOO_SMALL (not NO_EDGE since gross > 0)
	row := healthyRow(0.02, -0.01)
	got := Classify(row, baseConfig())
	if got.State != StateEdgeTooSmall {
		t.Fatalf("state=%s want EDGE_TOO_SMALL; net=%.4f", got.State, got.NetEdgeBuy)
	}
}

func TestClassify_NoEdge_WhenBothGrossNonPositive(t *testing.T) {
	row := healthyRow(-0.01, -0.01)
	got := Classify(row, baseConfig())
	if got.State != StateNoEdge {
		t.Fatalf("state=%s want NO_EDGE", got.State)
	}
}

func TestClassify_Overvalued_WhenSellGrossAboveThreshold(t *testing.T) {
	// PM bid is high relative to fair → selling makes sense.
	row := healthyRow(-0.05, 0.05)
	got := Classify(row, baseConfig())
	if got.State != StateOvervalued {
		t.Fatalf("state=%s want OVERVALUED", got.State)
	}
}

func TestClassify_StaleData_PinnacleTooOld(t *testing.T) {
	row := healthyRow(0.08, -0.01)
	row.PinnacleAgeSec = 30
	got := Classify(row, baseConfig())
	if got.State != StateStaleData {
		t.Fatalf("state=%s want STALE_DATA", got.State)
	}
}

func TestClassify_StaleData_PolymarketTooOld(t *testing.T) {
	row := healthyRow(0.08, -0.01)
	row.PMAgeSec = 30
	got := Classify(row, baseConfig())
	if got.State != StateStaleData {
		t.Fatalf("state=%s want STALE_DATA", got.State)
	}
}

func TestClassify_LowLiquidity_BelowMin(t *testing.T) {
	row := healthyRow(0.08, -0.01)
	row.PMLiquidity = sql.NullFloat64{Float64: 50, Valid: true}
	got := Classify(row, baseConfig())
	if got.State != StateLowLiquidity {
		t.Fatalf("state=%s want LOW_LIQUIDITY", got.State)
	}
}

func TestClassify_LowLiquidity_WhenNullLiquidity(t *testing.T) {
	row := healthyRow(0.08, -0.01)
	row.PMLiquidity = sql.NullFloat64{Valid: false}
	got := Classify(row, baseConfig())
	if got.State != StateLowLiquidity {
		t.Fatalf("state=%s want LOW_LIQUIDITY (null liquidity)", got.State)
	}
}

func TestClassify_MarketSuspended_WhenPinnacleNotOpen(t *testing.T) {
	row := healthyRow(0.08, -0.01)
	row.PinnacleMarketStatus = "suspended"
	got := Classify(row, baseConfig())
	if got.State != StateMarketSuspended {
		t.Fatalf("state=%s want MARKET_SUSPENDED", got.State)
	}
}

func TestClassify_MappingInvalid_UnknownOutcomeSide(t *testing.T) {
	row := healthyRow(0.08, -0.01)
	row.OutcomeSide = "unknown"
	got := Classify(row, baseConfig())
	if got.State != StateMappingInvalid {
		t.Fatalf("state=%s want MAPPING_INVALID", got.State)
	}
}

func TestClassify_MappingInvalid_WhenAskOrBidMissing(t *testing.T) {
	row := healthyRow(0.08, -0.01)
	row.PMAsk = sql.NullFloat64{Valid: false}
	got := Classify(row, baseConfig())
	if got.State != StateMappingInvalid {
		t.Fatalf("state=%s want MAPPING_INVALID", got.State)
	}
}

func TestClassify_NetEdgeBreakdownIsConsistent(t *testing.T) {
	row := healthyRow(0.10, -0.01)
	got := Classify(row, baseConfig())
	// recomputed cost components must match the documented formula
	wantSpread := 0.01 * 0.5
	if math.Abs(got.SpreadCost-wantSpread) > 1e-9 {
		t.Fatalf("spread_cost=%.6f want %.6f", got.SpreadCost, wantSpread)
	}
	sizeLiqRatio := 20.0 / 5000.0
	wantSlip := (50.0 + sizeLiqRatio*200.0) / 10000.0
	if math.Abs(got.SlippageBuffer-wantSlip) > 1e-9 {
		t.Fatalf("slip=%.6f want %.6f", got.SlippageBuffer, wantSlip)
	}
	if math.Abs(got.LatencyBuffer-(1.0*0.001)) > 1e-9 {
		t.Fatalf("latency=%.6f want %.6f", got.LatencyBuffer, 1.0*0.001)
	}
	if math.Abs(got.OverroundBuffer-(0.04*0.5)) > 1e-9 {
		t.Fatalf("overround=%.6f want %.6f", got.OverroundBuffer, 0.04*0.5)
	}
	wantNet := 0.10 - wantSpread - wantSlip - (1.0 * 0.001) - (0.04 * 0.5)
	if math.Abs(got.NetEdgeBuy-wantNet) > 1e-9 {
		t.Fatalf("net=%.6f want %.6f", got.NetEdgeBuy, wantNet)
	}
}

func TestCountByState(t *testing.T) {
	sigs := []Signal{
		{State: StateBuyCandidate}, {State: StateBuyCandidate}, {State: StateNoEdge},
	}
	got := CountByState(sigs)
	if got[StateBuyCandidate] != 2 || got[StateNoEdge] != 1 {
		t.Fatalf("got=%v", got)
	}
}

// OrderingMatters checks that the short-circuit chain runs before any
// numeric path: suspended/stale/low-liq beats edge classification even when
// the gross is huge.
func TestClassify_ShortCircuitOrdering(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*EdgeRow)
		want State
	}{
		{"unknown_side beats edge", func(r *EdgeRow) { r.OutcomeSide = "unknown" }, StateMappingInvalid},
		{"suspended beats edge", func(r *EdgeRow) { r.PinnacleMarketStatus = "suspended" }, StateMarketSuspended},
		{"stale pinnacle beats edge", func(r *EdgeRow) { r.PinnacleAgeSec = 99 }, StateStaleData},
		{"stale pm beats edge", func(r *EdgeRow) { r.PMAgeSec = 99 }, StateStaleData},
		{"low liq beats edge", func(r *EdgeRow) { r.PMLiquidity = sql.NullFloat64{Float64: 1, Valid: true} }, StateLowLiquidity},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := healthyRow(0.50, -0.01) // wildly profitable on paper
			tc.mut(&row)
			got := Classify(row, baseConfig())
			if got.State != tc.want {
				t.Fatalf("state=%s want %s", got.State, tc.want)
			}
		})
	}
}

func TestPromoteFadePairs_PromotesEdgeTooSmallOppositeOvervalued(t *testing.T) {
	// Same matchup, two sides. Home is OVERVALUED on PM (PM bid high). Away
	// has positive gross_buy but penalties leave net just below the 3pp gate
	// → EDGE_TOO_SMALL. We expect away to be promoted to BUY_CANDIDATE.
	home := healthyRow(0, 0.05) // gross sell 5pp → OVERVALUED
	home.OutcomeSide = "home"
	home.PolymarketTokenID = "tokHome"
	away := healthyRow(0.04, -0.05) // gross buy 4pp, penalties ~3pp → EDGE_TOO_SMALL but positive net
	away.OutcomeSide = "away"
	away.PolymarketTokenID = "tokAway"

	cfg := baseConfig()
	sigs := []Signal{Classify(home, cfg), Classify(away, cfg)}
	if sigs[0].State != StateOvervalued {
		t.Fatalf("home state=%s want OVERVALUED", sigs[0].State)
	}
	if sigs[1].State != StateEdgeTooSmall {
		t.Fatalf("away state=%s want EDGE_TOO_SMALL", sigs[1].State)
	}

	promoted := PromoteFadePairs(sigs)
	if promoted[0].State != StateOvervalued {
		t.Fatalf("home post-promote=%s; OVERVALUED side itself must stay", promoted[0].State)
	}
	if promoted[1].State != StateBuyCandidate {
		t.Fatalf("away post-promote=%s; want BUY_CANDIDATE", promoted[1].State)
	}
	if len(promoted[1].Reasons) == 0 || !contains(promoted[1].Reasons[0], "fade-pair") {
		t.Fatalf("expected fade-pair reason, got %v", promoted[1].Reasons)
	}
}

func TestPromoteFadePairs_SkipsThreeWaySoccer(t *testing.T) {
	// Soccer-like matchup with a 'draw' outcome present. OVERVALUED on home
	// must NOT promote EDGE_TOO_SMALL on away — the value could be in draw,
	// the away mapping is not safe.
	home := healthyRow(0, 0.05)
	home.OutcomeSide = "home"
	draw := healthyRow(-0.01, -0.01)
	draw.OutcomeSide = "draw"
	away := healthyRow(0.04, -0.05)
	away.OutcomeSide = "away"

	cfg := baseConfig()
	sigs := []Signal{Classify(home, cfg), Classify(draw, cfg), Classify(away, cfg)}
	promoted := PromoteFadePairs(sigs)
	if promoted[2].State != StateEdgeTooSmall {
		t.Fatalf("away in 3-way should stay EDGE_TOO_SMALL, got %s", promoted[2].State)
	}
}

func TestPromoteFadePairs_SkipsNegativeNetEdge(t *testing.T) {
	// EDGE_TOO_SMALL with NEGATIVE net edge (penalties exceed gross). The
	// fade pair confirms a divergence but the engine's own cost model still
	// says the trade loses on entry; don't promote.
	home := healthyRow(0, 0.05)
	home.OutcomeSide = "home"
	away := healthyRow(0.005, -0.05)
	away.OutcomeSide = "away"

	cfg := baseConfig()
	sigs := []Signal{Classify(home, cfg), Classify(away, cfg)}
	if sigs[1].NetEdgeBuy >= 0 {
		t.Fatalf("test setup: expected negative net edge, got %.4f", sigs[1].NetEdgeBuy)
	}
	promoted := PromoteFadePairs(sigs)
	if promoted[1].State != StateEdgeTooSmall {
		t.Fatalf("negative-net EDGE_TOO_SMALL should not promote, got %s", promoted[1].State)
	}
}

func TestPromoteFadePairs_NoPromoteWhenNoOppositeOvervalued(t *testing.T) {
	home := healthyRow(0, 0.01)
	home.OutcomeSide = "home" // not OVERVALUED
	away := healthyRow(0.04, -0.05)
	away.OutcomeSide = "away" // EDGE_TOO_SMALL positive-net
	cfg := baseConfig()
	sigs := []Signal{Classify(home, cfg), Classify(away, cfg)}
	promoted := PromoteFadePairs(sigs)
	if promoted[1].State != StateEdgeTooSmall {
		t.Fatalf("away should stay EDGE_TOO_SMALL without paired OVERVALUED, got %s", promoted[1].State)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestClassify_DepthAdjustedAskReplacesTopOfBookForSlippage(t *testing.T) {
	// With DepthAvgAsk set above PMAsk, slippage buffer = depth - top
	// instead of the legacy bps × size/liq heuristic. NetEdge should
	// shrink accordingly.
	row := healthyRow(0.08, -0.01) // gross 8pp
	row.PMAsk = sql.NullFloat64{Float64: 0.70, Valid: true}
	row.DepthAvgAsk = sql.NullFloat64{Float64: 0.72, Valid: true} // 2pp worse than top
	cfg := baseConfig()
	got := Classify(row, cfg)
	if got.SlippageBuffer < 0.0199 || got.SlippageBuffer > 0.0201 {
		t.Fatalf("SlippageBuffer=%v want 0.02 (depth − top)", got.SlippageBuffer)
	}
	// gross 8pp − spread 0.5pp − slip 2pp − latency 0.1pp − overround 2pp
	// = ~3.4pp; should still pass the 3pp threshold.
	if got.State != StateBuyCandidate {
		t.Fatalf("state=%s want BUY_CANDIDATE (net still positive past threshold)", got.State)
	}
}

func TestClassify_NoDepthAdjustmentFallsBackToBpsModel(t *testing.T) {
	// Without DepthAvgAsk, slippage should equal the legacy formula.
	row := healthyRow(0.08, -0.01)
	row.DepthAvgAsk = sql.NullFloat64{Valid: false}
	cfg := baseConfig()
	got := Classify(row, cfg)
	// base 50bps + size/liq 200bps × (20/5000) = 0.5pp + 0.08pp = 0.58pp
	if got.SlippageBuffer < 0.0050 || got.SlippageBuffer > 0.0052 {
		t.Fatalf("SlippageBuffer=%v want ~0.00508 (legacy bps: (50 + 200×20/5000)/10000)", got.SlippageBuffer)
	}
}

func TestClassify_DepthAdjustedBelowTopClampsToZero(t *testing.T) {
	// If depth-adjusted ask comes back <= top (shouldn't happen but
	// defensive), we should not over-subtract.
	row := healthyRow(0.08, -0.01)
	row.PMAsk = sql.NullFloat64{Float64: 0.70, Valid: true}
	row.DepthAvgAsk = sql.NullFloat64{Float64: 0.69, Valid: true}
	cfg := baseConfig()
	got := Classify(row, cfg)
	// depthAdjust would be -0.01 → clamped to 0 → fall back to bps
	if got.SlippageBuffer < 0.0050 || got.SlippageBuffer > 0.0052 {
		t.Fatalf("SlippageBuffer=%v want ~0.00508 (clamped fallback to legacy bps)", got.SlippageBuffer)
	}
}

func TestClassify_MatchPhaseEndedDropsAsStale(t *testing.T) {
	row := healthyRow(0.50, -0.01) // wildly profitable on paper
	row.MatchPhase = "ended"
	got := Classify(row, baseConfig())
	if got.State != StateStaleData {
		t.Fatalf("state=%s want STALE_DATA when match_phase=ended", got.State)
	}
}

func TestClassify_RecentEventGuardBeatsEdge(t *testing.T) {
	// Even a 50pp gross edge does not pass while Pinnacle just suspended
	// odds (goal / card / VAR window).
	row := healthyRow(0.50, -0.01)
	row.MatchEventState = "recent_event"
	got := Classify(row, baseConfig())
	if got.State != StateRecentEvent {
		t.Fatalf("state=%s want RECENT_EVENT", got.State)
	}
}

func TestClassify_StableEventStatePassesThroughNormalLogic(t *testing.T) {
	// event_state='stable' must NOT block — it's the happy path.
	row := healthyRow(0.08, -0.01)
	row.MatchEventState = "stable"
	got := Classify(row, baseConfig())
	if got.State != StateBuyCandidate {
		t.Fatalf("state=%s want BUY_CANDIDATE (stable event_state should not block)", got.State)
	}
}

func TestClassify_EmptyMatchStateDoesNotBlock(t *testing.T) {
	// Non-soccer matchups or rows without a match_state row keep going
	// through the normal edge path.
	row := healthyRow(0.08, -0.01)
	row.MatchEventState = ""
	row.MatchPhase = ""
	got := Classify(row, baseConfig())
	if got.State != StateBuyCandidate {
		t.Fatalf("state=%s want BUY_CANDIDATE (empty match_state must not block)", got.State)
	}
}
