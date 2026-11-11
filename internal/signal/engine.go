// Package signal classifies pinnacle_polymarket_cs2_moneyline_edges rows
// into actionable trade signals. The engine is pure: given an EdgeRow + Config
// it returns a Signal with a state, net-edge breakdown, and human-readable
// reasons. Persistence and polling live in the store and cmd layers.
//
// Step 4 of the live-only pipeline. Step 5 (executor) reads only the latest
// BUY_CANDIDATE-state signals.
package signal

import (
	"database/sql"
	"fmt"
	"math"
)

// State is the classification of an EdgeRow at one moment in time.
type State string

const (
	StateBuyCandidate    State = "BUY_CANDIDATE"
	StateOvervalued      State = "OVERVALUED"
	StateEdgeTooSmall    State = "EDGE_TOO_SMALL"
	StateNoEdge          State = "NO_EDGE"
	StateStaleData       State = "STALE_DATA"
	StateLowLiquidity    State = "LOW_LIQUIDITY"
	StateMappingInvalid  State = "MAPPING_INVALID"
	StateMarketSuspended State = "MARKET_SUSPENDED"
	// StateRecentEvent fires when pinnacle_match_state.event_state is
	// 'recent_event' — Pinnacle suspended its moneyline at least once
	// in the last 5 min, which strongly implies a goal / red card /
	// VAR review just happened. PM reacts to those slowly so signal-
	// engine's "edge" right after the event is often a stale-anchor
	// false-positive. We hold and let the dust settle.
	StateRecentEvent State = "RECENT_EVENT"
)

// EdgeRow mirrors one row of pinnacle_polymarket_cs2_moneyline_edges. NULLable
// columns use sql.NullFloat64 so the engine can branch on Valid.
type EdgeRow struct {
	MatchupID             int64
	PolymarketConditionID string
	PolymarketTokenID     string
	OutcomeSide           string // "home" | "away" | "unknown"
	OutcomeName           string
	HomeName              string
	AwayName              string
	LeagueName            string

	PinnacleFairProb float64
	PMAsk            sql.NullFloat64
	PMBid            sql.NullFloat64
	PMMid            sql.NullFloat64
	PMSpread         sql.NullFloat64
	PMLiquidity      sql.NullFloat64

	PinnacleMarketStatus  string
	PinnacleMarketVersion int64
	PinnacleOverround     float64

	GrossEdgeToBuy  float64
	GrossEdgeToSell float64

	// DepthAvgAsk is the size-weighted average ask price for the
	// engine's per-trade size (Config.ProposedSizeUSDC) walking the
	// current PM order book. Populated by ReadEdges via the
	// polymarket_depth_avg_price() SQL function. NULL when the
	// book lacks enough cumulative depth or no snapshot exists.
	// When valid, Classify uses it as the effective fill price
	// instead of PMAsk (top-of-book), which makes the gross edge
	// honest for non-trivial sizes.
	DepthAvgAsk sql.NullFloat64

	PinnacleAgeSec float64
	PMAgeSec       float64

	// MatchEventState mirrors pinnacle_match_state.event_state for
	// this row's matchup_id. 'recent_event' = a goal / card / VAR /
	// major play happened within the last 5 min (proxy: Pinnacle
	// suspended the moneyline). 'dormant' = market hasn't moved in 5
	// min. 'stable' = normal. Empty when no match_state row exists.
	MatchEventState string
	// MatchPhase mirrors pinnacle_match_state.match_phase: 'pre' /
	// 'first_half' / 'halftime' / 'second_half' / 'live' / 'ended' /
	// 'suspended' / 'unknown'. Empty when no match_state row exists.
	MatchPhase string
}

// Config is the engine's tuning surface. Edge values are fractions of
// probability, not percentages (0.03 = 3pp).
type Config struct {
	ProposedSizeUSDC float64

	MinNetEdgeBuy    float64
	MinGrossEdgeSell float64

	MaxPinnacleAgeSec float64
	MaxPMAgeSec       float64

	MinPMLiquidityUSDC float64

	SpreadCostMultiplier float64 // 0.5 = half-spread = mid→best cost
	SlippageBaseBps      float64 // flat slip in basis points
	SlippageVsLiqBps     float64 // additional bps per (size/liquidity) ratio
	LatencyBufferPerSec  float64 // penalty per second of staleness (fraction)
	OverroundShare       float64 // fraction of Pinnacle overround treated as safety margin
}

// Signal is the classifier's output.
type Signal struct {
	EdgeRow

	// ID is the signals.id from Postgres. Zero when the Signal was just
	// produced by Classify and hasn't been persisted yet; the store layer
	// fills it on read (ListRecentSignals) so downstream consumers like
	// the executor planner can reference it as a foreign key.
	ID int64

	State            State
	ProposedSizeUSDC float64
	ThresholdNetEdge float64

	SpreadCost      float64
	SlippageBuffer  float64
	LatencyBuffer   float64
	OverroundBuffer float64
	NetEdgeBuy      float64

	Reasons []string
}

// Classify is pure: same input → same output. Order of checks matters; we
// short-circuit on hard-disqualifying conditions before computing the
// numeric net edge so the cost fields stay zero for trivially-rejected rows.
func Classify(row EdgeRow, cfg Config) Signal {
	sig := Signal{
		EdgeRow:          row,
		ProposedSizeUSDC: cfg.ProposedSizeUSDC,
		ThresholdNetEdge: cfg.MinNetEdgeBuy,
	}

	if row.OutcomeSide == "" || row.OutcomeSide == "unknown" {
		sig.State = StateMappingInvalid
		sig.Reasons = []string{"outcome_side could not be aligned to home/away"}
		return sig
	}
	if row.PinnacleMarketStatus != "" && row.PinnacleMarketStatus != "open" {
		sig.State = StateMarketSuspended
		sig.Reasons = []string{"pinnacle market status: " + row.PinnacleMarketStatus}
		return sig
	}
	if row.PinnacleAgeSec > cfg.MaxPinnacleAgeSec {
		sig.State = StateStaleData
		sig.Reasons = []string{fmt.Sprintf("pinnacle age %.1fs > %.1fs", row.PinnacleAgeSec, cfg.MaxPinnacleAgeSec)}
		return sig
	}
	if row.PMAgeSec > cfg.MaxPMAgeSec {
		sig.State = StateStaleData
		sig.Reasons = []string{fmt.Sprintf("PM age %.1fs > %.1fs", row.PMAgeSec, cfg.MaxPMAgeSec)}
		return sig
	}
	if !row.PMAsk.Valid || !row.PMBid.Valid {
		sig.State = StateMappingInvalid
		sig.Reasons = []string{"PM bid/ask missing (no price snapshot for this token)"}
		return sig
	}
	// Match-state guardrails sit between MAPPING_INVALID and the
	// numeric edge path. A pinnacle_match_state row may not exist
	// (empty string) — that's fine, fall through to normal logic.
	if row.MatchPhase == "ended" {
		sig.State = StateStaleData
		sig.Reasons = []string{"pinnacle_match_state.match_phase=ended (period 0 closed); last_is_live not yet flipped"}
		return sig
	}
	if row.MatchEventState == "recent_event" {
		sig.State = StateRecentEvent
		sig.Reasons = []string{"pinnacle_match_state.event_state=recent_event (suspension burst in last 5 min); hold until book stabilises"}
		return sig
	}
	if !row.PMLiquidity.Valid || row.PMLiquidity.Float64 < cfg.MinPMLiquidityUSDC {
		sig.State = StateLowLiquidity
		liq := 0.0
		if row.PMLiquidity.Valid {
			liq = row.PMLiquidity.Float64
		}
		sig.Reasons = []string{fmt.Sprintf("PM liquidity %.2f < %.2f", liq, cfg.MinPMLiquidityUSDC)}
		return sig
	}

	// Cost components — all fractions of probability.
	if row.PMSpread.Valid {
		sig.SpreadCost = row.PMSpread.Float64 * cfg.SpreadCostMultiplier
	} else {
		sig.SpreadCost = (row.PMAsk.Float64 - row.PMBid.Float64) * cfg.SpreadCostMultiplier
	}
	if sig.SpreadCost < 0 {
		sig.SpreadCost = 0
	}

	// Slippage. When the order book ladder gave us a depth-adjusted
	// average ask for our trade size, the average IS the slippage —
	// no need to also tack on a size/liquidity heuristic on top. Drop
	// the heuristic to zero in that path so we don't double-count.
	// Without the ladder, fall back to the legacy bps model so old
	// behavior is preserved exactly.
	depthAdjust := 0.0
	if row.DepthAvgAsk.Valid && row.DepthAvgAsk.Float64 > 0 && row.PMAsk.Valid {
		depthAdjust = row.DepthAvgAsk.Float64 - row.PMAsk.Float64
		if depthAdjust < 0 {
			depthAdjust = 0 // book was thicker than top — ladder can't go below top
		}
	}
	if depthAdjust > 0 {
		sig.SlippageBuffer = depthAdjust
	} else {
		sizeLiqRatio := 0.0
		if row.PMLiquidity.Float64 > 0 {
			sizeLiqRatio = cfg.ProposedSizeUSDC / row.PMLiquidity.Float64
		}
		sig.SlippageBuffer = (cfg.SlippageBaseBps + sizeLiqRatio*cfg.SlippageVsLiqBps) / 10000.0
	}

	maxAge := row.PinnacleAgeSec
	if row.PMAgeSec > maxAge {
		maxAge = row.PMAgeSec
	}
	sig.LatencyBuffer = maxAge * cfg.LatencyBufferPerSec

	sig.OverroundBuffer = math.Abs(row.PinnacleOverround) * cfg.OverroundShare

	sig.NetEdgeBuy = row.GrossEdgeToBuy - sig.SpreadCost - sig.SlippageBuffer - sig.LatencyBuffer - sig.OverroundBuffer

	// Classification order:
	//   1. Overvalued (sell-side) takes precedence if we already hold (Step 5 will
	//      gate on actual inventory; here we still emit the signal).
	//   2. Buy candidate if net edge clears threshold.
	//   3. Else categorize whether gross was positive but eaten by costs.
	switch {
	case row.GrossEdgeToSell >= cfg.MinGrossEdgeSell:
		sig.State = StateOvervalued
		sig.Reasons = []string{fmt.Sprintf("PM bid %.4f > fair %.4f (gross_sell %.2fpp)",
			row.PMBid.Float64, row.PinnacleFairProb, row.GrossEdgeToSell*100)}
	case sig.NetEdgeBuy >= cfg.MinNetEdgeBuy:
		sig.State = StateBuyCandidate
		sig.Reasons = []string{fmt.Sprintf("net %.2fpp >= threshold %.2fpp (gross %.2fpp, spread %.2fpp, slip %.2fpp, lat %.2fpp, over %.2fpp)",
			sig.NetEdgeBuy*100, cfg.MinNetEdgeBuy*100,
			row.GrossEdgeToBuy*100, sig.SpreadCost*100, sig.SlippageBuffer*100,
			sig.LatencyBuffer*100, sig.OverroundBuffer*100)}
	case row.GrossEdgeToBuy > 0:
		sig.State = StateEdgeTooSmall
		sig.Reasons = []string{fmt.Sprintf("gross %.2fpp but net %.2fpp < threshold %.2fpp",
			row.GrossEdgeToBuy*100, sig.NetEdgeBuy*100, cfg.MinNetEdgeBuy*100)}
	default:
		sig.State = StateNoEdge
		sig.Reasons = []string{fmt.Sprintf("gross_buy %.2fpp, gross_sell %.2fpp",
			row.GrossEdgeToBuy*100, row.GrossEdgeToSell*100)}
	}

	return sig
}

// PromoteFadePairs converts EDGE_TOO_SMALL signals into BUY_CANDIDATE when the
// SAME matchup has an OVERVALUED signal on the OPPOSITE outcome and the market
// is 2-way (no "draw" outcome present in any signal row for this matchup). The
// math: in a binary market PM ask_A + ask_B is bounded near 1 + spread, so
// "PM bid_A > pinnacle_fair_A" (OVERVALUED A) implies "PM ask_B < fair_B"
// (cheap B). Without an inventory we can't sell A, but buying B captures the
// same divergence. Only promote when the EDGE_TOO_SMALL side's NetEdgeBuy is
// non-negative — otherwise the engine's own penalty model says the trade
// loses even before counting our certainty about the fade.
//
// 3-way markets (soccer) are excluded because OVERVALUED home doesn't
// uniquely point at "buy away" — draw could absorb the value instead.
func PromoteFadePairs(sigs []Signal) []Signal {
	// matchup_id -> outcome_side set with OVERVALUED
	overvaluedSides := map[int64]map[string]bool{}
	threeWay := map[int64]bool{}
	for _, s := range sigs {
		if s.OutcomeSide == "draw" {
			threeWay[s.MatchupID] = true
		}
		if s.State == StateOvervalued {
			if overvaluedSides[s.MatchupID] == nil {
				overvaluedSides[s.MatchupID] = map[string]bool{}
			}
			overvaluedSides[s.MatchupID][s.OutcomeSide] = true
		}
	}

	out := make([]Signal, len(sigs))
	for i, s := range sigs {
		out[i] = s
		if s.State != StateEdgeTooSmall {
			continue
		}
		if threeWay[s.MatchupID] {
			continue
		}
		if s.NetEdgeBuy < 0 {
			continue
		}
		var oppositeSide string
		switch s.OutcomeSide {
		case "home":
			oppositeSide = "away"
		case "away":
			oppositeSide = "home"
		default:
			continue
		}
		if !overvaluedSides[s.MatchupID][oppositeSide] {
			continue
		}
		out[i].State = StateBuyCandidate
		reason := fmt.Sprintf("fade-pair promote: opposite side %s is OVERVALUED on PM, this side's net %.2fpp under %.2fpp threshold but binary-market mirror makes it executable",
			oppositeSide, s.NetEdgeBuy*100, 0.0)
		out[i].Reasons = append([]string{reason}, s.Reasons...)
	}
	return out
}

// CountByState is a small convenience for tick telemetry.
func CountByState(sigs []Signal) map[State]int {
	out := map[State]int{}
	for _, s := range sigs {
		out[s.State]++
	}
	return out
}
