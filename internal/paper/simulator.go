// Package paper simulates virtual trades from the signals audit trail. v0
// is mark-to-market only: take the first BUY_CANDIDATE per (matchup,
// outcome_side) within a time window, "buy" proposed_size_usdc at that
// signal's pm_ask, and mark the position to the latest pm_mid for that
// outcome. Resolution-based realized PnL requires Polymarket close-state
// ingestion that doesn't exist yet — that's a follow-up layer.
//
// The simulator is pure: same inputs produce the same outputs. The store
// and cmd layers wrap it with I/O.
package paper

import (
	"sort"
	"time"

	sigpkg "github.com/your-org/pol-odds-collector/internal/signal"
)

// SignalRow is the minimal projection of internal/signal.Signal that the
// simulator needs. We re-declare here to keep the package boundary clean
// (paper does not depend on store).
type SignalRow struct {
	ID                    int64
	CreatedAt             time.Time
	MatchupID             int64
	OutcomeSide           string
	PolymarketConditionID string
	PolymarketTokenID     string
	HomeName              string
	AwayName              string
	LeagueName            string
	State                 sigpkg.State
	PMAsk                 float64 // 0 means missing — caller is expected to drop those upstream
	NetEdgeBuy            float64
	ProposedSizeUSDC      float64
}

// PriceRow is the current best price for marking-to-market.
type PriceRow struct {
	PolymarketTokenID string
	Bid               float64 // 0 means missing
	Ask               float64
	Mid               float64
	UpdatedAt         time.Time
}

// Resolution captures the outcome of a settled Polymarket market — namely
// which token paid $1. The simulator joins on PolymarketConditionID and
// compares trade.PolymarketTokenID to WinningTokenID.
type Resolution struct {
	ConditionID        string
	WinningTokenID     string
	WinningOutcomeName string
	ClosedAt           time.Time
}

// Trade is the synthesized paper trade.
type Trade struct {
	SignalID              int64
	OpenedAt              time.Time
	MatchupID             int64
	OutcomeSide           string
	PolymarketConditionID string
	PolymarketTokenID     string
	HomeName              string
	AwayName              string
	LeagueName            string

	SizeUSDC float64
	EntryAsk float64 // PM ask at the moment of signal (= our virtual fill price)
	Shares   float64 // SizeUSDC / EntryAsk

	ClaimedNetEdge float64 // signal.NetEdgeBuy at the time of fill (fraction)

	// MTM
	MarkPrice     float64 // latest pm_mid for the same token. 0 if unknown.
	MarkUpdatedAt time.Time
	MTMPnLUSDC    float64 // (MarkPrice/EntryAsk - 1) * SizeUSDC, 0 if unknown
	MTMEdgeFrac   float64 // MarkPrice - EntryAsk in probability units
	HasMTM        bool

	// Realized (populated when the market has settled)
	IsResolved      bool
	Won             bool    // trade.PolymarketTokenID == winning_token_id
	WinningTokenID  string  // for audit
	RealizedPayoff  float64 // 1.0 if Won else 0.0
	RealizedPnLUSDC float64 // shares * payoff - SizeUSDC
	ResolvedAt      time.Time
}

// Config tunes the simulator.
type Config struct {
	// If true (default), only the first BUY_CANDIDATE per (matchup, outcome)
	// counts as a trade. Subsequent re-fires within the window are ignored.
	DedupePerMatchupOutcome bool

	// If non-zero, override every signal's ProposedSizeUSDC with this value
	// (so re-runs can study sizing sensitivity). Zero = use signal's own.
	OverrideSizeUSDC float64
}

// Summary aggregates a slice of trades.
type Summary struct {
	CandidatesSeen      int
	TradesSimulated     int
	TradesWithMTM       int
	TradesUnpriced      int
	TotalSizeUSDC       float64
	TotalMTMPnLUSDC     float64
	AvgClaimedNetEdgePP float64
	AvgMTMEdgePP        float64

	// Realized (joined against polymarket_resolved_markets)
	TradesResolved       int
	TradesWon            int
	TotalRealizedPnLUSDC float64
	HitRate              float64 // TradesWon / TradesResolved, 0 if no resolved trades
}

// Simulate consumes the signal stream + a token→latest-price map +
// (optional) a condition→Resolution map and returns one Trade per
// (matchup, outcome) (or all candidates if dedupe is off). Input signals
// should already be filtered to State=BUY_CANDIDATE. Pass `resolutions=nil`
// to skip realized-PnL computation.
func Simulate(signals []SignalRow, latestPrices map[string]PriceRow, resolutions map[string]Resolution, cfg Config) ([]Trade, Summary) {
	// Sort by CreatedAt ascending so "first" wins under dedupe.
	sigs := make([]SignalRow, len(signals))
	copy(sigs, signals)
	sort.SliceStable(sigs, func(i, j int) bool { return sigs[i].CreatedAt.Before(sigs[j].CreatedAt) })

	taken := map[string]bool{}
	out := make([]Trade, 0, len(sigs))
	for _, s := range sigs {
		if s.State != sigpkg.StateBuyCandidate {
			continue
		}
		if s.PMAsk <= 0 {
			continue
		}
		key := dedupeKey(s.PolymarketTokenID, s.MatchupID, s.OutcomeSide)
		if cfg.DedupePerMatchupOutcome && taken[key] {
			continue
		}
		size := s.ProposedSizeUSDC
		if cfg.OverrideSizeUSDC > 0 {
			size = cfg.OverrideSizeUSDC
		}
		if size <= 0 {
			continue
		}
		t := Trade{
			SignalID:              s.ID,
			OpenedAt:              s.CreatedAt,
			MatchupID:             s.MatchupID,
			OutcomeSide:           s.OutcomeSide,
			PolymarketConditionID: s.PolymarketConditionID,
			PolymarketTokenID:     s.PolymarketTokenID,
			HomeName:              s.HomeName,
			AwayName:              s.AwayName,
			LeagueName:            s.LeagueName,
			SizeUSDC:              size,
			EntryAsk:              s.PMAsk,
			Shares:                size / s.PMAsk,
			ClaimedNetEdge:        s.NetEdgeBuy,
		}
		if p, ok := latestPrices[s.PolymarketTokenID]; ok {
			mark := markPriceFromPriceRow(p)
			if mark > 0 {
				t.MarkPrice = mark
				t.MarkUpdatedAt = p.UpdatedAt
				t.MTMEdgeFrac = mark - s.PMAsk
				t.MTMPnLUSDC = (mark/s.PMAsk - 1.0) * size
				t.HasMTM = true
			}
		}
		if r, ok := resolutions[s.PolymarketConditionID]; ok && r.WinningTokenID != "" {
			t.IsResolved = true
			t.WinningTokenID = r.WinningTokenID
			t.ResolvedAt = r.ClosedAt
			if s.PolymarketTokenID != "" && s.PolymarketTokenID == r.WinningTokenID {
				t.Won = true
				t.RealizedPayoff = 1.0
				t.RealizedPnLUSDC = t.Shares*1.0 - size
			} else {
				t.Won = false
				t.RealizedPayoff = 0.0
				t.RealizedPnLUSDC = -size
			}
		}
		out = append(out, t)
		if cfg.DedupePerMatchupOutcome {
			taken[key] = true
		}
	}

	return out, summarize(signals, out)
}

func summarize(allSignals []SignalRow, trades []Trade) Summary {
	s := Summary{}
	for _, sig := range allSignals {
		if sig.State == sigpkg.StateBuyCandidate {
			s.CandidatesSeen++
		}
	}
	s.TradesSimulated = len(trades)
	var sumSize, sumPnL, sumClaimedNet, sumMTMEdge, sumRealized float64
	for _, t := range trades {
		sumSize += t.SizeUSDC
		sumClaimedNet += t.ClaimedNetEdge
		if t.HasMTM {
			s.TradesWithMTM++
			sumPnL += t.MTMPnLUSDC
			sumMTMEdge += t.MTMEdgeFrac
		} else {
			s.TradesUnpriced++
		}
		if t.IsResolved {
			s.TradesResolved++
			sumRealized += t.RealizedPnLUSDC
			if t.Won {
				s.TradesWon++
			}
		}
	}
	s.TotalSizeUSDC = sumSize
	s.TotalMTMPnLUSDC = sumPnL
	s.TotalRealizedPnLUSDC = sumRealized
	if s.TradesSimulated > 0 {
		s.AvgClaimedNetEdgePP = (sumClaimedNet / float64(s.TradesSimulated)) * 100
	}
	if s.TradesWithMTM > 0 {
		s.AvgMTMEdgePP = (sumMTMEdge / float64(s.TradesWithMTM)) * 100
	}
	if s.TradesResolved > 0 {
		s.HitRate = float64(s.TradesWon) / float64(s.TradesResolved)
	}
	return s
}

// markPriceFromPriceRow picks the best mark-to-market price available from a
// snapshot: prefer mid, fall back to (bid+ask)/2 if both bid and ask are set,
// then to ask alone. Settled tokens commonly expose ask=0.001 with mid=null
// so requiring mid drops the most informative MTM signal — a near-zero ask
// after settlement IS the correct mark, not "unpriced".
func markPriceFromPriceRow(p PriceRow) float64 {
	if p.Mid > 0 {
		return p.Mid
	}
	if p.Bid > 0 && p.Ask > 0 {
		return (p.Bid + p.Ask) / 2
	}
	if p.Ask > 0 {
		return p.Ask
	}
	if p.Bid > 0 {
		return p.Bid
	}
	return 0
}

// dedupeKey picks the most specific identity the simulator can see for a
// candidate trade. polymarket_token_id is the actual tradeable unit and is
// stable across Pinnacle's parallel matchup_id quirk (the same real-world
// game can appear under two Pinnacle matchups that both map to the same PM
// condition + token). When the token is missing we fall back to the
// (matchup, side) tuple that older code used.
func dedupeKey(tokenID string, matchupID int64, side string) string {
	if tokenID != "" {
		return "tok:" + tokenID
	}
	return side + ":" + itoa(matchupID)
}

func itoa(n int64) string {
	// Avoid pulling in strconv just for this. Keeps the package lean.
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
