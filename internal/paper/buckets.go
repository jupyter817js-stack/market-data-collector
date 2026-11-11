package paper

import (
	"fmt"
	"sort"
)

// EdgeBucket carries a [lo, hi) percentage-points range and a label.
// hi == 0 means "open-ended above lo".
type EdgeBucket struct {
	LoPP  float64
	HiPP  float64
	Label string
}

// DefaultEdgeBuckets covers the practical net-edge claim range. Edges
// below the engine's MinNetEdgeBuy (3pp default) shouldn't fire as
// BUY_CANDIDATE, but the fade-promote path can produce 0-3pp; bucketed
// reporting lets us verify whether those marginal promotes earn their
// keep.
func DefaultEdgeBuckets() []EdgeBucket {
	return []EdgeBucket{
		{LoPP: 0, HiPP: 2, Label: "0-2pp"},
		{LoPP: 2, HiPP: 4, Label: "2-4pp"},
		{LoPP: 4, HiPP: 6, Label: "4-6pp"},
		{LoPP: 6, HiPP: 10, Label: "6-10pp"},
		{LoPP: 10, HiPP: 0, Label: "10pp+"},
	}
}

// BucketSummary aggregates Simulate output across one EdgeBucket.
type BucketSummary struct {
	Bucket            EdgeBucket
	TradesSimulated   int
	TradesWithMTM     int
	TradesResolved    int
	TradesWon         int
	TotalSizeUSDC     float64
	TotalMTMPnLUSDC   float64
	TotalRealizedUSDC float64
	AvgClaimedPP      float64 // mean of trade.ClaimedNetEdge × 100 across the bucket
	AvgMTMEdgePP      float64 // mean of trade.MTMEdgeFrac × 100 across with-MTM trades
	HitRate           float64 // TradesWon / TradesResolved, 0 if no resolved trades
}

// BucketByEdge groups trades by their ClaimedNetEdge (fraction) into
// the supplied buckets and aggregates simulator stats per bucket.
// Buckets are matched by [LoPP, HiPP); HiPP == 0 catches the open
// right tail. A trade whose claimed edge falls outside every bucket
// is dropped (caller's job to ensure full coverage).
func BucketByEdge(trades []Trade, buckets []EdgeBucket) []BucketSummary {
	out := make([]BucketSummary, len(buckets))
	for i, b := range buckets {
		out[i].Bucket = b
	}
	for _, tr := range trades {
		pp := tr.ClaimedNetEdge * 100
		idx := matchBucket(pp, buckets)
		if idx < 0 {
			continue
		}
		s := &out[idx]
		s.TradesSimulated++
		s.TotalSizeUSDC += tr.SizeUSDC
		s.AvgClaimedPP += pp
		if tr.HasMTM {
			s.TradesWithMTM++
			s.TotalMTMPnLUSDC += tr.MTMPnLUSDC
			s.AvgMTMEdgePP += tr.MTMEdgeFrac * 100
		}
		if tr.IsResolved {
			s.TradesResolved++
			s.TotalRealizedUSDC += tr.RealizedPnLUSDC
			if tr.Won {
				s.TradesWon++
			}
		}
	}
	for i := range out {
		s := &out[i]
		if s.TradesSimulated > 0 {
			s.AvgClaimedPP /= float64(s.TradesSimulated)
		}
		if s.TradesWithMTM > 0 {
			s.AvgMTMEdgePP /= float64(s.TradesWithMTM)
		}
		if s.TradesResolved > 0 {
			s.HitRate = float64(s.TradesWon) / float64(s.TradesResolved)
		}
	}
	// Keep the natural bucket order for the caller.
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Bucket.LoPP < out[j].Bucket.LoPP
	})
	return out
}

func matchBucket(pp float64, buckets []EdgeBucket) int {
	for i, b := range buckets {
		if pp < b.LoPP {
			continue
		}
		if b.HiPP == 0 || pp < b.HiPP {
			return i
		}
	}
	return -1
}

// FormatBuckets renders one line per bucket, suitable for fmt.Print.
// "n=" is trades simulated; "won=" is wins / resolved; "MTM%=" is
// total MTM PnL / total size; "real%=" is realized PnL / size on the
// resolved-only subset.
func FormatBuckets(rows []BucketSummary) string {
	out := "bucket          n   resolved  won   hit%    MTM$     MTM%    real$    real%   claimPP  MTMpp\n"
	for _, r := range rows {
		mtmPct := 0.0
		if r.TotalSizeUSDC > 0 {
			mtmPct = 100 * r.TotalMTMPnLUSDC / r.TotalSizeUSDC
		}
		realPct := 0.0
		if r.TradesResolved > 0 && r.TotalSizeUSDC > 0 {
			// realized fraction is over the SIZE OF RESOLVED trades only,
			// approximated as (resolved/simulated)×total_size.
			resSize := float64(r.TradesResolved) / float64(maxInt(r.TradesSimulated, 1)) * r.TotalSizeUSDC
			if resSize > 0 {
				realPct = 100 * r.TotalRealizedUSDC / resSize
			}
		}
		out += fmt.Sprintf("%-13s %3d  %5d/%-3d %3d %5.1f%%  %+8.2f %+6.2f%% %+8.2f %+6.2f%% %+5.2f   %+5.2f\n",
			r.Bucket.Label,
			r.TradesSimulated,
			r.TradesResolved, r.TradesSimulated,
			r.TradesWon,
			r.HitRate*100,
			r.TotalMTMPnLUSDC, mtmPct,
			r.TotalRealizedUSDC, realPct,
			r.AvgClaimedPP, r.AvgMTMEdgePP,
		)
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
