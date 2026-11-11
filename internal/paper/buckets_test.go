package paper

import (
	"testing"
)

func mkTrade(claimedFrac, mtmFrac float64, hasMTM bool, resolved bool, won bool, size float64, mtmPnL, realizedPnL float64) Trade {
	return Trade{
		ClaimedNetEdge:  claimedFrac,
		MTMEdgeFrac:     mtmFrac,
		HasMTM:          hasMTM,
		IsResolved:      resolved,
		Won:             won,
		SizeUSDC:        size,
		MTMPnLUSDC:      mtmPnL,
		RealizedPnLUSDC: realizedPnL,
	}
}

func TestBucketByEdge_PartitionsByClaimedPP(t *testing.T) {
	trades := []Trade{
		mkTrade(0.01, 0.005, true, true, false, 20, -2, -20), // 1pp → bucket 0-2
		mkTrade(0.025, 0.015, true, true, true, 20, 8, 10),   // 2.5pp → 2-4
		mkTrade(0.05, 0.04, true, false, false, 20, 8, 0),    // 5pp → 4-6
		mkTrade(0.08, 0.07, true, true, true, 20, 14, 15),    // 8pp → 6-10
		mkTrade(0.15, 0.12, true, true, true, 20, 24, 25),    // 15pp → 10+
	}
	rows := BucketByEdge(trades, DefaultEdgeBuckets())
	if len(rows) != 5 {
		t.Fatalf("rows=%d want 5", len(rows))
	}
	// Each bucket should hold exactly one trade.
	for _, r := range rows {
		if r.TradesSimulated != 1 {
			t.Fatalf("bucket %s simulated=%d want 1", r.Bucket.Label, r.TradesSimulated)
		}
	}
}

func TestBucketByEdge_AggregatesWithinBucket(t *testing.T) {
	trades := []Trade{
		mkTrade(0.03, 0.02, true, true, true, 20, 6, 10),
		mkTrade(0.035, 0.025, true, true, false, 20, 4, -20),
		mkTrade(0.038, 0.022, true, true, true, 20, 5, 12),
	}
	rows := BucketByEdge(trades, DefaultEdgeBuckets())
	var two4 *BucketSummary
	for i := range rows {
		if rows[i].Bucket.Label == "2-4pp" {
			two4 = &rows[i]
		}
	}
	if two4 == nil {
		t.Fatal("no 2-4pp bucket in output")
	}
	if two4.TradesSimulated != 3 {
		t.Fatalf("2-4pp simulated=%d want 3", two4.TradesSimulated)
	}
	if two4.TradesResolved != 3 || two4.TradesWon != 2 {
		t.Fatalf("2-4pp resolved=%d won=%d want 3/2", two4.TradesResolved, two4.TradesWon)
	}
	if two4.HitRate < 0.66 || two4.HitRate > 0.67 {
		t.Fatalf("2-4pp hit_rate=%v want ~0.667", two4.HitRate)
	}
	// Total MTM = 6 + 4 + 5 = 15 on size 60 → 25%
	if two4.TotalMTMPnLUSDC != 15 {
		t.Fatalf("MTM total=%v want 15", two4.TotalMTMPnLUSDC)
	}
}

func TestBucketByEdge_DropsTradesBelowZero(t *testing.T) {
	// Negative claimed edge shouldn't appear in any default bucket (lo=0 floor).
	trades := []Trade{
		mkTrade(-0.02, -0.03, true, false, false, 20, -8, 0),
		mkTrade(0.05, 0.04, true, false, false, 20, 8, 0),
	}
	rows := BucketByEdge(trades, DefaultEdgeBuckets())
	total := 0
	for _, r := range rows {
		total += r.TradesSimulated
	}
	if total != 1 {
		t.Fatalf("total trades bucketed=%d want 1 (negative-edge dropped)", total)
	}
}

func TestMatchBucket_OpenRightTail(t *testing.T) {
	buckets := DefaultEdgeBuckets()
	if matchBucket(15, buckets) < 0 {
		t.Fatal("15pp should match 10+ bucket")
	}
	if matchBucket(100, buckets) < 0 {
		t.Fatal("100pp should still match 10+ bucket (HiPP=0 = open right)")
	}
}

func TestFormatBuckets_RendersHeaderAndRows(t *testing.T) {
	trades := []Trade{mkTrade(0.05, 0.04, true, true, true, 20, 8, 10)}
	rows := BucketByEdge(trades, DefaultEdgeBuckets())
	out := FormatBuckets(rows)
	if len(out) < 100 {
		t.Fatalf("output too short: %q", out)
	}
}
