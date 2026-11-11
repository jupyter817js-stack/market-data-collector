package main

import (
	"testing"

	"github.com/your-org/pol-odds-collector/internal/clob"
)

func TestParseFillFromInfo(t *testing.T) {
	cases := []struct {
		name       string
		info       clob.OrderInfo
		wantShares float64
		wantCost   float64
		wantOK     bool
	}{
		{
			name:       "full fill",
			info:       clob.OrderInfo{SizeMatched: "32.787", AveragePrice: "0.61"},
			wantShares: 32.787,
			wantCost:   32.787 * 0.61,
			wantOK:     true,
		},
		{
			name:       "partial fill — half size",
			info:       clob.OrderInfo{SizeMatched: "16.5", AveragePrice: "0.55"},
			wantShares: 16.5,
			wantCost:   16.5 * 0.55,
			wantOK:     true,
		},
		{
			name:       "no match (FAK with no liquidity) size_matched = 0",
			info:       clob.OrderInfo{SizeMatched: "0", AveragePrice: "0.50"},
			wantShares: 0, wantCost: 0, wantOK: false,
		},
		{
			name:       "missing average price",
			info:       clob.OrderInfo{SizeMatched: "10", AveragePrice: ""},
			wantShares: 0, wantCost: 0, wantOK: false,
		},
		{
			name:       "non-numeric size",
			info:       clob.OrderInfo{SizeMatched: "abc", AveragePrice: "0.5"},
			wantShares: 0, wantCost: 0, wantOK: false,
		},
		{
			name:       "whitespace tolerated",
			info:       clob.OrderInfo{SizeMatched: "  40  ", AveragePrice: " 0.25 "},
			wantShares: 40, wantCost: 10, wantOK: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shares, cost, ok := parseFillFromInfo(tc.info)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if shares != tc.wantShares {
				t.Fatalf("shares=%v want %v", shares, tc.wantShares)
			}
			diff := cost - tc.wantCost
			if diff < -1e-6 || diff > 1e-6 {
				t.Fatalf("cost=%v want %v", cost, tc.wantCost)
			}
		})
	}
}
