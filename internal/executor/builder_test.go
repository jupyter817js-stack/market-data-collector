package executor

import (
	"database/sql"
	"math/big"
	"testing"

	sigpkg "github.com/your-org/pol-odds-collector/internal/signal"
)

func goodSignal(price float64, size float64) sigpkg.Signal {
	s := sigpkg.Signal{
		State:            sigpkg.StateBuyCandidate,
		ProposedSizeUSDC: size,
		NetEdgeBuy:       0.05,
	}
	s.ID = 42
	s.MatchupID = 100
	s.PolymarketConditionID = "0xcond"
	s.PolymarketTokenID = "tokA"
	s.OutcomeSide = "home"
	s.OutcomeName = "MOUZ"
	s.HomeName = "MOUZ"
	s.AwayName = "magic"
	s.LeagueName = "CS2 - PGL"
	s.PinnacleFairProb = 0.74
	s.PMAsk = sql.NullFloat64{Float64: price, Valid: true}
	return s
}

func TestBuild_PriceEncodingAtThirtyCents(t *testing.T) {
	s := goodSignal(0.30, 20.0)
	got, err := Build(s, BuildConfig{MakerAddress: "0xabc"})
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	// makerAmount = 20 * 10^6 = 20_000_000
	if got.MakerAmount.Cmp(big.NewInt(20_000_000)) != 0 {
		t.Fatalf("maker=%s want 20000000", got.MakerAmount)
	}
	// takerAmount = 20_000_000 / 0.30 = 66_666_666 (truncated)
	if got.TakerAmount.Cmp(big.NewInt(66_666_666)) != 0 {
		t.Fatalf("taker=%s want 66666666", got.TakerAmount)
	}
	if got.Side != SideBuy {
		t.Fatalf("side=%s want BUY", got.Side)
	}
	if got.OrderType != OrderTypeFAK {
		t.Fatalf("orderType=%s want FAK (default)", got.OrderType)
	}
	if got.SignatureType != SignatureTypeEOA {
		t.Fatalf("sigType=%d want 0", got.SignatureType)
	}
	if got.Maker != "0xabc" || got.Signer != "0xabc" {
		t.Fatalf("maker/signer=%s/%s want 0xabc both", got.Maker, got.Signer)
	}
}

func TestBuild_PriceEncodingAtOneCent(t *testing.T) {
	s := goodSignal(0.01, 20.0)
	got, err := Build(s, BuildConfig{MakerAddress: "0xabc"})
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	if got.MakerAmount.Cmp(big.NewInt(20_000_000)) != 0 {
		t.Fatalf("maker=%s", got.MakerAmount)
	}
	// 20M / 0.01 = 2_000_000_000 shares
	if got.TakerAmount.Cmp(big.NewInt(2_000_000_000)) != 0 {
		t.Fatalf("taker=%s want 2000000000", got.TakerAmount)
	}
}

func TestBuild_PriceEncodingAtNinetyNineCents(t *testing.T) {
	s := goodSignal(0.99, 20.0)
	got, err := Build(s, BuildConfig{MakerAddress: "0xabc"})
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	// 20M / 0.99 = 20_202_020 (truncated)
	if got.TakerAmount.Cmp(big.NewInt(20_202_020)) != 0 {
		t.Fatalf("taker=%s want 20202020", got.TakerAmount)
	}
}

func TestBuild_SubDollarSizeRoundsCorrectly(t *testing.T) {
	s := goodSignal(0.50, 0.50)
	got, err := Build(s, BuildConfig{MakerAddress: "0xabc"})
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	if got.MakerAmount.Cmp(big.NewInt(500_000)) != 0 {
		t.Fatalf("maker=%s want 500000", got.MakerAmount)
	}
	if got.TakerAmount.Cmp(big.NewInt(1_000_000)) != 0 {
		t.Fatalf("taker=%s want 1000000", got.TakerAmount)
	}
}

func TestBuild_SaltIsRandomAndNonZero(t *testing.T) {
	s := goodSignal(0.30, 20.0)
	a, _ := Build(s, BuildConfig{MakerAddress: "0xabc"})
	b, _ := Build(s, BuildConfig{MakerAddress: "0xabc"})
	if a.Salt.Sign() <= 0 || b.Salt.Sign() <= 0 {
		t.Fatalf("salts non-positive: a=%s b=%s", a.Salt, b.Salt)
	}
	if a.Salt.Cmp(b.Salt) == 0 {
		t.Fatalf("two builds returned identical salt %s (random source broken)", a.Salt)
	}
}

func TestBuild_RejectsMissingMaker(t *testing.T) {
	s := goodSignal(0.30, 20.0)
	if _, err := Build(s, BuildConfig{}); err == nil {
		t.Fatalf("expected error for missing maker")
	}
}

func TestBuild_RejectsNonBuyCandidate(t *testing.T) {
	s := goodSignal(0.30, 20.0)
	s.State = sigpkg.StateEdgeTooSmall
	if _, err := Build(s, BuildConfig{MakerAddress: "0xabc"}); err == nil {
		t.Fatalf("expected error for non-BUY state")
	}
}

func TestBuild_RejectsMissingTokenID(t *testing.T) {
	s := goodSignal(0.30, 20.0)
	s.PolymarketTokenID = ""
	if _, err := Build(s, BuildConfig{MakerAddress: "0xabc"}); err == nil {
		t.Fatalf("expected error for missing token id")
	}
}

func TestBuild_RejectsBadAskBoundaries(t *testing.T) {
	for _, price := range []float64{0, -0.1, 1.0, 1.5} {
		s := goodSignal(price, 20.0)
		s.PMAsk = sql.NullFloat64{Float64: price, Valid: true}
		if _, err := Build(s, BuildConfig{MakerAddress: "0xabc"}); err == nil {
			t.Fatalf("expected error for price %v", price)
		}
	}
}

func TestBuild_SizeOverrideApplied(t *testing.T) {
	s := goodSignal(0.30, 20.0)
	got, _ := Build(s, BuildConfig{MakerAddress: "0xabc", SizeOverrideUSDC: 5.0})
	if got.SizeUSDC != 5.0 {
		t.Fatalf("sizeUSDC=%v want 5", got.SizeUSDC)
	}
	if got.MakerAmount.Cmp(big.NewInt(5_000_000)) != 0 {
		t.Fatalf("maker=%s want 5000000", got.MakerAmount)
	}
}

func TestBuild_AuditFieldsCopied(t *testing.T) {
	s := goodSignal(0.30, 20.0)
	got, _ := Build(s, BuildConfig{MakerAddress: "0xabc"})
	if got.SourceSignalID != 42 || got.MatchupID != 100 || got.ConditionID != "0xcond" {
		t.Fatalf("audit fields not copied: %+v", got)
	}
	if got.HomeName != "MOUZ" || got.AwayName != "magic" || got.OutcomeSide != "home" {
		t.Fatalf("name fields not copied: %+v", got)
	}
}
