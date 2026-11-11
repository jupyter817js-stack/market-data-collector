// Package executor builds Polymarket CLOB order specs from signal-engine
// outputs. This package is intentionally signing-free: it produces the
// JSON-shaped order body that WOULD be POSTed to /order, but does no
// wallet IO, no EIP-712 hashing, and no HTTP submission. Those layers
// live in a future commit per docs/step5_executor_plan.md.
//
// Two reasons to keep the planner separate:
//  1. The arithmetic (price → maker/taker amounts in 6-decimal fixed
//     math) is the same regardless of who signs, so it's worth testing
//     in isolation.
//  2. Operators get a `proposed_orders` audit trail to validate before
//     any real money signs.
package executor

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"time"

	sigpkg "github.com/your-org/pol-odds-collector/internal/signal"
)

// USDCDecimals and CTFDecimals are both 6 on Polygon — both USDC and CTF
// outcome tokens use 6-decimal fixed-point. amount * 10^6 = on-chain int.
const (
	USDCDecimals = 6
	CTFDecimals  = 6
	oneUnit      = 1_000_000 // 10^6
)

// SignatureType enum matches Polymarket's CLOB spec.
type SignatureType int

const (
	SignatureTypeEOA        SignatureType = 0
	SignatureTypePolyProxy  SignatureType = 1
	SignatureTypeGnosisSafe SignatureType = 2
	SignatureTypePoly1271   SignatureType = 3
)

// OrderType enum.
type OrderType string

const (
	OrderTypeGTC OrderType = "GTC"
	OrderTypeFOK OrderType = "FOK"
	OrderTypeGTD OrderType = "GTD"
	OrderTypeFAK OrderType = "FAK"
)

// Side enum.
type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

// OrderSpec is the V2 planner output. Mirrors the on-wire EIP-712 Order
// struct (11 fields) plus REST-only audit fields.
//
// V2 dropped: taker, expiration, nonce, feeRateBps.
// V2 added:   timestamp (ms), metadata (bytes32), builder (bytes32).
type OrderSpec struct {
	Maker         string // funder/proxy address (B mode) or EOA (A mode)
	Signer        string // EOA address (always); equals Maker only for signatureType=0
	TokenID       string // CTF outcome token id (decimal string)
	Side          Side
	OrderType     OrderType
	MakerAmount   *big.Int      // 6-decimal fixed
	TakerAmount   *big.Int      // 6-decimal fixed
	Price         float64       // effective price (probability units; for audit only)
	SizeUSDC      float64       // dollar size (for audit; equal to USDC leg / 10^6)
	Salt          *big.Int      // JS-Number-safe (≤ 2^53). REST body sends as JSON NUMBER.
	SignatureType SignatureType // 0 EOA | 1 PolyProxy | 2 GnosisSafe | 3 POLY_1271 (Deposit Wallets)
	Timestamp     int64         // milliseconds (V2 field)
	Metadata      string        // bytes32 hex ("0x" + 64 chars); typically "0x" + 64 zeros
	Builder       string        // bytes32 hex; PM Builder ID ("0x" + 64 chars) or empty for none

	// Expiration is the REST body's "expiration" field (string seconds).
	// Kept for the REST layer; not part of the V2 EIP-712 hashed payload.
	Expiration int64

	// audit fields — not part of the on-wire order body
	SourceSignalID   int64
	MatchupID        int64
	ConditionID      string
	OutcomeSide      string
	OutcomeName      string
	HomeName         string
	AwayName         string
	LeagueName       string
	ClaimedNetEdge   float64
	PinnacleFairProb float64
	PinnacleAgeSec   float64
	PMAgeSec         float64
}

// BuildConfig tunes the planner. All fields are optional except the
// addresses; zero values pick safe defaults documented per-field.
type BuildConfig struct {
	// MakerAddress is the funder address that appears in the EIP-712
	// `maker` field. For wallet mode A (EOA-only) this equals
	// SignerAddress; for wallet mode B (POLY_1271 deposit wallets) this
	// is the proxy/funder address that holds USDC. Required.
	MakerAddress string

	// SignerAddress is the EOA that will sign the order with its
	// private key. Appears in the EIP-712 `signer` field. For mode A
	// the caller can leave this empty and Build will copy MakerAddress.
	SignerAddress string

	// SignatureType controls the EIP-712 / EIP-1271 path. 0=EOA,
	// 1=POLY_PROXY (legacy Magic Link), 2=POLY_GNOSIS_SAFE (legacy
	// browser wallet proxy), 3=POLY_1271 (current Polymarket Deposit
	// Wallets). If unspecified, defaults to 0 (EOA).
	SignatureType SignatureType

	// BuilderCode is the PM Builder ID (bytes32 hex). When set, the
	// order is attributed to your Builder, applying the Builder's
	// configured maker/taker fee rates. Empty -> 0x00..00 (no builder).
	BuilderCode string

	// Metadata is the optional bytes32 hex metadata field. Empty -> zero.
	Metadata string

	// OrderType defaults to FAK (fill-and-kill, no rest). Our fade strategy
	// takes whatever liquidity is available at the moment of signal; we
	// don't want to leave resting orders that fill at a worse mid later.
	OrderType OrderType

	// ExpirationFromNow is for the REST body's expiration field (string
	// seconds). 0 means GTC. V2 EIP-712 payload no longer hashes
	// expiration — kept here for REST forward-compat.
	ExpirationFromNow time.Duration

	// SizeOverrideUSDC, if positive, replaces the signal's proposed size
	// (handy for sensitivity testing in the planner before wiring funds).
	SizeOverrideUSDC float64
}

// Build converts a BUY-CANDIDATE signal into a planned V2 OrderSpec.
// Returns an error if required inputs are missing or arithmetic
// underflows. Currently BUY-only (the live strategy never starts
// SELLing without a prior BUY position); SELL builds are a future when
// exit logic lands.
//
// V2 wallet modes:
//   - SignatureType=0 (EOA): SignerAddress optional; defaults to MakerAddress
//   - SignatureType=3 (POLY_1271): MakerAddress=funder/proxy; SignerAddress=EOA (required)
func Build(s sigpkg.Signal, cfg BuildConfig) (OrderSpec, error) {
	if cfg.MakerAddress == "" {
		return OrderSpec{}, errors.New("executor: MakerAddress is required")
	}
	if s.State != sigpkg.StateBuyCandidate {
		return OrderSpec{}, fmt.Errorf("executor: signal state %s is not BUY_CANDIDATE", s.State)
	}
	if s.PolymarketTokenID == "" {
		return OrderSpec{}, errors.New("executor: signal has no PolymarketTokenID")
	}
	if !s.PMAsk.Valid || s.PMAsk.Float64 <= 0 || s.PMAsk.Float64 >= 1 {
		return OrderSpec{}, fmt.Errorf("executor: invalid PMAsk %v", s.PMAsk)
	}

	// Resolve signer: caller supplies it for non-EOA modes; for EOA we
	// copy MakerAddress.
	signerAddr := cfg.SignerAddress
	if signerAddr == "" {
		if cfg.SignatureType != SignatureTypeEOA {
			return OrderSpec{}, fmt.Errorf("executor: SignerAddress required when SignatureType=%d", cfg.SignatureType)
		}
		signerAddr = cfg.MakerAddress
	}

	size := s.ProposedSizeUSDC
	if cfg.SizeOverrideUSDC > 0 {
		size = cfg.SizeOverrideUSDC
	}
	if size <= 0 {
		return OrderSpec{}, fmt.Errorf("executor: non-positive size %v", size)
	}

	maker, taker := computeBuyAmounts(size, s.PMAsk.Float64)
	if maker.Sign() <= 0 || taker.Sign() <= 0 {
		return OrderSpec{}, fmt.Errorf("executor: amounts underflow (maker=%s taker=%s)", maker, taker)
	}

	salt, err := randomSalt()
	if err != nil {
		return OrderSpec{}, fmt.Errorf("executor: salt: %w", err)
	}

	orderType := cfg.OrderType
	if orderType == "" {
		orderType = OrderTypeFAK
	}

	var expiration int64
	if cfg.ExpirationFromNow > 0 {
		expiration = time.Now().UTC().Add(cfg.ExpirationFromNow).Unix()
	}

	// V2 EIP-712 timestamp is milliseconds; matches REST body's timestamp string.
	timestampMS := time.Now().UTC().UnixMilli()

	return OrderSpec{
		Maker:         cfg.MakerAddress,
		Signer:        signerAddr,
		TokenID:       s.PolymarketTokenID,
		Side:          SideBuy,
		OrderType:     orderType,
		MakerAmount:   maker,
		TakerAmount:   taker,
		Price:         s.PMAsk.Float64,
		SizeUSDC:      size,
		Expiration:    expiration,
		Salt:          salt,
		SignatureType: cfg.SignatureType,
		Timestamp:     timestampMS,
		Metadata:      cfg.Metadata,
		Builder:       cfg.BuilderCode,

		SourceSignalID:   s.ID,
		MatchupID:        s.MatchupID,
		ConditionID:      s.PolymarketConditionID,
		OutcomeSide:      s.OutcomeSide,
		OutcomeName:      s.OutcomeName,
		HomeName:         s.HomeName,
		AwayName:         s.AwayName,
		LeagueName:       s.LeagueName,
		ClaimedNetEdge:   s.NetEdgeBuy,
		PinnacleFairProb: s.PinnacleFairProb,
		PinnacleAgeSec:   s.PinnacleAgeSec,
		PMAgeSec:         s.PMAgeSec,
	}, nil
}

// computeBuyAmounts converts (size_usdc, price) to (makerAmount,
// takerAmount) in 6-decimal fixed math. For a BUY:
//
//	makerAmount = size_usdc * 10^6                (USDC we give, in micro-USDC)
//	takerAmount = makerAmount * 10^6 / price_e6   (shares we receive, in 6-dec)
//
// where price_e6 = round(price * 10^6). All math after the initial
// float→int rounding is exact integer division so we can't accumulate
// float drift (which previously gave 1_999_999_999 instead of
// 2_000_000_000 for price=0.01).
func computeBuyAmounts(sizeUSDC, price float64) (maker, taker *big.Int) {
	makerMicro := roundToInt64(sizeUSDC * float64(oneUnit))
	priceE6 := roundToInt64(price * float64(oneUnit))
	maker = big.NewInt(makerMicro)
	if priceE6 == 0 {
		return maker, big.NewInt(0)
	}
	// taker = maker * oneUnit / priceE6
	taker = new(big.Int).Mul(maker, big.NewInt(oneUnit))
	taker.Quo(taker, big.NewInt(priceE6))
	return maker, taker
}

func roundToInt64(f float64) int64 {
	if f >= 0 {
		return int64(f + 0.5)
	}
	return int64(f - 0.5)
}

// randomSalt returns a cryptographically-random uint256 as big.Int. Not
// derived from time so two simultaneous planners don't collide.
// randomSalt produces a JS-Number-safe (<= 2^53) random uint suitable for
// V2's REST body, which sends salt as a JSON NUMBER and runs through
// parseInt(salt, 10) server-side. RN1 reference uses
// round(rand_u64/u64::MAX * now_ms) ≈ 10^12, well under 2^53 ≈ 9×10^15.
//
// We use crypto/rand for unpredictability + multiply by now_ms so the
// salt is monotonic-ish (helps avoid replay edge cases without violating
// the JS Number ceiling).
func randomSalt() (*big.Int, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	// Use 6 bytes of randomness (48 bits = ~2.8e14) — comfortably below
	// 2^53 (~9e15) and provides enough uniqueness for our throughput.
	r := uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40
	// Mix with current millis so values trend upward (purely for replay
	// hygiene; not relied upon for correctness).
	nowMs := uint64(time.Now().UTC().UnixMilli())
	salt := (r ^ nowMs) & ((1 << 53) - 1)
	if salt == 0 {
		salt = 1 // exceedingly unlikely; just keeps the value positive
	}
	return new(big.Int).SetUint64(salt), nil
}
