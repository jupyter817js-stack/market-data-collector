package clob

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/your-org/pol-odds-collector/internal/executor"
)

// Deterministic private key for tests. ALL ZEROS plus 1 — never use on
// mainnet for anything other than test signatures. Derives address
// 0x7e5f4552091a69125d5dfcb7b8c2659029395bdf.
const testPrivKey = "0000000000000000000000000000000000000000000000000000000000000001"
const testSigner = "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"

// sampleOrderSpec returns a minimal valid OrderSpec for signing.
func sampleOrderSpec() executor.OrderSpec {
	return executor.OrderSpec{
		Maker:         testSigner,
		Signer:        testSigner,
		TokenID:       "12345678901234567890",
		Side:          executor.SideBuy,
		OrderType:     executor.OrderTypeFAK,
		MakerAmount:   big.NewInt(20_000_000), // $20 USDC (6-dec)
		TakerAmount:   big.NewInt(40_000_000), // $40 worth of token (price=0.5)
		Price:         0.5,
		SizeUSDC:      20.0,
		Expiration:    0,
		Salt:          big.NewInt(99999),
		SignatureType: executor.SignatureTypeEOA,
	}
}

func TestDomainSeparatorDeterministic(t *testing.T) {
	d1 := DefaultDomain().Separator()
	d2 := DefaultDomain().Separator()
	if d1 != d2 {
		t.Fatalf("domain separator non-deterministic: %s vs %s", d1.Hex(), d2.Hex())
	}
	// Different verifying contract → different separator.
	alt := DefaultDomain()
	alt.VerifyingContract = NegRiskCTFExchangeAddress
	if alt.Separator() == d1 {
		t.Fatalf("neg-risk and standard exchange must produce different domain separators")
	}
}

func TestHashOrderStable(t *testing.T) {
	spec := sampleOrderSpec()
	domain := DefaultDomain()
	h1, err := HashOrder(spec, domain)
	if err != nil {
		t.Fatalf("HashOrder: %v", err)
	}
	h2, err := HashOrder(spec, domain)
	if err != nil {
		t.Fatalf("HashOrder repeat: %v", err)
	}
	if h1 != h2 {
		t.Fatalf("HashOrder not deterministic: %s vs %s", h1.Hex(), h2.Hex())
	}
	// Changing salt must change the hash.
	spec.Salt = big.NewInt(100000)
	h3, err := HashOrder(spec, domain)
	if err != nil {
		t.Fatalf("HashOrder mutated: %v", err)
	}
	if h3 == h1 {
		t.Fatalf("salt change did not change order hash")
	}
}

func TestSignAndRecoverRoundTrip(t *testing.T) {
	spec := sampleOrderSpec()
	domain := DefaultDomain()
	so, err := Sign(spec, domain, testPrivKey)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if !strings.HasPrefix(so.Signature, "0x") {
		t.Fatalf("signature missing 0x prefix")
	}
	if len(so.Signature) != 2+130 {
		t.Fatalf("signature wrong length %d (want 132)", len(so.Signature))
	}
	if so.SignerAddr.Hex() != common.HexToAddress(testSigner).Hex() {
		t.Fatalf("SignerAddr=%s want %s", so.SignerAddr.Hex(), testSigner)
	}
	// Recover should round-trip back to the same signer.
	if err := VerifyTestRoundTrip(so.OrderHash, so.Signature, so.SignerAddr); err != nil {
		t.Fatalf("VerifyTestRoundTrip: %v", err)
	}
}

func TestSignRejectsKeyAddressMismatch(t *testing.T) {
	spec := sampleOrderSpec()
	spec.Signer = "0x000000000000000000000000000000000000dEaD" // not the test key's address
	_, err := Sign(spec, DefaultDomain(), testPrivKey)
	if err == nil {
		t.Fatalf("expected mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "derives") {
		t.Fatalf("expected derives-mismatch message, got %v", err)
	}
}

func TestSignRejectsMissingFields(t *testing.T) {
	spec := sampleOrderSpec()
	spec.MakerAmount = nil
	if _, err := Sign(spec, DefaultDomain(), testPrivKey); err == nil {
		t.Fatalf("expected error on nil MakerAmount")
	}
}

func TestVerifyDetectsTampering(t *testing.T) {
	spec := sampleOrderSpec()
	so, err := Sign(spec, DefaultDomain(), testPrivKey)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	// Recompute the digest after tampering with maker amount; signer must
	// now mismatch.
	spec.MakerAmount = big.NewInt(99_999_999)
	tampered, _ := HashOrder(spec, DefaultDomain())
	err = VerifyTestRoundTrip(tampered, so.Signature, so.SignerAddr)
	if err == nil {
		t.Fatalf("expected mismatch on tampered digest")
	}
}

func TestOrderTypeHashMatchesSpec(t *testing.T) {
	// V2 CTF Exchange ORDER_TYPEHASH. Recompute from the canonical
	// 186-byte typestring and compare to the package var so a refactor
	// that drops or reorders a field is caught.
	want := crypto.Keccak256Hash([]byte(
		"Order(uint256 salt,address maker,address signer,uint256 tokenId," +
			"uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType," +
			"uint256 timestamp,bytes32 metadata,bytes32 builder)",
	))
	if orderTypeHash != want {
		t.Fatalf("orderTypeHash drift (v2):\n got  %s\n want %s", orderTypeHash.Hex(), want.Hex())
	}
}
