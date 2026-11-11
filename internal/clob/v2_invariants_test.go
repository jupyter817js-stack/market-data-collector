package clob

import (
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/executor"
)

// T1: ORDER_TYPE_STRING length invariants for V2.
func TestV2_OrderTypeStringLen(t *testing.T) {
	if OrderTypeStringLen != 186 {
		t.Fatalf("ORDER_TYPE_STRING length = %d, want 186 (v2 invariant)", OrderTypeStringLen)
	}
	hexEnc := hex.EncodeToString([]byte(OrderTypeString))
	if len(hexEnc) != 372 {
		t.Fatalf("hex(ORDER_TYPE_STRING) length = %d, want 372", len(hexEnc))
	}
	// "%04x" of 186 must be "00ba"
	wantLenHex := "00ba"
	gotLenHex := fmtLenHex(OrderTypeStringLen)
	if gotLenHex != wantLenHex {
		t.Fatalf("len_hex = %q, want %q", gotLenHex, wantLenHex)
	}
}

func fmtLenHex(n int) string {
	// helper: render n as fixed-width 4-char lowercase hex
	const hexChars = "0123456789abcdef"
	return string([]byte{
		hexChars[(n>>12)&0xf],
		hexChars[(n>>8)&0xf],
		hexChars[(n>>4)&0xf],
		hexChars[n&0xf],
	})
}

// T2: V2 appDomainSeparator for the standard CTF Exchange must match the
// expected constant. Mismatch => one of name/version/chainId/contract is
// wrong.
func TestV2_AppDomainSeparator(t *testing.T) {
	got := DefaultDomain().Separator()
	if got != V2AppDomainSeparator {
		t.Fatalf("V2 appDomainSeparator mismatch:\n  got  %s\n  want %s",
			got.Hex(), V2AppDomainSeparator.Hex())
	}
}

// T3: HMAC reference vector — verifies URL-safe base64 + prehash format.
//
//	secret (b64url) = "hm4EDy1M769ujXjFdp8k-M1H0aJZa2UNHQGYSt-eY20="
//	ts              = 1700000000
//	method          = "GET"
//	path            = "/test"
//	body            = ""
//	expected sig    = "rCBAGWcGRk3AZ6oDdahhOkDcV_FpaOTisKMCMTonDTs="
func TestV2_HMACReferenceVector(t *testing.T) {
	creds := L2Credentials{
		APIKey:        "test-api-key",
		APISecret:     "hm4EDy1M769ujXjFdp8k-M1H0aJZa2UNHQGYSt-eY20=",
		APIPassphrase: "test-passphrase",
	}
	ts := time.Unix(1700000000, 0).UTC()
	_, sig, tsStr, _, err := L2Headers(creds, ts, "GET", "/test", "")
	if err != nil {
		t.Fatalf("L2Headers err: %v", err)
	}
	if tsStr != "1700000000" {
		t.Fatalf("ts string = %q, want 1700000000", tsStr)
	}
	want := "rCBAGWcGRk3AZ6oDdahhOkDcV_FpaOTisKMCMTonDTs="
	if sig != want {
		t.Fatalf("HMAC reference vector mismatch:\n  got  %s\n  want %s", sig, want)
	}
}

// HMAC must refuse paths with query strings (v2 bare-path rule).
func TestV2_HMACRejectsQueryInPath(t *testing.T) {
	creds := L2Credentials{
		APIKey:        "k",
		APISecret:     "hm4EDy1M769ujXjFdp8k-M1H0aJZa2UNHQGYSt-eY20=",
		APIPassphrase: "p",
	}
	_, _, _, _, err := L2Headers(creds, time.Unix(1700000000, 0).UTC(),
		"GET", "/balance-allowance?asset_type=COLLATERAL", "")
	if err == nil {
		t.Fatal("expected error rejecting query in path, got nil")
	}
	if !strings.Contains(err.Error(), "bare") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// T5: 1271 trailer structure invariants — verify wrapped sig layout via
// a synthetic OrderSpec. Confirms total length and trailer field layout
// without needing a real PM round-trip.
func TestV2_SignWrappedTrailerStructure(t *testing.T) {
	// Use the deterministic test key (well-known, not a real wallet).
	const testPK = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
	// derive the EOA address: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
	const testEOA = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

	spec := executor.OrderSpec{
		Maker:         "0x5617c83a5b41cb03Fd3fe2BD70C36cC50B60a1f2", // sample proxy
		Signer:        testEOA,
		TokenID:       "42264481236134722863278073323768265860677832848598472536689542143648934697887",
		Side:          executor.SideBuy,
		MakerAmount:   big.NewInt(5_000_000),  // 5 USDC * 10^6
		TakerAmount:   big.NewInt(11_111_111), // ~11.11 shares
		Salt:          big.NewInt(123456),
		SignatureType: executor.SignatureTypePoly1271,
		Timestamp:     1735689600000,
		Metadata:      "0x" + strings.Repeat("00", 32),
		Builder:       "0x3a0e4b7ea305b892f7a60ac85860a72e2074831ba34e4542a148f07bde467c44",
	}

	signed, err := SignWrapped(spec, DefaultDomain(), testPK)
	if err != nil {
		t.Fatalf("SignWrapped err: %v", err)
	}

	// Total length: 2 + 130 + 64 + 64 + 372 + 4 = 636
	if len(signed.Signature) != 636 {
		t.Fatalf("wire sig length = %d, want 636", len(signed.Signature))
	}
	if !strings.HasPrefix(signed.Signature, "0x") {
		t.Fatalf("wire sig missing 0x prefix")
	}

	sigBody := signed.Signature[2:] // 634 chars

	// Last 4 chars must be "00ba"
	if sigBody[len(sigBody)-4:] != "00ba" {
		t.Fatalf("trailer lenHex = %q, want 00ba", sigBody[len(sigBody)-4:])
	}

	// The 372 chars just before "00ba" must equal hex(ORDER_TYPE_STRING)
	wantTypeHex := hex.EncodeToString([]byte(OrderTypeString))
	gotTypeHex := sigBody[len(sigBody)-376 : len(sigBody)-4]
	if gotTypeHex != wantTypeHex {
		t.Fatalf("trailer typeStringHex mismatch:\n  got  %s\n  want %s", gotTypeHex, wantTypeHex)
	}

	// First 130 chars must decode to a valid 65-byte secp256k1 sig with v in {27,28}.
	innerSigHex := sigBody[:130]
	innerSig, err := hex.DecodeString(innerSigHex)
	if err != nil {
		t.Fatalf("inner sig hex decode err: %v", err)
	}
	if len(innerSig) != 65 {
		t.Fatalf("inner sig length = %d, want 65", len(innerSig))
	}
	if innerSig[64] != 27 && innerSig[64] != 28 {
		t.Fatalf("inner sig v = %d, want 27 or 28", innerSig[64])
	}
}

// SignOrder dispatch: signatureType=0 -> Sign, signatureType=3 -> SignWrapped.
func TestV2_SignOrderDispatch(t *testing.T) {
	const testPK = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
	const testEOA = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

	specEOA := executor.OrderSpec{
		Maker:         testEOA,
		Signer:        testEOA,
		TokenID:       "1",
		Side:          executor.SideBuy,
		MakerAmount:   big.NewInt(1_000_000),
		TakerAmount:   big.NewInt(2_000_000),
		Salt:          big.NewInt(1),
		SignatureType: executor.SignatureTypeEOA,
		Timestamp:     time.Now().UnixMilli(),
	}
	sigEOA, err := SignOrder(specEOA, DefaultDomain(), testPK)
	if err != nil {
		t.Fatalf("EOA SignOrder err: %v", err)
	}
	if len(sigEOA.Signature) != 132 { // "0x" + 130 (65 bytes hex)
		t.Fatalf("EOA wire sig length = %d, want 132", len(sigEOA.Signature))
	}

	spec1271 := specEOA
	spec1271.Maker = "0x5617c83a5b41cb03Fd3fe2BD70C36cC50B60a1f2"
	spec1271.SignatureType = executor.SignatureTypePoly1271
	sig1271, err := SignOrder(spec1271, DefaultDomain(), testPK)
	if err != nil {
		t.Fatalf("1271 SignOrder err: %v", err)
	}
	if len(sig1271.Signature) != 636 {
		t.Fatalf("1271 wire sig length = %d, want 636", len(sig1271.Signature))
	}
}
