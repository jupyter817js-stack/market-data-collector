package clob

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func commonHexAddr(s string) common.Address { return common.HexToAddress(s) }

func TestAuthDomainSeparatorStable(t *testing.T) {
	a := DefaultAuthDomain().Separator()
	b := DefaultAuthDomain().Separator()
	if a != b {
		t.Fatalf("auth domain separator non-deterministic: %s vs %s", a.Hex(), b.Hex())
	}
	if a == DefaultDomain().Separator() {
		t.Fatalf("auth domain must differ from order domain (no verifyingContract)")
	}
}

func TestSignAuthRoundTrip(t *testing.T) {
	req, err := SignAuth(testPrivKey, DefaultAuthDomain())
	if err != nil {
		t.Fatalf("SignAuth: %v", err)
	}
	if req.Address.Hex() != "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf" {
		t.Fatalf("derived addr=%s want testSigner", req.Address.Hex())
	}
	if !strings.HasPrefix(req.Signature, "0x") || len(req.Signature) != 132 {
		t.Fatalf("bad signature shape: %q", req.Signature)
	}
	if req.Nonce.BitLen() == 0 {
		t.Fatalf("expected non-zero random nonce")
	}
	if err := VerifyAuthRoundTrip(req, DefaultAuthDomain()); err != nil {
		t.Fatalf("VerifyAuthRoundTrip: %v", err)
	}
}

func TestVerifyAuthDetectsTampering(t *testing.T) {
	req, err := SignAuth(testPrivKey, DefaultAuthDomain())
	if err != nil {
		t.Fatalf("SignAuth: %v", err)
	}
	// Tampering with the nonce should make recovery fail.
	req.Nonce = new(big.Int).Add(req.Nonce, big.NewInt(1))
	if err := VerifyAuthRoundTrip(req, DefaultAuthDomain()); err == nil {
		t.Fatalf("expected mismatch on tampered nonce")
	}
}

func TestAuthHeadersShape(t *testing.T) {
	req := AuthRequest{
		Address:   commonHexAddr("0x000000000000000000000000000000000000dEaD"),
		Timestamp: time.Unix(1_700_000_000, 0).UTC(),
		Nonce:     big.NewInt(42),
		Signature: "0xff",
	}
	h := req.Headers()
	for _, key := range []string{"POLY_ADDRESS", "POLY_SIGNATURE", "POLY_TIMESTAMP", "POLY_NONCE"} {
		if _, ok := h[key]; !ok {
			t.Fatalf("missing header %s", key)
		}
	}
	if h["POLY_TIMESTAMP"] != "1700000000" {
		t.Fatalf("unexpected POLY_TIMESTAMP=%q", h["POLY_TIMESTAMP"])
	}
	if h["POLY_NONCE"] != "42" {
		t.Fatalf("unexpected POLY_NONCE=%q", h["POLY_NONCE"])
	}
}

func TestSignAuthTimestampWithinSeconds(t *testing.T) {
	before := time.Now().UTC()
	req, err := SignAuth(testPrivKey, DefaultAuthDomain())
	if err != nil {
		t.Fatalf("SignAuth: %v", err)
	}
	after := time.Now().UTC()
	if req.Timestamp.Before(before.Add(-time.Second)) || req.Timestamp.After(after.Add(time.Second)) {
		t.Fatalf("timestamp out of band: req=%s before=%s after=%s",
			req.Timestamp, before, after)
	}
}
