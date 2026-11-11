package clob

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// ClobAuthDomain is the EIP-712 domain used to bootstrap L2 credentials.
// Different from the Order domain (which targets the CTFExchange
// contract): this domain proves the EOA owns the address without
// committing to any specific contract.
//
// Polymarket TS SDK reference (signed-data shape):
//
//	domain   = { name: "ClobAuthDomain", version: "1", chainId: 137 }
//	types    = { ClobAuth: [
//	              { name: "address",   type: "address" },
//	              { name: "timestamp", type: "string"  },
//	              { name: "nonce",     type: "uint256" },
//	              { name: "message",   type: "string"  } ] }
//	value    = { address, timestamp, nonce, message }
//
// The signed payload is what POST /auth/api-key validates. Server
// returns L2 credentials (apiKey, secret, passphrase) that we then use
// as HMAC creds for trading.
type ClobAuthDomain struct {
	Name    string // "ClobAuthDomain"
	Version string // "1"
	ChainID int64  // 137 Polygon
}

// DefaultAuthDomain returns the canonical Polymarket L2-auth domain.
func DefaultAuthDomain() ClobAuthDomain {
	return ClobAuthDomain{Name: "ClobAuthDomain", Version: "1", ChainID: 137}
}

// ClobAuthMessage is the fixed message string Polymarket expects.
const ClobAuthMessage = "This message attests that I control the given wallet"

// AuthRequest is the parameters that go into POST /auth/api-key (and
// GET /auth/derive-api-key) request headers.
//
// V2 spec (§3-1) for mode B (POLY_1271 deposit wallets):
//   - POLY_ADDRESS header carries the funder (proxy) address, NOT the
//     signer EOA — this tells PM "issue api keys for this proxy".
//   - POLY_SIGNATURE is still produced with the EOA private key, so PM
//     can verify ownership of the proxy via 1271 isValidSignature.
//   - When Funder is empty, falls back to mode A (POLY_ADDRESS = EOA).
type AuthRequest struct {
	Address   common.Address // EOA address (always; derived from private key)
	Funder    common.Address // proxy/funder address; zero in mode A
	Timestamp time.Time
	Nonce     *big.Int
	Signature string // 0x-prefixed hex, 132 chars (65 bytes + 0x), v=27/28
}

// HasFunder returns true if a non-zero funder/proxy address was set.
func (r AuthRequest) HasFunder() bool {
	return r.Funder != (common.Address{})
}

var (
	clobAuthTypeHash = crypto.Keccak256Hash([]byte(
		"ClobAuth(address address,string timestamp,uint256 nonce,string message)",
	))
	authDomainTypeHash = crypto.Keccak256Hash([]byte(
		"EIP712Domain(string name,string version,uint256 chainId)",
	))
)

// Separator hashes the auth domain (no verifyingContract — that's why
// authDomainTypeHash differs from the order domainTypeHash).
func (d ClobAuthDomain) Separator() common.Hash {
	return crypto.Keccak256Hash(
		authDomainTypeHash.Bytes(),
		crypto.Keccak256Hash([]byte(d.Name)).Bytes(),
		crypto.Keccak256Hash([]byte(d.Version)).Bytes(),
		leftPad32(big.NewInt(d.ChainID).Bytes()),
	)
}

// HashAuth returns the EIP-712 digest of the auth message.
func HashAuth(addr common.Address, ts time.Time, nonce *big.Int, domain ClobAuthDomain) common.Hash {
	tsString := strconv.FormatInt(ts.Unix(), 10)
	structHash := crypto.Keccak256Hash(
		clobAuthTypeHash.Bytes(),
		leftPad32(addr.Bytes()),
		crypto.Keccak256Hash([]byte(tsString)).Bytes(),
		leftPad32(nonce.Bytes()),
		crypto.Keccak256Hash([]byte(ClobAuthMessage)).Bytes(),
	)
	prefix := []byte{0x19, 0x01}
	return crypto.Keccak256Hash(prefix, domain.Separator().Bytes(), structHash.Bytes())
}

// SignAuth signs a fresh ClobAuth payload (current time, nonce=0) with
// privateKeyHex. Returns AuthRequest ready for the http layer.
//
// V2 spec (§3-1): nonce is fixed at 0. PM rejects random nonces with
// 401. The EIP-712 message hashes the EOA address (always), not the
// funder — funder appears ONLY in the POLY_ADDRESS header that the
// server uses to choose which set of api-keys to return.
//
// funderHex is optional ("" or "0x000..." treated as absent). When set,
// the resulting AuthRequest will send POLY_ADDRESS=funder in headers.
func SignAuth(privateKeyHex string, domain ClobAuthDomain) (AuthRequest, error) {
	return SignAuthWithFunder(privateKeyHex, "", domain)
}

// ClobAuthTypeString is the EIP-712 typestring for the ClobAuth payload.
// Exactly 71 bytes. lenHex = "0047".
const ClobAuthTypeString = "ClobAuth(address address,string timestamp,uint256 nonce,string message)"

// ClobAuthTypeStringLen is the byte length of ClobAuthTypeString. Used
// in the ERC-7739 trailer for POLY_1271 wrapped auth.
var ClobAuthTypeStringLen = len(ClobAuthTypeString)

// HashClobAuthStruct returns hashStruct(ClobAuth) — the inner EIP-712
// struct hash without the domain prefix. Required for the contents_hash
// position in the ERC-7739 trailer.
func HashClobAuthStruct(addr common.Address, ts time.Time, nonce *big.Int) common.Hash {
	tsString := strconv.FormatInt(ts.Unix(), 10)
	return crypto.Keccak256Hash(
		clobAuthTypeHash.Bytes(),
		leftPad32(addr.Bytes()),
		crypto.Keccak256Hash([]byte(tsString)).Bytes(),
		leftPad32(nonce.Bytes()),
		crypto.Keccak256Hash([]byte(ClobAuthMessage)).Bytes(),
	)
}

// HashTypedDataSignClobAuth is the ERC-7739 inner-hash for ClobAuth, the
// auth-time analogue of HashTypedDataSign(Order, eoaSigner). Builds the
// TypedDataSign hash whose `contents` is hashStruct(ClobAuth) and whose
// inner EIP-712 domain is DepositWallet/1 with verifyingContract = the
// deposit wallet (funder/proxy) address.
//
// typeHash = keccak256(TypedDataSignHead || ClobAuthTypeString)
// hashStruct(TDS) = keccak256(
//
//	typeHash || contentsHash || keccak256("DepositWallet") ||
//	keccak256("1") || chainId(32) || pad32(funder) || bytes32(0))
func HashTypedDataSignClobAuth(contentsHash common.Hash, funder common.Address) common.Hash {
	typeHash := crypto.Keccak256Hash([]byte(TypedDataSignHead + ClobAuthTypeString))
	dwNameHash := crypto.Keccak256Hash([]byte(DepositWalletName))
	dwVerHash := crypto.Keccak256Hash([]byte(DepositWalletVersion))
	chainIDBytes := leftPad32(big.NewInt(137).Bytes())
	saltZero := make([]byte, 32)
	return crypto.Keccak256Hash(
		typeHash.Bytes(),
		contentsHash.Bytes(),
		dwNameHash.Bytes(),
		dwVerHash.Bytes(),
		chainIDBytes,
		leftPad32(funder.Bytes()),
		saltZero,
	)
}

// SignAuthWithFunder is SignAuth + an explicit funder/proxy override.
// Pass funderHex = "" for mode A (plain EOA ClobAuth signature).
//
// When funderHex is set, this performs the ERC-7739 wrap (Issue
// Polymarket/clob-client-v2#65) so PM's deposit wallet 1271 verifier
// accepts the L1 auth and issues an api-key bound to the funder/proxy
// rather than the bare EOA. Without this wrap, derive-api-key binds
// the key to the EOA and the resulting key is rejected by /order with
// "the order signer address has to be the address of the API KEY".
func SignAuthWithFunder(privateKeyHex, funderHex string, domain ClobAuthDomain) (AuthRequest, error) {
	pk, err := loadPrivateKey(privateKeyHex)
	if err != nil {
		return AuthRequest{}, err
	}
	addr := crypto.PubkeyToAddress(pk.PublicKey)
	ts := time.Now().UTC()
	nonce := big.NewInt(0) // spec §3-1

	var funder common.Address
	funderHex = strings.TrimSpace(funderHex)
	if funderHex != "" && funderHex != "0x"+strings.Repeat("0", 40) {
		if !common.IsHexAddress(funderHex) {
			return AuthRequest{}, fmt.Errorf("clob: SignAuthWithFunder: invalid funder address %q", funderHex)
		}
		funder = common.HexToAddress(funderHex)
	}

	if (funder == common.Address{}) {
		// Mode A: plain EOA ClobAuth signature.
		digest := HashAuth(addr, ts, nonce, domain)
		sig, err := crypto.Sign(digest.Bytes(), pk)
		if err != nil {
			return AuthRequest{}, fmt.Errorf("clob: auth ecsign: %w", err)
		}
		sig[64] += 27
		return AuthRequest{
			Address:   addr,
			Timestamp: ts,
			Nonce:     nonce,
			Signature: "0x" + hex.EncodeToString(sig),
		}, nil
	}

	// Mode B (POLY_1271): ERC-7739-wrap the ClobAuth signature.
	// Step 1: ClobAuth.address = funder (proxy), NOT EOA.
	// Step 2: contentsHash = hashStruct(ClobAuth)
	// Step 3: tdsStructHash = hashStruct(TypedDataSign(ClobAuth, ...))
	//         with verifyingContract = funder, DepositWallet/1 domain
	// Step 4: inner_digest = keccak256(0x1901 || appDomainSep || tdsStructHash)
	// Step 5: EOA signs inner_digest
	// Step 6: wire signature = innerSig || appDomainSep || contentsHash ||
	//                          ClobAuthTypeString.hex || lenHex("0047")
	contentsHash := HashClobAuthStruct(funder, ts, nonce)
	tdsStructHash := HashTypedDataSignClobAuth(contentsHash, funder)
	appDomainSep := domain.Separator()
	innerDigest := crypto.Keccak256Hash(
		[]byte{0x19, 0x01},
		appDomainSep.Bytes(),
		tdsStructHash.Bytes(),
	)
	innerSig, err := crypto.Sign(innerDigest.Bytes(), pk)
	if err != nil {
		return AuthRequest{}, fmt.Errorf("clob: auth ecsign (1271): %w", err)
	}
	if len(innerSig) != 65 {
		return AuthRequest{}, fmt.Errorf("clob: auth inner signature wrong length %d", len(innerSig))
	}
	innerSig[64] += 27

	clobAuthTypeHex := hex.EncodeToString([]byte(ClobAuthTypeString))         // 142 hex chars
	lenHex := fmt.Sprintf("%04x", ClobAuthTypeStringLen)                       // "0047"

	wireSig := "0x" +
		hex.EncodeToString(innerSig) + // 130
		hex.EncodeToString(appDomainSep.Bytes()) + // 64
		hex.EncodeToString(contentsHash.Bytes()) + // 64
		clobAuthTypeHex + // 142
		lenHex // 4
	// Total = 2 + 130 + 64 + 64 + 142 + 4 = 406 chars

	return AuthRequest{
		Address:   addr,
		Funder:    funder,
		Timestamp: ts,
		Nonce:     nonce,
		Signature: wireSig,
	}, nil
}

// VerifyAuthRoundTrip self-checks that the signature in req recovers to
// req.Address (the EOA). For wrapped (mode B) signatures the wire format
// is the ERC-7739 trailer rather than a bare 65-byte sig, so we skip
// roundtrip recovery there — the on-server isValidSignature call is the
// authoritative check.
func VerifyAuthRoundTrip(req AuthRequest, domain ClobAuthDomain) error {
	if req.HasFunder() {
		// Wrapped — server-side 1271 verifies it.
		return nil
	}
	digest := HashAuth(req.Address, req.Timestamp, req.Nonce, domain)
	sig, err := hex.DecodeString(strings.TrimPrefix(req.Signature, "0x"))
	if err != nil {
		return fmt.Errorf("clob: auth signature hex: %w", err)
	}
	if len(sig) != 65 {
		return fmt.Errorf("clob: auth signature wrong length %d", len(sig))
	}
	sigForRecover := make([]byte, 65)
	copy(sigForRecover, sig)
	if sigForRecover[64] >= 27 {
		sigForRecover[64] -= 27
	}
	pub, err := crypto.SigToPub(digest.Bytes(), sigForRecover)
	if err != nil {
		return fmt.Errorf("clob: auth ecrecover: %w", err)
	}
	got := crypto.PubkeyToAddress(*pub)
	if got != req.Address {
		return fmt.Errorf("clob: auth ecrecover got %s, expected %s", got.Hex(), req.Address.Hex())
	}
	return nil
}

// Headers returns the four POLY-* headers for /auth/api-key /
// /auth/derive-api-key. POLY_ADDRESS depends on wallet mode:
//
//   - Mode A (no funder): EOA address (lowercase). Signature is a plain
//     ECDSA over the ClobAuth EIP-712 digest.
//   - Mode B (funder set): funder/proxy address (lowercase). Signature
//     is the ERC-7739 wrapped form produced by SignAuthWithFunder. PM
//     calls the deposit wallet's isValidSignature with this signature
//     and binds the resulting api-key to the funder.
func (r AuthRequest) Headers() map[string]string {
	primaryAddr := r.Address
	sigType := "0"
	if r.HasFunder() {
		primaryAddr = r.Funder
		sigType = "3"
	}
	h := map[string]string{
		"POLY_ADDRESS":   strings.ToLower(primaryAddr.Hex()),
		"POLY_SIGNATURE": r.Signature,
		"POLY_TIMESTAMP": strconv.FormatInt(r.Timestamp.Unix(), 10),
		"POLY_NONCE":     r.Nonce.String(),
	}
	if r.HasFunder() {
		// POLY_SIGNATURE_TYPE header — some PM endpoints require an
		// explicit hint so the server picks the 1271 verification path
		// instead of plain ECDSA recover. Harmless when set in mode A.
		h["POLY_SIGNATURE_TYPE"] = sigType
		// POLY_FUNDER is the same address that's in POLY_ADDRESS for
		// mode B, but some implementations look for it as a separate
		// header to disambiguate.
		h["POLY_FUNDER"] = strings.ToLower(r.Funder.Hex())
		// POLY_SIGNER is the EOA in mode B (EOA private key that
		// produced the inner signature).
		h["POLY_SIGNER"] = strings.ToLower(r.Address.Hex())
	}
	return h
}
