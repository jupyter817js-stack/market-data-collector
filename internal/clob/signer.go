// Package clob produces signed Polymarket CLOB orders from the planner's
// OrderSpec. Implements the V2 (post 2026-04-28) offline EIP-712 signer
// + L2 HMAC helper + the EIP-1271 wrapping flow for signatureType=3
// (POLY_1271, Polymarket Deposit Wallets).
//
// EIP-712 domain V2 (Polygon, chainId=137):
//
//	EIP712Domain {
//	    string  name              = "Polymarket CTF Exchange"
//	    string  version           = "2"                  ← V2
//	    uint256 chainId           = 137
//	    address verifyingContract = <CTFExchange V2 or NegRisk V2>
//	}
//
// Order struct V2 (11 fields — DROPPED taker/expiration/nonce/feeRateBps,
// ADDED timestamp/metadata/builder):
//
//	struct Order {
//	    uint256 salt;
//	    address maker;
//	    address signer;
//	    uint256 tokenId;
//	    uint256 makerAmount;
//	    uint256 takerAmount;
//	    uint8   side;             // 0 = BUY, 1 = SELL
//	    uint8   signatureType;    // 0 EOA | 1 PolyProxy | 2 GnosisSafe | 3 POLY_1271 (Deposit Wallets)
//	    uint256 timestamp;        // milliseconds
//	    bytes32 metadata;
//	    bytes32 builder;          // PM Builder ID (0x... 32 bytes; 0 if no builder)
//	}
//
// signatureType=3 (POLY_1271) uses an additional EIP-1271 wrapping
// layer — see SignWrapped() for the payload contruction. The inner
// EIP-712 domain for the TypedDataSign is DepositWallet/1, with
// verifyingContract=order.signer and salt=bytes32(0).
//
// The CTF Exchange address depends on whether this is the standard or
// neg-risk exchange; Domain.VerifyingContract is the address that owns
// the order book the trader is interacting with. Both are configurable
// via DomainConfig so a misconfiguration is caught at sign time, not
// at submission time.
package clob

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/your-org/pol-odds-collector/internal/executor"
)

// DomainConfig is the EIP-712 domain. Values match Polymarket on Polygon
// mainnet; expose for tests / alt deployments.
type DomainConfig struct {
	Name              string         // "Polymarket CTF Exchange"
	Version           string         // "1"
	ChainID           int64          // 137 = Polygon mainnet
	VerifyingContract common.Address // CTF Exchange (or neg-risk) address
}

// Default Polymarket exchange addresses on Polygon V2 (2026-04-28+).
// The standard CTF Exchange holds binary CTF markets; the neg-risk
// variant holds multi-outcome neg-risk markets (e.g. multi-team event
// winners). V1 addresses are legacy; do not use for new orders.
var (
	// V2 addresses — current
	CTFExchangeAddress        = common.HexToAddress("0xE111180000d2663C0091e4f400237545B87B996B")
	NegRiskCTFExchangeAddress = common.HexToAddress("0xe2222d279d744050d28e00520010520000310F59")

	// V1 legacy — kept as named constants for diagnostic comparison only
	CTFExchangeAddressV1Legacy        = common.HexToAddress("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
	NegRiskCTFExchangeAddressV1Legacy = common.HexToAddress("0xC5d563A36AE78145C45a50134d48A1215220f80a")
)

// V2AppDomainSeparator is the expected EIP-712 domain separator hash for
// the V2 standard CTF Exchange (chainId 137). Used as a self-check in
// tests — if this constant does not match the computed
// DefaultDomain().Separator(), something is wrong with the domain
// (name/version/chainId/verifyingContract).
var V2AppDomainSeparator = common.HexToHash("0x3264e159346253e26a64e00b69032db0e7d32f94628de3e6eecb50304d7af3d2")

// DefaultDomain returns the canonical mainnet V2 domain pointed at the
// standard CTF Exchange. Override VerifyingContract for neg-risk
// markets (use NegRiskCTFExchangeAddress).
func DefaultDomain() DomainConfig {
	return DomainConfig{
		Name:              "Polymarket CTF Exchange",
		Version:           "2",
		ChainID:           137,
		VerifyingContract: CTFExchangeAddress,
	}
}

// DepositWalletDomain is the inner EIP-712 domain used for signatureType=3
// POLY_1271 wrapping. verifyingContract is the EOA address (= order.signer)
// and salt is bytes32(0). Used by SignWrapped.
type DepositWalletDomain struct {
	Name              string         // "DepositWallet"
	Version           string         // "1"
	ChainID           int64          // 137
	VerifyingContract common.Address // EOA address
}

const (
	DepositWalletName    = "DepositWallet"
	DepositWalletVersion = "1"
)

// SignedOrder is the planner's OrderSpec plus the 65-byte ECDSA signature
// produced by Sign. The Signature is in Ethereum r‖s‖v form (v = 27 or 28),
// hex-encoded with 0x prefix in the JSON-ready Signature field.
type SignedOrder struct {
	Spec       executor.OrderSpec
	Domain     DomainConfig
	OrderHash  common.Hash // EIP-712 digest that was actually signed
	Signature  string      // 0x-prefixed hex, 132 chars (65 bytes + 0x)
	SignerAddr common.Address
}

// V2 ORDER_TYPE_STRING. EXACTLY 186 bytes — verified by OrderTypeStringLen
// at init. The EIP-1271 wrapper's lenHex trailer ("00ba" = 186) must match.
const OrderTypeString = "Order(uint256 salt,address maker,address signer,uint256 tokenId," +
	"uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType," +
	"uint256 timestamp,bytes32 metadata,bytes32 builder)"

// OrderTypeStringLen is the byte length of OrderTypeString. Must be 186
// for V2 compatibility with the EIP-1271 trailer "00ba".
var OrderTypeStringLen = len(OrderTypeString)

// orderTypeHash = keccak256(OrderTypeString)
var orderTypeHash = crypto.Keccak256Hash([]byte(OrderTypeString))

// domainTypeHash = keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
var domainTypeHash = crypto.Keccak256Hash([]byte(
	"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
))

// DomainSeparator hashes the EIP-712 domain. Stable per (chainId, contract).
func (d DomainConfig) Separator() common.Hash {
	return crypto.Keccak256Hash(
		domainTypeHash.Bytes(),
		crypto.Keccak256Hash([]byte(d.Name)).Bytes(),
		crypto.Keccak256Hash([]byte(d.Version)).Bytes(),
		leftPad32(big.NewInt(d.ChainID).Bytes()),
		leftPad32(d.VerifyingContract.Bytes()),
	)
}

// HashOrderStruct returns ONLY hashStruct(Order) for V2 — the inner
// EIP-712 struct hash without the domain prefix. Used by SignWrapped
// for POLY_1271 contentsHash.
func HashOrderStruct(spec executor.OrderSpec) (common.Hash, error) {
	if spec.MakerAmount == nil || spec.TakerAmount == nil || spec.Salt == nil {
		return common.Hash{}, fmt.Errorf("clob: nil big.Int field in OrderSpec")
	}
	makerAddr, err := parseAddress(spec.Maker)
	if err != nil {
		return common.Hash{}, fmt.Errorf("clob: maker address: %w", err)
	}
	signerAddr, err := parseAddress(spec.Signer)
	if err != nil {
		return common.Hash{}, fmt.Errorf("clob: signer address: %w", err)
	}
	tokenID, ok := new(big.Int).SetString(spec.TokenID, 10)
	if !ok {
		return common.Hash{}, fmt.Errorf("clob: token id %q not decimal", spec.TokenID)
	}
	var sideByte byte
	switch spec.Side {
	case executor.SideBuy:
		sideByte = 0
	case executor.SideSell:
		sideByte = 1
	default:
		return common.Hash{}, fmt.Errorf("clob: unknown side %q", spec.Side)
	}

	// V2 Order fields (11): salt, maker, signer, tokenId, makerAmount,
	// takerAmount, side, signatureType, timestamp, metadata, builder.
	// metadata and builder are bytes32 — already 32 bytes.
	metadataBytes := bytes32From(spec.Metadata)
	builderBytes := bytes32From(spec.Builder)

	structHash := crypto.Keccak256Hash(
		orderTypeHash.Bytes(),
		leftPad32(spec.Salt.Bytes()),
		leftPad32(makerAddr.Bytes()),
		leftPad32(signerAddr.Bytes()),
		leftPad32(tokenID.Bytes()),
		leftPad32(spec.MakerAmount.Bytes()),
		leftPad32(spec.TakerAmount.Bytes()),
		leftPad32([]byte{sideByte}),
		leftPad32([]byte{byte(spec.SignatureType)}),
		leftPad32(big.NewInt(spec.Timestamp).Bytes()),
		metadataBytes,
		builderBytes,
	)
	return structHash, nil
}

// HashOrder produces the EIP-712 typed-data digest (V2) for spec under
// the given app domain. This is the bytes32 that gets fed into ecsign.
// Pure; no IO.
//
// For signatureType=0/1/2 the digest is what gets signed and submitted.
// For signatureType=3 (POLY_1271) the inner digest used for the wrapped
// signature is different — see SignWrapped().
func HashOrder(spec executor.OrderSpec, domain DomainConfig) (common.Hash, error) {
	structHash, err := HashOrderStruct(spec)
	if err != nil {
		return common.Hash{}, err
	}
	prefix := []byte{0x19, 0x01}
	digest := crypto.Keccak256Hash(prefix, domain.Separator().Bytes(), structHash.Bytes())
	return digest, nil
}

// bytes32From parses a "0x" hex string into exactly 32 bytes. Empty
// string -> all-zero. Returns the padded 32-byte slice.
func bytes32From(hex string) []byte {
	hex = strings.TrimSpace(hex)
	if hex == "" || hex == "0x" || hex == "0x0" {
		return make([]byte, 32)
	}
	hexBytes := common.FromHex(hex)
	if len(hexBytes) >= 32 {
		return hexBytes[len(hexBytes)-32:]
	}
	out := make([]byte, 32)
	copy(out[32-len(hexBytes):], hexBytes)
	return out
}

// Sign produces the EIP-712 signature for spec using privateKeyHex (no 0x
// prefix accepted or required). Returns a SignedOrder ready for the JSON
// POST body (signature is r‖s‖v with v=27/28). Pure cryptographic; does no
// IO.
func Sign(spec executor.OrderSpec, domain DomainConfig, privateKeyHex string) (SignedOrder, error) {
	pk, err := loadPrivateKey(privateKeyHex)
	if err != nil {
		return SignedOrder{}, err
	}
	digest, err := HashOrder(spec, domain)
	if err != nil {
		return SignedOrder{}, err
	}
	sig, err := crypto.Sign(digest.Bytes(), pk)
	if err != nil {
		return SignedOrder{}, fmt.Errorf("clob: ecsign: %w", err)
	}
	if len(sig) != 65 {
		return SignedOrder{}, fmt.Errorf("clob: signature wrong length %d", len(sig))
	}
	// crypto.Sign returns v as 0 or 1; Ethereum / EIP-712 wire format wants
	// 27 or 28. (Bumping to 27/28 mirrors what the TS SDK's signTypedData
	// returns.)
	sig[64] += 27

	signerAddr := crypto.PubkeyToAddress(pk.PublicKey)
	if !strings.EqualFold(signerAddr.Hex(), spec.Signer) {
		return SignedOrder{}, fmt.Errorf("clob: spec.Signer=%s but private key derives %s",
			spec.Signer, signerAddr.Hex())
	}

	return SignedOrder{
		Spec:       spec,
		Domain:     domain,
		OrderHash:  digest,
		Signature:  "0x" + hex.EncodeToString(sig),
		SignerAddr: signerAddr,
	}, nil
}

// TypedDataSignHead is the EIP-712 typestring prefix used to compute the
// 1271 wallet's TypedDataSign typeHash. NOT included in the trailer of
// the on-wire signature — only in the typeHash computation. Spec §4-1.
const TypedDataSignHead = "TypedDataSign(Order contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)"

// HashTypedDataSign returns hashStruct(TypedDataSign) for POLY_1271
// (signatureType=3). The inner EIP-712 domain is:
//
//	name              = "DepositWallet"
//	version           = "1"
//	chainId           = 137
//	verifyingContract = order.signer (= EOA wallet)
//	salt              = bytes32(0)
//
// typeHash = keccak256(TypedDataSignHead || OrderTypeString)
// hashStruct(TDS) = keccak256(
//   typeHash || contentsHash || keccak256("DepositWallet") ||
//   keccak256("1") || chainId(32) || pad32(EOA) || bytes32(0))
func HashTypedDataSign(contentsHash common.Hash, eoaSigner common.Address) common.Hash {
	typeHash := crypto.Keccak256Hash([]byte(TypedDataSignHead + OrderTypeString))
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
		leftPad32(eoaSigner.Bytes()),
		saltZero,
	)
}

// SignWrapped produces the POLY_1271 (signatureType=3) wrapped signature.
// Spec §4-1 / RN1 §C.1.4.
//
// Trailer layout (hex):
//
//	0x | innerSig(130) | appDomainSep(64) | contentsHash(64) | OrderTypeString.hex(372) | "00ba"
//
// Total length: 2 + 130 + 64 + 64 + 372 + 4 = 636 chars (= 317 bytes payload).
func SignWrapped(spec executor.OrderSpec, domain DomainConfig, privateKeyHex string) (SignedOrder, error) {
	pk, err := loadPrivateKey(privateKeyHex)
	if err != nil {
		return SignedOrder{}, err
	}
	contentsHash, err := HashOrderStruct(spec)
	if err != nil {
		return SignedOrder{}, err
	}
	signerAddr, err := parseAddress(spec.Signer)
	if err != nil {
		return SignedOrder{}, fmt.Errorf("clob: signer address: %w", err)
	}
	tdsStructHash := HashTypedDataSign(contentsHash, signerAddr)

	// inner digest = keccak256(0x1901 || appDomainSep || tdsStructHash)
	appDomainSep := domain.Separator()
	innerDigest := crypto.Keccak256Hash(
		[]byte{0x19, 0x01},
		appDomainSep.Bytes(),
		tdsStructHash.Bytes(),
	)

	innerSig, err := crypto.Sign(innerDigest.Bytes(), pk)
	if err != nil {
		return SignedOrder{}, fmt.Errorf("clob: ecsign (1271): %w", err)
	}
	if len(innerSig) != 65 {
		return SignedOrder{}, fmt.Errorf("clob: inner signature wrong length %d", len(innerSig))
	}
	innerSig[64] += 27

	// In POLY_1271 mode, spec.Signer may be the proxy contract (not the
	// EOA) — PM's deposit-wallet flow expects signer = funder so that
	// the api-key (proxy-bound) matches the on-wire order.signer. We
	// no longer assert EOA == spec.Signer here; the inner signature is
	// the cryptographic anchor and PM's 1271 isValidSignature on the
	// proxy verifies ownership of the EOA private key.
	derivedSigner := crypto.PubkeyToAddress(pk.PublicKey)

	// Trailer: innerSig || appDomainSep || contentsHash || ORDER_TYPE_STRING.hex || "00ba"
	orderTypeStringHex := hex.EncodeToString([]byte(OrderTypeString)) // 372 hex chars
	lenHex := fmt.Sprintf("%04x", OrderTypeStringLen)                 // "00ba"

	wireSig := "0x" +
		hex.EncodeToString(innerSig) + // 130
		hex.EncodeToString(appDomainSep.Bytes()) + // 64
		hex.EncodeToString(contentsHash.Bytes()) + // 64
		orderTypeStringHex + // 372
		lenHex // 4

	// Invariant: 2 + 130 + 64 + 64 + 372 + 4 = 636
	if len(wireSig) != 636 {
		return SignedOrder{}, fmt.Errorf("clob: SignWrapped wire sig wrong length %d, want 636", len(wireSig))
	}

	return SignedOrder{
		Spec:       spec,
		Domain:     domain,
		OrderHash:  innerDigest,
		Signature:  wireSig,
		SignerAddr: derivedSigner,
	}, nil
}

// SignOrder is the entry point that picks Sign or SignWrapped based on
// spec.SignatureType. Use this from the executor to keep wallet-mode
// branching out of caller code.
func SignOrder(spec executor.OrderSpec, domain DomainConfig, privateKeyHex string) (SignedOrder, error) {
	if spec.SignatureType == executor.SignatureTypePoly1271 {
		return SignWrapped(spec, domain, privateKeyHex)
	}
	return Sign(spec, domain, privateKeyHex)
}

// DeriveEOAAddress returns the 0x-prefixed checksum-hex address of the
// EOA that owns privateKeyHex. Useful at daemon startup to populate
// POLY_ADDRESS/maker when running in mode A (EOA-only) without requiring
// the operator to repeat the address in env.
func DeriveEOAAddress(privateKeyHex string) (string, error) {
	pk, err := loadPrivateKey(privateKeyHex)
	if err != nil {
		return "", err
	}
	return crypto.PubkeyToAddress(pk.PublicKey).Hex(), nil
}

// VerifyTestRoundTrip is a self-check that ecrecover on (digest, signature)
// returns the expected signer. Used by tests; cheap enough to call before
// submission too if paranoia is warranted.
func VerifyTestRoundTrip(digest common.Hash, signatureHex string, expectedSigner common.Address) error {
	sig, err := hex.DecodeString(strings.TrimPrefix(signatureHex, "0x"))
	if err != nil {
		return fmt.Errorf("clob: signature hex: %w", err)
	}
	if len(sig) != 65 {
		return fmt.Errorf("clob: signature wrong length %d", len(sig))
	}
	// Undo the +27 bump before passing to SigToPub (which expects 0/1).
	sigForRecover := make([]byte, 65)
	copy(sigForRecover, sig)
	if sigForRecover[64] >= 27 {
		sigForRecover[64] -= 27
	}
	pub, err := crypto.SigToPub(digest.Bytes(), sigForRecover)
	if err != nil {
		return fmt.Errorf("clob: ecrecover: %w", err)
	}
	got := crypto.PubkeyToAddress(*pub)
	if got != expectedSigner {
		return fmt.Errorf("clob: ecrecover got %s, expected %s", got.Hex(), expectedSigner.Hex())
	}
	return nil
}

func parseAddress(s string) (common.Address, error) {
	if !common.IsHexAddress(s) {
		return common.Address{}, fmt.Errorf("not a hex address: %q", s)
	}
	return common.HexToAddress(s), nil
}

func loadPrivateKey(hexStr string) (*ecdsa.PrivateKey, error) {
	hexStr = strings.TrimPrefix(strings.TrimSpace(hexStr), "0x")
	if hexStr == "" {
		return nil, fmt.Errorf("clob: empty private key (set POLYMARKET_PRIVATE_KEY)")
	}
	pk, err := crypto.HexToECDSA(hexStr)
	if err != nil {
		return nil, fmt.Errorf("clob: invalid private key: %w", err)
	}
	return pk, nil
}

func leftPad32(b []byte) []byte {
	if len(b) >= 32 {
		return b[len(b)-32:]
	}
	out := make([]byte, 32)
	copy(out[32-len(b):], b)
	return out
}
