package clob

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/executor"
)

// orderBody is the CLOB v2 (2026-04-28+) on-wire shape POST /order
// expects. Field type quirks vs the EIP-712 hashed payload (spec §4-2):
//
//   - salt: JSON NUMBER (uint64, ≤ 2^53). NOT string — PM server runs
//     parseInt(salt, 10).
//   - side: string "BUY" / "SELL". NOT int.
//   - timestamp: string of milliseconds (now * 1000).
//   - taker: omitempty — when not set, the field is dropped entirely
//     (sending "" or null can be rejected).
//   - expiration: string of unix seconds; "0" = GTC.
//   - metadata/builder: bytes32 hex ("0x" + 64 chars); "0x"+ 64 zeros if none.
//   - signatureType: int (0 EOA | 1 PolyProxy | 2 GnosisSafe | 3 POLY_1271).
//
// nonce and feeRateBps were dropped in V2 — do not include.
type orderBody struct {
	Salt          uint64 `json:"salt"` // NUMBER (≤ 2^53)
	Maker         string `json:"maker"`
	Signer        string `json:"signer"`
	Taker         string `json:"taker,omitempty"`
	TokenID       string `json:"tokenId"`
	MakerAmount   string `json:"makerAmount"`
	TakerAmount   string `json:"takerAmount"`
	Side          string `json:"side"` // "BUY" | "SELL"
	Expiration    string `json:"expiration"`
	SignatureType int    `json:"signatureType"`
	Timestamp     string `json:"timestamp"` // milliseconds
	Metadata      string `json:"metadata"`  // bytes32 hex
	Builder       string `json:"builder"`   // bytes32 hex
	Signature     string `json:"signature"` // 0x... (132 chars EOA / 636 chars 1271)
}

// orderEnvelope is the outer JSON the API actually parses (v2).
type orderEnvelope struct {
	DeferExec bool      `json:"deferExec"`
	PostOnly  bool      `json:"postOnly"`
	Order     orderBody `json:"order"`
	Owner     string    `json:"owner"`     // L2 api key (acts as account id server-side)
	OrderType string    `json:"orderType"` // FAK | FOK | GTC | GTD
}

// OrderResponse is the parsed JSON Polymarket returns from POST /order.
// Field names mirror the SDK; we only model what the executor + audit
// trail actually consume.
type OrderResponse struct {
	Success            bool     `json:"success"`
	ErrorMsg           string   `json:"errorMsg"`
	OrderID            string   `json:"orderID"`
	TransactionsHashes []string `json:"transactionsHashes,omitempty"`
	Status             string   `json:"status,omitempty"` // matched | live | delayed | unmatched
	MakingAmount       string   `json:"makingAmount,omitempty"`
	TakingAmount       string   `json:"takingAmount,omitempty"`
}

// buildOrderBody serializes a signed order spec into the V2 orderEnvelope
// the CLOB POST /order endpoint expects. Pure; no IO.
func buildOrderBody(signed SignedOrder, owner string, orderType executor.OrderType) (orderEnvelope, error) {
	spec := signed.Spec
	if spec.MakerAmount == nil || spec.TakerAmount == nil || spec.Salt == nil {
		return orderEnvelope{}, fmt.Errorf("clob: order body: nil big.Int field")
	}
	if strings.TrimSpace(owner) == "" {
		return orderEnvelope{}, fmt.Errorf("clob: order body: empty owner (api key required)")
	}
	if signed.Signature == "" {
		return orderEnvelope{}, fmt.Errorf("clob: order body: empty signature")
	}
	// V2 wire: side is "BUY" or "SELL" string.
	var sideStr string
	switch spec.Side {
	case executor.SideBuy:
		sideStr = "BUY"
	case executor.SideSell:
		sideStr = "SELL"
	default:
		return orderEnvelope{}, fmt.Errorf("clob: order body: unknown side %q", spec.Side)
	}
	// V2 salt is JSON NUMBER; must be ≤ 2^53. Build() already constrains
	// the salt, but defensively clamp here too.
	if !spec.Salt.IsUint64() {
		return orderEnvelope{}, fmt.Errorf("clob: order body: salt overflows uint64: %s", spec.Salt.String())
	}
	salt := spec.Salt.Uint64()
	if salt > (1<<53)-1 {
		return orderEnvelope{}, fmt.Errorf("clob: order body: salt > 2^53 (JS Number unsafe): %d", salt)
	}
	// V2 timestamp: milliseconds string. Falls back to "now" if Build()
	// did not set one (shouldn't happen with current Build() V2).
	tsMS := spec.Timestamp
	if tsMS == 0 {
		tsMS = time.Now().UTC().UnixMilli()
	}
	// metadata / builder must be bytes32 hex; default to all-zero.
	metadata := strings.TrimSpace(spec.Metadata)
	if metadata == "" {
		metadata = "0x" + strings.Repeat("0", 64)
	}
	builder := strings.TrimSpace(spec.Builder)
	if builder == "" {
		builder = "0x" + strings.Repeat("0", 64)
	}
	return orderEnvelope{
		DeferExec: false,
		PostOnly:  false,
		Order: orderBody{
			Salt:          salt,
			Maker:         strings.ToLower(spec.Maker),
			Signer:        strings.ToLower(spec.Signer),
			// Taker is left empty -> omitempty drops it from the JSON.
			TokenID:       spec.TokenID,
			MakerAmount:   spec.MakerAmount.String(),
			TakerAmount:   spec.TakerAmount.String(),
			Side:          sideStr,
			Expiration:    strconv.FormatInt(spec.Expiration, 10),
			SignatureType: int(spec.SignatureType),
			Timestamp:     strconv.FormatInt(tsMS, 10),
			Metadata:      metadata,
			Builder:       builder,
			Signature:     signed.Signature,
		},
		Owner:     owner,
		OrderType: string(orderType),
	}, nil
}

// SubmitOrder POSTs a signed order to /order with the four POLY-* L2
// headers HMAC-signed via creds. Returns the parsed response on 2xx;
// *APIError on non-2xx; transport errors wrapped.
//
// Caller should:
//   - persist a proposed_orders row with status='planned' BEFORE calling
//     SubmitOrder (so a crash mid-call leaves an audit trail)
//   - on success: update status='sent' (or 'filled' if response says so)
//   - on error: update status='rejected' with the error message
//
// This function does NOT touch the DB; pure HTTP.
func (c *Client) SubmitOrder(ctx context.Context, signed SignedOrder, creds L2Credentials, orderType executor.OrderType) (OrderResponse, error) {
	envelope, err := buildOrderBody(signed, creds.APIKey, orderType)
	if err != nil {
		return OrderResponse{}, err
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return OrderResponse{}, fmt.Errorf("clob: marshal order body: %w", err)
	}
	apiKey, sign, ts, passphrase, err := L2Headers(creds, time.Now().UTC(), http.MethodPost, "/order", string(body))
	if err != nil {
		return OrderResponse{}, fmt.Errorf("clob: L2 headers: %w", err)
	}
	// POLY_ADDRESS is the MAKER address (lowercase). For wallet mode A
	// (EOA-only) this equals the signer; for mode B (POLY_1271) this is
	// the funder/proxy address that holds USDC. Spec §3-2.
	headers := map[string]string{
		"POLY_ADDRESS":    strings.ToLower(signed.Spec.Maker),
		"POLY_API_KEY":    apiKey,
		"POLY_SIGNATURE":  sign,
		"POLY_TIMESTAMP":  ts,
		"POLY_PASSPHRASE": passphrase,
	}
	respBody, err := c.do(ctx, http.MethodPost, "/order", body, headers)
	if err != nil {
		return OrderResponse{}, err
	}
	var resp OrderResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return OrderResponse{}, fmt.Errorf("clob: SubmitOrder decode: %w (body=%s)", err, truncate(string(respBody), 240))
	}
	if !resp.Success {
		return resp, fmt.Errorf("clob: SubmitOrder server-reported failure: %s", resp.ErrorMsg)
	}
	return resp, nil
}

// BodyJSON returns the JSON-encoded envelope as it would be POSTed.
// Exposed for dry-run modes that want to log the wire body without
// touching the network.
func BodyJSON(signed SignedOrder, ownerAPIKey string, orderType executor.OrderType) ([]byte, error) {
	env, err := buildOrderBody(signed, ownerAPIKey, orderType)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(env, "", "  ")
}
