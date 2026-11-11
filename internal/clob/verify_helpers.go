package clob

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// GetBookSanity calls the public GET /book?token_id=... endpoint just to
// confirm network + cert + JSON parsing without requiring auth. Returns
// an error if the response is not 2xx OR the body doesn't decode to a
// shape with bids/asks arrays (PM has been known to return 200 with
// HTML on cert-pinning bugs).
func (c *Client) GetBookSanity(ctx context.Context, tokenID string) error {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return fmt.Errorf("clob: GetBookSanity: token_id required")
	}
	// /book accepts the token_id as a query parameter; no auth.
	path := "/book?token_id=" + tokenID
	body, err := c.do(ctx, http.MethodGet, path, nil, nil)
	if err != nil {
		return err
	}
	var book struct {
		AssetID string `json:"asset_id"`
		Bids    []struct {
			Price string `json:"price"`
			Size  string `json:"size"`
		} `json:"bids"`
		Asks []struct {
			Price string `json:"price"`
			Size  string `json:"size"`
		} `json:"asks"`
	}
	if err := json.Unmarshal(body, &book); err != nil {
		return fmt.Errorf("clob: GetBookSanity decode: %w", err)
	}
	return nil
}

// BalanceAllowance is the parsed response from /balance-allowance.
// Both fields are wei-denominated decimal strings (USDC 6 decimals).
// allowance==0 typically means the CTF Exchange contract has not been
// granted permission to spend the funder's USDC.
type BalanceAllowance struct {
	Balance   string `json:"balance"`
	Allowance string `json:"allowance"`
}

// GetBalanceAllowance calls GET /balance-allowance with the L2 HMAC.
//
// V2 spec invariants exercised here (any mismatch = 401):
//   - HMAC computed on BARE path "/balance-allowance" (query NOT
//     included) even though the URL carries asset_type and
//     signature_type query parameters.
//   - HMAC secret base64-decoded as URL-safe.
//   - POLY_ADDRESS header is the MAKER address, lowercase.
//
// makerAddress: the funder/proxy address (POLY_1271) or EOA (mode A).
// sigType: 0 (EOA) | 1 (PolyProxy) | 2 (GnosisSafe) | 3 (POLY_1271).
func (c *Client) GetBalanceAllowance(ctx context.Context, creds L2Credentials, makerAddress string, sigType int) (BalanceAllowance, error) {
	makerAddress = strings.TrimSpace(makerAddress)
	if makerAddress == "" {
		return BalanceAllowance{}, fmt.Errorf("clob: GetBalanceAllowance: makerAddress required")
	}
	hmacPath := "/balance-allowance"
	urlPath := fmt.Sprintf("/balance-allowance?asset_type=COLLATERAL&signature_type=%d", sigType)

	apiKey, sign, ts, passphrase, err := L2Headers(creds, time.Now().UTC(), http.MethodGet, hmacPath, "")
	if err != nil {
		return BalanceAllowance{}, fmt.Errorf("clob: GetBalanceAllowance L2 headers: %w", err)
	}
	headers := map[string]string{
		"POLY_ADDRESS":    strings.ToLower(makerAddress),
		"POLY_API_KEY":    apiKey,
		"POLY_SIGNATURE":  sign,
		"POLY_TIMESTAMP":  ts,
		"POLY_PASSPHRASE": passphrase,
	}
	body, err := c.do(ctx, http.MethodGet, urlPath, nil, headers)
	if err != nil {
		return BalanceAllowance{}, err
	}
	var ba BalanceAllowance
	if err := json.Unmarshal(body, &ba); err != nil {
		return BalanceAllowance{}, fmt.Errorf("clob: GetBalanceAllowance decode: %w (body=%s)", err, truncate(string(body), 240))
	}
	if ba.Balance == "" {
		ba.Balance = "0"
	}
	if ba.Allowance == "" {
		ba.Allowance = "0"
	}
	return ba, nil
}
