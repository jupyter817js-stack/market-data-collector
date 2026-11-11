package clob

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// L2Credentials are the L2 trading credentials issued by Polymarket's
// auth bootstrap (POST /auth/api-key). The bootstrap call itself uses
// the L1 EIP-712 signature; once issued, every trading request signs
// with HMAC-SHA256 against the secret.
type L2Credentials struct {
	APIKey        string // POLY-API-KEY header
	APISecret     string // base64; HMAC key
	APIPassphrase string // POLY-PASSPHRASE header
}

// L2Headers builds the four POLY-* headers required on every trading API
// request: KEY, SIGN, TIMESTAMP, PASSPHRASE. CLOB v2 (2026-04-28+):
//
//	POLY_SIGNATURE = base64url( HMAC-SHA256(secret_bytes, ts+METHOD+path+body) )
//
// **Critical v2 invariants (mismatch = 401):**
//   - secret is URL-safe base64 (contains `-`/`_`, not `+`/`/`); decode
//     accordingly. Standard base64 decode WILL silently corrupt the key.
//   - output is URL-safe base64 too.
//   - `path` is the BARE path — never include the query string. Caller
//     handles the split (e.g. for /balance-allowance?asset_type=...,
//     pass "/balance-allowance" here).
//   - body byte stream must EXACTLY match what is sent over the wire —
//     no whitespace re-formatting between HMAC sign and HTTP send.
//
// Returns the four header values in fixed order.
func L2Headers(creds L2Credentials, timestamp time.Time, method, path, body string) (key, sign, ts, passphrase string, err error) {
	if strings.TrimSpace(creds.APIKey) == "" || strings.TrimSpace(creds.APISecret) == "" {
		return "", "", "", "", fmt.Errorf("clob: L2 credentials missing api key or secret")
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		return "", "", "", "", fmt.Errorf("clob: L2 method required")
	}
	if !strings.HasPrefix(path, "/") {
		return "", "", "", "", fmt.Errorf("clob: L2 path must be absolute (got %q)", path)
	}
	if strings.ContainsAny(path, "?#") {
		return "", "", "", "", fmt.Errorf("clob: L2 path must be bare (no query string), got %q", path)
	}
	ts = strconv.FormatInt(timestamp.Unix(), 10)

	// v2 secret is URL-safe base64; decode that way as primary.
	secretBytes, err := base64.URLEncoding.DecodeString(creds.APISecret)
	if err != nil {
		// Fall back to standard base64 for any legacy creds.
		secretBytes, err = base64.StdEncoding.DecodeString(creds.APISecret)
		if err != nil {
			return "", "", "", "", fmt.Errorf("clob: L2 api secret not base64: %w", err)
		}
	}
	prehash := ts + method + path + body
	mac := hmac.New(sha256.New, secretBytes)
	mac.Write([]byte(prehash))
	sign = base64.URLEncoding.EncodeToString(mac.Sum(nil))

	return creds.APIKey, sign, ts, creds.APIPassphrase, nil
}
