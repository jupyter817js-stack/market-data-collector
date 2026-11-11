package clob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// HTTPDoer is the minimal interface we need from http.Client; lets tests
// inject a stub server or recording client.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// DefaultBaseURL is Polymarket's CLOB API origin on mainnet. The full
// endpoint path (/auth/api-key, /order, /order/{id}) is appended at the
// call site.
const DefaultBaseURL = "https://clob.polymarket.com"

// Client wraps an HTTPDoer with the CLOB base URL and a default request
// timeout. It is safe for concurrent use because http.Client itself is.
type Client struct {
	Doer    HTTPDoer
	BaseURL string
}

// NewClient returns a Client backed by a fresh http.Client with `timeout`.
// Pass 0 timeout to defer to the operating system default (not recommended
// for the trading path — orders should have an aggressive deadline so a
// stuck POST does not silently overstay the signal's freshness window).
func NewClient(baseURL string, timeout time.Duration) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")
	return &Client{
		Doer:    &http.Client{Timeout: timeout},
		BaseURL: baseURL,
	}
}

// APIError is returned for non-2xx CLOB responses.
type APIError struct {
	Status int
	URL    string
	Body   string
}

func (e *APIError) Error() string {
	if len(e.Body) > 240 {
		return fmt.Sprintf("clob: %s: http %d: %s...", e.URL, e.Status, e.Body[:240])
	}
	return fmt.Sprintf("clob: %s: http %d: %s", e.URL, e.Status, e.Body)
}

// do is the single HTTP egress point. headers are set verbatim (overwriting
// anything previously on the request). Returns the response body bytes on
// 2xx; *APIError on non-2xx; transport errors wrapped as fmt.Errorf.
func (c *Client) do(ctx context.Context, method, path string, body []byte, headers map[string]string) ([]byte, error) {
	b, _, err := c.doWithStatus(ctx, method, path, body, headers)
	return b, err
}

// doWithStatus is the same as `do` but also returns the HTTP status code,
// even on error. Callers can use this to branch on 404 (e.g. derive-api-
// key returns 404 for new accounts, which is fine — fall through to
// create).
func (c *Client) doWithStatus(ctx context.Context, method, path string, body []byte, headers map[string]string) ([]byte, int, error) {
	if !strings.HasPrefix(path, "/") {
		return nil, 0, fmt.Errorf("clob: do: path must be absolute, got %q", path)
	}
	url := c.BaseURL + path
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, 0, fmt.Errorf("clob: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Doer.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("clob: %s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return b, resp.StatusCode, &APIError{Status: resp.StatusCode, URL: url, Body: string(b)}
	}
	return b, resp.StatusCode, nil
}

// BootstrapL2 calls POST /auth/api-key with the L1-signed AuthRequest and
// parses the L2 credentials from the response. Pure: does not persist.
// Caller is responsible for storing the returned creds (see CredsStore).
//
// SECURITY: this exchanges the EOA's L1 signature for an API key that can
// place orders on behalf of the wallet. Treat the returned secret as a
// trading credential — do not log it, do not commit it, persist it only
// where you'd persist a wallet private key.
func (c *Client) BootstrapL2(ctx context.Context, authReq AuthRequest) (L2Credentials, error) {
	if err := VerifyAuthRoundTrip(authReq, DefaultAuthDomain()); err != nil {
		return L2Credentials{}, fmt.Errorf("clob: BootstrapL2: refusing to send unverifiable auth request: %w", err)
	}
	// V2 spec (§3-1): if the EOA already has API keys (created by PM UI
	// or another tool), POST /auth/api-key returns 400 "Could not create
	// api key". The right path then is GET /auth/derive-api-key which
	// returns the existing credential. Try derive first; if 404 fall
	// through to POST /auth/api-key.
	if creds, ok, derr := c.deriveL2(ctx, authReq); derr != nil {
		return L2Credentials{}, derr
	} else if ok {
		return creds, nil
	}
	return c.createL2(ctx, authReq)
}

// deriveL2 calls GET /auth/derive-api-key with the same L1 headers.
// Returns (creds, true, nil) on 200, (zero, false, nil) on 404 (no
// existing creds — caller should POST /auth/api-key), and an error
// otherwise.
func (c *Client) deriveL2(ctx context.Context, authReq AuthRequest) (L2Credentials, bool, error) {
	hdrs := authReq.Headers()
	fmt.Fprintf(os.Stderr, "[debug] deriveL2 headers: ADDR=%s SIG_LEN=%d TS=%s NONCE=%s\n",
		hdrs["POLY_ADDRESS"], len(hdrs["POLY_SIGNATURE"]), hdrs["POLY_TIMESTAMP"], hdrs["POLY_NONCE"])
	body, status, err := c.doWithStatus(ctx, http.MethodGet, "/auth/derive-api-key", nil, hdrs)
	fmt.Fprintf(os.Stderr, "[debug] deriveL2 raw response (status=%d): %s\n", status, truncate(string(body), 800))
	if status == http.StatusNotFound {
		return L2Credentials{}, false, nil
	}
	if err != nil {
		return L2Credentials{}, false, fmt.Errorf("clob: deriveL2: %w", err)
	}
	creds, perr := parseL2Creds(body)
	if perr != nil {
		return L2Credentials{}, false, fmt.Errorf("clob: deriveL2 decode: %w", perr)
	}
	return creds, true, nil
}

func (c *Client) createL2(ctx context.Context, authReq AuthRequest) (L2Credentials, error) {
	body, err := c.do(ctx, http.MethodPost, "/auth/api-key", []byte("{}"), authReq.Headers())
	if err != nil {
		return L2Credentials{}, err
	}
	creds, perr := parseL2Creds(body)
	if perr != nil {
		return L2Credentials{}, fmt.Errorf("clob: createL2 decode: %w", perr)
	}
	return creds, nil
}

func parseL2Creds(body []byte) (L2Credentials, error) {
	var resp struct {
		APIKey     string `json:"apiKey"`
		Secret     string `json:"secret"`
		Passphrase string `json:"passphrase"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return L2Credentials{}, fmt.Errorf("decode: %w (body=%s)", err, truncate(string(body), 240))
	}
	if resp.APIKey == "" || resp.Secret == "" {
		return L2Credentials{}, fmt.Errorf("empty credentials in response (body=%s)", truncate(string(body), 240))
	}
	return L2Credentials{
		APIKey:        resp.APIKey,
		APISecret:     resp.Secret,
		APIPassphrase: resp.Passphrase,
	}, nil
}

// RawRequest is a thin wrapper around `do` for callers that want to
// build their own headers (DELETE /order with HMAC etc.). Returns the
// body bytes on 2xx, *APIError on non-2xx.
func (c *Client) RawRequest(ctx context.Context, method, path string, body []byte, headers map[string]string) ([]byte, error) {
	return c.do(ctx, method, path, body, headers)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
