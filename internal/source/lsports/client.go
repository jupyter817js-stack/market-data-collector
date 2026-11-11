// Package lsports is a read-only client for the LSPORTS TRADE360 REST API.
//
// TRADE360 uses a single auth shape: every REST call POSTs a JSON body that
// includes {PackageId, UserName, Password} along with any endpoint-specific
// fields. There is no bearer token. Pre-match and in-play are separate
// "packages" (different PackageId values) so the credentials object carries
// the package ID — callers construct one Client per feed.
//
// This package is intentionally minimal: it is the substrate for a future
// in-play snapshot poller (cmd/lsports-collector) and an AMQP push consumer
// (cmd/lsports-inplay). The first deliverable is cmd/lsports-probe which
// validates credentials and discovers SportIds for Tennis / MLB / Soccer.
package lsports

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultBaseURL is the customer / metadata / subscription / distribution
// host. Snapshot calls use a different host (stm-snapshot.lsports.eu) and
// callers should construct a separate Client for that base URL.
const (
	DefaultBaseURL         = "https://stm-api.lsports.eu"
	DefaultSnapshotBaseURL = "https://stm-snapshot.lsports.eu"
)

type Credentials struct {
	PackageID int
	Username  string
	Password  string
}

type Client struct {
	http    *http.Client
	baseURL string
	creds   Credentials
}

func NewClient(baseURL string, creds Credentials, timeout time.Duration) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{
		http:    &http.Client{Timeout: timeout},
		baseURL: strings.TrimRight(baseURL, "/"),
		creds:   creds,
	}
}

// Post issues an authenticated POST to baseURL+path. Auth credentials are
// injected into the JSON body alongside the caller's extra fields. Returns
// the raw response body bytes and the HTTP status code. On non-2xx the error
// carries a truncated, credential-redacted body for diagnostic logging.
func (c *Client) Post(ctx context.Context, path string, extra map[string]any) ([]byte, int, error) {
	// TRADE360 uses camelCase JSON across the board: packageId / userName /
	// password. Earlier PascalCase guesses produced 404/400 against the
	// real endpoints because the server treated the body as missing auth.
	body := map[string]any{
		"packageId": c.creds.PackageID,
		"userName":  c.creds.Username,
		"password":  c.creds.Password,
	}
	for k, v := range extra {
		body[k] = v
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("lsports: marshal body: %w", err)
	}

	u := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(payload))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pol-odds-collector/lsports")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("lsports: %s: %s", u, c.redact(err.Error()))
	}
	defer resp.Body.Close()

	raw, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return raw, resp.StatusCode, fmt.Errorf("lsports: %s: read body: %w", u, readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return raw, resp.StatusCode, fmt.Errorf("lsports: %s: http %d: %s", u, resp.StatusCode, c.redact(truncate(string(raw), 400)))
	}
	return raw, resp.StatusCode, nil
}

// EnvelopeHeader is the LSPORTS-wide response wrapper.
//
// TRADE360 has an asymmetric casing convention observed on the live API as of
// 2026-05-27: request bodies are lowerCamelCase (packageId/userName/password)
// but response bodies are PascalCase (Header/Body/Sports/Id/Name). The
// public OpenAPI docs show lowerCamel in both directions but the live server
// returns PascalCase. Tags here match the live response. Go's case-insensitive
// JSON matching used to mask the discrepancy; pinning the tags to the actual
// wire format removes that quiet dependency.
type EnvelopeHeader struct {
	HttpStatusCode int             `json:"HttpStatusCode"`
	Errors         json.RawMessage `json:"Errors,omitempty"`
}

func (c *Credentials) Redact() Credentials {
	return Credentials{PackageID: c.PackageID, Username: "redacted", Password: "redacted"}
}

func (c *Client) redact(s string) string {
	if c.creds.Password != "" {
		s = strings.ReplaceAll(s, c.creds.Password, "redacted-pw")
	}
	if c.creds.Username != "" {
		s = strings.ReplaceAll(s, c.creds.Username, "redacted-user")
	}
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
