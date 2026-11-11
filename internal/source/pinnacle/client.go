package pinnacle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Default user-agent and referer. The guest API gates on a hard-coded
// x-api-key (Pinnacle's public frontend key) plus mild header-shape checks.
const (
	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
	defaultReferer   = "https://www.pinnacle.com/"
)

// Client is a low-level, read-only HTTP client for Pinnacle's guest API.
type Client struct {
	http      *http.Client
	baseURL   string
	apiKey    string
	userAgent string
	referer   string
}

// ClientOption tunes the client.
type ClientOption func(*Client)

// NewClient builds a Client. baseURL should not have a trailing slash.
func NewClient(baseURL, apiKey string, timeout time.Duration, opts ...ClientOption) *Client {
	c := &Client{
		http:      &http.Client{Timeout: timeout},
		baseURL:   baseURL,
		apiKey:    apiKey,
		userAgent: defaultUserAgent,
		referer:   defaultReferer,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// WithUserAgent overrides the default UA.
func WithUserAgent(ua string) ClientOption { return func(c *Client) { c.userAgent = ua } }

// WithReferer overrides the default Referer.
func WithReferer(r string) ClientOption { return func(c *Client) { c.referer = r } }

// WithHTTPClient injects a custom *http.Client (timeouts, transport).
func WithHTTPClient(h *http.Client) ClientOption { return func(c *Client) { c.http = h } }

// Sports calls GET /0.1/sports.
func (c *Client) Sports(ctx context.Context) ([]Sport, error) {
	var out []Sport
	if err := c.getJSON(ctx, "/0.1/sports", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Leagues calls GET /0.1/sports/{sportId}/leagues. Returns currently-active
// leagues unless includeAll is true, in which case the historical full list
// (?all=true) is returned. Note: the all=true response can exceed 3MB.
func (c *Client) Leagues(ctx context.Context, sportID int, includeAll bool) ([]League, error) {
	path := fmt.Sprintf("/0.1/sports/%d/leagues", sportID)
	if includeAll {
		path += "?all=true"
	}
	var out []League
	if err := c.getJSON(ctx, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Matchups calls GET /0.1/leagues/{leagueId}/matchups?brandId=0. Returned
// list contains both regular matchups (type="matchup") and prop/special
// markets (type="special"); callers should filter as needed.
func (c *Client) Matchups(ctx context.Context, leagueID int) ([]Matchup, error) {
	path := fmt.Sprintf("/0.1/leagues/%d/matchups?brandId=0", leagueID)
	var out []Matchup
	if err := c.getJSON(ctx, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// SportMatchups calls GET /0.1/sports/{sportId}/matchups, returning every
// matchup across every league for that sport in a single call. This is the
// cheapest way to discover live matchups: the per-league endpoint requires
// us to enumerate the league set first (and any league we miss → live
// matchups in that league are invisible to us). Response is large (tens of
// MB for soccer) so call infrequently (~15-30s) and let the markets loop
// drive the fast cadence.
func (c *Client) SportMatchups(ctx context.Context, sportID int) ([]Matchup, error) {
	path := fmt.Sprintf("/0.1/sports/%d/matchups", sportID)
	var out []Matchup
	if err := c.getJSON(ctx, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Markets calls GET /0.1/matchups/{matchupId}/markets/related/straight.
func (c *Client) Markets(ctx context.Context, matchupID int64) ([]Market, error) {
	path := fmt.Sprintf("/0.1/matchups/%d/markets/related/straight", matchupID)
	var out []Market
	if err := c.getJSON(ctx, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// HTTPError is returned for non-2xx responses so callers can branch on status.
type HTTPError struct {
	StatusCode int
	URL        string
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("pinnacle: %s: http %d: %s", e.URL, e.StatusCode, truncate(e.Body, 200))
}

// IsRateLimited reports whether the error is a 429 or 5xx the caller can retry.
func IsRateLimited(err error) bool {
	if err == nil {
		return false
	}
	he, ok := err.(*HTTPError)
	if !ok {
		return false
	}
	return he.StatusCode == http.StatusTooManyRequests || he.StatusCode/100 == 5
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	u := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Referer", c.referer)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("pinnacle: %s: %w", u, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return &HTTPError{StatusCode: resp.StatusCode, URL: u, Body: string(body)}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("pinnacle: %s: decode: %w (body=%s)", u, err, truncate(string(body), 200))
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
