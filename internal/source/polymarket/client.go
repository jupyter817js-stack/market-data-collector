package polymarket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const DefaultGammaBaseURL = "https://gamma-api.polymarket.com"

type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

type Client struct {
	baseURL    string
	httpClient *http.Client
	retry      RetryConfig
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries: 2,
		BaseDelay:  250 * time.Millisecond,
		MaxDelay:   2 * time.Second,
	}
}

func NewClient(baseURL string, timeout time.Duration) *Client {
	return NewClientWithRetry(baseURL, timeout, DefaultRetryConfig())
}

func NewClientWithRetry(baseURL string, timeout time.Duration, retry RetryConfig) *Client {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		baseURL = DefaultGammaBaseURL
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{Timeout: timeout},
		retry:      retry.withDefaults(),
	}
}

func (r RetryConfig) withDefaults() RetryConfig {
	defaults := DefaultRetryConfig()
	if r.MaxRetries < 0 {
		r.MaxRetries = defaults.MaxRetries
	}
	if r.BaseDelay <= 0 {
		r.BaseDelay = defaults.BaseDelay
	}
	if r.MaxDelay <= 0 {
		r.MaxDelay = defaults.MaxDelay
	}
	if r.MaxDelay < r.BaseDelay {
		r.MaxDelay = r.BaseDelay
	}
	return r
}

func (c *Client) ListSports(ctx context.Context, opts ListOptions) ([]Sport, error) {
	var sports []Sport
	if err := c.getList(ctx, "/sports", opts, "sports", &sports); err != nil {
		return nil, err
	}
	return sports, nil
}

func (c *Client) ListEvents(ctx context.Context, opts ListOptions) ([]Event, error) {
	var events []Event
	if err := c.getList(ctx, "/events", opts, "events", &events); err != nil {
		return nil, err
	}
	return events, nil
}

func (c *Client) ListMarkets(ctx context.Context, opts ListOptions) ([]Market, error) {
	var markets []Market
	if err := c.getList(ctx, "/markets", opts, "markets", &markets); err != nil {
		return nil, err
	}
	return markets, nil
}

// GetEvent fetches one Polymarket Gamma event by its event ID. It is the
// narrowest read-only call needed to back the Phase 12F targeted sync, where
// the operator has already identified the exact event ID via the Phase 12E
// discovery diagnostic and wants to ingest only that event's metadata locally.
//
// Gamma's /events/{id} endpoint typically returns a single event object, but
// some Gamma deployments wrap it in {"event": {...}} or in a one-element
// array; getObject below handles all three shapes. This call never approves
// aliases, never auto-maps markets, and never invokes any order or account
// endpoint.
func (c *Client) GetEvent(ctx context.Context, eventID string) (Event, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return Event{}, fmt.Errorf("polymarket gamma event id is required")
	}
	var event Event
	path := "/events/" + url.PathEscape(eventID)
	if err := c.getObject(ctx, path, "event", &event); err != nil {
		return Event{}, err
	}
	return event, nil
}

// getObject runs the same retry/error pipeline as getList but accepts a
// response that may be either a bare object, an envelope ({"event": {...}}
// / {"data": {...}}), or a single-element array. It does not honor any
// query-string options because Gamma's by-id endpoints take no parameters.
func (c *Client) getObject(ctx context.Context, path, envelopeKey string, out any) error {
	endpoint := c.baseURL + path
	var lastErr error
	for attempt := 0; attempt <= c.retry.MaxRetries; attempt++ {
		resp, retryable, err := c.getOnceObject(ctx, endpoint, envelopeKey, out)
		if err == nil {
			return nil
		}
		lastErr = err
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if attempt == c.retry.MaxRetries || !retryable {
			return lastErr
		}
		if err := sleepContext(ctx, c.retryDelay(attempt, resp)); err != nil {
			return err
		}
	}
	return lastErr
}

func (c *Client) getOnceObject(ctx context.Context, endpoint, envelopeKey string, out any) (*http.Response, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pol-odds-collector/0.1")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, false, ctxErr
		}
		return nil, true, fmt.Errorf("polymarket gamma request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, retryableStatus(resp.StatusCode), err
	}
	if resp.StatusCode == http.StatusNotFound {
		return resp, false, fmt.Errorf("polymarket gamma %s: not found", endpoint)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp, retryableStatus(resp.StatusCode), fmt.Errorf("polymarket gamma status %d: %s", resp.StatusCode, truncate(string(body), 400))
	}
	if err := decodeObject(body, envelopeKey, out); err != nil {
		return resp, false, err
	}
	return resp, false, nil
}

func decodeObject(body []byte, envelopeKey string, out any) error {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return fmt.Errorf("polymarket gamma response is empty")
	}
	switch body[0] {
	case '[':
		// Some deployments wrap a by-id response in a single-element array.
		var items []json.RawMessage
		if err := json.Unmarshal(body, &items); err != nil {
			return err
		}
		if len(items) == 0 {
			return fmt.Errorf("polymarket gamma response array is empty")
		}
		return json.Unmarshal(items[0], out)
	case '{':
		// Detect an envelope BEFORE attempting the bare unmarshal: Polymarket
		// item types (Event, Market, ...) accept extra keys silently in their
		// custom UnmarshalJSON, so a fallback-on-error chain would treat the
		// wrapper as the object and quietly produce an empty result.
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(body, &envelope); err == nil {
			for _, key := range []string{envelopeKey, "data"} {
				if raw, ok := envelope[key]; ok && !isNull(raw) {
					trimmed := bytes.TrimSpace(raw)
					if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
						return decodeObject(raw, envelopeKey, out)
					}
				}
			}
		}
		return json.Unmarshal(body, out)
	default:
		return fmt.Errorf("polymarket gamma response is not a JSON object or array")
	}
}

func (c *Client) getList(ctx context.Context, path string, opts ListOptions, envelopeKey string, out any) error {
	endpoint, err := c.endpoint(path, opts)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt <= c.retry.MaxRetries; attempt++ {
		resp, retryable, err := c.getOnce(ctx, endpoint, envelopeKey, out)
		if err == nil {
			return nil
		}

		lastErr = err
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if attempt == c.retry.MaxRetries || !retryable {
			return lastErr
		}
		if err := sleepContext(ctx, c.retryDelay(attempt, resp)); err != nil {
			return err
		}
	}
	return lastErr
}

func (c *Client) endpoint(path string, opts ListOptions) (string, error) {
	endpoint, err := url.Parse(c.baseURL + path)
	if err != nil {
		return "", err
	}
	q := endpoint.Query()
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Offset > 0 {
		q.Set("offset", strconv.Itoa(opts.Offset))
	}
	if opts.Active != nil {
		q.Set("active", strconv.FormatBool(*opts.Active))
	}
	if opts.Closed != nil {
		q.Set("closed", strconv.FormatBool(*opts.Closed))
	}
	if opts.Archived != nil {
		q.Set("archived", strconv.FormatBool(*opts.Archived))
	}
	if order := strings.TrimSpace(opts.Order); order != "" {
		q.Set("order", order)
	}
	if opts.Ascending != nil {
		q.Set("ascending", strconv.FormatBool(*opts.Ascending))
	}
	if slug := strings.TrimSpace(opts.TagSlug); slug != "" {
		q.Set("tag_slug", slug)
	}
	if id := strings.TrimSpace(opts.TagID); id != "" {
		q.Set("tag_id", id)
	}
	if cat := strings.TrimSpace(opts.Category); cat != "" {
		q.Set("category", cat)
	}
	endpoint.RawQuery = q.Encode()
	return endpoint.String(), nil
}

func (c *Client) getOnce(ctx context.Context, endpoint, envelopeKey string, out any) (*http.Response, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pol-odds-collector/0.1")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, false, ctxErr
		}
		return nil, true, fmt.Errorf("polymarket gamma request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, retryableStatus(resp.StatusCode), err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp, retryableStatus(resp.StatusCode), fmt.Errorf("polymarket gamma status %d: %s", resp.StatusCode, truncate(string(body), 400))
	}
	if err := decodeList(body, envelopeKey, out); err != nil {
		return resp, false, err
	}
	return resp, false, nil
}

func decodeList(body []byte, envelopeKey string, out any) error {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return fmt.Errorf("polymarket gamma response is empty")
	}
	if body[0] == '[' {
		return json.Unmarshal(body, out)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	for _, key := range []string{envelopeKey, "data"} {
		raw, ok := envelope[key]
		if !ok || isNull(raw) {
			continue
		}
		return json.Unmarshal(raw, out)
	}
	return fmt.Errorf("polymarket gamma response missing %q list", envelopeKey)
}

func (c *Client) retryDelay(attempt int, resp *http.Response) time.Duration {
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		if delay, ok := retryAfterDelay(resp.Header.Get("Retry-After"), time.Now()); ok {
			return minDuration(delay, c.retry.MaxDelay)
		}
	}

	delay := c.retry.BaseDelay
	for i := 0; i < attempt; i++ {
		if delay >= c.retry.MaxDelay/2 {
			delay = c.retry.MaxDelay
			break
		}
		delay *= 2
	}
	delay = minDuration(delay, c.retry.MaxDelay)
	return jitter(delay)
}

func retryableStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || (statusCode >= 500 && statusCode <= 599)
}

func retryAfterDelay(raw string, now time.Time) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}
	t, err := http.ParseTime(raw)
	if err != nil {
		return 0, false
	}
	delay := t.Sub(now)
	if delay < 0 {
		return 0, true
	}
	return delay, true
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func jitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	half := delay / 2
	if half <= 0 {
		return delay
	}
	return half + time.Duration(rand.Int63n(int64(delay-half)+1))
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
