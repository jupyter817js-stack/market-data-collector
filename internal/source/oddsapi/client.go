package oddsapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
)

type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

type Client struct {
	apiKey     string
	baseURL    string
	regions    string
	markets    string
	bookmakers string
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

func NewClient(apiKey, baseURL, regions, markets, bookmakers string, timeout time.Duration) *Client {
	return NewClientWithRetry(apiKey, baseURL, regions, markets, bookmakers, timeout, DefaultRetryConfig())
}

func NewClientWithRetry(apiKey, baseURL, regions, markets, bookmakers string, timeout time.Duration, retry RetryConfig) *Client {
	return &Client{
		apiKey:     apiKey,
		baseURL:    strings.TrimRight(baseURL, "/"),
		regions:    regions,
		markets:    markets,
		bookmakers: bookmakers,
		httpClient: &http.Client{Timeout: timeout},
		retry:      retry.withDefaults(),
	}
}

func (c *Client) Name() string { return model.SourceOddsAPI }

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

func (c *Client) FetchOdds(ctx context.Context, sportKey string) (model.FetchBatch, error) {
	endpoint, err := url.Parse(c.baseURL + "/v4/sports/" + url.PathEscape(sportKey) + "/odds/")
	if err != nil {
		return model.FetchBatch{}, err
	}

	q := endpoint.Query()
	q.Set("apiKey", c.apiKey)
	q.Set("markets", c.markets)
	q.Set("oddsFormat", "decimal")
	q.Set("dateFormat", "iso")
	if c.bookmakers != "" {
		q.Set("bookmakers", c.bookmakers)
	} else {
		q.Set("regions", c.regions)
	}
	endpoint.RawQuery = q.Encode()

	// Keep API keys out of logs and database metadata.
	safeURL := *endpoint
	safeQ := safeURL.Query()
	safeQ.Set("apiKey", "redacted")
	safeURL.RawQuery = safeQ.Encode()

	var lastBatch model.FetchBatch
	var lastErr error
	for attempt := 0; attempt <= c.retry.MaxRetries; attempt++ {
		batch, resp, retryable, err := c.fetchOnce(ctx, endpoint.String(), safeURL.String(), sportKey)
		if err == nil {
			return batch, nil
		}

		lastBatch = batch
		lastErr = err
		if ctxErr := ctx.Err(); ctxErr != nil {
			return lastBatch, ctxErr
		}
		if attempt == c.retry.MaxRetries || !retryable {
			return lastBatch, lastErr
		}
		if err := sleepContext(ctx, c.retryDelay(attempt, resp)); err != nil {
			return lastBatch, err
		}
	}
	return lastBatch, lastErr
}

func (c *Client) fetchOnce(ctx context.Context, endpointURL, safeURL, sportKey string) (model.FetchBatch, *http.Response, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointURL, nil)
	if err != nil {
		return model.FetchBatch{}, nil, false, err
	}
	req.Header.Set("User-Agent", "pol-odds-collector/0.1")

	resp, err := c.httpClient.Do(req)
	receivedAt := time.Now().UTC()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return model.FetchBatch{}, nil, false, ctxErr
		}
		return model.FetchBatch{}, nil, true, fmt.Errorf("odds api request failed: %s", c.redact(err.Error()))
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return model.FetchBatch{}, resp, retryableStatus(resp.StatusCode), err
	}

	batch := model.FetchBatch{
		Source:     c.Name(),
		SportKey:   sportKey,
		RequestURL: safeURL,
		StatusCode: resp.StatusCode,
		Headers:    interestingHeaders(resp.Header),
		Payload:    json.RawMessage(body),
		PayloadSHA: sha256Hex(body),
		ReceivedAt: receivedAt,
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return batch, resp, retryableStatus(resp.StatusCode), fmt.Errorf("odds api status %d: %s", resp.StatusCode, c.redact(truncate(string(body), 400)))
	}

	if err := json.Unmarshal(body, &batch.Events); err != nil {
		return batch, resp, false, fmt.Errorf("decode odds api response: %w", err)
	}
	return batch, resp, false, nil
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

func (c *Client) redact(s string) string {
	if c.apiKey == "" {
		return s
	}
	s = strings.ReplaceAll(s, c.apiKey, "redacted")
	s = strings.ReplaceAll(s, url.QueryEscape(c.apiKey), "redacted")
	s = strings.ReplaceAll(s, url.PathEscape(c.apiKey), "redacted")
	return s
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

func interestingHeaders(h http.Header) map[string]string {
	keys := []string{
		"x-requests-remaining",
		"x-requests-used",
		"x-requests-last",
		"content-type",
		"date",
	}
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		if value := h.Get(key); value != "" {
			out[key] = value
		}
	}
	return out
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
