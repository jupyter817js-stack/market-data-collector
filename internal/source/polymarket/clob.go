package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const DefaultCLOBBaseURL = "https://clob.polymarket.com"

type CLOBClient struct {
	baseURL    string
	httpClient *http.Client
	retry      RetryConfig
}

func NewCLOBClient(baseURL string, timeout time.Duration) *CLOBClient {
	return NewCLOBClientWithRetry(baseURL, timeout, DefaultRetryConfig())
}

func NewCLOBClientWithRetry(baseURL string, timeout time.Duration, retry RetryConfig) *CLOBClient {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		baseURL = DefaultCLOBBaseURL
	}
	return &CLOBClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{Timeout: timeout},
		retry:      retry.withDefaults(),
	}
}

func (c *CLOBClient) GetPrice(ctx context.Context, tokenID string, side Side) (CLOBPrice, error) {
	var price CLOBPrice
	tokenID, err := cleanTokenID(tokenID)
	if err != nil {
		return price, err
	}
	side, err = cleanSide(side)
	if err != nil {
		return price, err
	}

	q := url.Values{}
	q.Set("token_id", tokenID)
	q.Set("side", string(side))
	if err := c.getJSON(ctx, "/price", q, &price); err != nil {
		return CLOBPrice{}, err
	}
	return price, nil
}

func (c *CLOBClient) GetPrices(ctx context.Context, requests []CLOBPriceRequest) (CLOBPrices, error) {
	var prices CLOBPrices
	if len(requests) == 0 {
		return prices, fmt.Errorf("at least one price request is required")
	}

	tokenIDs := make([]string, 0, len(requests))
	sides := make([]string, 0, len(requests))
	for _, request := range requests {
		tokenID, err := cleanTokenID(request.TokenID)
		if err != nil {
			return prices, err
		}
		side, err := cleanSide(request.Side)
		if err != nil {
			return prices, err
		}
		tokenIDs = append(tokenIDs, tokenID)
		sides = append(sides, string(side))
	}

	q := url.Values{}
	q.Set("token_ids", strings.Join(tokenIDs, ","))
	q.Set("sides", strings.Join(sides, ","))
	if err := c.getJSON(ctx, "/prices", q, &prices); err != nil {
		return CLOBPrices{}, err
	}
	return prices, nil
}

func (c *CLOBClient) GetOrderBook(ctx context.Context, tokenID string) (CLOBOrderBook, error) {
	var book CLOBOrderBook
	tokenID, err := cleanTokenID(tokenID)
	if err != nil {
		return book, err
	}

	q := url.Values{}
	q.Set("token_id", tokenID)
	if err := c.getJSON(ctx, "/book", q, &book); err != nil {
		return CLOBOrderBook{}, err
	}
	return book, nil
}

func (c *CLOBClient) GetMidpoint(ctx context.Context, tokenID string) (CLOBMidpoint, error) {
	var midpoint CLOBMidpoint
	tokenID, err := cleanTokenID(tokenID)
	if err != nil {
		return midpoint, err
	}

	q := url.Values{}
	q.Set("token_id", tokenID)
	if err := c.getJSON(ctx, "/midpoint", q, &midpoint); err != nil {
		return CLOBMidpoint{}, err
	}
	return midpoint, nil
}

func (c *CLOBClient) GetSpread(ctx context.Context, tokenID string) (CLOBSpread, error) {
	var spread CLOBSpread
	tokenID, err := cleanTokenID(tokenID)
	if err != nil {
		return spread, err
	}

	q := url.Values{}
	q.Set("token_id", tokenID)
	if err := c.getJSON(ctx, "/spread", q, &spread); err != nil {
		return CLOBSpread{}, err
	}
	return spread, nil
}

func (c *CLOBClient) getJSON(ctx context.Context, path string, q url.Values, out any) error {
	endpoint, err := c.endpoint(path, q)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt <= c.retry.MaxRetries; attempt++ {
		resp, retryable, err := c.getJSONOnce(ctx, endpoint, out)
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

func (c *CLOBClient) endpoint(path string, q url.Values) (string, error) {
	endpoint, err := url.Parse(c.baseURL + path)
	if err != nil {
		return "", err
	}
	endpoint.RawQuery = q.Encode()
	return endpoint.String(), nil
}

func (c *CLOBClient) getJSONOnce(ctx context.Context, endpoint string, out any) (*http.Response, bool, error) {
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
		return nil, true, fmt.Errorf("polymarket clob request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, retryableStatus(resp.StatusCode), err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp, retryableStatus(resp.StatusCode), fmt.Errorf("polymarket clob status %d: %s", resp.StatusCode, truncate(string(body), 400))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return resp, false, err
	}
	return resp, false, nil
}

func (c *CLOBClient) retryDelay(attempt int, resp *http.Response) time.Duration {
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

func cleanTokenID(tokenID string) (string, error) {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return "", fmt.Errorf("token_id is required")
	}
	return tokenID, nil
}

func cleanSide(side Side) (Side, error) {
	side = Side(strings.ToUpper(strings.TrimSpace(string(side))))
	switch side {
	case SideBuy, SideSell:
		return side, nil
	default:
		return "", fmt.Errorf("side must be BUY or SELL")
	}
}
