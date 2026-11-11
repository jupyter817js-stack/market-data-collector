package betfair

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

const DefaultCatalogueEndpoint = "https://api.betfair.com/exchange/betting/json-rpc/v1"

const catalogueMethodListMarketCatalogue = "SportsAPING/v1.0/listMarketCatalogue"

var defaultCatalogueMarketProjection = []string{
	"EVENT",
	"EVENT_TYPE",
	"COMPETITION",
	"RUNNER_DESCRIPTION",
	"MARKET_START_TIME",
}

type CatalogueClient struct {
	endpoint     string
	appKey       string
	sessionToken string
	httpClient   *http.Client
}

type CatalogueClientOptions struct {
	Endpoint     string
	AppKey       string
	SessionToken string
	HTTPClient   *http.Client
}

type CatalogueLookupOptions struct {
	MarketIDs []string
}

type CatalogueMarket struct {
	MarketID        string            `json:"marketId"`
	MarketName      string            `json:"marketName,omitempty"`
	MarketStartTime string            `json:"marketStartTime,omitempty"`
	Event           CatalogueNamedRef `json:"event,omitempty"`
	EventType       CatalogueNamedRef `json:"eventType,omitempty"`
	Competition     CatalogueNamedRef `json:"competition,omitempty"`
	Runners         []CatalogueRunner `json:"runners,omitempty"`
}

type CatalogueNamedRef struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type CatalogueRunner struct {
	SelectionID  int64             `json:"selectionId"`
	RunnerName   string            `json:"runnerName,omitempty"`
	Handicap     *float64          `json:"handicap,omitempty"`
	SortPriority *int              `json:"sortPriority,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

func DefaultCatalogueMarketProjection() []string {
	return append([]string(nil), defaultCatalogueMarketProjection...)
}

func NewCatalogueClient(options CatalogueClientOptions) (*CatalogueClient, error) {
	endpoint := strings.TrimSpace(options.Endpoint)
	if endpoint == "" {
		endpoint = DefaultCatalogueEndpoint
	}
	appKey := strings.TrimSpace(options.AppKey)
	sessionToken := strings.TrimSpace(options.SessionToken)
	if appKey == "" {
		return nil, fmt.Errorf("betfair catalogue client: app key is required")
	}
	if sessionToken == "" {
		return nil, fmt.Errorf("betfair catalogue client: session token is required")
	}
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &CatalogueClient{
		endpoint:     endpoint,
		appKey:       appKey,
		sessionToken: sessionToken,
		httpClient:   httpClient,
	}, nil
}

func (c *CatalogueClient) ListMarketCatalogue(ctx context.Context, options CatalogueLookupOptions) ([]CatalogueMarket, error) {
	if c == nil {
		return nil, fmt.Errorf("betfair catalogue client is nil")
	}
	marketIDs, err := ValidateExplicitMarketIDs(options.MarketIDs, 5)
	if err != nil {
		return nil, fmt.Errorf("betfair catalogue lookup: %w", err)
	}
	payload, err := json.Marshal(catalogueRPCRequest{
		JSONRPC: "2.0",
		Method:  catalogueMethodListMarketCatalogue,
		Params: catalogueRPCParams{
			Filter: catalogueMarketFilter{
				MarketIDs: marketIDs,
			},
			MarketProjection: DefaultCatalogueMarketProjection(),
			MaxResults:       len(marketIDs),
		},
		ID: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("betfair catalogue request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("betfair catalogue request: %s", c.safeError(err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Application", c.appKey)
	req.Header.Set("X-Authentication", c.sessionToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("betfair catalogue request: %s", c.safeError(err))
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("betfair catalogue response: %s", c.safeError(err))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("betfair catalogue response status %d: %s", resp.StatusCode, c.safeString(string(body)))
	}
	var decoded catalogueRPCResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("betfair catalogue response decode: %s", c.safeError(err))
	}
	if decoded.Error != nil {
		return nil, fmt.Errorf("betfair catalogue API error: code=%d message=%s data=%s",
			decoded.Error.Code,
			c.safeString(decoded.Error.Message),
			c.safeString(string(decoded.Error.Data)),
		)
	}
	return decoded.Result, nil
}

func (c *CatalogueClient) safeError(err error) string {
	if err == nil {
		return ""
	}
	return c.safeString(err.Error())
}

func (c *CatalogueClient) safeString(s string) string {
	return RedactLiveString(s, c.appKey, c.sessionToken)
}

func ValidateExplicitMarketIDs(values []string, max int) ([]string, error) {
	ids := trimmedStrings(values)
	if len(ids) == 0 {
		return nil, fmt.Errorf("at least one market-id is required")
	}
	if max <= 0 {
		max = 5
	}
	if len(ids) > max {
		return nil, fmt.Errorf("too many market IDs: max %d", max)
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "*" || strings.Contains(id, "*") {
			return nil, fmt.Errorf("wildcard market IDs are not allowed")
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, nil
}

type catalogueRPCRequest struct {
	JSONRPC string             `json:"jsonrpc"`
	Method  string             `json:"method"`
	Params  catalogueRPCParams `json:"params"`
	ID      int                `json:"id"`
}

type catalogueRPCParams struct {
	Filter           catalogueMarketFilter `json:"filter"`
	MarketProjection []string              `json:"marketProjection"`
	MaxResults       int                   `json:"maxResults"`
}

type catalogueMarketFilter struct {
	MarketIDs []string `json:"marketIds"`
}

type catalogueRPCResponse struct {
	JSONRPC string             `json:"jsonrpc"`
	Result  []CatalogueMarket  `json:"result"`
	Error   *catalogueRPCError `json:"error"`
	ID      int                `json:"id"`
}

type catalogueRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}
