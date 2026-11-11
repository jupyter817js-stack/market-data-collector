package betfair

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	liveOperationAuthentication     = "authentication"
	liveOperationMarketSubscription = "marketSubscription"
	liveOperationHeartbeat          = "heartbeat"
)

const (
	MarketDataFieldMarketDefinition = "EX_MARKET_DEF"
	MarketDataFieldBestOffers       = "EX_BEST_OFFERS"
	MarketDataFieldLastTradedPrice  = "EX_LTP"
	MarketDataFieldTraded           = "EX_TRADED"
)

var defaultMarketDataFields = []string{
	MarketDataFieldMarketDefinition,
	MarketDataFieldBestOffers,
	MarketDataFieldLastTradedPrice,
	MarketDataFieldTraded,
}

const (
	redactedAppKey       = "[REDACTED_BETFAIR_APP_KEY]"
	redactedSessionToken = "[REDACTED_BETFAIR_SESSION_TOKEN]"
)

type MarketSubscriptionRequest struct {
	MarketIDs    []string
	MarketTypes  []string
	Fields       []string
	LadderLevels int
	HeartbeatMS  int
	ConflateMS   int
	Clock        string
	InitialClock string
}

func (r MarketSubscriptionRequest) withClocks(clock, initialClock string) MarketSubscriptionRequest {
	out := r
	out.Clock = strings.TrimSpace(clock)
	out.InitialClock = strings.TrimSpace(initialClock)
	return out
}

func (r MarketSubscriptionRequest) valid() bool {
	return len(trimmedStrings(r.MarketIDs)) > 0 || len(trimmedStrings(r.MarketTypes)) > 0
}

func DefaultMarketDataFields() []string {
	return append([]string(nil), defaultMarketDataFields...)
}

type authenticationRequestMessage struct {
	Op      string `json:"op"`
	ID      int    `json:"id"`
	AppKey  string `json:"appKey"`
	Session string `json:"session"`
}

type marketSubscriptionRequestMessage struct {
	Op               string               `json:"op"`
	ID               int                  `json:"id"`
	MarketFilter     liveMarketFilter     `json:"marketFilter"`
	MarketDataFilter liveMarketDataFilter `json:"marketDataFilter,omitempty"`
	HeartbeatMS      int                  `json:"heartbeatMs,omitempty"`
	ConflateMS       int                  `json:"conflateMs,omitempty"`
	Clock            string               `json:"clk,omitempty"`
	InitialClock     string               `json:"initialClk,omitempty"`
}

type liveMarketFilter struct {
	MarketIDs   []string `json:"marketIds,omitempty"`
	MarketTypes []string `json:"marketTypes,omitempty"`
}

type liveMarketDataFilter struct {
	Fields       []string `json:"fields,omitempty"`
	LadderLevels int      `json:"ladderLevels,omitempty"`
}

type heartbeatRequestMessage struct {
	Op string `json:"op"`
	ID int    `json:"id"`
}

func BuildAuthenticationMessage(id int, appKey, sessionToken string) ([]byte, error) {
	if strings.TrimSpace(appKey) == "" {
		return nil, fmt.Errorf("betfair live authentication message: app key is required")
	}
	if strings.TrimSpace(sessionToken) == "" {
		return nil, fmt.Errorf("betfair live authentication message: session token is required")
	}
	return marshalLiveMessage(authenticationRequestMessage{
		Op:      liveOperationAuthentication,
		ID:      id,
		AppKey:  appKey,
		Session: sessionToken,
	})
}

func BuildMarketSubscriptionMessage(id int, req MarketSubscriptionRequest) ([]byte, error) {
	req.MarketIDs = trimmedStrings(req.MarketIDs)
	req.MarketTypes = trimmedStrings(req.MarketTypes)
	req.Fields = normalizeMarketDataFields(req.Fields)
	req.Clock = strings.TrimSpace(req.Clock)
	req.InitialClock = strings.TrimSpace(req.InitialClock)
	if !req.valid() {
		return nil, fmt.Errorf("betfair live market subscription message: market ids or market types are required")
	}
	if req.LadderLevels < 0 {
		return nil, fmt.Errorf("betfair live market subscription message: ladder levels must be non-negative")
	}
	if req.HeartbeatMS < 0 || req.ConflateMS < 0 {
		return nil, fmt.Errorf("betfair live market subscription message: heartbeat and conflate intervals must be non-negative")
	}
	return marshalLiveMessage(marketSubscriptionRequestMessage{
		Op: liveOperationMarketSubscription,
		ID: id,
		MarketFilter: liveMarketFilter{
			MarketIDs:   req.MarketIDs,
			MarketTypes: req.MarketTypes,
		},
		MarketDataFilter: liveMarketDataFilter{
			Fields:       req.Fields,
			LadderLevels: req.LadderLevels,
		},
		HeartbeatMS:  req.HeartbeatMS,
		ConflateMS:   req.ConflateMS,
		Clock:        req.Clock,
		InitialClock: req.InitialClock,
	})
}

func BuildHeartbeatMessage(id int) ([]byte, error) {
	return marshalLiveMessage(heartbeatRequestMessage{
		Op: liveOperationHeartbeat,
		ID: id,
	})
}

func normalizeMarketDataFields(fields []string) []string {
	fields = trimmedStrings(fields)
	if len(fields) == 0 {
		return DefaultMarketDataFields()
	}
	return fields
}

func marshalLiveMessage(v any) ([]byte, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal betfair live message: %w", err)
	}
	payload = append(payload, '\r', '\n')
	return payload, nil
}

func RedactLivePayload(payload []byte) []byte {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return []byte(RedactLiveString(string(payload)))
	}
	redactLiveValue(value)
	redacted, err := json.Marshal(value)
	if err != nil {
		return []byte(RedactLiveString(string(payload)))
	}
	return redacted
}

func RedactLiveString(s string, secrets ...string) string {
	out := s
	for i, secret := range secrets {
		secret = strings.TrimSpace(secret)
		if secret == "" {
			continue
		}
		marker := redactedAppKey
		if i == 1 {
			marker = redactedSessionToken
		}
		out = strings.ReplaceAll(out, secret, marker)
	}
	return out
}

func redactLiveValue(v any) {
	switch value := v.(type) {
	case map[string]any:
		for key, item := range value {
			switch strings.ToLower(key) {
			case "appkey", "app_key":
				value[key] = redactedAppKey
			case "session", "sessiontoken", "session_token":
				value[key] = redactedSessionToken
			default:
				redactLiveValue(item)
			}
		}
	case []any:
		for _, item := range value {
			redactLiveValue(item)
		}
	}
}

func trimmedStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}
