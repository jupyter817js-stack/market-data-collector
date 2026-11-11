package polymarket

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ListOptions is the read-only query surface for Polymarket Gamma list
// endpoints. All filter fields below are optional. The defaults preserve the
// original behavior (limit/offset only) so existing callers are unaffected.
//
// Phase 12E added the lifecycle/order/tag/category fields to support targeted
// read-only discovery diagnostics (cmd/polymarket-sync --diagnose-coverage).
// Without these, the default Gamma response paginates from the oldest historical
// markets, which is why the operator saw only closed 2021 NBA markets despite
// scanning 1500 markets across 20 pages.
type ListOptions struct {
	Limit     int
	Offset    int
	Active    *bool  // closed=false / active=true
	Closed    *bool  // closed=true returns only closed markets
	Archived  *bool  // archived=true returns only archived markets
	Order     string // e.g. "endDate", "startDate", "id"
	Ascending *bool  // ascending=false sorts descending
	TagSlug   string // filter by tag slug (e.g. "sports", "nba")
	TagID     string // filter by tag id
	Category  string // filter by category
}

type Sport struct {
	ID       string
	Slug     string
	Title    string
	Category string
	Tags     []Tag
	Raw      json.RawMessage
}

type Event struct {
	ID        string
	Slug      string
	Title     string
	StartTime *time.Time
	EndTime   *time.Time
	Active    *bool
	Closed    *bool
	Archived  *bool
	Sport     string
	Category  string
	Tags      []Tag
	Markets   []Market
	Raw       json.RawMessage
}

type Market struct {
	ID          string
	ConditionID string
	Slug        string
	Question    string
	Title       string
	Category    string
	StartTime   *time.Time
	EndTime     *time.Time
	Active      *bool
	Closed      *bool
	Archived    *bool
	Tokens      []Token
	Raw         json.RawMessage
}

type Token struct {
	TokenID      string
	OutcomeID    string
	OutcomeName  string
	OutcomeIndex int
	Raw          json.RawMessage
}

type Tag struct {
	ID    string
	Slug  string
	Label string
	Raw   json.RawMessage
}

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type CLOBPriceRequest struct {
	TokenID string
	Side    Side
}

type CLOBPrice struct {
	Price float64
	Raw   json.RawMessage
}

type CLOBPrices struct {
	Prices map[string]map[Side]float64
	Raw    json.RawMessage
}

type CLOBOrderBook struct {
	Market         string
	AssetID        string
	Timestamp      string
	Hash           string
	Bids           []CLOBBookLevel
	Asks           []CLOBBookLevel
	MinOrderSize   string
	TickSize       string
	NegativeRisk   bool
	LastTradePrice string
	Raw            json.RawMessage
}

type CLOBBookLevel struct {
	Price string
	Size  string
	Raw   json.RawMessage
}

type CLOBMidpoint struct {
	MidPrice string
	Raw      json.RawMessage
}

type CLOBSpread struct {
	Spread string
	Raw    json.RawMessage
}

func (s *Sport) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	s.Raw = copyRaw(b)
	s.ID = stringField(fields, "id", "sportId", "sport_id")
	s.Slug = stringField(fields, "slug")
	s.Title = stringField(fields, "title", "label", "name")
	s.Category = stringField(fields, "category")
	s.Tags, err = tagSliceField(fields, "tags")
	return err
}

func (e *Event) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	e.Raw = copyRaw(b)
	e.ID = stringField(fields, "id", "eventId", "event_id")
	e.Slug = stringField(fields, "slug")
	e.Title = stringField(fields, "title", "question")
	e.StartTime, err = timeField(fields, "startTime", "startDate", "eventStartTime", "eventDate")
	if err != nil {
		return err
	}
	e.EndTime, err = timeField(fields, "endTime", "endDate", "closeTime", "closedTime")
	if err != nil {
		return err
	}
	e.Active, err = boolPtrField(fields, "active")
	if err != nil {
		return err
	}
	e.Closed, err = boolPtrField(fields, "closed")
	if err != nil {
		return err
	}
	e.Archived, err = boolPtrField(fields, "archived")
	if err != nil {
		return err
	}
	e.Sport = stringField(fields, "sport", "sportKey", "sportsMarketType")
	e.Category = stringField(fields, "category", "subcategory")
	e.Tags, err = tagSliceField(fields, "tags")
	if err != nil {
		return err
	}
	e.Markets, err = marketSliceField(fields, "markets")
	return err
}

func (m *Market) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	m.Raw = copyRaw(b)
	m.ID = stringField(fields, "id", "marketId", "market_id")
	m.ConditionID = stringField(fields, "conditionId", "condition_id")
	m.Slug = stringField(fields, "slug")
	m.Question = stringField(fields, "question")
	m.Title = stringField(fields, "title", "question")
	m.Category = stringField(fields, "category")
	m.StartTime, err = timeField(fields, "startTime", "startDate", "eventStartTime", "eventDate")
	if err != nil {
		return err
	}
	m.EndTime, err = timeField(fields, "endTime", "endDate", "closeTime", "closedTime")
	if err != nil {
		return err
	}
	m.Active, err = boolPtrField(fields, "active")
	if err != nil {
		return err
	}
	m.Closed, err = boolPtrField(fields, "closed")
	if err != nil {
		return err
	}
	m.Archived, err = boolPtrField(fields, "archived")
	if err != nil {
		return err
	}

	outcomes, err := stringSliceField(fields, "outcomes")
	if err != nil {
		return err
	}
	tokenIDs, err := stringSliceField(fields, "clobTokenIds", "clobTokenIDs", "tokenIds", "token_ids")
	if err != nil {
		return err
	}
	m.Tokens, err = tokenSliceField(fields, "tokens", "outcomeTokens")
	if err != nil {
		return err
	}
	if len(m.Tokens) == 0 {
		m.Tokens = tokensFromOutcomes(tokenIDs, outcomes)
	}
	return nil
}

func (t *Token) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	t.Raw = copyRaw(b)
	t.TokenID = stringField(fields, "token_id", "tokenId", "clobTokenId", "id")
	t.OutcomeID = stringField(fields, "outcome_id", "outcomeId")
	t.OutcomeName = stringField(fields, "outcome", "name", "outcomeName")
	t.OutcomeIndex = intField(fields, "outcomeIndex", "index")
	return nil
}

func (t *Tag) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	t.Raw = copyRaw(b)
	t.ID = stringField(fields, "id", "tagId", "tag_id")
	t.Slug = stringField(fields, "slug")
	t.Label = stringField(fields, "label", "name", "title")
	return nil
}

func (p *CLOBPrice) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	p.Raw = copyRaw(b)
	p.Price = floatField(fields, "price")
	return nil
}

func (p *CLOBPrices) UnmarshalJSON(b []byte) error {
	var prices map[string]map[Side]float64
	if err := json.Unmarshal(b, &prices); err != nil {
		return err
	}
	p.Raw = copyRaw(b)
	p.Prices = prices
	return nil
}

func (b *CLOBOrderBook) UnmarshalJSON(raw []byte) error {
	fields, err := objectFields(raw)
	if err != nil {
		return err
	}
	b.Raw = copyRaw(raw)
	b.Market = stringField(fields, "market")
	b.AssetID = stringField(fields, "asset_id", "assetId")
	b.Timestamp = stringField(fields, "timestamp")
	b.Hash = stringField(fields, "hash")
	b.Bids, err = bookLevelSliceField(fields, "bids")
	if err != nil {
		return err
	}
	b.Asks, err = bookLevelSliceField(fields, "asks")
	if err != nil {
		return err
	}
	b.MinOrderSize = stringField(fields, "min_order_size", "minOrderSize")
	b.TickSize = stringField(fields, "tick_size", "tickSize")
	b.NegativeRisk = boolField(fields, "neg_risk", "negRisk")
	b.LastTradePrice = stringField(fields, "last_trade_price", "lastTradePrice")
	return nil
}

func (l *CLOBBookLevel) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	l.Raw = copyRaw(b)
	l.Price = stringField(fields, "price")
	l.Size = stringField(fields, "size")
	return nil
}

func (m *CLOBMidpoint) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	m.Raw = copyRaw(b)
	m.MidPrice = stringField(fields, "mid_price", "midPrice")
	return nil
}

func (s *CLOBSpread) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	s.Raw = copyRaw(b)
	s.Spread = stringField(fields, "spread")
	return nil
}

func objectFields(b []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func copyRaw(b []byte) json.RawMessage {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func stringField(fields map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok || isNull(raw) {
			continue
		}
		if len(raw) > 0 && raw[0] == '"' {
			var s string
			if json.Unmarshal(raw, &s) == nil {
				return strings.TrimSpace(s)
			}
			continue
		}
		return strings.TrimSpace(string(raw))
	}
	return ""
}

func intField(fields map[string]json.RawMessage, keys ...string) int {
	raw := stringField(fields, keys...)
	n, _ := strconv.Atoi(raw)
	return n
}

func floatField(fields map[string]json.RawMessage, keys ...string) float64 {
	raw := stringField(fields, keys...)
	n, _ := strconv.ParseFloat(raw, 64)
	return n
}

func boolField(fields map[string]json.RawMessage, keys ...string) bool {
	raw := stringField(fields, keys...)
	v, _ := strconv.ParseBool(raw)
	return v
}

func boolPtrField(fields map[string]json.RawMessage, keys ...string) (*bool, error) {
	raw, ok := firstRaw(fields, keys...)
	if !ok || isNull(raw) {
		return nil, nil
	}
	rawString := stringField(fields, keys...)
	if rawString == "" {
		return nil, nil
	}
	v, err := strconv.ParseBool(rawString)
	if err != nil {
		return nil, fmt.Errorf("parse bool %q: %w", rawString, err)
	}
	return &v, nil
}

func timeField(fields map[string]json.RawMessage, keys ...string) (*time.Time, error) {
	raw := stringField(fields, keys...)
	if raw == "" {
		return nil, nil
	}
	ts, err := ParseGammaTimestamp(raw)
	if err != nil {
		return nil, err
	}
	return &ts, nil
}

// gammaTimestampLayouts is the prioritized list of layouts ParseGammaTimestamp
// tries against a Polymarket Gamma timestamp string. Strict RFC3339 is checked
// first because it is the documented shape and most fields use it. The
// space-separated variants are also accepted because the Gamma API has been
// observed returning shapes like "2026-04-01 07:32:39+00" for some event/market
// timestamps; the events diagnostic in cmd/polymarket-sync hit this in
// production. Numeric-offset shapes without a colon (e.g. "+00", "-0500") are
// covered by Go's "-07" / "-0700" specifiers.
//
// All layouts are UTC-normalized by the caller.
var gammaTimestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02T15:04:05-07:00",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05.999999999-0700",
	"2006-01-02T15:04:05-0700",
	"2006-01-02T15:04:05.999999999-07",
	"2006-01-02T15:04:05-07",
	"2006-01-02T15:04:05.999999999Z",
	"2006-01-02T15:04:05Z",
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05-07:00",
	"2006-01-02 15:04:05.999999999-0700",
	"2006-01-02 15:04:05-0700",
	"2006-01-02 15:04:05.999999999-07",
	"2006-01-02 15:04:05-07",
	"2006-01-02 15:04:05.999999999Z",
	"2006-01-02 15:04:05Z",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// ParseGammaTimestamp parses a Polymarket Gamma timestamp string. Gamma
// returns at least two distinct shapes in the wild — strict RFC3339
// ("2026-04-01T07:32:39Z" / "2026-04-01T07:32:39+00:00") and a space-separated
// shape with truncated offset ("2026-04-01 07:32:39+00"). This helper accepts
// both, plus a few defensive variants, and returns a UTC-normalized time.
//
// It is exported so the diagnostic and any future read-only Gamma
// post-processing share the same parsing contract. Bare dates without a time
// component are accepted as midnight UTC. Anything that does not match any
// supported layout returns an error that includes the offending raw string.
//
// This helper does not introduce trading, execution, auto-mapping, order/
// account streams, wallet/private-key handling, or any external API call.
func ParseGammaTimestamp(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, fmt.Errorf("parse polymarket gamma timestamp: empty string")
	}
	for _, layout := range gammaTimestampLayouts {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("parse polymarket gamma timestamp %q: no supported layout matched (accepted: RFC3339, RFC3339 with offset, \"YYYY-MM-DD HH:MM:SS+ZZ\", \"YYYY-MM-DD HH:MM:SS+ZZZZ\", \"YYYY-MM-DD HH:MM:SS+ZZ:ZZ\", \"YYYY-MM-DD HH:MM:SSZ\", \"YYYY-MM-DD HH:MM:SS\", \"YYYY-MM-DD\")", raw)
}

func stringSliceField(fields map[string]json.RawMessage, keys ...string) ([]string, error) {
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok || isNull(raw) {
			continue
		}
		return parseStringSlice(raw)
	}
	return nil, nil
}

func parseStringSlice(raw json.RawMessage) ([]string, error) {
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, nil
		}
		if strings.HasPrefix(s, "[") {
			return parseStringSlice(json.RawMessage(s))
		}
		return []string{s}, nil
	}

	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = bytes.TrimSpace(item)
		if len(item) == 0 || isNull(item) {
			continue
		}
		if item[0] == '"' {
			var s string
			if err := json.Unmarshal(item, &s); err != nil {
				return nil, err
			}
			out = append(out, strings.TrimSpace(s))
			continue
		}
		out = append(out, strings.TrimSpace(string(item)))
	}
	return out, nil
}

func tagSliceField(fields map[string]json.RawMessage, keys ...string) ([]Tag, error) {
	raw, ok := firstRaw(fields, keys...)
	if !ok || isNull(raw) {
		return nil, nil
	}
	var tags []Tag
	if err := json.Unmarshal(raw, &tags); err != nil {
		return nil, err
	}
	return tags, nil
}

func marketSliceField(fields map[string]json.RawMessage, keys ...string) ([]Market, error) {
	raw, ok := firstRaw(fields, keys...)
	if !ok || isNull(raw) {
		return nil, nil
	}
	var markets []Market
	if err := json.Unmarshal(raw, &markets); err != nil {
		return nil, err
	}
	return markets, nil
}

func tokenSliceField(fields map[string]json.RawMessage, keys ...string) ([]Token, error) {
	raw, ok := firstRaw(fields, keys...)
	if !ok || isNull(raw) {
		return nil, nil
	}
	var tokens []Token
	if err := json.Unmarshal(raw, &tokens); err != nil {
		return nil, err
	}
	return tokens, nil
}

func bookLevelSliceField(fields map[string]json.RawMessage, keys ...string) ([]CLOBBookLevel, error) {
	raw, ok := firstRaw(fields, keys...)
	if !ok || isNull(raw) {
		return nil, nil
	}
	var levels []CLOBBookLevel
	if err := json.Unmarshal(raw, &levels); err != nil {
		return nil, err
	}
	return levels, nil
}

func firstRaw(fields map[string]json.RawMessage, keys ...string) (json.RawMessage, bool) {
	for _, key := range keys {
		raw, ok := fields[key]
		if ok {
			return raw, true
		}
	}
	return nil, false
}

func tokensFromOutcomes(tokenIDs, outcomes []string) []Token {
	n := len(tokenIDs)
	if len(outcomes) > n {
		n = len(outcomes)
	}
	tokens := make([]Token, 0, n)
	for i := 0; i < n; i++ {
		token := Token{OutcomeIndex: i}
		if i < len(tokenIDs) {
			token.TokenID = tokenIDs[i]
		}
		if i < len(outcomes) {
			token.OutcomeName = outcomes[i]
		}
		tokens = append(tokens, token)
	}
	return tokens
}

func isNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
