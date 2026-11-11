package betfair

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type Operation string

const (
	OperationConnection   Operation = "connection"
	OperationStatus       Operation = "status"
	OperationHeartbeat    Operation = "heartbeat"
	OperationMarketChange Operation = "mcm"
	OperationOrderChange  Operation = "ocm"
)

type Message struct {
	Op               Operation
	ID               *int
	ConnectionID     string
	StatusCode       string
	ErrorCode        string
	ErrorMessage     string
	ConnectionClosed *bool
	Clock            string
	InitialClock     string
	PublishTimeMS    *int64
	ChangeType       string
	ConflateMS       *int
	HeartbeatMS      *int
	MarketChanges    []MarketChange
	OrderChanges     []json.RawMessage
	UnknownFields    map[string]json.RawMessage
	Raw              json.RawMessage
}

type MarketChange struct {
	ID               string
	Image            *bool
	MarketDefinition *MarketDefinition
	RunnerChanges    []RunnerChange
	UnknownFields    map[string]json.RawMessage
	Raw              json.RawMessage
}

type MarketDefinition struct {
	Status        string                     `json:"status,omitempty"`
	EventID       string                     `json:"eventId,omitempty"`
	EventName     string                     `json:"eventName,omitempty"`
	EventTypeID   string                     `json:"eventTypeId,omitempty"`
	MarketTime    string                     `json:"marketTime,omitempty"`
	Name          string                     `json:"name,omitempty"`
	InPlay        *bool                      `json:"inPlay,omitempty"`
	Runners       []RunnerDefinition         `json:"runners,omitempty"`
	UnknownFields map[string]json.RawMessage `json:"-"`
	Raw           json.RawMessage            `json:"-"`
}

type RunnerDefinition struct {
	ID            int64                      `json:"id,omitempty"`
	Handicap      *float64                   `json:"hc,omitempty"`
	Name          string                     `json:"name,omitempty"`
	Status        string                     `json:"status,omitempty"`
	SortPriority  *int                       `json:"sortPriority,omitempty"`
	UnknownFields map[string]json.RawMessage `json:"-"`
	Raw           json.RawMessage            `json:"-"`
}

type RunnerChange struct {
	SelectionID         int64
	Handicap            *float64
	Image               *bool
	LastTradedPrice     *float64
	TradedVolume        *float64
	BestAvailableToBack []LevelLadderChange
	BestAvailableToLay  []LevelLadderChange
	AvailableToBack     []PriceLadderChange
	AvailableToLay      []PriceLadderChange
	Traded              []PriceLadderChange
	UnknownFields       map[string]json.RawMessage
	Raw                 json.RawMessage
}

type LevelLadderChange struct {
	Level int
	Price float64
	Size  float64
	Raw   json.RawMessage
}

type PriceLadderChange struct {
	Price float64
	Size  float64
	Raw   json.RawMessage
}

func (m Message) IsControl() bool {
	switch m.Op {
	case OperationConnection, OperationStatus, OperationHeartbeat:
		return true
	default:
		return false
	}
}

func DecodeMessage(raw []byte) (Message, error) {
	raw = append([]byte(nil), raw...)
	if !json.Valid(raw) {
		return Message{}, fmt.Errorf("decode betfair stream message: malformed JSON")
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Message{}, fmt.Errorf("decode betfair stream message: %w", err)
	}
	if fields == nil {
		return Message{}, fmt.Errorf("decode betfair stream message: expected JSON object")
	}

	var op Operation
	if err := decodeRequired(fields, "op", &op); err != nil {
		return Message{}, err
	}

	msg := Message{
		Op:            op,
		UnknownFields: unknownFields(fields, knownMessageFields),
		Raw:           raw,
	}
	_ = decodeOptional(fields, "id", &msg.ID)
	_ = decodeOptional(fields, "connectionId", &msg.ConnectionID)
	_ = decodeOptional(fields, "statusCode", &msg.StatusCode)
	_ = decodeOptional(fields, "errorCode", &msg.ErrorCode)
	_ = decodeOptional(fields, "errorMessage", &msg.ErrorMessage)
	_ = decodeOptional(fields, "connectionClosed", &msg.ConnectionClosed)
	_ = decodeOptional(fields, "clk", &msg.Clock)
	_ = decodeOptional(fields, "initialClk", &msg.InitialClock)
	_ = decodeOptional(fields, "pt", &msg.PublishTimeMS)
	_ = decodeOptional(fields, "ct", &msg.ChangeType)
	_ = decodeOptional(fields, "conflateMs", &msg.ConflateMS)
	_ = decodeOptional(fields, "heartbeatMs", &msg.HeartbeatMS)
	if err := decodeMarketChanges(fields["mc"], &msg.MarketChanges); err != nil {
		return Message{}, err
	}
	if err := decodeOptional(fields, "oc", &msg.OrderChanges); err != nil {
		return Message{}, fmt.Errorf("decode betfair order changes: %w", err)
	}
	return msg, nil
}

var knownMessageFields = map[string]struct{}{
	"op":               {},
	"id":               {},
	"connectionId":     {},
	"statusCode":       {},
	"errorCode":        {},
	"errorMessage":     {},
	"connectionClosed": {},
	"clk":              {},
	"initialClk":       {},
	"pt":               {},
	"ct":               {},
	"conflateMs":       {},
	"heartbeatMs":      {},
	"mc":               {},
	"oc":               {},
}

var knownMarketChangeFields = map[string]struct{}{
	"id":               {},
	"img":              {},
	"marketDefinition": {},
	"rc":               {},
}

var knownMarketDefinitionFields = map[string]struct{}{
	"status":      {},
	"eventId":     {},
	"eventName":   {},
	"eventTypeId": {},
	"marketTime":  {},
	"name":        {},
	"inPlay":      {},
	"runners":     {},
}

var knownRunnerDefinitionFields = map[string]struct{}{
	"id":           {},
	"hc":           {},
	"name":         {},
	"status":       {},
	"sortPriority": {},
}

var knownRunnerChangeFields = map[string]struct{}{
	"id":   {},
	"hc":   {},
	"img":  {},
	"ltp":  {},
	"tv":   {},
	"batb": {},
	"batl": {},
	"atb":  {},
	"atl":  {},
	"trd":  {},
}

func decodeRequired[T any](fields map[string]json.RawMessage, name string, dest *T) error {
	raw, ok := fields[name]
	if !ok {
		return fmt.Errorf("decode betfair stream message: missing required field %q", name)
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		return fmt.Errorf("decode betfair stream message field %q: %w", name, err)
	}
	return nil
}

func decodeOptional[T any](fields map[string]json.RawMessage, name string, dest *T) error {
	raw, ok := fields[name]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		return fmt.Errorf("field %q: %w", name, err)
	}
	return nil
}

func decodeMarketChanges(raw json.RawMessage, dest *[]MarketChange) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return fmt.Errorf("decode betfair market changes: %w", err)
	}
	out := make([]MarketChange, 0, len(entries))
	for _, entry := range entries {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(entry, &fields); err != nil {
			return fmt.Errorf("decode betfair market change: %w", err)
		}
		change := MarketChange{
			UnknownFields: unknownFields(fields, knownMarketChangeFields),
			Raw:           append([]byte(nil), entry...),
		}
		if err := decodeOptional(fields, "id", &change.ID); err != nil {
			return fmt.Errorf("decode betfair market change id: %w", err)
		}
		if err := decodeOptional(fields, "img", &change.Image); err != nil {
			return fmt.Errorf("decode betfair market change image flag: %w", err)
		}
		if err := decodeOptional(fields, "marketDefinition", &change.MarketDefinition); err != nil {
			return fmt.Errorf("decode betfair market definition: %w", err)
		}
		if err := decodeOptional(fields, "rc", &change.RunnerChanges); err != nil {
			return fmt.Errorf("decode betfair runner changes: %w", err)
		}
		out = append(out, change)
	}
	*dest = out
	return nil
}

func (m *MarketDefinition) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	m.Raw = copyRaw(b)
	m.UnknownFields = unknownFields(fields, knownMarketDefinitionFields)
	if err := decodeOptional(fields, "status", &m.Status); err != nil {
		return fmt.Errorf("market definition status: %w", err)
	}
	if err := decodeOptional(fields, "eventId", &m.EventID); err != nil {
		return fmt.Errorf("market definition eventId: %w", err)
	}
	if err := decodeOptional(fields, "eventName", &m.EventName); err != nil {
		return fmt.Errorf("market definition eventName: %w", err)
	}
	if err := decodeOptional(fields, "eventTypeId", &m.EventTypeID); err != nil {
		return fmt.Errorf("market definition eventTypeId: %w", err)
	}
	if err := decodeOptional(fields, "marketTime", &m.MarketTime); err != nil {
		return fmt.Errorf("market definition marketTime: %w", err)
	}
	if err := decodeOptional(fields, "name", &m.Name); err != nil {
		return fmt.Errorf("market definition name: %w", err)
	}
	if err := decodeOptional(fields, "inPlay", &m.InPlay); err != nil {
		return fmt.Errorf("market definition inPlay: %w", err)
	}
	if err := decodeOptional(fields, "runners", &m.Runners); err != nil {
		return fmt.Errorf("market definition runners: %w", err)
	}
	return nil
}

func (r *RunnerDefinition) UnmarshalJSON(b []byte) error {
	fields, err := objectFields(b)
	if err != nil {
		return err
	}
	r.Raw = copyRaw(b)
	r.UnknownFields = unknownFields(fields, knownRunnerDefinitionFields)
	if err := decodeOptional(fields, "id", &r.ID); err != nil {
		return fmt.Errorf("runner definition id: %w", err)
	}
	if err := decodeOptional(fields, "hc", &r.Handicap); err != nil {
		return fmt.Errorf("runner definition handicap: %w", err)
	}
	if err := decodeOptional(fields, "name", &r.Name); err != nil {
		return fmt.Errorf("runner definition name: %w", err)
	}
	if err := decodeOptional(fields, "status", &r.Status); err != nil {
		return fmt.Errorf("runner definition status: %w", err)
	}
	if err := decodeOptional(fields, "sortPriority", &r.SortPriority); err != nil {
		return fmt.Errorf("runner definition sortPriority: %w", err)
	}
	return nil
}

func (r *RunnerChange) UnmarshalJSON(b []byte) error {
	raw := bytes.TrimSpace(b)
	if len(raw) == 0 || isJSONNull(raw) {
		return fmt.Errorf("runner change must be a JSON object")
	}
	r.Raw = copyRaw(raw)
	if raw[0] == '[' {
		return r.unmarshalCompactArray(raw)
	}
	fields, err := objectFields(raw)
	if err != nil {
		return err
	}
	r.UnknownFields = unknownFields(fields, knownRunnerChangeFields)
	if err := decodeOptional(fields, "id", &r.SelectionID); err != nil {
		return fmt.Errorf("runner change id: %w", err)
	}
	if err := decodeOptional(fields, "hc", &r.Handicap); err != nil {
		return fmt.Errorf("runner change handicap: %w", err)
	}
	if err := decodeOptional(fields, "img", &r.Image); err != nil {
		return fmt.Errorf("runner change image flag: %w", err)
	}
	if err := decodeOptional(fields, "ltp", &r.LastTradedPrice); err != nil {
		return fmt.Errorf("runner change last traded price: %w", err)
	}
	if err := decodeOptional(fields, "tv", &r.TradedVolume); err != nil {
		return fmt.Errorf("runner change traded volume: %w", err)
	}
	if err := decodeOptional(fields, "batb", &r.BestAvailableToBack); err != nil {
		return fmt.Errorf("runner change best available to back: %w", err)
	}
	if err := decodeOptional(fields, "batl", &r.BestAvailableToLay); err != nil {
		return fmt.Errorf("runner change best available to lay: %w", err)
	}
	if err := decodeOptional(fields, "atb", &r.AvailableToBack); err != nil {
		return fmt.Errorf("runner change available to back: %w", err)
	}
	if err := decodeOptional(fields, "atl", &r.AvailableToLay); err != nil {
		return fmt.Errorf("runner change available to lay: %w", err)
	}
	if err := decodeOptional(fields, "trd", &r.Traded); err != nil {
		return fmt.Errorf("runner change traded ladder: %w", err)
	}
	return nil
}

func (r *RunnerChange) unmarshalCompactArray(raw []byte) error {
	var fields []json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if len(fields) < 1 {
		return fmt.Errorf("compact runner change must include selection id")
	}
	if err := json.Unmarshal(fields[0], &r.SelectionID); err != nil {
		return fmt.Errorf("compact runner change selection id: %w", err)
	}
	if len(fields) > 1 && !isJSONNull(fields[1]) {
		var ltp float64
		if err := json.Unmarshal(fields[1], &ltp); err != nil {
			return fmt.Errorf("compact runner change last traded price: %w", err)
		}
		r.LastTradedPrice = &ltp
	}
	return nil
}

func (l *LevelLadderChange) UnmarshalJSON(b []byte) error {
	raw := bytes.TrimSpace(b)
	l.Raw = copyRaw(raw)
	var fields []json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if len(fields) != 3 {
		return fmt.Errorf("expected [level,price,size], got %d fields", len(fields))
	}
	if err := json.Unmarshal(fields[0], &l.Level); err != nil {
		return fmt.Errorf("level: %w", err)
	}
	if err := json.Unmarshal(fields[1], &l.Price); err != nil {
		return fmt.Errorf("price: %w", err)
	}
	if err := json.Unmarshal(fields[2], &l.Size); err != nil {
		return fmt.Errorf("size: %w", err)
	}
	if l.Level < 0 {
		return fmt.Errorf("level must be non-negative")
	}
	if l.Price < 0 {
		return fmt.Errorf("price must be non-negative")
	}
	if l.Size < 0 {
		return fmt.Errorf("size must be non-negative")
	}
	return nil
}

func (l *PriceLadderChange) UnmarshalJSON(b []byte) error {
	raw := bytes.TrimSpace(b)
	l.Raw = copyRaw(raw)
	var fields []json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if len(fields) != 2 {
		return fmt.Errorf("expected [price,size], got %d fields", len(fields))
	}
	if err := json.Unmarshal(fields[0], &l.Price); err != nil {
		return fmt.Errorf("price: %w", err)
	}
	if err := json.Unmarshal(fields[1], &l.Size); err != nil {
		return fmt.Errorf("size: %w", err)
	}
	if l.Price < 0 {
		return fmt.Errorf("price must be non-negative")
	}
	if l.Size < 0 {
		return fmt.Errorf("size must be non-negative")
	}
	return nil
}

func unknownFields(fields map[string]json.RawMessage, known map[string]struct{}) map[string]json.RawMessage {
	unknown := map[string]json.RawMessage{}
	for name, raw := range fields {
		if _, ok := known[name]; ok {
			continue
		}
		unknown[name] = append([]byte(nil), raw...)
	}
	if len(unknown) == 0 {
		return nil
	}
	return unknown
}

func objectFields(b []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("expected JSON object")
	}
	return fields, nil
}

func copyRaw(b []byte) json.RawMessage {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func isJSONNull(raw []byte) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
