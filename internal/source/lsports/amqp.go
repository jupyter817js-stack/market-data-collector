package lsports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQP TRADE360 in-play push consumer. Connects to LSPORTS RabbitMQ broker
// (stm-inplay.lsports.eu:5672, vhost StmInPlay, no TLS — confirmed by
// LSPORTS Support 2026-05-27). The queue name is the package ID wrapped
// in underscores, e.g. "_4044_". Auth uses the same username/password as
// the REST API.

const (
	DefaultAMQPHost  = "stm-inplay.lsports.eu"
	DefaultAMQPPort  = 5672
	DefaultAMQPVHost = "StmInPlay"
)

// MessageType integer codes from
// docs.lsports.eu/u/trade/integration/message-structure/message-types-overview
const (
	MessageTypeFixtureMetadata = 1
	MessageTypeLivescore       = 2
	MessageTypeMarket          = 3
	MessageTypeKeepAlive       = 31
	MessageTypeHeartbeat       = 32
	MessageTypeSettlement      = 35
)

// AMQPConfig is the operator-supplied connection profile.
type AMQPConfig struct {
	Host      string
	Port      int
	Username  string
	Password  string
	VHost     string
	Queue     string        // literal "_<PackageID>_" — supplied by caller
	Heartbeat time.Duration // 0 = client default (10s)
}

// WithDefaults fills in standard LSPORTS defaults for missing fields.
func (c AMQPConfig) WithDefaults() AMQPConfig {
	if c.Host == "" {
		c.Host = DefaultAMQPHost
	}
	if c.Port == 0 {
		c.Port = DefaultAMQPPort
	}
	if c.VHost == "" {
		c.VHost = DefaultAMQPVHost
	}
	return c
}

// URL builds the amqp:// URL used by amqp091.Dial. Username and password
// are URL-escaped so special characters (e.g. besdss.com which contains a
// dot) are handled correctly. NO TLS — the LSPORTS stm-inplay endpoint
// listens plaintext on 5672.
func (c AMQPConfig) URL() string {
	u := &url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   "/" + c.VHost,
	}
	return u.String()
}

// AMQPMessage is one parsed message off the queue. RawPayload preserves the
// full JSON body for downstream persistence; the typed fields below are a
// best-effort projection extracted from common LSPORTS envelopes.
type AMQPMessage struct {
	Header          AMQPMessageHeader
	RawPayload      json.RawMessage
	DeliveryTag     uint64
	ReceivedAt      time.Time
	ObservationKind string // 'livescore' | 'market' | 'settlement' | 'fixture_meta' | 'heartbeat' | 'keepalive' | 'other'
}

// AMQPMessageHeader is the LSPORTS push-message envelope header. Field shapes
// observed in TRADE360 AMQP messages mirror the snapshot envelope: a Type
// integer, MsgGuid, and a millisecond ServerTimestamp. The MessageId may
// also be present.
type AMQPMessageHeader struct {
	Type            int    `json:"Type"`
	MsgGuid         string `json:"MsgGuid"`
	ServerTimestamp int64  `json:"ServerTimestamp"`
	MessageId       int64  `json:"MessageId,omitempty"`
}

// SourcePublishTime returns the parsed ServerTimestamp as UTC, auto-detecting
// seconds/ms/us units. Mirrors SnapshotHeader.SnapshotServerTime.
func (h AMQPMessageHeader) SourcePublishTime() time.Time {
	switch {
	case h.ServerTimestamp == 0:
		return time.Time{}
	case h.ServerTimestamp > 1e14:
		return time.UnixMicro(h.ServerTimestamp).UTC()
	case h.ServerTimestamp > 1e11:
		return time.UnixMilli(h.ServerTimestamp).UTC()
	default:
		return time.Unix(h.ServerTimestamp, 0).UTC()
	}
}

// ObservationKindForType maps the integer Type code from the LSPORTS message
// envelope to our observation_kind enum on lsports_inplay_observations.
func ObservationKindForType(msgType int) string {
	switch msgType {
	case MessageTypeFixtureMetadata:
		return "fixture_meta"
	case MessageTypeLivescore:
		return "livescore"
	case MessageTypeMarket:
		return "market"
	case MessageTypeKeepAlive:
		return "keepalive"
	case MessageTypeHeartbeat:
		return "heartbeat"
	case MessageTypeSettlement:
		return "settlement"
	default:
		return "other"
	}
}

// ParseAMQPMessage decodes one AMQP delivery into our typed AMQPMessage.
// The raw bytes are preserved verbatim in RawPayload so persistence keeps
// the full payload for post-hoc field discovery.
func ParseAMQPMessage(d amqp.Delivery) (AMQPMessage, error) {
	var env struct {
		Header AMQPMessageHeader `json:"Header"`
	}
	if err := json.Unmarshal(d.Body, &env); err != nil {
		return AMQPMessage{}, fmt.Errorf("parse amqp envelope: %w", err)
	}
	return AMQPMessage{
		Header:          env.Header,
		RawPayload:      json.RawMessage(d.Body),
		DeliveryTag:     d.DeliveryTag,
		ReceivedAt:      time.Now().UTC(),
		ObservationKind: ObservationKindForType(env.Header.Type),
	}, nil
}

// FixtureIDFromMessage tries to extract a FixtureId from the message Body.
// LSPORTS push messages (observed 2026-05-28 from a live AMQP capture) wrap
// the fixture in an Events[] array:
//
//	{
//	  "Body": {
//	    "Events": [
//	      { "FixtureId": 19023704, "Fixture": {...}, "Livescore": {...} }
//	    ]
//	  }
//	}
//
// This is the canonical shape for type=1/2/3/35 push messages. Other
// shapes are kept as fallbacks for non-Events messages (e.g. Settlement,
// FixtureMetadata may differ).
func FixtureIDFromMessage(raw []byte) int64 {
	var env struct {
		Body json.RawMessage `json:"Body"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return 0
	}

	// Canonical shape: Body.Events[].FixtureId
	var eventsObj struct {
		Events []struct {
			FixtureID int64 `json:"FixtureId"`
			Fixture   *struct {
				ID int64 `json:"Id"`
			} `json:"Fixture"`
		} `json:"Events"`
	}
	if err := json.Unmarshal(env.Body, &eventsObj); err == nil && len(eventsObj.Events) > 0 {
		if eventsObj.Events[0].FixtureID > 0 {
			return eventsObj.Events[0].FixtureID
		}
		if eventsObj.Events[0].Fixture != nil && eventsObj.Events[0].Fixture.ID > 0 {
			return eventsObj.Events[0].Fixture.ID
		}
	}

	// Fallback shapes (other message types may use these).
	type probe struct {
		FixtureID int64 `json:"FixtureId"`
		Fixture   *struct {
			ID int64 `json:"Id"`
		} `json:"Fixture"`
	}
	var single probe
	if err := json.Unmarshal(env.Body, &single); err == nil {
		if single.FixtureID > 0 {
			return single.FixtureID
		}
		if single.Fixture != nil && single.Fixture.ID > 0 {
			return single.Fixture.ID
		}
	}
	var arr []probe
	if err := json.Unmarshal(env.Body, &arr); err == nil && len(arr) > 0 {
		if arr[0].FixtureID > 0 {
			return arr[0].FixtureID
		}
		if arr[0].Fixture != nil && arr[0].Fixture.ID > 0 {
			return arr[0].Fixture.ID
		}
	}
	return 0
}

// AMQPConsumer wraps an amqp091.Connection + Channel against the LSPORTS
// in-play push queue. Reconnect is deliberately the caller's responsibility:
// the consumer surfaces a notification channel on connection loss and the
// caller (cmd/lsports-amqp-capture) decides backoff/retry. This keeps the
// consumer stateless w.r.t. reconnection policy.
type AMQPConsumer struct {
	cfg  AMQPConfig
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewAMQPConsumer(cfg AMQPConfig) *AMQPConsumer {
	return &AMQPConsumer{cfg: cfg.WithDefaults()}
}

// Connect dials the broker, opens a channel, and sets QoS for our consumer.
// Returns an error wrapped from amqp091 if the broker is unreachable or
// credentials are wrong; the operator should log and retry with backoff.
func (c *AMQPConsumer) Connect(ctx context.Context) error {
	cfg := amqp.Config{}
	if c.cfg.Heartbeat > 0 {
		cfg.Heartbeat = c.cfg.Heartbeat
	}
	conn, err := amqp.DialConfig(c.cfg.URL(), cfg)
	if err != nil {
		return fmt.Errorf("amqp dial: %w", c.redact(err))
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("amqp channel: %w", err)
	}
	// Prefetch a small batch — we process serially and persist before ack.
	if err := ch.Qos(50, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("amqp qos: %w", err)
	}
	c.conn = conn
	c.ch = ch
	return nil
}

// CloseNotify returns the channel-close notification (1-shot). The caller
// can wait on this to detect connection loss and trigger reconnect.
func (c *AMQPConsumer) CloseNotify() chan *amqp.Error {
	if c.conn == nil {
		return nil
	}
	return c.conn.NotifyClose(make(chan *amqp.Error, 1))
}

// Consume starts delivering messages from the queue. Caller MUST ack after
// successful persistence. Auto-ack is off so a crash mid-write doesn't
// silently drop messages.
func (c *AMQPConsumer) Consume(ctx context.Context, consumerTag string) (<-chan amqp.Delivery, error) {
	if c.ch == nil {
		return nil, errors.New("amqp consumer not connected")
	}
	if c.cfg.Queue == "" {
		return nil, errors.New("amqp consumer: Queue not set")
	}
	deliveries, err := c.ch.ConsumeWithContext(ctx,
		c.cfg.Queue,
		consumerTag, // consumer
		false,       // autoAck — false: we ack after persistence
		false,       // exclusive
		false,       // noLocal
		false,       // noWait
		nil,         // args
	)
	if err != nil {
		return nil, fmt.Errorf("amqp consume: %w", err)
	}
	return deliveries, nil
}

func (c *AMQPConsumer) Close() error {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// redact removes the username/password from the error string so credentials
// don't leak into logs.
func (c *AMQPConsumer) redact(err error) error {
	if err == nil {
		return nil
	}
	s := err.Error()
	if c.cfg.Password != "" {
		s = replaceAll(s, c.cfg.Password, "redacted-pw")
		s = replaceAll(s, url.QueryEscape(c.cfg.Password), "redacted-pw")
	}
	if c.cfg.Username != "" {
		s = replaceAll(s, c.cfg.Username, "redacted-user")
		s = replaceAll(s, url.QueryEscape(c.cfg.Username), "redacted-user")
	}
	return errors.New(s)
}

func replaceAll(s, old, new string) string {
	if old == "" {
		return s
	}
	out := ""
	for {
		i := indexOf(s, old)
		if i < 0 {
			return out + s
		}
		out += s[:i] + new
		s = s[i+len(old):]
	}
}

func indexOf(s, substr string) int {
	n, m := len(s), len(substr)
	if m == 0 || m > n {
		return -1
	}
	for i := 0; i+m <= n; i++ {
		if s[i:i+m] == substr {
			return i
		}
	}
	return -1
}
