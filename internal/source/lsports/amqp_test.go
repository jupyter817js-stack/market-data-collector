package lsports

import (
	"encoding/json"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestAMQPConfigURLEscapesCredentials(t *testing.T) {
	cfg := AMQPConfig{
		Host:     "stm-inplay.lsports.eu",
		Port:     5672,
		Username: "sample-user",
		Password: "p@ss/w:ord",
		VHost:    "StmInPlay",
		Queue:    "_sample_queue_",
	}
	got := cfg.URL()
	// amqp scheme, vhost in path
	want := "amqp://sample-user:p%40ss%2Fw%3Aord@stm-inplay.lsports.eu:5672/StmInPlay"
	if got != want {
		t.Fatalf("URL mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestWithDefaults(t *testing.T) {
	out := AMQPConfig{Username: "u", Password: "p", Queue: "_sample_queue_"}.WithDefaults()
	if out.Host != DefaultAMQPHost || out.Port != DefaultAMQPPort || out.VHost != DefaultAMQPVHost {
		t.Fatalf("defaults not applied: %+v", out)
	}
}

func TestObservationKindForType(t *testing.T) {
	cases := []struct{ in int; want string }{
		{1, "fixture_meta"},
		{2, "livescore"},
		{3, "market"},
		{31, "keepalive"},
		{32, "heartbeat"},
		{35, "settlement"},
		{999, "other"},
	}
	for _, c := range cases {
		if got := ObservationKindForType(c.in); got != c.want {
			t.Fatalf("ObservationKindForType(%d): got %q want %q", c.in, got, c.want)
		}
	}
}

func TestParseAMQPMessageExtractsHeader(t *testing.T) {
	body := []byte(`{"Header":{"Type":2,"MsgGuid":"abc","ServerTimestamp":1779942574271},"Body":[]}`)
	d := amqp.Delivery{Body: body, DeliveryTag: 7}
	msg, err := ParseAMQPMessage(d)
	if err != nil {
		t.Fatalf("ParseAMQPMessage: %v", err)
	}
	if msg.Header.Type != 2 || msg.Header.MsgGuid != "abc" {
		t.Fatalf("header parse failed: %+v", msg.Header)
	}
	if msg.ObservationKind != "livescore" {
		t.Fatalf("ObservationKind mismatch: %q", msg.ObservationKind)
	}
	if msg.DeliveryTag != 7 {
		t.Fatalf("delivery tag mismatch: %d", msg.DeliveryTag)
	}
	if !msg.Header.SourcePublishTime().IsZero() && msg.Header.SourcePublishTime().Year() != 2026 {
		t.Fatalf("source publish time year: %v", msg.Header.SourcePublishTime())
	}
	// raw bytes preserved
	var probe struct {
		Header struct {
			Type int
		} `json:"Header"`
	}
	if err := json.Unmarshal(msg.RawPayload, &probe); err != nil || probe.Header.Type != 2 {
		t.Fatalf("raw payload preservation failed: err=%v probe=%+v", err, probe)
	}
}

func TestFixtureIDFromMessageObjectAndArray(t *testing.T) {
	objBody := []byte(`{"Header":{},"Body":{"FixtureId":19012395,"Scoreboard":{}}}`)
	if got := FixtureIDFromMessage(objBody); got != 19012395 {
		t.Fatalf("object form: got %d want 19012395", got)
	}
	arrBody := []byte(`{"Header":{},"Body":[{"FixtureId":19012395}]}`)
	if got := FixtureIDFromMessage(arrBody); got != 19012395 {
		t.Fatalf("array form: got %d want 19012395", got)
	}
	nestedBody := []byte(`{"Header":{},"Body":{"Fixture":{"Id":19012395}}}`)
	if got := FixtureIDFromMessage(nestedBody); got != 19012395 {
		t.Fatalf("nested form: got %d want 19012395", got)
	}
	noFix := []byte(`{"Header":{},"Body":{"unrelated":"value"}}`)
	if got := FixtureIDFromMessage(noFix); got != 0 {
		t.Fatalf("none case: got %d want 0", got)
	}
}

func TestAMQPConsumerRedactsCredentialsInError(t *testing.T) {
	c := &AMQPConsumer{cfg: AMQPConfig{Username: "alice", Password: "sekret-pw"}}
	got := c.redact(errAMQP("failed to authenticate sekret-pw for user alice"))
	if got == nil {
		t.Fatalf("expected error")
	}
	s := got.Error()
	if contains(s, "sekret-pw") || contains(s, "alice") {
		t.Fatalf("credential leak: %q", s)
	}
}

func TestSourcePublishTimeZero(t *testing.T) {
	h := AMQPMessageHeader{}
	if !h.SourcePublishTime().IsZero() {
		t.Fatalf("expected zero time for zero ServerTimestamp")
	}
	h.ServerTimestamp = 1
	if h.SourcePublishTime().IsZero() {
		t.Fatalf("expected non-zero parsed seconds for ServerTimestamp=1")
	}
	_ = time.Time{}
}

// helpers ------------------------------------------------------------

type errAMQPstr string

func (e errAMQPstr) Error() string { return string(e) }
func errAMQP(s string) error       { return errAMQPstr(s) }

func contains(s, sub string) bool { return indexOf(s, sub) >= 0 }
