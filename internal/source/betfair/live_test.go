package betfair

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	liveTestAppKey  = "APP-SECRET-123"
	liveTestSession = "SESSION-SECRET-456"
)

func TestLiveConnectorDisabledByDefault(t *testing.T) {
	connector := NewLiveConnector(nil, LiveConnectorOptions{})
	_, err := connector.Run(context.Background())
	if !errors.Is(err, ErrLiveConnectorDisabled) {
		t.Fatalf("disabled connector error=%v want ErrLiveConnectorDisabled", err)
	}
	if connector.State() != LiveStateDisabled {
		t.Fatalf("disabled connector state=%q want disabled", connector.State())
	}
}

func TestLiveConnectorValidation(t *testing.T) {
	base := liveTestOptions()
	tests := []struct {
		name   string
		mutate func(*LiveConnectorOptions)
		want   string
	}{
		{"missing app key", func(o *LiveConnectorOptions) { o.AppKey = "" }, "app key is required"},
		{"missing session token", func(o *LiveConnectorOptions) { o.SessionToken = "" }, "session token is required"},
		{"missing subscription", func(o *LiveConnectorOptions) { o.Subscription = MarketSubscriptionRequest{} }, "subscription"},
		{"missing transport", func(o *LiveConnectorOptions) {}, "transport is required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := base
			tc.mutate(&opts)
			var transport LiveTransport
			if tc.name != "missing transport" {
				transport = &fakeLiveTransport{conns: []*fakeLiveConn{newFakeLiveConn()}}
			}
			connector := NewLiveConnector(transport, opts)
			_, err := connector.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation error=%v want containing %q", err, tc.want)
			}
			assertNoLiveSecrets(t, err.Error())
		})
	}
}

func TestLiveAuthenticationMessageConstructionAndRedaction(t *testing.T) {
	payload, err := BuildAuthenticationMessage(7, liveTestAppKey, liveTestSession)
	if err != nil {
		t.Fatalf("build auth message: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(trimCRLF(payload), &got); err != nil {
		t.Fatalf("decode auth payload: %v", err)
	}
	if got["op"] != liveOperationAuthentication || got["appKey"] != liveTestAppKey || got["session"] != liveTestSession {
		t.Fatalf("auth payload=%s", payload)
	}
	redacted := string(RedactLivePayload(payload))
	assertNoLiveSecrets(t, redacted)
	if !strings.Contains(redacted, redactedAppKey) || !strings.Contains(redacted, redactedSessionToken) {
		t.Fatalf("redacted auth payload=%s", redacted)
	}
}

func TestLiveMarketSubscriptionMessageConstruction(t *testing.T) {
	payload, err := BuildMarketSubscriptionMessage(8, MarketSubscriptionRequest{
		MarketIDs:    []string{" 1.111 ", ""},
		MarketTypes:  []string{"MATCH_ODDS"},
		Fields:       DefaultMarketDataFields(),
		LadderLevels: 3,
		HeartbeatMS:  5000,
		ConflateMS:   100,
		Clock:        "clk-1",
		InitialClock: "initial-1",
	})
	if err != nil {
		t.Fatalf("build subscription message: %v", err)
	}
	var got struct {
		Op           string `json:"op"`
		ID           int    `json:"id"`
		MarketFilter struct {
			MarketIDs   []string `json:"marketIds"`
			MarketTypes []string `json:"marketTypes"`
		} `json:"marketFilter"`
		MarketDataFilter struct {
			Fields       []string `json:"fields"`
			LadderLevels int      `json:"ladderLevels"`
		} `json:"marketDataFilter"`
		HeartbeatMS  int    `json:"heartbeatMs"`
		ConflateMS   int    `json:"conflateMs"`
		Clock        string `json:"clk"`
		InitialClock string `json:"initialClk"`
	}
	if err := json.Unmarshal(trimCRLF(payload), &got); err != nil {
		t.Fatalf("decode subscription payload: %v", err)
	}
	if got.Op != liveOperationMarketSubscription || got.ID != 8 {
		t.Fatalf("subscription op/id=%q/%d", got.Op, got.ID)
	}
	if len(got.MarketFilter.MarketIDs) != 1 || got.MarketFilter.MarketIDs[0] != "1.111" {
		t.Fatalf("market ids=%v want [1.111]", got.MarketFilter.MarketIDs)
	}
	if got.MarketDataFilter.LadderLevels != 3 || got.HeartbeatMS != 5000 || got.ConflateMS != 100 {
		t.Fatalf("subscription filters=%+v heartbeat=%d conflate=%d", got.MarketDataFilter, got.HeartbeatMS, got.ConflateMS)
	}
	if !reflect.DeepEqual(got.MarketDataFilter.Fields, DefaultMarketDataFields()) {
		t.Fatalf("subscription fields=%v want %v", got.MarketDataFilter.Fields, DefaultMarketDataFields())
	}
	if got.Clock != "clk-1" || got.InitialClock != "initial-1" {
		t.Fatalf("clocks=%q/%q", got.Clock, got.InitialClock)
	}
}

func TestLiveMarketSubscriptionMessageDefaultsMarketDataFields(t *testing.T) {
	payload, err := BuildMarketSubscriptionMessage(9, MarketSubscriptionRequest{
		MarketIDs:    []string{"1.111"},
		LadderLevels: 3,
	})
	if err != nil {
		t.Fatalf("build subscription message with default fields: %v", err)
	}
	var got struct {
		MarketDataFilter struct {
			Fields       []string `json:"fields"`
			LadderLevels int      `json:"ladderLevels"`
		} `json:"marketDataFilter"`
	}
	if err := json.Unmarshal(trimCRLF(payload), &got); err != nil {
		t.Fatalf("decode subscription payload: %v", err)
	}
	if !reflect.DeepEqual(got.MarketDataFilter.Fields, DefaultMarketDataFields()) {
		t.Fatalf("default subscription fields=%v want %v", got.MarketDataFilter.Fields, DefaultMarketDataFields())
	}
	if got.MarketDataFilter.LadderLevels != 3 {
		t.Fatalf("default subscription ladderLevels=%d want 3", got.MarketDataFilter.LadderLevels)
	}
}

func TestLiveMarketSubscriptionMessageUsesStreamAPIFieldNames(t *testing.T) {
	payload, err := BuildMarketSubscriptionMessage(10, MarketSubscriptionRequest{
		MarketIDs:    []string{"1.257710194"},
		Fields:       DefaultMarketDataFields(),
		LadderLevels: 3,
	})
	if err != nil {
		t.Fatalf("build explicit subscription message: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(trimCRLF(payload), &got); err != nil {
		t.Fatalf("decode explicit subscription payload: %v", err)
	}
	if got["op"] != liveOperationMarketSubscription || got["id"] != float64(10) {
		t.Fatalf("subscription op/id=%+v", got)
	}
	if _, ok := got["market_filter"]; ok {
		t.Fatalf("subscription used snake_case market_filter: %+v", got)
	}
	if _, ok := got["market_data_filter"]; ok {
		t.Fatalf("subscription used snake_case market_data_filter: %+v", got)
	}
	marketFilter, ok := got["marketFilter"].(map[string]any)
	if !ok {
		t.Fatalf("subscription missing marketFilter: %+v", got)
	}
	if _, ok := marketFilter["market_ids"]; ok {
		t.Fatalf("subscription used snake_case market_ids: %+v", marketFilter)
	}
	marketIDs, ok := marketFilter["marketIds"].([]any)
	if !ok || len(marketIDs) != 1 || marketIDs[0] != "1.257710194" {
		t.Fatalf("subscription marketIds=%+v", marketFilter["marketIds"])
	}
	if _, ok := marketFilter["marketTypes"]; ok {
		t.Fatalf("explicit market ID subscription included broad marketTypes: %+v", marketFilter)
	}
	marketDataFilter, ok := got["marketDataFilter"].(map[string]any)
	if !ok {
		t.Fatalf("subscription missing marketDataFilter: %+v", got)
	}
	if _, ok := marketDataFilter["ladder_levels"]; ok {
		t.Fatalf("subscription used snake_case ladder_levels: %+v", marketDataFilter)
	}
	fields, ok := marketDataFilter["fields"].([]any)
	if !ok || len(fields) != len(DefaultMarketDataFields()) {
		t.Fatalf("subscription fields=%+v", marketDataFilter["fields"])
	}
	for _, required := range DefaultMarketDataFields() {
		found := false
		for _, field := range fields {
			if field == required {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("subscription fields=%+v missing %s", fields, required)
		}
	}
	if marketDataFilter["ladderLevels"] != float64(3) {
		t.Fatalf("subscription ladderLevels=%+v want 3", marketDataFilter["ladderLevels"])
	}
	text := string(payload)
	if strings.Contains(strings.ToLower(text), "order") || strings.Contains(strings.ToLower(text), "account") || strings.Contains(text, "*") {
		t.Fatalf("subscription included disallowed scope: %s", text)
	}
}

func TestLiveConnectorHappyPathFakeTransport(t *testing.T) {
	conn := newFakeLiveConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS","clk":"auth-clk","initialClk":"initial-auth"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"sub-clk"}`,
		`{"op":"heartbeat","id":3,"clk":"heartbeat-clk"}`,
		liveMarketImageMessage("1.111", "event-1", "Home FC", 2.30, 75, 2.34, 80),
	)
	connector := NewLiveConnector(&fakeLiveTransport{conns: []*fakeLiveConn{conn}}, liveTestOptions())
	result, err := connector.Run(context.Background())
	if err != nil {
		t.Fatalf("run live connector happy path: %v", err)
	}
	if result.State != LiveStateStopped || result.ConnectionAttempts != 1 || result.ReconnectAttempts != 0 {
		t.Fatalf("result state/attempts=%+v", result)
	}
	if len(conn.writes) != 2 {
		t.Fatalf("writes=%d want auth+subscription", len(conn.writes))
	}
	assertNoLiveSecrets(t, string(RedactLivePayload(conn.writes[0])))
	if result.LatestClock != "heartbeat-clk" || result.InitialClock != "initial-auth" {
		t.Fatalf("clocks=%q/%q", result.LatestClock, result.InitialClock)
	}
	if result.HeartbeatState != StreamStateHealthy {
		t.Fatalf("heartbeat state=%q want healthy", result.HeartbeatState)
	}
	if len(result.Candidates) != 2 {
		t.Fatalf("candidate count=%d want 2", len(result.Candidates))
	}
	back := requireLiveCandidate(t, result.Candidates, "1.111", 101, ExchangeSideBack)
	lay := requireLiveCandidate(t, result.Candidates, "1.111", 101, ExchangeSideLay)
	if back.PriceDecimal != 2.30 || back.AvailableSize != 75 || lay.PriceDecimal != 2.34 || lay.AvailableSize != 80 {
		t.Fatalf("candidate prices back=%+v lay=%+v", back, lay)
	}
	if back.LastTradedPrice == nil || *back.LastTradedPrice != 2.20 || back.TradedVolume == nil || *back.TradedVolume != 120 {
		t.Fatalf("candidate metadata ltp/tv=%v/%v", back.LastTradedPrice, back.TradedVolume)
	}
}

func TestLiveConnectorSkipsInitialConnectionBeforeAuthStatus(t *testing.T) {
	conn := newFakeLiveConn(
		`{"op":"connection","connectionId":"conn-1"}`,
		`{"op":"status","id":1,"statusCode":"SUCCESS","clk":"auth-clk","initialClk":"initial-auth"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS","clk":"sub-clk"}`,
		liveMarketImageMessage("1.111", "event-1", "Home FC", 2.30, 75, 2.34, 80),
	)
	connector := NewLiveConnector(&fakeLiveTransport{conns: []*fakeLiveConn{conn}}, liveTestOptions())
	result, err := connector.Run(context.Background())
	if err != nil {
		t.Fatalf("run live connector with initial connection message: %v", err)
	}
	if !result.Authenticated || !result.Subscribed {
		t.Fatalf("auth/subscription flags=%+v", result)
	}
	if result.MessagesParsed != 4 || result.ControlMessages != 3 || result.MarketChangeMessages != 1 {
		t.Fatalf("message counters=%+v", result)
	}
	if result.LatestClock != "sub-clk" || result.InitialClock != "initial-auth" {
		t.Fatalf("clocks=%q/%q", result.LatestClock, result.InitialClock)
	}
	if len(result.Candidates) != 2 {
		t.Fatalf("candidate count=%d want 2", len(result.Candidates))
	}
}

func TestLiveConnectorStatusFailuresRedactSecrets(t *testing.T) {
	tests := []struct {
		name string
		conn *fakeLiveConn
		want string
	}{
		{
			name: "auth failure",
			conn: newFakeLiveConn(`{"op":"status","id":1,"statusCode":"FAILURE","errorCode":"AUTH","errorMessage":"bad ` + liveTestAppKey + ` ` + liveTestSession + `"}`),
			want: "authentication failed",
		},
		{
			name: "subscription failure",
			conn: newFakeLiveConn(
				`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
				`{"op":"status","id":2,"statusCode":"FAILURE","errorCode":"SUB","errorMessage":"bad `+liveTestAppKey+` `+liveTestSession+`"}`,
			),
			want: "subscription failed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			connector := NewLiveConnector(&fakeLiveTransport{conns: []*fakeLiveConn{tc.conn}}, liveTestOptions())
			_, err := connector.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("status failure error=%v want containing %q", err, tc.want)
			}
			assertNoLiveSecrets(t, err.Error())
		})
	}
}

func TestLiveConnectorHeartbeatDoesNotMutatePricesAndCanAge(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	opts := liveTestOptions()
	opts.Now = func() time.Time { return now }
	conn := newFakeLiveConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
		liveMarketImageMessage("1.111", "event-1", "Home FC", 2.10, 50, 2.12, 60),
		`{"op":"heartbeat","id":3,"clk":"heartbeat-only"}`,
	)
	connector := NewLiveConnector(&fakeLiveTransport{conns: []*fakeLiveConn{conn}}, opts)
	result, err := connector.Run(context.Background())
	if err != nil {
		t.Fatalf("run heartbeat connector: %v", err)
	}
	back := requireLiveCandidate(t, result.Candidates, "1.111", 101, ExchangeSideBack)
	if back.PriceDecimal != 2.10 || back.AvailableSize != 50 {
		t.Fatalf("heartbeat mutated price candidate=%+v", back)
	}
	if result.LatestClock != "heartbeat-only" {
		t.Fatalf("heartbeat clock=%q want heartbeat-only", result.LatestClock)
	}
	if state := connector.HeartbeatState(now.Add(1500 * time.Millisecond)); state != StreamStateAging {
		t.Fatalf("heartbeat state after 1.5s=%q want aging", state)
	}
	if state := connector.HeartbeatState(now.Add(3 * time.Second)); state != StreamStateStale {
		t.Fatalf("heartbeat state after 3s=%q want stale", state)
	}
}

func TestLiveConnectorReconnectUsesLatestClocksAndPreservesSubscription(t *testing.T) {
	first := newFakeDisconnectLiveConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS","initialClk":"initial-one"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
		liveMarketImageMessageWithClock("1.111", "event-1", "Home FC", 2.10, 50, 2.12, 60, "clk-one", "initial-one"),
	)
	second := newFakeLiveConn(
		`{"op":"status","id":3,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":4,"statusCode":"SUCCESS"}`,
	)
	opts := liveTestOptions()
	opts.MaxReconnectAttempts = 1
	connector := NewLiveConnector(&fakeLiveTransport{conns: []*fakeLiveConn{first, second}}, opts)
	result, err := connector.Run(context.Background())
	if err != nil {
		t.Fatalf("run reconnect connector: %v", err)
	}
	if result.ReconnectAttempts != 1 || result.ConnectionAttempts != 2 {
		t.Fatalf("attempts reconnect=%d connections=%d want 1/2", result.ReconnectAttempts, result.ConnectionAttempts)
	}
	if len(second.writes) != 2 {
		t.Fatalf("second connection writes=%d want 2", len(second.writes))
	}
	var sub struct {
		MarketFilter struct {
			MarketIDs []string `json:"marketIds"`
		} `json:"marketFilter"`
		Clock        string `json:"clk"`
		InitialClock string `json:"initialClk"`
	}
	if err := json.Unmarshal(trimCRLF(second.writes[1]), &sub); err != nil {
		t.Fatalf("decode reconnect subscription: %v", err)
	}
	if len(sub.MarketFilter.MarketIDs) != 1 || sub.MarketFilter.MarketIDs[0] != "1.111" {
		t.Fatalf("reconnect market ids=%v want original [1.111]", sub.MarketFilter.MarketIDs)
	}
	if sub.Clock != "clk-one" || sub.InitialClock != "initial-one" {
		t.Fatalf("reconnect clocks=%q/%q want clk-one/initial-one", sub.Clock, sub.InitialClock)
	}
}

func TestLiveConnectorMaxReconnectAttempts(t *testing.T) {
	first := newFakeDisconnectLiveConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
	)
	opts := liveTestOptions()
	opts.MaxReconnectAttempts = 0
	connector := NewLiveConnector(&fakeLiveTransport{conns: []*fakeLiveConn{first}}, opts)
	_, err := connector.Run(context.Background())
	if !errors.Is(err, ErrLiveMaxReconnects) {
		t.Fatalf("max reconnect error=%v want ErrLiveMaxReconnects", err)
	}
}

func TestLiveConnectorContextCancellationStopsReconnect(t *testing.T) {
	connected := make(chan struct{}, 1)
	transport := &fakeLiveTransport{
		connectErrs: []error{errFakeLiveDisconnect, errFakeLiveDisconnect},
		onConnect: func(count int) {
			if count == 1 {
				connected <- struct{}{}
			}
		},
	}
	opts := liveTestOptions()
	opts.MaxReconnectAttempts = 10
	opts.ReconnectBaseDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		connector := NewLiveConnector(transport, opts)
		_, err := connector.Run(ctx)
		done <- err
	}()
	<-connected
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation error=%v want context.Canceled", err)
	}
	if transport.connectCount() != 1 {
		t.Fatalf("connect attempts after cancellation=%d want 1", transport.connectCount())
	}
}

// TestLiveConnectorBlockedReadEOFFromContextCancellationReturnsContextErr verifies
// that when liveCloseOnContext closes the underlying connection in response to a
// context cancellation and the blocked read returns io.EOF (as a clean TLS
// close-notify can on Linux), the connector reports the context error instead of
// silently treating the close as a clean stream EOF. Regression for a CI flake
// where TestRuntimeRealPilotContextCancellationStopsBlockedRead and
// TestRuntimeLocalTLSDurationStop intermittently observed stopped_reason=eof.
func TestLiveConnectorBlockedReadEOFFromContextCancellationReturnsContextErr(t *testing.T) {
	blocked := make(chan struct{})
	conn := newCloseToEOFLiveConn(
		[]string{
			`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
			`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
		},
		blocked,
	)
	connector := NewLiveConnector(&singleLiveTransport{conn: conn}, liveTestOptions())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := connector.Run(ctx)
		done <- err
	}()

	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connector to block in stream-loop read")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("connector cancellation error=%v want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connector cancellation")
	}
}

// TestLiveConnectorBlockedReadEOFFromContextDeadlineReturnsDeadline mirrors the
// blocked-read case for context.DeadlineExceeded, which is how DurationLimit
// surfaces. This is the same root cause as the cancellation flake.
func TestLiveConnectorBlockedReadEOFFromContextDeadlineReturnsDeadline(t *testing.T) {
	blocked := make(chan struct{})
	conn := newCloseToEOFLiveConn(
		[]string{
			`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
			`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
		},
		blocked,
	)
	connector := NewLiveConnector(&singleLiveTransport{conn: conn}, liveTestOptions())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := connector.Run(ctx)
		done <- err
	}()

	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connector to block in stream-loop read")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("connector deadline error=%v want context.DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connector deadline stop")
	}
}

func TestLiveConnectorMalformedStreamMessageFailsDeterministically(t *testing.T) {
	conn := newFakeLiveConn(
		`{"op":"status","id":1,"statusCode":"SUCCESS"}`,
		`{"op":"status","id":2,"statusCode":"SUCCESS"}`,
		`{"op":"mcm"`,
	)
	transport := &fakeLiveTransport{conns: []*fakeLiveConn{conn, newFakeLiveConn()}}
	opts := liveTestOptions()
	opts.MaxReconnectAttempts = 1
	connector := NewLiveConnector(transport, opts)
	_, err := connector.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "parse betfair live stream message") {
		t.Fatalf("malformed stream error=%v want parse error", err)
	}
	if transport.connectCount() != 1 {
		t.Fatalf("malformed stream reconnect count=%d want no reconnect", transport.connectCount())
	}
}

func liveTestOptions() LiveConnectorOptions {
	now := time.UnixMilli(1715000001000).UTC()
	return LiveConnectorOptions{
		Enabled:      true,
		AppKey:       liveTestAppKey,
		SessionToken: liveTestSession,
		Subscription: MarketSubscriptionRequest{
			MarketIDs:    []string{"1.111"},
			MarketTypes:  []string{"MATCH_ODDS"},
			Fields:       DefaultMarketDataFields(),
			LadderLevels: 3,
			HeartbeatMS:  500,
		},
		HeartbeatAgingAfter:  time.Second,
		HeartbeatStaleAfter:  2 * time.Second,
		MaxReconnectAttempts: 0,
		Now: func() time.Time {
			return now
		},
	}
}

func liveMarketImageMessage(marketID, eventID, runnerName string, backPrice, backSize, layPrice, laySize float64) string {
	return liveMarketImageMessageWithClock(marketID, eventID, runnerName, backPrice, backSize, layPrice, laySize, "", "")
}

func liveMarketImageMessageWithClock(marketID, eventID, runnerName string, backPrice, backSize, layPrice, laySize float64, clock, initialClock string) string {
	msg := map[string]any{
		"op":  "mcm",
		"id":  10,
		"pt":  float64(1715000000000),
		"ct":  "SUB_IMAGE",
		"clk": clock,
		"mc": []any{map[string]any{
			"id":  marketID,
			"img": true,
			"marketDefinition": map[string]any{
				"status":     "OPEN",
				"eventId":    eventID,
				"eventName":  runnerName + " v Away FC",
				"marketTime": "2026-05-07T12:00:00.000Z",
				"name":       "Match Odds",
				"inPlay":     false,
				"runners": []any{map[string]any{
					"id":     101,
					"name":   runnerName,
					"status": "ACTIVE",
				}},
			},
			"rc": []any{map[string]any{
				"id":   101,
				"ltp":  2.20,
				"tv":   120,
				"batb": []any{[]any{0, backPrice, backSize}},
				"batl": []any{[]any{0, layPrice, laySize}},
			}},
		}},
	}
	if initialClock != "" {
		msg["initialClk"] = initialClock
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func requireLiveCandidate(t *testing.T, candidates []QuoteCandidate, marketID string, selectionID int64, side ExchangeSide) QuoteCandidate {
	t.Helper()
	for _, candidate := range candidates {
		if candidate.MarketID == marketID && candidate.SelectionID == selectionID && candidate.ExchangeSide == side {
			return candidate
		}
	}
	t.Fatalf("candidate market=%s selection=%d side=%s not found in %+v", marketID, selectionID, side, candidates)
	return QuoteCandidate{}
}

func assertNoLiveSecrets(t *testing.T, s string) {
	t.Helper()
	if strings.Contains(s, liveTestAppKey) || strings.Contains(s, liveTestSession) {
		t.Fatalf("live output contains unredacted secret: %s", s)
	}
}

func trimCRLF(payload []byte) []byte {
	return []byte(strings.TrimRight(string(payload), "\r\n"))
}
