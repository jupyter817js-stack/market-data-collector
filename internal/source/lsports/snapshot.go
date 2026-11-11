package lsports

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Snapshot envelope. Distinct from EnvelopeHeader (metadata API) because the
// snapshot host does NOT return HttpStatusCode — it returns Type/MsgGuid/
// ServerTimestamp. ServerTimestamp is the LSPORTS-side publish clock and is
// what we record as source_publish_time for latency analysis.
type SnapshotHeader struct {
	Type            int    `json:"Type"`
	MsgGuid         string `json:"MsgGuid"`
	ServerTimestamp int64  `json:"ServerTimestamp"` // epoch ms (assumed; verify on first capture)
}

// SnapshotServerTime returns the parsed ServerTimestamp as a UTC time.Time.
// Returns the zero value if ServerTimestamp is 0 or unparseable.
func (h SnapshotHeader) SnapshotServerTime() time.Time {
	if h.ServerTimestamp == 0 {
		return time.Time{}
	}
	// LSPORTS docs are silent on units; both Unix seconds and Unix ms appear
	// in TRADE360 implementations. Disambiguate by magnitude:
	//   ~1e9 = seconds, ~1e12 = milliseconds, ~1e15 = microseconds.
	switch {
	case h.ServerTimestamp > 1e14:
		return time.UnixMicro(h.ServerTimestamp).UTC()
	case h.ServerTimestamp > 1e11:
		return time.UnixMilli(h.ServerTimestamp).UTC()
	default:
		return time.Unix(h.ServerTimestamp, 0).UTC()
	}
}

// SnapshotFilter is the optional filter set for snapshot requests. Empty
// fields are omitted from the request body so the server treats them as
// "no filter" — but per LSPORTS rate-limit docs, callers SHOULD always
// provide at least `fixtures` so the request counts as "filtered" (1/sec
// cap rather than 1/15sec for full requests).
type SnapshotFilter struct {
	Fixtures    []int
	Sports      []int
	Leagues     []int
	Tournaments []int
	Markets     []int
	FromDate    *time.Time
	ToDate      *time.Time
	Timestamp   *time.Time // returns only items updated since this point
}

func (f SnapshotFilter) toBody() map[string]any {
	body := map[string]any{}
	if len(f.Fixtures) > 0 {
		body["fixtures"] = f.Fixtures
	}
	if len(f.Sports) > 0 {
		body["sports"] = f.Sports
	}
	if len(f.Leagues) > 0 {
		body["leagues"] = f.Leagues
	}
	if len(f.Tournaments) > 0 {
		body["tournaments"] = f.Tournaments
	}
	if len(f.Markets) > 0 {
		body["markets"] = f.Markets
	}
	if f.FromDate != nil {
		body["fromDate"] = f.FromDate.UTC().Unix()
	}
	if f.ToDate != nil {
		body["toDate"] = f.ToDate.UTC().Unix()
	}
	if f.Timestamp != nil {
		body["timestamp"] = f.Timestamp.UTC().UnixMilli()
	}
	return body
}

// SnapshotResponse keeps the parsed header plus the raw bytes so the latency
// daemon can both (a) read ServerTimestamp directly and (b) persist the full
// payload to lsports_inplay_observations.raw_payload.
type SnapshotResponse struct {
	Header SnapshotHeader
	Raw    json.RawMessage
}

// GetScores calls POST /InPlay/GetScores against the snapshot host. Body is
// the livescore (period/score/status) snapshot for the supplied fixtures.
// One LSPORTS observation per fixture.
func (c *Client) GetScores(ctx context.Context, filter SnapshotFilter) (SnapshotResponse, error) {
	return c.snapshotCall(ctx, "/InPlay/GetScores", filter)
}

// GetFixtureMarkets calls POST /InPlay/GetFixtureMarkets — current market /
// bet / price state for the supplied fixtures.
func (c *Client) GetFixtureMarkets(ctx context.Context, filter SnapshotFilter) (SnapshotResponse, error) {
	return c.snapshotCall(ctx, "/InPlay/GetFixtureMarkets", filter)
}

// GetInPlayFixtures calls POST /InPlay/GetFixtures — minimal fixture
// metadata (id, league, participants, start time) for in-play fixtures
// matching the filter.
func (c *Client) GetInPlayFixtures(ctx context.Context, filter SnapshotFilter) (SnapshotResponse, error) {
	return c.snapshotCall(ctx, "/InPlay/GetFixtures", filter)
}

// GetInPlayEvents calls POST /InPlay/GetEvents — generic "all-in-one"
// snapshot. Use Scores + FixtureMarkets in production; Events is mostly
// useful for first-contact schema verification.
func (c *Client) GetInPlayEvents(ctx context.Context, filter SnapshotFilter) (SnapshotResponse, error) {
	return c.snapshotCall(ctx, "/InPlay/GetEvents", filter)
}

func (c *Client) snapshotCall(ctx context.Context, path string, filter SnapshotFilter) (SnapshotResponse, error) {
	raw, _, err := c.Post(ctx, path, filter.toBody())
	if err != nil {
		return SnapshotResponse{}, err
	}
	var env struct {
		Header SnapshotHeader  `json:"Header"`
		Body   json.RawMessage `json:"Body"`
	}
	if jerr := json.Unmarshal(raw, &env); jerr != nil {
		return SnapshotResponse{Raw: raw}, fmt.Errorf("lsports: %s: decode envelope: %w", path, jerr)
	}
	return SnapshotResponse{Header: env.Header, Raw: raw}, nil
}
