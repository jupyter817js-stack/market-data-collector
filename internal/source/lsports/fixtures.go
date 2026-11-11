package lsports

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Fixture is a minimal projection of one row from POST /Fixtures/Get.
// LSPORTS returns many more fields; we capture only what the operator
// needs to decide whether to spend a quota slot on this fixture.
//
// PascalCase JSON tags match the live response shape (see EnvelopeHeader
// for the asymmetry rationale).
type Fixture struct {
	ID           int64        `json:"FixtureId"`
	StartDate    string       `json:"StartDate"`
	Status       int          `json:"Status,omitempty"`
	Sport        SportRef     `json:"Sport,omitempty"`
	Location     LocationRef  `json:"Location,omitempty"`
	League       LeagueRef    `json:"League,omitempty"`
	Participants []Competitor `json:"Participants,omitempty"`
	LastUpdate   string       `json:"LastUpdate,omitempty"`
}

type SportRef struct {
	ID   int    `json:"Id"`
	Name string `json:"Name"`
}

type LocationRef struct {
	ID   int    `json:"Id,omitempty"`
	Name string `json:"Name,omitempty"`
}

type LeagueRef struct {
	ID   int    `json:"Id"`
	Name string `json:"Name"`
}

// Competitor is one participant; LSPORTS uses Position 1=Home and 2=Away
// for two-sided matches.
type Competitor struct {
	ID       int64  `json:"Id"`
	Name     string `json:"Name"`
	Position int    `json:"Position,omitempty"`
}

// FixtureStatus integer codes. Docs do not enumerate them exhaustively;
// these are the values we care about for the (C) refund semantic — anything
// other than NotStarted means the match may already be in or past
// InProgress, so a Subscribe is risky.
const (
	FixtureStatusNotStarted = 1
	FixtureStatusInProgress = 2
	FixtureStatusFinished   = 3
)

// HomeParticipant returns the Position=1 competitor name, if present.
func (f Fixture) HomeParticipant() string {
	for _, p := range f.Participants {
		if p.Position == 1 {
			return p.Name
		}
	}
	return ""
}

// AwayParticipant returns the Position=2 competitor name, if present.
func (f Fixture) AwayParticipant() string {
	for _, p := range f.Participants {
		if p.Position == 2 {
			return p.Name
		}
	}
	return ""
}

// CommenceTime parses StartDate. LSPORTS has been observed returning either
// strict RFC3339 or "YYYY-MM-DDTHH:MM:SS" without an offset; we try both.
func (f Fixture) CommenceTime() (time.Time, bool) {
	if f.StartDate == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, f.StartDate); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

type fixturesBody struct {
	Fixtures []Fixture `json:"Fixtures"`
}

// fixturesEnvelope uses json.RawMessage for Body so we can tolerate the
// LSPORTS quirk where some error/empty responses come back with a numeric
// or string Body (e.g. error indicators) instead of the expected object.
// We decode the object shape best-effort and fall back to "no fixtures".
type fixturesEnvelope struct {
	Header EnvelopeHeader  `json:"Header"`
	Body   json.RawMessage `json:"Body"`
}

// FixturesFilter is the optional filter set for POST /Fixtures/Get. Each
// field maps to the corresponding TRADE360 request parameter. All fields
// are optional; an empty filter returns all fixtures the package is
// entitled to (which during the trial is intentionally narrow).
//
// Quota cost: 0. Safe to poll for fixture discovery.
type FixturesFilter struct {
	SportIDs  []int
	LeagueIDs []int
	FromDate  *time.Time
	ToDate    *time.Time
}

// GetFixtures calls POST /Fixtures/Get. Returns a minimal projection of the
// fixture list and the raw response bytes.
func (c *Client) GetFixtures(ctx context.Context, filter FixturesFilter) ([]Fixture, []byte, error) {
	body := map[string]any{}
	if len(filter.SportIDs) > 0 {
		body["sportIds"] = filter.SportIDs
	}
	if len(filter.LeagueIDs) > 0 {
		body["leagueIds"] = filter.LeagueIDs
	}
	if filter.FromDate != nil {
		body["fromDate"] = filter.FromDate.UTC().Format(time.RFC3339)
	}
	if filter.ToDate != nil {
		body["toDate"] = filter.ToDate.UTC().Format(time.RFC3339)
	}

	raw, _, err := c.Post(ctx, "/Fixtures/Get", body)
	if err != nil {
		return nil, raw, err
	}
	var env fixturesEnvelope
	if jerr := json.Unmarshal(raw, &env); jerr != nil {
		return nil, raw, fmt.Errorf("lsports: /Fixtures/Get: decode envelope: %w", jerr)
	}
	if env.Header.HttpStatusCode != 0 && env.Header.HttpStatusCode != 200 {
		return nil, raw, fmt.Errorf("lsports: /Fixtures/Get: envelope status %d: %s",
			env.Header.HttpStatusCode, string(env.Header.Errors))
	}
	// Body shape is normally {"Fixtures":[...]} but LSPORTS occasionally
	// returns a bare number or string in error/edge cases. Decode best-effort
	// and treat unrecognized shapes as "no fixtures" so the caller continues.
	if len(env.Body) == 0 {
		return nil, raw, nil
	}
	var bodyObj fixturesBody
	if jerr := json.Unmarshal(env.Body, &bodyObj); jerr != nil {
		return nil, raw, nil
	}
	return bodyObj.Fixtures, raw, nil
}
