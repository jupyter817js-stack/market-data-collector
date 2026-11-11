package lsports

import (
	"context"
	"encoding/json"
	"fmt"
)

// Sport is one row of POST /Sports/Get.
//
// Response casing note: live TRADE360 returns PascalCase ("Id", "Name") even
// though the public docs show lowerCamel. Tags here match the wire format.
type Sport struct {
	ID   int    `json:"Id"`
	Name string `json:"Name"`
}

// League is one row of POST /Leagues/Get.
type League struct {
	ID      int    `json:"Id"`
	Name    string `json:"Name"`
	SportID int    `json:"SportId,omitempty"`
}

type sportsBody struct {
	Sports []Sport `json:"Sports"`
}

type sportsEnvelope struct {
	Header EnvelopeHeader `json:"Header"`
	Body   sportsBody     `json:"Body"`
}

type leaguesBody struct {
	Leagues []League `json:"Leagues"`
}

type leaguesEnvelope struct {
	Header EnvelopeHeader `json:"Header"`
	Body   leaguesBody    `json:"Body"`
}

// GetSports calls POST /Sports/Get. Returns the parsed sport list and the raw
// response bytes (the probe dumps the latter so we can verify schema without
// another network call).
func (c *Client) GetSports(ctx context.Context) ([]Sport, []byte, error) {
	raw, _, err := c.Post(ctx, "/Sports/Get", nil)
	if err != nil {
		return nil, raw, err
	}
	var env sportsEnvelope
	if jerr := json.Unmarshal(raw, &env); jerr != nil {
		return nil, raw, fmt.Errorf("lsports: /Sports/Get: decode envelope: %w", jerr)
	}
	if env.Header.HttpStatusCode != 0 && env.Header.HttpStatusCode != 200 {
		return nil, raw, fmt.Errorf("lsports: /Sports/Get: envelope status %d: %s",
			env.Header.HttpStatusCode, string(env.Header.Errors))
	}
	return env.Body.Sports, raw, nil
}

// GetLeagues calls POST /Leagues/Get filtered to one sport.
func (c *Client) GetLeagues(ctx context.Context, sportID int) ([]League, []byte, error) {
	// Request body keys stay lowerCamel — that side of the API does honor
	// the documented casing.
	raw, _, err := c.Post(ctx, "/Leagues/Get", map[string]any{
		"sportIds": []int{sportID},
	})
	if err != nil {
		return nil, raw, err
	}
	var env leaguesEnvelope
	if jerr := json.Unmarshal(raw, &env); jerr != nil {
		return nil, raw, fmt.Errorf("lsports: /Leagues/Get: decode envelope: %w", jerr)
	}
	if env.Header.HttpStatusCode != 0 && env.Header.HttpStatusCode != 200 {
		return nil, raw, fmt.Errorf("lsports: /Leagues/Get: envelope status %d: %s",
			env.Header.HttpStatusCode, string(env.Header.Errors))
	}
	return env.Body.Leagues, raw, nil
}
