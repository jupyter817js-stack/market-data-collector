package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// GetEventBySlug calls Gamma /events?slug={slug} and returns the first
// matching event (with markets and tokens). Used by auto-discover to
// resolve an LSPORTS fixture (home/away team names + date) into the PM
// event id, winner-market condition_id, and token ids.
//
// Returns (Event{}, false, nil) when slug resolves to zero events — the
// auto-discover loop should treat that as "PM event not listed yet" and
// retry later, not as a fatal error.
func (c *Client) GetEventBySlug(ctx context.Context, slug string) (Event, bool, error) {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return Event{}, false, fmt.Errorf("polymarket gamma slug is required")
	}
	endpoint := c.baseURL + "/events?" + url.Values{"slug": []string{slug}}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Event{}, false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pol-odds-collector/0.1")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Event{}, false, ctxErr
		}
		return Event{}, false, fmt.Errorf("polymarket gamma request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Event{}, false, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Event{}, false, fmt.Errorf("polymarket gamma status %d: %s", resp.StatusCode, truncate(string(body), 400))
	}

	var events []Event
	if err := json.Unmarshal(body, &events); err != nil {
		// May be a single-object response; try that shape.
		var single Event
		if err2 := json.Unmarshal(body, &single); err2 == nil && single.ID != "" {
			return single, true, nil
		}
		return Event{}, false, fmt.Errorf("polymarket gamma response decode: %w", err)
	}
	if len(events) == 0 {
		return Event{}, false, nil
	}
	return events[0], true, nil
}

// PickWinnerMarket walks an event's markets and returns the moneyline
// "team A vs. team B" winner market (the one whose two tokens carry the
// team full names, not Over/Under/Spread). PM lists this as the FIRST
// market on MLB events but defensive scanning beats positional assumption.
//
// Returns (Market{}, false) if no two-team binary market is found.
func PickWinnerMarket(ev Event, homeName, awayName string) (Market, bool) {
	homeLower := strings.ToLower(strings.TrimSpace(homeName))
	awayLower := strings.ToLower(strings.TrimSpace(awayName))
	for _, mkt := range ev.Markets {
		if len(mkt.Tokens) != 2 {
			continue
		}
		var sawHome, sawAway bool
		for _, t := range mkt.Tokens {
			low := strings.ToLower(strings.TrimSpace(t.OutcomeName))
			if low == homeLower {
				sawHome = true
			}
			if low == awayLower {
				sawAway = true
			}
		}
		if sawHome && sawAway {
			return mkt, true
		}
	}
	return Market{}, false
}
