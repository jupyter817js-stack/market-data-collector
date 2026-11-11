// Package pinnacle is a read-only client for Pinnacle's public guest API
// (https://guest.api.arcadia.pinnacle.com). Pinnacle is the same line engine
// as PS3838 (which is its Asian-facing skin); we scrape this public surface
// while PS3838 account-level API access is unavailable. Strictly read-only —
// this package never places bets.
package pinnacle

import "time"

// Sport is one row of GET /0.1/sports.
type Sport struct {
	ID                int    `json:"id"`
	Name              string `json:"name"`
	PrimaryMarketType string `json:"primaryMarketType"`
	IsFeatured        bool   `json:"isFeatured"`
	MatchupCount      int    `json:"matchupCount"`
}

// League is one row of GET /0.1/sports/{sportId}/leagues.
type League struct {
	ID             int    `json:"id"`
	Name           string `json:"name"`
	Group          string `json:"group"`
	MatchupCount   int    `json:"matchupCount"`
	MatchupCountSE int    `json:"matchupCountSE"`
	IsFeatured     bool   `json:"isFeatured"`
	Sport          struct {
		ID int `json:"id"`
	} `json:"sport"`
}

// Participant is one element of Matchup.Participants.
type Participant struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Alignment string `json:"alignment"`
	Rotation  int    `json:"rotation"`
	Order     int    `json:"order"`
}

// Matchup is one row of GET /0.1/leagues/{leagueId}/matchups.
type Matchup struct {
	ID           int64         `json:"id"`
	Type         string        `json:"type"` // "matchup" | "special"
	StartTime    time.Time     `json:"startTime"`
	Status       string        `json:"status"`
	IsLive       bool          `json:"isLive"`
	LiveStatus   int           `json:"liveStatus"`
	HasMarkets   bool          `json:"hasMarkets"`
	HasLive      bool          `json:"hasLive"`
	Period       int           `json:"period"`
	Participants []Participant `json:"participants"`
	League       struct {
		ID    int    `json:"id"`
		Name  string `json:"name"`
		Group string `json:"group"`
		Sport struct {
			ID int `json:"id"`
		} `json:"sport"`
	} `json:"league"`
}

// HomeAway returns the two participant names in (home, away) order. Pinnacle
// uses `alignment` ("home"|"away"|"neutral") to indicate sides; for esports
// most matches are neutral so we fall back to participant order. The first
// participant is treated as home in that case.
func (m Matchup) HomeAway() (homeName string, awayName string, homeID, awayID int64) {
	var home, away *Participant
	for i := range m.Participants {
		p := &m.Participants[i]
		switch p.Alignment {
		case "home":
			home = p
		case "away":
			away = p
		}
	}
	if home == nil || away == nil {
		// Fall back to first two by order.
		if len(m.Participants) >= 1 && home == nil {
			home = &m.Participants[0]
		}
		if len(m.Participants) >= 2 && away == nil {
			away = &m.Participants[1]
		}
	}
	if home != nil {
		homeName, homeID = home.Name, home.ID
	}
	if away != nil {
		awayName, awayID = away.Name, away.ID
	}
	return
}

// Price is one element of Market.Prices.
type Price struct {
	Designation string  `json:"designation"` // "home"|"away"|"draw"|"over"|"under"
	Points      float64 `json:"points,omitempty"`
	Price       int     `json:"price"` // American odds
}

// Limit is one element of Market.Limits.
type Limit struct {
	Type   string  `json:"type"` // "maxRiskStake"
	Amount float64 `json:"amount"`
}

// Market is one element of GET /0.1/matchups/{matchupId}/markets/related/straight.
type Market struct {
	MatchupID   int64   `json:"matchupId"`
	Type        string  `json:"type"`   // "moneyline"|"spread"|"total"|"team_total"
	Period      int     `json:"period"` // 0 = full match, 1+ = per-period (map)
	Status      string  `json:"status"` // "open"|"suspended"
	Key         string  `json:"key"`
	IsAlternate bool    `json:"isAlternate"`
	Version     int64   `json:"version"`
	Side        string  `json:"side,omitempty"`
	Prices      []Price `json:"prices"`
	Limits      []Limit `json:"limits"`
}
