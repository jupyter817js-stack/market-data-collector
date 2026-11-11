package polymarket

import "strings"

// MLB team full-name to PM slug short-code mapping. PM event slugs follow
// the pattern "mlb-{away_short}-{home_short}-{YYYY-MM-DD}" (e.g.
// mlb-atl-cin-2026-05-30). Codes are 2-3 chars matching what Polymarket
// uses in their slugs; verified against live Polymarket pages 2026-05-30.
//
// Some teams have alternates (e.g. LA Angels = laa OR ana historically; we
// use the current PM convention). If a slug-formation call fails the
// caller falls back to a search-based path.
var mlbTeamShortCodes = map[string]string{
	"arizona diamondbacks":  "ari",
	"atlanta braves":        "atl",
	"baltimore orioles":     "bal",
	"boston red sox":        "bos",
	"chicago cubs":          "chc",
	"chicago white sox":     "cws",
	"cincinnati reds":       "cin",
	"cleveland guardians":   "cle",
	"colorado rockies":      "col",
	"detroit tigers":        "det",
	"houston astros":        "hou",
	"kansas city royals":    "kc",
	"los angeles angels":    "laa",
	"los angeles dodgers":   "lad",
	"miami marlins":         "mia",
	"milwaukee brewers":     "mil",
	"minnesota twins":       "min",
	"new york mets":         "nym",
	"new york yankees":      "nyy",
	"oakland athletics":     "oak",
	"athletics":             "ath", // current 2025+ naming after Oakland departure
	"philadelphia phillies": "phi",
	"pittsburgh pirates":    "pit",
	"san diego padres":      "sd",
	"san francisco giants":  "sf",
	"seattle mariners":      "sea",
	"st. louis cardinals":   "stl",
	"st louis cardinals":    "stl",
	"tampa bay rays":        "tb",
	"texas rangers":         "tex",
	"toronto blue jays":     "tor",
	"washington nationals":  "wsh",
}

// MLBShortCode returns the PM slug short code for a team. Comparison is
// case-insensitive and trims surrounding whitespace. Returns the empty
// string if no mapping is known — caller should fall back to a search
// path or skip the fixture.
func MLBShortCode(fullName string) string {
	key := strings.ToLower(strings.TrimSpace(fullName))
	return mlbTeamShortCodes[key]
}

// BuildMLBEventSlug constructs PM's daily MLB winner-market slug from
// LSPORTS-style home/away full team names and the local game date
// (UTC YYYY-MM-DD). Returns empty string if either team name is unknown.
//
// PM convention: away team first in the slug, home team second.
//
//	mlb-{away_short}-{home_short}-{YYYY-MM-DD}
func BuildMLBEventSlug(homeName, awayName, gameDateYYYYMMDD string) string {
	homeShort := MLBShortCode(homeName)
	awayShort := MLBShortCode(awayName)
	if homeShort == "" || awayShort == "" {
		return ""
	}
	return "mlb-" + awayShort + "-" + homeShort + "-" + gameDateYYYYMMDD
}
