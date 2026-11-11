// Command pinnacle-probe is a read-only verification tool against Pinnacle's
// public guest API (https://guest.api.arcadia.pinnacle.com). It exists to
// confirm the public x-api-key still works, list active leagues for a sport,
// and snapshot live matchups + their markets. It does not write to PostgreSQL
// and does not place bets. Pinnacle is the same line engine as PS3838.
//
// Usage:
//
//	go run ./cmd/pinnacle-probe -mode sports
//	go run ./cmd/pinnacle-probe -mode leagues -sport 12 -filter cs2,cs:go,counter
//	go run ./cmd/pinnacle-probe -mode matchups -league 263958
//	go run ./cmd/pinnacle-probe -mode markets -match 1630920768
//	go run ./cmd/pinnacle-probe -mode live -sport 12  (full pipeline: active CS leagues → matchups → live markets)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
)

type sportEntry struct {
	ID                int    `json:"id"`
	Name              string `json:"name"`
	IsFeatured        bool   `json:"isFeatured"`
	MatchupCount      int    `json:"matchupCount"`
	PrimaryMarketType string `json:"primaryMarketType"`
}

type leagueEntry struct {
	ID             int    `json:"id"`
	Name           string `json:"name"`
	Group          string `json:"group"`
	MatchupCount   int    `json:"matchupCount"`
	MatchupCountSE int    `json:"matchupCountSE"`
	IsFeatured     bool   `json:"isFeatured"`
}

type participant struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Alignment string `json:"alignment"`
	Rotation  int    `json:"rotation"`
}

type matchupEntry struct {
	ID           int64         `json:"id"`
	Type         string        `json:"type"` // matchup | special
	StartTime    string        `json:"startTime"`
	Status       string        `json:"status"`
	IsLive       bool          `json:"isLive"`
	LiveStatus   int           `json:"liveStatus"`
	HasMarkets   bool          `json:"hasMarkets"`
	HasLive      bool          `json:"hasLive"`
	Period       int           `json:"period"`
	Participants []participant `json:"participants"`
	League       leagueEntry   `json:"league"`
}

type priceEntry struct {
	Designation string  `json:"designation"`
	Points      float64 `json:"points,omitempty"`
	Price       int     `json:"price"`
}

type marketEntry struct {
	MatchupID   int64        `json:"matchupId"`
	Type        string       `json:"type"`   // moneyline | spread | total | team_total
	Period      int          `json:"period"` // 0=full, 1+=per-map
	Status      string       `json:"status"` // open | suspended
	Key         string       `json:"key"`
	IsAlternate bool         `json:"isAlternate"`
	Version     int64        `json:"version"`
	Side        string       `json:"side,omitempty"`
	Prices      []priceEntry `json:"prices"`
	Limits      []struct {
		Type   string  `json:"type"`
		Amount float64 `json:"amount"`
	} `json:"limits"`
}

func main() {
	mode := flag.String("mode", "live", "sports | leagues | matchups | markets | live")
	sportID := flag.Int("sport", 12, "sport id (default 12 = E Sports)")
	leagueID := flag.Int("league", 0, "league id (for matchups mode)")
	matchID := flag.Int64("match", 0, "matchup id (for markets mode)")
	filter := flag.String("filter", "", "comma-separated case-insensitive substrings to filter league names (e.g. cs2,cs:go,counter)")
	timeout := flag.Duration("timeout", 15*time.Second, "HTTP timeout")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(2)
	}

	c := &client{
		http:    &http.Client{Timeout: *timeout},
		baseURL: cfg.PinnacleBaseURL,
		apiKey:  cfg.PinnacleAPIKey,
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout+5*time.Second)
	defer cancel()

	filters := splitLower(*filter)
	if len(filters) == 0 && len(cfg.PinnacleLeagueNameIncludes) > 0 && *mode == "live" {
		filters = cfg.PinnacleLeagueNameIncludes
	}

	switch *mode {
	case "sports":
		runSports(ctx, c)
	case "leagues":
		runLeagues(ctx, c, *sportID, filters)
	case "matchups":
		if *leagueID == 0 {
			die("-league required")
		}
		runMatchups(ctx, c, *leagueID)
	case "markets":
		if *matchID == 0 {
			die("-match required")
		}
		runMarkets(ctx, c, *matchID)
	case "live":
		runLivePipeline(ctx, c, *sportID, filters)
	default:
		die("unknown -mode: " + *mode)
	}
}

func runSports(ctx context.Context, c *client) {
	var sports []sportEntry
	if err := c.getJSON(ctx, "/0.1/sports", &sports); err != nil {
		die(err.Error())
	}
	sort.Slice(sports, func(i, j int) bool {
		if sports[i].MatchupCount != sports[j].MatchupCount {
			return sports[i].MatchupCount > sports[j].MatchupCount
		}
		return sports[i].Name < sports[j].Name
	})
	fmt.Printf("%-5s  %-25s  %-10s  %s\n", "ID", "NAME", "MATCHES", "PRIMARY")
	for _, s := range sports {
		fmt.Printf("%-5d  %-25s  %-10d  %s\n", s.ID, s.Name, s.MatchupCount, s.PrimaryMarketType)
	}
}

func runLeagues(ctx context.Context, c *client, sportID int, filters []string) {
	var lgs []leagueEntry
	path := fmt.Sprintf("/0.1/sports/%d/leagues", sportID)
	if err := c.getJSON(ctx, path, &lgs); err != nil {
		die(err.Error())
	}
	if len(filters) > 0 {
		lgs = filterLeagues(lgs, filters)
	}
	sort.Slice(lgs, func(i, j int) bool { return lgs[i].MatchupCount > lgs[j].MatchupCount })
	fmt.Printf("%-7s  %-7s  %-18s  %s\n", "ID", "MATCHES", "GROUP", "NAME")
	for _, l := range lgs {
		fmt.Printf("%-7d  %-7d  %-18s  %s\n", l.ID, l.MatchupCount, truncate(l.Group, 18), l.Name)
	}
}

func runMatchups(ctx context.Context, c *client, leagueID int) {
	var ms []matchupEntry
	path := fmt.Sprintf("/0.1/leagues/%d/matchups?brandId=0", leagueID)
	if err := c.getJSON(ctx, path, &ms); err != nil {
		die(err.Error())
	}
	main, props := 0, 0
	for _, m := range ms {
		if m.Type == "matchup" {
			main++
		} else {
			props++
		}
	}
	fmt.Printf("league=%d  total=%d (main=%d props=%d)\n\n", leagueID, len(ms), main, props)
	for _, m := range ms {
		if m.Type != "matchup" {
			continue
		}
		var p1, p2 string
		if len(m.Participants) >= 2 {
			p1, p2 = m.Participants[0].Name, m.Participants[1].Name
		}
		liveFlag := " "
		if m.IsLive {
			liveFlag = "*"
		}
		fmt.Printf("%s id=%-12d hasMarkets=%v hasLive=%v status=%-8s liveStatus=%d  %s vs %s  starts=%s\n",
			liveFlag, m.ID, m.HasMarkets, m.HasLive, m.Status, m.LiveStatus, p1, p2, m.StartTime)
	}
}

func runMarkets(ctx context.Context, c *client, matchID int64) {
	var mkts []marketEntry
	path := fmt.Sprintf("/0.1/matchups/%d/markets/related/straight", matchID)
	if err := c.getJSON(ctx, path, &mkts); err != nil {
		die(err.Error())
	}
	// Group ML by period for readable summary; show all moneylines + period totals.
	fmt.Printf("matchup=%d  markets=%d\n\n", matchID, len(mkts))
	for _, m := range mkts {
		if m.IsAlternate {
			continue
		}
		switch m.Type {
		case "moneyline":
			fmt.Printf("[ML period=%d status=%-9s key=%s] ", m.Period, m.Status, m.Key)
			for _, p := range m.Prices {
				dec, prob := americanToDecimal(p.Price), americanToImpliedProb(p.Price)
				fmt.Printf("%s amer=%+d dec=%.3f imp=%.2f%%  ", p.Designation, p.Price, dec, prob*100)
			}
			ml := pickMaxLimit(m.Limits, "maxRiskStake")
			fmt.Printf("maxRisk=$%.0f\n", ml)
			noVig := nonVigPair(m.Prices)
			if len(noVig) == 2 {
				fmt.Printf("    no-vig:  %s=%.2f%%  %s=%.2f%%\n", m.Prices[0].Designation, noVig[0]*100, m.Prices[1].Designation, noVig[1]*100)
			}
		case "total":
			fmt.Printf("[TOT period=%d status=%-9s pts=%g] ", m.Period, m.Status, lineFromPrices(m.Prices))
			for _, p := range m.Prices {
				fmt.Printf("%s amer=%+d  ", p.Designation, p.Price)
			}
			fmt.Println()
		case "spread":
			fmt.Printf("[SPR period=%d status=%-9s pts=%+g] ", m.Period, m.Status, lineFromPrices(m.Prices))
			for _, p := range m.Prices {
				fmt.Printf("%s amer=%+d  ", p.Designation, p.Price)
			}
			fmt.Println()
		}
	}
}

func runLivePipeline(ctx context.Context, c *client, sportID int, filters []string) {
	fmt.Printf("=== leagues (sport=%d, filter=%v) ===\n", sportID, filters)
	var lgs []leagueEntry
	if err := c.getJSON(ctx, fmt.Sprintf("/0.1/sports/%d/leagues", sportID), &lgs); err != nil {
		die(err.Error())
	}
	lgs = filterLeagues(lgs, filters)
	active := lgs[:0]
	for _, l := range lgs {
		if l.MatchupCount > 0 {
			active = append(active, l)
		}
	}
	if len(active) == 0 {
		fmt.Println("(no active leagues match filter)")
		return
	}
	for _, l := range active {
		fmt.Printf("\n--- league %d (%s) %d matchups ---\n", l.ID, l.Name, l.MatchupCount)
		var ms []matchupEntry
		if err := c.getJSON(ctx, fmt.Sprintf("/0.1/leagues/%d/matchups?brandId=0", l.ID), &ms); err != nil {
			fmt.Fprintf(os.Stderr, "  matchups err: %v\n", err)
			continue
		}
		for _, m := range ms {
			if m.Type != "matchup" {
				continue
			}
			var p1, p2 string
			if len(m.Participants) >= 2 {
				p1, p2 = m.Participants[0].Name, m.Participants[1].Name
			}
			liveFlag := " "
			if m.IsLive {
				liveFlag = "*"
			}
			fmt.Printf("  %s match=%-12d isLive=%v hasMarkets=%v  %s vs %s  starts=%s\n",
				liveFlag, m.ID, m.IsLive, m.HasMarkets, p1, p2, m.StartTime)
			if !m.IsLive || !m.HasMarkets {
				continue
			}
			var mkts []marketEntry
			if err := c.getJSON(ctx, fmt.Sprintf("/0.1/matchups/%d/markets/related/straight", m.ID), &mkts); err != nil {
				fmt.Fprintf(os.Stderr, "    markets err: %v\n", err)
				continue
			}
			for _, mk := range mkts {
				if mk.Type != "moneyline" || mk.IsAlternate || mk.Period != 0 {
					continue
				}
				fmt.Printf("      ML(match) status=%s  ", mk.Status)
				for _, p := range mk.Prices {
					fmt.Printf("%s %+d (dec %.3f)  ", p.Designation, p.Price, americanToDecimal(p.Price))
				}
				nv := nonVigPair(mk.Prices)
				if len(nv) == 2 {
					fmt.Printf("no-vig %s=%.1f%% %s=%.1f%%", mk.Prices[0].Designation, nv[0]*100, mk.Prices[1].Designation, nv[1]*100)
				}
				fmt.Println()
			}
		}
	}
}

// ---- helpers ----

type client struct {
	http    *http.Client
	baseURL string
	apiKey  string
}

func (c *client) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
	req.Header.Set("Referer", "https://www.pinnacle.com/")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s: http %d: %s", path, resp.StatusCode, truncate(string(body), 200))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("%s: decode: %w", path, err)
	}
	return nil
}

func filterLeagues(in []leagueEntry, filters []string) []leagueEntry {
	if len(filters) == 0 {
		return in
	}
	out := make([]leagueEntry, 0, len(in))
	for _, l := range in {
		n := strings.ToLower(l.Name)
		for _, f := range filters {
			if strings.Contains(n, f) {
				out = append(out, l)
				break
			}
		}
	}
	return out
}

func americanToDecimal(a int) float64 {
	if a == 0 {
		return 0
	}
	if a > 0 {
		return 1 + float64(a)/100.0
	}
	return 1 + 100.0/float64(-a)
}

func americanToImpliedProb(a int) float64 {
	if a == 0 {
		return 0
	}
	if a > 0 {
		return 100.0 / float64(a+100)
	}
	return float64(-a) / float64(-a+100)
}

// nonVigPair normalizes a two-outcome market by dividing raw implied
// probabilities by their sum. Only returns 2 values when prices length is 2.
func nonVigPair(prices []priceEntry) []float64 {
	if len(prices) != 2 {
		return nil
	}
	p0 := americanToImpliedProb(prices[0].Price)
	p1 := americanToImpliedProb(prices[1].Price)
	s := p0 + p1
	if s <= 0 {
		return nil
	}
	return []float64{p0 / s, p1 / s}
}

func pickMaxLimit(limits []struct {
	Type   string  `json:"type"`
	Amount float64 `json:"amount"`
}, typ string) float64 {
	for _, l := range limits {
		if l.Type == typ {
			return l.Amount
		}
	}
	return 0
}

func lineFromPrices(prices []priceEntry) float64 {
	for _, p := range prices {
		if p.Points != 0 {
			return p.Points
		}
	}
	return 0
}

func splitLower(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.ToLower(strings.TrimSpace(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
