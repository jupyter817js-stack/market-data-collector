// Command ps3838-probe is a one-shot read-only verification tool against the
// PS3838 Lines API. It exists to confirm credentials work, list sports/leagues
// with current offerings, and (for live mode) snapshot the in-play fixture set
// for a chosen sport. It does not write to PostgreSQL and does not place bets.
//
//	go run ./cmd/ps3838-probe -mode sports
//	go run ./cmd/ps3838-probe -mode leagues -sport 12
//	go run ./cmd/ps3838-probe -mode fixtures -sport 12 -live
//	go run ./cmd/ps3838-probe -mode esports-search
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
)

type sportsResp struct {
	Sports []struct {
		ID           int    `json:"id"`
		Name         string `json:"name"`
		HasOfferings bool   `json:"hasOfferings"`
		EventCount   int    `json:"eventCount"`
	} `json:"sports"`
}

type leaguesResp struct {
	Leagues []struct {
		ID                int    `json:"id"`
		Name              string `json:"name"`
		HomeTeamType      string `json:"homeTeamType"`
		HasOfferings      bool   `json:"hasOfferings"`
		AllowRoundRobins  bool   `json:"allowRoundRobins"`
		LeagueSpecialsCnt int    `json:"leagueSpecialsCount"`
		EventSpecialsCnt  int    `json:"eventSpecialsCount"`
		EventCount        int    `json:"eventCount"`
	} `json:"leagues"`
}

type fixturesResp struct {
	SportID int   `json:"sportId"`
	Last    int64 `json:"last"`
	League  []struct {
		ID     int    `json:"id"`
		Name   string `json:"name"`
		Events []struct {
			ID         int64     `json:"id"`
			ParentID   int64     `json:"parentId"`
			Starts     time.Time `json:"starts"`
			Home       string    `json:"home"`
			Away       string    `json:"away"`
			LiveStatus int       `json:"liveStatus"`
			Status     string    `json:"status"`
			Version    int64     `json:"version"`
		} `json:"events"`
	} `json:"league"`
}

func main() {
	mode := flag.String("mode", "sports", "sports | leagues | fixtures | esports-search")
	sportID := flag.Int("sport", 0, "sport id (for leagues / fixtures modes)")
	leagueIDs := flag.String("leagues", "", "comma-separated league ids (optional, fixtures mode)")
	live := flag.Bool("live", false, "fixtures mode: only live (isLive=1)")
	timeout := flag.Duration("timeout", 15*time.Second, "HTTP timeout")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(2)
	}
	if cfg.PS3838Username == "" || cfg.PS3838Password == "" {
		fmt.Fprintln(os.Stderr, "set PS3838_USERNAME and PS3838_PASSWORD in .env first")
		os.Exit(2)
	}

	client := &http.Client{Timeout: *timeout}
	auth := basicAuth(cfg.PS3838Username, cfg.PS3838Password)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout+5*time.Second)
	defer cancel()

	switch *mode {
	case "sports":
		runSports(ctx, client, cfg.PS3838BaseURL, auth)
	case "leagues":
		if *sportID == 0 {
			die("--sport required for leagues mode")
		}
		runLeagues(ctx, client, cfg.PS3838BaseURL, auth, *sportID)
	case "fixtures":
		if *sportID == 0 {
			die("--sport required for fixtures mode")
		}
		runFixtures(ctx, client, cfg.PS3838BaseURL, auth, *sportID, *leagueIDs, *live)
	case "esports-search":
		runEsportsSearch(ctx, client, cfg.PS3838BaseURL, auth)
	default:
		die("unknown -mode: " + *mode)
	}
}

func runSports(ctx context.Context, c *http.Client, base, auth string) {
	var r sportsResp
	if err := getJSON(ctx, c, base+"/v3/sports", auth, &r); err != nil {
		die("GET /v3/sports: " + err.Error())
	}
	sort.Slice(r.Sports, func(i, j int) bool {
		if r.Sports[i].HasOfferings != r.Sports[j].HasOfferings {
			return r.Sports[i].HasOfferings
		}
		return r.Sports[i].Name < r.Sports[j].Name
	})
	fmt.Printf("%-5s  %-30s  %-10s  %s\n", "ID", "NAME", "OFFERS", "EVENTS")
	for _, s := range r.Sports {
		offers := "no"
		if s.HasOfferings {
			offers = "YES"
		}
		fmt.Printf("%-5d  %-30s  %-10s  %d\n", s.ID, s.Name, offers, s.EventCount)
	}
}

func runLeagues(ctx context.Context, c *http.Client, base, auth string, sportID int) {
	u := fmt.Sprintf("%s/v3/leagues?sportId=%d", base, sportID)
	var r leaguesResp
	if err := getJSON(ctx, c, u, auth, &r); err != nil {
		die("GET /v3/leagues: " + err.Error())
	}
	sort.Slice(r.Leagues, func(i, j int) bool {
		if r.Leagues[i].HasOfferings != r.Leagues[j].HasOfferings {
			return r.Leagues[i].HasOfferings
		}
		if r.Leagues[i].EventCount != r.Leagues[j].EventCount {
			return r.Leagues[i].EventCount > r.Leagues[j].EventCount
		}
		return r.Leagues[i].Name < r.Leagues[j].Name
	})
	fmt.Printf("%-7s  %-50s  %-10s  %s\n", "ID", "NAME", "OFFERS", "EVENTS")
	for _, l := range r.Leagues {
		offers := "no"
		if l.HasOfferings {
			offers = "YES"
		}
		fmt.Printf("%-7d  %-50s  %-10s  %d\n", l.ID, l.Name, offers, l.EventCount)
	}
}

func runFixtures(ctx context.Context, c *http.Client, base, auth string, sportID int, leagueCSV string, live bool) {
	q := url.Values{}
	q.Set("sportId", strconv.Itoa(sportID))
	if leagueCSV != "" {
		q.Set("leagueIds", leagueCSV)
	}
	if live {
		q.Set("isLive", "1")
	}
	u := fmt.Sprintf("%s/v3/fixtures?%s", base, q.Encode())
	var r fixturesResp
	if err := getJSON(ctx, c, u, auth, &r); err != nil {
		die("GET /v3/fixtures: " + err.Error())
	}
	fmt.Printf("last=%d  leagues=%d\n\n", r.Last, len(r.League))
	for _, lg := range r.League {
		fmt.Printf("[%d] %s (%d events)\n", lg.ID, lg.Name, len(lg.Events))
		for _, e := range lg.Events {
			fmt.Printf("    %d  live=%d  status=%s  %s vs %s  starts=%s\n",
				e.ID, e.LiveStatus, e.Status, e.Home, e.Away, e.Starts.Format(time.RFC3339))
		}
	}
}

// runEsportsSearch lists all sports first, finds candidates whose name suggests
// esports/CS2, then for each candidate dumps its top-level leagues so we can
// confirm Counter-Strike's league IDs.
func runEsportsSearch(ctx context.Context, c *http.Client, base, auth string) {
	var sr sportsResp
	if err := getJSON(ctx, c, base+"/v3/sports", auth, &sr); err != nil {
		die("GET /v3/sports: " + err.Error())
	}
	for _, s := range sr.Sports {
		n := strings.ToLower(s.Name)
		if !strings.Contains(n, "esport") && !strings.Contains(n, "e-sport") && !strings.Contains(n, "e sport") {
			continue
		}
		fmt.Printf("==> sport id=%d name=%q offerings=%v events=%d\n", s.ID, s.Name, s.HasOfferings, s.EventCount)
		var lr leaguesResp
		if err := getJSON(ctx, c, fmt.Sprintf("%s/v3/leagues?sportId=%d", base, s.ID), auth, &lr); err != nil {
			fmt.Fprintf(os.Stderr, "  leagues fetch failed: %v\n", err)
			continue
		}
		sort.Slice(lr.Leagues, func(i, j int) bool { return lr.Leagues[i].Name < lr.Leagues[j].Name })
		for _, l := range lr.Leagues {
			if !l.HasOfferings && l.EventCount == 0 {
				continue
			}
			name := strings.ToLower(l.Name)
			marker := ""
			if strings.Contains(name, "counter") || strings.Contains(name, "cs2") || strings.Contains(name, "cs:go") || strings.Contains(name, "csgo") {
				marker = "  <-- CS2 candidate"
			}
			fmt.Printf("    league id=%-7d events=%-4d  %s%s\n", l.ID, l.EventCount, l.Name, marker)
		}
	}
}

func getJSON(ctx context.Context, c *http.Client, u, auth string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Basic "+auth)
	req.Header.Set("Accept", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(body), 400))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode: %w (body=%s)", err, truncate(string(body), 200))
	}
	return nil
}

func basicAuth(u, p string) string {
	return base64.StdEncoding.EncodeToString([]byte(u + ":" + p))
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
