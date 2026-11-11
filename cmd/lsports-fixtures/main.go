// cmd/lsports-fixtures is a quota-free fixture browser for the LSPORTS
// TRADE360 in-play package. It calls POST /Fixtures/Get with the operator's
// sport / time-window filters and prints a table of upcoming candidates so
// the operator can pick which one to spend a quota slot on via
// cmd/lsports-subscribe.
//
// This command does NOT call Subscribe/Unsubscribe and does NOT change
// quota. It is safe to invoke as often as needed (subject to the
// per-endpoint 5-second LSPORTS rate limit).
//
// Usage:
//
//	go run ./cmd/lsports-fixtures               # default: next 3h, all configured SPORT_IDS
//	go run ./cmd/lsports-fixtures --window 6h   # next 6 hours
//	go run ./cmd/lsports-fixtures --sport 54094 # Tennis only
//	go run ./cmd/lsports-fixtures --output json
//
// Required env (same as cmd/lsports-quota):
//
//	LSPORTS_USERNAME, LSPORTS_PASSWORD
//	LSPORTS_INPLAY_PACKAGE_ID   (default 4044)
//	LSPORTS_SPORT_IDS           (comma-separated; default uses --sport flag or returns all)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/source/lsports"
)

func main() {
	var (
		window     = flag.Duration("window", 3*time.Hour, "look-ahead window for upcoming fixtures (e.g. 1h, 6h)")
		sportFlag  = flag.Int("sport", 0, "restrict to this single SportId; 0 = use LSPORTS_SPORT_IDS env (or all)")
		outputFmt  = flag.String("output", "table", "table | json")
		limitFlag  = flag.Int("limit", 50, "max fixtures to print")
		showAll    = flag.Bool("all", false, "ignore the time window and print everything LSPORTS returns")
		quotaCheck = flag.Bool("quota", true, "also print current /Package/GetPackageQuota result (zero-cost call)")
		source     = flag.String("source", "metadata", "metadata (catalog /Fixtures/Get on stm-api) | snapshot (live /InPlay/GetFixtures on stm-snapshot) | events (broadest /InPlay/GetEvents)")
	)
	flag.Parse()

	_, _ = config.Load() // dotenv side effect only

	base := envOr("LSPORTS_BASE_URL", lsports.DefaultBaseURL)
	user := strings.TrimSpace(os.Getenv("LSPORTS_USERNAME"))
	pw := os.Getenv("LSPORTS_PASSWORD")
	pkgStr := envOr("LSPORTS_INPLAY_PACKAGE_ID", "4044")
	pkg, err := strconv.Atoi(pkgStr)
	if err != nil {
		fail("LSPORTS_INPLAY_PACKAGE_ID must be integer: %v", err)
	}
	if user == "" || pw == "" {
		fail("LSPORTS_USERNAME and LSPORTS_PASSWORD are required (set them in .env)")
	}

	sportIDs := resolveSportIDs(*sportFlag)

	// Pick the host + endpoint based on --source. Metadata is the catalog
	// view (/Fixtures/Get on stm-api.lsports.eu); snapshot is the live
	// in-play surface (/InPlay/GetFixtures on stm-snapshot.lsports.eu);
	// events is the broadest dump (/InPlay/GetEvents).
	snapshotBase := envOr("LSPORTS_SNAPSHOT_BASE_URL", lsports.DefaultSnapshotBaseURL)
	var clientBase, endpointLabel string
	switch *source {
	case "metadata":
		clientBase = base
		endpointLabel = "/Fixtures/Get @ " + base
	case "snapshot":
		clientBase = snapshotBase
		endpointLabel = "/InPlay/GetFixtures @ " + snapshotBase
	case "events":
		clientBase = snapshotBase
		endpointLabel = "/InPlay/GetEvents @ " + snapshotBase
	default:
		fail("--source must be one of: metadata | snapshot | events (got %q)", *source)
	}

	client := lsports.NewClient(clientBase, lsports.Credentials{
		PackageID: pkg,
		Username:  user,
		Password:  pw,
	}, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	now := time.Now().UTC()
	var (
		from = now
		to   = now.Add(*window)
	)

	var (
		fixtures []lsports.Fixture
		raw      []byte
	)
	switch *source {
	case "metadata":
		filter := lsports.FixturesFilter{SportIDs: sportIDs}
		if !*showAll {
			filter.FromDate = &from
			filter.ToDate = &to
		}
		fmt.Fprintf(os.Stderr, "fetching via %s: sport_ids=%v window=%s..%s (showAll=%v)\n",
			endpointLabel, sportIDs, from.Format(time.RFC3339), to.Format(time.RFC3339), *showAll)
		fixtures, raw, err = client.GetFixtures(ctx, filter)
	case "snapshot":
		snapFilter := lsports.SnapshotFilter{Sports: sportIDs}
		fmt.Fprintf(os.Stderr, "fetching via %s: sport_ids=%v\n", endpointLabel, sportIDs)
		var resp lsports.SnapshotResponse
		resp, err = client.GetInPlayFixtures(ctx, snapFilter)
		raw = resp.Raw
		// Best-effort: try to parse the Body as []Fixture so the table
		// view works. If the schema differs we still print the raw bytes.
		fixtures = parseSnapshotFixtures(resp.Raw)
	case "events":
		snapFilter := lsports.SnapshotFilter{Sports: sportIDs}
		fmt.Fprintf(os.Stderr, "fetching via %s: sport_ids=%v\n", endpointLabel, sportIDs)
		var resp lsports.SnapshotResponse
		resp, err = client.GetInPlayEvents(ctx, snapFilter)
		raw = resp.Raw
		fixtures = parseSnapshotFixtures(resp.Raw)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s FAILED: %v\n", endpointLabel, err)
		fmt.Fprintln(os.Stderr, "--- raw response (first 1KB) ---")
		fmt.Fprintln(os.Stderr, head(string(raw), 1024))
		os.Exit(2)
	}
	if len(fixtures) == 0 {
		fmt.Fprintf(os.Stderr, "--- %s returned 0 parsed fixtures; raw response head (first 2KB) ---\n", endpointLabel)
		fmt.Fprintln(os.Stderr, head(string(raw), 2048))
		fmt.Fprintln(os.Stderr, "--- end raw ---")
	}

	// Sort by commence_time ascending so the next-to-start fixtures are at
	// the top — that is the operational priority for Subscribe-Then-Maybe-
	// Refund (less time to monitor PM coverage = higher urgency).
	sort.SliceStable(fixtures, func(i, j int) bool {
		ti, oi := fixtures[i].CommenceTime()
		tj, oj := fixtures[j].CommenceTime()
		switch {
		case !oi && !oj:
			return fixtures[i].ID < fixtures[j].ID
		case !oi:
			return false
		case !oj:
			return true
		default:
			return ti.Before(tj)
		}
	})

	if len(fixtures) > *limitFlag {
		fixtures = fixtures[:*limitFlag]
	}

	var quota *lsports.Quota
	if *quotaCheck && *source == "metadata" {
		q, _, qerr := client.GetPackageQuota(ctx)
		if qerr != nil {
			fmt.Fprintf(os.Stderr, "warning: quota check failed: %v\n", qerr)
		} else {
			quota = &q
		}
	}

	switch *outputFmt {
	case "json":
		printJSON(fixtures, quota)
	case "table", "":
		printTable(fixtures, quota, now)
	default:
		fail("--output must be table or json (got %q)", *outputFmt)
	}
}

// parseSnapshotFixtures tries to extract a slice of Fixture from a snapshot
// response Body. The snapshot Body shape is array-of-events; we attempt the
// same minimal projection used by the metadata client. Failure returns an
// empty slice (caller still gets the raw bytes for inspection).
func parseSnapshotFixtures(raw []byte) []lsports.Fixture {
	var env struct {
		Body []lsports.Fixture `json:"Body"`
	}
	if err := json.Unmarshal(raw, &env); err == nil {
		return env.Body
	}
	return nil
}

func resolveSportIDs(flagSport int) []int {
	if flagSport > 0 {
		return []int{flagSport}
	}
	raw := strings.TrimSpace(os.Getenv("LSPORTS_SPORT_IDS"))
	if raw == "" {
		return nil
	}
	var ids []int
	for _, part := range strings.Split(raw, ",") {
		p := strings.TrimSpace(part)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			fail("LSPORTS_SPORT_IDS bad entry %q: %v", p, err)
		}
		ids = append(ids, n)
	}
	return ids
}

func printTable(fixtures []lsports.Fixture, quota *lsports.Quota, now time.Time) {
	if quota != nil {
		fmt.Printf("Quota: %d/%d remaining (used %d)\n",
			quota.CreditRemaining, quota.CreditLimit, quota.UsedCredit)
	}
	fmt.Printf("\n%d fixture(s):\n", len(fixtures))
	if len(fixtures) == 0 {
		fmt.Println("  (none returned by LSPORTS for this filter)")
		return
	}
	fmt.Printf("%-12s  %-9s  %-12s  %-22s  %-32s  %s\n",
		"FixtureId", "Sport", "League", "Starts (UTC)", "Home", "Away (mins until start)")
	fmt.Println(strings.Repeat("-", 130))
	for _, f := range fixtures {
		startStr := "-"
		untilStr := "?"
		if t, ok := f.CommenceTime(); ok {
			startStr = t.Format("01-02 15:04")
			minutes := int(t.Sub(now).Minutes())
			switch {
			case minutes < 0:
				untilStr = fmt.Sprintf("(%dm ago)", -minutes)
			default:
				untilStr = fmt.Sprintf("(in %dm)", minutes)
			}
		}
		fmt.Printf("%-12d  %-9s  %-12.12s  %-22s  %-32.32s  %-24.24s %s\n",
			f.ID,
			truncStr(f.Sport.Name, 9),
			f.League.Name,
			startStr,
			f.HomeParticipant(),
			f.AwayParticipant(),
			untilStr,
		)
	}
	fmt.Println()
	fmt.Println("To subscribe one (1 quota slot deducted; refundable before InProgress):")
	fmt.Println("  go run ./cmd/lsports-subscribe --fixture-id <ID> --operator you --reason \"...\" \\")
	fmt.Println("      --polymarket-confirmed yes  --really-subscribe")
}

type tableRow struct {
	FixtureID    int64  `json:"FixtureId"`
	Sport        string `json:"Sport"`
	League       string `json:"League"`
	StartUTC     string `json:"StartUTC,omitempty"`
	MinutesUntil *int   `json:"MinutesUntilStart,omitempty"`
	Home         string `json:"Home"`
	Away         string `json:"Away"`
}

func printJSON(fixtures []lsports.Fixture, quota *lsports.Quota) {
	now := time.Now().UTC()
	rows := make([]tableRow, 0, len(fixtures))
	for _, f := range fixtures {
		row := tableRow{
			FixtureID: f.ID,
			Sport:     f.Sport.Name,
			League:    f.League.Name,
			Home:      f.HomeParticipant(),
			Away:      f.AwayParticipant(),
		}
		if t, ok := f.CommenceTime(); ok {
			row.StartUTC = t.Format(time.RFC3339)
			minutes := int(t.Sub(now).Minutes())
			row.MinutesUntil = &minutes
		}
		rows = append(rows, row)
	}
	out := map[string]any{
		"fixtures": rows,
	}
	if quota != nil {
		out["quota"] = quota
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func head(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "\n... [truncated]"
}

func truncStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
