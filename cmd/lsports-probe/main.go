// cmd/lsports-probe is a one-shot credential verifier for the LSPORTS
// TRADE360 REST API. It calls POST /Metadata/GetSports, prints the matching
// SportIds for Tennis / Baseball / Soccer (the trial integration's priority
// order), and dumps the head of the raw response for shape inspection. It
// makes no other LSPORTS calls and does not touch PostgreSQL.
//
// Usage:
//
//	go run ./cmd/lsports-probe
//
// Required env (set in .env):
//
//	LSPORTS_USERNAME              (e.g. besdss.com)
//	LSPORTS_PASSWORD              (LSPORTS-issued; KEEP OUT OF GIT)
//	LSPORTS_INPLAY_PACKAGE_ID     (default 4044)
//	LSPORTS_BASE_URL              (default https://stm-api.lsports.eu)
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/source/lsports"
)

func main() {
	// config.Load() runs the project's .env loader as a side effect. We
	// intentionally do not validate the rest of Config here — the probe is
	// self-contained and only needs the LSPORTS_* vars.
	_, _ = config.Load()

	base := envOr("LSPORTS_BASE_URL", lsports.DefaultBaseURL)
	user := strings.TrimSpace(os.Getenv("LSPORTS_USERNAME"))
	pw := os.Getenv("LSPORTS_PASSWORD")
	pkgStr := envOr("LSPORTS_INPLAY_PACKAGE_ID", "4044")

	pkg, err := strconv.Atoi(pkgStr)
	if err != nil {
		fail("LSPORTS_INPLAY_PACKAGE_ID must be a positive integer: %v", err)
	}
	if user == "" || pw == "" {
		fail("LSPORTS_USERNAME and LSPORTS_PASSWORD are required (set them in .env)")
	}

	client := lsports.NewClient(base, lsports.Credentials{
		PackageID: pkg,
		Username:  user,
		Password:  pw,
	}, 15*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fmt.Printf("LSPORTS probe — base=%s pkg=%d user=<redacted>\n", base, pkg)
	fmt.Println("calling POST /Sports/Get ...")
	sports, raw, err := client.GetSports(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetSports FAILED: %v\n", err)
		fmt.Fprintln(os.Stderr, "--- raw response (first 2KB) ---")
		fmt.Fprintln(os.Stderr, head(string(raw), 2048))
		os.Exit(1)
	}
	fmt.Printf("OK: %d sports returned\n\n", len(sports))

	// Priority keywords reflect the user-confirmed sport order:
	// Tennis → MLB (baseball) → Soccer. We match loosely on substring so the
	// probe surfaces whichever LSPORTS taxonomy uses (e.g. "Soccer" vs
	// "Football", "Baseball" vs "MLB").
	keywords := []string{"tennis", "baseball", "mlb", "soccer", "football"}
	fmt.Println("Priority sport candidates (filtered by keyword):")
	matched := 0
	for _, s := range sports {
		n := strings.ToLower(s.Name)
		for _, kw := range keywords {
			if strings.Contains(n, kw) {
				fmt.Printf("  SportId=%-6d  Name=%q\n", s.ID, s.Name)
				matched++
				break
			}
		}
	}
	if matched == 0 {
		fmt.Println("  (no matches — dumping all sports below for inspection)")
		for _, s := range sports {
			fmt.Printf("  SportId=%-6d  Name=%q\n", s.ID, s.Name)
		}
	}

	fmt.Println()
	fmt.Println("--- raw response (first 1KB, for shape verification) ---")
	fmt.Println(head(string(raw), 1024))
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

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
