// cmd/lsports-quota is a read-only LSPORTS Package quota inspector. It calls
// POST /Package/GetPackageQuota and prints the current credit limit, credit
// used, and credit remaining. The call itself does NOT consume quota.
//
// This command is the safety gate every Subscribe action depends on:
// without confident quota visibility we cannot make budget-disciplined
// subscribe decisions. Run it before and after every cmd/lsports-subscribe
// to keep a paper trail of where the 100-fixture trial budget went.
//
// Usage:
//
//	go run ./cmd/lsports-quota
//
// Required env (same as cmd/lsports-probe):
//
//	LSPORTS_USERNAME
//	LSPORTS_PASSWORD
//	LSPORTS_INPLAY_PACKAGE_ID   (default 4044)
//	LSPORTS_BASE_URL            (default https://stm-api.lsports.eu)
//	LSPORTS_QUOTA_FLOOR         (default 20 — warns when below)
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
	_, _ = config.Load() // dotenv side effect

	base := envOr("LSPORTS_BASE_URL", lsports.DefaultBaseURL)
	user := strings.TrimSpace(os.Getenv("LSPORTS_USERNAME"))
	pw := os.Getenv("LSPORTS_PASSWORD")
	pkgStr := envOr("LSPORTS_INPLAY_PACKAGE_ID", "4044")
	floorStr := envOr("LSPORTS_QUOTA_FLOOR", "20")

	pkg, err := strconv.Atoi(pkgStr)
	if err != nil {
		fail("LSPORTS_INPLAY_PACKAGE_ID must be integer: %v", err)
	}
	floor, err := strconv.Atoi(floorStr)
	if err != nil || floor < 0 {
		fail("LSPORTS_QUOTA_FLOOR must be a non-negative integer: %v", err)
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

	fmt.Printf("LSPORTS quota — base=%s pkg=%d user=<redacted>\n", base, pkg)
	fmt.Println("calling POST /Package/GetPackageQuota ...")
	q, raw, err := client.GetPackageQuota(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetPackageQuota FAILED: %v\n", err)
		fmt.Fprintln(os.Stderr, "--- raw response (first 1KB) ---")
		fmt.Fprintln(os.Stderr, head(string(raw), 1024))
		os.Exit(2)
	}

	fmt.Println()
	fmt.Printf("  creditLimit      = %d\n", q.CreditLimit)
	fmt.Printf("  usedCredit       = %d\n", q.UsedCredit)
	fmt.Printf("  creditRemaining  = %d\n", q.CreditRemaining)
	fmt.Println()

	if q.CreditLimit > 0 {
		pct := float64(q.CreditRemaining) / float64(q.CreditLimit) * 100
		fmt.Printf("  utilization      = %.1f%% used (%d of %d), %.1f%% remaining\n",
			100-pct, q.UsedCredit, q.CreditLimit, pct)
	}

	fmt.Println()
	switch {
	case q.CreditRemaining < 0:
		fmt.Println("STATE: ERROR — creditRemaining is negative; verify LSPORTS_INPLAY_PACKAGE_ID")
		os.Exit(3)
	case q.CreditRemaining == 0:
		fmt.Println("STATE: EXHAUSTED — no remaining quota; Subscribe will fail")
		os.Exit(4)
	case q.CreditRemaining < floor:
		fmt.Printf("STATE: BELOW FLOOR — %d remaining < floor %d. Subscribe gated commands will refuse.\n",
			q.CreditRemaining, floor)
		os.Exit(5)
	default:
		fmt.Printf("STATE: OK — %d remaining >= floor %d\n", q.CreditRemaining, floor)
	}

	// (C) refund semantic reminder — every operator interaction with this
	// command should remind them of the quota model so subscribe decisions
	// are made with the full picture.
	fmt.Println()
	fmt.Println("Quota model (LSPORTS Support 2026-05-27):")
	fmt.Println("  Subscribe deducts 1 immediately.")
	fmt.Println("  Unsubscribe BEFORE the match enters InProgress refunds the slot.")
	fmt.Println("  Unsubscribe AFTER InProgress does NOT refund.")
	fmt.Println("  Treat usedCredit as a sum of (currently subscribed + already InProgress-burned).")
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
