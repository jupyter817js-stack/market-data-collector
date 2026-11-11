// cmd/lsports-unsubscribe calls POST /Fixtures/Unsubscribe for a SINGLE
// fixture and updates the lsports_subscriptions ledger to reflect what
// LSPORTS actually did. The (C) refund semantic means:
//
//   Unsubscribe BEFORE the match entered InProgress  -> quota refunded
//                                                      -> ledger row -> 'refunded'
//   Unsubscribe AFTER  the match entered InProgress  -> quota NOT refunded
//                                                      -> ledger row -> 'unsubscribed_too_late'
//
// We do not need a separate operator flag for the two cases — the post-call
// quota diff tells us which one happened (+1 = refunded, 0 = too late).
//
// Usage:
//
//	# dry-run:
//	go run ./cmd/lsports-unsubscribe --fixture-id 12345 --operator you --reason "PM dropped coverage"
//
//	# actual unsubscribe:
//	go run ./cmd/lsports-unsubscribe --fixture-id 12345 --operator you --reason "PM dropped coverage" --really-unsubscribe
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/lsports"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	var (
		fixtureID         = flag.Int64("fixture-id", 0, "REQUIRED: the LSPORTS fixture ID to unsubscribe")
		operator          = flag.String("operator", "", "REQUIRED: your name / identifier for the audit ledger")
		reason            = flag.String("reason", "", "REQUIRED: why we are unsubscribing")
		reallyUnsubscribe = flag.Bool("really-unsubscribe", false, "actually call /Fixtures/Unsubscribe (default is dry-run)")
	)
	flag.Parse()

	_, _ = config.Load()

	if *fixtureID <= 0 {
		fail("--fixture-id is required and must be > 0")
	}
	if strings.TrimSpace(*operator) == "" {
		fail("--operator is required")
	}
	if strings.TrimSpace(*reason) == "" {
		fail("--reason is required")
	}

	base := envOr("LSPORTS_BASE_URL", lsports.DefaultBaseURL)
	user := strings.TrimSpace(os.Getenv("LSPORTS_USERNAME"))
	pw := os.Getenv("LSPORTS_PASSWORD")
	pkgStr := envOr("LSPORTS_INPLAY_PACKAGE_ID", "4044")
	dbURL := envOr("DATABASE_URL", "")

	pkg, err := strconv.Atoi(pkgStr)
	if err != nil {
		fail("LSPORTS_INPLAY_PACKAGE_ID must be integer: %v", err)
	}
	if user == "" || pw == "" {
		fail("LSPORTS_USERNAME and LSPORTS_PASSWORD are required in .env")
	}
	if dbURL == "" {
		fail("DATABASE_URL is required in .env")
	}

	client := lsports.NewClient(base, lsports.Credentials{
		PackageID: pkg,
		Username:  user,
		Password:  pw,
	}, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	st, err := store.Open(ctx, dbURL, false, normalize.FreshnessPolicy{})
	if err != nil {
		fail("open postgres: %v", err)
	}
	defer func() { _ = st.Close() }()

	// Pre-call quota snapshot.
	qBefore, _, err := client.GetPackageQuota(ctx)
	if err != nil {
		fail("pre-unsubscribe quota check failed: %v", err)
	}

	fmt.Println("=== lsports-unsubscribe plan ===")
	fmt.Printf("  fixture_id      : %d\n", *fixtureID)
	fmt.Printf("  operator        : %s\n", *operator)
	fmt.Printf("  reason          : %s\n", *reason)
	fmt.Printf("  quota now       : %d remaining of %d (used %d)\n",
		qBefore.CreditRemaining, qBefore.CreditLimit, qBefore.UsedCredit)
	fmt.Printf("  expected (refund): %d remaining\n", qBefore.CreditRemaining+1)
	fmt.Printf("  expected (no refund / too late): %d remaining\n", qBefore.CreditRemaining)
	fmt.Printf("  mode            : %s\n", ifElse(*reallyUnsubscribe, "REAL", "DRY-RUN"))

	if !*reallyUnsubscribe {
		fmt.Println("\nDRY-RUN: no /Fixtures/Unsubscribe call was made. Pass --really-unsubscribe to commit.")
		return
	}

	if _, err := client.Unsubscribe(ctx, int(*fixtureID)); err != nil {
		fail("Unsubscribe REST call failed: %v", err)
	}

	qAfter, _, qerr := client.GetPackageQuota(ctx)
	if qerr != nil {
		fmt.Fprintf(os.Stderr, "WARN: post-unsubscribe quota check failed: %v\n", qerr)
		fmt.Fprintln(os.Stderr, "Recording ledger transition as 'unsubscribed_too_late' (conservative; rerun quota check manually to verify).")
		if err := st.MarkLSportsSubscriptionUnsubscribedTooLate(ctx, *fixtureID, *operator, *reason, qBefore.CreditRemaining); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: ledger update failed: %v\n", err)
			os.Exit(7)
		}
		fmt.Println("\nLedger updated: unsubscribed_too_late (defensive — verify quota manually).")
		return
	}

	diff := qAfter.CreditRemaining - qBefore.CreditRemaining
	switch {
	case diff >= 1:
		if err := st.MarkLSportsSubscriptionRefunded(ctx, *fixtureID, *operator, *reason, qAfter.CreditRemaining); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: REST refund succeeded but ledger update failed: %v\n", err)
			fmt.Fprintf(os.Stderr, "       LSPORTS-side state: quota %d -> %d (refunded). Fix ledger manually.\n",
				qBefore.CreditRemaining, qAfter.CreditRemaining)
			os.Exit(7)
		}
		fmt.Printf("\nOK: refunded. Quota %d -> %d (delta +%d). Ledger row -> refunded.\n",
			qBefore.CreditRemaining, qAfter.CreditRemaining, diff)
	case diff == 0:
		if err := st.MarkLSportsSubscriptionUnsubscribedTooLate(ctx, *fixtureID, *operator, *reason, qAfter.CreditRemaining); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: ledger update failed: %v\n", err)
			os.Exit(7)
		}
		fmt.Printf("\nUnsubscribed AFTER InProgress: quota unchanged (%d). Slot permanently consumed. Ledger row -> unsubscribed_too_late.\n",
			qAfter.CreditRemaining)
	default:
		// Negative diff would be very surprising. Record loudly.
		fmt.Fprintf(os.Stderr, "WARN: unexpected quota diff after Unsubscribe: before=%d after=%d (diff=%d).\n",
			qBefore.CreditRemaining, qAfter.CreditRemaining, diff)
		if err := st.MarkLSportsSubscriptionUnsubscribedTooLate(ctx, *fixtureID, *operator, *reason, qAfter.CreditRemaining); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: ledger update failed: %v\n", err)
			os.Exit(7)
		}
	}
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func ifElse(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

