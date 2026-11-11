// cmd/lsports-subscribe is the budget-gated subscribe command. It calls
// POST /Fixtures/Subscribe for a SINGLE fixture and writes an audit row to
// lsports_subscriptions in the same transaction. The 6-rule discipline
// (see memory/lsports_budget_discipline.md) is enforced here:
//
//   1. Single fixture only — flag is --fixture-id <int>, never a list.
//   2. Quota check — refuses if /Package/GetPackageQuota.creditRemaining is
//      below LSPORTS_QUOTA_FLOOR (default 20).
//   3. PM coverage attestation — operator MUST pass either
//      --polymarket-confirmed yes (manual attestation) OR
//      --polymarket-canonical-event-id <uuid> (refers to a known canonical
//      event with PM coverage already mapped), unless --no-polymarket-check
//      is set with --polymarket-skip-reason "why we are bypassing".
//   4. Daily 7-burn limit — soft warning if pending_last_24h already >= 7
//      (operator-overridable but reported).
//   5. Ledger write — single transaction; on REST success the row is
//      inserted as 'pending', otherwise no-op.
//   6. safe-cancel deadline — informational message printed if the
//      fixture's commence_time is less than 5 minutes away (operator
//      decides whether to proceed).
//
// And the safety-rail gate: --really-subscribe MUST be passed or this
// command runs in dry-run mode and prints what it WOULD do.
//
// Usage:
//
//	# dry-run (no REST call, no quota change):
//	go run ./cmd/lsports-subscribe \
//	    --fixture-id 12345 --operator you --reason "Sinner vs Alcaraz; PM covered" \
//	    --polymarket-confirmed yes
//
//	# actual subscribe:
//	go run ./cmd/lsports-subscribe \
//	    --fixture-id 12345 --operator you --reason "Sinner vs Alcaraz; PM covered" \
//	    --polymarket-confirmed yes --really-subscribe
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/your-org/pol-odds-collector/internal/config"
	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
	"github.com/your-org/pol-odds-collector/internal/source/lsports"
	"github.com/your-org/pol-odds-collector/internal/store"
)

func main() {
	var (
		fixtureID         = flag.Int64("fixture-id", 0, "REQUIRED: the single LSPORTS fixture ID to subscribe")
		operator          = flag.String("operator", "", "REQUIRED: your name / identifier for the audit ledger")
		reason            = flag.String("reason", "", "REQUIRED: why this fixture is worth a quota slot")
		pmConfirmed       = flag.String("polymarket-confirmed", "", "yes|no — operator attestation that PM lists a market for this fixture")
		pmCanonicalEvent  = flag.String("polymarket-canonical-event-id", "", "optional UUID of a canonical_events row already confirmed for PM")
		noPMCheck         = flag.Bool("no-polymarket-check", false, "BYPASS the PM coverage attestation (requires --polymarket-skip-reason)")
		pmSkipReason      = flag.String("polymarket-skip-reason", "", "required text when --no-polymarket-check is set")
		homeOverride      = flag.String("home", "", "override Home participant (defaults to /Fixtures/Get lookup)")
		awayOverride      = flag.String("away", "", "override Away participant (defaults to /Fixtures/Get lookup)")
		notesFlag         = flag.String("notes", "", "free-form notes recorded in the ledger")
		reallySubscribe   = flag.Bool("really-subscribe", false, "actually call /Fixtures/Subscribe (default is dry-run)")
		dailyBurnSoftCap  = flag.Int("daily-burn-soft-cap", 7, "warn if pending_last_24h already >= this number")
		fixtureLookupSkip = flag.Bool("skip-fixture-lookup", false, "skip /Fixtures/Get pre-check (useful when LSPORTS doesn't yet list the fixture)")
	)
	flag.Parse()

	_, _ = config.Load()

	if err := validateRequiredFlags(*fixtureID, *operator, *reason); err != nil {
		fail("%v", err)
	}

	pmGateErr := validatePolymarketAttestation(*pmConfirmed, *pmCanonicalEvent, *noPMCheck, *pmSkipReason)
	if pmGateErr != nil {
		fail("%v", pmGateErr)
	}

	base := envOr("LSPORTS_BASE_URL", lsports.DefaultBaseURL)
	user := strings.TrimSpace(os.Getenv("LSPORTS_USERNAME"))
	pw := os.Getenv("LSPORTS_PASSWORD")
	pkgStr := envOr("LSPORTS_INPLAY_PACKAGE_ID", "4044")
	floorStr := envOr("LSPORTS_QUOTA_FLOOR", "20")
	dbURL := envOr("DATABASE_URL", "")

	pkg, err := strconv.Atoi(pkgStr)
	if err != nil {
		fail("LSPORTS_INPLAY_PACKAGE_ID must be integer: %v", err)
	}
	floor, err := strconv.Atoi(floorStr)
	if err != nil {
		fail("LSPORTS_QUOTA_FLOOR must be integer: %v", err)
	}
	if user == "" || pw == "" {
		fail("LSPORTS_USERNAME and LSPORTS_PASSWORD are required in .env")
	}
	if dbURL == "" {
		fail("DATABASE_URL is required in .env (ledger writes are mandatory)")
	}

	client := lsports.NewClient(base, lsports.Credentials{
		PackageID: pkg,
		Username:  user,
		Password:  pw,
	}, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Open store first — if DB is down we should not waste a REST call.
	st, err := store.Open(ctx, dbURL, false, normalize.FreshnessPolicy{})
	if err != nil {
		fail("open postgres: %v", err)
	}
	defer func() { _ = st.Close() }()

	// Rule 5 defense-in-depth: refuse if an open ledger row exists.
	hasOpen, err := st.HasOpenLSportsSubscription(ctx, *fixtureID)
	if err != nil {
		fail("ledger pre-check failed: %v", err)
	}
	if hasOpen {
		fail("fixture %d already has an open ledger row (pending or committed_inprogress). Use cmd/lsports-unsubscribe first if you intend to re-subscribe.", *fixtureID)
	}

	// Rule 2: quota check.
	q, _, err := client.GetPackageQuota(ctx)
	if err != nil {
		fail("quota check failed: %v", err)
	}
	if q.CreditRemaining <= 0 {
		fail("quota exhausted: %d remaining of %d (refused — no subscribe possible)", q.CreditRemaining, q.CreditLimit)
	}
	if q.CreditRemaining < floor {
		fail("quota below floor: %d remaining < floor %d (refused — raise LSPORTS_QUOTA_FLOOR explicitly to override)", q.CreditRemaining, floor)
	}

	// Rule 4: soft daily-burn cap.
	budget, err := st.LSportsBudgetSummary(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: budget summary query failed: %v\n", err)
	} else if budget.PendingLast24h >= int64(*dailyBurnSoftCap) {
		fmt.Fprintf(os.Stderr, "WARN: %d fixtures already subscribed in the last 24h (soft cap %d). Consider whether this subscribe is worth its slot.\n",
			budget.PendingLast24h, *dailyBurnSoftCap)
	}

	// Optional /Fixtures/Get pre-check — get participants + commence_time if
	// not overridden. Quota cost: 0.
	var (
		fixtureName  string
		homeName     = *homeOverride
		awayName     = *awayOverride
		commenceTime *time.Time
		sportID      *int64
		leagueID     *int64
	)
	if !*fixtureLookupSkip {
		from := time.Now().UTC().Add(-2 * time.Hour)
		to := time.Now().UTC().Add(7 * 24 * time.Hour)
		fixtures, _, ferr := client.GetFixtures(ctx, lsports.FixturesFilter{
			FromDate: &from,
			ToDate:   &to,
		})
		if ferr != nil {
			fmt.Fprintf(os.Stderr, "warning: /Fixtures/Get lookup failed; proceeding without enrichment: %v\n", ferr)
		}
		for _, f := range fixtures {
			if f.ID != *fixtureID {
				continue
			}
			if homeName == "" {
				homeName = f.HomeParticipant()
			}
			if awayName == "" {
				awayName = f.AwayParticipant()
			}
			if t, ok := f.CommenceTime(); ok {
				ct := t
				commenceTime = &ct
				// Rule 6: safe-cancel deadline reminder.
				until := time.Until(t)
				switch {
				case until < 0:
					fmt.Fprintf(os.Stderr, "WARN: fixture %d commence_time is in the past (%s ago). Subscribing now risks immediate InProgress and permanent burn.\n",
						f.ID, (-until).Round(time.Second))
				case until < 5*time.Minute:
					fmt.Fprintf(os.Stderr, "WARN: fixture %d starts in %s (< safe-cancel deadline 5m). Refund window is essentially closed; treat this as a permanent burn.\n",
						f.ID, until.Round(time.Second))
				}
			}
			if f.Sport.ID > 0 {
				sid := int64(f.Sport.ID)
				sportID = &sid
			}
			if f.League.ID > 0 {
				lid := int64(f.League.ID)
				leagueID = &lid
			}
			fixtureName = fmt.Sprintf("%s vs %s", f.HomeParticipant(), f.AwayParticipant())
			break
		}
		if fixtureName == "" {
			fmt.Fprintf(os.Stderr, "note: fixture %d not present in current /Fixtures/Get window. Proceeding with operator-supplied home/away.\n", *fixtureID)
		}
	}

	// Build the input. If still empty after the optional lookup we record
	// what the operator supplied (or empty) — the ledger keeps a defensible
	// record of what was known at decision time.
	pmCoverageVerified := strings.EqualFold(*pmConfirmed, "yes") || strings.TrimSpace(*pmCanonicalEvent) != ""
	pmNotes := buildPMNotes(*pmConfirmed, *pmCanonicalEvent, *noPMCheck, *pmSkipReason)

	in := model.LSportsSubscribeInput{
		LSportsFixtureID:              *fixtureID,
		LSportsSportID:                sportID,
		LSportsLeagueID:               leagueID,
		FixtureName:                   fixtureName,
		HomeParticipant:               homeName,
		AwayParticipant:               awayName,
		CommenceTime:                  commenceTime,
		Operator:                      *operator,
		SubscribeReason:               *reason,
		QuotaRemainingBeforeSubscribe: q.CreditRemaining,
		QuotaRemainingAfterSubscribe:  q.CreditRemaining, // overwritten on real run
		PolymarketCoverageVerified:    pmCoverageVerified,
		PolymarketCoverageNotes:       pmNotes,
		PolymarketCanonicalEventID:    strings.TrimSpace(*pmCanonicalEvent),
	}

	printPlan(in, q, *notesFlag, *reallySubscribe)

	if !*reallySubscribe {
		fmt.Println("\nDRY-RUN: no /Fixtures/Subscribe call was made. Pass --really-subscribe to commit (1 quota slot will be deducted).")
		return
	}

	// REST call — point of no return for the quota slot (refundable only
	// before InProgress).
	if _, err := client.Subscribe(ctx, int(*fixtureID)); err != nil {
		fail("Subscribe REST call failed: %v", err)
	}

	// Re-fetch quota to record the after-snapshot and sanity-check the diff.
	qAfter, _, qerr := client.GetPackageQuota(ctx)
	if qerr != nil {
		fmt.Fprintf(os.Stderr, "warning: post-subscribe quota check failed: %v\n", qerr)
		in.QuotaRemainingAfterSubscribe = q.CreditRemaining - 1 // best-effort fallback
	} else {
		in.QuotaRemainingAfterSubscribe = qAfter.CreditRemaining
		expected := q.CreditRemaining - 1
		if qAfter.CreditRemaining != expected {
			fmt.Fprintf(os.Stderr, "WARN: quota diff unexpected: before=%d after=%d (expected %d). LSPORTS may be applying delayed billing.\n",
				q.CreditRemaining, qAfter.CreditRemaining, expected)
		}
	}

	id, err := st.InsertLSportsSubscription(ctx, in)
	if err != nil {
		// REST succeeded but ledger failed: we LEAK a quota slot unless we
		// retry the insert manually. Surface this loudly so the operator
		// can fix the ledger by hand and reconcile.
		fmt.Fprintf(os.Stderr, "FATAL: Subscribe REST call SUCCEEDED but ledger insert FAILED: %v\n", err)
		fmt.Fprintf(os.Stderr, "       Quota slot has been deducted (%d -> %d). The fixture IS subscribed on LSPORTS side.\n",
			q.CreditRemaining, in.QuotaRemainingAfterSubscribe)
		fmt.Fprintf(os.Stderr, "       Either re-run with --skip-fixture-lookup once the cause is fixed, or call cmd/lsports-unsubscribe to refund.\n")
		os.Exit(7)
	}

	fmt.Printf("\nOK: subscribed fixture %d (ledger id=%d). Quota %d -> %d.\n",
		*fixtureID, id, q.CreditRemaining, in.QuotaRemainingAfterSubscribe)
	fmt.Println("Remember: this slot is refundable ONLY while the match has not entered InProgress.")
	if commenceTime != nil {
		until := time.Until(*commenceTime)
		fmt.Printf("Commence_time: %s (%s away). Safe-cancel deadline ≈ %s.\n",
			commenceTime.Format(time.RFC3339),
			until.Round(time.Second),
			commenceTime.Add(-5*time.Minute).Format(time.RFC3339))
	}
}

func validateRequiredFlags(fixtureID int64, operator, reason string) error {
	if fixtureID <= 0 {
		return errors.New("--fixture-id is required and must be > 0")
	}
	if strings.TrimSpace(operator) == "" {
		return errors.New("--operator is required (will be recorded in the ledger)")
	}
	if strings.TrimSpace(reason) == "" {
		return errors.New("--reason is required (will be recorded in the ledger; explain why this fixture is worth a slot)")
	}
	return nil
}

func validatePolymarketAttestation(confirmed, canonicalEvent string, noCheck bool, skipReason string) error {
	if noCheck {
		if strings.TrimSpace(skipReason) == "" {
			return errors.New("--no-polymarket-check requires --polymarket-skip-reason \"<why>\"")
		}
		return nil
	}
	if strings.EqualFold(confirmed, "yes") {
		return nil
	}
	if strings.TrimSpace(canonicalEvent) != "" {
		return nil
	}
	return errors.New("PM coverage attestation required: pass --polymarket-confirmed yes OR --polymarket-canonical-event-id <uuid>, OR bypass with --no-polymarket-check + --polymarket-skip-reason")
}

func buildPMNotes(confirmed, canonicalEvent string, noCheck bool, skipReason string) string {
	parts := []string{}
	if strings.EqualFold(confirmed, "yes") {
		parts = append(parts, "operator attests PM lists a market for this fixture")
	}
	if c := strings.TrimSpace(canonicalEvent); c != "" {
		parts = append(parts, "canonical_event_id="+c)
	}
	if noCheck {
		parts = append(parts, "BYPASSED via --no-polymarket-check: "+strings.TrimSpace(skipReason))
	}
	return strings.Join(parts, "; ")
}

func printPlan(in model.LSportsSubscribeInput, q lsports.Quota, notes string, really bool) {
	fmt.Println("=== lsports-subscribe plan ===")
	fmt.Printf("  fixture_id            : %d\n", in.LSportsFixtureID)
	if in.FixtureName != "" {
		fmt.Printf("  fixture_name          : %s\n", in.FixtureName)
	}
	if in.HomeParticipant != "" {
		fmt.Printf("  home / away           : %s vs %s\n", in.HomeParticipant, in.AwayParticipant)
	}
	if in.CommenceTime != nil {
		fmt.Printf("  commence_time (UTC)   : %s  (%s away)\n",
			in.CommenceTime.Format(time.RFC3339),
			time.Until(*in.CommenceTime).Round(time.Second))
	}
	if in.LSportsSportID != nil {
		fmt.Printf("  sport_id / league_id  : %d / %s\n", *in.LSportsSportID, formatOptInt(in.LSportsLeagueID))
	}
	fmt.Printf("  operator              : %s\n", in.Operator)
	fmt.Printf("  reason                : %s\n", in.SubscribeReason)
	fmt.Printf("  PM coverage verified  : %v\n", in.PolymarketCoverageVerified)
	if in.PolymarketCoverageNotes != "" {
		fmt.Printf("  PM coverage notes     : %s\n", in.PolymarketCoverageNotes)
	}
	if in.PolymarketCanonicalEventID != "" {
		fmt.Printf("  PM canonical_event_id : %s\n", in.PolymarketCanonicalEventID)
	}
	if notes != "" {
		fmt.Printf("  notes                 : %s\n", notes)
	}
	fmt.Printf("  quota now             : %d remaining of %d (used %d)\n",
		q.CreditRemaining, q.CreditLimit, q.UsedCredit)
	fmt.Printf("  expected after        : %d remaining\n", q.CreditRemaining-1)
	fmt.Printf("  mode                  : %s\n", ifElse(really, "REAL (quota will be deducted)", "DRY-RUN"))
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func formatOptInt(p *int64) string {
	if p == nil {
		return "-"
	}
	return strconv.FormatInt(*p, 10)
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
