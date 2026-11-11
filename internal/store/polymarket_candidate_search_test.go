package store

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/your-org/pol-odds-collector/internal/model"
	"github.com/your-org/pol-odds-collector/internal/normalize"
)

// TestUnresolvedReviewQueryUsesUUIDArrayMembership pins the SQL shape that
// looks up review rows whose candidate_canonical_event_ids (uuid[]) contains
// a given canonical event id. Earlier code used `ILIKE '%' || $1 || '%'` which
// raised `operator does not exist: uuid[] ~~* text` at runtime. This source-
// level guard catches a regression without needing PostgreSQL.
func TestUnresolvedReviewQueryUsesUUIDArrayMembership(t *testing.T) {
	src, err := os.ReadFile("polymarket_candidate_search.go")
	if err != nil {
		t.Fatalf("read polymarket_candidate_search.go: %v", err)
	}
	// Normalize line endings: Windows checkouts with core.autocrlf=true
	// land CRLF here, but the test's `\n}\n` body-end marker only matches
	// LF. Strip all '\r' so the same predicate works regardless of host
	// EOL style.
	body := strings.ReplaceAll(string(src), "\r\n", "\n")

	const fnMarker = "func (s *PostgresStore) unresolvedPolymarketReviewIDsForCanonical("
	startIdx := strings.Index(body, fnMarker)
	if startIdx == -1 {
		t.Fatalf("could not locate %q in source", fnMarker)
	}
	// Bound the inspection to the function body. Closing brace at column 0 of
	// the next top-level form is enough since gofmt enforces that.
	rest := body[startIdx:]
	endIdx := strings.Index(rest, "\n}\n")
	if endIdx == -1 {
		t.Fatalf("could not find end of unresolvedPolymarketReviewIDsForCanonical body")
	}
	fnBody := rest[:endIdx]

	// candidate_canonical_event_ids is uuid[] in event_match_reviews. The query
	// MUST use proper array membership (= ANY or @>), never ILIKE / ~~*.
	if !strings.Contains(fnBody, "candidate_canonical_event_ids") {
		t.Fatal("function no longer references candidate_canonical_event_ids; the array membership guard is now meaningless — re-evaluate this test")
	}
	if !(strings.Contains(fnBody, "= ANY (COALESCE(candidate_canonical_event_ids") ||
		strings.Contains(fnBody, "= ANY(COALESCE(candidate_canonical_event_ids") ||
		strings.Contains(fnBody, "candidate_canonical_event_ids @>")) {
		t.Fatalf("unresolvedPolymarketReviewIDsForCanonical must check candidate_canonical_event_ids with uuid array membership (= ANY or @>); body:\n%s", fnBody)
	}
	for _, forbidden := range []string{
		"candidate_canonical_event_ids ILIKE",
		"candidate_canonical_event_ids ilike",
		"candidate_canonical_event_ids ~~*",
		"candidate_canonical_event_ids ~~",
	} {
		if strings.Contains(fnBody, forbidden) {
			t.Fatalf("unresolvedPolymarketReviewIDsForCanonical must not apply text operators to uuid[]; found %q", forbidden)
		}
	}
	// Must hit both review views so the bug fix covers unresolved AND ambiguous.
	for _, want := range []string{"unresolved_polymarket_event_matches", "ambiguous_polymarket_event_matches"} {
		if !strings.Contains(fnBody, want) {
			t.Fatalf("unresolvedPolymarketReviewIDsForCanonical must read from %s", want)
		}
	}
}

// TestDBIntegrationSearchPolymarketCandidatesUUIDArrayQuery is the opt-in DB
// test that proves the live SQL succeeds against a real PostgreSQL instance
// when candidate_canonical_event_ids contains the canonical event id. It is
// the regression test for the operator-reported bug:
//
//	operator does not exist: uuid[] ~~* text
//
// Skipped unless RUN_DB_TESTS=1 and TEST_DATABASE_URL are set.
func TestDBIntegrationSearchPolymarketCandidatesUUIDArrayQuery(t *testing.T) {
	ctx, db, dsn := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	st, err := Open(ctx, dsn, false, normalize.FreshnessPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	commence := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	var canonicalID string
	if err := db.QueryRowContext(ctx, `
        INSERT INTO canonical_events (
            sport_key, league, home_team, away_team,
            home_team_norm, away_team_norm, commence_time, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        RETURNING id::text
    `,
		"basketball_nba", "NBA",
		"Cleveland Cavaliers", "Detroit Pistons",
		"cleveland cavaliers", "detroit pistons",
		commence,
	).Scan(&canonicalID); err != nil {
		t.Fatalf("insert canonical event: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
        INSERT INTO event_match_reviews (
            source, source_event_id, source_market_id, source_condition_id,
            event_title, raw_start_time, sport_key, league,
            match_status, confidence_score, reason,
            candidate_canonical_event_ids, payload
        ) VALUES (
            'polymarket', 'pm-evt-1', 'pm-mkt-1', 'pm-cond-1',
            'NBA: Cavaliers vs Pistons', $1, 'basketball_nba', 'NBA',
            'no_match', 50, 'fixture',
            ARRAY[$2::uuid]::uuid[],
            '{}'::jsonb
        )
    `, commence, canonicalID); err != nil {
		t.Fatalf("insert event_match_reviews: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
        INSERT INTO polymarket_events (polymarket_event_id, slug, title, active, closed, archived)
        VALUES ('pm-evt-1', 'cavaliers-vs-pistons', 'NBA: Cleveland Cavaliers vs Detroit Pistons', true, false, false)
    `); err != nil {
		t.Fatalf("insert polymarket_events: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
        INSERT INTO polymarket_markets (
            polymarket_market_id, polymarket_event_id, condition_id,
            question, slug, market_type, active, closed, archived
        ) VALUES (
            'pm-mkt-1', 'pm-evt-1', 'pm-cond-1',
            'Will the Cleveland Cavaliers beat the Detroit Pistons?', 'cavs-vs-pistons',
            'binary', true, false, false
        )
    `); err != nil {
		t.Fatalf("insert polymarket_markets: %v", err)
	}

	// This is the call path that previously failed with
	//   operator does not exist: uuid[] ~~* text
	// because the helper applied ILIKE to a uuid[] column.
	report, err := st.SearchPolymarketCandidates(ctx, model.PolymarketCandidateSearch{
		CanonicalEventID: canonicalID,
		Limit:            10,
	})
	if err != nil {
		t.Fatalf("SearchPolymarketCandidates: %v", err)
	}
	if report.CanonicalEvent.ID != canonicalID {
		t.Fatalf("returned canonical event id=%q want %q", report.CanonicalEvent.ID, canonicalID)
	}
	if len(report.UnresolvedReviewIDs) == 0 {
		t.Fatalf("UnresolvedReviewIDs is empty; expected the seeded review row to be discovered via uuid[] membership")
	}
}

// TestPolymarketSearchTokensDropsShortNoise is a tiny sanity test for the
// token extractor used by SearchPolymarketCandidates. Kept here so the file
// has at least one non-IO test that runs under `go test ./...` without
// PostgreSQL.
func TestPolymarketSearchTokensDropsShortNoise(t *testing.T) {
	got := PolymarketSearchTokens("Cleveland Cavaliers FC")
	if !contains(got, "cavaliers") {
		t.Fatalf("expected last-word token 'cavaliers' in %v", got)
	}
	if !contains(got, "cleveland") {
		t.Fatalf("expected leading-word token 'cleveland' in %v", got)
	}
	for _, token := range got {
		if len(token) < 4 {
			t.Fatalf("token %q is shorter than the 4-char floor; %v", token, got)
		}
	}
	if got := PolymarketSearchTokens(""); len(got) != 0 {
		t.Fatalf("empty team name should produce no tokens; got %v", got)
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
