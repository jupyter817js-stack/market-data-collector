package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestApproveSQLCreatesOrUpdatesConfirmedAlias(t *testing.T) {
	for _, sql := range []string{insertPolymarketConfirmedAliasSQL, updatePolymarketConfirmedAliasSQL} {
		if !strings.Contains(sql, "polymarket_event_aliases") {
			t.Fatalf("approval SQL must target polymarket_event_aliases:\n%s", sql)
		}
		if !strings.Contains(sql, "confirmed") {
			t.Fatalf("approval SQL must mark aliases confirmed:\n%s", sql)
		}
	}
	if !strings.Contains(upsertManualEventAliasSQL, "manual_event_aliases") {
		t.Fatalf("approval should also preserve a manual alias record:\n%s", upsertManualEventAliasSQL)
	}
	if !strings.Contains(insertMatchReviewDecisionSQL, "match_review_decisions") {
		t.Fatalf("approval should record a reviewer decision:\n%s", insertMatchReviewDecisionSQL)
	}
}

func TestRejectSQLDoesNotCreateAlias(t *testing.T) {
	if !strings.Contains(insertMatchReviewDecisionSQL, "match_review_decisions") {
		t.Fatalf("rejection should record a reviewer decision:\n%s", insertMatchReviewDecisionSQL)
	}
	if strings.Contains(insertMatchReviewDecisionSQL, "polymarket_event_aliases") {
		t.Fatalf("decision insert must not create edge-eligible aliases:\n%s", insertMatchReviewDecisionSQL)
	}
}

func TestOnlyConfirmedAliasesUnlockPolymarketEdges(t *testing.T) {
	sql := readMigration(t, "006_polymarket_edges.sql")
	if !strings.Contains(sql, "lower(pa.match_status) = 'confirmed'") {
		t.Fatalf("edge view must require confirmed polymarket aliases:\n%s", sql)
	}
	if !strings.Contains(sql, "sportsbook_polymarket_moneyline_edges") {
		t.Fatalf("edge migration missing sportsbook_polymarket_moneyline_edges view")
	}
}

func TestMatchReviewWorkflowMigrationExposesReviewViews(t *testing.T) {
	sql := readMigration(t, "007_match_review_workflow.sql")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS match_review_decisions",
		"CREATE OR REPLACE VIEW unresolved_polymarket_event_matches",
		"CREATE OR REPLACE VIEW ambiguous_polymarket_event_matches",
		"CREATE OR REPLACE VIEW confirmed_polymarket_event_aliases",
		"decision_status IN ('approved', 'rejected')",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("match review migration missing %q:\n%s", want, sql)
		}
	}
}

func readMigration(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
	if err != nil {
		t.Fatalf("read migration %s: %v", name, err)
	}
	return string(body)
}
