package store

import (
	"strings"
	"testing"
)

func TestBetfairMappingMigrationShape(t *testing.T) {
	sql := readMigration(t, "013_betfair_mapping_views.sql")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS betfair_event_aliases",
		"CREATE TABLE IF NOT EXISTS betfair_market_aliases",
		"CREATE TABLE IF NOT EXISTS betfair_runner_aliases",
		"CREATE OR REPLACE VIEW betfair_unmapped_events",
		"CREATE OR REPLACE VIEW betfair_unmapped_markets",
		"CREATE OR REPLACE VIEW betfair_unmapped_runners",
		"CREATE OR REPLACE VIEW betfair_confirmed_event_aliases",
		"CREATE OR REPLACE VIEW betfair_confirmed_market_aliases",
		"CREATE OR REPLACE VIEW betfair_confirmed_runner_aliases",
		"CREATE OR REPLACE VIEW betfair_mapping_conflicts",
		"CREATE OR REPLACE VIEW fair_value_ready_betfair_exchange_quotes",
		"alias_status IN ('confirmed', 'rejected', 'unresolved', 'ambiguous')",
		"canonical_outcome_side IN ('', 'home', 'away', 'draw', 'over', 'under', 'other')",
		"q.exchange_side IN ('back', 'lay')",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("betfair mapping migration missing %q:\n%s", want, sql)
		}
	}
}

func TestBetfairMappedViewRequiresAllConfirmedAliases(t *testing.T) {
	sql := readMigration(t, "013_betfair_mapping_views.sql")
	for _, want := range []string{
		"JOIN betfair_confirmed_event_aliases",
		"JOIN betfair_confirmed_market_aliases",
		"JOIN betfair_confirmed_runner_aliases",
		"ma.canonical_event_id = ea.canonical_event_id",
		"ra.selection_id = q.selection_id",
		"ra.handicap = q.handicap",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("mapped Betfair view must require confirmed aliases via %q:\n%s", want, sql)
		}
	}
}
