package store

import (
	"strings"
	"testing"
)

func TestSportsbookBetfairCoverageAuditMigrationShape(t *testing.T) {
	sql := readMigration(t, "018_sportsbook_betfair_coverage_audit.sql")
	for _, want := range []string{
		"CREATE OR REPLACE VIEW sportsbook_betfair_comparison_coverage_audit",
		"FROM fair_value_ready_betfair_exchange_quotes",
		"FROM betfair_exchange_quote_readiness",
		"FROM fair_value_moneyline_consensus",
		"FROM sportsbook_betfair_moneyline_comparison",
		"mapped_betfair_quote_count",
		"ready_betfair_runner_book_count",
		"sportsbook_consensus_outcome_count",
		"comparison_row_count",
		"gap_reason",
		"mapping_missing",
		"no_sportsbook_consensus",
		"no_matching_outcome",
		"unsupported_market_family",
		"comparison_ready",
		"stale_or_low_confidence",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("coverage audit migration missing %q:\n%s", want, sql)
		}
	}
}

func TestCoverageAuditSQLIsReadOnlyDiagnostic(t *testing.T) {
	sql := strings.ToLower(readMigration(t, "018_sportsbook_betfair_coverage_audit.sql"))
	for _, forbidden := range []string{
		"insert into",
		"update ",
		"delete from",
		"order placement",
		"private_key",
		"wallet",
		"trade_signal",
		"buy_signal",
		"sell_signal",
		"recommendation",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("coverage audit migration must remain read-only and non-trading; found %q:\n%s", forbidden, sql)
		}
	}
}

func TestPolymarketComparisonReadinessMigrationShape(t *testing.T) {
	sql := readMigration(t, "019_polymarket_comparison_readiness_views.sql")
	for _, want := range []string{
		"CREATE OR REPLACE VIEW polymarket_comparison_coverage_audit",
		"FROM canonical_events",
		"FROM confirmed_polymarket_event_aliases",
		"FROM polymarket_markets",
		"FROM polymarket_tokens",
		"FROM polymarket_price_snapshots",
		"FROM fair_value_moneyline_consensus",
		"FROM fair_value_ready_betfair_exchange_quotes",
		"FROM betfair_exchange_quote_readiness",
		"FROM sportsbook_polymarket_moneyline_edges",
		"FROM sportsbook_polymarket_moneyline_edge_readiness",
		"polymarket_token_count",
		"polymarket_price_snapshot_count",
		"latest_polymarket_price_time",
		"has_best_bid",
		"has_best_ask",
		"has_depth_or_liquidity",
		"polymarket_stale_state",
		"resolution_status",
		"resolution_verified",
		"comparison_ready",
		"gap_reason",
		"next_manual_action",
		"external_fair_value_missing",
		"sportsbook_consensus_missing",
		"polymarket_mapping_missing",
		"polymarket_token_missing",
		"polymarket_price_missing",
		"polymarket_price_stale",
		"polymarket_liquidity_low",
		"resolution_unverified",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("polymarket readiness migration missing %q:\n%s", want, sql)
		}
	}
}

func TestPolymarketComparisonReadinessMigrationIsReadOnlyDiagnostic(t *testing.T) {
	sql := strings.ToLower(readMigration(t, "019_polymarket_comparison_readiness_views.sql"))
	for _, forbidden := range []string{
		"insert into",
		"update ",
		"delete from",
		"order placement",
		"private_key",
		"wallet",
		"trade_signal",
		"buy_signal",
		"sell_signal",
		"recommendation",
		"midpoint_executable",
		"executable_midpoint",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("polymarket readiness migration must remain read-only and non-trading; found %q:\n%s", forbidden, sql)
		}
	}
}
