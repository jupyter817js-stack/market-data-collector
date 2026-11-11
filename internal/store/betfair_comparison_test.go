package store

import (
	"strings"
	"testing"
)

func TestBetfairSportsbookComparisonMigrationShape(t *testing.T) {
	sql := readMigration(t, "015_betfair_sportsbook_comparison_views.sql")
	for _, want := range []string{
		"CREATE OR REPLACE VIEW sportsbook_betfair_moneyline_comparison",
		"CREATE OR REPLACE VIEW sportsbook_betfair_moneyline_ready_comparison",
		"CREATE OR REPLACE VIEW data_quality_betfair_sportsbook_comparison_anomalies",
		"FROM fair_value_moneyline_consensus",
		"FROM betfair_exchange_quote_readiness",
		"WHERE canonical_market_family = 'h2h'",
		"b.canonical_event_id = s.canonical_event_id",
		"b.canonical_outcome_name = s.outcome_name",
		"b.canonical_outcome_side = s.outcome_side",
		"s.average_no_vig_probability AS sportsbook_avg_no_vig_probability",
		"1::numeric / s.average_no_vig_probability",
		"1::numeric / b.best_back_price",
		"1::numeric / b.best_lay_price",
		"back_probability_gap",
		"lay_probability_gap",
		"best_back_price",
		"best_lay_price",
		"last_traded_price",
		"traded_volume",
		"betfair_readiness_state = 'ready'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("betfair sportsbook comparison migration missing %q:\n%s", want, sql)
		}
	}
}

func TestBetfairSportsbookComparisonNoForbiddenOutputTerms(t *testing.T) {
	sql := strings.ToLower(readMigration(t, "015_betfair_sportsbook_comparison_views.sql"))
	for _, forbidden := range []string{
		"buy_signal",
		"sell_signal",
		"trade_signal",
		"bet_signal",
		"arbitrage",
		"recommendation",
		"executable_price",
		"execution_decision",
		"order_placement",
		"private_key",
		"wallet",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("comparison migration must not create prohibited output %q:\n%s", forbidden, sql)
		}
	}
}
