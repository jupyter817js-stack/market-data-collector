package store

import (
	"strings"
	"testing"
)

func TestBetfairQualityReadinessMigrationShape(t *testing.T) {
	sql := readMigration(t, "014_betfair_quality_readiness_views.sql")
	for _, want := range []string{
		"CREATE OR REPLACE VIEW betfair_mapped_runner_books",
		"CREATE OR REPLACE VIEW data_quality_betfair_exchange_anomalies",
		"CREATE OR REPLACE VIEW betfair_exchange_quote_readiness",
		"best_back_price",
		"best_lay_price",
		"back_lay_spread",
		"relative_spread",
		"missing_back_side",
		"missing_lay_side",
		"stale_quote",
		"suspended_or_closed_market",
		"in_play_market",
		"wide_spread",
		"thin_liquidity",
		"low_confidence",
		"mapping_conflict",
		"0.05::numeric AS wide_relative_spread",
		"10::numeric AS min_visible_size",
		"0.70::numeric AS min_normalized_confidence",
		"last_traded_price",
		"traded_volume",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("betfair quality/readiness migration missing %q:\n%s", want, sql)
		}
	}
}

func TestBetfairReadinessDoesNotCreateSyntheticPrice(t *testing.T) {
	sql := readMigration(t, "014_betfair_quality_readiness_views.sql")
	for _, forbidden := range []string{
		"executable_price",
		"trade_signal",
		"buy_signal",
		"sell_signal",
		"recommendation",
	} {
		if strings.Contains(strings.ToLower(sql), forbidden) {
			t.Fatalf("betfair readiness migration must not create trading/signal output %q:\n%s", forbidden, sql)
		}
	}
}
