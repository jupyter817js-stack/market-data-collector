package store

import (
	"strings"
	"testing"
)

func TestPolymarketSQLShapes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "event upsert",
			sql:  upsertPolymarketEventSQL,
			want: []string{"INSERT INTO polymarket_events", "ON CONFLICT (polymarket_event_id)", "gamma_payload"},
		},
		{
			name: "market upsert",
			sql:  upsertPolymarketMarketSQL,
			want: []string{"INSERT INTO polymarket_markets", "ON CONFLICT (condition_id)", "gamma_payload", "clob_payload"},
		},
		{
			name: "token upsert",
			sql:  upsertPolymarketTokenSQL,
			want: []string{"INSERT INTO polymarket_tokens", "ON CONFLICT (token_id)", "condition_id"},
		},
		{
			name: "sync run insert",
			sql:  insertPolymarketSyncRunSQL,
			want: []string{"INSERT INTO polymarket_sync_runs", "pages_fetched", "errors_count"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, want := range tc.want {
				if !strings.Contains(tc.sql, want) {
					t.Fatalf("SQL missing %q:\n%s", want, tc.sql)
				}
			}
		})
	}
}

func TestPolymarketPriceSnapshotSQLIsAppendOnly(t *testing.T) {
	if !strings.Contains(insertPolymarketPriceSnapshotSQL, "INSERT INTO polymarket_price_snapshots") {
		t.Fatalf("snapshot SQL targets wrong table:\n%s", insertPolymarketPriceSnapshotSQL)
	}
	if strings.Contains(insertPolymarketPriceSnapshotSQL, "ON CONFLICT") {
		t.Fatalf("price snapshots must be append-only, got upsert SQL:\n%s", insertPolymarketPriceSnapshotSQL)
	}
	for _, want := range []string{
		"best_bid",
		"best_ask",
		"midpoint",
		"spread",
		"last_trade_price",
		"liquidity",
		"depth_summary",
		"source_timestamp",
		"raw_payload",
	} {
		if !strings.Contains(insertPolymarketPriceSnapshotSQL, want) {
			t.Fatalf("snapshot SQL missing %q:\n%s", want, insertPolymarketPriceSnapshotSQL)
		}
	}
}

func TestPolymarketPriceSnapshotQualityMigration(t *testing.T) {
	sql := readMigration(t, "008_polymarket_price_snapshot_quality.sql")
	for _, want := range []string{
		"ADD COLUMN IF NOT EXISTS best_bid",
		"ADD COLUMN IF NOT EXISTS best_ask",
		"ADD COLUMN IF NOT EXISTS midpoint",
		"ADD COLUMN IF NOT EXISTS spread",
		"ADD COLUMN IF NOT EXISTS raw_payload",
		"polymarket_price_snapshots_market_received_idx",
		"polymarket_price_snapshots_condition_received_idx",
		"edge_to_buy_cost",
		"analytical_midpoint_edge",
		"best_ask_buy_cost",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("quality migration missing %q:\n%s", want, sql)
		}
	}
}

func TestPolymarketSyncRunsMigration(t *testing.T) {
	sql := readMigration(t, "010_polymarket_sync_runs.sql")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS polymarket_sync_runs",
		"sync_type text NOT NULL",
		"pages_fetched int NOT NULL",
		"prices_fetched int NOT NULL",
		"errors_count int NOT NULL",
		"polymarket_sync_runs_type_started_idx",
		"polymarket_sync_runs_status_started_idx",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("sync runs migration missing %q:\n%s", want, sql)
		}
	}
}

func TestJSONPayloadDefaultsAndValidation(t *testing.T) {
	payload, err := jsonPayload(nil)
	if err != nil {
		t.Fatalf("jsonPayload(nil) returned error: %v", err)
	}
	if payload != "{}" {
		t.Fatalf("payload=%q want {}", payload)
	}

	payload, err = jsonPayload([]byte(`{"ok":true}`))
	if err != nil {
		t.Fatalf("jsonPayload(valid) returned error: %v", err)
	}
	if payload != `{"ok":true}` {
		t.Fatalf("payload=%q", payload)
	}

	if _, err := jsonPayload([]byte(`{bad`)); err == nil {
		t.Fatal("jsonPayload(invalid) returned nil error")
	}
}
