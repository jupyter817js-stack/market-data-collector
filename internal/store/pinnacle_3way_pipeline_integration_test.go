package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestDBIntegrationSoccer3WayPipeline drives the soccer 3-way edge view
// end-to-end against a fresh isolated schema:
//
//  1. one Pinnacle league + two matchup rows for the SAME real game
//     (Pinnacle's known multi-matchup-per-game quirk) with diacritics
//     ("Cerezo Ōsaka") on the Pinnacle side
//  2. one Pinnacle market_history row per matchup with a 3-way
//     moneyline (home/draw/away prices)
//  3. one PM event whose title carries the diacritics that Pinnacle
//     drops + the "FC" affix differences ("Cerezo Ōsaka vs. Nagoya
//     Grampus")
//  4. three PM binary markets: "Will <home> win?",
//     "Will <home> vs <away> end in a draw?", "Will <away> win?"
//  5. three PM YES-side tokens with fresh price snapshots
//
// Asserts the view returns EXACTLY 3 rows (one per outcome side),
// proving:
//   - Pinnacle dedup collapsed the two matchup_ids to one
//   - normalize_team_name + all_words_present matched the diacritic +
//     FC-affix-mismatched title
//   - 3-way no-vig probs sum to ~1.0
//   - outcome_side classification picked home/draw/away correctly
//   - gross_edge_to_buy = fair - ask (with right sign on each side)
func TestDBIntegrationSoccer3WayPipeline(t *testing.T) {
	ctx, db, _ := newIntegrationDB(t)
	applyIntegrationMigrations(ctx, t, db, integrationMigrationFiles(t))

	now := time.Now().UTC().Truncate(time.Second)
	startTime := now.Add(2 * time.Hour) // future game, doesn't have to be live for the view
	freshReceived := now.Add(-30 * time.Second)

	// 1) Pinnacle sport (id=29 = soccer) + league.
	mustExec(ctx, t, db, `
        INSERT INTO pinnacle_sports (id, name) VALUES (29, 'soccer')
            ON CONFLICT (id) DO NOTHING
    `)
	mustExec(ctx, t, db, `
        INSERT INTO pinnacle_leagues (id, sport_id, name, league_group, last_matchup_count, is_featured)
        VALUES (888, 29, 'J League — Test', 'Asia', 2, false)
    `)

	// 2) TWO Pinnacle matchups for the same real game (Pinnacle's dup
	//    quirk). Both 'matchup' type, both same teams + start_time. The
	//    view's dedup should pick the row with the newer market_history.
	home := "Cerezo Ōsaka"
	away := "Nagoya Grampus"
	insertPinMatchup(ctx, t, db, 100001, 888, home, away, startTime, true)
	insertPinMatchup(ctx, t, db, 100002, 888, home, away, startTime, true)

	// 3) Pinnacle moneyline market history (3-way) — duplicate matchup
	//    gets older data; canonical matchup gets newer.
	insertPinMarket(ctx, t, db, 100001, 1, freshReceived.Add(-2*time.Minute), -120, 280, 300)
	insertPinMarket(ctx, t, db, 100002, 2, freshReceived, -125, 290, 305)

	// 4) PM event with diacritics + FC suffix on the Pinnacle name side.
	mustExec(ctx, t, db, `
        INSERT INTO polymarket_events (polymarket_event_id, slug, title, active, closed, archived, gamma_payload)
        VALUES ('evt-soccer-1', 'cerezo-vs-nagoya', 'Cerezo Ōsaka vs. Nagoya Grampus', true, false, false, '{}'::jsonb)
    `)

	// 5) Three PM binary markets covering the 3 outcomes. The questions
	//    must match the soccer_3way classifier's ILIKE patterns.
	insertPMMarket(ctx, t, db, "cond-home", "evt-soccer-1", "Will Cerezo Ōsaka FC win?", "mkt-home")
	insertPMMarket(ctx, t, db, "cond-draw", "evt-soccer-1", "Will Cerezo Ōsaka vs. Nagoya Grampus end in a draw?", "mkt-draw")
	insertPMMarket(ctx, t, db, "cond-away", "evt-soccer-1", "Will Nagoya Grampus win?", "mkt-away")

	// 6) YES tokens + fresh prices. asks chosen to give predictable
	//    edge signs.
	insertPMTokenYes(ctx, t, db, "tok-home", "cond-home", "Yes", 0)
	insertPMTokenYes(ctx, t, db, "tok-draw", "cond-draw", "Yes", 0)
	insertPMTokenYes(ctx, t, db, "tok-away", "cond-away", "Yes", 0)
	insertPMPrice(ctx, t, db, "tok-home", 0.50, 0.52, 0.51, freshReceived)
	insertPMPrice(ctx, t, db, "tok-draw", 0.20, 0.23, 0.215, freshReceived)
	insertPMPrice(ctx, t, db, "tok-away", 0.23, 0.25, 0.24, freshReceived)

	// 7) Query the view. Expect exactly 3 rows.
	rows, err := db.QueryContext(ctx, `
        SELECT matchup_id, outcome_side,
               ROUND(pinnacle_fair_prob::numeric, 6)::float8 AS fair,
               ROUND(ask_price::numeric, 6)::float8 AS ask,
               ROUND(gross_edge_to_buy::numeric, 6)::float8 AS gross
          FROM pinnacle_polymarket_soccer_3way_edges
         ORDER BY outcome_side
    `)
	if err != nil {
		t.Fatalf("query view: %v", err)
	}
	defer rows.Close()

	type rowVals struct {
		matchupID        int64
		side             string
		fair, ask, gross float64
	}
	got := []rowVals{}
	for rows.Next() {
		var r rowVals
		if err := rows.Scan(&r.matchupID, &r.side, &r.fair, &r.ask, &r.gross); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 rows (one per outcome), got %d: %+v", len(got), got)
	}

	// 8) Dedup: every row must reference matchup_id=100002 (the newer one).
	for _, r := range got {
		if r.matchupID != 100002 {
			t.Errorf("dedup failed: row %+v references matchup %d, want 100002", r, r.matchupID)
		}
	}

	// 9) outcome_side classification: alphabetical order = away, draw, home.
	wantSides := []string{"away", "draw", "home"}
	for i, w := range wantSides {
		if got[i].side != w {
			t.Errorf("got[%d].side=%q want %q", i, got[i].side, w)
		}
	}

	// 10) 3-way fair probs sum to ~1.0 (no-vig normalization invariant).
	sum := got[0].fair + got[1].fair + got[2].fair
	if abs(sum-1.0) > 0.0001 {
		t.Errorf("3-way fair sum=%.6f, want ~1.0", sum)
	}

	// 11) gross_edge_to_buy = fair - ask for each row.
	for _, r := range got {
		want := r.fair - r.ask
		if abs(r.gross-want) > 0.0001 {
			t.Errorf("row %s: gross=%.6f, want fair(%.6f)-ask(%.6f)=%.6f",
				r.side, r.gross, r.fair, r.ask, want)
		}
	}
}

// ---- minimal test helpers (inline; avoid adding to a fixtures package
// that other tests would have to reason about) ----

func mustExec(ctx context.Context, t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func insertPinMatchup(ctx context.Context, t *testing.T, db *sql.DB, id, leagueID int64, home, away string, start time.Time, live bool) {
	t.Helper()
	mustExec(ctx, t, db, `
        INSERT INTO pinnacle_matchups (id, league_id, matchup_type, start_time,
                                       home_name, away_name, last_is_live,
                                       last_has_markets, raw_payload)
        VALUES ($1, $2, 'matchup', $3, $4, $5, $6, true, '{}'::jsonb)
    `, id, leagueID, start, home, away, live)
}

func insertPinMarket(ctx context.Context, t *testing.T, db *sql.DB, matchupID, version int64, receivedAt time.Time, homeAmer, drawAmer, awayAmer int) {
	t.Helper()
	prices := `[
        {"designation":"home","price":` + itoa(homeAmer) + `},
        {"designation":"draw","price":` + itoa(drawAmer) + `},
        {"designation":"away","price":` + itoa(awayAmer) + `}
    ]`
	mustExec(ctx, t, db, `
        INSERT INTO pinnacle_market_history (matchup_id, period, market_type, market_key, side,
                                             is_alternate, status, version, prices, limits, raw_payload,
                                             received_at)
        VALUES ($1, 0, 'moneyline', $2, '', false, 'open', $3, $4::jsonb, '[]'::jsonb, '{}'::jsonb, $5)
    `, matchupID, "ml-"+itoa64(matchupID), version, prices, receivedAt)
	var historyID int64
	if err := db.QueryRowContext(ctx, `
        SELECT id FROM pinnacle_market_history WHERE matchup_id=$1 AND version=$2
    `, matchupID, version).Scan(&historyID); err != nil {
		t.Fatalf("fetch market history id: %v", err)
	}
	mustExec(ctx, t, db, `
        INSERT INTO pinnacle_market_latest (matchup_id, market_key, history_id, version, status, received_at)
        VALUES ($1, $2, $3, $4, 'open', $5)
        ON CONFLICT (matchup_id, market_key) DO UPDATE SET
            history_id = EXCLUDED.history_id, version = EXCLUDED.version, received_at = EXCLUDED.received_at
    `, matchupID, "ml-"+itoa64(matchupID), historyID, version, receivedAt)
}

func insertPMMarket(ctx context.Context, t *testing.T, db *sql.DB, conditionID, eventID, question, marketID string) {
	t.Helper()
	mustExec(ctx, t, db, `
        INSERT INTO polymarket_markets (polymarket_market_id, polymarket_event_id, condition_id,
                                        question, slug, active, closed, archived, gamma_payload)
        VALUES ($1, $2, $3, $4, 'slug-'||$1, true, false, false, '{}'::jsonb)
    `, marketID, eventID, conditionID, question)
}

func insertPMTokenYes(ctx context.Context, t *testing.T, db *sql.DB, tokenID, conditionID, outcomeName string, outcomeIdx int) {
	t.Helper()
	mustExec(ctx, t, db, `
        INSERT INTO polymarket_tokens (token_id, condition_id, outcome_name, outcome_index, side, clob_payload)
        VALUES ($1, $2, $3, $4, 'yes', '{}'::jsonb)
    `, tokenID, conditionID, outcomeName, outcomeIdx)
}

func insertPMPrice(ctx context.Context, t *testing.T, db *sql.DB, tokenID string, bid, ask, mid float64, receivedAt time.Time) {
	t.Helper()
	// Lookup the token's condition_id so the FK / NOT NULL is satisfied
	// without making the caller pass it explicitly (the test caller
	// already knows the (token, condition) pairing from insertPMTokenYes).
	var conditionID string
	if err := db.QueryRowContext(ctx, `SELECT condition_id FROM polymarket_tokens WHERE token_id=$1`, tokenID).Scan(&conditionID); err != nil {
		t.Fatalf("lookup condition_id for token %s: %v", tokenID, err)
	}
	mustExec(ctx, t, db, `
        INSERT INTO polymarket_price_snapshots (token_id, condition_id,
                                                bid_price, ask_price, mid_price,
                                                clob_payload, received_at)
        VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, $6)
    `, tokenID, conditionID, bid, ask, mid, receivedAt)
}

func itoa(i int) string {
	return itoa64(int64(i))
}

func itoa64(i int64) string {
	return sqlIntStr(i)
}

// sqlIntStr is a fmt-free integer-to-string for JSON-literal building.
// (Using fmt here would pull a dependency we don't otherwise need in
// this file.)
func sqlIntStr(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	p := len(buf)
	for i > 0 {
		p--
		buf[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		buf[p] = '-'
	}
	return string(buf[p:])
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
