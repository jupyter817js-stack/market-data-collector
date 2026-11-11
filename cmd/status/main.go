// Command status prints a compact snapshot of the live pipeline's current
// state — collector freshness, CS2 inventory, signal output, paper PnL,
// and proposed orders. It's the "is anything alive?" check that doesn't
// require remembering ten SQL queries.
//
// Tables and views that don't exist (because migration N hasn't been
// applied yet) are skipped with a friendly note, so this command is
// useful even on a half-set-up database.
//
//	go run ./cmd/status                         # full snapshot
//	go run ./cmd/status -window 6h              # narrower time window
//	go run ./cmd/status -section signals,edges  # subset
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/pol-odds-collector/internal/config"
)

const allSections = "sources,cs2,aliases,edges,signals,paper,orders"

func main() {
	window := flag.Duration("window", 1*time.Hour, "look-back window for rate stats")
	sections := flag.String("section", allSections, "comma-separated subset: "+allSections)
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ping db: %v\n", err)
		os.Exit(1)
	}

	wanted := map[string]bool{}
	for _, s := range strings.Split(*sections, ",") {
		wanted[strings.TrimSpace(s)] = true
	}

	fmt.Printf("=== pipeline status @ %s (window=%s) ===\n\n", time.Now().UTC().Format(time.RFC3339), *window)

	if wanted["sources"] {
		printCollectorFreshness(ctx, db, *window)
	}
	if wanted["cs2"] {
		printCS2Inventory(ctx, db)
	}
	if wanted["aliases"] {
		printAliases(ctx, db)
	}
	if wanted["edges"] {
		printEdges(ctx, db)
	}
	if wanted["signals"] {
		printSignals(ctx, db, *window)
	}
	if wanted["paper"] {
		printPaperRuns(ctx, db)
	}
	if wanted["orders"] {
		printProposedOrders(ctx, db)
	}
}

// ---- sections ----

func printCollectorFreshness(ctx context.Context, db *sql.DB, window time.Duration) {
	section("Collectors")
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "WHAT\tLAST WRITE\tAGE\tROWS LAST WINDOW")

	rows := []struct {
		label, query string
		args         []any
	}{
		{"pinnacle_market_history", `SELECT MAX(received_at), COUNT(*) FILTER (WHERE received_at >= now() - $1::interval) FROM pinnacle_market_history`, []any{window.String()}},
		{"pinnacle_matchups", `SELECT MAX(last_seen_at), COUNT(*) FILTER (WHERE last_seen_at >= now() - $1::interval) FROM pinnacle_matchups`, []any{window.String()}},
		{"pinnacle_collector_runs", `SELECT MAX(started_at), COUNT(*) FILTER (WHERE started_at >= now() - $1::interval) FROM pinnacle_collector_runs`, []any{window.String()}},
		{"polymarket_price_snapshots", `SELECT MAX(received_at), COUNT(*) FILTER (WHERE received_at >= now() - $1::interval) FROM polymarket_price_snapshots`, []any{window.String()}},
		{"polymarket_markets", `SELECT MAX(updated_at), COUNT(*) FILTER (WHERE updated_at >= now() - $1::interval) FROM polymarket_markets`, []any{window.String()}},
		{"polymarket_sync_runs", `SELECT MAX(started_at), COUNT(*) FILTER (WHERE started_at >= now() - $1::interval) FROM polymarket_sync_runs`, []any{window.String()}},
		{"signals", `SELECT MAX(created_at), COUNT(*) FILTER (WHERE created_at >= now() - $1::interval) FROM signals`, []any{window.String()}},
		{"signal_runs", `SELECT MAX(started_at), COUNT(*) FILTER (WHERE started_at >= now() - $1::interval) FROM signal_runs`, []any{window.String()}},
	}

	for _, r := range rows {
		var ts sql.NullTime
		var cnt int64
		if err := db.QueryRowContext(ctx, r.query, r.args...).Scan(&ts, &cnt); err != nil {
			if isMissingTable(err) {
				fmt.Fprintf(w, "%s\t(table missing — apply migrations)\t-\t-\n", r.label)
				continue
			}
			fmt.Fprintf(w, "%s\tERROR: %v\t-\t-\n", r.label, err)
			continue
		}
		if !ts.Valid {
			fmt.Fprintf(w, "%s\t(no rows)\t-\t0\n", r.label)
			continue
		}
		age := time.Since(ts.Time)
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\n",
			r.label, ts.Time.UTC().Format("15:04:05"), prettyAge(age), cnt)
	}
	w.Flush()
}

func printCS2Inventory(ctx context.Context, db *sql.DB) {
	// Sport-rollup first: how many active leagues / matchups per sport in our DB.
	section("All-sport inventory (Pinnacle, by sport_id)")
	rrows, err := db.QueryContext(ctx, `
        SELECT l.sport_id,
               COUNT(DISTINCT l.id) FILTER (WHERE l.last_matchup_count > 0) AS leagues,
               SUM(l.last_matchup_count)                                    AS pinnacle_matchupcount,
               COUNT(DISTINCT m.id) FILTER (WHERE m.matchup_type='matchup'
                                              AND m.last_is_live)            AS live_in_db,
               COUNT(DISTINCT m.id) FILTER (WHERE m.matchup_type='matchup'
                                              AND m.start_time BETWEEN now() AND now() + interval '24 hours') AS upcoming_24h
          FROM pinnacle_leagues l
          LEFT JOIN pinnacle_matchups m ON m.league_id = l.id
         GROUP BY l.sport_id
         HAVING SUM(l.last_matchup_count) > 0 OR COUNT(m.id) > 0
         ORDER BY 3 DESC NULLS LAST
    `)
	if err == nil {
		defer rrows.Close()
		wr := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		fmt.Fprintln(wr, "SPORT\tID\tLEAGUES\tPIN_MATCH\tLIVE_DB\tUPCOMING24H")
		any := false
		for rrows.Next() {
			var sid, leagues int
			var pm, live, up sql.NullInt64
			_ = rrows.Scan(&sid, &leagues, &pm, &live, &up)
			fmt.Fprintf(wr, "%s\t%d\t%d\t%d\t%d\t%d\n",
				sportName(sid), sid, leagues, pm.Int64, live.Int64, up.Int64)
			any = true
		}
		wr.Flush()
		if !any {
			fmt.Println("  (no active sports in db)")
		}
	} else if isMissingTable(err) {
		fmt.Println("  (pinnacle_leagues missing)")
	}

	section("CS2 inventory (Pinnacle)")
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)

	// Active CS2 leagues
	rows, err := db.QueryContext(ctx, `
        SELECT l.id, l.name, l.last_matchup_count
          FROM pinnacle_leagues l
         WHERE l.sport_id = 12
           AND l.last_matchup_count > 0
           AND (LOWER(l.name) LIKE '%cs2%' OR LOWER(l.name) LIKE '%cs:go%' OR LOWER(l.name) LIKE '%counter%')
         ORDER BY l.last_matchup_count DESC
    `)
	if err != nil {
		if isMissingTable(err) {
			fmt.Println("  (pinnacle_leagues table missing)")
			return
		}
		fmt.Printf("  ERROR: %v\n", err)
		return
	}
	defer rows.Close()
	fmt.Fprintln(w, "LEAGUE_ID\tNAME\tMATCHES")
	n := 0
	for rows.Next() {
		var id, cnt int
		var name string
		_ = rows.Scan(&id, &name, &cnt)
		fmt.Fprintf(w, "%d\t%s\t%d\n", id, trunc(name, 40), cnt)
		n++
	}
	w.Flush()
	if n == 0 {
		fmt.Println("  (no active CS2 leagues)")
	}

	// Live matchups
	var liveCount int
	err = db.QueryRowContext(ctx, `
        SELECT COUNT(*) FROM pinnacle_matchups m
          JOIN pinnacle_leagues l ON l.id = m.league_id
         WHERE l.sport_id = 12
           AND m.matchup_type = 'matchup'
           AND m.last_is_live
           AND (LOWER(l.name) LIKE '%cs2%' OR LOWER(l.name) LIKE '%cs:go%' OR LOWER(l.name) LIKE '%counter%')
    `).Scan(&liveCount)
	if err == nil {
		fmt.Printf("  live matchups currently: %d\n", liveCount)
	}

	// Upcoming today
	var upcomingCount int
	err = db.QueryRowContext(ctx, `
        SELECT COUNT(*) FROM pinnacle_matchups m
          JOIN pinnacle_leagues l ON l.id = m.league_id
         WHERE l.sport_id = 12
           AND m.matchup_type = 'matchup'
           AND m.start_time BETWEEN now() AND now() + interval '24 hours'
           AND (LOWER(l.name) LIKE '%cs2%' OR LOWER(l.name) LIKE '%cs:go%' OR LOWER(l.name) LIKE '%counter%')
    `).Scan(&upcomingCount)
	if err == nil {
		fmt.Printf("  upcoming next 24h:      %d\n", upcomingCount)
	}
}

func printAliases(ctx context.Context, db *sql.DB) {
	section("Pinnacle ↔ Polymarket aliases")
	var confirmed, candidate, rejected int
	err := db.QueryRowContext(ctx, `
        SELECT
          COUNT(*) FILTER (WHERE match_status = 'confirmed'),
          COUNT(*) FILTER (WHERE match_status = 'candidate'),
          COUNT(*) FILTER (WHERE match_status = 'rejected')
        FROM pinnacle_event_aliases
    `).Scan(&confirmed, &candidate, &rejected)
	if err != nil {
		if isMissingTable(err) {
			fmt.Println("  (pinnacle_event_aliases missing — apply migration 022)")
			return
		}
		fmt.Printf("  ERROR: %v\n", err)
		return
	}
	fmt.Printf("  confirmed: %d   candidate: %d   rejected: %d\n", confirmed, candidate, rejected)

	if candidate > 0 {
		fmt.Println("  → promote with: go run ./cmd/pinnacle-match -mode promote -matchup ... -condition ...")
	}

	// Confirmed list (top 5 most recent)
	rows, err := db.QueryContext(ctx, `
        SELECT pa.pinnacle_matchup_id, m.home_name, m.away_name, pa.updated_at
          FROM pinnacle_event_aliases pa
          JOIN pinnacle_matchups m ON m.id = pa.pinnacle_matchup_id
         WHERE pa.match_status = 'confirmed'
         ORDER BY pa.updated_at DESC
         LIMIT 5
    `)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var mid int64
		var h, a string
		var ts time.Time
		_ = rows.Scan(&mid, &h, &a, &ts)
		fmt.Printf("    %d %s vs %s (%s)\n", mid, h, a, ts.UTC().Format("01-02 15:04"))
	}
}

func printEdges(ctx context.Context, db *sql.DB) {
	section("Top live edges (pinnacle_polymarket_moneyline_edges)")
	rows, err := db.QueryContext(ctx, `
        SELECT matchup_id, pinnacle_sport_id, league_name, home_name, away_name, outcome_side,
               COALESCE(pinnacle_fair_prob, 0), ask_price, bid_price,
               COALESCE(gross_edge_to_buy, 0),
               COALESCE(pinnacle_age_sec, 0), COALESCE(pm_age_sec, 0)
          FROM pinnacle_polymarket_moneyline_edges
         ORDER BY gross_edge_to_buy DESC NULLS LAST
         LIMIT 10
    `)
	if err != nil {
		if isMissingTable(err) {
			fmt.Println("  (edge view missing — apply migration 027; also needs confirmed aliases)")
			return
		}
		fmt.Printf("  ERROR: %v\n", err)
		return
	}
	defer rows.Close()
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "MATCH\tSPORT\tLEAGUE\tHOME\tAWAY\tSIDE\tFAIR\tASK\tBID\tGROSS\tPIN_AGE\tPM_AGE")
	n := 0
	for rows.Next() {
		var mid int64
		var sportID int
		var league, home, away, side string
		var fair, ask, bid sql.NullFloat64
		var gross, pinAge, pmAge float64
		_ = rows.Scan(&mid, &sportID, &league, &home, &away, &side, &fair, &ask, &bid, &gross, &pinAge, &pmAge)
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%+.3f\t%.0f\t%.0f\n",
			mid, sportName(sportID), trunc(league, 22), trunc(home, 14), trunc(away, 14), side,
			fmtNullProb(fair), fmtNullProb(ask), fmtNullProb(bid),
			gross, pinAge, pmAge)
		n++
	}
	w.Flush()
	if n == 0 {
		fmt.Println("  (no edge rows yet — needs confirmed aliases + live CS2 odds + PM prices)")
	}
}

func printSignals(ctx context.Context, db *sql.DB, window time.Duration) {
	section(fmt.Sprintf("Signals in last %s", window))
	rows, err := db.QueryContext(ctx, `
        SELECT state, COUNT(*) FROM signals
         WHERE created_at >= now() - $1::interval
         GROUP BY state
         ORDER BY 2 DESC
    `, window.String())
	if err != nil {
		if isMissingTable(err) {
			fmt.Println("  (signals table missing — apply migration 023)")
			return
		}
		fmt.Printf("  ERROR: %v\n", err)
		return
	}
	defer rows.Close()
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "STATE\tCOUNT")
	any := false
	for rows.Next() {
		var s string
		var c int
		_ = rows.Scan(&s, &c)
		fmt.Fprintf(w, "%s\t%d\n", s, c)
		any = true
	}
	w.Flush()
	if !any {
		fmt.Println("  (no signals in window)")
	}

	// Latest fresh BUY candidates (per-matchup+outcome)
	fresh, err := db.QueryContext(ctx, `
        SELECT matchup_id, home_name, away_name, outcome_side,
               COALESCE(pinnacle_fair_prob, 0), pm_ask,
               COALESCE(net_edge_buy, 0), created_at
          FROM signals_buy_candidates_fresh
         ORDER BY net_edge_buy DESC NULLS LAST
         LIMIT 5
    `)
	if err != nil {
		return
	}
	defer fresh.Close()
	fmt.Println("\n  fresh BUY_CANDIDATEs (signals_buy_candidates_fresh):")
	w2 := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w2, "  MATCH\tHOME\tAWAY\tSIDE\tFAIR\tASK\tNET_PP\tCREATED")
	n := 0
	for fresh.Next() {
		var mid int64
		var h, a, side string
		var fair float64
		var ask sql.NullFloat64
		var net float64
		var ts time.Time
		_ = fresh.Scan(&mid, &h, &a, &side, &fair, &ask, &net, &ts)
		fmt.Fprintf(w2, "  %d\t%s\t%s\t%s\t%.3f\t%s\t%+.2f\t%s\n",
			mid, trunc(h, 14), trunc(a, 14), side, fair, fmtNullProb(ask), net*100, ts.UTC().Format("15:04:05"))
		n++
	}
	w2.Flush()
	if n == 0 {
		fmt.Println("  (no fresh BUY_CANDIDATE)")
	}
}

func printPaperRuns(ctx context.Context, db *sql.DB) {
	section("Paper-PnL runs (latest 5)")
	rows, err := db.QueryContext(ctx, `
        SELECT id, started_at,
               trades_simulated, trades_with_mtm,
               ROUND(total_size_usdc::numeric, 2)        AS size,
               ROUND(total_mtm_pnl_usdc::numeric, 2)     AS mtm,
               ROUND(avg_mtm_edge_pp::numeric, 2)        AS mtm_pp,
               COALESCE(trades_resolved, 0)              AS resolved,
               COALESCE(trades_won, 0)                   AS wins,
               ROUND(total_realized_pnl_usdc::numeric, 2) AS realized,
               hit_rate
          FROM paper_pnl_runs
         ORDER BY started_at DESC
         LIMIT 5
    `)
	if err != nil {
		if isMissingTable(err) {
			fmt.Println("  (paper_pnl_runs missing — apply migration 024+025)")
			return
		}
		fmt.Printf("  ERROR: %v\n", err)
		return
	}
	defer rows.Close()
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTARTED\tTRADES\tMTM\tSIZE\tMTM_PNL\tMTM_PP\tRES\tWINS\tREALIZED\tHITRATE")
	n := 0
	for rows.Next() {
		var id int64
		var ts time.Time
		var simulated, mtm int
		var size, mtmPnL, mtmPP, realized float64
		var resolved, wins int
		var hitRate sql.NullFloat64
		_ = rows.Scan(&id, &ts, &simulated, &mtm, &size, &mtmPnL, &mtmPP,
			&resolved, &wins, &realized, &hitRate)
		hr := "-"
		if hitRate.Valid {
			hr = fmt.Sprintf("%.1f%%", hitRate.Float64*100)
		}
		fmt.Fprintf(w, "%d\t%s\t%d\t%d\t$%.2f\t$%+.2f\t%+.2f\t%d\t%d\t$%+.2f\t%s\n",
			id, ts.UTC().Format("01-02 15:04"), simulated, mtm, size, mtmPnL, mtmPP,
			resolved, wins, realized, hr)
		n++
	}
	w.Flush()
	if n == 0 {
		fmt.Println("  (no paper runs yet — run: go run ./cmd/paper-pnl -since 24h -write-run)")
	}
}

func printProposedOrders(ctx context.Context, db *sql.DB) {
	section("Proposed orders (latest 5)")
	rows, err := db.QueryContext(ctx, `
        SELECT id, created_at, status, matchup_id, home_name, away_name,
               outcome_side, side, size_usdc, price,
               maker_amount_6dec, taker_amount_6dec, claimed_net_edge
          FROM proposed_orders
         ORDER BY created_at DESC
         LIMIT 5
    `)
	if err != nil {
		if isMissingTable(err) {
			fmt.Println("  (proposed_orders missing — apply migration 026)")
			return
		}
		fmt.Printf("  ERROR: %v\n", err)
		return
	}
	defer rows.Close()
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCREATED\tSTATUS\tMATCH\tHOME\tAWAY\tSIDE_OUT\tSIDE_BS\tSIZE\tPRICE\tMAKER\tTAKER\tCLAIM_PP")
	n := 0
	for rows.Next() {
		var id, mid int64
		var ts time.Time
		var status, h, a, outSide, bs string
		var size, price, claim float64
		var maker, taker int64
		_ = rows.Scan(&id, &ts, &status, &mid, &h, &a, &outSide, &bs, &size, &price, &maker, &taker, &claim)
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t$%.2f\t%.3f\t%d\t%d\t%+.2f\n",
			id, ts.UTC().Format("01-02 15:04"), status, mid, trunc(h, 14), trunc(a, 14),
			outSide, bs, size, price, maker, taker, claim*100)
		n++
	}
	w.Flush()
	if n == 0 {
		fmt.Println("  (no proposed orders — run: go run ./cmd/executor-plan -mode plan -maker 0x...)")
	}
}

// ---- helpers ----

func section(title string) {
	fmt.Printf("──── %s ────\n", title)
}

func isMissingTable(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return contains(s, "does not exist") || contains(s, "42P01")
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func prettyAge(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%.0fs", d.Seconds())
	case d < time.Hour:
		return fmt.Sprintf("%.1fm", d.Minutes())
	case d < 24*time.Hour:
		return fmt.Sprintf("%.1fh", d.Hours())
	default:
		return fmt.Sprintf("%.1fd", d.Hours()/24)
	}
}

// sportName returns a short label for the most common Pinnacle sport ids.
// Unknown ids fall back to the numeric id. Probed 2026-05-17.
func sportName(id int) string {
	switch id {
	case 3:
		return "Baseball"
	case 4:
		return "Basketball"
	case 6:
		return "Boxing"
	case 8:
		return "Cricket"
	case 12:
		return "ESports"
	case 15:
		return "Football"
	case 17:
		return "Golf"
	case 19:
		return "Hockey"
	case 22:
		return "MMA"
	case 26:
		return "RugbyL"
	case 27:
		return "RugbyU"
	case 29:
		return "Soccer"
	case 33:
		return "Tennis"
	case 34:
		return "Volleyball"
	}
	return fmt.Sprintf("sport_%d", id)
}

func fmtNullProb(v sql.NullFloat64) string {
	if !v.Valid {
		return "-"
	}
	return fmt.Sprintf("%.3f", v.Float64)
}
