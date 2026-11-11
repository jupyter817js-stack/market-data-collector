// Command pinnacle-match scans the pinnacle_polymarket_match_candidates view
// and writes/upserts candidate aliases into pinnacle_event_aliases.
//
// Modes:
//
//	go run ./cmd/pinnacle-match -mode list           # show candidates, do not write
//	go run ./cmd/pinnacle-match -mode write          # upsert candidates (status='candidate')
//	go run ./cmd/pinnacle-match -mode confirmed      # show currently-confirmed aliases
//	go run ./cmd/pinnacle-match -mode promote -matchup ... -condition ...   # mark one as confirmed
//	go run ./cmd/pinnacle-match -mode reject  -matchup ... -condition ...   # mark one as rejected
//	go run ./cmd/pinnacle-match -mode auto-confirm [-dry-run] [-loop 60s]   # confirm live/imminent safe matches
//
// auto-confirm criteria (all must hold to flip a candidate to confirmed):
//   - matchup is currently live OR starts within the next 30 min
//   - both team names appear as whole words in the PM event title (\y bound)
//   - both team names are at least 4 chars (prevents "Bula" ⊂ "Nebula")
//   - PM market question matches an H2H shape: no O/U, Map N, Halftime, 1H/2H,
//     Total, Handicap, Odd/Even, Spread, etc.
//   - PM event is active=true, closed=false
//   - Pinnacle start_time and PM event start_time within ±60 min
//
// Inserted rows get match_status='confirmed', match_confidence=95.0, and notes
// starting with 'auto:' so they're easy to revoke en masse.
//
// Read-only against external services; only PostgreSQL is touched.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/pol-odds-collector/internal/config"
)

func main() {
	mode := flag.String("mode", "list", "list | write | confirmed | promote | reject | auto-confirm")
	matchupID := flag.Int64("matchup", 0, "Pinnacle matchup id (promote/reject)")
	conditionID := flag.String("condition", "", "Polymarket condition id (promote/reject)")
	minConf := flag.Float64("min-confidence", 80.0, "for -mode write: only write candidates with suggested_confidence >= this")
	notes := flag.String("notes", "", "free-text notes recorded on the row")
	dryRun := flag.Bool("dry-run", false, "for -mode auto-confirm: show what would be inserted, do not write")
	loop := flag.Duration("loop", 0, "for -mode auto-confirm: run repeatedly with this interval (e.g. 60s); zero = run once")
	window := flag.Duration("window", 30*time.Minute, "for -mode auto-confirm: include matchups starting within this window (default 30m; bump for end-to-end testing)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(2)
	}
	// auto-confirm with -loop runs indefinitely until SIGINT/SIGTERM; the
	// other modes are short one-shots that should die fast if the DB hangs.
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if *mode == "auto-confirm" && *loop > 0 {
		ctx, cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	}
	defer cancel()

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		die("open db: " + err.Error())
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		die("ping db: " + err.Error())
	}

	switch *mode {
	case "list":
		runList(ctx, db, *minConf)
	case "write":
		runWrite(ctx, db, *minConf, *notes)
	case "confirmed":
		runConfirmed(ctx, db)
	case "promote":
		if *matchupID == 0 || *conditionID == "" {
			die("-matchup and -condition required")
		}
		runUpdateStatus(ctx, db, *matchupID, *conditionID, "confirmed", *notes)
	case "reject":
		if *matchupID == 0 || *conditionID == "" {
			die("-matchup and -condition required")
		}
		runUpdateStatus(ctx, db, *matchupID, *conditionID, "rejected", *notes)
	case "auto-confirm":
		runAutoConfirm(ctx, db, *dryRun, *loop, *window)
	default:
		die("unknown -mode: " + *mode)
	}
}

func runList(ctx context.Context, db *sql.DB, minConf float64) {
	rows, err := db.QueryContext(ctx, `
        SELECT pinnacle_matchup_id, pinnacle_league_name, home_name, away_name,
               polymarket_condition_id, polymarket_event_title, suggested_confidence
          FROM pinnacle_polymarket_match_candidates
         WHERE suggested_confidence >= $1
      ORDER BY suggested_confidence DESC, pinnacle_matchup_id
    `, minConf)
	if err != nil {
		die("query: " + err.Error())
	}
	defer rows.Close()
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "MATCHUP\tCONF\tLEAGUE\tHOME\tAWAY\tCONDITION\tPM TITLE")
	n := 0
	for rows.Next() {
		var (
			mid                    int64
			leagueName, home, away string
			cond, pmTitle          string
			conf                   float64
		)
		if err := rows.Scan(&mid, &leagueName, &home, &away, &cond, &pmTitle, &conf); err != nil {
			die("scan: " + err.Error())
		}
		fmt.Fprintf(w, "%d\t%.1f\t%s\t%s\t%s\t%s\t%s\n",
			mid, conf, truncate(leagueName, 22), truncate(home, 20), truncate(away, 20), truncate(cond, 18), truncate(pmTitle, 60))
		n++
	}
	w.Flush()
	fmt.Fprintf(os.Stdout, "\n%d candidate(s) with confidence >= %.1f\n", n, minConf)
}

func runWrite(ctx context.Context, db *sql.DB, minConf float64, notes string) {
	res, err := db.ExecContext(ctx, `
        INSERT INTO pinnacle_event_aliases
            (pinnacle_matchup_id, polymarket_condition_id,
             polymarket_event_id, polymarket_market_id,
             match_status, match_confidence, notes)
        SELECT
            c.pinnacle_matchup_id,
            c.polymarket_condition_id,
            c.polymarket_event_id,
            c.polymarket_market_id,
            'candidate',
            c.suggested_confidence,
            $2
          FROM pinnacle_polymarket_match_candidates c
         WHERE c.suggested_confidence >= $1
        ON CONFLICT (pinnacle_matchup_id, polymarket_condition_id) DO UPDATE SET
            match_confidence = EXCLUDED.match_confidence,
            polymarket_event_id = COALESCE(pinnacle_event_aliases.polymarket_event_id, EXCLUDED.polymarket_event_id),
            polymarket_market_id = COALESCE(pinnacle_event_aliases.polymarket_market_id, EXCLUDED.polymarket_market_id),
            updated_at = now()
        WHERE pinnacle_event_aliases.match_status = 'candidate'
    `, minConf, notes)
	if err != nil {
		die("insert candidates: " + err.Error())
	}
	rows, _ := res.RowsAffected()
	fmt.Printf("upserted %d candidate alias row(s) (confidence >= %.1f)\n", rows, minConf)
	fmt.Println("review with: go run ./cmd/pinnacle-match -mode list")
	fmt.Println("confirm with: go run ./cmd/pinnacle-match -mode promote -matchup ... -condition ...")
}

func runConfirmed(ctx context.Context, db *sql.DB) {
	rows, err := db.QueryContext(ctx, `
        SELECT pa.pinnacle_matchup_id, m.home_name, m.away_name,
               pa.polymarket_condition_id, pa.match_confidence, pa.updated_at
          FROM pinnacle_event_aliases pa
          JOIN pinnacle_matchups m ON m.id = pa.pinnacle_matchup_id
         WHERE pa.match_status = 'confirmed'
      ORDER BY pa.updated_at DESC
    `)
	if err != nil {
		die("query confirmed: " + err.Error())
	}
	defer rows.Close()
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "MATCHUP\tHOME\tAWAY\tCONDITION\tCONF\tUPDATED")
	for rows.Next() {
		var (
			mid              int64
			home, away, cond string
			conf             sql.NullFloat64
			updated          time.Time
		)
		if err := rows.Scan(&mid, &home, &away, &cond, &conf, &updated); err != nil {
			die("scan: " + err.Error())
		}
		cs := "-"
		if conf.Valid {
			cs = fmt.Sprintf("%.1f", conf.Float64)
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\n",
			mid, truncate(home, 20), truncate(away, 20), truncate(cond, 18), cs, updated.Format(time.RFC3339))
	}
	w.Flush()
}

// autoConfirmSelectSQL: 2-way (tennis/MLB/etc.) safe candidates. For the
// 3-way soccer path see autoConfirmSoccerSelectSQL below. Pattern \y is the
// PostgreSQL word-boundary anchor.
const autoConfirmSelectSQL = `
SELECT c.pinnacle_matchup_id,
       c.home_name,
       c.away_name,
       c.polymarket_condition_id,
       c.polymarket_event_id,
       c.polymarket_market_id,
       c.polymarket_event_title,
       pm.question AS polymarket_question,
       m.last_is_live,
       m.start_time AS pinnacle_start_time,
       c.polymarket_start_time
  FROM pinnacle_polymarket_match_candidates c
  JOIN pinnacle_matchups m   ON m.id = c.pinnacle_matchup_id
  JOIN polymarket_events  pe ON pe.polymarket_event_id = c.polymarket_event_id
  JOIN polymarket_markets pm ON pm.condition_id        = c.polymarket_condition_id
 WHERE (m.last_is_live = true
        OR (m.start_time IS NOT NULL
            AND m.start_time BETWEEN now() AND now() + ($1::text)::interval))
   AND length(c.home_name) >= 4
   AND length(c.away_name) >= 4
   AND lower(c.polymarket_event_title) ~ ('\y' || lower(c.home_name) || '\y')
   AND lower(c.polymarket_event_title) ~ ('\y' || lower(c.away_name) || '\y')
   -- Question must be a pure "Team A vs. Team B" H2H (the literal money-line
   -- market). Block every prop/derivative we've observed: O/U, totals, maps,
   -- halves, handicaps, draws, BTTS, completed-settlement markets, etc.
   AND pm.question !~* '(o/u|over|under|map\s*[0-9]|halftime|1h|2h|first half|second half|total|handicap|odd/even|spread|will\s|\?|draw|tie\b|both teams|btts|completed|to score|to win to nil|clean sheet|correct score)'
   AND pm.question  ~* '(\svs\.?\s)'
   AND pe.active = true
   AND pe.closed = false
   AND (c.polymarket_start_time IS NULL
        OR ABS(EXTRACT(EPOCH FROM (c.pinnacle_start_time - c.polymarket_start_time))) < 3600)
   AND NOT EXISTS (
        SELECT 1 FROM pinnacle_event_aliases ea
         WHERE ea.pinnacle_matchup_id     = c.pinnacle_matchup_id
           AND ea.polymarket_condition_id = c.polymarket_condition_id
           AND ea.match_status IN ('confirmed','rejected')
   )
 ORDER BY c.pinnacle_matchup_id, c.polymarket_condition_id;
`

const autoConfirmInsertSQL = `
INSERT INTO pinnacle_event_aliases
    (pinnacle_matchup_id, polymarket_condition_id,
     polymarket_event_id, polymarket_market_id,
     match_status, match_confidence, notes)
VALUES ($1,$2,$3,$4,'confirmed',95.0,$5)
ON CONFLICT (pinnacle_matchup_id, polymarket_condition_id) DO UPDATE SET
    match_status     = 'confirmed',
    match_confidence = 95.0,
    notes            = EXCLUDED.notes,
    updated_at       = now()
 WHERE pinnacle_event_aliases.match_status = 'candidate';
`

// autoConfirmSoccerSelectSQL: 3-way soccer safe candidates. Returns one row
// per (matchup, PM event) where the event contains EXACTLY ONE of each:
// home-win binary, away-win binary, and draw binary. We bundle the 3
// conditions per row so the Go side inserts the trio atomically.
const autoConfirmSoccerSelectSQL = `
WITH per_event AS (
  SELECT m.id AS matchup_id,
         m.home_name, m.away_name,
         m.last_is_live,
         pe.polymarket_event_id,
         pe.title AS event_title,
         MAX(CASE WHEN pm.question ILIKE '%end in a draw%' THEN pm.condition_id END) AS draw_condition,
         MAX(CASE WHEN pm.question ILIKE '%end in a draw%' THEN pm.polymarket_market_id END) AS draw_market_id,
         MAX(CASE WHEN pm.question ILIKE 'Will %win%'
                       AND position(LOWER(m.home_name) IN LOWER(pm.question)) > 0
                       AND position(LOWER(m.away_name) IN LOWER(pm.question)) = 0
                  THEN pm.condition_id END) AS home_condition,
         MAX(CASE WHEN pm.question ILIKE 'Will %win%'
                       AND position(LOWER(m.home_name) IN LOWER(pm.question)) > 0
                       AND position(LOWER(m.away_name) IN LOWER(pm.question)) = 0
                  THEN pm.polymarket_market_id END) AS home_market_id,
         MAX(CASE WHEN pm.question ILIKE 'Will %win%'
                       AND position(LOWER(m.away_name) IN LOWER(pm.question)) > 0
                       AND position(LOWER(m.home_name) IN LOWER(pm.question)) = 0
                  THEN pm.condition_id END) AS away_condition,
         MAX(CASE WHEN pm.question ILIKE 'Will %win%'
                       AND position(LOWER(m.away_name) IN LOWER(pm.question)) > 0
                       AND position(LOWER(m.home_name) IN LOWER(pm.question)) = 0
                  THEN pm.polymarket_market_id END) AS away_market_id
    FROM pinnacle_matchups m
    JOIN pinnacle_leagues l ON l.id = m.league_id AND l.sport_id = 29
    JOIN polymarket_events pe ON pe.active = true AND pe.closed = false
     AND LOWER(pe.title) ~ ('\y' || LOWER(m.home_name) || '\y')
     AND LOWER(pe.title) ~ ('\y' || LOWER(m.away_name) || '\y')
    JOIN polymarket_markets pm ON pm.polymarket_event_id = pe.polymarket_event_id
     AND pm.active = true AND pm.closed = false
     AND (pm.question ILIKE '%end in a draw%' OR pm.question ILIKE 'Will %win%')
   WHERE (m.last_is_live = true
          OR (m.start_time IS NOT NULL
              AND m.start_time BETWEEN now() AND now() + ($1::text)::interval))
     AND m.matchup_type = 'matchup'
     AND length(m.home_name) >= 4
     AND length(m.away_name) >= 4
   GROUP BY m.id, m.home_name, m.away_name, m.last_is_live,
            pe.polymarket_event_id, pe.title
)
SELECT matchup_id, home_name, away_name, last_is_live, event_title,
       polymarket_event_id,
       home_condition,  home_market_id,
       draw_condition,  draw_market_id,
       away_condition,  away_market_id
  FROM per_event c
 WHERE c.home_condition IS NOT NULL
   AND c.draw_condition IS NOT NULL
   AND c.away_condition IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM pinnacle_event_aliases ea
         WHERE ea.pinnacle_matchup_id = c.matchup_id
           AND ea.polymarket_condition_id IN
               (c.home_condition, c.draw_condition, c.away_condition)
           AND ea.match_status IN ('confirmed','rejected')
   )
 ORDER BY c.matchup_id;
`

func runAutoConfirm(ctx context.Context, db *sql.DB, dryRun bool, loopEvery, window time.Duration) {
	// Pass the window to SQL as an interval text (e.g. "1800 seconds"). Both
	// SQL constants use $1::text::interval for the imminent-kickoff gate.
	intervalArg := fmt.Sprintf("%d seconds", int(window.Seconds()))
	tick := func() {
		twoWay := runAutoConfirm2Way(ctx, db, dryRun, intervalArg)
		threeWay := runAutoConfirmSoccer3Way(ctx, db, dryRun, intervalArg)
		if twoWay == 0 && threeWay == 0 {
			slog.Info("auto-confirm: no safe candidates this tick", "window", window)
		}
	}

	if loopEvery <= 0 {
		tick()
		return
	}
	t := time.NewTicker(loopEvery)
	defer t.Stop()
	tick()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick()
		}
	}
}

// runAutoConfirm2Way handles tennis/MLB/etc. single-market H2H candidates.
// Returns the number of new confirmed rows inserted (0 in dry-run mode even
// if candidates were found).
func runAutoConfirm2Way(ctx context.Context, db *sql.DB, dryRun bool, windowInterval string) int {
	rows, err := db.QueryContext(ctx, autoConfirmSelectSQL, windowInterval)
	if err != nil {
		slog.Error("auto-confirm 2way select", "err", err)
		return 0
	}
	defer rows.Close()
	type row struct {
		MatchupID                int64
		Home, Away               string
		Condition, Event, Market string
		Title, Question          string
		Live                     bool
		PinnStart, PMStart       sql.NullTime
	}
	var found []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.MatchupID, &r.Home, &r.Away,
			&r.Condition, &r.Event, &r.Market, &r.Title, &r.Question,
			&r.Live, &r.PinnStart, &r.PMStart); err != nil {
			slog.Error("auto-confirm 2way scan", "err", err)
			return 0
		}
		found = append(found, r)
	}
	if len(found) == 0 {
		return 0
	}
	for _, r := range found {
		slog.Info("auto-confirm 2way candidate",
			"matchup", r.MatchupID, "home", r.Home, "away", r.Away,
			"live", r.Live, "pm_title", r.Title, "pm_question", r.Question)
	}
	if dryRun {
		slog.Info("auto-confirm 2way dry-run; no writes", "count", len(found))
		return 0
	}
	inserted := 0
	for _, r := range found {
		note := fmt.Sprintf("auto: live=%t, title=%q", r.Live, truncate(r.Title, 80))
		res, err := db.ExecContext(ctx, autoConfirmInsertSQL,
			r.MatchupID, r.Condition, r.Event, r.Market, note)
		if err != nil {
			slog.Error("auto-confirm 2way insert", "matchup", r.MatchupID, "err", err)
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			inserted++
		}
	}
	slog.Info("auto-confirm 2way done", "candidates", len(found), "newly_confirmed", inserted)
	return inserted
}

// runAutoConfirmSoccer3Way handles soccer matchups where PM lists 3 binary
// markets ("Will X win" / "...end in a draw") for the same event. Inserts
// 3 confirmed aliases per matched (matchup, event) — one for home, draw,
// away — atomically per matchup (skips the whole trio if any of the 3
// conditions already has a non-candidate alias).
func runAutoConfirmSoccer3Way(ctx context.Context, db *sql.DB, dryRun bool, windowInterval string) int {
	rows, err := db.QueryContext(ctx, autoConfirmSoccerSelectSQL, windowInterval)
	if err != nil {
		slog.Error("auto-confirm 3way select", "err", err)
		return 0
	}
	defer rows.Close()
	type trio struct {
		MatchupID        int64
		Home, Away       string
		Live             bool
		Title            string
		EventID          string
		HomeCond, HomeMk string
		DrawCond, DrawMk string
		AwayCond, AwayMk string
	}
	var found []trio
	for rows.Next() {
		var t trio
		if err := rows.Scan(&t.MatchupID, &t.Home, &t.Away, &t.Live, &t.Title,
			&t.EventID,
			&t.HomeCond, &t.HomeMk,
			&t.DrawCond, &t.DrawMk,
			&t.AwayCond, &t.AwayMk); err != nil {
			slog.Error("auto-confirm 3way scan", "err", err)
			return 0
		}
		found = append(found, t)
	}
	if len(found) == 0 {
		return 0
	}
	for _, t := range found {
		slog.Info("auto-confirm 3way candidate",
			"matchup", t.MatchupID, "home", t.Home, "away", t.Away,
			"live", t.Live, "pm_event", t.EventID, "pm_title", t.Title)
	}
	if dryRun {
		slog.Info("auto-confirm 3way dry-run; no writes", "matchups", len(found))
		return 0
	}
	inserted := 0
	for _, t := range found {
		note := fmt.Sprintf("auto-soccer-3way: live=%t, title=%q", t.Live, truncate(t.Title, 80))
		// Insert the 3 conditions atomically — if any of the 3 inserts fails
		// the whole trio is rolled back, otherwise we'd leave the matchup
		// in an inconsistent partial-confirmation state.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			slog.Error("auto-confirm 3way begin tx", "matchup", t.MatchupID, "err", err)
			continue
		}
		ok := true
		for _, leg := range []struct {
			side string
			cond string
			mk   string
		}{
			{"home", t.HomeCond, t.HomeMk},
			{"draw", t.DrawCond, t.DrawMk},
			{"away", t.AwayCond, t.AwayMk},
		} {
			if _, err := tx.ExecContext(ctx, autoConfirmInsertSQL,
				t.MatchupID, leg.cond, t.EventID, leg.mk, note+" side="+leg.side); err != nil {
				slog.Error("auto-confirm 3way insert", "matchup", t.MatchupID, "side", leg.side, "err", err)
				ok = false
				break
			}
		}
		if !ok {
			_ = tx.Rollback()
			continue
		}
		if err := tx.Commit(); err != nil {
			slog.Error("auto-confirm 3way commit", "matchup", t.MatchupID, "err", err)
			continue
		}
		inserted += 3
	}
	slog.Info("auto-confirm 3way done", "matchups", len(found), "newly_confirmed_rows", inserted)
	return inserted
}

func runUpdateStatus(ctx context.Context, db *sql.DB, matchupID int64, conditionID, status, notes string) {
	res, err := db.ExecContext(ctx, `
        UPDATE pinnacle_event_aliases
           SET match_status = $3,
               notes = CASE WHEN $4 = '' THEN notes ELSE $4 END,
               updated_at = now()
         WHERE pinnacle_matchup_id = $1
           AND polymarket_condition_id = $2
    `, matchupID, conditionID, status, notes)
	if err != nil {
		die("update: " + err.Error())
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		die(fmt.Sprintf("no alias row for matchup=%d condition=%s (run -mode write first?)", matchupID, conditionID))
	}
	fmt.Printf("updated %d row(s) to status=%s\n", rows, status)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
