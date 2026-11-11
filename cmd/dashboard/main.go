// Command dashboard serves a single-page HTML view of the live pipeline.
// It is read-only and auto-refreshes via meta tag — no WebSocket / JS
// required. Default address :8090; open http://localhost:8090 in any
// browser. Sections gracefully skip if their tables are missing so the
// page is useful on a half-migrated DB.
//
//	go run ./cmd/dashboard                  # listen on :8090, refresh 5s
//	go run ./cmd/dashboard -addr :9090 -refresh 2s
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/pol-odds-collector/internal/config"
)

func main() {
	addr := flag.String("addr", ":8090", "HTTP listen address")
	refresh := flag.Duration("refresh", 5*time.Second, "browser auto-refresh interval")
	window := flag.Duration("window", time.Hour, "look-back window for rate stats")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(2)
	}
	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		slog.Error("open db", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.PingContext(context.Background()); err != nil {
		slog.Error("ping db", "err", err)
		os.Exit(1)
	}

	tmpl := template.Must(template.New("dashboard").
		Funcs(template.FuncMap{
			"pp":  func(f float64) string { return fmt.Sprintf("%+.2fpp", f*100) },
			"pct": func(f float64) string { return fmt.Sprintf("%.1f%%", f*100) },
			"price": func(v sql.NullFloat64) string {
				if !v.Valid {
					return "-"
				}
				return fmt.Sprintf("%.3f", v.Float64)
			},
			"prob":    func(f float64) string { return fmt.Sprintf("%.3f", f) },
			"usd":     func(f float64) string { return fmt.Sprintf("$%.2f", f) },
			"usdSign": func(f float64) string { return fmt.Sprintf("$%+.2f", f) },
			"ago":     prettyAge,
			"freshClass": func(d time.Duration) string {
				switch {
				case d < 10*time.Second:
					return "fresh"
				case d < 2*time.Minute:
					return "ok"
				case d < 10*time.Minute:
					return "warn"
				default:
					return "stale"
				}
			},
			"signClass": func(f float64) string {
				switch {
				case f > 0:
					return "pos"
				case f < 0:
					return "neg"
				}
				return ""
			},
			"trunc": func(s string, n int) string {
				if len(s) <= n {
					return s
				}
				return s[:n-1] + "…"
			},
		}).
		Parse(htmlTemplate))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		data := collect(ctx, db, *window)
		data.RefreshSec = int(refresh.Seconds())
		data.WindowLabel = window.String()
		data.GeneratedAt = time.Now().UTC()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if err := tmpl.Execute(w, data); err != nil {
			slog.Error("render", "err", err)
		}
	})

	slog.Info("dashboard listening", "addr", *addr, "refresh", *refresh, "open", "http://localhost"+*addr)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
}

// ---- data ----

type pageData struct {
	GeneratedAt time.Time
	RefreshSec  int
	WindowLabel string

	Collectors    []collectorRow
	Sports        []sportRow
	Aliases       aliasCounts
	ConfirmedHead []confirmedAlias
	Edges         []edgeRow
	Soccer3Way    []edgeRow
	SignalCounts  []stateCount
	FreshBuys     []freshBuy
	PaperRuns     []paperRunRow
	Orders        []orderRow
}

type collectorRow struct {
	Label   string
	Last    sql.NullTime
	Age     time.Duration
	WindowN int64
	Missing bool
}

type sportRow struct {
	SportID, Leagues int
	PinMatch         sql.NullInt64
	Live             sql.NullInt64
	Upcoming24h      sql.NullInt64
	Name             string
}

type aliasCounts struct {
	Confirmed int
	Candidate int
	Rejected  int
	Missing   bool
}

type confirmedAlias struct {
	MatchupID  int64
	Home, Away string
	Updated    time.Time
}

type edgeRow struct {
	MatchupID        int64
	SportID          int
	SportName        string
	League           string
	Home, Away, Side string
	Fair             float64
	Ask, Bid         sql.NullFloat64
	Gross            float64
	PinAge, PMAge    float64
}

type stateCount struct {
	State string
	Count int
}

type freshBuy struct {
	MatchupID  int64
	Home, Away string
	Side       string
	Fair       float64
	Ask        sql.NullFloat64
	Net        float64
	CreatedAt  time.Time
}

type paperRunRow struct {
	ID             int64
	StartedAt      time.Time
	Trades         int
	MTM            int
	Size, MTMPnL   float64
	MTMPP          float64
	Resolved, Wins int
	Realized       float64
	HitRate        sql.NullFloat64
}

type orderRow struct {
	ID               int64
	CreatedAt        time.Time
	Status           string
	MatchupID        int64
	Home, Away, Side string
	BS               string
	Size, Price      float64
	Maker, Taker     int64
	ClaimedNetEdge   float64
}

func collect(ctx context.Context, db *sql.DB, window time.Duration) *pageData {
	d := &pageData{}
	d.Collectors = collectFreshness(ctx, db, window)
	d.Sports = collectSports(ctx, db)
	d.Aliases = collectAliases(ctx, db)
	d.ConfirmedHead = collectConfirmedHead(ctx, db, 5)
	d.Edges = collectEdges(ctx, db, 12)
	d.Soccer3Way = collectSoccer3Way(ctx, db, 12)
	d.SignalCounts = collectSignalCounts(ctx, db, window)
	d.FreshBuys = collectFreshBuys(ctx, db, 8)
	d.PaperRuns = collectPaperRuns(ctx, db, 5)
	d.Orders = collectOrders(ctx, db, 8)
	return d
}

func collectFreshness(ctx context.Context, db *sql.DB, window time.Duration) []collectorRow {
	rows := []struct {
		label, q string
		args     []any
	}{
		{"pinnacle_market_history", `SELECT MAX(received_at), COUNT(*) FILTER (WHERE received_at >= now()-$1::interval) FROM pinnacle_market_history`, []any{window.String()}},
		{"pinnacle_matchups", `SELECT MAX(last_seen_at), COUNT(*) FILTER (WHERE last_seen_at >= now()-$1::interval) FROM pinnacle_matchups`, []any{window.String()}},
		{"pinnacle_collector_runs", `SELECT MAX(started_at), COUNT(*) FILTER (WHERE started_at >= now()-$1::interval) FROM pinnacle_collector_runs`, []any{window.String()}},
		{"polymarket_price_snapshots", `SELECT MAX(received_at), COUNT(*) FILTER (WHERE received_at >= now()-$1::interval) FROM polymarket_price_snapshots`, []any{window.String()}},
		{"polymarket_markets", `SELECT MAX(updated_at), COUNT(*) FILTER (WHERE updated_at >= now()-$1::interval) FROM polymarket_markets`, []any{window.String()}},
		{"polymarket_sync_runs", `SELECT MAX(started_at), COUNT(*) FILTER (WHERE started_at >= now()-$1::interval) FROM polymarket_sync_runs`, []any{window.String()}},
		{"signals", `SELECT MAX(created_at), COUNT(*) FILTER (WHERE created_at >= now()-$1::interval) FROM signals`, []any{window.String()}},
		// polymarket_dead_tokens: rows currently in backoff. WindowN here is
		// "how many tokens are being skipped right now" not how many were
		// added during the dashboard window — different semantics, but the
		// number is what the operator cares about ("how many tokens am I not
		// even trying to price?").
		{"polymarket_dead_tokens", `SELECT MAX(last_seen_at), COUNT(*) FILTER (WHERE skip_until > now()) FROM polymarket_dead_tokens`, nil},
	}
	out := make([]collectorRow, 0, len(rows))
	for _, r := range rows {
		var ts sql.NullTime
		var cnt int64
		err := db.QueryRowContext(ctx, r.q, r.args...).Scan(&ts, &cnt)
		row := collectorRow{Label: r.label, Last: ts, WindowN: cnt}
		if err != nil {
			row.Missing = isMissingTable(err)
		}
		if ts.Valid {
			row.Age = time.Since(ts.Time)
		}
		out = append(out, row)
	}
	return out
}

func collectSports(ctx context.Context, db *sql.DB) []sportRow {
	rows, err := db.QueryContext(ctx, `
        SELECT l.sport_id,
               COUNT(DISTINCT l.id) FILTER (WHERE l.last_matchup_count > 0),
               SUM(l.last_matchup_count),
               COUNT(DISTINCT m.id) FILTER (WHERE m.matchup_type='matchup' AND m.last_is_live),
               COUNT(DISTINCT m.id) FILTER (WHERE m.matchup_type='matchup'
                                              AND m.start_time BETWEEN now() AND now() + interval '24 hours')
          FROM pinnacle_leagues l
          LEFT JOIN pinnacle_matchups m ON m.league_id = l.id
         GROUP BY l.sport_id
         HAVING SUM(l.last_matchup_count) > 0 OR COUNT(m.id) > 0
         ORDER BY 3 DESC NULLS LAST
    `)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []sportRow{}
	for rows.Next() {
		var r sportRow
		if err := rows.Scan(&r.SportID, &r.Leagues, &r.PinMatch, &r.Live, &r.Upcoming24h); err != nil {
			continue
		}
		r.Name = sportName(r.SportID)
		out = append(out, r)
	}
	return out
}

func collectAliases(ctx context.Context, db *sql.DB) aliasCounts {
	var c aliasCounts
	err := db.QueryRowContext(ctx, `
        SELECT COUNT(*) FILTER (WHERE match_status = 'confirmed'),
               COUNT(*) FILTER (WHERE match_status = 'candidate'),
               COUNT(*) FILTER (WHERE match_status = 'rejected')
          FROM pinnacle_event_aliases
    `).Scan(&c.Confirmed, &c.Candidate, &c.Rejected)
	if err != nil {
		c.Missing = isMissingTable(err)
	}
	return c
}

func collectConfirmedHead(ctx context.Context, db *sql.DB, n int) []confirmedAlias {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
        SELECT pa.pinnacle_matchup_id, m.home_name, m.away_name, pa.updated_at
          FROM pinnacle_event_aliases pa
          JOIN pinnacle_matchups m ON m.id = pa.pinnacle_matchup_id
         WHERE pa.match_status = 'confirmed'
         ORDER BY pa.updated_at DESC
         LIMIT %d
    `, n))
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []confirmedAlias{}
	for rows.Next() {
		var a confirmedAlias
		_ = rows.Scan(&a.MatchupID, &a.Home, &a.Away, &a.Updated)
		out = append(out, a)
	}
	return out
}

func collectEdges(ctx context.Context, db *sql.DB, n int) []edgeRow {
	// _live view = last_is_live=true AND ask_price > 0.005, so the
	// section truly contains live actionable edges instead of fake
	// 60pp settlement noise.
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
        SELECT matchup_id, pinnacle_sport_id, league_name, home_name, away_name, outcome_side,
               COALESCE(pinnacle_fair_prob, 0), ask_price, bid_price,
               COALESCE(gross_edge_to_buy, 0),
               COALESCE(pinnacle_age_sec, 0), COALESCE(pm_age_sec, 0)
          FROM pinnacle_polymarket_moneyline_edges_live
         ORDER BY gross_edge_to_buy DESC NULLS LAST
         LIMIT %d
    `, n))
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []edgeRow{}
	for rows.Next() {
		var r edgeRow
		_ = rows.Scan(&r.MatchupID, &r.SportID, &r.League, &r.Home, &r.Away, &r.Side,
			&r.Fair, &r.Ask, &r.Bid, &r.Gross, &r.PinAge, &r.PMAge)
		r.SportName = sportName(r.SportID)
		out = append(out, r)
	}
	return out
}

// collectSoccer3Way is the same shape as collectEdges but reads from the
// soccer-only view. Two reasons to surface it separately:
//   - soccer 3-way uses title-join (no confirmed alias required), so it
//     can be empty even when 2-way base shows rows — easier to debug
//     when split out
//   - the bias of the 3-way path (PM "Will X win?" binaries) often has
//     wider spreads than 2-way; visually grouping makes that obvious
func collectSoccer3Way(ctx context.Context, db *sql.DB, n int) []edgeRow {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
        SELECT matchup_id, pinnacle_sport_id, league_name, home_name, away_name, outcome_side,
               COALESCE(pinnacle_fair_prob, 0), ask_price, bid_price,
               COALESCE(gross_edge_to_buy, 0),
               COALESCE(pinnacle_age_sec, 0), COALESCE(pm_age_sec, 0)
          FROM pinnacle_polymarket_soccer_3way_edges
         WHERE last_is_live = true
           AND ask_price IS NOT NULL
           AND ask_price > 0.005
           AND pm_age_sec < 120
           AND pinnacle_age_sec < 60
         ORDER BY gross_edge_to_buy DESC NULLS LAST
         LIMIT %d
    `, n))
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []edgeRow{}
	for rows.Next() {
		var r edgeRow
		_ = rows.Scan(&r.MatchupID, &r.SportID, &r.League, &r.Home, &r.Away, &r.Side,
			&r.Fair, &r.Ask, &r.Bid, &r.Gross, &r.PinAge, &r.PMAge)
		r.SportName = sportName(r.SportID)
		out = append(out, r)
	}
	return out
}

func collectSignalCounts(ctx context.Context, db *sql.DB, window time.Duration) []stateCount {
	rows, err := db.QueryContext(ctx, `
        SELECT state, COUNT(*) FROM signals
         WHERE created_at >= now() - $1::interval
         GROUP BY 1 ORDER BY 2 DESC
    `, window.String())
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []stateCount{}
	for rows.Next() {
		var s stateCount
		_ = rows.Scan(&s.State, &s.Count)
		out = append(out, s)
	}
	return out
}

func collectFreshBuys(ctx context.Context, db *sql.DB, n int) []freshBuy {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
        SELECT matchup_id, home_name, away_name, outcome_side,
               COALESCE(pinnacle_fair_prob, 0), pm_ask,
               COALESCE(net_edge_buy, 0), created_at
          FROM signals_buy_candidates_fresh
         ORDER BY net_edge_buy DESC NULLS LAST
         LIMIT %d
    `, n))
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []freshBuy{}
	for rows.Next() {
		var b freshBuy
		_ = rows.Scan(&b.MatchupID, &b.Home, &b.Away, &b.Side, &b.Fair, &b.Ask, &b.Net, &b.CreatedAt)
		out = append(out, b)
	}
	return out
}

func collectPaperRuns(ctx context.Context, db *sql.DB, n int) []paperRunRow {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
        SELECT id, started_at, trades_simulated, trades_with_mtm,
               total_size_usdc, total_mtm_pnl_usdc, avg_mtm_edge_pp,
               COALESCE(trades_resolved,0), COALESCE(trades_won,0),
               total_realized_pnl_usdc, hit_rate
          FROM paper_pnl_runs
         ORDER BY started_at DESC LIMIT %d
    `, n))
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []paperRunRow{}
	for rows.Next() {
		var r paperRunRow
		_ = rows.Scan(&r.ID, &r.StartedAt, &r.Trades, &r.MTM, &r.Size, &r.MTMPnL,
			&r.MTMPP, &r.Resolved, &r.Wins, &r.Realized, &r.HitRate)
		out = append(out, r)
	}
	return out
}

func collectOrders(ctx context.Context, db *sql.DB, n int) []orderRow {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
        SELECT id, created_at, status, matchup_id, home_name, away_name,
               outcome_side, side, size_usdc, price,
               maker_amount_6dec, taker_amount_6dec, claimed_net_edge
          FROM proposed_orders
         ORDER BY created_at DESC LIMIT %d
    `, n))
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []orderRow{}
	for rows.Next() {
		var r orderRow
		_ = rows.Scan(&r.ID, &r.CreatedAt, &r.Status, &r.MatchupID, &r.Home, &r.Away,
			&r.Side, &r.BS, &r.Size, &r.Price, &r.Maker, &r.Taker, &r.ClaimedNetEdge)
		out = append(out, r)
	}
	return out
}

// ---- helpers ----

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

func prettyAge(d time.Duration) string {
	switch {
	case d == 0:
		return "—"
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

// ---- template ----

const htmlTemplate = `<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="{{.RefreshSec}}">
<title>pol-odds dashboard</title>
<style>
  :root {
    --bg: #0e1116; --panel: #161b22; --border: #30363d; --text: #c9d1d9;
    --muted: #8b949e; --accent: #58a6ff; --good: #3fb950; --warn: #d29922; --bad: #f85149;
    --fresh: #3fb950; --ok: #58a6ff; --stale: #f85149;
  }
  body { background: var(--bg); color: var(--text); font: 13px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace; margin: 0; padding: 16px; }
  h1 { font-size: 18px; margin: 0 0 4px; color: var(--accent); }
  .meta { color: var(--muted); font-size: 11px; margin-bottom: 16px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 12px; }
  .full { grid-column: 1 / -1; }
  .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 6px; padding: 10px 12px; }
  .panel h2 { font-size: 12px; margin: 0 0 8px; color: var(--accent); text-transform: uppercase; letter-spacing: 0.5px; }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th, td { padding: 4px 6px; text-align: left; border-bottom: 1px solid var(--border); vertical-align: top; }
  th { color: var(--muted); font-weight: normal; font-size: 11px; text-transform: uppercase; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .fresh { color: var(--fresh); }
  .ok    { color: var(--ok); }
  .warn  { color: var(--warn); }
  .stale { color: var(--stale); }
  .pos   { color: var(--good); }
  .neg   { color: var(--bad); }
  .pill  { display: inline-block; padding: 1px 6px; border-radius: 8px; font-size: 11px; background: var(--border); }
  .pill.confirmed { background: rgba(63,185,80,0.2); color: var(--good); }
  .pill.candidate { background: rgba(210,153,34,0.2); color: var(--warn); }
  .pill.rejected  { background: rgba(248,81,73,0.2); color: var(--bad); }
  .pill.live      { background: rgba(63,185,80,0.2); color: var(--good); }
  .empty { color: var(--muted); font-style: italic; padding: 8px 0; }
  .footer { color: var(--muted); font-size: 11px; margin-top: 16px; text-align: center; }
  a { color: var(--accent); text-decoration: none; }
</style>
</head>
<body>

<h1>pol-odds live dashboard</h1>
<div class="meta">
  {{.GeneratedAt.Format "2006-01-02 15:04:05"}} UTC · window {{.WindowLabel}} · auto-refresh {{.RefreshSec}}s
</div>

<div class="grid">

  <div class="panel">
    <h2>collectors</h2>
    <table>
      <tr><th>source</th><th>last</th><th>age</th><th class="num">rows/win</th></tr>
      {{range .Collectors}}
      <tr>
        <td>{{.Label}}</td>
        {{if .Missing}}
          <td colspan="2" class="stale">(table missing — apply migration)</td><td class="num">-</td>
        {{else if not .Last.Valid}}
          <td colspan="2" class="muted">(no rows)</td><td class="num">0</td>
        {{else}}
          <td>{{.Last.Time.Format "15:04:05"}}</td>
          <td class="{{freshClass .Age}}">{{ago .Age}}</td>
          <td class="num">{{.WindowN}}</td>
        {{end}}
      </tr>
      {{end}}
    </table>
  </div>

  <div class="panel">
    <h2>aliases</h2>
    {{if .Aliases.Missing}}
      <div class="empty">pinnacle_event_aliases missing (apply migration 022)</div>
    {{else}}
      <div style="margin-bottom:8px">
        <span class="pill confirmed">confirmed {{.Aliases.Confirmed}}</span>
        <span class="pill candidate">candidate {{.Aliases.Candidate}}</span>
        <span class="pill rejected">rejected {{.Aliases.Rejected}}</span>
      </div>
      {{if .ConfirmedHead}}
      <table>
        <tr><th>matchup</th><th>home</th><th>away</th><th>updated</th></tr>
        {{range .ConfirmedHead}}
        <tr><td>{{.MatchupID}}</td><td>{{trunc .Home 18}}</td><td>{{trunc .Away 18}}</td><td>{{.Updated.Format "01-02 15:04"}}</td></tr>
        {{end}}
      </table>
      {{else}}<div class="empty">no confirmed aliases yet</div>{{end}}
    {{end}}
  </div>

  <div class="panel full">
    <h2>sports inventory (Pinnacle)</h2>
    {{if .Sports}}
    <table>
      <tr><th>sport</th><th class="num">id</th><th class="num">active leagues</th><th class="num">pinnacle matches</th><th class="num">live now</th><th class="num">upcoming 24h</th></tr>
      {{range .Sports}}
      <tr>
        <td>{{.Name}}</td>
        <td class="num">{{.SportID}}</td>
        <td class="num">{{.Leagues}}</td>
        <td class="num">{{if .PinMatch.Valid}}{{.PinMatch.Int64}}{{else}}-{{end}}</td>
        <td class="num">{{if .Live.Valid}}{{if gt .Live.Int64 0}}<span class="pill live">{{.Live.Int64}}</span>{{else}}0{{end}}{{else}}-{{end}}</td>
        <td class="num">{{if .Upcoming24h.Valid}}{{.Upcoming24h.Int64}}{{else}}-{{end}}</td>
      </tr>
      {{end}}
    </table>
    {{else}}<div class="empty">no sport inventory (no pinnacle_leagues rows)</div>{{end}}
  </div>

  <div class="panel full">
    <h2>top live edges <span class="empty">(live=true, ask&gt;0.005, pinnacle &lt;5min)</span></h2>
    {{if .Edges}}
    <table>
      <tr><th>matchup</th><th>sport</th><th>league</th><th>home</th><th>away</th><th>side</th><th class="num">fair</th><th class="num">ask</th><th class="num">bid</th><th class="num">gross</th><th class="num">pin age</th><th class="num">pm age</th></tr>
      {{range .Edges}}
      <tr>
        <td>{{.MatchupID}}</td>
        <td>{{.SportName}}</td>
        <td>{{trunc .League 22}}</td>
        <td>{{trunc .Home 16}}</td>
        <td>{{trunc .Away 16}}</td>
        <td>{{.Side}}</td>
        <td class="num">{{prob .Fair}}</td>
        <td class="num">{{price .Ask}}</td>
        <td class="num">{{price .Bid}}</td>
        <td class="num {{signClass .Gross}}">{{pp .Gross}}</td>
        <td class="num">{{printf "%.0fs" .PinAge}}</td>
        <td class="num">{{printf "%.0fs" .PMAge}}</td>
      </tr>
      {{end}}
    </table>
    {{else}}<div class="empty">no edge rows — needs confirmed aliases + live odds + PM prices</div>{{end}}
  </div>

  <div class="panel full">
    <h2>soccer 3-way live edges <span class="empty">(title-match path, no alias needed)</span></h2>
    {{if .Soccer3Way}}
    <table>
      <tr><th>matchup</th><th>league</th><th>home</th><th>away</th><th>side</th><th class="num">fair</th><th class="num">ask</th><th class="num">bid</th><th class="num">gross</th><th class="num">pin age</th><th class="num">pm age</th></tr>
      {{range .Soccer3Way}}
      <tr>
        <td>{{.MatchupID}}</td>
        <td>{{trunc .League 28}}</td>
        <td>{{trunc .Home 18}}</td>
        <td>{{trunc .Away 18}}</td>
        <td>{{.Side}}</td>
        <td class="num">{{prob .Fair}}</td>
        <td class="num">{{price .Ask}}</td>
        <td class="num">{{price .Bid}}</td>
        <td class="num {{signClass .Gross}}">{{pp .Gross}}</td>
        <td class="num">{{printf "%.0fs" .PinAge}}</td>
        <td class="num">{{printf "%.0fs" .PMAge}}</td>
      </tr>
      {{end}}
    </table>
    {{else}}<div class="empty">no soccer 3-way live edges — needs live PM-covered soccer match + fresh prices both sides</div>{{end}}
  </div>

  <div class="panel">
    <h2>signals ({{.WindowLabel}})</h2>
    {{if .SignalCounts}}
    <table>
      <tr><th>state</th><th class="num">count</th></tr>
      {{range .SignalCounts}}
      <tr><td>{{.State}}</td><td class="num">{{.Count}}</td></tr>
      {{end}}
    </table>
    {{else}}<div class="empty">no signals in window</div>{{end}}
  </div>

  <div class="panel">
    <h2>fresh BUY_CANDIDATEs</h2>
    {{if .FreshBuys}}
    <table>
      <tr><th>matchup</th><th>match</th><th>side</th><th class="num">fair</th><th class="num">ask</th><th class="num">net</th><th>at</th></tr>
      {{range .FreshBuys}}
      <tr>
        <td>{{.MatchupID}}</td>
        <td>{{trunc .Home 12}} vs {{trunc .Away 12}}</td>
        <td>{{.Side}}</td>
        <td class="num">{{prob .Fair}}</td>
        <td class="num">{{price .Ask}}</td>
        <td class="num pos">{{pp .Net}}</td>
        <td>{{.CreatedAt.Format "15:04:05"}}</td>
      </tr>
      {{end}}
    </table>
    {{else}}<div class="empty">no fresh BUY_CANDIDATE right now</div>{{end}}
  </div>

  <div class="panel">
    <h2>paper PnL runs</h2>
    {{if .PaperRuns}}
    <table>
      <tr><th>id</th><th>started</th><th class="num">trades</th><th class="num">size</th><th class="num">MTM</th><th class="num">real</th><th class="num">hit</th></tr>
      {{range .PaperRuns}}
      <tr>
        <td>{{.ID}}</td>
        <td>{{.StartedAt.Format "01-02 15:04"}}</td>
        <td class="num">{{.Trades}}</td>
        <td class="num">{{usd .Size}}</td>
        <td class="num {{signClass .MTMPnL}}">{{usdSign .MTMPnL}}</td>
        <td class="num {{signClass .Realized}}">{{usdSign .Realized}}</td>
        <td class="num">{{if .HitRate.Valid}}{{pct .HitRate.Float64}}{{else}}-{{end}}</td>
      </tr>
      {{end}}
    </table>
    {{else}}<div class="empty">no paper runs yet (run: go run ./cmd/paper-pnl -since 24h -write-run)</div>{{end}}
  </div>

  <div class="panel">
    <h2>proposed orders</h2>
    {{if .Orders}}
    <table>
      <tr><th>id</th><th>created</th><th>status</th><th>matchup</th><th>side</th><th>BS</th><th class="num">size</th><th class="num">price</th><th class="num">claim</th></tr>
      {{range .Orders}}
      <tr>
        <td>{{.ID}}</td>
        <td>{{.CreatedAt.Format "01-02 15:04"}}</td>
        <td><span class="pill">{{.Status}}</span></td>
        <td>{{.MatchupID}} {{trunc .Home 10}}/{{trunc .Away 10}}</td>
        <td>{{.Side}}</td>
        <td>{{.BS}}</td>
        <td class="num">{{usd .Size}}</td>
        <td class="num">{{prob .Price}}</td>
        <td class="num pos">{{pp .ClaimedNetEdge}}</td>
      </tr>
      {{end}}
    </table>
    {{else}}<div class="empty">no proposed orders (run: go run ./cmd/executor-plan -mode plan -maker 0x...)</div>{{end}}
  </div>

</div>

<div class="footer">
  pol-odds-collector · live-only Pinnacle ↔ Polymarket pipeline · auto-refresh disabled? <a href="/">manual refresh</a>
</div>
</body>
</html>
`
