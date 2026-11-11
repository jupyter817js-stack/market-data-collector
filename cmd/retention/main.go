package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultDatabaseURL                      = "postgres://pol:pol@localhost:5432/pol_odds?sslmode=disable"
	defaultRawSnapshotsDays                 = 90
	defaultQuoteHistoryDays                 = 180
	defaultSourceRunsDays                   = 180
	defaultPolymarketPriceSnapshotsDays     = 30
	defaultMaxDeleteRows                int = 5000
)

type retentionConfig struct {
	DatabaseURL                  string
	Execute                      bool
	MaxDeleteRows                int
	RawSnapshotsDays             int
	QuoteHistoryDays             int
	SourceRunsDays               int
	PolymarketPriceSnapshotsDays int
	Now                          time.Time
}

type retentionOperation struct {
	TableName string
	Days      int
	CountSQL  string
	DeleteSQL string
	Safety    string
}

type retentionResult struct {
	TableName     string
	Cutoff        time.Time
	RowsMatched   int64
	RowsDeleted   int64
	DeleteLimited bool
	Safety        string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		slog.Error("retention failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	cfg, err := parseConfig(args, out)
	if err != nil {
		return err
	}

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	results, err := applyRetention(ctx, db, cfg)
	if err != nil {
		return err
	}
	printResults(out, cfg, results)
	return nil
}

func parseConfig(args []string, out io.Writer) (retentionConfig, error) {
	fs := flag.NewFlagSet("retention", flag.ContinueOnError)
	fs.SetOutput(out)

	cfg := retentionConfig{}
	fs.StringVar(&cfg.DatabaseURL, "database-url", envDefault("DATABASE_URL", defaultDatabaseURL), "PostgreSQL connection string")
	fs.BoolVar(&cfg.Execute, "execute", false, "actually delete eligible rows; default is dry-run")
	fs.IntVar(&cfg.MaxDeleteRows, "max-delete-rows", defaultMaxDeleteRows, "maximum rows to delete per table in one execution")
	fs.IntVar(&cfg.RawSnapshotsDays, "raw-snapshots-days", defaultRawSnapshotsDays, "delete eligible raw_snapshots older than this many days; 0 disables")
	fs.IntVar(&cfg.QuoteHistoryDays, "quote-history-days", defaultQuoteHistoryDays, "delete eligible quote_history older than this many days; 0 disables")
	fs.IntVar(&cfg.SourceRunsDays, "source-runs-days", defaultSourceRunsDays, "delete source_runs older than this many days; 0 disables")
	fs.IntVar(&cfg.PolymarketPriceSnapshotsDays, "polymarket-price-snapshots-days", defaultPolymarketPriceSnapshotsDays, "delete eligible polymarket_price_snapshots older than this many days; 0 disables")

	if err := fs.Parse(args); err != nil {
		return retentionConfig{}, err
	}
	cfg.DatabaseURL = strings.TrimSpace(cfg.DatabaseURL)
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = defaultDatabaseURL
	}
	cfg.Now = time.Now().UTC()
	if cfg.MaxDeleteRows <= 0 {
		return retentionConfig{}, errors.New("max-delete-rows must be positive")
	}
	if err := validateDays("raw-snapshots-days", cfg.RawSnapshotsDays); err != nil {
		return retentionConfig{}, err
	}
	if err := validateDays("quote-history-days", cfg.QuoteHistoryDays); err != nil {
		return retentionConfig{}, err
	}
	if err := validateDays("source-runs-days", cfg.SourceRunsDays); err != nil {
		return retentionConfig{}, err
	}
	if err := validateDays("polymarket-price-snapshots-days", cfg.PolymarketPriceSnapshotsDays); err != nil {
		return retentionConfig{}, err
	}
	return cfg, nil
}

func applyRetention(ctx context.Context, db *sql.DB, cfg retentionConfig) ([]retentionResult, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin retention transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	results := make([]retentionResult, 0, 4)
	for _, op := range retentionOperations(cfg) {
		if op.Days == 0 {
			continue
		}
		result, err := applyRetentionOperation(ctx, tx, cfg, op)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit retention transaction: %w", err)
	}
	return results, nil
}

func applyRetentionOperation(ctx context.Context, tx *sql.Tx, cfg retentionConfig, op retentionOperation) (retentionResult, error) {
	cutoff := cfg.Now.Add(-time.Duration(op.Days) * 24 * time.Hour)
	result := retentionResult{
		TableName: op.TableName,
		Cutoff:    cutoff,
		Safety:    op.Safety,
	}

	if err := tx.QueryRowContext(ctx, op.CountSQL, cutoff).Scan(&result.RowsMatched); err != nil {
		return result, fmt.Errorf("count retention candidates for %s: %w", op.TableName, err)
	}
	if !cfg.Execute || result.RowsMatched == 0 {
		return result, nil
	}

	if err := tx.QueryRowContext(ctx, op.DeleteSQL, cutoff, cfg.MaxDeleteRows).Scan(&result.RowsDeleted); err != nil {
		return result, fmt.Errorf("delete retention candidates for %s: %w", op.TableName, err)
	}
	result.DeleteLimited = result.RowsMatched > result.RowsDeleted
	return result, nil
}

func retentionOperations(cfg retentionConfig) []retentionOperation {
	return []retentionOperation{
		{
			TableName: "quote_history",
			Days:      cfg.QuoteHistoryDays,
			CountSQL: `
                SELECT count(*)
                FROM quote_history qh
                WHERE qh.written_at < $1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM latest_quotes lq
                      WHERE lq.quote_history_id = qh.id
                  )
            `,
			DeleteSQL: `
                WITH victims AS (
                    SELECT qh.id
                    FROM quote_history qh
                    WHERE qh.written_at < $1
                      AND NOT EXISTS (
                          SELECT 1
                          FROM latest_quotes lq
                          WHERE lq.quote_history_id = qh.id
                      )
                    ORDER BY qh.written_at, qh.id
                    LIMIT $2
                ),
                deleted AS (
                    DELETE FROM quote_history qh
                    USING victims
                    WHERE qh.id = victims.id
                    RETURNING qh.id
                )
                SELECT count(*) FROM deleted
            `,
			Safety: "keeps rows referenced by latest_quotes",
		},
		{
			TableName: "raw_snapshots",
			Days:      cfg.RawSnapshotsDays,
			CountSQL: `
                SELECT count(*)
                FROM raw_snapshots rs
                WHERE rs.received_at < $1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM quote_history qh
                      WHERE qh.raw_snapshot_id = rs.id
                  )
            `,
			DeleteSQL: `
                WITH victims AS (
                    SELECT rs.id
                    FROM raw_snapshots rs
                    WHERE rs.received_at < $1
                      AND NOT EXISTS (
                          SELECT 1
                          FROM quote_history qh
                          WHERE qh.raw_snapshot_id = rs.id
                      )
                    ORDER BY rs.received_at, rs.id
                    LIMIT $2
                ),
                deleted AS (
                    DELETE FROM raw_snapshots rs
                    USING victims
                    WHERE rs.id = victims.id
                    RETURNING rs.id
                )
                SELECT count(*) FROM deleted
            `,
			Safety: "deletes only unreferenced raw snapshots",
		},
		{
			TableName: "source_runs",
			Days:      cfg.SourceRunsDays,
			CountSQL: `
                SELECT count(*)
                FROM source_runs
                WHERE finished_at < $1
            `,
			DeleteSQL: `
                WITH victims AS (
                    SELECT id
                    FROM source_runs
                    WHERE finished_at < $1
                    ORDER BY finished_at, id
                    LIMIT $2
                ),
                deleted AS (
                    DELETE FROM source_runs sr
                    USING victims
                    WHERE sr.id = victims.id
                    RETURNING sr.id
                )
                SELECT count(*) FROM deleted
            `,
			Safety: "telemetry only; no quote dependencies",
		},
		{
			TableName: "polymarket_price_snapshots",
			Days:      cfg.PolymarketPriceSnapshotsDays,
			CountSQL: `
                WITH latest AS (
                    SELECT DISTINCT ON (token_id) id
                    FROM polymarket_price_snapshots
                    ORDER BY token_id, received_at DESC, id DESC
                )
                SELECT count(*)
                FROM polymarket_price_snapshots pps
                LEFT JOIN latest
                  ON latest.id = pps.id
                WHERE pps.received_at < $1
                  AND latest.id IS NULL
            `,
			DeleteSQL: `
                WITH latest AS (
                    SELECT DISTINCT ON (token_id) id
                    FROM polymarket_price_snapshots
                    ORDER BY token_id, received_at DESC, id DESC
                ),
                victims AS (
                    SELECT pps.id
                    FROM polymarket_price_snapshots pps
                    LEFT JOIN latest
                      ON latest.id = pps.id
                    WHERE pps.received_at < $1
                      AND latest.id IS NULL
                    ORDER BY pps.received_at, pps.id
                    LIMIT $2
                ),
                deleted AS (
                    DELETE FROM polymarket_price_snapshots pps
                    USING victims
                    WHERE pps.id = victims.id
                    RETURNING pps.id
                )
                SELECT count(*) FROM deleted
            `,
			Safety: "keeps latest snapshot per token",
		},
	}
}

func printResults(out io.Writer, cfg retentionConfig, results []retentionResult) {
	mode := "dry-run"
	if cfg.Execute {
		mode = "execute"
	}
	_, _ = fmt.Fprintf(out, "retention mode=%s dry_run=%t max_delete_rows=%d\n", mode, !cfg.Execute, cfg.MaxDeleteRows)
	_, _ = fmt.Fprintln(out, "protected_tables=latest_quotes,canonical_events,source_event_aliases,manual_event_aliases,polymarket_event_aliases,match_review_decisions")
	if len(results) == 0 {
		_, _ = fmt.Fprintln(out, "no retention operations enabled")
		return
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "table\tcutoff\trows_matched\trows_deleted\tlimited\tsafety")
	for _, result := range results {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%t\t%s\n",
			result.TableName,
			result.Cutoff.UTC().Format(time.RFC3339),
			result.RowsMatched,
			result.RowsDeleted,
			result.DeleteLimited,
			result.Safety,
		)
	}
	_ = tw.Flush()
}

func validateDays(name string, days int) error {
	if days < 0 {
		return fmt.Errorf("%s must be >= 0", name)
	}
	return nil
}

func envDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
