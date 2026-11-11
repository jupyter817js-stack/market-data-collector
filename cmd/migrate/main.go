package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/pol-odds-collector/internal/config"
)

const (
	defaultDatabaseURL = "postgres://pol:pol@localhost:5432/pol_odds?sslmode=disable"
	migrationsDir      = "migrations"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Load .env so DATABASE_URL set in the project root is honored. Without
	// this the migrate command silently falls back to the hardcoded
	// defaultDatabaseURL (localhost:5432) — which historically did not match
	// the actual project DB on port 5433 and caused migrations to apply to a
	// stale orphan container.
	_, _ = config.Load()

	if err := run(context.Background()); err != nil {
		slog.Error("migration failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	db, err := sql.Open("pgx", databaseURL())
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	if err := ensureSchemaMigrations(ctx, db); err != nil {
		return err
	}

	files, err := migrationFiles(migrationsDir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		slog.Info("no migration files found", "dir", migrationsDir)
		return nil
	}

	for _, file := range files {
		if err := applyMigration(ctx, db, file); err != nil {
			return err
		}
	}
	return nil
}

func databaseURL() string {
	v := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if v == "" {
		return defaultDatabaseURL
	}
	return v
}

func ensureSchemaMigrations(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename text PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        )
    `)
	if err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}
	return nil
}

func migrationFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		files = append(files, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(files)
	return files, nil
}

func applyMigration(ctx context.Context, db *sql.DB, path string) error {
	filename := filepath.Base(path)
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", filename, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", filename, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `LOCK TABLE schema_migrations IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("lock schema_migrations for %s: %w", filename, err)
	}

	applied, err := migrationApplied(ctx, tx, filename)
	if err != nil {
		return err
	}
	if applied {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit skipped migration %s: %w", filename, err)
		}
		slog.Info("migration already applied", "filename", filename)
		return nil
	}

	if strings.TrimSpace(string(body)) == "" {
		return fmt.Errorf("migration %s is empty", filename)
	}
	// pgx/v5 stdlib uses the extended query protocol, which only allows ONE
	// SQL command per Exec. Migration files contain many statements, so we
	// split here and exec each separately. If any statement fails the
	// deferred tx.Rollback above unwinds the whole file, and we never insert
	// into schema_migrations — so the migration stays un-applied and the
	// operator sees the actual SQL error.
	stmts := splitSQLStatements(string(body))
	if len(stmts) == 0 {
		return fmt.Errorf("migration %s contains no executable statements", filename)
	}
	for i, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply migration %s statement %d: %w", filename, i+1, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO schema_migrations (filename, applied_at)
        VALUES ($1, now())
    `, filename); err != nil {
		return fmt.Errorf("record migration %s: %w", filename, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", filename, err)
	}
	slog.Info("migration applied", "filename", filename)
	return nil
}

func migrationApplied(ctx context.Context, tx *sql.Tx, filename string) (bool, error) {
	var applied bool
	err := tx.QueryRowContext(ctx, `
        SELECT EXISTS (
            SELECT 1
            FROM schema_migrations
            WHERE filename = $1
        )
    `, filename).Scan(&applied)
	if err != nil {
		return false, fmt.Errorf("check migration %s: %w", filename, err)
	}
	return applied, nil
}
