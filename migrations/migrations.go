package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"
)

//go:embed 001_init.sql 002_leases.sql 003_job_lifecycle.sql 004_attempt_default.sql 005_queue_controls.sql 006_claim_rank.sql
var files embed.FS

var ordered = []string{"001_init.sql", "002_leases.sql", "003_job_lifecycle.sql", "004_attempt_default.sql", "005_queue_controls.sql", "006_claim_rank.sql"}

func Apply(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(746299017)`); err != nil {
		return fmt.Errorf("lock schema: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
	    version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create migration ledger: %w", err)
	}
	for _, name := range ordered {
		var applied bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (
		    SELECT 1 FROM schema_migrations WHERE version = $1
		)`, name).Scan(&applied); err != nil {
			return fmt.Errorf("read migration ledger: %w", err)
		}
		if applied {
			continue
		}
		script, err := files.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		for _, statement := range strings.Split(string(script), ";") {
			if strings.TrimSpace(statement) == "" {
				continue
			}
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply migration %s: %w", name, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema: %w", err)
	}
	return nil
}
