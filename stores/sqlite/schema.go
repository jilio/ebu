package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// Schema version for migrations
const currentSchemaVersion = 1

// Schema definitions
const (
	createEventsTable = `
		CREATE TABLE IF NOT EXISTS events (
			position INTEGER PRIMARY KEY AUTOINCREMENT,
			type TEXT NOT NULL,
			data BLOB NOT NULL,
			timestamp DATETIME NOT NULL
		)`

	createEventsTypeIndex = `CREATE INDEX IF NOT EXISTS idx_events_type ON events(type)`

	createSubscriptionPositionsTable = `
		CREATE TABLE IF NOT EXISTS subscription_positions (
			subscription_id TEXT PRIMARY KEY,
			position INTEGER NOT NULL,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`

	createSchemaVersionTable = `
		CREATE TABLE IF NOT EXISTS schema_version (
			version INTEGER PRIMARY KEY,
			applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`
)

// migrate applies database migrations if needed
func migrate(ctx context.Context, db *sql.DB) error {
	// Create schema version table first (idempotent)
	_, err := db.ExecContext(ctx, createSchemaVersionTable)
	if err != nil {
		return fmt.Errorf("create schema_version table: %w", err)
	}

	// Check current version
	var version int
	err = db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_version").Scan(&version)
	if err != nil {
		return fmt.Errorf("get schema version: %w", err)
	}

	// Apply migrations
	if version < 1 {
		return migrateV1(ctx, db)
	}

	return nil
}

// migrateV1 applies the initial schema
func migrateV1(ctx context.Context, db *sql.DB) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Execute all schema creation in transaction
	statements := []string{
		createEventsTable,
		createEventsTypeIndex,
		createSubscriptionPositionsTable,
		"INSERT INTO schema_version (version) VALUES (1)",
	}

	for _, stmt := range statements {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec schema: %w", err)
		}
	}

	return tx.Commit()
}
