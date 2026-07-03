package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// Schema version for migrations
const currentSchemaVersion = 2

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

	// createSnapshotsTable (schema v2) holds one compaction snapshot per id: an
	// opaque blob tagged with the position it reflects, so a caller can load it
	// and replay only events with a greater position.
	createSnapshotsTable = `
		CREATE TABLE IF NOT EXISTS snapshots (
			snapshot_id TEXT PRIMARY KEY,
			position    INTEGER NOT NULL,
			data        BLOB NOT NULL,
			updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
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

	// Apply migrations in order. migrateV1 seeds version=1, so a v0 database runs
	// v1 then v2; a v1 database runs only v2; a v2 database applies nothing.
	if version < 1 {
		if err := migrateV1(ctx, db); err != nil {
			return err
		}
	}
	if version < 2 {
		return migrateV2(ctx, db)
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

// migrateV2 adds the snapshots table for log compaction.
func migrateV2(ctx context.Context, db *sql.DB) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	statements := []string{
		createSnapshotsTable,
		"INSERT INTO schema_version (version) VALUES (2)",
	}

	for _, stmt := range statements {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec schema: %w", err)
		}
	}

	return tx.Commit()
}
