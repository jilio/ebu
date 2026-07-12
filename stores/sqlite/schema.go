package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Schema version for migrations
const currentSchemaVersion = 4

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

	// dropEventsTypeIndex (schema v3) removes idx_events_type: no query in
	// this store ever filters by type, so the index was pure write
	// amplification on every Append.
	dropEventsTypeIndex = `DROP INDEX IF EXISTS idx_events_type`

	// Schema v4 adds the event envelope (see eventbus.Event): a globally
	// unique event ID, the publishing bus's origin, and publisher-supplied
	// metadata (stored as a JSON object). All three are nullable so rows
	// written by earlier versions read back as empty envelope fields.
	addEventIDColumn  = `ALTER TABLE events ADD COLUMN event_id TEXT`
	addOriginColumn   = `ALTER TABLE events ADD COLUMN origin TEXT`
	addMetadataColumn = `ALTER TABLE events ADD COLUMN metadata TEXT`
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

	// Apply migrations in order. Each step seeds its own version row, so a v0
	// database runs every step through the current version; an older database
	// runs only the missing suffix. Version rows are inserted with INSERT OR
	// IGNORE so two
	// processes racing through migrate() (both reading the same stale version)
	// both succeed instead of the loser failing on the schema_version PK.
	if version < 1 {
		if err := migrateV1(ctx, db); err != nil {
			return err
		}
	}
	if version < 2 {
		if err := migrateV2(ctx, db); err != nil {
			return err
		}
	}
	if version < 3 {
		if err := migrateV3(ctx, db); err != nil {
			return err
		}
	}
	if version < currentSchemaVersion {
		return migrateV4(ctx, db)
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
		"INSERT OR IGNORE INTO schema_version (version) VALUES (1)",
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
		"INSERT OR IGNORE INTO schema_version (version) VALUES (2)",
	}

	for _, stmt := range statements {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec schema: %w", err)
		}
	}

	return tx.Commit()
}

// migrateV3 drops the unused idx_events_type index (see dropEventsTypeIndex).
func migrateV3(ctx context.Context, db *sql.DB) (err error) {
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
		dropEventsTypeIndex,
		"INSERT OR IGNORE INTO schema_version (version) VALUES (3)",
	}

	for _, stmt := range statements {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec schema: %w", err)
		}
	}

	return tx.Commit()
}

// migrateV4 adds the event envelope columns (see addEventIDColumn).
//
// ALTER TABLE ADD COLUMN has no IF NOT EXISTS, so — unlike the earlier,
// naturally idempotent migrations — a duplicate-column error must be treated
// as success: two processes racing through migrate() (both reading the same
// stale version) may both attempt the ALTERs, and the loser would otherwise
// fail on a column the winner already added.
func migrateV4(ctx context.Context, db *sql.DB) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for _, stmt := range []string{addEventIDColumn, addOriginColumn, addMetadataColumn} {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			if isDuplicateColumn(err) {
				err = nil
				continue
			}
			return fmt.Errorf("exec schema: %w", err)
		}
	}

	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO schema_version (version) VALUES (4)"); err != nil {
		return fmt.Errorf("exec schema: %w", err)
	}

	return tx.Commit()
}

// isDuplicateColumn reports whether err is SQLite's "duplicate column name"
// error from ALTER TABLE ADD COLUMN. SQLite reports it as a plain
// SQLITE_ERROR, so the message is the only discriminator.
func isDuplicateColumn(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate column name")
}
