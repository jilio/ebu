package sqlite

import (
	"context"
	"errors"
	"fmt"
)

// ErrReadOnly is returned by mutations on a store opened with OpenReadOnly.
var ErrReadOnly = errors.New("sqlite: store is read-only")

// OpenReadOnly opens an existing file-backed store without changing its data,
// schema or journal mode. It never creates or migrates a database. The stored
// schema must match the version this package reads; WithAutoMigrate(true) is
// rejected. Other options have the same meaning as for New.
// The path is a literal filename, not a SQLite URI.
//
// SQLite mode=ro is used, not immutable=1: reads can observe later commits by
// a concurrent writer. SQLite may create or update WAL shared-memory sidecars
// when the directory permits it; this is not an immutable filesystem snapshot.
// Open validates the schema, while corruption encountered reading event pages
// is reported by the read operation. No integrity scan or repair is performed.
func OpenReadOnly(path string, opts ...Option) (*SQLiteStore, error) {
	if path == "" || path == ":memory:" {
		return nil, errors.New("sqlite: read-only opening requires an existing file path")
	}
	cfg := defaultConfig()
	cfg.path = path
	cfg.autoMigrate = false
	cfg.readOnly = true
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.autoMigrate {
		return nil, errors.New("sqlite: read-only opening cannot enable automatic migration")
	}
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}
	db, err := dbOpener("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open read-only database: %w", err)
	}
	var version int
	if err := db.QueryRowContext(context.Background(), "SELECT MAX(version) FROM schema_version").Scan(&version); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: inspect read-only schema: %w", err)
	}
	if version != currentSchemaVersion {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: unsupported read-only schema version %d (expected %d)", version, currentSchemaVersion)
	}
	// database/sql Prepare may defer SQL compilation until first use. Execute
	// bounded read queries so a missing table or column fails opening rather
	// than appearing to be a valid initialized store. No event pages are scanned.
	for _, query := range []string{
		"SELECT position, type, data, timestamp, event_id, origin, metadata FROM events LIMIT 0",
		"SELECT subscription_id, position, updated_at FROM subscription_positions LIMIT 0",
		"SELECT snapshot_id, position, data, updated_at FROM snapshots LIMIT 0",
	} {
		rows, err := db.QueryContext(context.Background(), query)
		if err == nil {
			err = rows.Close()
		}
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("sqlite: inspect read-only schema: %w", err)
		}
	}
	return newFromDB(db, cfg, nil)
}
