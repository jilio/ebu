package sqlite

import (
	"context"
	"database/sql"

	eventbus "github.com/jilio/ebu"
)

// RunMigrate runs migration on a database (exported for testing)
func RunMigrate(ctx context.Context, db *sql.DB) error {
	return migrate(ctx, db)
}

// RunMigrateV1 runs v1 migration on a database (exported for testing)
func RunMigrateV1(ctx context.Context, db *sql.DB) error {
	return migrateV1(ctx, db)
}

// NewFromDB creates a store from an existing db connection (exported for testing)
// This allows testing the error path in newFromDB when prepareStatements fails
func NewFromDB(db *sql.DB) (*SQLiteStore, error) {
	return newFromDB(db, defaultConfig())
}

// SetDBOpener sets the database opener function (for testing)
func SetDBOpener(opener func(driverName, dataSourceName string) (*sql.DB, error)) {
	dbOpener = opener
}

// ResetDBOpener restores the default database opener
func ResetDBOpener() {
	dbOpener = sql.Open
}

// GetDB exposes the internal db for testing
func (s *SQLiteStore) GetDB() *sql.DB {
	return s.db
}

// RowScanner is the interface for scanning rows (exported for testing)
type RowScanner = rowScanner

// ScanEvents exposes scanEvents for testing
func (s *SQLiteStore) ScanEvents(rows RowScanner) ([]any, error) {
	events, err := s.scanEvents(rows)
	if err != nil {
		return nil, err
	}
	// Convert to []any to avoid importing eventbus in export_test
	result := make([]any, len(events))
	for i, e := range events {
		result[i] = e
	}
	return result, nil
}

// StreamRowsYieldFunc is the yield function type for StreamRows
type StreamRowsYieldFunc = func(*eventbus.StoredEvent, error) bool

// StreamRows exposes streamRows for testing error paths
func (s *SQLiteStore) StreamRows(
	ctx context.Context,
	rows RowScanner,
	eventCount *int,
	iterErr *error,
	yield StreamRowsYieldFunc,
) {
	s.streamRows(ctx, rows, eventCount, iterErr, yield)
}

// StreamBatch exposes streamBatch for testing error paths (e.g., rows.Close() error)
func (s *SQLiteStore) StreamBatch(
	rows RowScanner,
	eventCount *int,
	iterErr *error,
	yield StreamRowsYieldFunc,
) (batchCount int, lastPos int64, cont bool) {
	return s.streamBatch(rows, eventCount, iterErr, yield)
}
