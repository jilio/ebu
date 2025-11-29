package sqlite

import (
	"context"
	"database/sql"
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
