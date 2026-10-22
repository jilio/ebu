package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// Tests for error paths that the rest of the suite does not reach, so every
// statement of the module is exercised.

// It replaces a package variable, so it must not run in parallel with other
// tests of this package (none call t.Parallel).
func TestNewReportsAnUnresolvablePath(t *testing.T) {
	absPath = func(string) (string, error) { return "", errors.New("getwd: no such file or directory") }
	t.Cleanup(func() { absPath = filepath.Abs })
	for _, open := range []func(string, ...Option) (*SQLiteStore, error){New, OpenReadOnly} {
		if store, err := open("relative.db"); err == nil {
			_ = store.Close()
			t.Fatal("opened a store whose path cannot be resolved")
		} else if !strings.Contains(err.Error(), "resolve database path") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestMigratePropagatesV3Error(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "v3_propagate.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		createSchemaVersionTable,
		"INSERT INTO schema_version (version) VALUES (2)",
		"CREATE TRIGGER no_v3 BEFORE INSERT ON schema_version WHEN NEW.version = 3 BEGIN SELECT RAISE(ABORT, 'v3 refused'); END",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := RunMigrate(context.Background(), db); err == nil || !strings.Contains(err.Error(), "v3 refused") {
		t.Fatalf("migrate = %v, want the v3 failure", err)
	}
}

func TestMigrateV4Errors(t *testing.T) {
	ctx := context.Background()
	open := func(t *testing.T, statements ...string) *sql.DB {
		t.Helper()
		db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "v4.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		for _, statement := range statements {
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		return db
	}

	t.Run("begin on a closed db", func(t *testing.T) {
		db := open(t)
		_ = db.Close()
		if err := RunMigrateV4(ctx, db); err == nil || !strings.Contains(err.Error(), "begin transaction") {
			t.Fatalf("migrateV4 = %v", err)
		}
	})
	t.Run("column error other than a duplicate", func(t *testing.T) {
		db := open(t, createSchemaVersionTable) // no events table
		if err := RunMigrateV4(ctx, db); err == nil || !strings.Contains(err.Error(), "no such table: events") {
			t.Fatalf("migrateV4 = %v", err)
		}
	})
	t.Run("version insert error", func(t *testing.T) {
		db := open(t, createSchemaVersionTable, createEventsTable,
			"CREATE TRIGGER no_v4 BEFORE INSERT ON schema_version WHEN NEW.version = 4 BEGIN SELECT RAISE(ABORT, 'v4 refused'); END")
		if err := RunMigrateV4(ctx, db); err == nil || !strings.Contains(err.Error(), "v4 refused") {
			t.Fatalf("migrateV4 = %v", err)
		}
		// The failed transaction rolled back its ALTERs.
		var columns int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('events') WHERE name = 'event_id'`).Scan(&columns); err != nil || columns != 0 {
			t.Fatalf("event_id columns after rollback = %d, %v", columns, err)
		}
	})
}
