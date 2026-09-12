package sqlite

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

func TestFileConnectionPragmasAcrossPool(t *testing.T) {
	for _, constructor := range []struct {
		name     string
		open     func(string, ...Option) (*SQLiteStore, error)
		readOnly bool
	}{
		{"writer", New, false},
		{"reader", OpenReadOnly, true},
	} {
		for _, timeout := range []struct {
			name string
			opts []Option
			want int
		}{
			{"default", nil, 5000},
			{"custom", []Option{WithBusyTimeout(1250 * time.Millisecond)}, 1250},
		} {
			t.Run(constructor.name+"/"+timeout.name, func(t *testing.T) {
				path, _ := readOnlyFixture(t)
				rawReadOnlyFixtureSQL(t, path, "PRAGMA journal_mode=DELETE")
				before := readOnlyFileBytes(t, path)
				store, err := constructor.open(path, timeout.opts...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				store.db.SetMaxOpenConns(3)
				var connections []*sql.Conn
				// Hold all connections at once to force three separate SQLite
				// connections, including ones opened after schema validation.
				for range 3 {
					connection, err := store.db.Conn(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = connection.Close() })
					connections = append(connections, connection)
				}
				for i, connection := range connections {
					for pragma, want := range map[string]int{
						"busy_timeout": timeout.want,
						"cache_size":   -64000,
						"temp_store":   2, // MEMORY
						"mmap_size":    268435456,
					} {
						var got int
						if err := connection.QueryRowContext(t.Context(), "PRAGMA "+pragma).Scan(&got); err != nil || got != want {
							t.Errorf("connection %d: %s=%d, want %d (error %v)", i, pragma, got, want, err)
						}
					}
					if constructor.readOnly {
						var synchronous int
						if err := connection.QueryRowContext(t.Context(), "PRAGMA synchronous").Scan(&synchronous); err != nil || synchronous != 2 {
							t.Fatalf("connection %d changed SQLite's default synchronous setting: %d (error %v)", i, synchronous, err)
						}
						var queryOnly int
						if err := connection.QueryRowContext(t.Context(), "PRAGMA query_only").Scan(&queryOnly); err != nil || queryOnly != 1 {
							t.Fatalf("connection %d: query_only=%d (error %v)", i, queryOnly, err)
						}
						if _, err := connection.ExecContext(t.Context(), "CREATE TEMP TABLE forbidden (value TEXT)"); err == nil {
							t.Fatal("read-only connection accepted temporary table creation")
						}
						// Even without query_only, URI mode=ro must still protect
						// every connection from data and schema changes.
						if _, err := connection.ExecContext(t.Context(), "PRAGMA query_only=OFF"); err != nil {
							t.Fatal(err)
						}
						for _, statement := range []string{"DELETE FROM events", "CREATE TABLE forbidden (value TEXT)"} {
							if _, err := connection.ExecContext(t.Context(), statement); err == nil {
								t.Fatalf("connection %d accepted %s", i, statement)
							}
						}
					} else {
						var synchronous int
						if err := connection.QueryRowContext(t.Context(), "PRAGMA synchronous").Scan(&synchronous); err != nil || synchronous != 1 {
							t.Fatalf("connection %d: synchronous=%d (error %v)", i, synchronous, err)
						}
					}
					if err := connection.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				if constructor.readOnly {
					if !bytes.Equal(before, readOnlyFileBytes(t, path)) {
						t.Fatal("pooled read-only pragmas or rejected writes changed database bytes")
					}
					for _, suffix := range []string{"-wal", "-shm", "-journal"} {
						if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
							t.Fatalf("read-only opening created sidecar %s: %v", suffix, err)
						}
					}
				}
			})
		}
	}
}

func TestOpenReadOnlyTransactionDoesNotAcquireWriterLock(t *testing.T) {
	path, _ := readOnlyFixture(t)
	writer, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	reader, err := OpenReadOnly(path, WithBusyTimeout(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writeTx, err := writer.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writeTx.Rollback() })
	if _, err := writeTx.ExecContext(t.Context(), "UPDATE events SET type='uncommitted'"); err != nil {
		t.Fatal(err)
	}
	// Copying the writer's _txlock=immediate into the reader DSN would make
	// this begin fail while the writer's transaction is still open.
	readTx, err := reader.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("read transaction tried to acquire a writer lock: %v", err)
	}
	t.Cleanup(func() { _ = readTx.Rollback() })
	var eventType string
	if err := readTx.QueryRowContext(t.Context(), "SELECT type FROM events LIMIT 1").Scan(&eventType); err != nil || eventType != "example" {
		t.Fatalf("read transaction: type=%q error=%v", eventType, err)
	}
}

func TestNewAndOpenReadOnlyUseLiteralFilePaths(t *testing.T) {
	for _, name := range []string{
		"escaped%41.db",
		"literal%00.db",
		"query?mode=ro&_pragma=query_only(1).db",
		"fragment#events.db",
		"literal%3fmode=rw?mode=memory&cache=shared#fragment.db",
		"space + café.db",
	} {
		for _, relative := range []bool{false, true} {
			prefix := "absolute/"
			if relative {
				prefix = "relative/"
			}
			t.Run(prefix+name, func(t *testing.T) {
				directory := t.TempDir()
				path := filepath.Join(directory, name)
				if relative {
					t.Chdir(directory)
					path = name
				}
				writer, err := New(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = writer.Close() })
				offset, err := writer.Append(t.Context(), &eventbus.Event{Type: "literal-path", Data: []byte("payload"), Timestamp: time.Now()})
				if err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				entries, err := os.ReadDir(directory)
				if err != nil || len(entries) != 1 || entries[0].Name() != name {
					t.Fatalf("literal filename changed: entries=%v error=%v", entries, err)
				}
				before := readOnlyFileBytes(t, path)
				reader, err := OpenReadOnly(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = reader.Close() })
				events, tail, err := reader.Read(t.Context(), eventbus.OffsetOldest, 0)
				if err != nil || len(events) != 1 || tail != offset || events[0].Type != "literal-path" {
					t.Fatalf("literal path round trip: events=%v tail=%q error=%v", events, tail, err)
				}
				reader.db.SetMaxOpenConns(1)
				if _, err := reader.db.Exec("PRAGMA query_only=OFF"); err != nil {
					t.Fatal(err)
				}
				if _, err := reader.db.Exec("DELETE FROM events"); err == nil {
					t.Fatal("literal filename overrode read-only mode")
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before, readOnlyFileBytes(t, path)) {
					t.Fatal("read-only opening changed literal database")
				}
			})
		}
	}
}

func TestConstructorsRejectNULWithoutOpeningPrefix(t *testing.T) {
	path, _ := readOnlyFixture(t)
	before := readOnlyFileBytes(t, path)
	for _, constructor := range []struct {
		name string
		open func(string, ...Option) (*SQLiteStore, error)
	}{
		{"writer", New},
		{"reader", OpenReadOnly},
	} {
		t.Run(constructor.name, func(t *testing.T) {
			store, err := constructor.open(path + "\x00suffix")
			if store != nil {
				_ = store.Close()
			}
			if err == nil || store != nil {
				t.Fatal("invalid NUL filename opened its existing prefix")
			}
			if !bytes.Equal(before, readOnlyFileBytes(t, path)) {
				t.Fatal("invalid filename modified its existing prefix")
			}
		})
	}
}
