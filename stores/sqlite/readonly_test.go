package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

func readOnlyFixture(t *testing.T) (string, []eventbus.Offset) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	writer, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	var offsets []eventbus.Offset
	for i := range 5 {
		offset, err := writer.Append(t.Context(), &eventbus.Event{
			ID: fmt.Sprintf("event-%d", i), Type: "example", Data: []byte(fmt.Sprint(i)),
			Timestamp: time.Unix(int64(i), 0).UTC(), Origin: "writer", Metadata: map[string]string{"example": "metadata"},
		})
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, offset)
	}
	if err := writer.SaveOffset(t.Context(), "reader", offsets[1]); err != nil {
		t.Fatal(err)
	}
	if err := writer.SaveSnapshot(t.Context(), "projection", offsets[1], json.RawMessage(`{"count":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return path, offsets
}

func rawReadOnlyFixtureSQL(t *testing.T, path, statement string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}

func readOnlyFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestOpenReadOnlyReadsWithoutDataSchemaOrJournalModeChanges(t *testing.T) {
	for _, journalMode := range []string{"WAL", "DELETE"} {
		t.Run(journalMode, func(t *testing.T) {
			path, offsets := readOnlyFixture(t)
			rawReadOnlyFixtureSQL(t, path, "PRAGMA journal_mode="+journalMode)
			before := readOnlyFileBytes(t, path)
			hook := &testMetricsHook{}
			reader, err := OpenReadOnly(path, WithAutoMigrate(false), WithBusyTimeout(time.Second), WithStreamBatchSize(2), WithMetricsHook(hook))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			events, next, err := reader.Read(t.Context(), eventbus.OffsetOldest, 2)
			if err != nil || len(events) != 2 || next != offsets[1] || events[0].ID != "event-0" || events[0].Metadata["example"] != "metadata" {
				t.Fatalf("first read: events=%v next=%q err=%v", events, next, err)
			}
			var streamed []string
			for event, err := range reader.ReadStream(t.Context(), next) {
				if err != nil {
					t.Fatal(err)
				}
				streamed = append(streamed, event.ID)
			}
			if !reflect.DeepEqual(streamed, []string{"event-2", "event-3", "event-4"}) {
				t.Fatalf("stream=%v", streamed)
			}
			if events, tail, err := reader.Read(t.Context(), eventbus.OffsetNewest, 0); err != nil || len(events) != 0 || tail != offsets[4] {
				t.Fatalf("opaque tail=%q events=%v err=%v", tail, events, err)
			}
			if offset, err := reader.LoadOffset(t.Context(), "reader"); err != nil || offset != offsets[1] {
				t.Fatalf("saved offset=%q %v", offset, err)
			}
			if _, found, err := reader.LookupOffset(t.Context(), "missing"); err != nil || found {
				t.Fatalf("missing offset found=%v err=%v", found, err)
			}
			if offset, blob, err := reader.LoadSnapshot(t.Context(), "projection"); err != nil || offset != offsets[1] || string(blob) != `{"count":2}` {
				t.Fatalf("snapshot=%q at=%q err=%v", blob, offset, err)
			}
			for _, mutate := range []func() error{
				func() error { _, err := reader.Append(t.Context(), nil); return err },
				func() error { return reader.SaveOffset(t.Context(), "reader", eventbus.OffsetNewest) },
				func() error {
					return reader.SaveSnapshot(t.Context(), "projection", offsets[4], json.RawMessage(`{"changed":true}`))
				},
				func() error { _, err := reader.TruncateBefore(t.Context(), offsets[4]); return err },
				func() error { _, err := reader.TruncateBefore(t.Context(), eventbus.OffsetOldest); return err },
			} {
				if err := mutate(); !errors.Is(err, ErrReadOnly) {
					t.Fatalf("mutation returned %v", err)
				}
			}
			if hook.appendCount != 1 || !errors.Is(hook.lastAppendErr, ErrReadOnly) || hook.saveOffsetCount != 1 || !errors.Is(hook.lastSaveOffErr, ErrReadOnly) {
				t.Fatal("mutation rejection was not reported to metrics hook")
			}
			// The underlying SQLite connection must enforce mode=ro even if a
			// future internal call accidentally bypasses the public method guard.
			reader.db.SetMaxOpenConns(1)
			if _, err := reader.db.Exec("PRAGMA query_only=OFF"); err != nil {
				t.Fatal(err)
			}
			for _, statement := range []string{"CREATE TABLE unexpected (value TEXT)", "DELETE FROM events", "UPDATE schema_version SET version=99"} {
				if _, err := reader.db.Exec(statement); err == nil {
					t.Fatalf("read-only SQLite accepted %s", statement)
				}
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if after := readOnlyFileBytes(t, path); !bytes.Equal(before, after) {
				t.Fatal("read-only open/read/rejected mutation/close changed database bytes")
			}
			if journalMode == "DELETE" {
				if _, err := os.Stat(path + "-wal"); !os.IsNotExist(err) {
					t.Fatal("read-only opening created WAL for a rollback-journal database")
				}
			}
		})
	}
}

func TestOpenReadOnlyRejectsMissingUninitializedCorruptAndUnsupportedWithoutRepair(t *testing.T) {
	for _, kind := range []string{"missing", "empty", "garbage", "truncated", "unrelated", "missing-schema-version", "empty-schema-version", "older-schema", "future-schema", "missing-events", "missing-offsets", "missing-snapshots", "missing-event-column"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.db")
			switch kind {
			case "missing":
			case "empty", "garbage":
				raw := []byte(nil)
				if kind == "garbage" {
					raw = []byte("not a SQLite database")
				}
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			case "unrelated":
				rawReadOnlyFixtureSQL(t, path, "CREATE TABLE unrelated (value TEXT)")
			default:
				path, _ = readOnlyFixture(t)
				switch kind {
				case "truncated":
					if err := os.Truncate(path, 64); err != nil {
						t.Fatal(err)
					}
				case "missing-schema-version":
					rawReadOnlyFixtureSQL(t, path, "DROP TABLE schema_version")
				case "empty-schema-version":
					rawReadOnlyFixtureSQL(t, path, "DELETE FROM schema_version")
				case "older-schema":
					rawReadOnlyFixtureSQL(t, path, "DELETE FROM schema_version WHERE version=4")
				case "future-schema":
					rawReadOnlyFixtureSQL(t, path, "INSERT INTO schema_version(version) VALUES(5)")
				case "missing-events":
					rawReadOnlyFixtureSQL(t, path, "DROP TABLE events")
				case "missing-offsets":
					rawReadOnlyFixtureSQL(t, path, "DROP TABLE subscription_positions")
				case "missing-snapshots":
					rawReadOnlyFixtureSQL(t, path, "DROP TABLE snapshots")
				case "missing-event-column":
					rawReadOnlyFixtureSQL(t, path, "ALTER TABLE events DROP COLUMN metadata")
				}
			}
			var before []byte
			if kind != "missing" {
				before = readOnlyFileBytes(t, path)
			}
			reader, err := OpenReadOnly(path)
			if err == nil || reader != nil {
				if reader != nil {
					_ = reader.Close()
				}
				t.Fatal("invalid database opened read-only successfully")
			}
			if kind == "missing" {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("read-only open created missing database")
				}
			} else if after := readOnlyFileBytes(t, path); !bytes.Equal(before, after) {
				t.Fatal("failed read-only opening repaired or mutated database")
			}
		})
	}
}

func TestOpenReadOnlyRejectsWriteOptionsAndEscapesLiteralFilenames(t *testing.T) {
	for _, path := range []string{"", ":memory:"} {
		if reader, err := OpenReadOnly(path); err == nil || reader != nil {
			t.Fatalf("read-only path %q accepted", path)
		}
	}
	missing := filepath.Join(t.TempDir(), "must-not-create.db")
	if reader, err := OpenReadOnly(missing, WithAutoMigrate(true)); err == nil || reader != nil {
		t.Fatal("write-enabling option accepted")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("conflicting options created a database")
	}
	path, _ := readOnlyFixture(t)
	literal := filepath.Join(filepath.Dir(path), "literal%3fmode=rw?mode=rwc#fragment.db")
	if err := os.Rename(path, literal); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReadOnly(literal)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if events, _, err := reader.Read(t.Context(), eventbus.OffsetOldest, 0); err != nil || len(events) != 5 {
		t.Fatalf("escaped path read: %d %v", len(events), err)
	}
	if _, err := reader.Append(t.Context(), nil); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("append with default options: %v", err)
	}
	if err := reader.SaveOffset(t.Context(), "reader", eventbus.OffsetOldest); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("save offset with default options: %v", err)
	}
}

func TestOpenReadOnlyReportsPathAndDriverErrors(t *testing.T) {
	t.Run("removed-working-directory", func(t *testing.T) {
		directory := t.TempDir()
		t.Chdir(directory)
		if err := os.Remove(directory); err != nil {
			t.Fatal(err)
		}
		if reader, err := OpenReadOnly("relative.db"); err == nil || reader != nil {
			t.Fatal("unresolvable relative path was accepted")
		}
	})
	t.Run("driver-open", func(t *testing.T) {
		previous := dbOpener
		t.Cleanup(func() { dbOpener = previous })
		want := errors.New("driver open failed")
		dbOpener = func(string, string) (*sql.DB, error) { return nil, want }
		if reader, err := OpenReadOnly(filepath.Join(t.TempDir(), "events.db")); !errors.Is(err, want) || reader != nil {
			t.Fatalf("driver failure: reader=%v error=%v", reader, err)
		}
	})
}

func TestOpenReadOnlyObservesConcurrentWALCommitsAndPooledReadOnlyConnections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	writer, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	writer.db.SetMaxOpenConns(1)
	if _, err := writer.db.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReadOnly(path, WithStreamBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	if events, _, err := reader.Read(t.Context(), eventbus.OffsetOldest, 0); err != nil || len(events) != 0 {
		t.Fatalf("initialized empty store: events=%v err=%v", events, err)
	}
	reader.db.SetMaxOpenConns(3)
	var connections []*sql.Conn
	for range 3 {
		connection, err := reader.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, connection)
	}
	for _, connection := range connections {
		var queryOnly int
		if err := connection.QueryRowContext(t.Context(), "PRAGMA query_only").Scan(&queryOnly); err != nil || queryOnly != 1 {
			t.Fatalf("pooled query_only=%d err=%v", queryOnly, err)
		}
		if _, err := connection.ExecContext(t.Context(), "DELETE FROM events"); err == nil {
			t.Fatal("pooled connection was writable")
		}
		if err := connection.Close(); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	committed := make(chan error)
	done := make(chan struct{})
	defer func() { cancel(); <-done }()
	go func() {
		defer close(done)
		for i := range 30 {
			_, err := writer.Append(ctx, &eventbus.Event{ID: fmt.Sprintf("concurrent-%d", i), Type: "event", Data: []byte("payload"), Timestamp: time.Now()})
			select {
			case committed <- err:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var cursor eventbus.Offset = eventbus.OffsetOldest
	for i := range 30 {
		select {
		case err := <-committed:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		events, next, err := reader.Read(ctx, cursor, 1)
		if err != nil || len(events) != 1 || events[0].ID != fmt.Sprintf("concurrent-%d", i) {
			t.Fatalf("WAL reader at %d: %v %v", i, events, err)
		}
		if order, err := reader.CompareOffsets(cursor, next); err != nil || order >= 0 {
			t.Fatalf("opaque offsets did not advance: %q -> %q (%v)", cursor, next, err)
		}
		cursor = next
	}
	if _, err := os.Stat(path + "-wal"); err != nil {
		t.Fatal("writer fixture did not retain a live WAL")
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if events, tail, err := reader.Read(ctx, eventbus.OffsetNewest, 0); err != nil || len(events) != 0 || tail != cursor {
		t.Fatalf("tail after writer close=%q err=%v", tail, err)
	}
}
