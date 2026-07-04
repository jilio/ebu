package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
	sqlite "github.com/jilio/ebu/stores/sqlite"
)

// openRawDB opens a bare sqlite db (no migrations) for migration-path tests.
func openRawDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "raw.db"))
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func appendN(t *testing.T, store *sqlite.SQLiteStore, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if _, err := store.Append(ctx, &eventbus.Event{
			Type:      "E",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
}

func TestSnapshotSaveLoadRoundTrip(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	blob := json.RawMessage(`{"deployments":{"d1":{}},"aliases":{"api":"d1"}}`)
	if err := store.SaveSnapshot(ctx, "deployments", "5", blob); err != nil {
		t.Fatalf("save: %v", err)
	}
	at, got, err := store.LoadSnapshot(ctx, "deployments")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if at != "00000000000000000005" {
		t.Errorf("offset: want position 5, got %s", at)
	}
	if string(got) != string(blob) {
		t.Errorf("blob mismatch: %s", got)
	}
}

func TestSnapshotLoadMissingKey(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	at, blob, err := store.LoadSnapshot(context.Background(), "nope")
	if err != nil {
		t.Fatalf("missing key must not error: %v", err)
	}
	if at != eventbus.OffsetOldest || blob != nil {
		t.Errorf("missing key must be (OffsetOldest, nil), got (%q, %v)", at, blob)
	}
}

func TestSnapshotUpsert(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "k", "3", json.RawMessage(`"first"`)); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(ctx, "k", "9", json.RawMessage(`"second"`)); err != nil {
		t.Fatal(err)
	}
	at, blob, err := store.LoadSnapshot(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if at != "00000000000000000009" || string(blob) != `"second"` {
		t.Errorf("upsert must keep the latest, got (%q, %s)", at, blob)
	}
	// Exactly one row for the key.
	var count int
	if err := store.GetDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM snapshots WHERE snapshot_id = ?", "k").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("upsert must not create a second row, got %d", count)
	}
}

func TestSnapshotSaveAtOffsetOldest(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "k", eventbus.OffsetOldest, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	at, _, err := store.LoadSnapshot(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if at != "00000000000000000000" { // position 0 round-trips zero-padded
		t.Errorf("OffsetOldest must store position 0, got %q", at)
	}
}

func TestSnapshotSaveInvalidOffset(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SaveSnapshot(context.Background(), "k", "not-a-number", json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected error for a non-numeric offset")
	}
}

func TestSnapshotSaveExecError(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	store.GetDB().Close() // close the pool so the prepared stmt exec fails
	if err := store.SaveSnapshot(context.Background(), "k", "1", json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected exec error on a closed db")
	}
}

func TestSnapshotLoadScanError(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	store.GetDB().Close()
	if _, _, err := store.LoadSnapshot(context.Background(), "k"); err == nil {
		t.Fatal("expected a non-ErrNoRows error on a closed db")
	}
}

func TestTruncateBeforeOldestIsNoop(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	appendN(t, store, 3)
	deleted, err := store.TruncateBefore(context.Background(), eventbus.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 {
		t.Errorf("OffsetOldest truncate must delete nothing, got %d", deleted)
	}
}

func TestTruncateBeforeInvalidOffset(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.TruncateBefore(context.Background(), "xyz"); err == nil {
		t.Fatal("expected error for a non-numeric offset")
	}
}

// TestTruncateBeforeCrossesDigitBoundary is the must-fix guard: truncate at "10"
// over 15 events must delete positions 1..10 (integer compare), NOT "10" < "2"
// lexical nonsense. A string-compare implementation would fail this.
func TestTruncateBeforeCrossesDigitBoundary(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	appendN(t, store, 15)

	deleted, err := store.TruncateBefore(ctx, "10")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 10 {
		t.Fatalf("truncate at 10 over 15 events must delete exactly 10, got %d", deleted)
	}
	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 {
		t.Fatalf("want 5 surviving events, got %d", len(events))
	}
	for _, e := range events {
		pos, _ := strconv.Atoi(string(e.Offset))
		if pos <= 10 {
			t.Fatalf("position %d should have been truncated", pos)
		}
	}
}

func TestTruncateBeforeIdempotent(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	appendN(t, store, 8)

	if d, err := store.TruncateBefore(ctx, "5"); err != nil || d != 5 {
		t.Fatalf("first truncate: deleted=%d err=%v", d, err)
	}
	if d, err := store.TruncateBefore(ctx, "5"); err != nil || d != 0 {
		t.Fatalf("re-truncate must delete nothing: deleted=%d err=%v", d, err)
	}
}

// TestTruncateThenReplayResumes proves a snapshot+truncate leaves the log
// resumable: after saving @7 and truncating <=7, replay/Read from offset "7"
// yields exactly positions 8..N.
func TestTruncateThenReplayResumes(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	appendN(t, store, 12)

	if err := store.SaveSnapshot(ctx, "deployments", "7", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TruncateBefore(ctx, "7"); err != nil {
		t.Fatal(err)
	}
	at, _, err := store.LoadSnapshot(ctx, "deployments")
	if err != nil {
		t.Fatal(err)
	}
	events, _, err := store.Read(ctx, at, 0) // Read is exclusive: position > 7
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 {
		t.Fatalf("want events 8..12, got %d", len(events))
	}
	if events[0].Offset != "00000000000000000008" {
		t.Errorf("resume must start at position 8, got %s", events[0].Offset)
	}
}

func TestTruncateBeforeExecError(t *testing.T) {
	store, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, store, 3)
	store.GetDB().Close()
	if _, err := store.TruncateBefore(context.Background(), "2"); err == nil {
		t.Fatal("expected a DELETE error on a closed db")
	}
}

func TestMigrateV2AutoOnNewStore(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "v2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var version int
	if err := store.GetDB().QueryRow("SELECT MAX(version) FROM schema_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Errorf("fresh store must be at schema version 2, got %d", version)
	}
	// snapshots table is usable end to end.
	if err := store.SaveSnapshot(context.Background(), "k", "1", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("snapshots table not created: %v", err)
	}
}

func TestMigrateV1ToV2Upgrade(t *testing.T) {
	ctx := context.Background()
	db := openRawDB(t)
	// Simulate a v1 database: schema_version present at version 1, no snapshots.
	if _, err := db.Exec("CREATE TABLE schema_version (version INTEGER PRIMARY KEY, applied_at DATETIME DEFAULT CURRENT_TIMESTAMP)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO schema_version (version) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	// RunMigrate sees version 1 and must apply only v2 (add snapshots, bump to 2).
	if err := sqlite.RunMigrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow("SELECT MAX(version) FROM schema_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Errorf("v1->v2 upgrade must reach 2, got %d", version)
	}
	if _, err := db.Exec("INSERT INTO snapshots (snapshot_id, position, data) VALUES ('k', 1, x'00')"); err != nil {
		t.Errorf("snapshots table missing after upgrade: %v", err)
	}
}

func TestMigrateV2Idempotent(t *testing.T) {
	ctx := context.Background()
	db := openRawDB(t)
	if err := sqlite.RunMigrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := sqlite.RunMigrate(ctx, db); err != nil { // second run: no-op
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM schema_version").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 { // exactly rows for v1 and v2, no duplicates
		t.Errorf("idempotent migrate must leave 2 version rows, got %d", count)
	}
}

func TestMigrateV2ExecSchemaError(t *testing.T) {
	ctx := context.Background()
	db := openRawDB(t)
	if _, err := db.Exec("CREATE TABLE schema_version (version INTEGER PRIMARY KEY, applied_at DATETIME)"); err != nil {
		t.Fatal(err)
	}
	// Pre-insert version 2 so migrateV2's own INSERT hits a PK conflict.
	if _, err := db.Exec("INSERT INTO schema_version (version) VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	err := sqlite.RunMigrateV2(ctx, db)
	if err == nil {
		t.Fatal("expected exec error from the duplicate version insert")
	}
}

func TestMigrateV2BeginTxError(t *testing.T) {
	db := openRawDB(t)
	db.Close() // BeginTx must fail on a closed db
	if err := sqlite.RunMigrateV2(context.Background(), db); err == nil {
		t.Fatal("expected begin transaction error on a closed db")
	}
}

// TestMigratePropagatesV1Error drives a migrateV1 failure THROUGH migrate() (a v0
// db) so the ordered-migration error return is exercised.
func TestMigratePropagatesV1Error(t *testing.T) {
	ctx := context.Background()
	db := openRawDB(t)
	// A pre-existing object named idx_events_type collides with migrateV1's
	// CREATE INDEX idx_events_type, failing v1 from inside migrate().
	if _, err := db.Exec("CREATE TABLE idx_events_type (x INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if err := sqlite.RunMigrate(ctx, db); err == nil {
		t.Fatal("expected migrate to propagate the v1 failure")
	}
}
