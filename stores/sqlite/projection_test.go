package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

func appendJSON(t *testing.T, store eventbus.EventStore, typ string, payload any) eventbus.Offset {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := store.Append(context.Background(), &eventbus.Event{ID: eventbus.NewEventID(), Type: typ, Data: data, Timestamp: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return offset
}

// --- A job queue with leases and due times ---------------------------------

type jobEvent struct {
	ID         string `json:"id"`
	DueAt      int64  `json:"due_at,omitempty"`
	Worker     string `json:"worker,omitempty"`
	LeaseUntil int64  `json:"lease_until,omitempty"`
}

func jobQueueProjection(source eventbus.EventStore) ProjectionConfig {
	return ProjectionConfig{
		Name:    "jobs",
		Version: 1,
		Source:  source,
		Setup: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE IF NOT EXISTS jobs (
					id          TEXT PRIMARY KEY,
					state       TEXT NOT NULL,
					due_at      INTEGER NOT NULL,
					worker      TEXT,
					lease_until INTEGER
				);
				CREATE INDEX IF NOT EXISTS idx_jobs_due ON jobs(state, due_at)`)
			return err
		},
		Reset: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS jobs`)
			return err
		},
		Apply: func(ctx context.Context, tx *sql.Tx, event *eventbus.StoredEvent) error {
			var job jobEvent
			if err := json.Unmarshal(event.Data, &job); err != nil {
				return err
			}
			var err error
			switch event.Type {
			case "job.enqueued":
				_, err = tx.ExecContext(ctx, `INSERT INTO jobs (id, state, due_at) VALUES (?, 'ready', ?)`, job.ID, job.DueAt)
			case "job.leased":
				// Log order arbitrates: the first lease of a ready job wins,
				// later competing leases change nothing.
				_, err = tx.ExecContext(ctx, `UPDATE jobs SET state = 'leased', worker = ?, lease_until = ? WHERE id = ? AND state = 'ready'`, job.Worker, job.LeaseUntil, job.ID)
			case "job.done":
				_, err = tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, job.ID)
			}
			return err
		},
	}
}

func dueJobs(t *testing.T, p *Projection, now int64) []string {
	t.Helper()
	rows, err := p.DB().QueryContext(t.Context(), `SELECT id FROM jobs WHERE state = 'ready' AND due_at <= ? ORDER BY due_at`, now)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestProjectionJobQueueIsQueryableByIndex(t *testing.T) {
	store := newTrimStore(t)
	p, err := store.OpenProjection(t.Context(), jobQueueProjection(store))
	if err != nil {
		t.Fatal(err)
	}
	appendJSON(t, store, "job.enqueued", jobEvent{ID: "a", DueAt: 30})
	appendJSON(t, store, "job.enqueued", jobEvent{ID: "b", DueAt: 10})
	appendJSON(t, store, "job.enqueued", jobEvent{ID: "c", DueAt: 20})
	appendJSON(t, store, "job.leased", jobEvent{ID: "b", Worker: "w1", LeaseUntil: 99})
	appendJSON(t, store, "job.leased", jobEvent{ID: "b", Worker: "w2", LeaseUntil: 120})
	last := appendJSON(t, store, "job.done", jobEvent{ID: "c"})

	checkpoint, err := p.CatchUp(t.Context())
	if err != nil || checkpoint != last || p.Checkpoint() != last {
		t.Fatalf("CatchUp = %q, %v (checkpoint %q, want %q)", checkpoint, err, p.Checkpoint(), last)
	}
	if got := dueJobs(t, p, 40); strings.Join(got, ",") != "a" {
		t.Fatalf("due jobs = %v", got)
	}
	var worker string
	if err := p.DB().QueryRowContext(t.Context(), `SELECT worker FROM jobs WHERE id = 'b'`).Scan(&worker); err != nil || worker != "w1" {
		t.Fatalf("lease holder = %q, %v; the first lease in log order must win", worker, err)
	}
	var plan strings.Builder
	rows, err := p.DB().QueryContext(t.Context(), `EXPLAIN QUERY PLAN SELECT id FROM jobs WHERE state = 'ready' AND due_at <= 40 ORDER BY due_at`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail)
	}
	rows.Close()
	if !strings.Contains(plan.String(), "idx_jobs_due") {
		t.Fatalf("query plan does not use the projection's index: %s", plan.String())
	}
	if p.Name() != "jobs" {
		t.Fatalf("Name() = %q", p.Name())
	}
}

// --- Counters: exactly-once, non-idempotent updates -------------------------

type counterProjection struct {
	applied atomic.Int64
	failOn  atomic.Value // string event ID that fails once
}

func (c *counterProjection) config(name string, source eventbus.EventStore, batch int) ProjectionConfig {
	return ProjectionConfig{
		Name:      name,
		Version:   1,
		Source:    source,
		BatchSize: batch,
		Setup: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS error_groups (
				fingerprint TEXT PRIMARY KEY, count INTEGER NOT NULL, last_seen TEXT NOT NULL)`)
			return err
		},
		Reset: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS error_groups`)
			return err
		},
		Apply: func(ctx context.Context, tx *sql.Tx, event *eventbus.StoredEvent) error {
			if fail, _ := c.failOn.Load().(string); fail != "" && fail == event.ID {
				c.failOn.Store("")
				return errors.New("injected apply failure")
			}
			c.applied.Add(1)
			var group struct{ Fingerprint string }
			if err := json.Unmarshal(event.Data, &group); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO error_groups (fingerprint, count, last_seen) VALUES (?, 1, ?)
				ON CONFLICT(fingerprint) DO UPDATE SET count = count + 1, last_seen = excluded.last_seen`,
				group.Fingerprint, string(event.Offset))
			return err
		},
	}
}

func groupCounts(t *testing.T, db *sql.DB) map[string]int {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT fingerprint, count FROM error_groups`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var fp string
		var n int
		if err := rows.Scan(&fp, &n); err != nil {
			t.Fatal(err)
		}
		counts[fp] = n
	}
	return counts
}

func appendGroups(t *testing.T, store eventbus.EventStore, n int) []eventbus.Offset {
	t.Helper()
	offsets := make([]eventbus.Offset, n)
	for i := range n {
		data, _ := json.Marshal(map[string]string{"Fingerprint": fmt.Sprintf("g%d", i%3)})
		offset, err := store.Append(context.Background(), &eventbus.Event{ID: fmt.Sprintf("e%d", i+1), Type: "error.reported", Data: data, Timestamp: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		offsets[i] = offset
	}
	return offsets
}

func TestProjectionAppliesEachEventExactlyOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	store, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	counter := &counterProjection{}
	p, err := store.OpenProjection(t.Context(), counter.config("groups", store, 5))
	if err != nil {
		t.Fatal(err)
	}
	offsets := appendGroups(t, store, 12)

	// The second batch fails on its second event: the whole batch rolls back.
	counter.failOn.Store("e7")
	checkpoint, err := p.CatchUp(t.Context())
	if err == nil || !strings.Contains(err.Error(), "injected apply failure") || checkpoint != offsets[4] {
		t.Fatalf("CatchUp = %q, %v", checkpoint, err)
	}
	if counts := groupCounts(t, p.DB()); counts["g0"]+counts["g1"]+counts["g2"] != 5 {
		t.Fatalf("a failed batch leaked into the tables: %v", counts)
	}
	// Retrying applies the rest once.
	if checkpoint, err = p.CatchUp(t.Context()); err != nil || checkpoint != offsets[11] {
		t.Fatalf("CatchUp = %q, %v", checkpoint, err)
	}
	if counts := groupCounts(t, p.DB()); counts["g0"] != 4 || counts["g1"] != 4 || counts["g2"] != 4 {
		t.Fatalf("counts = %v", counts)
	}

	// Restart: the tables are the snapshot; only the tail is applied.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	restarted := &counterProjection{}
	p, err = store.OpenProjection(t.Context(), restarted.config("groups", store, 5))
	if err != nil || p.Checkpoint() != offsets[11] {
		t.Fatalf("reopened checkpoint = %q, %v", p.Checkpoint(), err)
	}
	appendGroups(t, store, 2)
	if _, err := p.CatchUp(t.Context()); err != nil {
		t.Fatal(err)
	}
	if restarted.applied.Load() != 2 {
		t.Fatalf("restart re-applied %d events, want only the 2-event tail", restarted.applied.Load())
	}
	if counts := groupCounts(t, p.DB()); counts["g0"] != 5 || counts["g1"] != 5 || counts["g2"] != 4 {
		t.Fatalf("counts after restart = %v", counts)
	}
}

func TestProjectionInstancesShareOneCheckpoint(t *testing.T) {
	store := newTrimStore(t)
	counters := []*counterProjection{{}, {}, {}}
	var projections []*Projection
	for _, c := range counters {
		p, err := store.OpenProjection(t.Context(), c.config("groups", store, 3))
		if err != nil {
			t.Fatal(err)
		}
		projections = append(projections, p)
	}
	const total = 90
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		appendGroups(t, store, total)
	}()
	for _, p := range projections {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 40 {
				if _, err := p.CatchUp(context.Background()); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for _, p := range projections {
		if _, err := p.CatchUp(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	applied := counters[0].applied.Load() + counters[1].applied.Load() + counters[2].applied.Load()
	counts := groupCounts(t, store.db)
	if applied != total || counts["g0"]+counts["g1"]+counts["g2"] != total {
		t.Fatalf("applied %d, counted %v; every event must be applied once across instances", applied, counts)
	}
}

func TestProjectionRebuildsOnVersionChange(t *testing.T) {
	store := newTrimStore(t)
	counter := &counterProjection{}
	cfg := counter.config("groups", store, 0)
	p, err := store.OpenProjection(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	appendGroups(t, store, 6)
	if _, err := p.CatchUp(t.Context()); err != nil {
		t.Fatal(err)
	}

	cfg.Version = 2
	cfg.Reset = nil
	if _, err := store.OpenProjection(t.Context(), cfg); err == nil || !strings.Contains(err.Error(), "requires Reset") {
		t.Fatalf("version change without Reset = %v", err)
	}
	rebuilt := &counterProjection{}
	cfg2 := rebuilt.config("groups", store, 0)
	cfg2.Version = 2
	p2, err := store.OpenProjection(t.Context(), cfg2)
	if err != nil || p2.Checkpoint() != eventbus.OffsetOldest || len(groupCounts(t, store.db)) != 0 {
		t.Fatalf("rebuild open = %q, %v, %v", p2.Checkpoint(), err, groupCounts(t, store.db))
	}
	// An older binary must not discard the newer projection.
	if _, err := store.OpenProjection(t.Context(), counter.config("groups", store, 0)); !errors.Is(err, ErrProjectionRebuild) || !strings.Contains(err.Error(), "newer than 1") {
		t.Fatalf("downgrade = %v", err)
	}
	// The old instance notices instead of applying stale logic.
	appendGroups(t, store, 1)
	if _, err := p.CatchUp(t.Context()); !errors.Is(err, ErrProjectionVersionChanged) {
		t.Fatalf("stale instance CatchUp = %v", err)
	}
	if _, err := p2.CatchUp(t.Context()); err != nil || rebuilt.applied.Load() != 7 {
		t.Fatalf("rebuild applied %d events, %v", rebuilt.applied.Load(), err)
	}
}

func TestProjectionOverAnotherStoreAndCompaction(t *testing.T) {
	source := eventbus.NewMemoryStore()
	db := newTrimStore(t)
	counter := &counterProjection{}
	p, err := db.OpenProjection(t.Context(), counter.config("groups", source, 2))
	if err != nil {
		t.Fatal(err)
	}
	appendGroups(t, source, 5)
	checkpoint, err := p.CatchUp(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// Compacting the source through the checkpoint rewrites no payloads: the
	// projection's tables already hold the state.
	if result, err := source.Trim(t.Context(), eventbus.RetentionPolicy{Through: checkpoint}); err != nil || result.Deleted != 5 {
		t.Fatalf("Trim = %+v, %v", result, err)
	}
	appendGroups(t, source, 1)
	if _, err := p.CatchUp(t.Context()); err != nil || counter.applied.Load() != 6 {
		t.Fatalf("CatchUp after compaction = %v (applied %d)", err, counter.applied.Load())
	}

	// A source trimmed past the checkpoint is reported, not skipped.
	appendGroups(t, source, 2)
	events, _, _ := source.Read(t.Context(), p.Checkpoint(), 0)
	if _, err := source.Trim(t.Context(), eventbus.RetentionPolicy{Through: events[0].Offset}); err != nil {
		t.Fatal(err)
	}
	before := p.Checkpoint()
	if checkpoint, err := p.CatchUp(t.Context()); !errors.Is(err, eventbus.ErrOffsetTrimmed) || checkpoint != before {
		t.Fatalf("CatchUp behind the trimmed prefix = %q, %v", checkpoint, err)
	}
}

// advancingSource returns no events but moves its resume token once, like a
// remote log that skipped an empty or undecodable chunk.
type advancingSource struct{ eventbus.EventStore }

func (advancingSource) Read(_ context.Context, from eventbus.Offset, _ int) ([]*eventbus.StoredEvent, eventbus.Offset, error) {
	return nil, "token-1", nil
}

func TestProjectionPersistsAnAdvancedEmptyRead(t *testing.T) {
	store := newTrimStore(t)
	counter := &counterProjection{}
	p, err := store.OpenProjection(t.Context(), counter.config("groups", advancingSource{store}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint, err := p.CatchUp(t.Context()); err != nil || checkpoint != "token-1" {
		t.Fatalf("CatchUp = %q, %v", checkpoint, err)
	}
	var stored string
	if err := store.db.QueryRow(`SELECT checkpoint FROM ebu_projections WHERE name = 'groups'`).Scan(&stored); err != nil || stored != "token-1" {
		t.Fatalf("stored checkpoint = %q, %v", stored, err)
	}
}

func TestOpenProjectionValidation(t *testing.T) {
	store := newTrimStore(t)
	valid := (&counterProjection{}).config("groups", store, 0)
	for _, tc := range []struct {
		name   string
		mutate func(*ProjectionConfig)
		want   string
	}{
		{"name", func(c *ProjectionConfig) { c.Name = "" }, "name is required"},
		{"source", func(c *ProjectionConfig) { c.Source = nil }, "requires a Source"},
		{"apply", func(c *ProjectionConfig) { c.Apply = nil }, "requires Apply"},
		{"batch", func(c *ProjectionConfig) { c.BatchSize = -1 }, "must not be negative"},
		{"setup", func(c *ProjectionConfig) {
			c.Setup = func(context.Context, *sql.Tx) error { return errors.New("no schema") }
		}, "setup: no schema"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			if _, err := store.OpenProjection(t.Context(), cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("OpenProjection = %v, want %q", err, tc.want)
			}
		})
	}

	path, _ := readOnlyFixture(t)
	reader, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.OpenProjection(t.Context(), valid); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only OpenProjection = %v", err)
	}
}

func TestProjectionStorageErrors(t *testing.T) {
	ctx := t.Context()
	store := newTrimStore(t)
	cfg := (&counterProjection{}).config("groups", store, 0)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.OpenProjection(cancelled, cfg); err == nil || !strings.Contains(err.Error(), "begin") {
		t.Fatalf("cancelled OpenProjection = %v", err)
	}
	if _, err := store.db.Exec(`CREATE INDEX ebu_projections ON events(type)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenProjection(ctx, cfg); err == nil || !strings.Contains(err.Error(), "create checkpoint table") {
		t.Fatalf("OpenProjection = %v", err)
	}
	if _, err := store.db.Exec(`DROP INDEX ebu_projections; ` + createProjectionsTable + `;
		CREATE TRIGGER no_register BEFORE INSERT ON ebu_projections BEGIN SELECT RAISE(ABORT, 'no'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenProjection(ctx, cfg); err == nil || !strings.Contains(err.Error(), "register") {
		t.Fatalf("OpenProjection = %v", err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER no_register; INSERT INTO ebu_projections (name, version, checkpoint) VALUES ('groups', 'one', '')`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenProjection(ctx, cfg); err == nil || !strings.Contains(err.Error(), "load checkpoint") {
		t.Fatalf("OpenProjection = %v", err)
	}
	if _, err := store.db.Exec(`UPDATE ebu_projections SET version = 0`); err != nil {
		t.Fatal(err)
	}
	failingReset := cfg
	failingReset.Reset = func(context.Context, *sql.Tx) error { return errors.New("cannot reset") }
	if _, err := store.OpenProjection(ctx, failingReset); err == nil || !strings.Contains(err.Error(), "reset: cannot reset") {
		t.Fatalf("OpenProjection = %v", err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER no_reset BEFORE UPDATE ON ebu_projections BEGIN SELECT RAISE(ABORT, 'no'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenProjection(ctx, cfg); err == nil || !strings.Contains(err.Error(), "reset checkpoint") {
		t.Fatalf("OpenProjection = %v", err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER no_reset; UPDATE ebu_projections SET version = 1`); err != nil {
		t.Fatal(err)
	}

	p, err := store.OpenProjection(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	appendGroups(t, store, 2)
	if _, err := p.CatchUp(cancelled); err == nil {
		t.Fatal("CatchUp with a cancelled context succeeded")
	}
	if _, err := store.db.Exec(`CREATE TRIGGER no_save BEFORE UPDATE ON ebu_projections BEGIN SELECT RAISE(ABORT, 'no'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CatchUp(ctx); err == nil || !strings.Contains(err.Error(), "save checkpoint") {
		t.Fatalf("CatchUp = %v", err)
	}
	if counts := groupCounts(t, store.db); len(counts) != 0 {
		t.Fatalf("a failed checkpoint save leaked applied rows: %v", counts)
	}
	if _, err := store.db.Exec(`DROP TRIGGER no_save; DELETE FROM ebu_projections`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CatchUp(ctx); err == nil || !strings.Contains(err.Error(), "load checkpoint") {
		t.Fatalf("CatchUp without a checkpoint row = %v", err)
	}
	// A source read failure is reported with the checkpoint it read after.
	broken := cfg
	broken.Name = "broken"
	broken.Source = failingReadSource{store}
	bp, err := store.OpenProjection(ctx, broken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bp.CatchUp(ctx); err == nil || !strings.Contains(err.Error(), `read after ""`) {
		t.Fatalf("CatchUp = %v", err)
	}
}

type failingReadSource struct{ eventbus.EventStore }

func (failingReadSource) Read(context.Context, eventbus.Offset, int) ([]*eventbus.StoredEvent, eventbus.Offset, error) {
	return nil, "", errors.New("source unavailable")
}

// trimmerSource reports a trim boundary (or fails) through Trim.
type trimmerSource struct {
	eventbus.EventStore
	boundary eventbus.Offset
	err      error
}

func (s trimmerSource) Trim(context.Context, eventbus.RetentionPolicy) (eventbus.TrimResult, error) {
	return eventbus.TrimResult{TrimmedThrough: s.boundary}, s.err
}

func TestProjectionRefusesRebuildOverATrimmedSource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source func(t *testing.T, store *SQLiteStore) eventbus.EventStore
		want   string
	}{
		{"same sqlite store", func(t *testing.T, store *SQLiteStore) eventbus.EventStore {
			offsets := appendGroups(t, store, 3)
			if _, err := store.Trim(t.Context(), eventbus.RetentionPolicy{Through: offsets[0]}); err != nil {
				t.Fatal(err)
			}
			return store
		}, "trimmed through"},
		{"memory store", func(t *testing.T, _ *SQLiteStore) eventbus.EventStore {
			source := eventbus.NewMemoryStore()
			offsets := appendGroups(t, source, 3)
			if _, err := source.Trim(t.Context(), eventbus.RetentionPolicy{Through: offsets[0]}); err != nil {
				t.Fatal(err)
			}
			return source
		}, "trimmed through"},
		{"failing boundary lookup", func(t *testing.T, store *SQLiteStore) eventbus.EventStore {
			return trimmerSource{EventStore: store, err: errors.New("boundary unavailable")}
		}, "check source before rebuild: boundary unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTrimStore(t)
			source := tc.source(t, store)
			cfg := (&counterProjection{}).config("groups", source, 0)
			if _, err := store.OpenProjection(t.Context(), cfg); err != nil {
				t.Fatal(err)
			}
			cfg.Version = 2
			_, err := store.OpenProjection(t.Context(), cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("rebuild = %v, want %q", err, tc.want)
			}
			if tc.want == "trimmed through" && !errors.Is(err, ErrProjectionRebuild) {
				t.Fatalf("rebuild = %v, want ErrProjectionRebuild", err)
			}
			var version int
			if err := store.db.QueryRow(`SELECT version FROM ebu_projections WHERE name = 'groups'`).Scan(&version); err != nil || version != 1 {
				t.Fatalf("a refused rebuild changed the stored version to %d (%v)", version, err)
			}
		})
	}
	// A source that cannot report trimming does not block a rebuild, and an
	// untrimmed trimmer allows it.
	for _, source := range []eventbus.EventStore{struct{ eventbus.EventStore }{eventbus.NewMemoryStore()}, eventbus.NewMemoryStore()} {
		store := newTrimStore(t)
		cfg := (&counterProjection{}).config("groups", source, 0)
		if _, err := store.OpenProjection(t.Context(), cfg); err != nil {
			t.Fatal(err)
		}
		cfg.Version = 2
		if _, err := store.OpenProjection(t.Context(), cfg); err != nil {
			t.Fatalf("rebuild over %T = %v", source, err)
		}
	}
}

func TestZeroProjectionCheckpoint(t *testing.T) {
	if got := (&Projection{}).Checkpoint(); got != eventbus.OffsetOldest {
		t.Fatalf("zero Projection checkpoint = %q", got)
	}
}

func TestProjectionApplyPanicReleasesTheWriteLock(t *testing.T) {
	store, err := New(filepath.Join(t.TempDir(), "events.db"), WithBusyTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	counter := &counterProjection{}
	cfg := counter.config("groups", store, 0)
	apply := cfg.Apply
	var panicked atomic.Bool
	cfg.Apply = func(ctx context.Context, tx *sql.Tx, event *eventbus.StoredEvent) error {
		if panicked.CompareAndSwap(false, true) {
			panic("apply bug")
		}
		return apply(ctx, tx, event)
	}
	p, err := store.OpenProjection(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	appendGroups(t, store, 3)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Apply panic did not propagate")
			}
		}()
		_, _ = p.CatchUp(t.Context())
	}()
	// The panicking batch released its write lock: appends and the retry work.
	appendGroups(t, store, 1)
	if _, err := p.CatchUp(t.Context()); err != nil || counter.applied.Load() != 4 {
		t.Fatalf("CatchUp after a panic = %v (applied %d)", err, counter.applied.Load())
	}
}

func TestProjectionIgnoresTrimLookupFailureWithoutRebuild(t *testing.T) {
	store := newTrimStore(t)
	source := trimmerSource{EventStore: store, err: errors.New("boundary unavailable")}
	cfg := (&counterProjection{}).config("groups", source, 0)
	for range 2 {
		if _, err := store.OpenProjection(t.Context(), cfg); err != nil {
			t.Fatalf("OpenProjection without a pending rebuild = %v", err)
		}
	}
}

// stuckSource returns the same event forever without advancing its offset.
type stuckSource struct{ eventbus.EventStore }

func (stuckSource) Read(_ context.Context, from eventbus.Offset, _ int) ([]*eventbus.StoredEvent, eventbus.Offset, error) {
	data, _ := json.Marshal(map[string]string{"Fingerprint": "g"})
	return []*eventbus.StoredEvent{{Offset: from, Type: "error.reported", Data: data}}, from, nil
}

func TestProjectionRejectsASourceThatDoesNotAdvance(t *testing.T) {
	store := newTrimStore(t)
	counter := &counterProjection{}
	p, err := store.OpenProjection(t.Context(), counter.config("groups", stuckSource{store}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.CatchUp(t.Context()); err == nil || !strings.Contains(err.Error(), "without advancing its offset") {
		t.Fatalf("CatchUp = %v", err)
	}
	if counter.applied.Load() != 0 {
		t.Fatalf("applied %d events from a stuck source", counter.applied.Load())
	}
}

// Codex review findings: after another instance rebuilt the projection, an
// idle stale instance must not report a successful catch-up, and its cached
// Checkpoint is not a retention bound (Committed is).
func TestProjectionStaleInstanceAfterARebuild(t *testing.T) {
	ctx := t.Context()
	store := newTrimStore(t)
	appendGroups(t, store, 4)
	old := &counterProjection{}
	a, err := store.OpenProjection(ctx, old.config("groups", store, 0))
	if err != nil {
		t.Fatal(err)
	}
	tail, err := a.CatchUp(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if committed, err := a.Committed(ctx); err != nil || committed != tail {
		t.Fatalf("Committed = %q, %v; want %q", committed, err, tail)
	}

	cfg := (&counterProjection{}).config("groups", store, 0)
	cfg.Version = 2
	b, err := store.OpenProjection(ctx, cfg) // resets the tables
	if err != nil {
		t.Fatal(err)
	}
	// No new events: the stale instance's read is empty, yet it must notice.
	if _, err := a.CatchUp(ctx); !errors.Is(err, ErrProjectionVersionChanged) {
		t.Fatalf("stale CatchUp = %v, want ErrProjectionVersionChanged", err)
	}
	if a.Checkpoint() != tail {
		t.Fatalf("cached checkpoint = %q", a.Checkpoint())
	}
	if _, err := a.Committed(ctx); !errors.Is(err, ErrProjectionVersionChanged) {
		t.Fatalf("stale Committed = %v, want ErrProjectionVersionChanged", err)
	}
	if committed, err := b.Committed(ctx); err != nil || committed != eventbus.OffsetOldest {
		t.Fatalf("Committed during the rebuild = %q, %v; want OffsetOldest", committed, err)
	}
	if _, err := b.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}
	if committed, err := b.Committed(ctx); err != nil || committed != tail {
		t.Fatalf("Committed after the rebuild = %q, %v", committed, err)
	}
}

// An instance whose source no longer holds the events another instance
// applied (retention trimmed them through the committed checkpoint) adopts
// the stored checkpoint instead of reporting an older one.
func TestProjectionAdoptsACheckpointAdvancedElsewhere(t *testing.T) {
	ctx := t.Context()
	source := eventbus.NewMemoryStore()
	db := newTrimStore(t)
	appendGroups(t, source, 3)
	a, err := db.OpenProjection(ctx, (&counterProjection{}).config("groups", source, 0))
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.OpenProjection(ctx, (&counterProjection{}).config("groups", source, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}
	committed, err := b.Committed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Trim(ctx, eventbus.RetentionPolicy{Through: committed}); err != nil {
		t.Fatal(err)
	}
	if checkpoint, err := a.CatchUp(ctx); err != nil || checkpoint != committed || a.Checkpoint() != committed {
		t.Fatalf("CatchUp = %q, %v; want the adopted %q", checkpoint, err, committed)
	}
}

func TestProjectionCommittedLoadFailure(t *testing.T) {
	store := newTrimStore(t)
	p, err := store.OpenProjection(t.Context(), (&counterProjection{}).config("groups", store, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DELETE FROM ebu_projections`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Committed(t.Context()); err == nil || !strings.Contains(err.Error(), "load checkpoint") {
		t.Fatalf("Committed = %v", err)
	}
	if _, err := p.CatchUp(t.Context()); err == nil || !strings.Contains(err.Error(), "load checkpoint") {
		t.Fatalf("CatchUp = %v", err)
	}
}

// Review-bot finding: with the same store as source and a pool capped at one
// connection, the rebuild guard must not need a second connection.
func TestProjectionRebuildWithASingleConnection(t *testing.T) {
	for _, trimmed := range []bool{false, true} {
		store := newTrimStore(t)
		offsets := appendGroups(t, store, 3)
		if trimmed {
			if _, err := store.Trim(t.Context(), eventbus.RetentionPolicy{Through: offsets[0]}); err != nil {
				t.Fatal(err)
			}
		}
		store.db.SetMaxOpenConns(1)
		cfg := (&counterProjection{}).config("groups", store, 0)
		if _, err := store.OpenProjection(t.Context(), cfg); err != nil {
			t.Fatal(err)
		}
		cfg.Version = 2
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := store.OpenProjection(ctx, cfg)
		cancel()
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			t.Fatalf("trimmed=%v: the rebuild guard deadlocked on a single connection", trimmed)
		case trimmed && !errors.Is(err, ErrProjectionRebuild):
			t.Fatalf("rebuild over a trimmed source = %v, want ErrProjectionRebuild", err)
		case !trimmed && err != nil:
			t.Fatalf("rebuild = %v", err)
		}
	}
}

func TestProjectionMissingResetIsARebuildError(t *testing.T) {
	store := newTrimStore(t)
	cfg := (&counterProjection{}).config("groups", store, 0)
	if _, err := store.OpenProjection(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Version, cfg.Reset = 2, nil
	if _, err := store.OpenProjection(t.Context(), cfg); !errors.Is(err, ErrProjectionRebuild) || !strings.Contains(err.Error(), "requires Reset") {
		t.Fatalf("OpenProjection = %v", err)
	}
}

// The rebuild guard reads another SQLite store's boundary through that
// store's own pool.
func TestProjectionRebuildOverAnotherSQLiteStore(t *testing.T) {
	for _, trimmed := range []bool{false, true} {
		source := newTrimStore(t)
		offsets := appendGroups(t, source, 3)
		if trimmed {
			if _, err := source.Trim(t.Context(), eventbus.RetentionPolicy{Through: offsets[0]}); err != nil {
				t.Fatal(err)
			}
		}
		db := newTrimStore(t)
		cfg := (&counterProjection{}).config("groups", source, 0)
		if _, err := db.OpenProjection(t.Context(), cfg); err != nil {
			t.Fatal(err)
		}
		cfg.Version = 2
		_, err := db.OpenProjection(t.Context(), cfg)
		if trimmed && (!errors.Is(err, ErrProjectionRebuild) || !strings.Contains(err.Error(), string(offsets[0]))) {
			t.Fatalf("rebuild over a trimmed SQLite source = %v", err)
		}
		if !trimmed && err != nil {
			t.Fatalf("rebuild over an untrimmed SQLite source = %v", err)
		}
	}
}
