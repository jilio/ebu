package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

var trimEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newTrimStore(t *testing.T) *SQLiteStore {
	t.Helper()
	store, err := New(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// appendTrimEvents appends n events one minute apart; partition(i) sets the
// "tenant" metadata value ("" leaves the key out).
func appendTrimEvents(t *testing.T, store *SQLiteStore, n int, partition func(int) string) []eventbus.Offset {
	t.Helper()
	offsets := make([]eventbus.Offset, n)
	for i := range n {
		event := &eventbus.Event{ID: fmt.Sprintf("e%d", i+1), Type: "t", Data: json.RawMessage(fmt.Sprint(i + 1)),
			Timestamp: trimEpoch.Add(time.Duration(i) * time.Minute)}
		if partition != nil {
			if p := partition(i); p != "" {
				event.Metadata = map[string]string{"tenant": p, "other": "x"}
			}
		}
		offset, err := store.Append(t.Context(), event)
		if err != nil {
			t.Fatal(err)
		}
		offsets[i] = offset
	}
	return offsets
}

func trimmedIDs(t *testing.T, store *SQLiteStore) string {
	t.Helper()
	events, _, err := store.Read(t.Context(), eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(events))
	for i, event := range events {
		ids[i] = event.ID
	}
	return strings.Join(ids, ",")
}

func TestTrimPrefixRules(t *testing.T) {
	for _, tc := range []struct {
		name    string
		policy  func([]eventbus.Offset) eventbus.RetentionPolicy
		deleted int64
		through int // 1-based ID of the newest removed event
	}{
		{"zero policy", func([]eventbus.Offset) eventbus.RetentionPolicy { return eventbus.RetentionPolicy{} }, 0, 0},
		{"through", func(o []eventbus.Offset) eventbus.RetentionPolicy { return eventbus.RetentionPolicy{Through: o[2]} }, 3, 3},
		{"through beyond the tail", func([]eventbus.Offset) eventbus.RetentionPolicy {
			return eventbus.RetentionPolicy{Through: formatOffset(1000)}
		}, 6, 6},
		{"legacy unpadded through", func([]eventbus.Offset) eventbus.RetentionPolicy { return eventbus.RetentionPolicy{Through: "2"} }, 2, 2},
		{"older than", func([]eventbus.Offset) eventbus.RetentionPolicy {
			return eventbus.RetentionPolicy{OlderThan: trimEpoch.Add(150 * time.Second)}
		}, 3, 3},
		{"older than everything", func([]eventbus.Offset) eventbus.RetentionPolicy {
			return eventbus.RetentionPolicy{OlderThan: trimEpoch.Add(time.Hour)}
		}, 6, 6},
		{"older than nothing", func([]eventbus.Offset) eventbus.RetentionPolicy {
			return eventbus.RetentionPolicy{OlderThan: trimEpoch}
		}, 0, 0},
		{"max events", func([]eventbus.Offset) eventbus.RetentionPolicy { return eventbus.RetentionPolicy{MaxEvents: 2} }, 4, 4},
		{"max events above size", func([]eventbus.Offset) eventbus.RetentionPolicy { return eventbus.RetentionPolicy{MaxEvents: 9} }, 0, 0},
		{"strictest rule wins", func(o []eventbus.Offset) eventbus.RetentionPolicy {
			return eventbus.RetentionPolicy{Through: o[0], OlderThan: trimEpoch.Add(90 * time.Second), MaxEvents: 5}
		}, 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTrimStore(t)
			offsets := appendTrimEvents(t, store, 6, nil)
			result, err := store.Trim(t.Context(), tc.policy(offsets))
			if err != nil {
				t.Fatal(err)
			}
			want := eventbus.OffsetOldest
			if tc.through > 0 {
				want = offsets[tc.through-1]
			}
			if result.Deleted != tc.deleted || result.TrimmedThrough != want {
				t.Fatalf("Trim = %+v, want deleted %d through %q", result, tc.deleted, want)
			}
			var ids []string
			for i := tc.through + 1; i <= 6; i++ {
				ids = append(ids, fmt.Sprintf("e%d", i))
			}
			if got := trimmedIDs(t, store); got != strings.Join(ids, ",") {
				t.Fatalf("retained %q, want %q", got, strings.Join(ids, ","))
			}
		})
	}
}

func TestTrimAgeStopsAtFirstNewerEvent(t *testing.T) {
	store := newTrimStore(t)
	zone := time.FixedZone("UTC+5", 5*3600)
	for i, ts := range []time.Time{{}, trimEpoch.In(zone), trimEpoch.Add(time.Hour), trimEpoch} {
		if _, err := store.Append(t.Context(), &eventbus.Event{ID: fmt.Sprintf("e%d", i+1), Type: "t", Data: json.RawMessage(`1`), Timestamp: ts}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := store.Trim(t.Context(), eventbus.RetentionPolicy{OlderThan: trimEpoch.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 2 || trimmedIDs(t, store) != "e3,e4" {
		t.Fatalf("Trim = %+v, retained %q", result, trimmedIDs(t, store))
	}
}

func TestTrimPerPartition(t *testing.T) {
	store := newTrimStore(t)
	partitions := []string{"a", "b", "a", "", "a", `we"ird.key`, "", "a"}
	offsets := appendTrimEvents(t, store, len(partitions), func(i int) string { return partitions[i] })
	result, err := store.Trim(t.Context(), eventbus.RetentionPolicy{PartitionKey: "tenant", MaxPerPartition: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 2 || result.TrimmedThrough != eventbus.OffsetOldest || trimmedIDs(t, store) != "e2,e4,e5,e6,e7,e8" {
		t.Fatalf("Trim = %+v, retained %q", result, trimmedIDs(t, store))
	}
	// Compaction semantics: readers are not told, offsets do not move.
	events, next, err := store.Read(t.Context(), offsets[0], 0)
	if err != nil || len(events) != 6 || events[0].Offset != offsets[1] || next != offsets[7] {
		t.Fatalf("Read = %d events, next %q, %v", len(events), next, err)
	}
	// Metadata survives, and the key is matched exactly, not as a JSON path.
	if result, err := store.Trim(t.Context(), eventbus.RetentionPolicy{PartitionKey: `we"ird.key`, MaxPerPartition: 1}); err != nil || result.Deleted != 5 {
		t.Fatalf("Trim by an unused key = %+v, %v", result, err)
	}
	if got := trimmedIDs(t, store); got != "e8" {
		t.Fatalf("retained %q", got)
	}
	// Prefix rules apply first; the cap counts what remains.
	store = newTrimStore(t)
	offsets = appendTrimEvents(t, store, len(partitions), func(i int) string { return partitions[i] })
	result, err = store.Trim(t.Context(), eventbus.RetentionPolicy{Through: offsets[4], PartitionKey: "tenant", MaxPerPartition: 1})
	if err != nil || result.Deleted != 5 || result.TrimmedThrough != offsets[4] || trimmedIDs(t, store) != "e6,e7,e8" {
		t.Fatalf("Trim = %+v, %v, retained %q", result, err, trimmedIDs(t, store))
	}
}

func TestTrimReadsReportTheGap(t *testing.T) {
	ctx := t.Context()
	store := newTrimStore(t)
	offsets := appendTrimEvents(t, store, 5, nil)
	result, err := store.Trim(ctx, eventbus.RetentionPolicy{Through: offsets[2]})
	if err != nil {
		t.Fatal(err)
	}

	_, next, err := store.Read(ctx, offsets[1], 0)
	var trimmed *eventbus.OffsetTrimmedError
	if !errors.As(err, &trimmed) || !errors.Is(err, eventbus.ErrOffsetTrimmed) || next != offsets[1] {
		t.Fatalf("Read behind the boundary = %v (next %q)", err, next)
	}
	if trimmed.Offset != offsets[1] || trimmed.ResumeFrom != result.TrimmedThrough {
		t.Fatalf("trimmed = %+v", trimmed)
	}
	if events, _, err := store.Read(ctx, trimmed.ResumeFrom, 0); err != nil || len(events) != 2 || events[0].ID != "e4" {
		t.Fatalf("Read from ResumeFrom = %d events, %v", len(events), err)
	}
	if events, _, err := store.Read(ctx, eventbus.OffsetOldest, 1); err != nil || len(events) != 1 {
		t.Fatalf("Read(OffsetOldest) = %d events, %v", len(events), err)
	}
	if _, tail, err := store.Read(ctx, eventbus.OffsetNewest, 0); err != nil || tail != offsets[4] {
		t.Fatalf("Read(OffsetNewest) = %q, %v", tail, err)
	}
	var streamed []string
	for event, err := range store.ReadStream(ctx, offsets[0]) {
		if !errors.Is(err, eventbus.ErrOffsetTrimmed) {
			t.Fatalf("ReadStream behind the boundary yielded %v, %v", event, err)
		}
		streamed = append(streamed, "gap")
	}
	if len(streamed) != 1 {
		t.Fatalf("ReadStream yielded %v", streamed)
	}

	// Removing everything still reports the boundary to readers behind it.
	all, err := store.Trim(ctx, eventbus.RetentionPolicy{MaxEvents: 0, OlderThan: trimEpoch.Add(time.Hour)})
	if err != nil || all.Deleted != 2 || all.TrimmedThrough != offsets[4] {
		t.Fatalf("Trim everything = %+v, %v", all, err)
	}
	if _, _, err := store.Read(ctx, offsets[3], 0); !errors.Is(err, eventbus.ErrOffsetTrimmed) {
		t.Fatalf("Read from a removed event = %v", err)
	}
	for _, err := range store.ReadStream(ctx, offsets[3]) {
		if !errors.Is(err, eventbus.ErrOffsetTrimmed) {
			t.Fatalf("empty ReadStream behind the boundary = %v", err)
		}
	}
	// The boundary never moves back and offsets are never reused.
	if again, err := store.Trim(ctx, eventbus.RetentionPolicy{Through: offsets[0]}); err != nil || again.TrimmedThrough != offsets[4] {
		t.Fatalf("Trim behind the boundary = %+v, %v", again, err)
	}
	offset, err := store.Append(ctx, &eventbus.Event{Type: "t", Data: json.RawMessage(`6`), Timestamp: time.Now()})
	if err != nil || offset != formatOffset(6) {
		t.Fatalf("Append after Trim = %q, %v", offset, err)
	}
	if events, _, err := store.Read(ctx, offsets[4], 0); err != nil || len(events) != 1 {
		t.Fatalf("Read from the boundary = %d events, %v", len(events), err)
	}
}

func TestTrimGapMidStream(t *testing.T) {
	ctx := t.Context()
	store, err := New(filepath.Join(t.TempDir(), "events.db"), WithStreamBatchSize(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	offsets := appendTrimEvents(t, store, 10, nil)

	var got []string
	var gap error
	for event, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
		if err != nil {
			gap = err
			break
		}
		got = append(got, event.ID)
		if event.ID == "e3" {
			// Between batches, another writer trims past this stream.
			if _, err := store.Trim(ctx, eventbus.RetentionPolicy{Through: offsets[5]}); err != nil {
				t.Fatal(err)
			}
		}
	}
	var trimmed *eventbus.OffsetTrimmedError
	if strings.Join(got, ",") != "e1,e2,e3" || !errors.As(gap, &trimmed) || trimmed.Offset != offsets[2] || trimmed.ResumeFrom != offsets[5] {
		t.Fatalf("stream yielded %v then %v", got, gap)
	}
}

func TestTruncateBeforeKeepsItsSilentSemantics(t *testing.T) {
	store := newTrimStore(t)
	offsets := appendTrimEvents(t, store, 4, nil)
	if _, err := store.TruncateBefore(t.Context(), offsets[2]); err != nil {
		t.Fatal(err)
	}
	if events, _, err := store.Read(t.Context(), offsets[0], 0); err != nil || len(events) != 1 {
		t.Fatalf("TruncateBefore changed reader behavior: %d events, %v", len(events), err)
	}
	if result, err := store.Trim(t.Context(), eventbus.RetentionPolicy{}); err != nil || result.TrimmedThrough != eventbus.OffsetOldest {
		t.Fatalf("TruncateBefore recorded a trim boundary: %+v, %v", result, err)
	}
}

func TestTrimKeepsSchemaReadableByEarlierReleases(t *testing.T) {
	path, offsets := readOnlyFixture(t)
	// A database that never trimmed has no retention table and reads fine.
	reader, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	if events, _, err := reader.Read(t.Context(), offsets[0], 0); err != nil || len(events) != 4 {
		t.Fatalf("read-only Read = %d events, %v", len(events), err)
	}
	if _, err := reader.Trim(t.Context(), eventbus.RetentionPolicy{MaxEvents: 1}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only Trim = %v", err)
	}

	writer, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Trim(t.Context(), eventbus.RetentionPolicy{Through: offsets[1]}); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := writer.db.QueryRow("SELECT MAX(version) FROM schema_version").Scan(&version); err != nil || version != currentSchemaVersion {
		t.Fatalf("schema version after Trim = %d, %v", version, err)
	}
	// Readers opened before or after the first Trim see the boundary.
	if _, _, err := reader.Read(t.Context(), offsets[0], 0); !errors.Is(err, eventbus.ErrOffsetTrimmed) {
		t.Fatalf("existing read-only store missed the boundary: %v", err)
	}
	_ = reader.Close()
	reopened, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, _, err := reopened.Read(t.Context(), offsets[0], 0); !errors.Is(err, eventbus.ErrOffsetTrimmed) {
		t.Fatalf("reopened read-only store missed the boundary: %v", err)
	}
}

func TestTrimErrors(t *testing.T) {
	ctx := t.Context()
	store := newTrimStore(t)
	appendTrimEvents(t, store, 3, func(int) string { return "a" })
	if _, err := store.Trim(ctx, eventbus.RetentionPolicy{MaxEvents: -1}); err == nil {
		t.Fatal("invalid policy accepted")
	}
	if _, err := store.Trim(ctx, eventbus.RetentionPolicy{Through: "not-a-number"}); err == nil || !strings.Contains(err.Error(), "invalid trim offset") {
		t.Fatalf("bad offset = %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Trim(cancelled, eventbus.RetentionPolicy{MaxEvents: 1}); err == nil || !strings.Contains(err.Error(), "begin") {
		t.Fatalf("cancelled Trim = %v", err)
	}

	// A corrupt boundary row surfaces on Trim and on reads.
	if _, err := store.Trim(ctx, eventbus.RetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("INSERT INTO ebu_retention (id, trimmed_through) VALUES (1, 'garbage')"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Trim(ctx, eventbus.RetentionPolicy{}); err == nil || !strings.Contains(err.Error(), "read boundary") {
		t.Fatalf("Trim with a corrupt boundary = %v", err)
	}
	// A read that returns the immediate successor cannot have crossed a
	// prefix trim and skips the lookup; any other read consults it.
	if events, _, err := store.Read(ctx, formatOffset(1), 0); err != nil || len(events) != 2 {
		t.Fatalf("successor read = %d events, %v", len(events), err)
	}
	if _, _, err := store.Read(ctx, formatOffset(3), 0); err == nil || !strings.Contains(err.Error(), "read trim boundary") {
		t.Fatalf("Read with a corrupt boundary = %v", err)
	}
	if _, err := store.db.Exec("DELETE FROM ebu_retention"); err != nil {
		t.Fatal(err)
	}

	// Statement failures inside the transaction roll back.
	for _, tc := range []struct {
		name, breakSQL, restoreSQL string
		policy                     eventbus.RetentionPolicy
		want                       string
	}{
		{"create", "DROP TABLE ebu_retention; CREATE INDEX ebu_retention ON events(type)", "DROP INDEX ebu_retention",
			eventbus.RetentionPolicy{}, "create retention table"},
		{"timestamps", "ALTER TABLE events RENAME COLUMN timestamp TO ts", "ALTER TABLE events RENAME COLUMN ts TO timestamp",
			eventbus.RetentionPolicy{OlderThan: trimEpoch.Add(time.Hour)}, "scan timestamps"},
		{"count boundary", "ALTER TABLE events RENAME TO events_moved; CREATE VIEW events AS SELECT 'x' AS position, * FROM events_moved",
			"DROP VIEW events; ALTER TABLE events_moved RENAME TO events", eventbus.RetentionPolicy{MaxEvents: 1}, "find count boundary"},
		{"prefix delete", "CREATE TRIGGER no_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'kept'); END", "DROP TRIGGER no_delete",
			eventbus.RetentionPolicy{MaxEvents: 1}, "delete prefix"},
		{"partition cap", "CREATE TRIGGER no_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'kept'); END", "DROP TRIGGER no_delete",
			eventbus.RetentionPolicy{PartitionKey: "tenant", MaxPerPartition: 1}, "apply partition cap"},
		{"record boundary", createRetentionTable + "; CREATE TRIGGER no_boundary BEFORE INSERT ON ebu_retention BEGIN SELECT RAISE(ABORT, 'no'); END", "DROP TRIGGER no_boundary",
			eventbus.RetentionPolicy{MaxEvents: 1}, "record boundary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.db.Exec(tc.breakSQL); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := store.db.Exec(tc.restoreSQL); err != nil {
					t.Fatal(err)
				}
			}()
			if _, err := store.Trim(ctx, tc.policy); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Trim = %v, want %q", err, tc.want)
			}
		})
	}
	if got := trimmedIDs(t, store); got != "e1,e2,e3" {
		t.Fatalf("a failed Trim removed events: %q", got)
	}
}

func TestTrimScanErrors(t *testing.T) {
	store := newTrimStore(t)
	if _, err := store.db.Exec(`INSERT INTO events (type, data, timestamp) VALUES ('t', '1', 'not a time')`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Trim(t.Context(), eventbus.RetentionPolicy{OlderThan: trimEpoch}); err == nil || !strings.Contains(err.Error(), "scan timestamp") {
		t.Fatalf("Trim over an unparsable timestamp = %v", err)
	}
	// Positions stored as text make the prefix boundary unreadable.
	store = newTrimStore(t)
	appendTrimEvents(t, store, 2, nil)
	if _, err := store.db.Exec(`ALTER TABLE events RENAME TO events_moved; CREATE VIEW events AS SELECT position + 0.5 AS position, type, data, timestamp FROM events_moved`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Trim(t.Context(), eventbus.RetentionPolicy{Through: formatOffset(2)}); err == nil || !strings.Contains(err.Error(), "find prefix boundary") {
		t.Fatalf("Trim = %v", err)
	}
}

func TestTrimConcurrentWithAppendsAndReads(t *testing.T) {
	ctx := context.Background()
	store := newTrimStore(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	run := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := fn(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	n := 0
	run(func() error {
		n++
		_, err := store.Append(ctx, &eventbus.Event{Type: "t", Data: json.RawMessage(`1`), Timestamp: time.Now(),
			Metadata: map[string]string{"tenant": fmt.Sprint(n % 4)}})
		return err
	})
	run(func() error {
		_, err := store.Trim(ctx, eventbus.RetentionPolicy{MaxEvents: 40, PartitionKey: "tenant", MaxPerPartition: 8})
		return err
	})
	from := eventbus.OffsetOldest
	run(func() error {
		events, next, err := store.Read(ctx, from, 5)
		var trimmed *eventbus.OffsetTrimmedError
		switch {
		case errors.As(err, &trimmed):
			from = trimmed.ResumeFrom
			return nil
		case err != nil:
			return err
		}
		for i := 1; i < len(events); i++ {
			if events[i].Offset <= events[i-1].Offset {
				return fmt.Errorf("offsets out of order: %q after %q", events[i].Offset, events[i-1].Offset)
			}
		}
		from = next
		return nil
	})
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestTrimLogs(t *testing.T) {
	store, err := New(":memory:", WithLogger(&testLogger{t: t}))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	appendTrimEvents(t, store, 3, nil)
	if result, err := store.Trim(t.Context(), eventbus.RetentionPolicy{MaxEvents: 1}); err != nil || result.Deleted != 2 {
		t.Fatalf("Trim on a :memory: store = %+v, %v", result, err)
	}
}

func TestTrimPartitionEdgeCases(t *testing.T) {
	store := newTrimStore(t)
	for i, md := range []map[string]string{nil, {"tenant": ""}, {"other": "x"}, {"tenant": "a"}} {
		if _, err := store.Append(t.Context(), &eventbus.Event{ID: fmt.Sprintf("e%d", i+1), Type: "t", Data: json.RawMessage(`1`), Metadata: md, Timestamp: trimEpoch}); err != nil {
			t.Fatal(err)
		}
	}
	// Metadata that is not valid JSON (never written by this store) counts as
	// having no key instead of failing the whole Trim.
	if _, err := store.db.Exec(`INSERT INTO events (type, data, timestamp, metadata) VALUES ('t', '1', ?, 'not json')`, trimEpoch); err != nil {
		t.Fatal(err)
	}
	// A missing key, an empty value and malformed metadata share one partition.
	result, err := store.Trim(t.Context(), eventbus.RetentionPolicy{PartitionKey: "tenant", MaxPerPartition: 1})
	if err != nil || result.Deleted != 3 {
		t.Fatalf("Trim = %+v, %v", result, err)
	}
	var remaining []int64
	rows, err := store.db.Query(`SELECT position FROM events ORDER BY position`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var p int64
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		remaining = append(remaining, p)
	}
	rows.Close()
	if fmt.Sprint(remaining) != "[4 5]" {
		t.Fatalf("remaining positions = %v, want the newest of each partition [4 5]", remaining)
	}
}

func TestTrimBoundaryLookupFailure(t *testing.T) {
	store := newTrimStore(t)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.trimmedThrough(cancelled); err == nil || !strings.Contains(err.Error(), "look up trim boundary") {
		t.Fatalf("trimmedThrough = %v", err)
	}
}

func TestTrimEmptyLog(t *testing.T) {
	store := newTrimStore(t)
	result, err := store.Trim(t.Context(), eventbus.RetentionPolicy{
		Through: formatOffset(5), OlderThan: trimEpoch, MaxEvents: 1, PartitionKey: "tenant", MaxPerPartition: 1,
	})
	if err != nil || result.Deleted != 0 || result.TrimmedThrough != eventbus.OffsetOldest {
		t.Fatalf("Trim on an empty log = %+v, %v", result, err)
	}
	if offset, err := store.Append(t.Context(), &eventbus.Event{Type: "t", Data: json.RawMessage(`1`), Timestamp: time.Now()}); err != nil || offset != formatOffset(1) {
		t.Fatalf("Append after an empty Trim = %q, %v", offset, err)
	}
}
