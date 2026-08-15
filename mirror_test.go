package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- fakes -----------------------------------------------------------------

// mirrorScriptStore is an EventStore whose behavior is supplied per test.
type mirrorScriptStore struct {
	appendFn func(ctx context.Context, event *Event) (Offset, error)
	readFn   func(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error)
}

func (s *mirrorScriptStore) Append(ctx context.Context, event *Event) (Offset, error) {
	return s.appendFn(ctx, event)
}

func (s *mirrorScriptStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	return s.readFn(ctx, from, limit)
}

// mirrorComparerStore adds a scripted EventStoreOffsetComparer.
type mirrorComparerStore struct {
	mirrorScriptStore
	compareFn func(left, right Offset) (int, error)
}

func (s *mirrorComparerStore) CompareOffsets(left, right Offset) (int, error) {
	return s.compareFn(left, right)
}

// mirrorTailerStore adds a scripted EventStoreTailer.
type mirrorTailerStore struct {
	mirrorScriptStore
	tailFn func(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error]
}

func (s *mirrorTailerStore) Tail(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
	return s.tailFn(ctx, from)
}

// mirrorTailerComparerStore combines both optional capabilities.
type mirrorTailerComparerStore struct {
	mirrorTailerStore
	compareFn func(left, right Offset) (int, error)
}

func (s *mirrorTailerComparerStore) CompareOffsets(left, right Offset) (int, error) {
	return s.compareFn(left, right)
}

// mirrorLegacyOffsets is a SubscriptionStore WITHOUT SubscriptionStoreLookup.
// saveErr makes SaveOffset fail after savesBeforeErr successful saves.
type mirrorLegacyOffsets struct {
	mu             sync.Mutex
	offsets        map[string]Offset
	saves          int
	savesBeforeErr int
	saveErr        error
	loadErr        error
	history        []Offset
}

func newMirrorLegacyOffsets() *mirrorLegacyOffsets {
	return &mirrorLegacyOffsets{offsets: make(map[string]Offset)}
}

func (s *mirrorLegacyOffsets) SaveOffset(_ context.Context, id string, offset Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil && s.saves >= s.savesBeforeErr {
		return s.saveErr
	}
	s.saves++
	s.offsets[id] = offset
	s.history = append(s.history, offset)
	return nil
}

func (s *mirrorLegacyOffsets) savedHistory() []Offset {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Offset(nil), s.history...)
}

func (s *mirrorLegacyOffsets) LoadOffset(_ context.Context, id string) (Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return OffsetOldest, s.loadErr
	}
	return s.offsets[id], nil
}

func (s *mirrorLegacyOffsets) saved(id string) Offset {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsets[id]
}

// mirrorLookupOffsets scripts the SubscriptionStoreLookup capability.
type mirrorLookupOffsets struct {
	mirrorLegacyOffsets
	lookupFn func(ctx context.Context, id string) (Offset, bool, error)
}

func (s *mirrorLookupOffsets) LookupOffset(ctx context.Context, id string) (Offset, bool, error) {
	return s.lookupFn(ctx, id)
}

// mirrorSwappableStore delegates to an inner MemoryStore that a test can
// replace mid-run, simulating a source log rebuilt from an older copy.
type mirrorSwappableStore struct {
	mu    sync.Mutex
	inner *MemoryStore
}

func (s *mirrorSwappableStore) store() *MemoryStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner
}

func (s *mirrorSwappableStore) swap(inner *MemoryStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inner = inner
}

func (s *mirrorSwappableStore) Append(ctx context.Context, event *Event) (Offset, error) {
	return s.store().Append(ctx, event)
}

func (s *mirrorSwappableStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	return s.store().Read(ctx, from, limit)
}

func (s *mirrorSwappableStore) CompareOffsets(left, right Offset) (int, error) {
	return s.store().CompareOffsets(left, right)
}

// --- helpers ---------------------------------------------------------------

// mirrorSeed appends a fully populated envelope to store and returns it as
// read back, offset included.
func mirrorSeed(t *testing.T, store *MemoryStore, id string) *StoredEvent {
	t.Helper()
	offset, err := store.Append(context.Background(), &Event{
		ID:        id,
		Origin:    "origin-" + id,
		Type:      "mirror.test",
		Data:      json.RawMessage(fmt.Sprintf(`{"seed":%q}`, id)),
		Metadata:  map[string]string{"trace": "t-" + id},
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
	})
	if err != nil {
		t.Fatalf("seed append: %v", err)
	}
	events, _, err := store.Read(context.Background(), OffsetOldest, 0)
	if err != nil {
		t.Fatalf("seed read back: %v", err)
	}
	for _, stored := range events {
		if stored.Offset == offset {
			return stored
		}
	}
	t.Fatalf("seeded event %q not found at offset %q", id, offset)
	return nil
}

// mirrorWaitFor polls cond until it holds or the deadline passes.
func mirrorWaitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not reached before deadline")
}

// mirrorCount returns the number of events currently in store.
func mirrorCount(t *testing.T, store *MemoryStore) int {
	t.Helper()
	events, _, err := store.Read(context.Background(), OffsetOldest, 0)
	if err != nil {
		t.Fatalf("count read: %v", err)
	}
	return len(events)
}

// runMirror starts Mirror on a goroutine and returns its result channel.
func runMirror(ctx context.Context, src, dst EventStore, id string, offsets SubscriptionStore, opts ...MirrorOption) chan error {
	done := make(chan error, 1)
	go func() {
		done <- Mirror(ctx, src, dst, id, offsets, opts...)
	}()
	return done
}

// mirrorShutdown cancels the mirror and asserts it returns context.Canceled.
func mirrorShutdown(t *testing.T, cancel context.CancelFunc, done chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Mirror returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Mirror did not return after cancellation")
	}
}

// errorCollector is a threadsafe MirrorOnError sink.
type errorCollector struct {
	mu   sync.Mutex
	errs []error
}

func (c *errorCollector) add(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

func (c *errorCollector) contains(substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, err := range c.errs {
		if strings.Contains(err.Error(), substr) {
			return true
		}
	}
	return false
}

// --- validation ------------------------------------------------------------

func TestMirrorValidation(t *testing.T) {
	src := NewMemoryStore()
	dst := NewMemoryStore()
	offsets := NewMemoryStore()

	cases := []struct {
		name string
		run  func() error
		want string
	}{
		{"nil source", func() error {
			return Mirror(context.Background(), nil, dst, "m", offsets)
		}, "source and a destination"},
		{"nil destination", func() error {
			return Mirror(context.Background(), src, nil, "m", offsets)
		}, "source and a destination"},
		{"empty subscription ID", func() error {
			return Mirror(context.Background(), src, dst, "", offsets)
		}, "subscription ID cannot be empty"},
		{"nil offsets", func() error {
			return Mirror(context.Background(), src, dst, "m", nil)
		}, "requires a SubscriptionStore"},
		{"nil option", func() error {
			return Mirror(context.Background(), src, dst, "m", offsets, nil)
		}, "option cannot be nil"},
		{"non-positive poll interval", func() error {
			return Mirror(context.Background(), src, dst, "m", offsets, MirrorPollInterval(0))
		}, "poll interval must be positive"},
		{"negative dedup window", func() error {
			return Mirror(context.Background(), src, dst, "m", offsets, MirrorDedupWindow(-1))
		}, "dedup window cannot be negative"},
		{"nil OnForward", func() error {
			return Mirror(context.Background(), src, dst, "m", offsets, MirrorOnForward(nil))
		}, "OnForward callback cannot be nil"},
		{"nil OnError", func() error {
			return Mirror(context.Background(), src, dst, "m", offsets, MirrorOnError(nil))
		}, "OnError callback cannot be nil"},
		{"rewind without comparer", func() error {
			plain := &mirrorScriptStore{}
			return Mirror(context.Background(), plain, dst, "m", offsets, MirrorResetOnSourceRewind())
		}, "requires the source store to implement EventStoreOffsetComparer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestMirrorCheckpointLoadFailures(t *testing.T) {
	src := NewMemoryStore()
	dst := NewMemoryStore()

	t.Run("lookup error", func(t *testing.T) {
		offsets := &mirrorLookupOffsets{lookupFn: func(context.Context, string) (Offset, bool, error) {
			return OffsetOldest, false, errors.New("lookup boom")
		}}
		err := Mirror(context.Background(), src, dst, "m", offsets)
		if err == nil || !strings.Contains(err.Error(), "load mirror offset") || !strings.Contains(err.Error(), "lookup boom") {
			t.Fatalf("got %v, want load mirror offset error", err)
		}
	})

	t.Run("legacy load error", func(t *testing.T) {
		offsets := newMirrorLegacyOffsets()
		offsets.loadErr = errors.New("load boom")
		err := Mirror(context.Background(), src, dst, "m", offsets)
		if err == nil || !strings.Contains(err.Error(), "load boom") {
			t.Fatalf("got %v, want load boom", err)
		}
	})

	t.Run("symbolic checkpoint", func(t *testing.T) {
		offsets := &mirrorLookupOffsets{lookupFn: func(context.Context, string) (Offset, bool, error) {
			return OffsetNewest, true, nil
		}}
		err := Mirror(context.Background(), src, dst, "m", offsets)
		if err == nil || !strings.Contains(err.Error(), "not a durable checkpoint") {
			t.Fatalf("got %v, want symbolic checkpoint error", err)
		}
	})
}

// --- happy path ------------------------------------------------------------

func TestMirrorCopiesEnvelopeVerbatim(t *testing.T) {
	src := NewMemoryStore()
	dst := NewMemoryStore()
	offsets := NewMemoryStore()

	want := []*StoredEvent{
		mirrorSeed(t, src, "ev-1"),
		mirrorSeed(t, src, "ev-2"),
		mirrorSeed(t, src, "ev-3"),
	}

	var forwarded sync.Map
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runMirror(ctx, src, dst, "envelope", offsets,
		MirrorPollInterval(2*time.Millisecond),
		MirrorOnForward(func(stored *StoredEvent) { forwarded.Store(stored.ID, true) }),
	)

	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == len(want) })
	mirrorShutdown(t, cancel, done)

	got, _, err := dst.Read(context.Background(), OffsetOldest, 0)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	for i, stored := range got {
		w := want[i]
		if stored.ID != w.ID || stored.Origin != w.Origin || stored.Type != w.Type {
			t.Fatalf("event %d envelope mismatch: got %+v, want %+v", i, stored, w)
		}
		if string(stored.Data) != string(w.Data) {
			t.Fatalf("event %d data mismatch: got %s, want %s", i, stored.Data, w.Data)
		}
		if stored.Metadata["trace"] != w.Metadata["trace"] {
			t.Fatalf("event %d metadata mismatch: got %v, want %v", i, stored.Metadata, w.Metadata)
		}
		if !stored.Timestamp.Equal(w.Timestamp) {
			t.Fatalf("event %d timestamp mismatch: got %v, want %v", i, stored.Timestamp, w.Timestamp)
		}
		if _, ok := forwarded.Load(w.ID); !ok {
			t.Fatalf("OnForward not called for %q", w.ID)
		}
	}

	// The checkpoint is the source offset of the last forwarded event.
	saved, found, err := offsets.LookupOffset(context.Background(), "envelope")
	if err != nil || !found {
		t.Fatalf("checkpoint lookup: found=%v err=%v", found, err)
	}
	if saved != want[len(want)-1].Offset {
		t.Fatalf("checkpoint = %q, want %q", saved, want[len(want)-1].Offset)
	}
}

func TestMirrorResumesFromCheckpoint(t *testing.T) {
	src := NewMemoryStore()
	dst := NewMemoryStore()
	offsets := NewMemoryStore()

	mirrorSeed(t, src, "a")
	mirrorSeed(t, src, "b")

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "resume", offsets, MirrorPollInterval(2*time.Millisecond))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 2 })
	mirrorShutdown(t, cancel, done)

	mirrorSeed(t, src, "c")

	// A fresh Mirror call resumes from the durable checkpoint: only "c" moves.
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := runMirror(ctx2, src, dst, "resume", offsets, MirrorPollInterval(2*time.Millisecond))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 3 })
	mirrorShutdown(t, cancel2, done2)

	got, _, _ := dst.Read(context.Background(), OffsetOldest, 0)
	ids := make(map[string]int)
	for _, stored := range got {
		ids[stored.ID]++
	}
	for _, id := range []string{"a", "b", "c"} {
		if ids[id] != 1 {
			t.Fatalf("event %q mirrored %d times, want exactly once", id, ids[id])
		}
	}
}

func TestMirrorLegacyOffsetStoreResumes(t *testing.T) {
	src := NewMemoryStore()
	dst := NewMemoryStore()
	offsets := newMirrorLegacyOffsets()

	first := mirrorSeed(t, src, "legacy-1")

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "legacy", offsets, MirrorPollInterval(2*time.Millisecond))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 1 })
	mirrorShutdown(t, cancel, done)
	if offsets.saved("legacy") != first.Offset {
		t.Fatalf("legacy checkpoint = %q, want %q", offsets.saved("legacy"), first.Offset)
	}

	mirrorSeed(t, src, "legacy-2")
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := runMirror(ctx2, src, dst, "legacy", offsets, MirrorPollInterval(2*time.Millisecond))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 2 })
	mirrorShutdown(t, cancel2, done2)

	got, _, _ := dst.Read(context.Background(), OffsetOldest, 0)
	if got[0].ID != "legacy-1" || got[1].ID != "legacy-2" || len(got) != 2 {
		t.Fatalf("unexpected destination contents: %+v", got)
	}
}

// --- failure handling ------------------------------------------------------

func TestMirrorRetriesFailedAppend(t *testing.T) {
	src := NewMemoryStore()
	inner := NewMemoryStore()
	offsets := NewMemoryStore()
	mirrorSeed(t, src, "retry-1")

	var appends int
	var mu sync.Mutex
	dst := &mirrorScriptStore{
		appendFn: func(ctx context.Context, event *Event) (Offset, error) {
			mu.Lock()
			appends++
			n := appends
			mu.Unlock()
			if n == 1 {
				return OffsetOldest, errors.New("append boom")
			}
			return inner.Append(ctx, event)
		},
		readFn: inner.Read,
	}

	collector := &errorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "retry", offsets,
		MirrorPollInterval(2*time.Millisecond),
		MirrorOnError(collector.add),
	)
	mirrorWaitFor(t, func() bool { return mirrorCount(t, inner) == 1 })
	mirrorShutdown(t, cancel, done)

	if !collector.contains("append event at source offset") {
		t.Fatal("append failure was not reported")
	}
	got, _, _ := inner.Read(context.Background(), OffsetOldest, 0)
	if len(got) != 1 || got[0].ID != "retry-1" {
		t.Fatalf("destination = %+v, want exactly one retry-1", got)
	}
}

func TestMirrorRetriesFailedRead(t *testing.T) {
	inner := NewMemoryStore()
	dst := NewMemoryStore()
	offsets := NewMemoryStore()
	mirrorSeed(t, inner, "read-1")

	var reads int
	var mu sync.Mutex
	src := &mirrorScriptStore{
		appendFn: inner.Append,
		readFn: func(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
			mu.Lock()
			reads++
			n := reads
			mu.Unlock()
			if n == 1 {
				return nil, OffsetOldest, errors.New("read boom")
			}
			return inner.Read(ctx, from, limit)
		},
	}

	collector := &errorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "read-retry", offsets,
		MirrorPollInterval(2*time.Millisecond),
		MirrorOnError(collector.add),
	)
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 1 })
	mirrorShutdown(t, cancel, done)

	if !collector.contains("read after offset") {
		t.Fatal("read failure was not reported")
	}
}

func TestMirrorReportsFailedOffsetSave(t *testing.T) {
	src := NewMemoryStore()
	dst := NewMemoryStore()
	offsets := newMirrorLegacyOffsets()
	offsets.saveErr = errors.New("save boom")
	offsets.savesBeforeErr = 1 // the first save proves the store, later ones fail
	mirrorSeed(t, src, "save-1")
	mirrorSeed(t, src, "save-2")

	collector := &errorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "save", offsets,
		MirrorPollInterval(2*time.Millisecond),
		MirrorOnError(collector.add),
	)
	// Events still flow: the checkpoint is redundant with the events.
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 2 })
	mirrorWaitFor(t, func() bool { return collector.contains("save offset") })
	mirrorShutdown(t, cancel, done)
}

// TestMirrorFirstCheckpointSaveFailureIsFatalOnEmptyChunk covers the fatal
// path through the empty-chunk resume-token advance.
func TestMirrorFirstCheckpointSaveFailureIsFatalOnEmptyChunk(t *testing.T) {
	src := &mirrorScriptStore{
		readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetOldest {
				return nil, "05", nil // empty chunk: the resume token advances
			}
			return nil, from, nil
		},
	}
	offsets := newMirrorLegacyOffsets()
	offsets.saveErr = errors.New("incompatible token")

	done := runMirror(context.Background(), src, NewMemoryStore(), "fatal-chunk", offsets, MirrorPollInterval(2*time.Millisecond))
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "initial checkpoint save failed") {
			t.Fatalf("got %v, want fatal initial checkpoint error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Mirror did not fail fast on the first checkpoint save")
	}
}

// TestMirrorTailCancelDuringInPlaceRetry covers cancellation while the tail
// path is backing off between in-place append retries.
func TestMirrorTailCancelDuringInPlaceRetry(t *testing.T) {
	e1 := &StoredEvent{Offset: "01", ID: "t-1", Type: "t", Data: json.RawMessage(`1`)}
	src := mirrorScriptedTailer(nil, []any{e1})
	collector := &errorCollector{}
	dst := &mirrorScriptStore{
		appendFn: func(context.Context, *Event) (Offset, error) {
			return OffsetOldest, errors.New("append boom")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "tail-retry-cancel", NewMemoryStore(),
		MirrorPollInterval(time.Hour), MirrorOnError(collector.add))
	mirrorWaitFor(t, func() bool { return collector.contains("append boom") })
	mirrorShutdown(t, cancel, done)
}

// TestMirrorRewindResetSaveFailureIsReported covers the reset path whose
// direct SaveOffset fails: the reset is reported, and so is the failed save.
func TestMirrorRewindResetSaveFailureIsReported(t *testing.T) {
	offsets := newMirrorLegacyOffsets()
	if err := offsets.SaveOffset(context.Background(), "reset-save", "40"); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	offsets.saveErr = errors.New("save boom")

	src := mirrorRewindFixture(func(int) ([]*StoredEvent, Offset, error) {
		return nil, "30", nil // tail permanently behind the checkpoint
	})

	collector := &errorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, NewMemoryStore(), "reset-save", offsets,
		MirrorPollInterval(2*time.Millisecond),
		MirrorResetOnSourceRewind(),
		MirrorOnError(collector.add))
	mirrorWaitFor(t, func() bool {
		return collector.contains("source log was rebuilt") && collector.contains("save offset")
	})
	mirrorShutdown(t, cancel, done)
}

// TestMirrorFirstCheckpointSaveFailureIsFatalOnTail is the tail-path variant
// of the misconfiguration fail-fast.
func TestMirrorFirstCheckpointSaveFailureIsFatalOnTail(t *testing.T) {
	e1 := &StoredEvent{Offset: "01", ID: "t-1", Type: "t", Data: json.RawMessage(`1`)}
	src := mirrorScriptedTailer(nil, []any{e1})
	offsets := newMirrorLegacyOffsets()
	offsets.saveErr = errors.New("incompatible token")

	done := runMirror(context.Background(), src, NewMemoryStore(), "fatal-tail", offsets, MirrorPollInterval(2*time.Millisecond))
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "initial checkpoint save failed") {
			t.Fatalf("got %v, want fatal initial checkpoint error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Mirror did not fail fast on the first checkpoint save")
	}
}

// TestMirrorFirstCheckpointSaveFailureIsFatal pins the misconfiguration
// fail-fast: a checkpoint store that cannot store the source's offset tokens
// fails the very first save, and Mirror must end with an error instead of
// running healthily while every restart would re-copy the whole source.
func TestMirrorFirstCheckpointSaveFailureIsFatal(t *testing.T) {
	src := NewMemoryStore()
	dst := NewMemoryStore()
	offsets := newMirrorLegacyOffsets()
	offsets.saveErr = errors.New("incompatible token")
	mirrorSeed(t, src, "fatal-1")

	done := runMirror(context.Background(), src, dst, "fatal", offsets, MirrorPollInterval(2*time.Millisecond))
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "initial checkpoint save failed") {
			t.Fatalf("got %v, want fatal initial checkpoint error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Mirror did not fail fast on the first checkpoint save")
	}
}

// --- deduplication ---------------------------------------------------------

// mirrorRereadingSource returns a source whose second batch redelivers the
// first event (same ID, as a chunk protocol would) alongside a new one.
func mirrorRereadingSource() *mirrorScriptStore {
	e1 := &StoredEvent{Offset: "01", ID: "dup", Type: "t", Data: json.RawMessage(`1`)}
	e2 := &StoredEvent{Offset: "02", ID: "fresh", Type: "t", Data: json.RawMessage(`2`)}
	return &mirrorScriptStore{
		readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			switch from {
			case OffsetOldest:
				return []*StoredEvent{e1}, "01", nil
			case "01":
				return []*StoredEvent{e1, e2}, "02", nil
			default:
				return nil, from, nil
			}
		},
	}
}

func TestMirrorDedupAbsorbsRereads(t *testing.T) {
	dst := NewMemoryStore()
	offsets := NewMemoryStore()

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, mirrorRereadingSource(), dst, "dedup", offsets, MirrorPollInterval(2*time.Millisecond))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 2 })
	// Give the mirror a few more polls to prove no duplicate arrives.
	time.Sleep(20 * time.Millisecond)
	mirrorShutdown(t, cancel, done)

	got, _, _ := dst.Read(context.Background(), OffsetOldest, 0)
	if len(got) != 2 || got[0].ID != "dup" || got[1].ID != "fresh" {
		t.Fatalf("destination = %+v, want [dup fresh]", got)
	}
}

func TestMirrorDedupWindowZeroDisables(t *testing.T) {
	dst := NewMemoryStore()
	offsets := NewMemoryStore()

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, mirrorRereadingSource(), dst, "nodedup", offsets,
		MirrorPollInterval(2*time.Millisecond), MirrorDedupWindow(0))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 3 })
	mirrorShutdown(t, cancel, done)

	got, _, _ := dst.Read(context.Background(), OffsetOldest, 0)
	ids := make(map[string]int)
	for _, stored := range got {
		ids[stored.ID]++
	}
	if ids["dup"] != 2 || ids["fresh"] != 1 {
		t.Fatalf("destination counts = %v, want dup:2 fresh:1", ids)
	}
}

// TestMirrorBulkCopyDoesNotSleepBetweenBatches pins the poll loop's progress
// rule: a full batch whose resume token advanced is progress, and the next
// read follows immediately. With a poll interval of an hour, copying multiple
// batches within the test deadline is only possible if no per-batch sleep
// happens (the regression this guards: comparing next against the per-event
// advanced offset instead of the batch's read-from offset).
func TestMirrorBulkCopyDoesNotSleepBetweenBatches(t *testing.T) {
	src := NewMemoryStore()
	dst := NewMemoryStore()
	offsets := NewMemoryStore()

	const n = 250 // > 2 poll batches (batch size 100)
	for i := 0; i < n; i++ {
		if _, err := src.Append(context.Background(), &Event{
			ID:   fmt.Sprintf("bulk-%03d", i),
			Type: "t",
			Data: json.RawMessage(`1`),
		}); err != nil {
			t.Fatalf("seed append %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "bulk", offsets, MirrorPollInterval(time.Hour))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == n })
	mirrorShutdown(t, cancel, done)
}

// TestMirrorInPlaceRetryDoesNotReforwardPrefix pins the retry strategy: a
// failed append retries that event in place instead of re-reading the batch,
// so the already forwarded prefix is never re-appended — proven with the
// dedup window disabled entirely.
func TestMirrorInPlaceRetryDoesNotReforwardPrefix(t *testing.T) {
	batch := []*StoredEvent{
		{Offset: "01", ID: "a", Type: "t", Data: json.RawMessage(`1`)},
		{Offset: "02", ID: "b", Type: "t", Data: json.RawMessage(`2`)},
		{Offset: "03", ID: "c", Type: "t", Data: json.RawMessage(`3`)},
	}
	src := &mirrorScriptStore{
		readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetOldest {
				return batch, "03", nil
			}
			return nil, from, nil
		},
	}

	inner := NewMemoryStore()
	var mu sync.Mutex
	cFailures := 0
	dst := &mirrorScriptStore{
		appendFn: func(ctx context.Context, event *Event) (Offset, error) {
			if event.ID == "c" {
				mu.Lock()
				cFailures++
				n := cFailures
				mu.Unlock()
				if n <= 2 {
					return OffsetOldest, errors.New("append boom")
				}
			}
			return inner.Append(ctx, event)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "inplace", NewMemoryStore(),
		MirrorPollInterval(2*time.Millisecond), MirrorDedupWindow(0))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, inner) == 3 })
	mirrorShutdown(t, cancel, done)

	got, _, _ := inner.Read(context.Background(), OffsetOldest, 0)
	counts := make(map[string]int)
	for _, stored := range got {
		counts[stored.ID]++
	}
	for _, id := range []string{"a", "b", "c"} {
		if counts[id] != 1 {
			t.Fatalf("event %q appended %d times, want exactly once (counts=%v)", id, counts[id], counts)
		}
	}
}

// TestMirrorCheckpointNeverMovesBackward pins the monotonicity guard: a chunk
// can interleave an exact embedded offset with a lower chunk-start token, and
// the durable checkpoint must keep the higher one.
func TestMirrorCheckpointNeverMovesBackward(t *testing.T) {
	batch := []*StoredEvent{
		{Offset: "04", ID: "m-1", Type: "t", Data: json.RawMessage(`1`)}, // embedded offset
		{Offset: "03", ID: "m-2", Type: "t", Data: json.RawMessage(`2`)}, // lower chunk-start token
	}
	src := &mirrorComparerStore{
		mirrorScriptStore: mirrorScriptStore{
			readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
				if from == OffsetOldest {
					return batch, "04", nil
				}
				return nil, from, nil
			},
		},
		compareFn: func(left, right Offset) (int, error) {
			return strings.Compare(string(left), string(right)), nil
		},
	}
	dst := NewMemoryStore()
	offsets := newMirrorLegacyOffsets()

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "monotonic", offsets, MirrorPollInterval(2*time.Millisecond))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 2 })
	mirrorShutdown(t, cancel, done)

	for _, saved := range offsets.savedHistory() {
		if saved == "03" {
			t.Fatalf("checkpoint moved backward: %v", offsets.savedHistory())
		}
	}
	if offsets.saved("monotonic") != "04" {
		t.Fatalf("final checkpoint = %q, want 04", offsets.saved("monotonic"))
	}
}

// --- store contract corners ------------------------------------------------

func TestMirrorPersistsEmptyChunkAdvance(t *testing.T) {
	dst := NewMemoryStore()
	offsets := NewMemoryStore()
	event := &StoredEvent{Offset: "06", ID: "after-gap", Type: "t", Data: json.RawMessage(`1`)}
	src := &mirrorScriptStore{
		readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			switch from {
			case OffsetOldest:
				// An empty chunk: no events, but the resume token advances.
				return nil, "05", nil
			case "05":
				return []*StoredEvent{event}, "06", nil
			default:
				return nil, from, nil
			}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "gap", offsets, MirrorPollInterval(2*time.Millisecond))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 1 })
	mirrorWaitFor(t, func() bool {
		saved, found, err := offsets.LookupOffset(context.Background(), "gap")
		return err == nil && found && saved == "06"
	})
	mirrorShutdown(t, cancel, done)
}

// --- cancellation ----------------------------------------------------------

func TestMirrorCancellation(t *testing.T) {
	t.Run("pre-cancelled poll", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := Mirror(ctx, NewMemoryStore(), NewMemoryStore(), "m", NewMemoryStore())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	})

	t.Run("pre-cancelled tail", func(t *testing.T) {
		src := &mirrorTailerStore{
			tailFn: func(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
				return func(func(*StoredEvent, error) bool) {}
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := Mirror(ctx, src, NewMemoryStore(), "m", NewMemoryStore())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	})

	t.Run("read error with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		src := &mirrorScriptStore{
			readFn: func(context.Context, Offset, int) ([]*StoredEvent, Offset, error) {
				cancel()
				return nil, OffsetOldest, errors.New("read boom")
			},
		}
		err := Mirror(ctx, src, NewMemoryStore(), "m", NewMemoryStore())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	})

	t.Run("append error with cancelled context", func(t *testing.T) {
		src := NewMemoryStore()
		mirrorSeed(t, src, "cancel-1")
		ctx, cancel := context.WithCancel(context.Background())
		var reported bool
		dst := &mirrorScriptStore{
			appendFn: func(context.Context, *Event) (Offset, error) {
				cancel()
				return OffsetOldest, errors.New("append boom")
			},
		}
		err := Mirror(ctx, src, dst, "m", NewMemoryStore(), MirrorOnError(func(error) { reported = true }))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
		if reported {
			t.Fatal("shutdown-time append failure should not be reported")
		}
	})

	t.Run("cancel during read-error backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		collector := &errorCollector{}
		src := &mirrorScriptStore{
			readFn: func(context.Context, Offset, int) ([]*StoredEvent, Offset, error) {
				return nil, OffsetOldest, errors.New("read boom")
			},
		}
		done := runMirror(ctx, src, NewMemoryStore(), "m", NewMemoryStore(),
			MirrorPollInterval(time.Hour), MirrorOnError(collector.add))
		mirrorWaitFor(t, func() bool { return collector.contains("read boom") })
		mirrorShutdown(t, cancel, done)
	})

	t.Run("cancel during append-error backoff", func(t *testing.T) {
		src := NewMemoryStore()
		mirrorSeed(t, src, "backoff-1")
		ctx, cancel := context.WithCancel(context.Background())
		collector := &errorCollector{}
		dst := &mirrorScriptStore{
			appendFn: func(context.Context, *Event) (Offset, error) {
				return OffsetOldest, errors.New("append boom")
			},
		}
		done := runMirror(ctx, src, dst, "m", NewMemoryStore(),
			MirrorPollInterval(time.Hour), MirrorOnError(collector.add))
		mirrorWaitFor(t, func() bool { return collector.contains("append boom") })
		mirrorShutdown(t, cancel, done)
	})

	t.Run("cancel at loop top after advance", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		src := &mirrorScriptStore{
			readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
				// An empty chunk that advances; cancelling here means the next
				// iteration's top-of-loop check fires, not a sleep.
				cancel()
				return nil, from + "x", nil
			},
		}
		err := Mirror(ctx, src, NewMemoryStore(), "m", NewMemoryStore())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	})
}

// --- source rewind (reconcile) ---------------------------------------------

func TestMirrorResetOnSourceRewindRecovers(t *testing.T) {
	original := NewMemoryStore()
	src := &mirrorSwappableStore{inner: original}
	dst := NewMemoryStore()
	offsets := NewMemoryStore()

	mirrorSeed(t, original, "old-1")
	mirrorSeed(t, original, "old-2")

	collector := &errorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "rewind", offsets,
		MirrorPollInterval(2*time.Millisecond),
		MirrorResetOnSourceRewind(),
		MirrorOnError(collector.add),
	)
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 2 })

	// Rebuild the source from an older, shorter copy: the mirror's checkpoint
	// is now past the source tail.
	rebuilt := NewMemoryStore()
	mirrorSeed(t, rebuilt, "rebuilt-1")
	src.swap(rebuilt)

	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 3 })
	mirrorShutdown(t, cancel, done)

	if !collector.contains("source log was rebuilt") {
		t.Fatal("rewind reset was not reported")
	}
	got, _, _ := dst.Read(context.Background(), OffsetOldest, 0)
	if got[len(got)-1].ID != "rebuilt-1" {
		t.Fatalf("destination tail = %+v, want rebuilt-1", got[len(got)-1])
	}
}

func TestMirrorRewindStartupReset(t *testing.T) {
	src := NewMemoryStore()
	dst := NewMemoryStore()
	offsets := NewMemoryStore()
	mirrorSeed(t, src, "s-1")

	// A checkpoint far past the tail, as left behind by a source restore.
	if err := offsets.SaveOffset(context.Background(), "startup", "00000000000000000099"); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	collector := &errorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "startup", offsets,
		MirrorPollInterval(2*time.Millisecond),
		MirrorResetOnSourceRewind(),
		MirrorOnError(collector.add),
	)
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 1 })
	mirrorShutdown(t, cancel, done)

	if !collector.contains("source log was rebuilt") {
		t.Fatal("startup reset was not reported")
	}
}

// TestMirrorRewindRecoversFromErroringSource pins the read-error reconcile: a
// rebuilt source may reject the stale checkpoint with an error rather than
// answer with an empty read, and the rewind check must run on that path too.
func TestMirrorRewindRecoversFromErroringSource(t *testing.T) {
	dst := NewMemoryStore()
	offsets := NewMemoryStore()
	stale := Offset("05")
	if err := offsets.SaveOffset(context.Background(), "err-rewind", stale); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	event := &StoredEvent{Offset: "01", ID: "reborn-1", Type: "t", Data: json.RawMessage(`1`)}
	var mu sync.Mutex
	tailCalls := 0
	src := &mirrorComparerStore{
		mirrorScriptStore: mirrorScriptStore{
			readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
				switch from {
				case OffsetNewest:
					mu.Lock()
					tailCalls++
					n := tailCalls
					mu.Unlock()
					if n == 1 {
						return nil, "07", nil // startup: healthy, checkpoint <= tail
					}
					return nil, "02", nil // rebuilt tail, behind the checkpoint
				case stale:
					// The rebuild happened right after startup: the stale
					// checkpoint now errors instead of reading as idle, so
					// only the read-error path can trigger the reset.
					return nil, from, errors.New("invalid offset")
				case OffsetOldest:
					return []*StoredEvent{event}, "01", nil
				default:
					return nil, from, nil
				}
			},
		},
		compareFn: func(left, right Offset) (int, error) {
			return strings.Compare(string(left), string(right)), nil
		},
	}

	collector := &errorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "err-rewind", offsets,
		MirrorPollInterval(2*time.Millisecond),
		MirrorResetOnSourceRewind(),
		MirrorOnError(collector.add))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 1 })
	mirrorShutdown(t, cancel, done)

	if !collector.contains("source log was rebuilt") {
		t.Fatal("reset after erroring read was not reported")
	}
	got, _, _ := dst.Read(context.Background(), OffsetOldest, 0)
	if got[0].ID != "reborn-1" {
		t.Fatalf("destination = %+v, want reborn-1", got)
	}
}

// TestMirrorRewindSingleGlitchDoesNotReset pins the two-check confirmation:
// one transiently stale tail read (a lagging replica) must not destroy the
// checkpoint and trigger a full re-append.
func TestMirrorRewindSingleGlitchDoesNotReset(t *testing.T) {
	dst := NewMemoryStore()
	offsets := NewMemoryStore()
	if err := offsets.SaveOffset(context.Background(), "glitch", "40"); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	src := mirrorRewindFixture(func(call int) ([]*StoredEvent, Offset, error) {
		if call == 2 {
			return nil, "30", nil // one stale tail read
		}
		return nil, "50", nil // otherwise healthy: checkpoint 40 <= tail 50
	})

	collector := &errorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "glitch", offsets,
		MirrorPollInterval(2*time.Millisecond),
		MirrorResetOnSourceRewind(),
		MirrorOnError(collector.add))

	mirrorWaitFor(t, func() bool { return collector.contains("if the next check agrees") })
	// Give several more reconcile cycles a chance to (wrongly) confirm.
	time.Sleep(100 * time.Millisecond)
	mirrorShutdown(t, cancel, done)

	if collector.contains("source log was rebuilt") {
		t.Fatal("a single stale tail read must not reset the checkpoint")
	}
	saved, _, err := offsets.LookupOffset(context.Background(), "glitch")
	if err != nil || saved != "40" {
		t.Fatalf("checkpoint = (%q, %v), want unchanged 40", saved, err)
	}
}

func TestMirrorRewindStartupHealthyCheckpoint(t *testing.T) {
	src := NewMemoryStore()
	dst := NewMemoryStore()
	offsets := NewMemoryStore()
	stored := mirrorSeed(t, src, "healthy-1")

	// Checkpoint equals the tail: nothing to forward, nothing to reset.
	if err := offsets.SaveOffset(context.Background(), "healthy", stored.Offset); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	collector := &errorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "healthy", offsets,
		MirrorPollInterval(2*time.Millisecond),
		MirrorResetOnSourceRewind(),
		MirrorOnError(collector.add),
	)
	time.Sleep(20 * time.Millisecond)
	mirrorShutdown(t, cancel, done)

	if mirrorCount(t, dst) != 0 {
		t.Fatal("healthy checkpoint should not re-forward anything")
	}
	if collector.contains("rebuilt") {
		t.Fatal("healthy checkpoint should not reset")
	}
}

// mirrorRewindFixture builds a comparer source whose tail resolution is
// scripted per call number (1-based). Reads from any concrete offset return
// an idle empty batch.
func mirrorRewindFixture(tailFn func(call int) ([]*StoredEvent, Offset, error)) *mirrorComparerStore {
	var mu sync.Mutex
	calls := 0
	return &mirrorComparerStore{
		mirrorScriptStore: mirrorScriptStore{
			readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
				if from == OffsetNewest {
					mu.Lock()
					calls++
					n := calls
					mu.Unlock()
					return tailFn(n)
				}
				return nil, from, nil
			},
		},
		compareFn: func(left, right Offset) (int, error) {
			return strings.Compare(string(left), string(right)), nil
		},
	}
}

func TestMirrorReconcileFailures(t *testing.T) {
	dst := NewMemoryStore()

	checkpointed := func() *MemoryStore {
		offsets := NewMemoryStore()
		if err := offsets.SaveOffset(context.Background(), "m", "05"); err != nil {
			t.Fatalf("seed checkpoint: %v", err)
		}
		return offsets
	}

	// Startup reconcile failures are best-effort like every later one: they
	// are reported, and the mirror keeps running (the same error one loop
	// iteration later would be retried forever).
	reportedNotFatal := func(t *testing.T, src EventStore, want string) {
		t.Helper()
		collector := &errorCollector{}
		ctx, cancel := context.WithCancel(context.Background())
		done := runMirror(ctx, src, dst, "m", checkpointed(),
			MirrorPollInterval(2*time.Millisecond),
			MirrorResetOnSourceRewind(),
			MirrorOnError(collector.add))
		mirrorWaitFor(t, func() bool { return collector.contains(want) })
		mirrorShutdown(t, cancel, done)
	}

	t.Run("startup tail read error is reported, not fatal", func(t *testing.T) {
		reportedNotFatal(t, mirrorRewindFixture(func(int) ([]*StoredEvent, Offset, error) {
			return nil, OffsetOldest, errors.New("tail boom")
		}), "resolve source tail")
	})

	t.Run("tail returning events is reported", func(t *testing.T) {
		reportedNotFatal(t, mirrorRewindFixture(func(int) ([]*StoredEvent, Offset, error) {
			return []*StoredEvent{{Offset: "01"}}, "01", nil
		}), "returned 1 event(s)")
	})

	t.Run("tail staying symbolic is reported", func(t *testing.T) {
		reportedNotFatal(t, mirrorRewindFixture(func(int) ([]*StoredEvent, Offset, error) {
			return nil, OffsetNewest, nil
		}), "symbolic offset")
	})

	t.Run("compare error is reported", func(t *testing.T) {
		src := mirrorRewindFixture(func(int) ([]*StoredEvent, Offset, error) {
			return nil, "01", nil
		})
		src.compareFn = func(Offset, Offset) (int, error) {
			return 0, errors.New("compare boom")
		}
		reportedNotFatal(t, src, "compare checkpoint")
	})

	t.Run("startup reconcile with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		src := mirrorRewindFixture(func(int) ([]*StoredEvent, Offset, error) {
			cancel()
			return nil, OffsetOldest, errors.New("tail boom")
		})
		err := Mirror(ctx, src, dst, "m", checkpointed(), MirrorResetOnSourceRewind())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	})

	t.Run("mid-run reconcile failure is reported", func(t *testing.T) {
		src := mirrorRewindFixture(func(call int) ([]*StoredEvent, Offset, error) {
			if call == 1 {
				return nil, "05", nil // startup: checkpoint == tail, healthy
			}
			return nil, OffsetOldest, errors.New("tail boom")
		})
		collector := &errorCollector{}
		ctx, cancel := context.WithCancel(context.Background())
		done := runMirror(ctx, src, dst, "m", checkpointed(),
			MirrorPollInterval(2*time.Millisecond),
			MirrorResetOnSourceRewind(),
			MirrorOnError(collector.add))
		mirrorWaitFor(t, func() bool { return collector.contains("tail boom") })
		mirrorShutdown(t, cancel, done)
	})

	t.Run("read-error reconcile with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var mu sync.Mutex
		tailCalls := 0
		src := &mirrorComparerStore{
			mirrorScriptStore: mirrorScriptStore{
				readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
					if from == OffsetNewest {
						mu.Lock()
						tailCalls++
						n := tailCalls
						mu.Unlock()
						if n == 1 {
							return nil, "05", nil // startup: healthy
						}
						cancel()
						return nil, OffsetOldest, errors.New("tail boom")
					}
					return nil, from, errors.New("read boom") // forces the read-error path
				},
			},
			compareFn: func(left, right Offset) (int, error) {
				return strings.Compare(string(left), string(right)), nil
			},
		}
		done := runMirror(ctx, src, dst, "m", checkpointed(),
			MirrorPollInterval(2*time.Millisecond),
			MirrorResetOnSourceRewind())
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Mirror did not return")
		}
	})

	t.Run("mid-run reconcile with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		src := mirrorRewindFixture(func(call int) ([]*StoredEvent, Offset, error) {
			if call == 1 {
				return nil, "05", nil
			}
			cancel()
			return nil, OffsetOldest, errors.New("tail boom")
		})
		done := runMirror(ctx, src, dst, "m", checkpointed(),
			MirrorPollInterval(2*time.Millisecond),
			MirrorResetOnSourceRewind())
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Mirror did not return")
		}
	})
}

// --- tail mode -------------------------------------------------------------

// mirrorScriptedTailer yields the given batches, one Tail call per batch, then
// blocks until ctx is cancelled on later calls.
func mirrorScriptedTailer(read func(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error), batches ...[]any) *mirrorTailerStore {
	var mu sync.Mutex
	call := 0
	s := &mirrorTailerStore{}
	s.readFn = read
	s.tailFn = func(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
		mu.Lock()
		call++
		n := call
		mu.Unlock()
		return func(yield func(*StoredEvent, error) bool) {
			if n > len(batches) {
				<-ctx.Done()
				return
			}
			for _, item := range batches[n-1] {
				var ok bool
				switch v := item.(type) {
				case *StoredEvent:
					ok = yield(v, nil)
				case error:
					ok = yield(nil, v)
				}
				if !ok {
					return
				}
			}
		}
	}
	return s
}

func TestMirrorTailForwards(t *testing.T) {
	dst := NewMemoryStore()
	offsets := NewMemoryStore()
	e1 := &StoredEvent{Offset: "01", ID: "t-1", Type: "t", Data: json.RawMessage(`1`)}
	e2 := &StoredEvent{Offset: "02", ID: "t-2", Type: "t", Data: json.RawMessage(`2`)}
	src := mirrorScriptedTailer(nil, []any{e1, e2})

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "tail", offsets, MirrorPollInterval(2*time.Millisecond))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 2 })
	mirrorWaitFor(t, func() bool {
		saved, found, err := offsets.LookupOffset(context.Background(), "tail")
		return err == nil && found && saved == "02"
	})
	mirrorShutdown(t, cancel, done)
}

func TestMirrorTailErrorRestarts(t *testing.T) {
	dst := NewMemoryStore()
	e1 := &StoredEvent{Offset: "01", ID: "t-1", Type: "t", Data: json.RawMessage(`1`)}
	src := mirrorScriptedTailer(nil,
		[]any{errors.New("tail stream boom")},
		[]any{e1},
	)

	collector := &errorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "tail-err", NewMemoryStore(),
		MirrorPollInterval(2*time.Millisecond), MirrorOnError(collector.add))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, dst) == 1 })
	mirrorShutdown(t, cancel, done)

	if !collector.contains("tail after offset") {
		t.Fatal("tail failure was not reported")
	}
}

func TestMirrorTailAppendFailureRetries(t *testing.T) {
	inner := NewMemoryStore()
	e1 := &StoredEvent{Offset: "01", ID: "t-1", Type: "t", Data: json.RawMessage(`1`)}
	src := mirrorScriptedTailer(nil, []any{e1}, []any{e1})

	var appends int
	var mu sync.Mutex
	dst := &mirrorScriptStore{
		appendFn: func(ctx context.Context, event *Event) (Offset, error) {
			mu.Lock()
			appends++
			n := appends
			mu.Unlock()
			if n == 1 {
				return OffsetOldest, errors.New("append boom")
			}
			return inner.Append(ctx, event)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runMirror(ctx, src, dst, "tail-retry", NewMemoryStore(), MirrorPollInterval(2*time.Millisecond))
	mirrorWaitFor(t, func() bool { return mirrorCount(t, inner) == 1 })
	mirrorShutdown(t, cancel, done)

	got, _, _ := inner.Read(context.Background(), OffsetOldest, 0)
	if len(got) != 1 || got[0].ID != "t-1" {
		t.Fatalf("destination = %+v, want exactly one t-1", got)
	}
}

func TestMirrorTailCancellation(t *testing.T) {
	t.Run("cancelled while iterating", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		e1 := &StoredEvent{Offset: "01", ID: "t-1", Type: "t", Data: json.RawMessage(`1`)}
		src := &mirrorTailerStore{
			tailFn: func(context.Context, Offset) iter.Seq2[*StoredEvent, error] {
				return func(yield func(*StoredEvent, error) bool) {
					if yield(e1, nil) {
						cancel() // The iterator then ends silently, as on ctx.Done.
					}
				}
			},
		}
		err := Mirror(ctx, src, NewMemoryStore(), "m", NewMemoryStore())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	})

	t.Run("append error with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		e1 := &StoredEvent{Offset: "01", ID: "t-1", Type: "t", Data: json.RawMessage(`1`)}
		src := mirrorScriptedTailer(nil, []any{e1})
		dst := &mirrorScriptStore{
			appendFn: func(context.Context, *Event) (Offset, error) {
				cancel()
				return OffsetOldest, errors.New("append boom")
			},
		}
		err := Mirror(ctx, src, dst, "m", NewMemoryStore())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	})

	t.Run("reconcile cancellation between tails", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var mu sync.Mutex
		tailCalls := 0
		src := &mirrorTailerComparerStore{
			mirrorTailerStore: mirrorTailerStore{
				mirrorScriptStore: mirrorScriptStore{
					readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
						if from == OffsetNewest {
							mu.Lock()
							started := tailCalls > 0
							mu.Unlock()
							if started {
								cancel()
								return nil, OffsetOldest, errors.New("tail boom")
							}
							return nil, "05", nil // startup: healthy
						}
						return nil, from, nil
					},
				},
				tailFn: func(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
					mu.Lock()
					tailCalls++
					mu.Unlock()
					return func(func(*StoredEvent, error) bool) {} // ends immediately
				},
			},
			compareFn: func(left, right Offset) (int, error) {
				return strings.Compare(string(left), string(right)), nil
			},
		}
		offsets := NewMemoryStore()
		if err := offsets.SaveOffset(context.Background(), "m", "05"); err != nil {
			t.Fatalf("seed checkpoint: %v", err)
		}
		err := Mirror(ctx, src, NewMemoryStore(), "m", offsets, MirrorResetOnSourceRewind())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	})
}
