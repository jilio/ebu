package state

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

// compactStore is a MemoryStore (EventStore, EventStoreTrimmer,
// EventStoreOffsetComparer) with snapshot support.
type compactStore struct {
	*eventbus.MemoryStore
	*fakeSnapshotStore
}

func newCompactStore() *compactStore {
	return &compactStore{eventbus.NewMemoryStore(), newFakeSnapshotStore()}
}

// minimalSnapshotStore implements only EventStore and EventStoreSnapshotter.
type minimalSnapshotStore struct {
	eventbus.EventStore
	*fakeSnapshotStore
}

// truncatingStore removes events with EventStoreTruncator instead of Trim.
type truncatingStore struct {
	minimalSnapshotStore
	eventbus.EventStoreOffsetComparer
	through     eventbus.Offset
	truncateErr error
}

func (s *truncatingStore) TruncateBefore(_ context.Context, through eventbus.Offset) (int64, error) {
	s.through = through
	return 7, s.truncateErr
}

// failingTrimStore fails Trim or CompareOffsets.
type failingTrimStore struct {
	*compactStore
	trimErr    error
	compareErr error
}

func (s *failingTrimStore) Trim(ctx context.Context, policy eventbus.RetentionPolicy) (eventbus.TrimResult, error) {
	if s.trimErr != nil {
		return eventbus.TrimResult{}, s.trimErr
	}
	return s.compactStore.Trim(ctx, policy)
}

func (s *failingTrimStore) CompareOffsets(left, right eventbus.Offset) (int, error) {
	if s.compareErr != nil {
		return 0, s.compareErr
	}
	return s.compactStore.CompareOffsets(left, right)
}

// compactFixture publishes n users through a bus on store and materializes
// them, returning the bus, materializer, users collection and the offsets.
func compactFixture(t *testing.T, store eventbus.EventStore, n int) (*eventbus.EventBus, *Materializer, *TypedCollection[User], []eventbus.Offset) {
	t.Helper()
	ctx := context.Background()
	bus := eventbus.New(eventbus.WithStore(store))
	for i := range n {
		msg, _ := Insert(string(rune('a'+i)), User{Name: strings.Repeat("x", i+1)})
		if err := eventbus.TryPublish(bus, msg); err != nil {
			t.Fatal(err)
		}
	}
	mat := NewMaterializer()
	users := NewTypedCollection[User](NewMemoryStore[User]())
	RegisterCollection(mat, users)
	var offsets []eventbus.Offset
	if err := bus.Replay(ctx, eventbus.OffsetOldest, func(e *eventbus.StoredEvent) error {
		offsets = append(offsets, e.Offset)
		return mat.Apply(e)
	}); err != nil {
		t.Fatal(err)
	}
	return bus, mat, users, offsets
}

func TestCompactSnapshotsTrimsAndRestores(t *testing.T) {
	ctx := context.Background()
	store := newCompactStore()
	bus, mat, _, offsets := compactFixture(t, store, 4)

	result, err := mat.Compact(ctx, store, "users")
	if err != nil {
		t.Fatal(err)
	}
	if result.Snapshot != offsets[3] || result.Through != offsets[3] || result.Removed != 4 {
		t.Fatalf("Compact = %+v", result)
	}
	if events, _, _ := store.Read(ctx, eventbus.OffsetOldest, 0); len(events) != 0 {
		t.Fatalf("%d events survived compaction", len(events))
	}

	// A later change, then a cold start: snapshot + tail rebuilds the state.
	msg, _ := Update("a", User{Name: "updated"})
	if err := eventbus.TryPublish(bus, msg); err != nil {
		t.Fatal(err)
	}
	cold := NewMaterializer()
	coldUsers := NewTypedCollection[User](NewMemoryStore[User]())
	RegisterCollection(cold, coldUsers)
	from, err := cold.LoadSnapshotFrom(ctx, store, "users")
	if err != nil || from != offsets[3] {
		t.Fatalf("LoadSnapshotFrom = %q, %v", from, err)
	}
	if err := cold.Replay(ctx, bus, from); err != nil {
		t.Fatal(err)
	}
	all, _ := coldUsers.All()
	if user, _, _ := coldUsers.Get("a"); len(all) != 4 || user.Name != "updated" {
		t.Fatalf("cold start state = %v", all)
	}

	// A reader still behind the compacted prefix is told, not skipped.
	if err := bus.Replay(ctx, offsets[1], func(*eventbus.StoredEvent) error { return nil }); !errors.Is(err, eventbus.ErrOffsetTrimmed) {
		t.Fatalf("Replay behind the compacted prefix = %v", err)
	}
}

func TestCompactKeepsOffsetsOtherReadersNeed(t *testing.T) {
	ctx := context.Background()
	store := newCompactStore()
	_, mat, _, offsets := compactFixture(t, store, 5)

	result, err := mat.Compact(ctx, store, "users", offsets[4], offsets[1], offsets[3])
	if err != nil {
		t.Fatal(err)
	}
	if result.Snapshot != offsets[4] || result.Through != offsets[1] || result.Removed != 2 {
		t.Fatalf("Compact = %+v", result)
	}
	if events, _, err := store.Read(ctx, offsets[1], 0); err != nil || len(events) != 3 {
		t.Fatalf("the slowest reader lost events: %d, %v", len(events), err)
	}

	result, err = mat.Compact(ctx, store, "users", eventbus.OffsetOldest)
	if err != nil || result.Through != eventbus.OffsetOldest || result.Removed != 0 {
		t.Fatalf("Compact keeping everything = %+v, %v", result, err)
	}
	if events, _, _ := store.Read(ctx, eventbus.OffsetOldest, 0); len(events) != 3 {
		t.Fatalf("OffsetOldest keep removed events: %d left", len(events))
	}
}

func TestCompactFallsBackToTruncatorOrSnapshotOnly(t *testing.T) {
	ctx := context.Background()
	memory := eventbus.NewMemoryStore()
	truncating := &truncatingStore{
		minimalSnapshotStore:     minimalSnapshotStore{memory, newFakeSnapshotStore()},
		EventStoreOffsetComparer: memory,
	}
	_, mat, _, offsets := compactFixture(t, truncating, 3)
	result, err := mat.Compact(ctx, truncating, "users", offsets[1])
	if err != nil || result.Through != offsets[1] || result.Removed != 7 || truncating.through != offsets[1] {
		t.Fatalf("Compact via truncator = %+v, %v (truncated through %q)", result, err, truncating.through)
	}
	truncating.truncateErr = errors.New("disk full")
	if _, err := mat.Compact(ctx, truncating, "users"); err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("truncate error = %v", err)
	}

	snapshotOnly := minimalSnapshotStore{eventbus.NewMemoryStore(), newFakeSnapshotStore()}
	_, mat, _, offsets = compactFixture(t, snapshotOnly, 2)
	result, err = mat.Compact(ctx, snapshotOnly, "users")
	if err != nil || result.Snapshot != offsets[1] || result.Through != eventbus.OffsetOldest || result.Removed != 0 {
		t.Fatalf("snapshot-only Compact = %+v, %v", result, err)
	}
	if snapshotOnly.offsets["users"] != offsets[1] {
		t.Fatal("snapshot was not saved")
	}
}

func TestCompactRejections(t *testing.T) {
	ctx := context.Background()
	store := newCompactStore()
	_, mat, _, offsets := compactFixture(t, store, 2)

	if _, err := mat.Compact(ctx, eventbus.NewMemoryStore(), "users"); err == nil || !strings.Contains(err.Error(), "EventStoreSnapshotter") {
		t.Fatalf("non-snapshotter = %v", err)
	}
	if _, err := mat.Compact(ctx, store, "users", eventbus.OffsetNewest); err == nil || !strings.Contains(err.Error(), "symbolic") {
		t.Fatalf("OffsetNewest keep = %v", err)
	}
	noComparer := minimalSnapshotStore{struct{ eventbus.EventStore }{eventbus.NewMemoryStore()}, newFakeSnapshotStore()}
	if _, err := mat.Compact(ctx, noComparer, "users", offsets[0]); err == nil || !strings.Contains(err.Error(), "EventStoreOffsetComparer") {
		t.Fatalf("keep without comparer = %v", err)
	}
	if _, err := NewMaterializer().Compact(ctx, store, "users"); err == nil || !strings.Contains(err.Error(), "no events applied") {
		t.Fatalf("empty materializer = %v", err)
	}

	store.saveErr = errors.New("snapshot unavailable")
	if _, err := mat.Compact(ctx, store, "users"); err == nil || !strings.Contains(err.Error(), "snapshot unavailable") {
		t.Fatalf("save error = %v", err)
	}
	if events, _, _ := store.Read(ctx, eventbus.OffsetOldest, 0); len(events) != 2 {
		t.Fatal("events were removed although the snapshot failed")
	}
	store.saveErr = nil

	failing := &failingTrimStore{compactStore: store, compareErr: errors.New("incomparable")}
	if _, err := mat.Compact(ctx, failing, "users", offsets[0]); err == nil || !strings.Contains(err.Error(), "incomparable") {
		t.Fatalf("compare error = %v", err)
	}
	failing.compareErr, failing.trimErr = nil, errors.New("trim failed")
	if _, err := mat.Compact(ctx, failing, "users"); err == nil || !strings.Contains(err.Error(), "trim failed") {
		t.Fatalf("trim error = %v", err)
	}
	if events, _, _ := store.Read(ctx, eventbus.OffsetOldest, 0); len(events) != 2 {
		t.Fatal("events were removed although Trim failed")
	}
}

func TestCompactSnapshotFormatMatchesSaveSnapshotTo(t *testing.T) {
	ctx := context.Background()
	store := newCompactStore()
	_, mat, _, _ := compactFixture(t, store, 2)
	if err := mat.SaveSnapshotTo(ctx, store, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := mat.Compact(ctx, store, "b", eventbus.OffsetOldest); err != nil {
		t.Fatal(err)
	}
	var a, b map[string]json.RawMessage
	if json.Unmarshal(store.blobs["a"], &a) != nil || json.Unmarshal(store.blobs["b"], &b) != nil || string(a["collections"]) != string(b["collections"]) {
		t.Fatalf("Compact snapshot differs from SaveSnapshotTo: %s vs %s", store.blobs["a"], store.blobs["b"])
	}
}

// blockingSaveStore holds SaveSnapshot until released, so a test can act
// between Compact's capture and its trim.
type blockingSaveStore struct {
	*compactStore
	entered chan struct{}
	release chan struct{}
}

func (s *blockingSaveStore) SaveSnapshot(ctx context.Context, id string, at eventbus.Offset, blob json.RawMessage) error {
	s.entered <- struct{}{}
	<-s.release
	return s.compactStore.SaveSnapshot(ctx, id, at, blob)
}

// Codex review finding: LoadSnapshotFrom restoring an older snapshot between
// Compact's capture and its trim would leave the materializer behind a log
// prefix that is then removed. The two are serialized.
func TestCompactIsSerializedWithLoadSnapshotFrom(t *testing.T) {
	ctx := context.Background()
	base := newCompactStore()
	_, mat, _, offsets := compactFixture(t, base, 4)
	// An older snapshot that a concurrent restore could pick up.
	if err := base.fakeSnapshotStore.SaveSnapshot(ctx, "users", offsets[1], json.RawMessage(`{"version":1,"key_codec":"percent-v1","collections":{"User":{}}}`)); err != nil {
		t.Fatal(err)
	}
	store := &blockingSaveStore{compactStore: base, entered: make(chan struct{}), release: make(chan struct{})}

	compacted := make(chan error, 1)
	go func() {
		_, err := mat.Compact(ctx, store, "users")
		compacted <- err
	}()
	<-store.entered // Compact captured the state and is saving it

	loaded := make(chan eventbus.Offset, 1)
	go func() {
		offset, err := mat.LoadSnapshotFrom(ctx, store, "users")
		if err != nil {
			t.Error(err)
		}
		loaded <- offset
	}()
	select {
	case offset := <-loaded:
		t.Fatalf("LoadSnapshotFrom ran during Compact and restored %q", offset)
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	if err := <-compacted; err != nil {
		t.Fatal(err)
	}
	if offset := <-loaded; offset != offsets[3] {
		t.Fatalf("LoadSnapshotFrom = %q, want the compacted snapshot %q", offset, offsets[3])
	}
	if mat.LastOffset() != offsets[3] {
		t.Fatalf("LastOffset = %q", mat.LastOffset())
	}
}

// A failed trim reports that nothing was trimmed, next to the saved snapshot.
func TestCompactFailureReportsNothingTrimmed(t *testing.T) {
	ctx := context.Background()
	store := newCompactStore()
	_, mat, _, offsets := compactFixture(t, store, 2)
	failing := &failingTrimStore{compactStore: store, trimErr: errors.New("trim failed")}
	result, err := mat.Compact(ctx, failing, "users")
	if err == nil || result.Snapshot != offsets[1] || result.Through != eventbus.OffsetOldest || result.Removed != 0 {
		t.Fatalf("Compact = %+v, %v", result, err)
	}
	memory := eventbus.NewMemoryStore()
	truncating := &truncatingStore{
		minimalSnapshotStore:     minimalSnapshotStore{memory, newFakeSnapshotStore()},
		EventStoreOffsetComparer: memory,
		truncateErr:              errors.New("disk full"),
	}
	_, mat, _, offsets = compactFixture(t, truncating, 2)
	if result, err := mat.Compact(ctx, truncating, "users"); err == nil || result.Snapshot != offsets[1] || result.Through != eventbus.OffsetOldest {
		t.Fatalf("Compact via a failing truncator = %+v, %v", result, err)
	}
}
