package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type followLookupEvent struct {
	Value int
}

func TestMemoryStoreLookupOffsetDistinguishesMissingAndOldest(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	offset, found, err := store.LookupOffset(ctx, "projection")
	if err != nil {
		t.Fatalf("LookupOffset(missing) error = %v", err)
	}
	if found || offset != OffsetOldest {
		t.Fatalf("LookupOffset(missing) = (%q, %v), want (%q, false)", offset, found, OffsetOldest)
	}

	if err := store.SaveOffset(ctx, "projection", OffsetOldest); err != nil {
		t.Fatalf("SaveOffset(OffsetOldest) error = %v", err)
	}
	offset, found, err = store.LookupOffset(ctx, "projection")
	if err != nil {
		t.Fatalf("LookupOffset(stored oldest) error = %v", err)
	}
	if !found || offset != OffsetOldest {
		t.Fatalf("LookupOffset(stored oldest) = (%q, %v), want (%q, true)", offset, found, OffsetOldest)
	}

	const concrete Offset = "checkpoint-7"
	if err := store.SaveOffset(ctx, "projection", concrete); err != nil {
		t.Fatalf("SaveOffset(concrete) error = %v", err)
	}
	offset, found, err = store.LookupOffset(ctx, "projection")
	if err != nil {
		t.Fatalf("LookupOffset(concrete) error = %v", err)
	}
	if !found || offset != concrete {
		t.Fatalf("LookupOffset(concrete) = (%q, %v), want (%q, true)", offset, found, concrete)
	}
}

// followBoundaryStore is an instrumented durable tailer used to verify the
// startup transaction: lookup, optional "$" resolution, initial save, Tail.
type followBoundaryStore struct {
	lookupOffset Offset
	lookupFound  bool
	lookupErr    error

	readEvents []*StoredEvent
	readNext   Offset
	readErr    error
	saveErr    error

	mu          sync.Mutex
	operations  []string
	saved       []Offset
	readFrom    []Offset
	readLimits  []int
	tailFrom    []Offset
	tailStarted chan Offset
}

func (s *followBoundaryStore) Append(context.Context, *Event) (Offset, error) {
	return OffsetOldest, errors.New("append not supported")
}

func (s *followBoundaryStore) Read(_ context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	s.mu.Lock()
	s.operations = append(s.operations, fmt.Sprintf("read:%s:%d", from, limit))
	s.readFrom = append(s.readFrom, from)
	s.readLimits = append(s.readLimits, limit)
	events := append([]*StoredEvent(nil), s.readEvents...)
	next, err := s.readNext, s.readErr
	s.mu.Unlock()
	return events, next, err
}

func (s *followBoundaryStore) SaveOffset(_ context.Context, _ string, offset Offset) error {
	s.mu.Lock()
	s.operations = append(s.operations, "save:"+string(offset))
	s.saved = append(s.saved, offset)
	err := s.saveErr
	s.mu.Unlock()
	return err
}

func (s *followBoundaryStore) LoadOffset(ctx context.Context, id string) (Offset, error) {
	offset, _, err := s.LookupOffset(ctx, id)
	return offset, err
}

func (s *followBoundaryStore) LookupOffset(context.Context, string) (Offset, bool, error) {
	return s.lookupOffset, s.lookupFound, s.lookupErr
}

func (s *followBoundaryStore) Tail(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
	return func(func(*StoredEvent, error) bool) {
		s.mu.Lock()
		s.operations = append(s.operations, "tail:"+string(from))
		s.tailFrom = append(s.tailFrom, from)
		started := s.tailStarted
		s.mu.Unlock()
		if started != nil {
			started <- from
		}
		<-ctx.Done()
	}
}

func (s *followBoundaryStore) snapshot() (operations []string, saved, readFrom, tailFrom []Offset, readLimits []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.operations...),
		append([]Offset(nil), s.saved...),
		append([]Offset(nil), s.readFrom...),
		append([]Offset(nil), s.tailFrom...),
		append([]int(nil), s.readLimits...)
}

func TestDurableFollowPersistsEveryFirstRunBoundaryBeforeTail(t *testing.T) {
	saveFailure := errors.New("checkpoint unavailable")
	for _, tc := range []struct {
		name      string
		opts      []FollowOption
		readNext  Offset
		wantSaved Offset
		wantReads int
	}{
		{name: "default oldest", wantSaved: OffsetOldest},
		{name: "explicit oldest", opts: []FollowOption{FollowFrom(OffsetOldest)}, wantSaved: OffsetOldest},
		{name: "explicit concrete", opts: []FollowOption{FollowFrom("checkpoint-7")}, wantSaved: "checkpoint-7"},
		{name: "explicit newest", opts: []FollowOption{FollowFrom(OffsetNewest)}, readNext: "tail-9", wantSaved: "tail-9", wantReads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &followBoundaryStore{readNext: tc.readNext, saveErr: saveFailure}
			bus := New(WithStore(store))
			opts := append([]FollowOption{FollowWithSubscriptionID("first-run")}, tc.opts...)

			err := bus.Follow(context.Background(), opts...)
			if !errors.Is(err, saveFailure) {
				t.Fatalf("Follow error = %v, want initial save failure", err)
			}

			_, saved, readFrom, tailFrom, readLimits := store.snapshot()
			if len(saved) != 1 || saved[0] != tc.wantSaved {
				t.Fatalf("saved offsets = %q, want [%q]", saved, tc.wantSaved)
			}
			if len(tailFrom) != 0 {
				t.Fatalf("Tail started despite failed initial checkpoint: %q", tailFrom)
			}
			if len(readFrom) != tc.wantReads {
				t.Fatalf("Read calls = %d, want %d", len(readFrom), tc.wantReads)
			}
			if tc.wantReads == 1 && (readFrom[0] != OffsetNewest || readLimits[0] != 0) {
				t.Fatalf("initial tail resolution = Read(%q, %d), want Read(%q, 0)", readFrom[0], readLimits[0], OffsetNewest)
			}
		})
	}
}

func TestDurableFollowCancelledBeforeInitialBoundarySave(t *testing.T) {
	store := &followBoundaryStore{}
	bus := New(WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := bus.Follow(ctx, FollowWithSubscriptionID("cancelled-first-run"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Follow error = %v, want context.Canceled", err)
	}
	operations, saved, readFrom, tailFrom, _ := store.snapshot()
	if len(operations) != 0 || len(saved) != 0 || len(readFrom) != 0 || len(tailFrom) != 0 {
		t.Fatalf("cancelled startup side effects: operations=%q saved=%q read=%q tail=%q", operations, saved, readFrom, tailFrom)
	}
}

// followBoundaryPollStore forwards the durable-checkpoint capability without
// forwarding Tail, forcing Follow through its polling path.
type followBoundaryPollStore struct {
	checkpoint  *followBoundaryStore
	readStarted chan Offset
}

func (s *followBoundaryPollStore) Append(ctx context.Context, event *Event) (Offset, error) {
	return s.checkpoint.Append(ctx, event)
}

func (s *followBoundaryPollStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	s.checkpoint.mu.Lock()
	s.checkpoint.operations = append(s.checkpoint.operations, fmt.Sprintf("read:%s:%d", from, limit))
	s.checkpoint.readFrom = append(s.checkpoint.readFrom, from)
	s.checkpoint.readLimits = append(s.checkpoint.readLimits, limit)
	s.checkpoint.mu.Unlock()
	s.readStarted <- from
	<-ctx.Done()
	return nil, from, ctx.Err()
}

func (s *followBoundaryPollStore) SaveOffset(ctx context.Context, id string, offset Offset) error {
	return s.checkpoint.SaveOffset(ctx, id, offset)
}

func (s *followBoundaryPollStore) LoadOffset(ctx context.Context, id string) (Offset, error) {
	return s.checkpoint.LoadOffset(ctx, id)
}

func (s *followBoundaryPollStore) LookupOffset(ctx context.Context, id string) (Offset, bool, error) {
	return s.checkpoint.LookupOffset(ctx, id)
}

func TestDurableFollowPersistsFirstRunBoundaryBeforePolling(t *testing.T) {
	checkpoint := &followBoundaryStore{}
	store := &followBoundaryPollStore{checkpoint: checkpoint, readStarted: make(chan Offset, 1)}
	bus := New(WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- bus.Follow(ctx, FollowWithSubscriptionID("poll-first-run"), FollowFrom("checkpoint-7"), fastPoll)
	}()

	select {
	case from := <-store.readStarted:
		if from != "checkpoint-7" {
			cancel()
			t.Fatalf("first poll started from %q, want %q", from, Offset("checkpoint-7"))
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("polling did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Follow returned %v, want context.Canceled", err)
	}

	operations, saved, readFrom, tailFrom, readLimits := checkpoint.snapshot()
	wantOperations := []string{"save:checkpoint-7", "read:checkpoint-7:100"}
	if !reflect.DeepEqual(operations, wantOperations) {
		t.Fatalf("poll startup operations = %q, want %q", operations, wantOperations)
	}
	if !reflect.DeepEqual(saved, []Offset{"checkpoint-7"}) ||
		!reflect.DeepEqual(readFrom, []Offset{"checkpoint-7"}) ||
		!reflect.DeepEqual(readLimits, []int{100}) || len(tailFrom) != 0 {
		t.Fatalf("poll startup state saved=%q read=%q/%v tail=%q", saved, readFrom, readLimits, tailFrom)
	}
}

func TestDurableFollowResolvesNonemptyTailBeforeStartingTailer(t *testing.T) {
	store := &followBoundaryStore{
		readNext:    "tail-9",
		tailStarted: make(chan Offset, 1),
	}
	bus := New(WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- bus.Follow(ctx, FollowWithSubscriptionID("live"), FollowFrom(OffsetNewest), fastPoll)
	}()

	select {
	case from := <-store.tailStarted:
		if from != "tail-9" {
			cancel()
			t.Fatalf("Tail started from %q, want resolved tail %q", from, Offset("tail-9"))
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("Tail did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Follow returned %v, want context.Canceled", err)
	}

	operations, saved, readFrom, tailFrom, readLimits := store.snapshot()
	wantOperations := []string{"read:$:0", "save:tail-9", "tail:tail-9"}
	if !reflect.DeepEqual(operations, wantOperations) {
		t.Fatalf("startup operations = %q, want %q", operations, wantOperations)
	}
	if !reflect.DeepEqual(saved, []Offset{"tail-9"}) ||
		!reflect.DeepEqual(readFrom, []Offset{OffsetNewest}) ||
		!reflect.DeepEqual(readLimits, []int{0}) ||
		!reflect.DeepEqual(tailFrom, []Offset{"tail-9"}) {
		t.Fatalf("startup state saved=%q read=%q/%v tail=%q", saved, readFrom, readLimits, tailFrom)
	}
}

func TestDurableFollowRejectsInvalidInitialNewestResolution(t *testing.T) {
	for _, tc := range []struct {
		name       string
		readEvents []*StoredEvent
		readNext   Offset
		readErr    error
		wantError  string
	}{
		{
			name:      "read failure",
			readErr:   errors.New("tail lookup unavailable"),
			wantError: "tail lookup unavailable",
		},
		{
			name:       "history returned",
			readEvents: []*StoredEvent{{Offset: "tail-9"}},
			readNext:   "tail-9",
			wantError:  "returned 1 event(s), want none",
		},
		{
			name:      "symbolic tail returned",
			readNext:  OffsetNewest,
			wantError: "returned symbolic offset",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &followBoundaryStore{readEvents: tc.readEvents, readNext: tc.readNext, readErr: tc.readErr}
			bus := New(WithStore(store))
			err := bus.Follow(context.Background(), FollowWithSubscriptionID("invalid-tail"), FollowFrom(OffsetNewest))
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("Follow error = %v, want containing %q", err, tc.wantError)
			}

			_, saved, readFrom, tailFrom, readLimits := store.snapshot()
			if len(saved) != 0 || len(tailFrom) != 0 {
				t.Fatalf("invalid resolution advanced startup: saved=%q tail=%q", saved, tailFrom)
			}
			if !reflect.DeepEqual(readFrom, []Offset{OffsetNewest}) || !reflect.DeepEqual(readLimits, []int{0}) {
				t.Fatalf("resolution read = %q/%v, want [%q]/[0]", readFrom, readLimits, OffsetNewest)
			}
		})
	}
}

func TestDurableFollowHonorsFoundOldestCheckpoint(t *testing.T) {
	store := &followBoundaryStore{
		lookupOffset: OffsetOldest,
		lookupFound:  true,
		tailStarted:  make(chan Offset, 1),
	}
	bus := New(WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- bus.Follow(ctx, FollowWithSubscriptionID("existing"), FollowFrom(OffsetNewest), fastPoll)
	}()

	select {
	case from := <-store.tailStarted:
		if from != OffsetOldest {
			cancel()
			t.Fatalf("Tail started from %q, want found OffsetOldest", from)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("Tail did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Follow returned %v, want context.Canceled", err)
	}

	operations, saved, readFrom, tailFrom, _ := store.snapshot()
	if !reflect.DeepEqual(operations, []string{"tail:"}) || len(saved) != 0 || len(readFrom) != 0 || !reflect.DeepEqual(tailFrom, []Offset{OffsetOldest}) {
		t.Fatalf("found-oldest startup operations=%q saved=%q read=%q tail=%q", operations, saved, readFrom, tailFrom)
	}
}

func TestDurableFollowRejectsFoundSymbolicCheckpoint(t *testing.T) {
	store := &followBoundaryStore{lookupOffset: OffsetNewest, lookupFound: true}
	bus := New(WithStore(store))
	err := bus.Follow(context.Background(), FollowWithSubscriptionID("symbolic"), FollowFrom(OffsetOldest))
	if err == nil || !strings.Contains(err.Error(), "symbolic offset") {
		t.Fatalf("Follow error = %v, want symbolic checkpoint rejection", err)
	}
	operations, saved, readFrom, tailFrom, _ := store.snapshot()
	if len(operations) != 0 || len(saved) != 0 || len(readFrom) != 0 || len(tailFrom) != 0 {
		t.Fatalf("symbolic checkpoint advanced startup: operations=%q saved=%q read=%q tail=%q", operations, saved, readFrom, tailFrom)
	}
}

type legacyFollowCheckpointStore struct {
	loads Offset
	saves atomic.Int32
}

func (s *legacyFollowCheckpointStore) LoadOffset(context.Context, string) (Offset, error) {
	return s.loads, nil
}

func (s *legacyFollowCheckpointStore) SaveOffset(context.Context, string, Offset) error {
	s.saves.Add(1)
	return nil
}

type readCountingMemoryStore struct {
	*MemoryStore
	reads atomic.Int32
}

func (s *readCountingMemoryStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	s.reads.Add(1)
	return s.MemoryStore.Read(ctx, from, limit)
}

func TestDurableFollowRejectsAmbiguousLegacyFirstRunOffset(t *testing.T) {
	for _, from := range []Offset{"checkpoint-7", OffsetNewest} {
		t.Run(string(from), func(t *testing.T) {
			events := &readCountingMemoryStore{MemoryStore: NewMemoryStore()}
			checkpoints := &legacyFollowCheckpointStore{loads: OffsetOldest}
			bus := New(WithStore(events), WithSubscriptionStore(checkpoints))

			err := bus.Follow(context.Background(), FollowWithSubscriptionID("legacy"), FollowFrom(from))
			if err == nil || !strings.Contains(err.Error(), "SubscriptionStoreLookup") {
				t.Fatalf("FollowFrom(%q) error = %v, want legacy ambiguity rejection", from, err)
			}
			if got := events.reads.Load(); got != 0 {
				t.Fatalf("Follow read %d time(s) after legacy ambiguity", got)
			}
			if got := checkpoints.saves.Load(); got != 0 {
				t.Fatalf("Follow saved %d checkpoint(s) after legacy ambiguity", got)
			}
		})
	}
}

// restartTailStore deliberately suppresses the first Tail call until it is
// cancelled. Later calls yield the events currently after their start token.
type restartTailStore struct {
	*MemoryStore
	newestReads atomic.Int32
	tailCalls   atomic.Int32
	tailStarted chan Offset
}

func (s *restartTailStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	if from == OffsetNewest {
		s.newestReads.Add(1)
	}
	return s.MemoryStore.Read(ctx, from, limit)
}

func (s *restartTailStore) Tail(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
	return func(yield func(*StoredEvent, error) bool) {
		call := s.tailCalls.Add(1)
		s.tailStarted <- from
		if call == 1 {
			<-ctx.Done()
			return
		}

		events, _, err := s.MemoryStore.Read(ctx, from, 0)
		if err != nil {
			yield(nil, err)
			return
		}
		for _, event := range events {
			if !yield(event, nil) {
				return
			}
		}
		<-ctx.Done()
	}
}

func appendFollowLookupEvent(t *testing.T, store *MemoryStore, value int) Offset {
	t.Helper()
	data, err := json.Marshal(followLookupEvent{Value: value})
	if err != nil {
		t.Fatal(err)
	}
	offset, err := store.Append(context.Background(), &Event{
		ID:        NewEventID(),
		Type:      typeNameOf(reflect.TypeOf(followLookupEvent{})),
		Data:      data,
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("Append(%d) error = %v", value, err)
	}
	return offset
}

func TestDurableFollowRestartKeepsTailCapturedBeforeFirstYield(t *testing.T) {
	store := &restartTailStore{MemoryStore: NewMemoryStore(), tailStarted: make(chan Offset, 2)}
	initialTail := OffsetOldest // the concrete tail of an empty stream
	bus := New(WithStore(store))
	delivered := make(chan followLookupEvent, 1)
	if err := Subscribe(bus, func(event followLookupEvent) { delivered <- event }); err != nil {
		t.Fatal(err)
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- bus.Follow(firstCtx, FollowWithSubscriptionID("restart"), FollowFrom(OffsetNewest), fastPoll)
	}()
	select {
	case from := <-store.tailStarted:
		if from != initialTail {
			cancelFirst()
			t.Fatalf("first Tail started from %q, want %q", from, initialTail)
		}
	case <-time.After(5 * time.Second):
		cancelFirst()
		t.Fatal("first Tail did not start")
	}
	if offset, found, err := store.LookupOffset(context.Background(), "restart"); err != nil || !found || offset != initialTail {
		cancelFirst()
		t.Fatalf("initial checkpoint before first yield = (%q, %v, %v), want (%q, true, nil)", offset, found, err, initialTail)
	}
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Follow returned %v, want context.Canceled", err)
	}

	downtimeOffset := appendFollowLookupEvent(t, store.MemoryStore, 2)
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- bus.Follow(secondCtx, FollowWithSubscriptionID("restart"), FollowFrom(OffsetNewest), fastPoll)
	}()
	select {
	case from := <-store.tailStarted:
		if from != initialTail {
			cancelSecond()
			t.Fatalf("restarted Tail started from %q, want saved %q", from, initialTail)
		}
	case <-time.After(5 * time.Second):
		cancelSecond()
		t.Fatal("restarted Tail did not start")
	}
	select {
	case event := <-delivered:
		if event.Value != 2 {
			cancelSecond()
			t.Fatalf("restarted follower delivered value %d, want downtime value 2", event.Value)
		}
	case <-time.After(5 * time.Second):
		cancelSecond()
		t.Fatal("restarted follower skipped the event appended during downtime")
	}
	waitFor(t, 5*time.Second, "downtime event checkpoint", func() bool {
		offset, found, err := store.LookupOffset(context.Background(), "restart")
		return err == nil && found && offset == downtimeOffset
	})
	cancelSecond()
	if err := <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("second Follow returned %v, want context.Canceled", err)
	}
	if got := store.newestReads.Load(); got != 1 {
		t.Fatalf("Read(OffsetNewest) calls = %d, want 1 across first run and restart", got)
	}
}
