package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type followEvent struct {
	Value int
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// startFollower runs bus.Follow on a goroutine and returns a stop function
// that cancels it and waits for it to return.
func startFollower(t *testing.T, bus *EventBus, opts ...FollowOption) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bus.Follow(ctx, opts...) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Follow returned %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Follow did not return after cancel")
		}
	}
}

// fastPoll keeps follower tests snappy.
var fastPoll = FollowPollInterval(2 * time.Millisecond)

func TestFollowCrossBusDelivery(t *testing.T) {
	store := NewMemoryStore()
	publisher := New(WithStore(store))
	consumer := New(WithStore(store))

	var got []int
	var mu sync.Mutex
	Subscribe(consumer, func(e followEvent) {
		mu.Lock()
		got = append(got, e.Value)
		mu.Unlock()
	})

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll)
	defer stop()

	for i := 1; i <= 3; i++ {
		Publish(publisher, followEvent{Value: i})
	}

	waitFor(t, 5*time.Second, "cross-bus delivery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	for i, v := range got {
		if v != i+1 {
			t.Errorf("event %d: got %d, want %d (stream order must be preserved)", i, v, i+1)
		}
	}
}

func TestFollowSkipsOwnEventsByDefault(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	var count atomic.Int32
	Subscribe(bus, func(e followEvent) { count.Add(1) })

	stop := startFollower(t, bus, FollowFrom(OffsetOldest), fastPoll)
	defer stop()

	Publish(bus, followEvent{Value: 1}) // delivered locally at publish time

	// Give the follower ample time to (incorrectly) echo the event back.
	time.Sleep(50 * time.Millisecond)
	if n := count.Load(); n != 1 {
		t.Errorf("expected exactly 1 delivery (live only), got %d", n)
	}
}

func TestFollowIncludeOwnDeliversTwice(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	var count atomic.Int32
	Subscribe(bus, func(e followEvent) { count.Add(1) })

	stop := startFollower(t, bus, FollowFrom(OffsetOldest), fastPoll, FollowIncludeOwn())
	defer stop()

	Publish(bus, followEvent{Value: 1})

	waitFor(t, 5*time.Second, "own-event echo", func() bool { return count.Load() == 2 })
}

func TestWithLogDeliveryPublishesOnlyThroughFollower(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store), WithLogDelivery())

	var got []int
	var mu sync.Mutex
	SubscribeContext(bus, func(ctx context.Context, e followEvent) {
		if _, ok := OffsetFromContext(ctx); !ok {
			t.Error("log-delivered event must carry its offset")
		}
		if _, ok := EventIDFromContext(ctx); !ok {
			t.Error("log-delivered event must carry its ID")
		}
		mu.Lock()
		got = append(got, e.Value)
		mu.Unlock()
	})

	// Without a follower, publishes append to the log but deliver nothing.
	Publish(bus, followEvent{Value: 1})
	Publish(bus, followEvent{Value: 2})
	mu.Lock()
	if len(got) != 0 {
		t.Fatalf("log-delivery mode must not dispatch at publish time, got %v", got)
	}
	mu.Unlock()

	stop := startFollower(t, bus, FollowFrom(OffsetOldest), fastPoll)
	defer stop()

	Publish(bus, followEvent{Value: 3})

	waitFor(t, 5*time.Second, "log delivery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	for i, v := range got {
		if v != i+1 {
			t.Errorf("position %d: got %d, want %d (log order)", i, v, i+1)
		}
	}
}

func TestWithLogDeliveryIdenticalOrderAcrossBuses(t *testing.T) {
	store := NewMemoryStore()
	busA := New(WithStore(store), WithLogDelivery())
	busB := New(WithStore(store), WithLogDelivery())

	var gotA, gotB []int
	var mu sync.Mutex
	Subscribe(busA, func(e followEvent) { mu.Lock(); gotA = append(gotA, e.Value); mu.Unlock() })
	Subscribe(busB, func(e followEvent) { mu.Lock(); gotB = append(gotB, e.Value); mu.Unlock() })

	stopA := startFollower(t, busA, FollowFrom(OffsetOldest), fastPoll)
	defer stopA()
	stopB := startFollower(t, busB, FollowFrom(OffsetOldest), fastPoll)
	defer stopB()

	// Interleave publishers: the log serializes them into one total order.
	var wg sync.WaitGroup
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			if v%2 == 0 {
				Publish(busA, followEvent{Value: v})
			} else {
				Publish(busB, followEvent{Value: v})
			}
		}(i)
	}
	wg.Wait()

	waitFor(t, 5*time.Second, "both buses to consume the log", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(gotA) == 10 && len(gotB) == 10
	})
	mu.Lock()
	defer mu.Unlock()
	for i := range gotA {
		if gotA[i] != gotB[i] {
			t.Fatalf("order diverged at %d: A=%v B=%v", i, gotA, gotB)
		}
	}
}

func TestFollowDefaultStartsAtTail(t *testing.T) {
	store := NewMemoryStore()
	publisher := New(WithStore(store))
	consumer := New(WithStore(store))

	Publish(publisher, followEvent{Value: 1}) // history: must NOT be delivered

	var count atomic.Int32
	Subscribe(consumer, func(e followEvent) { count.Add(1) })

	stop := startFollower(t, consumer, fastPoll)
	defer stop()

	// Wait until the follower has resolved the tail, then publish live.
	time.Sleep(20 * time.Millisecond)
	Publish(publisher, followEvent{Value: 2})

	waitFor(t, 5*time.Second, "live event", func() bool { return count.Load() >= 1 })
	time.Sleep(20 * time.Millisecond)
	if n := count.Load(); n != 1 {
		t.Errorf("expected only the live event, got %d deliveries", n)
	}
}

func TestFollowDurableResume(t *testing.T) {
	store := NewMemoryStore()
	publisher := New(WithStore(store))
	consumer := New(WithStore(store))

	var count atomic.Int32
	Subscribe(consumer, func(e followEvent) { count.Add(1) })

	Publish(publisher, followEvent{Value: 1})
	Publish(publisher, followEvent{Value: 2})

	// First run: no saved offset, durable followers consume the full log.
	stop := startFollower(t, consumer, fastPoll, FollowWithSubscriptionID("proj"))
	waitFor(t, 5*time.Second, "first run", func() bool { return count.Load() == 2 })
	stop()

	Publish(publisher, followEvent{Value: 3})
	Publish(publisher, followEvent{Value: 4})

	// Second run resumes after the saved offset: only the new events arrive.
	stop = startFollower(t, consumer, fastPoll, FollowWithSubscriptionID("proj"))
	defer stop()
	waitFor(t, 5*time.Second, "resumed run", func() bool { return count.Load() == 4 })
	time.Sleep(20 * time.Millisecond)
	if n := count.Load(); n != 4 {
		t.Errorf("expected 4 total deliveries (no replays), got %d", n)
	}
}

func TestFollowDedupDropsDuplicateIDs(t *testing.T) {
	store := NewMemoryStore()
	consumer := New(WithStore(store))

	var count atomic.Int32
	Subscribe(consumer, func(e followEvent) { count.Add(1) })

	// Two log entries with the same event ID: an at-least-once duplicate.
	dup := &Event{ID: NewEventID(), Type: "eventbus.followEvent", Data: json.RawMessage(`{"Value":1}`), Timestamp: time.Now()}
	ctx := context.Background()
	if _, err := store.Append(ctx, dup); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, dup); err != nil {
		t.Fatal(err)
	}

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll)
	defer stop()

	waitFor(t, 5*time.Second, "delivery", func() bool { return count.Load() >= 1 })
	time.Sleep(20 * time.Millisecond)
	if n := count.Load(); n != 1 {
		t.Errorf("expected the duplicate to be dropped, got %d deliveries", n)
	}
}

func TestFollowDedupWindowZeroDisables(t *testing.T) {
	store := NewMemoryStore()
	consumer := New(WithStore(store))

	var count atomic.Int32
	Subscribe(consumer, func(e followEvent) { count.Add(1) })

	dup := &Event{ID: NewEventID(), Type: "eventbus.followEvent", Data: json.RawMessage(`{"Value":1}`), Timestamp: time.Now()}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := store.Append(ctx, dup); err != nil {
			t.Fatal(err)
		}
	}

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll, FollowDedupWindow(0))
	defer stop()

	waitFor(t, 5*time.Second, "both duplicates", func() bool { return count.Load() == 2 })
}

func TestDedupRingEviction(t *testing.T) {
	r := newDedupRing(2)
	if r.observe("a") || r.observe("b") {
		t.Fatal("fresh IDs must not read as seen")
	}
	if !r.observe("a") {
		t.Fatal("a is within the window and must read as seen")
	}
	// c evicts the oldest slot; after d, a is long gone.
	r.observe("c")
	r.observe("d")
	if r.observe("a") {
		t.Error("a should have been evicted from a window of 2")
	}
}

func TestFollowSkipsPoisonAndUnknownTypes(t *testing.T) {
	store := NewMemoryStore()
	consumer := New(WithStore(store))

	var reports []error
	var mu sync.Mutex
	consumer.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
		mu.Lock()
		reports = append(reports, err)
		mu.Unlock()
	})

	var count atomic.Int32
	Subscribe(consumer, func(e followEvent) { count.Add(1) })

	ctx := context.Background()
	appendRaw := func(typeName, data string) {
		t.Helper()
		if _, err := store.Append(ctx, &Event{ID: NewEventID(), Type: typeName, Data: json.RawMessage(data), Timestamp: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	appendRaw("eventbus.followEvent", `{"Value":"poison"}`) // wrong field type: undecodable
	appendRaw("some.unsubscribed.type", `{}`)               // no local subscriber
	appendRaw("eventbus.followEvent", `{"Value":7}`)        // healthy

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll)
	defer stop()

	waitFor(t, 5*time.Second, "healthy event after poison", func() bool { return count.Load() == 1 })
	mu.Lock()
	defer mu.Unlock()
	if len(reports) != 1 {
		t.Fatalf("expected exactly one poison report, got %d: %v", len(reports), reports)
	}
}

type followV1 struct {
	Name string
}

type followV2 struct {
	FullName string
}

func TestFollowAppliesUpcasts(t *testing.T) {
	store := NewMemoryStore()
	consumer := New(WithStore(store))
	if err := RegisterUpcast(consumer, func(v1 followV1) followV2 {
		return followV2{FullName: v1.Name}
	}); err != nil {
		t.Fatal(err)
	}

	var got atomic.Value
	Subscribe(consumer, func(e followV2) { got.Store(e.FullName) })

	if _, err := store.Append(context.Background(), &Event{
		ID: NewEventID(), Type: "eventbus.followV1", Data: json.RawMessage(`{"Name":"Ada"}`), Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll)
	defer stop()

	waitFor(t, 5*time.Second, "upcasted delivery", func() bool { return got.Load() != nil })
	if v := got.Load(); v != "Ada" {
		t.Errorf("got %v, want Ada", v)
	}
}

// bareStore implements only EventStore — no SubscriptionStore — to exercise
// the FollowWithSubscriptionID validation path.
type bareStore struct {
	inner *MemoryStore
}

func (b *bareStore) Append(ctx context.Context, event *Event) (Offset, error) {
	return b.inner.Append(ctx, event)
}

func (b *bareStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	return b.inner.Read(ctx, from, limit)
}

func TestFollowValidation(t *testing.T) {
	ctx := context.Background()

	if err := New().Follow(ctx); err == nil {
		t.Error("expected error without a store")
	}

	bus := New(WithStore(NewMemoryStore()))
	if err := bus.Follow(ctx, nil); err == nil {
		t.Error("expected error for nil option")
	}
	if err := bus.Follow(ctx, FollowWithSubscriptionID("")); err == nil {
		t.Error("expected error for empty subscription ID")
	}
	if err := bus.Follow(ctx, FollowPollInterval(0)); err == nil {
		t.Error("expected error for non-positive poll interval")
	}
	if err := bus.Follow(ctx, FollowDedupWindow(-1)); err == nil {
		t.Error("expected error for negative dedup window")
	}

	bare := New(WithStore(&bareStore{inner: NewMemoryStore()}))
	if err := bare.Follow(ctx, FollowWithSubscriptionID("x")); err == nil {
		t.Error("expected error when no SubscriptionStore is available")
	}
}

func TestWithLogDeliveryWithoutStorePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected New(WithLogDelivery()) without a store to panic")
		}
	}()
	New(WithLogDelivery())
}

// flakyStore fails operations a set number of times before delegating,
// exercising the follower's retry paths.
type flakyStore struct {
	*MemoryStore
	readFails atomic.Int32
	saveFails atomic.Int32
	loadFails atomic.Int32
}

func (f *flakyStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	if f.readFails.Add(-1) >= 0 {
		return nil, from, errors.New("transient read failure")
	}
	return f.MemoryStore.Read(ctx, from, limit)
}

func (f *flakyStore) SaveOffset(ctx context.Context, id string, offset Offset) error {
	if f.saveFails.Add(-1) >= 0 {
		return errors.New("transient save failure")
	}
	return f.MemoryStore.SaveOffset(ctx, id, offset)
}

func (f *flakyStore) LoadOffset(ctx context.Context, id string) (Offset, error) {
	if f.loadFails.Add(-1) >= 0 {
		return OffsetOldest, errors.New("load failure")
	}
	return f.MemoryStore.LoadOffset(ctx, id)
}

func TestFollowRetriesReadErrors(t *testing.T) {
	store := &flakyStore{MemoryStore: NewMemoryStore()}
	store.readFails.Store(2)
	consumer := New(WithStore(store))

	var reported atomic.Int32
	consumer.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
		reported.Add(1)
	})

	var count atomic.Int32
	Subscribe(consumer, func(e followEvent) { count.Add(1) })

	if _, err := store.MemoryStore.Append(context.Background(), &Event{
		ID: NewEventID(), Type: "eventbus.followEvent", Data: json.RawMessage(`{"Value":1}`), Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll)
	defer stop()

	waitFor(t, 5*time.Second, "delivery after read failures", func() bool { return count.Load() == 1 })
	if reported.Load() == 0 {
		t.Error("read failures must be reported")
	}
}

func TestFollowReportsSaveOffsetErrors(t *testing.T) {
	store := &flakyStore{MemoryStore: NewMemoryStore()}
	store.saveFails.Store(1)
	consumer := New(WithStore(store))

	var reported atomic.Int32
	consumer.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
		reported.Add(1)
	})
	var count atomic.Int32
	Subscribe(consumer, func(e followEvent) { count.Add(1) })

	Publish(New(WithStore(store.MemoryStore)), followEvent{Value: 1})

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll, FollowWithSubscriptionID("s"))
	defer stop()

	waitFor(t, 5*time.Second, "delivery despite save failure", func() bool { return count.Load() == 1 })
	waitFor(t, 5*time.Second, "save failure report", func() bool { return reported.Load() >= 1 })
}

func TestFollowLoadOffsetFailureIsFatal(t *testing.T) {
	store := &flakyStore{MemoryStore: NewMemoryStore()}
	store.loadFails.Store(1)
	consumer := New(WithStore(store))
	if err := consumer.Follow(context.Background(), FollowWithSubscriptionID("s")); err == nil {
		t.Error("expected Follow to fail when the saved offset cannot be loaded")
	}
}

// fixtureTailer implements EventStore + EventStoreTailer: Tail yields the
// scripted events (optionally an error first), then blocks until ctx is done.
type fixtureTailer struct {
	events   []*StoredEvent
	failures atomic.Int32
}

func (f *fixtureTailer) Append(ctx context.Context, event *Event) (Offset, error) {
	return "", errors.New("read-only fixture")
}

func (f *fixtureTailer) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	return nil, from, nil
}

func (f *fixtureTailer) Tail(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
	return func(yield func(*StoredEvent, error) bool) {
		if f.failures.Add(-1) >= 0 {
			yield(nil, errors.New("transient tail failure"))
			return
		}
		for _, ev := range f.events {
			if ev.Offset <= from && ev.Offset != "" {
				continue // already consumed on a previous tail
			}
			if !yield(ev, nil) {
				return
			}
		}
		<-ctx.Done()
	}
}

func TestFollowTailerPath(t *testing.T) {
	store := &fixtureTailer{events: []*StoredEvent{
		{Offset: "00000000000000000001", ID: NewEventID(), Type: "eventbus.followEvent", Data: json.RawMessage(`{"Value":1}`)},
		{Offset: "00000000000000000002", ID: NewEventID(), Type: "eventbus.followEvent", Data: json.RawMessage(`{"Value":2}`)},
	}}
	store.failures.Store(1) // first tail fails; follower must report and re-tail

	consumer := New(WithStore(store))
	var reported atomic.Int32
	consumer.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
		reported.Add(1)
	})

	var got []int
	var mu sync.Mutex
	Subscribe(consumer, func(e followEvent) { mu.Lock(); got = append(got, e.Value); mu.Unlock() })

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll)
	defer stop()

	waitFor(t, 5*time.Second, "tailed delivery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 2
	})
	if reported.Load() == 0 {
		t.Error("tail failure must be reported")
	}
}

func TestFollowTailerEmptyOffsetEvent(t *testing.T) {
	// An event with no offset (some external tail sources) must still deliver,
	// just without an offset in the handler context.
	store := &fixtureTailer{events: []*StoredEvent{
		{Offset: "", ID: NewEventID(), Type: "eventbus.followEvent", Data: json.RawMessage(`{"Value":9}`)},
	}}
	consumer := New(WithStore(store))

	var sawOffset atomic.Bool
	var count atomic.Int32
	SubscribeContext(consumer, func(ctx context.Context, e followEvent) {
		_, ok := OffsetFromContext(ctx)
		sawOffset.Store(ok)
		count.Add(1)
	})

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll)
	defer stop()

	waitFor(t, 5*time.Second, "offsetless delivery", func() bool { return count.Load() == 1 })
	if sawOffset.Load() {
		t.Error("an offsetless event must not fabricate an offset in context")
	}
}

func TestRegisterFollowDecoderIdempotent(t *testing.T) {
	bus := New()
	Subscribe(bus, func(e followEvent) {})
	Subscribe(bus, func(e followEvent) {})
	bus.followMu.RLock()
	defer bus.followMu.RUnlock()
	if len(bus.followDecoders) != 1 {
		t.Errorf("expected 1 decoder, got %d", len(bus.followDecoders))
	}
}

// --- Cancellation branch coverage ---

func TestFollowPreCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	poll := New(WithStore(NewMemoryStore()))
	if err := poll.Follow(ctx, fastPoll); !errors.Is(err, context.Canceled) {
		t.Errorf("poll path: got %v, want context.Canceled", err)
	}

	tail := New(WithStore(&fixtureTailer{}))
	if err := tail.Follow(ctx, fastPoll); !errors.Is(err, context.Canceled) {
		t.Errorf("tail path: got %v, want context.Canceled", err)
	}
}

func TestFollowCancelDuringTailSleep(t *testing.T) {
	// An empty store parks the poll loop in its at-the-tail sleep; cancelling
	// there must end Follow promptly despite the huge interval.
	bus := New(WithStore(NewMemoryStore()))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bus.Follow(ctx, FollowPollInterval(time.Hour)) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow did not return from the tail sleep")
	}
}

func TestFollowCancelDuringErrorBackoff(t *testing.T) {
	store := &flakyStore{MemoryStore: NewMemoryStore()}
	store.readFails.Store(1 << 30) // never recovers
	bus := New(WithStore(store))

	var reported atomic.Int32
	bus.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
		reported.Add(1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bus.Follow(ctx, FollowPollInterval(time.Hour)) }()
	waitFor(t, 5*time.Second, "read error report", func() bool { return reported.Load() >= 1 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow did not return from the error backoff")
	}
}

func TestFollowCancelDuringTailBackoff(t *testing.T) {
	store := &fixtureTailer{}
	store.failures.Store(1 << 30) // every tail attempt fails
	bus := New(WithStore(store))

	var reported atomic.Int32
	bus.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
		reported.Add(1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bus.Follow(ctx, FollowPollInterval(time.Hour)) }()
	waitFor(t, 5*time.Second, "tail error report", func() bool { return reported.Load() >= 1 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow did not return from the tail backoff")
	}
}

// cancellingStore fails Read after cancelling the follower's context,
// simulating a store call that failed *because* of shutdown: the follower
// must return the cancellation, not report and retry.
type cancellingStore struct {
	inner  *MemoryStore
	cancel context.CancelFunc
}

func (c *cancellingStore) Append(ctx context.Context, event *Event) (Offset, error) {
	return c.inner.Append(ctx, event)
}

func (c *cancellingStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	c.cancel()
	return nil, from, errors.New("read aborted by shutdown")
}

func TestFollowReadErrorFromCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancellingStore{inner: NewMemoryStore(), cancel: cancel}
	bus := New(WithStore(store))

	reportedBeforeCancel := false
	bus.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
		reportedBeforeCancel = true
	})

	if err := bus.Follow(ctx, fastPoll); !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want context.Canceled", err)
	}
	if reportedBeforeCancel {
		t.Error("a read failing due to shutdown must not be reported as a store error")
	}
}
