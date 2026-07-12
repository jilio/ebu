package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"reflect"
	"slices"
	"strings"
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

func TestWithLogDeliveryDefaultFollowIncludesPreStartPublishes(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store), WithLogDelivery())

	delivered := make(chan followEvent, 1)
	Subscribe(bus, func(e followEvent) { delivered <- e })

	// This publish happens deterministically before Follow is even invoked.
	// In log-delivery mode there is no local fallback, so a live-only default
	// would lose it when Follow later resolved OffsetNewest.
	Publish(bus, followEvent{Value: 42})

	stop := startFollower(t, bus, fastPoll)
	defer stop()

	select {
	case event := <-delivered:
		if event.Value != 42 {
			t.Fatalf("got value %d, want 42", event.Value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("default log follower lost an event published before startup")
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

type checkpointSpy struct {
	completed   <-chan struct{}
	checkpoints chan Offset
	early       atomic.Bool
}

func (s *checkpointSpy) LoadOffset(context.Context, string) (Offset, error) {
	return OffsetOldest, nil
}

func (s *checkpointSpy) SaveOffset(_ context.Context, _ string, offset Offset) error {
	if offset != OffsetOldest {
		select {
		case <-s.completed:
		default:
			s.early.Store(true)
		}
	}
	s.checkpoints <- offset
	return nil
}

func TestDurableFollowCheckpointsAfterAsyncHandlerCompletion(t *testing.T) {
	store := NewMemoryStore()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	completed := make(chan struct{})
	checkpoints := make(chan Offset, 1)
	spy := &checkpointSpy{completed: completed, checkpoints: checkpoints}
	consumer := New(WithStore(store), WithSubscriptionStore(spy))

	Subscribe(consumer, func(followEvent) {
		close(started)
		<-release
		close(completed)
	}, Async(), Once())

	publisher := New(WithStore(store))
	Publish(publisher, followEvent{Value: 1})

	stop := startFollower(t, consumer, FollowWithSubscriptionID("async-checkpoint"), fastPoll)
	defer stop()
	defer releaseHandler()
	select {
	case offset := <-checkpoints:
		if offset != OffsetOldest {
			t.Fatalf("first-run checkpoint = %q, want OffsetOldest", offset)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first-run boundary was not checkpointed")
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("async handler did not start")
	}

	select {
	case offset := <-checkpoints:
		t.Fatalf("checkpoint %s was saved while the async handler was blocked", offset)
	case <-time.After(50 * time.Millisecond):
	}

	releaseHandler()
	select {
	case offset := <-checkpoints:
		if offset == OffsetOldest {
			t.Fatal("saved checkpoint did not advance")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint was not saved after async handler completion")
	}
	if spy.early.Load() {
		t.Fatal("SaveOffset was invoked before the async handler completed")
	}
}

func TestPublishAsyncRemainsNonBlocking(t *testing.T) {
	bus := New()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHandler()
	Subscribe(bus, func(followEvent) {
		close(started)
		<-release
	}, Async())

	published := make(chan struct{})
	go func() {
		Publish(bus, followEvent{Value: 1})
		close(published)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("async handler did not start")
	}
	select {
	case <-published:
		// Publish returned while the handler remained blocked, as required.
	case <-time.After(5 * time.Second):
		releaseHandler()
		t.Fatal("Publish waited for an async handler")
	}
	releaseHandler()
	bus.Wait()
}

func TestDurableFollowKeepsInternalWaitMarkerOutOfUserContext(t *testing.T) {
	store := NewMemoryStore()
	Publish(New(WithStore(store)), followEvent{Value: 1})

	consumer := New(WithStore(store))
	leaked := make(chan bool, 1)
	if err := SubscribeContext(consumer, func(ctx context.Context, _ followEvent) {
		_, exposed := ctx.Value(durableDispatchCtxKey{}).(bool)
		leaked <- exposed
	}); err != nil {
		t.Fatal(err)
	}

	stop := startFollower(t, consumer, FollowWithSubscriptionID("private-durable-marker"), fastPoll)
	defer stop()
	select {
	case exposed := <-leaked:
		if exposed {
			t.Fatal("durable dispatch exposed its internal wait marker to a user handler")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("durable follower did not deliver the stored event")
	}
}

func TestDurableFollowPollRetriesSyncPanicBeforeCheckpoint(t *testing.T) {
	const retryDelay = 30 * time.Millisecond
	store := NewMemoryStore()
	consumer := New(WithStore(store))

	var attempts atomic.Int32
	var successfulOnceAttempts atomic.Int32
	var firstAttemptAt, secondAttemptAt atomic.Int64
	retried := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	reports := make(chan error, 1)
	consumer.SetPersistenceErrorHandler(func(_ any, _ reflect.Type, errorValue error) {
		reports <- errorValue
	})

	Subscribe(consumer, func(followEvent) {
		successfulOnceAttempts.Add(1)
	}, Once())
	Subscribe(consumer, func(followEvent) {
		switch attempts.Add(1) {
		case 1:
			firstAttemptAt.Store(time.Now().UnixNano())
			panic("retry synchronous follow handler")
		case 2:
			secondAttemptAt.Store(time.Now().UnixNano())
			close(retried)
			<-release
		}
	}, Once())

	Publish(New(WithStore(store)), followEvent{Value: 1})
	stop := startFollower(t, consumer, FollowWithSubscriptionID("sync-panic"), FollowPollInterval(retryDelay))
	defer func() {
		releaseHandler()
		stop()
	}()

	select {
	case err := <-reports:
		if err == nil {
			t.Fatal("panic report was nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("synchronous handler panic was not reported")
	}
	select {
	case <-retried:
	case <-time.After(5 * time.Second):
		t.Fatal("failed synchronous event was not retried")
	}

	if elapsed := time.Duration(secondAttemptAt.Load() - firstAttemptAt.Load()); elapsed < retryDelay {
		t.Fatalf("panic retry hot-looped after %v; want at least %v", elapsed, retryDelay)
	}
	if got := successfulOnceAttempts.Load(); got != 2 {
		t.Fatalf("successful Once sibling ran %d time(s), want 2 across event retry", got)
	}
	if offset, err := store.LoadOffset(context.Background(), "sync-panic"); err != nil || offset != OffsetOldest {
		t.Fatalf("blocked retry checkpoint = %q, err=%v; want OffsetOldest", offset, err)
	}

	releaseHandler()
	waitFor(t, 5*time.Second, "sync retry checkpoint", func() bool {
		offset, _ := store.LoadOffset(context.Background(), "sync-panic")
		return offset != OffsetOldest
	})
	if got := HandlerCount[followEvent](consumer); got != 0 {
		t.Fatalf("successful Once retry left %d handler(s) registered", got)
	}
}

func TestDurableFollowCheckpointsSuccessfulPrefixBeforeMidBatchFailure(t *testing.T) {
	store := NewMemoryStore()
	producer := New(WithStore(store))
	Publish(producer, followEvent{Value: 1})
	Publish(producer, followEvent{Value: 2})
	stored, _, err := store.Read(context.Background(), OffsetOldest, 0)
	if err != nil || len(stored) != 2 {
		t.Fatalf("stored events = %d, err=%v", len(stored), err)
	}

	reported := make(chan struct{}, 1)
	consumer := New(
		WithStore(store),
		WithPersistenceErrorHandler(func(any, reflect.Type, error) {
			select {
			case reported <- struct{}{}:
			default:
			}
		}),
	)
	var firstCalls atomic.Int32
	Subscribe(consumer, func(event followEvent) {
		if event.Value == 1 {
			firstCalls.Add(1)
			return
		}
		panic("second event fails")
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Follow(ctx,
			FollowWithSubscriptionID("mid-batch-prefix"),
			FollowPollInterval(time.Hour))
	}()
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("mid-batch handler failure was not reported")
	}

	if offset, loadErr := store.LoadOffset(context.Background(), "mid-batch-prefix"); loadErr != nil || offset != stored[0].Offset {
		cancel()
		t.Fatalf("checkpoint after mid-batch failure = %q, err=%v; want first event %q", offset, loadErr, stored[0].Offset)
	}
	if got := firstCalls.Load(); got != 1 {
		cancel()
		t.Fatalf("successful prefix handler calls = %d, want 1", got)
	}

	cancel()
	if followErr := <-done; !errors.Is(followErr, context.Canceled) {
		t.Fatalf("Follow returned %v, want context.Canceled", followErr)
	}
}

func TestDurableFollowTailRetriesAsyncPanicBeforeCheckpoint(t *testing.T) {
	const retryDelay = 30 * time.Millisecond
	stored := &StoredEvent{
		Offset: "00000000000000000001",
		ID:     NewEventID(),
		Type:   "eventbus.followEvent",
		Data:   json.RawMessage(`{"Value":1}`),
	}
	events := &fixtureTailer{events: []*StoredEvent{stored}}
	checkpoints := NewMemoryStore()
	consumer := New(WithStore(events), WithSubscriptionStore(checkpoints))

	var attempts atomic.Int32
	var firstAttemptAt, secondAttemptAt atomic.Int64
	retried := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	reports := make(chan error, 1)
	consumer.SetPersistenceErrorHandler(func(_ any, _ reflect.Type, errorValue error) {
		reports <- errorValue
	})

	Subscribe(consumer, func(followEvent) {
		switch attempts.Add(1) {
		case 1:
			firstAttemptAt.Store(time.Now().UnixNano())
			panic("retry asynchronous follow handler")
		case 2:
			secondAttemptAt.Store(time.Now().UnixNano())
			close(retried)
			<-release
		}
	}, Async(), Once())

	stop := startFollower(t, consumer, FollowWithSubscriptionID("async-panic"), FollowPollInterval(retryDelay))
	defer func() {
		releaseHandler()
		stop()
	}()

	select {
	case err := <-reports:
		if err == nil {
			t.Fatal("panic report was nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("asynchronous handler panic was not reported")
	}
	select {
	case <-retried:
	case <-time.After(5 * time.Second):
		t.Fatal("failed asynchronous event was not re-tailed")
	}

	if elapsed := time.Duration(secondAttemptAt.Load() - firstAttemptAt.Load()); elapsed < retryDelay {
		t.Fatalf("tail panic retry hot-looped after %v; want at least %v", elapsed, retryDelay)
	}
	if offset, err := checkpoints.LoadOffset(context.Background(), "async-panic"); err != nil || offset != OffsetOldest {
		t.Fatalf("blocked async retry checkpoint = %q, err=%v; want OffsetOldest", offset, err)
	}

	releaseHandler()
	waitFor(t, 5*time.Second, "async retry checkpoint", func() bool {
		offset, _ := checkpoints.LoadOffset(context.Background(), "async-panic")
		return offset == stored.Offset
	})
	if got := HandlerCount[followEvent](consumer); got != 0 {
		t.Fatalf("successful async Once retry left %d handler(s) registered", got)
	}
}

func TestDurableFollowRetriesUpcastFailureBeforeCheckpoint(t *testing.T) {
	const retryDelay = 30 * time.Millisecond
	store := NewMemoryStore()
	consumer := New(WithStore(store))

	var attempts atomic.Int32
	var firstAttemptAt, secondAttemptAt atomic.Int64
	if err := RegisterUpcastFunc(consumer, "follow.old", "eventbus.followEvent", func(data json.RawMessage) (json.RawMessage, string, error) {
		switch attempts.Add(1) {
		case 1:
			firstAttemptAt.Store(time.Now().UnixNano())
			panic("transient upcast panic")
		default:
			secondAttemptAt.Store(time.Now().UnixNano())
			return data, "eventbus.followEvent", nil
		}
	}); err != nil {
		t.Fatal(err)
	}

	delivered := make(chan followEvent, 1)
	Subscribe(consumer, func(event followEvent) { delivered <- event })
	reports := make(chan error, 1)
	consumer.SetPersistenceErrorHandler(func(_ any, _ reflect.Type, errorValue error) {
		reports <- errorValue
	})
	if _, err := store.Append(context.Background(), &Event{
		ID: NewEventID(), Type: "follow.old", Data: json.RawMessage(`{"Value":7}`), Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	stop := startFollower(t, consumer, FollowWithSubscriptionID("upcast-retry"), FollowPollInterval(retryDelay))
	defer stop()

	select {
	case err := <-reports:
		if err == nil {
			t.Fatal("upcast report was nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upcast failure was not reported")
	}
	if offset, err := store.LoadOffset(context.Background(), "upcast-retry"); err != nil || offset != OffsetOldest {
		t.Fatalf("failed upcast checkpoint = %q, err=%v; want OffsetOldest", offset, err)
	}

	select {
	case event := <-delivered:
		if event.Value != 7 {
			t.Fatalf("upcasted value = %d, want 7", event.Value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event was not retried after transient upcast failure")
	}
	if elapsed := time.Duration(secondAttemptAt.Load() - firstAttemptAt.Load()); elapsed < retryDelay {
		t.Fatalf("upcast retry hot-looped after %v; want at least %v", elapsed, retryDelay)
	}
	waitFor(t, 5*time.Second, "upcast retry checkpoint", func() bool {
		offset, _ := store.LoadOffset(context.Background(), "upcast-retry")
		return offset != OffsetOldest
	})
}

func TestDurableFollowRetriesFilterPanicBeforeCheckpoint(t *testing.T) {
	store := NewMemoryStore()
	Publish(New(WithStore(store)), followEvent{Value: 9})
	var panicOnce atomic.Bool
	panicOnce.Store(true)
	var panicCalls atomic.Int32
	reported := make(chan struct{}, 1)
	consumer := New(
		WithStore(store),
		WithPanicHandler(func(any, reflect.Type, any) { panicCalls.Add(1) }),
		WithPersistenceErrorHandler(func(any, reflect.Type, error) {
			select {
			case reported <- struct{}{}:
			default:
			}
		}),
	)
	delivered := make(chan followEvent, 1)
	if err := Subscribe(consumer, func(event followEvent) { delivered <- event },
		WithFilter(func(followEvent) bool {
			if panicOnce.CompareAndSwap(true, false) {
				panic("transient durable filter panic")
			}
			return true
		})); err != nil {
		t.Fatal(err)
	}

	stop := startFollower(t, consumer,
		FollowWithSubscriptionID("filter-panic-retry"),
		FollowPollInterval(10*time.Millisecond))
	defer stop()
	select {
	case <-reported:
		if offset, _ := store.LoadOffset(context.Background(), "filter-panic-retry"); offset != OffsetOldest {
			t.Fatalf("filter panic advanced checkpoint to %q", offset)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("filter panic was not reported")
	}
	select {
	case event := <-delivered:
		if event.Value != 9 {
			t.Fatalf("retried filter event = %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event was not retried after filter panic")
	}
	if panicCalls.Load() != 1 {
		t.Fatalf("filter PanicHandler calls = %d, want 1", panicCalls.Load())
	}
	waitFor(t, 5*time.Second, "filter retry checkpoint", func() bool {
		offset, _ := store.LoadOffset(context.Background(), "filter-panic-retry")
		return offset != OffsetOldest
	})
}

func TestDurableFollowCancellationInterruptsAsyncWait(t *testing.T) {
	store := NewMemoryStore()
	consumer := New(WithStore(store))
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHandler()

	Subscribe(consumer, func(followEvent) {
		close(started)
		<-release
	}, Async())
	Publish(New(WithStore(store)), followEvent{Value: 1})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Follow(ctx, FollowWithSubscriptionID("cancel-wait"), fastPoll)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("async handler did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow cancellation waited for blocked async handler")
	}
	if offset, err := store.LoadOffset(context.Background(), "cancel-wait"); err != nil || offset != OffsetOldest {
		t.Fatalf("cancelled async checkpoint = %q, err=%v; want OffsetOldest", offset, err)
	}

	releaseHandler()
	consumer.Wait()
}

func TestDurableFollowCancellationRetriesStartedAsyncOnceHandler(t *testing.T) {
	store := NewMemoryStore()
	consumer := New(WithStore(store))
	started := make(chan struct{})
	release := make(chan struct{})
	retried := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHandler()

	var attempts atomic.Int32
	subscription, err := SubscribeWithHandle(consumer, func(followEvent) {
		switch attempts.Add(1) {
		case 1:
			close(started)
			<-release
		case 2:
			close(retried)
		}
	}, Async(), Once())
	if err != nil {
		t.Fatal(err)
	}
	Publish(New(WithStore(store)), followEvent{Value: 1})

	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- consumer.Follow(ctx, FollowWithSubscriptionID("cancel-started-async-once"), fastPoll)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("async Once handler did not start")
	}

	cancel()
	select {
	case err := <-firstDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow cancellation waited for the started async Once handler")
	}
	if offset, err := store.LoadOffset(context.Background(), "cancel-started-async-once"); err != nil || offset != OffsetOldest {
		t.Fatalf("cancelled async Once checkpoint = %q, err=%v; want OffsetOldest", offset, err)
	}
	if got := atomic.LoadUint32(&subscription.h.executed); got != 1 {
		t.Fatalf("running cancelled Once handler executed state = %d, want claimed until completion", got)
	}
	if got := atomic.LoadUint32(&subscription.h.onceInFlight); got != 1 {
		t.Fatalf("running cancelled Once handler in-flight state = %d, want 1", got)
	}
	if got := HandlerCount[followEvent](consumer); got != 1 {
		t.Fatalf("cancelled attempt removed Once handler; count=%d", got)
	}

	// Restart immediately while the abandoned invocation is still blocked. The
	// new follower must not skip/checkpoint it or run the Once handler
	// concurrently; it retries only after that invocation finishes rolling back.
	stop := startFollower(t, consumer, FollowWithSubscriptionID("cancel-started-async-once"), fastPoll)
	defer stop()
	select {
	case <-retried:
		t.Fatal("Once handler retried concurrently with its cancelled invocation")
	case <-time.After(50 * time.Millisecond):
	}
	if offset, _ := store.LoadOffset(context.Background(), "cancel-started-async-once"); offset != OffsetOldest {
		t.Fatalf("immediate restart checkpointed in-flight Once handler at %q", offset)
	}

	releaseHandler()
	select {
	case <-retried:
	case <-time.After(5 * time.Second):
		t.Fatal("started async Once handler was not retried after cancellation")
	}
	waitFor(t, 5*time.Second, "async Once retry checkpoint", func() bool {
		offset, _ := store.LoadOffset(context.Background(), "cancel-started-async-once")
		return offset != OffsetOldest
	})
	waitFor(t, 5*time.Second, "async Once removal after retry", func() bool {
		return HandlerCount[followEvent](consumer) == 0
	})
	consumer.Wait()
}

func TestDurableFollowWaitsForRunningReplayCoordinator(t *testing.T) {
	store := NewMemoryStore()
	consumer := New(WithStore(store), WithReplayBatchSize(1))
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	var firstStartOnce, secondStartOnce, releaseFirstOnce, releaseSecondOnce sync.Once
	defer releaseFirstOnce.Do(func() { close(releaseFirst) })
	defer releaseSecondOnce.Do(func() { close(releaseSecond) })

	if err := SubscribeWithReplay(context.Background(), consumer, "inner-replay-frontier", func(event followEvent) {
		switch event.Value {
		case 1:
			firstStartOnce.Do(func() { close(firstStarted) })
			<-releaseFirst
		case 2:
			secondStartOnce.Do(func() { close(secondStarted) })
			<-releaseSecond
		}
	}); err != nil {
		t.Fatal(err)
	}

	firstPublishDone := make(chan struct{})
	go func() {
		Publish(consumer, followEvent{Value: 1})
		close(firstPublishDone)
	}()
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("inner replay coordinator did not start its first handler")
	}

	// Append event 2 from another bus so only the durable Follow dispatch wakes
	// this consumer's replay marker. The marker is already busy on event 1.
	Publish(New(WithStore(store)), followEvent{Value: 2})
	events, _, err := store.Read(context.Background(), OffsetOldest, 0)
	if err != nil || len(events) != 2 {
		t.Fatalf("stored events = %d, err=%v; want 2", len(events), err)
	}
	firstOffset, secondOffset := events[0].Offset, events[1].Offset

	stop := startFollower(t, consumer, FollowWithSubscriptionID("outer-follow-frontier"), fastPoll)
	defer stop()
	waitFor(t, 5*time.Second, "outer follower's first checkpoint", func() bool {
		offset, _ := store.LoadOffset(context.Background(), "outer-follow-frontier")
		return offset == firstOffset
	})
	if offset, _ := store.LoadOffset(context.Background(), "outer-follow-frontier"); offset == secondOffset {
		t.Fatal("outer Follow checkpoint passed the blocked replay coordinator")
	}

	releaseFirstOnce.Do(func() { close(releaseFirst) })
	select {
	case <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("inner replay coordinator did not begin the followed event")
	}
	if offset, _ := store.LoadOffset(context.Background(), "outer-follow-frontier"); offset != firstOffset {
		t.Fatalf("outer Follow checkpoint = %q while inner handler blocked, want %q", offset, firstOffset)
	}

	releaseSecondOnce.Do(func() { close(releaseSecond) })
	waitFor(t, 5*time.Second, "outer checkpoint after inner replay completion", func() bool {
		offset, _ := store.LoadOffset(context.Background(), "outer-follow-frontier")
		return offset == secondOffset
	})
	select {
	case <-firstPublishDone:
	case <-time.After(5 * time.Second):
		t.Fatal("leading publish did not return after replay completion")
	}
	consumer.Wait()
}

func TestDurableTailCancellationInterruptsAsyncWait(t *testing.T) {
	stored := &StoredEvent{
		Offset: "00000000000000000001",
		ID:     NewEventID(),
		Type:   "eventbus.followEvent",
		Data:   json.RawMessage(`{"Value":1}`),
	}
	events := &fixtureTailer{events: []*StoredEvent{stored}}
	checkpoints := NewMemoryStore()
	consumer := New(WithStore(events), WithSubscriptionStore(checkpoints))
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHandler()

	Subscribe(consumer, func(followEvent) {
		close(started)
		<-release
	}, Async())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Follow(ctx, FollowWithSubscriptionID("tail-cancel-wait"), fastPoll)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("tailed async handler did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tail cancellation waited for blocked async handler")
	}
	if offset, err := checkpoints.LoadOffset(context.Background(), "tail-cancel-wait"); err != nil || offset != OffsetOldest {
		t.Fatalf("cancelled tail checkpoint = %q, err=%v; want OffsetOldest", offset, err)
	}

	releaseHandler()
	consumer.Wait()
}

type doneObservingContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (c *doneObservingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestDurableFollowCancellationInterruptsFailureBackoff(t *testing.T) {
	store := NewMemoryStore()
	consumer := New(WithStore(store))
	reported := make(chan struct{}, 1)
	consumer.SetPersistenceErrorHandler(func(any, reflect.Type, error) {
		reported <- struct{}{}
	})
	Subscribe(consumer, func(followEvent) { panic("enter retry backoff") })
	Publish(New(WithStore(store)), followEvent{Value: 1})

	base, cancel := context.WithCancel(context.Background())
	ctx := &doneObservingContext{Context: base, observed: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- consumer.Follow(ctx, FollowWithSubscriptionID("cancel-failure-backoff"), FollowPollInterval(time.Hour))
	}()

	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("handler failure was not reported")
	}
	select {
	case <-ctx.observed:
	case <-time.After(5 * time.Second):
		t.Fatal("follower did not enter failure backoff")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failure backoff ignored cancellation")
	}
	if offset, err := store.LoadOffset(context.Background(), "cancel-failure-backoff"); err != nil || offset != OffsetOldest {
		t.Fatalf("failure-backoff checkpoint = %q, err=%v; want OffsetOldest", offset, err)
	}
}

func TestDurableFollowCancellationInterruptsAsyncSemaphore(t *testing.T) {
	store := NewMemoryStore()
	consumer := New(WithStore(store), WithAsyncHandlerLimit(1))
	// Occupy the only slot so Follow deterministically reaches the cancellable
	// semaphore acquisition without starting the handler.
	consumer.asyncSem <- struct{}{}
	defer func() { <-consumer.asyncSem }()

	var ran atomic.Bool
	subscription, err := SubscribeWithHandle(consumer, func(followEvent) { ran.Store(true) }, Async(), Once())
	if err != nil {
		t.Fatal(err)
	}
	Publish(New(WithStore(store)), followEvent{Value: 1})

	base, cancel := context.WithCancel(context.Background())
	ctx := &doneObservingContext{Context: base, observed: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- consumer.Follow(ctx, FollowWithSubscriptionID("cancel-semaphore"), fastPoll)
	}()

	select {
	case <-ctx.observed:
	case <-time.After(5 * time.Second):
		t.Fatal("Follow did not begin semaphore acquisition")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow cancellation remained blocked on async semaphore")
	}
	if ran.Load() {
		t.Fatal("handler started without acquiring async capacity")
	}
	if got := atomic.LoadUint32(&subscription.h.executed); got != 0 {
		t.Fatalf("cancelled semaphore wait consumed Once handler (executed=%d)", got)
	}
	if got := HandlerCount[followEvent](consumer); got != 1 {
		t.Fatalf("cancelled semaphore wait removed Once handler; count=%d", got)
	}
	if offset, err := store.LoadOffset(context.Background(), "cancel-semaphore"); err != nil || offset != OffsetOldest {
		t.Fatalf("semaphore-cancel checkpoint = %q, err=%v; want OffsetOldest", offset, err)
	}
}

func TestNonDurableFollowContinuesAfterRecoveredPanic(t *testing.T) {
	store := NewMemoryStore()
	consumer := New(WithStore(store))
	delivered := make(chan struct{}, 1)
	Subscribe(consumer, func(event followEvent) {
		if event.Value == 1 {
			panic("non-durable failure")
		}
		delivered <- struct{}{}
	})
	publisher := New(WithStore(store))
	Publish(publisher, followEvent{Value: 1})
	Publish(publisher, followEvent{Value: 2})

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll)
	defer stop()
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("non-durable follower stopped after recovered panic")
	}
}

func TestDispatchSkipsAsyncCapacityForConsumedOnceHandler(t *testing.T) {
	bus := New(WithAsyncHandlerLimit(1))
	eventType := reflect.TypeOf(followEvent{})
	h, err := buildPayloadHandler(Handler[followEvent](func(followEvent) {}), eventType, []SubscribeOption{Async(), Once()})
	if err != nil {
		t.Fatal(err)
	}
	atomic.StoreUint32(&h.executed, 1)
	bus.addHandler(eventType, h)

	if err := dispatchWithMode(bus, context.Background(), eventType, "eventbus.followEvent", followEvent{}, dispatchWaitForAsync); err != nil {
		t.Fatal(err)
	}
	if slots := len(bus.asyncSem); slots != 0 {
		t.Fatalf("consumed Once handler leaked %d async slot(s)", slots)
	}
}

func TestDispatchWaitAggregatesSyncAndAsyncPanics(t *testing.T) {
	bus := New()
	Subscribe(bus, func(followEvent) { panic("synchronous aggregate failure") })
	Subscribe(bus, func(followEvent) { panic("asynchronous aggregate failure") }, Async())

	eventType := reflect.TypeOf(followEvent{})
	err := dispatchWithMode(bus, context.Background(), eventType, "eventbus.followEvent", followEvent{}, dispatchWaitForAsync)
	if err == nil || !strings.Contains(err.Error(), "synchronous aggregate failure") ||
		!strings.Contains(err.Error(), "asynchronous aggregate failure") {
		t.Fatalf("dispatch error did not aggregate both panics: %v", err)
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

func TestFollowPoisonWakesResumableSubscriptions(t *testing.T) {
	t.Run("ReplaySkip checkpoints both consumers", func(t *testing.T) {
		store := NewMemoryStore()
		bus := New(WithStore(store))
		var replayCalls atomic.Int32
		if err := SubscribeWithReplay(context.Background(), bus, "projection-skip", func(followEvent) {
			replayCalls.Add(1)
		}, WithReplayErrorPolicy(ReplaySkip)); err != nil {
			t.Fatal(err)
		}
		poisonOffset, err := store.Append(context.Background(), &Event{
			ID: NewEventID(), Type: "eventbus.followEvent", Data: json.RawMessage(`{"Value":"poison"}`), Timestamp: time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}

		stop := startFollower(t, bus, FollowFrom(OffsetOldest), FollowWithSubscriptionID("transport-skip"), fastPoll)
		defer stop()
		waitFor(t, 5*time.Second, "poison checkpoints", func() bool {
			projection, _ := store.LoadOffset(context.Background(), "projection-skip")
			transport, _ := store.LoadOffset(context.Background(), "transport-skip")
			return projection == poisonOffset && transport == poisonOffset
		})
		if replayCalls.Load() != 0 {
			t.Fatalf("poison payload reached replay handler %d time(s)", replayCalls.Load())
		}
	})

	t.Run("ReplayAbort blocks the outer durable checkpoint", func(t *testing.T) {
		store := NewMemoryStore()
		var mu sync.Mutex
		var reports []string
		bus := New(
			WithStore(store),
			WithPersistenceErrorHandler(func(_ any, _ reflect.Type, err error) {
				mu.Lock()
				reports = append(reports, err.Error())
				mu.Unlock()
			}),
		)
		var replayCalls atomic.Int32
		if err := SubscribeWithReplay(context.Background(), bus, "projection-abort", func(followEvent) {
			replayCalls.Add(1)
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(context.Background(), &Event{
			ID: NewEventID(), Type: "eventbus.followEvent", Data: json.RawMessage(`{"Value":"poison"}`), Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- bus.Follow(ctx, FollowFrom(OffsetOldest), FollowWithSubscriptionID("transport-abort"), fastPoll)
		}()
		waitFor(t, 5*time.Second, "ReplayAbort report", func() bool {
			mu.Lock()
			defer mu.Unlock()
			return slices.ContainsFunc(reports, func(report string) bool {
				return strings.Contains(report, "projection-abort")
			})
		})
		if projection, _ := store.LoadOffset(context.Background(), "projection-abort"); projection != OffsetOldest {
			t.Fatalf("ReplayAbort projection checkpoint = %q, want OffsetOldest", projection)
		}
		if transport, _ := store.LoadOffset(context.Background(), "transport-abort"); transport != OffsetOldest {
			t.Fatalf("ReplayAbort transport checkpoint = %q, want OffsetOldest", transport)
		}
		if replayCalls.Load() != 0 {
			t.Fatalf("poison payload reached ReplayAbort handler %d time(s)", replayCalls.Load())
		}

		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Follow returned %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Follow did not stop after ReplayAbort cancellation")
		}
		Clear[followEvent](bus)
		bus.Wait()
	})
}

func TestDispatchInternalHandlersHonorsCancellationBeforeSignal(t *testing.T) {
	bus := New()
	eventType := reflect.TypeOf(followEvent{})
	var calls int
	bus.addHandler(eventType, &internalHandler{
		eventType: eventType,
		internalDelivery: func(context.Context, any) error {
			calls++
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := dispatchInternalHandlers(bus, ctx, eventType, "eventbus.followEvent", followEvent{}, dispatchNonBlocking)
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("cancelled internal dispatch = err:%v calls:%d", err, calls)
	}
}

type followV1 struct {
	Name string
}

type followV2 struct {
	FullName string
}

type rawLegacyFollowEvent struct {
	Value int
}

func (rawLegacyFollowEvent) EventTypeName() string { return "legacy.follow" }

type transientLegacyFollowEvent struct {
	Value int
}

func (transientLegacyFollowEvent) EventTypeName() string { return "legacy.transient-follow" }

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

func TestFollowRetriesRawUpcastReturnedTypeMismatch(t *testing.T) {
	store := NewMemoryStore()
	var reports atomic.Int32
	bus := New(
		WithStore(store),
		WithPersistenceErrorHandler(func(_ any, _ reflect.Type, err error) {
			if strings.Contains(err.Error(), "declared successor") {
				reports.Add(1)
			}
		}),
	)
	if err := RegisterUpcastFunc(bus, "legacy.follow", "eventbus.followEvent", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "legacy.follow", nil
	}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	if err := Subscribe(bus, func(followEvent) { calls.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), &Event{
		ID: NewEventID(), Type: "legacy.follow", Data: json.RawMessage(`{"Value":1}`), Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- bus.Follow(ctx, FollowFrom(OffsetOldest), FollowWithSubscriptionID("upcast-contract"), fastPoll)
	}()
	waitFor(t, 5*time.Second, "upcast contract report", func() bool { return reports.Load() > 0 })
	if calls.Load() != 0 {
		t.Fatalf("mismatched upcast delivered %d event(s)", calls.Load())
	}
	if offset, _ := store.LoadOffset(context.Background(), "upcast-contract"); offset != OffsetOldest {
		t.Fatalf("mismatched upcast checkpoint = %q, want OffsetOldest", offset)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow did not stop after upcast contract cancellation")
	}
}

func TestNonDurableFollowSkipsFailedUpcastWithoutLegacyDispatch(t *testing.T) {
	store := NewMemoryStore()
	var reports atomic.Int32
	bus := New(
		WithStore(store),
		WithPersistenceErrorHandler(func(_ any, _ reflect.Type, err error) {
			if strings.Contains(err.Error(), "declared successor") {
				reports.Add(1)
			}
		}),
	)
	if err := RegisterUpcastFunc(bus, "legacy.follow", "eventbus.followEvent", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "legacy.follow", nil
	}); err != nil {
		t.Fatal(err)
	}
	var legacyCalls, currentCalls atomic.Int32
	if err := Subscribe(bus, func(rawLegacyFollowEvent) { legacyCalls.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if err := Subscribe(bus, func(followEvent) { currentCalls.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), &Event{
		ID: NewEventID(), Type: "legacy.follow", Data: json.RawMessage(`{"Value":1}`), Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), &Event{
		ID: NewEventID(), Type: "eventbus.followEvent", Data: json.RawMessage(`{"Value":2}`), Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	stop := startFollower(t, bus, FollowFrom(OffsetOldest), fastPoll)
	defer stop()
	waitFor(t, 5*time.Second, "healthy event after failed upcast", func() bool {
		return reports.Load() > 0 && currentCalls.Load() == 1
	})
	if legacyCalls.Load() != 0 {
		t.Fatalf("failed upcast dispatched legacy handler %d time(s)", legacyCalls.Load())
	}
}

func TestNonDurableFollowUpcastFailureWakesReplayRetryWithoutLaterEvent(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store), WithLogDelivery())
	var attempts atomic.Int32
	if err := RegisterUpcastFunc(bus, "legacy.transient-follow", "eventbus.followEvent", func(data json.RawMessage) (json.RawMessage, string, error) {
		if attempts.Add(1) <= 2 {
			return data, "legacy.transient-follow", nil
		}
		return data, "eventbus.followEvent", nil
	}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	if err := SubscribeWithReplay(context.Background(), bus, "transient-upcast-replay", func(event followEvent) {
		if event.Value != 7 {
			t.Errorf("upcasted event value = %d, want 7", event.Value)
		}
		calls.Add(1)
	}); err != nil {
		t.Fatal(err)
	}
	Publish(bus, transientLegacyFollowEvent{Value: 7})
	if calls.Load() != 0 {
		t.Fatal("WithLogDelivery ran replay handler before Follow")
	}

	stop := startFollower(t, bus, fastPoll)
	defer stop()
	waitFor(t, 5*time.Second, "autonomous replay upcast retry", func() bool {
		return attempts.Load() >= 3 && calls.Load() == 1
	})
	if offset, err := store.LoadOffset(context.Background(), "transient-upcast-replay"); err != nil || offset == OffsetOldest {
		t.Fatalf("autonomous upcast retry checkpoint = %q, %v", offset, err)
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

type ownershipReadStore struct {
	*MemoryStore
	started chan struct{}
	once    sync.Once
}

func (s *ownershipReadStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	blocked := false
	s.once.Do(func() {
		blocked = true
		close(s.started)
	})
	if blocked {
		<-ctx.Done()
		return nil, from, ctx.Err()
	}
	return s.MemoryStore.Read(ctx, from, limit)
}

func TestDurableSubscriptionIDHasOneInProcessOwner(t *testing.T) {
	store := &ownershipReadStore{MemoryStore: NewMemoryStore(), started: make(chan struct{})}
	bus := New(WithStore(store))
	if err := Subscribe(bus, func(followEvent) {}); err != nil {
		t.Fatal(err)
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- bus.Follow(firstCtx, FollowWithSubscriptionID("shared-owner"), fastPoll)
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		cancelFirst()
		t.Fatal("first durable Follow did not enter its read loop")
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	cancelSecond()
	if err := bus.Follow(secondCtx, FollowWithSubscriptionID("shared-owner"), fastPoll); err == nil || !strings.Contains(err.Error(), "already registered") {
		cancelFirst()
		t.Fatalf("simultaneous Follow ownership error = %v", err)
	}
	if err := SubscribeWithReplay(context.Background(), bus, "shared-owner", func(followEvent) {}); err == nil || !strings.Contains(err.Error(), "already registered") {
		cancelFirst()
		t.Fatalf("Follow/replay ownership error = %v", err)
	}

	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Follow returned %v, want context.Canceled", err)
	}

	// Follow releases ownership when the blocking call exits, so a resumable
	// subscription may now claim the ID for its documented bus lifetime.
	if err := SubscribeWithReplay(context.Background(), bus, "shared-owner", func(followEvent) {}); err != nil {
		t.Fatalf("released Follow ID could not be reused: %v", err)
	}
	if err := bus.Follow(context.Background(), FollowWithSubscriptionID("shared-owner"), fastPoll); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("successful replay lifetime ownership error = %v", err)
	}
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

func (f *flakyStore) LookupOffset(ctx context.Context, id string) (Offset, bool, error) {
	if f.loadFails.Add(-1) >= 0 {
		return OffsetOldest, false, errors.New("load failure")
	}
	return f.MemoryStore.LookupOffset(ctx, id)
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
	consumer := New(WithStore(store))

	var reported atomic.Int32
	consumer.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
		reported.Add(1)
	})
	var count atomic.Int32
	Subscribe(consumer, func(e followEvent) { count.Add(1) })

	Publish(New(WithStore(store.MemoryStore)), followEvent{Value: 1})
	// Seed the first-run boundary so this test exercises a non-fatal progress
	// checkpoint failure, not the fatal initialization checkpoint.
	if err := store.MemoryStore.SaveOffset(context.Background(), "s", OffsetOldest); err != nil {
		t.Fatal(err)
	}
	store.saveFails.Store(1)

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
	// Empty is the valid OffsetOldest/chunk-start resume token. It must remain
	// distinguishable from a stale offset inherited from the follower context.
	store := &fixtureTailer{events: []*StoredEvent{
		{Offset: "", ID: NewEventID(), Type: "eventbus.followEvent", Data: json.RawMessage(`{"Value":9}`)},
	}}
	consumer := New(WithStore(store))

	var sawOldest atomic.Bool
	var count atomic.Int32
	SubscribeContext(consumer, func(ctx context.Context, e followEvent) {
		offset, ok := OffsetFromContext(ctx)
		sawOldest.Store(ok && offset == OffsetOldest)
		count.Add(1)
	})

	stop := startFollower(t, consumer, FollowFrom(OffsetOldest), fastPoll)
	defer stop()

	waitFor(t, 5*time.Second, "offsetless delivery", func() bool { return count.Load() == 1 })
	if !sawOldest.Load() {
		t.Error("an empty OffsetOldest event did not preserve its exact context offset")
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

func TestFollowCancellationDuringDeliveryDoesNotAdvance(t *testing.T) {
	stored := &StoredEvent{
		Offset: "00000000000000000001",
		ID:     NewEventID(),
		Type:   "eventbus.followEvent",
		Data:   json.RawMessage(`{"Value":1}`),
	}

	t.Run("poll", func(t *testing.T) {
		store := NewMemoryStore()
		if _, err := store.Append(context.Background(), &Event{
			ID: stored.ID, Type: stored.Type, Data: stored.Data, Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
		assertFollowCancelsDuringDelivery(t, store)
	})

	t.Run("tail", func(t *testing.T) {
		assertFollowCancelsDuringDelivery(t, &fixtureTailer{events: []*StoredEvent{stored}})
	})
}

func TestDurableFollowCancellationByLastOnceHandlerRollsBackRegistration(t *testing.T) {
	store := NewMemoryStore()
	Publish(New(WithStore(store)), followEvent{Value: 1})
	consumer := New(WithStore(store))
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	var attempts atomic.Int32
	subscription, err := SubscribeWithHandle(consumer, func(followEvent) {
		if attempts.Add(1) == 1 {
			cancelFirst()
		}
	}, Once())
	if err != nil {
		t.Fatal(err)
	}

	if err := consumer.Follow(firstCtx, FollowWithSubscriptionID("last-once-cancel"), fastPoll); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Follow = %v, want context.Canceled", err)
	}
	if offset, _ := store.LoadOffset(context.Background(), "last-once-cancel"); offset != OffsetOldest {
		t.Fatalf("cancelled Once attempt checkpoint = %q", offset)
	}
	if got := atomic.LoadUint32(&subscription.h.executed); got != 0 {
		t.Fatalf("cancelled last Once handler executed state = %d, want rollback", got)
	}
	if HandlerCount[followEvent](consumer) != 1 {
		t.Fatal("cancelled last Once handler was removed before retry")
	}

	stop := startFollower(t, consumer, FollowWithSubscriptionID("last-once-cancel"), fastPoll)
	defer stop()
	waitFor(t, 5*time.Second, "last Once retry checkpoint", func() bool {
		offset, _ := store.LoadOffset(context.Background(), "last-once-cancel")
		return offset != OffsetOldest
	})
	if got := attempts.Load(); got != 2 {
		t.Fatalf("last Once handler attempts = %d, want 2", got)
	}
}

type saveCountingStore struct {
	saves atomic.Int32
}

func (*saveCountingStore) LoadOffset(context.Context, string) (Offset, error) {
	return OffsetOldest, nil
}

func (s *saveCountingStore) SaveOffset(context.Context, string, Offset) error {
	s.saves.Add(1)
	return nil
}

func assertFollowCancelsDuringDelivery(t *testing.T, store EventStore) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	checkpoints := &saveCountingStore{}
	bus := New(WithStore(store), WithSubscriptionStore(checkpoints))
	Subscribe(bus, func(followEvent) { cancel() })

	if err := bus.Follow(ctx, FollowWithSubscriptionID("cancel-during-delivery"), fastPoll); !errors.Is(err, context.Canceled) {
		t.Fatalf("Follow returned %v, want context.Canceled", err)
	}
	if got := checkpoints.saves.Load(); got != 1 {
		t.Fatalf("cancellation during delivery saved %d checkpoint(s), want only the initial boundary", got)
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
