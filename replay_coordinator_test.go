package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type coordinatedEvent struct {
	ID        int    `json:"id"`
	Transient string `json:"-"`
}

func (coordinatedEvent) EventTypeName() string { return "test.coordinated.v1" }

func TestSubscribeWithReplayCheckpointCannotLeapfrogBlockedEvent(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))
	started := make(chan struct{})
	release := make(chan struct{})
	secondHandled := make(chan struct{})
	var startOnce, secondOnce sync.Once
	var mu sync.Mutex
	var order []int

	if err := SubscribeWithReplay(context.Background(), bus, "ordered-frontier", func(event coordinatedEvent) {
		if event.ID == 1 {
			startOnce.Do(func() { close(started) })
			<-release
		}
		mu.Lock()
		order = append(order, event.ID)
		mu.Unlock()
		if event.ID == 2 {
			secondOnce.Do(func() { close(secondHandled) })
		}
	}); err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan struct{})
	go func() {
		Publish(bus, coordinatedEvent{ID: 1})
		close(firstDone)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first handler did not block")
	}

	// The second publish may coalesce behind the active log-drain leader, but
	// it must never save its newer offset while event 1 is unfinished.
	Publish(bus, coordinatedEvent{ID: 2})
	if offset, err := store.LoadOffset(context.Background(), "ordered-frontier"); err != nil || offset != OffsetOldest {
		t.Fatalf("checkpoint leapfrogged blocked event: offset=%q err=%v", offset, err)
	}
	select {
	case <-secondHandled:
		t.Fatal("second event ran before the earlier event completed")
	default:
	}

	close(release)
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first publish did not finish")
	}
	bus.Wait()
	select {
	case <-secondHandled:
	case <-time.After(5 * time.Second):
		t.Fatal("coalesced second event was not drained")
	}

	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(order, []int{1, 2}) {
		t.Fatalf("delivery order = %v, want [1 2]", order)
	}
	if offset, _ := store.LoadOffset(context.Background(), "ordered-frontier"); offset != "00000000000000000002" {
		t.Fatalf("final checkpoint = %q, want event 2", offset)
	}
}

func TestSubscribeWithReplayPanicBlocksLaterCheckpoint(t *testing.T) {
	store := NewMemoryStore()
	var allowFirst atomic.Bool
	var successful []int
	bus := New(WithStore(store), WithPanicHandler(func(any, reflect.Type, any) {}))
	if err := SubscribeWithReplay(context.Background(), bus, "panic-frontier", func(event coordinatedEvent) {
		if event.ID == 1 && !allowFirst.Load() {
			panic("event 1 not ready")
		}
		successful = append(successful, event.ID)
	}); err != nil {
		t.Fatal(err)
	}

	Publish(bus, coordinatedEvent{ID: 1})
	Publish(bus, coordinatedEvent{ID: 2})
	if offset, _ := store.LoadOffset(context.Background(), "panic-frontier"); offset != OffsetOldest {
		t.Fatalf("panic gap was checkpointed past: %q", offset)
	}
	if len(successful) != 0 {
		t.Fatalf("later event ran past panic gap: %v", successful)
	}

	allowFirst.Store(true)
	Publish(bus, coordinatedEvent{ID: 3})
	bus.Wait()
	if !reflect.DeepEqual(successful, []int{1, 2, 3}) {
		t.Fatalf("retry order = %v, want [1 2 3]", successful)
	}
	if offset, _ := store.LoadOffset(context.Background(), "panic-frontier"); offset != "00000000000000000003" {
		t.Fatalf("checkpoint after recovery = %q, want event 3", offset)
	}
}

func TestSubscribeWithReplayReentrantPublishesCoalesceWithoutDeadlock(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))
	var mu sync.Mutex
	var order []int
	if err := SubscribeWithReplay(context.Background(), bus, "reentrant", func(event coordinatedEvent) {
		mu.Lock()
		order = append(order, event.ID)
		mu.Unlock()
		if event.ID == 1 {
			Publish(bus, coordinatedEvent{ID: 2})
			Publish(bus, coordinatedEvent{ID: 3})
		}
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		Publish(bus, coordinatedEvent{ID: 1})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reentrant publish deadlocked")
	}
	bus.Wait()
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(order, []int{1, 2, 3}) {
		t.Fatalf("reentrant delivery order = %v, want [1 2 3]", order)
	}
}

func TestSubscribeWithReplayHistoricalReentrantPublishesDoNotDeadlock(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))
	Publish(bus, coordinatedEvent{ID: 1})
	var mu sync.Mutex
	var order []int
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- SubscribeWithReplay(context.Background(), bus, "historical-reentrant", func(event coordinatedEvent) {
			mu.Lock()
			order = append(order, event.ID)
			mu.Unlock()
			if event.ID == 1 {
				Publish(bus, coordinatedEvent{ID: 2})
				Publish(bus, coordinatedEvent{ID: 3})
			}
		})
	}()
	select {
	case err := <-subscribeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("historical handler's repeated reentrant publishes deadlocked setup")
	}
	bus.Wait()
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(order, []int{1, 2, 3}) {
		t.Fatalf("historical reentrant order = %v, want [1 2 3]", order)
	}
}

func TestSubscribeWithReplayWaitTracksCoalescedDrain(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))
	started := make(chan struct{})
	release := make(chan struct{})
	secondHandled := make(chan struct{})
	var startOnce, secondOnce sync.Once

	if err := SubscribeWithReplay(context.Background(), bus, "wait-tracks-coalesced", func(event coordinatedEvent) {
		if event.ID == 1 {
			startOnce.Do(func() { close(started) })
			<-release
		}
		if event.ID == 2 {
			secondOnce.Do(func() { close(secondHandled) })
		}
	}); err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan struct{})
	go func() {
		Publish(bus, coordinatedEvent{ID: 1})
		close(firstDone)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first replay handler did not block")
	}

	// This publish returns after recording pending work. Wait must see that work
	// before Publish returns, even though its background drain is not launched
	// until the blocked leader completes.
	Publish(bus, coordinatedEvent{ID: 2})
	waitDone := make(chan struct{})
	go func() {
		bus.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		t.Fatal("Wait returned while a coalesced replay drain was pending")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after the coalesced drain completed")
	}
	select {
	case <-secondHandled:
	default:
		t.Fatal("Wait returned before the coalesced event was handled")
	}
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("leading Publish did not return")
	}
}

type replayCloseAwareStore struct {
	*MemoryStore
	closeOnce sync.Once
	closed    chan struct{}
}

func (s *replayCloseAwareStore) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func TestSubscribeWithReplayShutdownWaitsForCoalescedDrain(t *testing.T) {
	store := &replayCloseAwareStore{MemoryStore: NewMemoryStore(), closed: make(chan struct{})}
	bus := New(WithStore(store))
	started := make(chan struct{})
	release := make(chan struct{})
	secondHandled := make(chan struct{})
	var startOnce, secondOnce sync.Once
	if err := SubscribeWithReplay(context.Background(), bus, "shutdown-tracks-coalesced", func(event coordinatedEvent) {
		if event.ID == 1 {
			startOnce.Do(func() { close(started) })
			<-release
		}
		if event.ID == 2 {
			secondOnce.Do(func() { close(secondHandled) })
		}
	}); err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan struct{})
	go func() {
		Publish(bus, coordinatedEvent{ID: 1})
		close(firstDone)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first replay handler did not block")
	}
	Publish(bus, coordinatedEvent{ID: 2})

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- bus.Shutdown(context.Background()) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned with replay work pending: %v", err)
	case <-store.closed:
		t.Fatal("Shutdown closed the event store with replay work pending")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return after replay work completed")
	}
	select {
	case <-secondHandled:
	default:
		t.Fatal("Shutdown returned before the coalesced event was handled")
	}
	select {
	case <-store.closed:
	default:
		t.Fatal("Shutdown did not close the store after replay completed")
	}
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("leading Publish did not return")
	}
}

type blockingSetupReadStore struct {
	*MemoryStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingSetupReadStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	blocked := false
	if from == OffsetNewest {
		s.once.Do(func() {
			blocked = true
			close(s.started)
		})
	}
	if blocked {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, from, ctx.Err()
		}
	}
	return s.MemoryStore.Read(ctx, from, limit)
}

func TestSubscribeWithReplayClearDuringSetupRollsBack(t *testing.T) {
	tests := []struct {
		name  string
		clear func(*EventBus)
	}{
		{name: "typed clear", clear: func(bus *EventBus) { Clear[coordinatedEvent](bus) }},
		{name: "clear all", clear: ClearAll},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &blockingSetupReadStore{
				MemoryStore: NewMemoryStore(),
				started:     make(chan struct{}),
				release:     make(chan struct{}),
			}
			bus := New(WithStore(store))
			setupDone := make(chan error, 1)
			go func() {
				setupDone <- SubscribeWithReplay(context.Background(), bus, "clear-during-setup", func(coordinatedEvent) {})
			}()

			select {
			case <-store.started:
			case <-time.After(5 * time.Second):
				t.Fatal("subscription setup did not reach the captured-tail read")
			}
			tt.clear(bus)
			close(store.release)

			select {
			case err := <-setupDone:
				if err == nil || !strings.Contains(err.Error(), "removed during setup") {
					t.Fatalf("setup error = %v, want removal error", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("subscription setup did not roll back after Clear")
			}
			if got := HandlerCount[coordinatedEvent](bus); got != 0 {
				t.Fatalf("handler count after setup rollback = %d, want 0", got)
			}
			if got := bus.replayMarkerCount.Load(); got != 0 {
				t.Fatalf("global marker count after setup rollback = %d, want 0", got)
			}

			// The failed setup must release both its active ID ownership and its
			// provisional type claim, so the caller can safely retry.
			if err := SubscribeWithReplay(context.Background(), bus, "clear-during-setup", func(coordinatedEvent) {}); err != nil {
				t.Fatalf("retry after setup rollback: %v", err)
			}
			if got := bus.replayMarkerCount.Load(); got != 1 {
				t.Fatalf("global marker count after retry = %d, want 1", got)
			}
		})
	}
}

func TestCompleteReplaySetupRejectsRemovedOrClosedMarker(t *testing.T) {
	for _, tc := range []struct {
		name        string
		register    bool
		closeBefore bool
		wantClosed  bool
	}{
		{name: "marker already removed", wantClosed: true},
		{name: "coordinator already closed", register: true, closeBefore: true, wantClosed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryStore()
			bus := New(WithStore(store))
			eventType := reflect.TypeOf(coordinatedEvent{})
			reservation, err := bus.reservePersistedTypes(persistedTypeSpec{
				name: "test.coordinated.v1", eventType: eventType,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer reservation.Rollback()
			coordinator := &replayCoordinator[coordinatedEvent]{
				bus: bus, subStore: store, subscriptionID: "complete-setup",
				waitCh: make(chan struct{}), done: make(chan struct{}),
			}
			marker := &internalHandler{
				eventType:        eventType,
				internalDelivery: func(context.Context, any) error { return nil },
				onRemove:         coordinator.deactivate,
			}
			if tc.closeBefore {
				coordinator.deactivate()
			}
			if tc.register {
				bus.addHandler(eventType, marker)
			}

			err = completeReplaySetup(bus, eventType, marker, coordinator, reservation, false)
			if err == nil || !strings.Contains(err.Error(), "removed during setup") {
				t.Fatalf("complete setup error = %v", err)
			}
			bus.removeHandler(eventType, marker)
			if coordinator.closed != tc.wantClosed || HandlerCount[coordinatedEvent](bus) != 0 {
				t.Fatalf("failed completion state = closed:%v handlers:%d", coordinator.closed, HandlerCount[coordinatedEvent](bus))
			}
		})
	}
}

func TestClearDeactivatesReplayCoordinator(t *testing.T) {
	tests := []struct {
		name  string
		clear func(*EventBus)
	}{
		{name: "typed clear", clear: func(bus *EventBus) { Clear[coordinatedEvent](bus) }},
		{name: "clear all", clear: ClearAll},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStore()
			bus := New(WithStore(store))
			started := make(chan struct{})
			release := make(chan struct{})
			var startOnce sync.Once
			var mu sync.Mutex
			var handled []int
			if err := SubscribeWithReplay(context.Background(), bus, "clear-coordinator", func(event coordinatedEvent) {
				mu.Lock()
				handled = append(handled, event.ID)
				mu.Unlock()
				if event.ID == 1 {
					startOnce.Do(func() { close(started) })
					<-release
				}
			}); err != nil {
				t.Fatal(err)
			}

			firstDone := make(chan struct{})
			go func() {
				Publish(bus, coordinatedEvent{ID: 1})
				close(firstDone)
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("first replay handler did not block")
			}
			Publish(bus, coordinatedEvent{ID: 2}) // leaves a tracked pending drain
			tt.clear(bus)
			Publish(bus, coordinatedEvent{ID: 3}) // persisted after removal
			close(release)

			select {
			case <-firstDone:
			case <-time.After(5 * time.Second):
				t.Fatal("leading Publish did not return after Clear")
			}
			waitDone := make(chan struct{})
			go func() {
				bus.Wait()
				close(waitDone)
			}()
			select {
			case <-waitDone:
			case <-time.After(5 * time.Second):
				t.Fatal("Clear did not release the replay work token")
			}

			mu.Lock()
			got := append([]int(nil), handled...)
			mu.Unlock()
			if !reflect.DeepEqual(got, []int{1}) {
				t.Fatalf("post-clear replay deliveries = %v, want only the in-flight event", got)
			}
			if HandlerCount[coordinatedEvent](bus) != 0 {
				t.Fatal("Clear left the replay marker registered")
			}
			if got := bus.replayMarkerCount.Load(); got != 0 {
				t.Fatalf("Clear left %d global replay marker(s)", got)
			}
		})
	}
}

func TestUnsubscribeSkipsReplayMarker(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))
	var replayCalls, ordinaryCalls, removeCalls int
	if err := SubscribeWithReplay(context.Background(), bus, "unsubscribe-marker", func(coordinatedEvent) {
		replayCalls++
	}); err != nil {
		t.Fatal(err)
	}
	ordinary := Handler[coordinatedEvent](func(coordinatedEvent) { ordinaryCalls++ })
	if err := Subscribe(bus, ordinary); err != nil {
		t.Fatal(err)
	}

	// Give the public handler a synthetic lifecycle hook to verify Unsubscribe
	// runs hooks for a matched entry while skipping the preceding nil marker.
	eventType := reflect.TypeOf(coordinatedEvent{})
	shard := bus.getShard(eventType)
	shard.mu.Lock()
	for _, handler := range shard.handlers[eventType] {
		if handler.handler != nil {
			handler.onRemove = func() { removeCalls++ }
		}
	}
	shard.mu.Unlock()

	if err := Unsubscribe[coordinatedEvent](bus, ordinary); err != nil {
		t.Fatal(err)
	}
	Publish(bus, coordinatedEvent{ID: 1})
	if replayCalls != 1 || ordinaryCalls != 0 || removeCalls != 1 {
		t.Fatalf("calls after unsubscribe = replay:%d ordinary:%d remove:%d, want 1/0/1", replayCalls, ordinaryCalls, removeCalls)
	}
	if HandlerCount[coordinatedEvent](bus) != 1 {
		t.Fatalf("handler count = %d, want only replay marker", HandlerCount[coordinatedEvent](bus))
	}
}

func TestUnsubscribeRejectsInvalidHandlerWithoutPanic(t *testing.T) {
	bus := New()
	var nilHandler Handler[coordinatedEvent]
	if err := Unsubscribe[coordinatedEvent](bus, nilHandler); err == nil {
		t.Fatal("typed nil handler was accepted")
	}
	if err := Unsubscribe[coordinatedEvent, any](bus, nil); err == nil {
		t.Fatal("nil interface handler was accepted")
	}
	if err := Unsubscribe[coordinatedEvent](bus, 42); err == nil {
		t.Fatal("non-function handler was accepted")
	}
}

func TestSubscribeWithReplayDeliversOnlyPersistedRepresentation(t *testing.T) {
	store := &errorStore{failAppend: true}
	bus := New(WithStore(store))
	var calls int
	if err := SubscribeWithReplay(context.Background(), bus, "persisted-only", func(coordinatedEvent) {
		calls++
	}); err != nil {
		t.Fatal(err)
	}

	Publish(bus, coordinatedEvent{ID: 1})
	bus.Wait()
	if calls != 0 {
		t.Fatalf("append failure delivered %d non-durable event(s)", calls)
	}
}

func TestSubscribeWithReplayLiveValueIsDecodedFromLog(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))
	var got coordinatedEvent
	if err := SubscribeWithReplay(context.Background(), bus, "durable-value", func(event coordinatedEvent) {
		got = event
	}); err != nil {
		t.Fatal(err)
	}
	Publish(bus, coordinatedEvent{ID: 7, Transient: "not durable"})
	if got.ID != 7 || got.Transient != "" {
		t.Fatalf("log-decoded value = %+v, want ID 7 with no transient field", got)
	}
}

func TestSubscribeWithReplayLiveFilterAndObservabilityRunOnce(t *testing.T) {
	store := NewMemoryStore()
	var filters, starts, completes int
	obs := &testObservability{
		onHandlerStart: func(ctx context.Context, _ string, _ bool) context.Context {
			starts++
			return ctx
		},
		onHandlerComplete: func(context.Context, string, time.Duration, error) {
			completes++
		},
	}
	bus := New(WithStore(store), WithObservability(obs))
	if err := SubscribeWithReplay(context.Background(), bus, "single-boundary", func(coordinatedEvent) {},
		WithFilter(func(coordinatedEvent) bool { filters++; return true })); err != nil {
		t.Fatal(err)
	}
	Publish(bus, coordinatedEvent{ID: 1})
	if filters != 1 || starts != 1 || completes != 1 {
		t.Fatalf("live delivery boundary = filter:%d start:%d complete:%d, want 1 each", filters, starts, completes)
	}
}

type failNextReadStore struct {
	*MemoryStore
	fail atomic.Bool
}

type panicNextReadStore struct {
	*MemoryStore
	panicReads atomic.Int32
}

func (s *panicNextReadStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	if from != OffsetNewest && s.panicReads.Add(-1) >= 0 {
		panic("transient store read panic")
	}
	return s.MemoryStore.Read(ctx, from, limit)
}

func (s *failNextReadStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	if from != OffsetNewest && s.fail.CompareAndSwap(true, false) {
		return nil, from, errors.New("transient live read failure")
	}
	return s.MemoryStore.Read(ctx, from, limit)
}

func TestSubscribeWithReplayLiveReadFailureRetriesAutonomously(t *testing.T) {
	store := &failNextReadStore{MemoryStore: NewMemoryStore()}
	var reported atomic.Int32
	bus := New(
		WithStore(store),
		WithPersistenceErrorHandler(func(any, reflect.Type, error) { reported.Add(1) }),
	)
	var got []int
	if err := SubscribeWithReplay(context.Background(), bus, "read-retry", func(event coordinatedEvent) {
		got = append(got, event.ID)
	}); err != nil {
		t.Fatal(err)
	}

	store.fail.Store(true)
	Publish(bus, coordinatedEvent{ID: 1})
	bus.Wait()
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("autonomous read retry delivery = %v, want [1]", got)
	}

	Publish(bus, coordinatedEvent{ID: 2})
	bus.Wait()
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("read retry order = %v, want [1 2]", got)
	}
	if reported.Load() == 0 {
		t.Fatal("live read failure was not reported")
	}
}

func TestSubscribeWithReplayStorePanicRetriesAutonomously(t *testing.T) {
	store := &panicNextReadStore{MemoryStore: NewMemoryStore()}
	var reports atomic.Int32
	bus := New(
		WithStore(store),
		WithPersistenceErrorHandler(func(any, reflect.Type, error) { reports.Add(1) }),
	)
	var got []int
	if err := SubscribeWithReplay(context.Background(), bus, "read-panic-retry", func(event coordinatedEvent) {
		got = append(got, event.ID)
	}); err != nil {
		t.Fatal(err)
	}

	store.panicReads.Store(2)
	Publish(bus, coordinatedEvent{ID: 1})
	bus.Wait()
	if !reflect.DeepEqual(got, []int{1}) || reports.Load() != 2 {
		t.Fatalf("store panic recovery = events:%v reports:%d, want [1]/2", got, reports.Load())
	}
}

type blockingTransientReadStore struct {
	*MemoryStore
	fail    atomic.Bool
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingTransientReadStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	if from != OffsetNewest && s.fail.CompareAndSwap(true, false) {
		s.once.Do(func() { close(s.started) })
		select {
		case <-s.release:
			return nil, from, errors.New("blocked transient read failure")
		case <-ctx.Done():
			return nil, from, ctx.Err()
		}
	}
	return s.MemoryStore.Read(ctx, from, limit)
}

func TestSubscribeWithReplayReadFailurePreservesConcurrentSignal(t *testing.T) {
	store := &blockingTransientReadStore{
		MemoryStore: NewMemoryStore(),
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	store.fail.Store(true)
	bus := New(WithStore(store))
	var mu sync.Mutex
	var got []int
	if err := SubscribeWithReplay(context.Background(), bus, "read-pending", func(event coordinatedEvent) {
		mu.Lock()
		got = append(got, event.ID)
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan struct{})
	go func() {
		Publish(bus, coordinatedEvent{ID: 1})
		close(firstDone)
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("leading replay read did not block")
	}

	// This signal returns after being coalesced behind the failing leader. It
	// must remain tracked and cause a retry after that leader reports its error.
	Publish(bus, coordinatedEvent{ID: 2})
	close(store.release)
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("leading Publish did not return after read failure")
	}
	bus.Wait()

	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("delivery after failed leader with pending signal = %v, want [1 2]", got)
	}
}

func TestSubscribeWithReplayTransientFilterPanicDoesNotWedgeCoordinator(t *testing.T) {
	store := NewMemoryStore()
	var reports atomic.Int32
	var panics atomic.Int32
	bus := New(
		WithStore(store),
		WithPersistenceErrorHandler(func(any, reflect.Type, error) { reports.Add(1) }),
		WithPanicHandler(func(any, reflect.Type, any) { panics.Add(1) }),
	)
	var panicOnce atomic.Bool
	panicOnce.Store(true)
	var got []int
	if err := SubscribeWithReplay(context.Background(), bus, "filter-panic-retry", func(event coordinatedEvent) {
		got = append(got, event.ID)
	}, WithFilter(func(coordinatedEvent) bool {
		if panicOnce.CompareAndSwap(true, false) {
			panic("transient filter panic")
		}
		return true
	})); err != nil {
		t.Fatal(err)
	}

	Publish(bus, coordinatedEvent{ID: 1})
	bus.Wait()
	Publish(bus, coordinatedEvent{ID: 2})
	bus.Wait()
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("delivery after recovered coordinator panic = %v, want [1 2]", got)
	}
	if reports.Load() != 1 {
		t.Fatalf("filter panic persistence reports = %d, want 1", reports.Load())
	}
	if panics.Load() != 1 {
		t.Fatalf("filter PanicHandler calls = %d, want 1", panics.Load())
	}
}

func TestStoredEventContextMasksUnrelatedEnvelope(t *testing.T) {
	type contextKey string
	base := context.WithValue(context.Background(), contextKey("keep"), "value")
	base = context.WithValue(base, offsetCtxKey{}, Offset("stale-offset"))
	base = context.WithValue(base, eventIDCtxKey{}, "stale-id")
	base = context.WithValue(base, metadataCtxKey{}, map[string]string{"stale": "metadata"})

	empty := contextForStoredEvent(base, &StoredEvent{})
	if offset, ok := OffsetFromContext(empty); !ok || offset != OffsetOldest {
		t.Fatalf("empty-but-valid stored offset = %q, %v; want OffsetOldest", offset, ok)
	}
	if _, ok := EventIDFromContext(empty); ok {
		t.Fatal("empty stored ID inherited the signal ID")
	}
	if _, ok := MetadataFromContext(empty); ok {
		t.Fatal("empty stored metadata inherited signal metadata")
	}
	if got := empty.Value(contextKey("keep")); got != "value" {
		t.Fatalf("unrelated context value = %v, want value", got)
	}

	stored := &StoredEvent{
		Offset:   "stored-offset",
		ID:       "stored-id",
		Metadata: map[string]string{"stored": "metadata"},
	}
	full := contextForStoredEvent(base, stored)
	if offset, ok := OffsetFromContext(full); !ok || offset != stored.Offset {
		t.Fatalf("stored offset = %q, %v", offset, ok)
	}
	if id, ok := EventIDFromContext(full); !ok || id != stored.ID {
		t.Fatalf("stored ID = %q, %v", id, ok)
	}
	if metadata, ok := MetadataFromContext(full); !ok || metadata["stored"] != "metadata" {
		t.Fatalf("stored metadata = %v, %v", metadata, ok)
	}
}

func TestSubscribeWithReplayRejectsDuplicateAndEmptyIDs(t *testing.T) {
	bus := New(WithStore(NewMemoryStore()))
	if err := SubscribeWithReplay(context.Background(), bus, "", func(coordinatedEvent) {}); err == nil || !strings.Contains(err.Error(), "cannot be empty") {
		t.Fatalf("empty subscription ID error = %v", err)
	}
	if err := SubscribeWithReplay(context.Background(), bus, "duplicate", func(coordinatedEvent) {}); err != nil {
		t.Fatal(err)
	}
	if err := SubscribeWithReplay(context.Background(), bus, "duplicate", func(coordinatedEvent) {}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate subscription ID error = %v", err)
	}
	Clear[coordinatedEvent](bus)
	if err := SubscribeWithReplay(context.Background(), bus, "duplicate", func(coordinatedEvent) {}); err == nil || !strings.Contains(err.Error(), "cannot be reused") {
		t.Fatalf("cleared subscription ID was reused: %v", err)
	}
}

type coordinatorReadFixture struct {
	read func(context.Context, Offset, int) ([]*StoredEvent, Offset, error)
	save func(context.Context, string, Offset) error
}

type comparingCoordinatorReadFixture struct {
	*coordinatorReadFixture
	compare func(Offset, Offset) (int, error)
}

func (s *comparingCoordinatorReadFixture) CompareOffsets(left, right Offset) (int, error) {
	return s.compare(left, right)
}

type cancelOnLoadStore struct {
	*MemoryStore
	cancel context.CancelFunc
}

func (s *cancelOnLoadStore) LoadOffset(ctx context.Context, id string) (Offset, error) {
	s.cancel()
	return s.MemoryStore.LoadOffset(ctx, id)
}

func (*coordinatorReadFixture) Append(context.Context, *Event) (Offset, error) {
	return OffsetOldest, errors.New("append not used")
}

func (s *coordinatorReadFixture) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	return s.read(ctx, from, limit)
}

func (s *coordinatorReadFixture) SaveOffset(ctx context.Context, id string, offset Offset) error {
	if s.save != nil {
		return s.save(ctx, id, offset)
	}
	return nil
}

func (*coordinatorReadFixture) LoadOffset(context.Context, string) (Offset, error) {
	return OffsetOldest, nil
}

type coordinatorFixtureStore interface {
	EventStore
	SubscriptionStore
}

func newCoordinatorFixture(t *testing.T, store coordinatorFixtureStore) *replayCoordinator[coordinatedEvent] {
	t.Helper()
	bus := New(WithStore(store))
	eventType := reflect.TypeOf(coordinatedEvent{})
	h, err := buildContextHandler(ContextHandler[coordinatedEvent](func(context.Context, coordinatedEvent) {}), eventType, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &replayCoordinator[coordinatedEvent]{
		bus:            bus,
		subStore:       store,
		subscriptionID: "fixture",
		eventType:      eventType,
		typeName:       "test.coordinated.v1",
		userHandler:    h,
		waitCh:         make(chan struct{}),
		done:           make(chan struct{}),
	}
}

func TestReplayCoordinatorReadCursorEdgeCases(t *testing.T) {
	t.Run("symbolic newest tail is rejected", func(t *testing.T) {
		store := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from != OffsetNewest {
				t.Fatalf("unexpected replay read after symbolic tail: %q", from)
			}
			return nil, OffsetNewest, nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		err := coordinator.drainToCapturedTail(context.Background())
		if err == nil || !strings.Contains(err.Error(), "concrete resumable tail") {
			t.Fatalf("symbolic tail error = %v", err)
		}
	})

	t.Run("newest tail cannot return history", func(t *testing.T) {
		store := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from != OffsetNewest {
				t.Fatalf("unexpected replay read after invalid tail result: %q", from)
			}
			return []*StoredEvent{{Offset: "tail"}}, "tail", nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		err := coordinator.drainToCapturedTail(context.Background())
		if err == nil || !strings.Contains(err.Error(), "returned 1 event(s)") {
			t.Fatalf("history-returning tail error = %v", err)
		}
	})

	t.Run("empty advancing batches reach barrier", func(t *testing.T) {
		store := &coordinatorReadFixture{}
		store.read = func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetNewest {
				return nil, "tail", nil
			}
			return nil, "tail", nil
		}
		coordinator := newCoordinatorFixture(t, store)
		if err := coordinator.drainToCapturedTail(context.Background()); err != nil {
			t.Fatal(err)
		}
		if coordinator.scanOffset != "tail" {
			t.Fatalf("scan offset = %q, want tail", coordinator.scanOffset)
		}
	})

	t.Run("empty non-advancing batch is the captured tail", func(t *testing.T) {
		store := &coordinatorReadFixture{}
		store.read = func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetNewest {
				return nil, "tail", nil
			}
			return nil, from, nil
		}
		coordinator := newCoordinatorFixture(t, store)
		if err := coordinator.drainToCapturedTail(context.Background()); err != nil {
			t.Fatal(err)
		}
		if coordinator.scanOffset != OffsetOldest {
			t.Fatalf("empty tail moved scan offset to %q", coordinator.scanOffset)
		}
	})

	t.Run("zero-event gap continues then next offset reaches barrier", func(t *testing.T) {
		store := &coordinatorReadFixture{}
		store.read = func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			switch from {
			case OffsetNewest:
				return nil, "tail", nil
			case OffsetOldest:
				return nil, "gap", nil
			default:
				return []*StoredEvent{{
					Offset: "embedded",
					Type:   "foreign",
					Data:   json.RawMessage(`{}`),
				}}, "tail", nil
			}
		}
		coordinator := newCoordinatorFixture(t, store)
		if err := coordinator.drainToCapturedTail(context.Background()); err != nil {
			t.Fatal(err)
		}
		if coordinator.scanOffset != "tail" {
			t.Fatalf("scan offset = %q, want tail", coordinator.scanOffset)
		}
	})

	t.Run("events with non-advancing next offset fail", func(t *testing.T) {
		store := &coordinatorReadFixture{}
		store.read = func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetNewest {
				return nil, "tail", nil
			}
			return []*StoredEvent{{Offset: "odd", Type: "foreign", Data: json.RawMessage(`{}`)}}, from, nil
		}
		coordinator := newCoordinatorFixture(t, store)
		err := coordinator.drainToCapturedTail(context.Background())
		if err == nil || !strings.Contains(err.Error(), "non-advancing") {
			t.Fatalf("non-advancing read error = %v", err)
		}
	})

	t.Run("store ordering stops after an indivisible unit crosses the tail", func(t *testing.T) {
		reads := 0
		base := &coordinatorReadFixture{}
		base.read = func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			reads++
			switch from {
			case OffsetNewest:
				return nil, "00000000000000000002", nil
			case OffsetOldest:
				return []*StoredEvent{
					{Offset: "00000000000000000001", Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":1}`)},
					{Offset: "00000000000000000003", Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":3}`)},
				}, "00000000000000000003", nil
			default:
				t.Fatalf("unexpected third read from %q", from)
				return nil, from, nil
			}
		}
		store := &comparingCoordinatorReadFixture{
			coordinatorReadFixture: base,
			compare: func(left, right Offset) (int, error) {
				switch {
				case left < right:
					return -1, nil
				case left > right:
					return 1, nil
				default:
					return 0, nil
				}
			},
		}
		coordinator := newCoordinatorFixture(t, store)
		var got []int
		h, err := buildContextHandler(ContextHandler[coordinatedEvent](func(_ context.Context, event coordinatedEvent) {
			got = append(got, event.ID)
		}), reflect.TypeOf(coordinatedEvent{}), nil)
		if err != nil {
			t.Fatal(err)
		}
		coordinator.userHandler = h

		if err := coordinator.drainToCapturedTail(context.Background()); err != nil {
			t.Fatal(err)
		}
		if reads != 2 {
			t.Fatalf("drain performed %d reads, want tail capture plus one bounded read", reads)
		}
		if !reflect.DeepEqual(got, []int{1, 3}) {
			t.Fatalf("deliveries = %v, want the one crossing unit [1 3]", got)
		}
		if coordinator.scanOffset != "00000000000000000003" {
			t.Fatalf("scan offset = %q, want actual overrun token", coordinator.scanOffset)
		}
	})

	t.Run("store ordering errors abort the drain", func(t *testing.T) {
		base := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from != OffsetNewest {
				t.Fatalf("ordering failure should precede history read from %q", from)
			}
			return nil, "00000000000000000002", nil
		}}
		store := &comparingCoordinatorReadFixture{
			coordinatorReadFixture: base,
			compare: func(Offset, Offset) (int, error) {
				return 0, errors.New("offset ordering unavailable")
			},
		}
		coordinator := newCoordinatorFixture(t, store)
		err := coordinator.drainToCapturedTail(context.Background())
		if err == nil || !strings.Contains(err.Error(), "compare replay offset") ||
			!strings.Contains(err.Error(), "offset ordering unavailable") {
			t.Fatalf("ordering error = %v", err)
		}
	})

	for _, tc := range []struct {
		name   string
		events []*StoredEvent
		next   Offset
		failAt Offset
	}{
		{name: "ordering error after empty advancing read", next: "gap", failAt: "gap"},
		{
			name:   "ordering error after stored event",
			events: []*StoredEvent{{Offset: "event", Type: "foreign", Data: json.RawMessage(`{}`)}},
			next:   "next",
			failAt: "event",
		},
		{
			name:   "ordering error after returned next offset",
			events: []*StoredEvent{{Offset: "event", Type: "foreign", Data: json.RawMessage(`{}`)}},
			next:   "next",
			failAt: "next",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
				if from == OffsetNewest {
					return nil, "tail", nil
				}
				if from != OffsetOldest {
					t.Fatalf("unexpected additional history read from %q", from)
				}
				return tc.events, tc.next, nil
			}}
			store := &comparingCoordinatorReadFixture{
				coordinatorReadFixture: base,
				compare: func(left, _ Offset) (int, error) {
					if left == tc.failAt {
						return 0, fmt.Errorf("cannot order %s", left)
					}
					return -1, nil
				},
			}
			coordinator := newCoordinatorFixture(t, store)
			err := coordinator.drainToCapturedTail(context.Background())
			if err == nil || !strings.Contains(err.Error(), "compare replay offset") ||
				!strings.Contains(err.Error(), "cannot order "+string(tc.failAt)) {
				t.Fatalf("ordering error = %v", err)
			}
		})
	}
}

func TestReplayCoordinatorCancellationAtEveryDrainBoundary(t *testing.T) {
	t.Run("before tail capture", func(t *testing.T) {
		reads := 0
		store := &coordinatorReadFixture{read: func(context.Context, Offset, int) ([]*StoredEvent, Offset, error) {
			reads++
			return nil, OffsetOldest, nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := coordinator.drainToCapturedTail(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled drain = %v", err)
		}
		if reads != 0 {
			t.Fatalf("pre-cancelled drain performed %d reads", reads)
		}
	})

	t.Run("after tail capture", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from != OffsetNewest {
				t.Fatalf("cancelled drain read from %q after tail capture", from)
			}
			cancel()
			return nil, "tail", nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		if err := coordinator.drainToCapturedTail(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("tail-cancelled drain = %v", err)
		}
	})

	t.Run("after empty advancing batch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetNewest {
				return nil, "tail", nil
			}
			cancel()
			return nil, "gap", nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		if err := coordinator.drainToCapturedTail(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("empty-batch cancellation = %v", err)
		}
	})

	t.Run("before stored event", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetNewest {
				return nil, "tail", nil
			}
			cancel()
			return []*StoredEvent{{Offset: "tail", Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":1}`)}}, "tail", nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		if err := coordinator.drainToCapturedTail(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("event-boundary cancellation = %v", err)
		}
	})

	t.Run("after stored handler", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var saves int
		store := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetNewest {
				return nil, "tail", nil
			}
			return []*StoredEvent{{Offset: "tail", Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":1}`)}}, "tail", nil
		}, save: func(context.Context, string, Offset) error {
			saves++
			return nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		h, err := buildContextHandler(ContextHandler[coordinatedEvent](func(context.Context, coordinatedEvent) {
			cancel()
		}), reflect.TypeOf(coordinatedEvent{}), nil)
		if err != nil {
			t.Fatal(err)
		}
		coordinator.userHandler = h
		if err := coordinator.drainToCapturedTail(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("post-handler cancellation = %v", err)
		}
		if saves != 0 {
			t.Fatalf("post-handler cancellation saved %d checkpoint(s), want 0", saves)
		}
	})

	t.Run("after replay-skip report", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var saves int
		store := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetNewest {
				return nil, "tail", nil
			}
			return []*StoredEvent{{Offset: "tail", Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":`)}}, "tail", nil
		}, save: func(context.Context, string, Offset) error {
			saves++
			return nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		coordinator.userHandler.replayErrorPolicy = ReplaySkip
		coordinator.bus.SetPersistenceErrorHandler(func(any, reflect.Type, error) { cancel() })
		if err := coordinator.drainToCapturedTail(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("replay-skip cancellation = %v", err)
		}
		if saves != 0 {
			t.Fatalf("cancelled replay skip saved %d checkpoint(s), want 0", saves)
		}
	})

	t.Run("after filtered event", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetNewest {
				return nil, "tail", nil
			}
			return []*StoredEvent{{Offset: "tail", Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":1}`)}}, "tail", nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		h, err := buildContextHandler(ContextHandler[coordinatedEvent](func(context.Context, coordinatedEvent) {}),
			reflect.TypeOf(coordinatedEvent{}), []SubscribeOption{WithFilter(func(coordinatedEvent) bool {
				cancel()
				return false
			})})
		if err != nil {
			t.Fatal(err)
		}
		coordinator.userHandler = h
		if err := coordinator.drainToCapturedTail(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("post-filter cancellation = %v", err)
		}
	})
}

func TestSubscribeWithReplayPreCancelledContextHasNoSideEffects(t *testing.T) {
	store := NewMemoryStore()
	Publish(New(WithStore(store)), coordinatedEvent{ID: 1})
	bus := New(WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls int
	if err := SubscribeWithReplay(ctx, bus, "cancelled-setup", func(coordinatedEvent) { calls++ }); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled setup = %v", err)
	}
	if calls != 0 || HandlerCount[coordinatedEvent](bus) != 0 {
		t.Fatalf("pre-cancelled setup side effects = calls:%d handlers:%d", calls, HandlerCount[coordinatedEvent](bus))
	}
	if offset, _ := store.LoadOffset(context.Background(), "cancelled-setup"); offset != OffsetOldest {
		t.Fatalf("pre-cancelled setup checkpoint = %q", offset)
	}
	if err := SubscribeWithReplay(context.Background(), bus, "cancelled-setup", func(coordinatedEvent) { calls++ }); err != nil {
		t.Fatalf("cancelled setup did not release its ID: %v", err)
	}
	if calls != 1 {
		t.Fatalf("successful retry calls = %d, want 1", calls)
	}
}

func TestSubscribeWithReplayCancellationDuringOffsetLoadRollsBack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnLoadStore{MemoryStore: NewMemoryStore(), cancel: cancel}
	bus := New(WithStore(store))
	if err := SubscribeWithReplay(ctx, bus, "cancel-during-load", func(coordinatedEvent) {}); !errors.Is(err, context.Canceled) {
		t.Fatalf("load-time cancellation = %v", err)
	}
	if HandlerCount[coordinatedEvent](bus) != 0 {
		t.Fatal("load-time cancellation installed a replay marker")
	}
	if err := SubscribeWithReplay(context.Background(), bus, "cancel-during-load", func(coordinatedEvent) {}); err != nil {
		t.Fatalf("load-time cancellation did not release ID/type claim: %v", err)
	}
}

func TestReplayCoordinatorDeactivationStopsEachDrainPhase(t *testing.T) {
	t.Run("before tail capture", func(t *testing.T) {
		reads := 0
		store := &coordinatorReadFixture{read: func(context.Context, Offset, int) ([]*StoredEvent, Offset, error) {
			reads++
			return nil, OffsetOldest, nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		coordinator.deactivate()
		if err := coordinator.drainToCapturedTail(context.Background()); err != nil {
			t.Fatal(err)
		}
		if reads != 0 {
			t.Fatalf("closed coordinator performed %d store reads", reads)
		}
	})

	t.Run("after tail capture", func(t *testing.T) {
		var coordinator *replayCoordinator[coordinatedEvent]
		store := &coordinatorReadFixture{}
		store.read = func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from != OffsetNewest {
				t.Fatalf("closed coordinator read from %q after capturing the tail", from)
			}
			coordinator.deactivate()
			return nil, "tail", nil
		}
		coordinator = newCoordinatorFixture(t, store)
		if err := coordinator.drainToCapturedTail(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("between stored events", func(t *testing.T) {
		batch := []*StoredEvent{
			{Offset: "first", Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":1}`)},
			{Offset: "tail", Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":2}`)},
		}
		store := &coordinatorReadFixture{}
		store.read = func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetNewest {
				return nil, "tail", nil
			}
			return batch, "tail", nil
		}
		coordinator := newCoordinatorFixture(t, store)
		var got []int
		h, err := buildContextHandler(ContextHandler[coordinatedEvent](func(_ context.Context, event coordinatedEvent) {
			got = append(got, event.ID)
			coordinator.deactivate()
		}), reflect.TypeOf(coordinatedEvent{}), nil)
		if err != nil {
			t.Fatal(err)
		}
		coordinator.userHandler = h
		if err := coordinator.drainToCapturedTail(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, []int{1}) {
			t.Fatalf("deliveries after deactivation = %v, want [1]", got)
		}
	})
}

func TestReplayCoordinatorActivationDefersPendingCatchUp(t *testing.T) {
	reads := atomic.Int32{}
	store := &coordinatorReadFixture{read: func(context.Context, Offset, int) ([]*StoredEvent, Offset, error) {
		reads.Add(1)
		return nil, OffsetOldest, nil
	}}
	coordinator := newCoordinatorFixture(t, store)
	coordinator.bus.asyncStarted()
	coordinator.tracked = true
	coordinator.pending = true
	needsCatchUp, err := coordinator.activate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !needsCatchUp || coordinator.active || reads.Load() != 0 {
		t.Fatalf("deferred activation = catch-up:%v active:%v reads:%d", needsCatchUp, coordinator.active, reads.Load())
	}
	coordinator.finalizeActivation(needsCatchUp)
	coordinator.bus.Wait()
	coordinator.bus.asyncMu.Lock()
	remaining := coordinator.bus.asyncCount
	coordinator.bus.asyncMu.Unlock()
	if remaining != 0 || coordinator.tracked || !coordinator.active || reads.Load() == 0 {
		t.Fatalf("final activation state = count:%d tracked:%v active:%v reads:%d", remaining, coordinator.tracked, coordinator.active, reads.Load())
	}
}

func TestReplayCoordinatorActivationLifecycleEdges(t *testing.T) {
	t.Run("activation error releases pending token", func(t *testing.T) {
		store := &coordinatorReadFixture{read: func(context.Context, Offset, int) ([]*StoredEvent, Offset, error) {
			return nil, OffsetOldest, errors.New("tail unavailable")
		}}
		coordinator := newCoordinatorFixture(t, store)
		coordinator.bus.asyncStarted()
		coordinator.tracked = true
		if _, err := coordinator.activate(context.Background()); err == nil {
			t.Fatal("activation read failure was ignored")
		}
		coordinator.bus.asyncMu.Lock()
		remaining := coordinator.bus.asyncCount
		coordinator.bus.asyncMu.Unlock()
		if remaining != 0 || coordinator.tracked || !coordinator.closed {
			t.Fatalf("failed activation state = count:%d tracked:%v closed:%v", remaining, coordinator.tracked, coordinator.closed)
		}
	})

	t.Run("closed coordinator cannot finalize", func(t *testing.T) {
		coordinator := &replayCoordinator[coordinatedEvent]{closed: true}
		coordinator.finalizeActivation(true)
		if coordinator.active {
			t.Fatal("closed coordinator became active")
		}
	})

	t.Run("deactivation during catch-up is an activation error", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		store := &coordinatorReadFixture{read: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
			if from == OffsetNewest {
				close(started)
				<-release
			}
			return nil, OffsetOldest, nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		done := make(chan error, 1)
		go func() {
			_, err := coordinator.activate(context.Background())
			done <- err
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("activation catch-up did not start")
		}
		coordinator.deactivate()
		close(release)
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "removed during setup") {
				t.Fatalf("activation error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("activation did not stop after deactivation")
		}
	})

	t.Run("finalize defensively creates missing pending token", func(t *testing.T) {
		store := &coordinatorReadFixture{read: func(context.Context, Offset, int) ([]*StoredEvent, Offset, error) {
			return nil, OffsetOldest, nil
		}}
		coordinator := newCoordinatorFixture(t, store)
		coordinator.finalizeActivation(true)
		coordinator.bus.Wait()
		if !coordinator.active || coordinator.tracked {
			t.Fatalf("finalized coordinator state = active:%v tracked:%v", coordinator.active, coordinator.tracked)
		}
	})
}

func TestReplayCoordinatorRetriesWholeSharedOffsetBatch(t *testing.T) {
	batch := []*StoredEvent{
		{Offset: OffsetOldest, Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":1}`)},
		{Offset: OffsetOldest, Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":2}`)},
		{Offset: "chunk-end", Type: "test.coordinated.v1", Data: json.RawMessage(`{"id":3}`)},
	}
	store := &coordinatorReadFixture{}
	store.read = func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
		if from == OffsetNewest {
			return nil, "chunk-end", nil
		}
		return batch, "chunk-end", nil
	}
	coordinator := newCoordinatorFixture(t, store)
	var failSecond atomic.Bool
	failSecond.Store(true)
	var got []int
	h, err := buildContextHandler(ContextHandler[coordinatedEvent](func(_ context.Context, event coordinatedEvent) {
		if event.ID == 2 && failSecond.CompareAndSwap(true, false) {
			panic("middle event failed")
		}
		got = append(got, event.ID)
	}), reflect.TypeOf(coordinatedEvent{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	coordinator.userHandler = h
	coordinator.bus.SetPanicHandler(func(any, reflect.Type, any) {})

	if err := coordinator.drainToCapturedTail(context.Background()); err == nil {
		t.Fatal("expected first shared-offset batch attempt to fail")
	}
	if coordinator.scanOffset != OffsetOldest {
		t.Fatalf("failed batch advanced scan cursor to %q", coordinator.scanOffset)
	}
	if err := coordinator.drainToCapturedTail(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{1, 1, 2, 3}) {
		t.Fatalf("shared-offset retry delivery = %v, want [1 1 2 3]", got)
	}
}

func TestReplayCoordinatorScheduledDrainHasIndependentWaitToken(t *testing.T) {
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	var startOnce sync.Once
	store := &coordinatorReadFixture{}
	store.read = func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
		if from == OffsetNewest {
			startOnce.Do(func() { close(readStarted) })
			<-releaseRead
		}
		return nil, OffsetOldest, nil
	}
	coordinator := newCoordinatorFixture(t, store)
	coordinator.active = true

	// There is deliberately no pending-work token here. This isolates the
	// scheduleDrain goroutine's own token, which must cover the case where a
	// durable contender consumed and released the pending token first. Holding
	// mu forces the goroutine to remain preempted before signal can take over.
	coordinator.mu.Lock()
	coordinator.scheduleDrain(0)
	waitDone := make(chan struct{})
	go func() {
		coordinator.bus.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		coordinator.mu.Unlock()
		close(releaseRead)
		t.Fatal("Wait returned before the scheduled goroutine acquired coordinator leadership")
	case <-time.After(50 * time.Millisecond):
		coordinator.mu.Unlock()
	}
	select {
	case <-readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduled drain did not start after coordinator leadership opened")
	}
	select {
	case <-waitDone:
		t.Fatal("Wait returned while the independently tracked drain was blocked")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseRead)
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after the scheduled drain completed")
	}
}

func TestReplayCoordinatorDeactivationCancelsDelayedRetry(t *testing.T) {
	coordinator := newCoordinatorFixture(t, &coordinatorReadFixture{read: func(context.Context, Offset, int) ([]*StoredEvent, Offset, error) {
		return nil, OffsetOldest, nil
	}})
	coordinator.active = true
	coordinator.scheduleDrain(time.Hour)
	coordinator.deactivate()
	coordinator.bus.Wait()
	coordinator.bus.asyncMu.Lock()
	remaining := coordinator.bus.asyncCount
	coordinator.bus.asyncMu.Unlock()
	if remaining != 0 {
		t.Fatalf("delayed retry left async count %d", remaining)
	}
}

func TestReplayCoordinatorSignalClosedAndCancellation(t *testing.T) {
	closed := &replayCoordinator[coordinatedEvent]{closed: true}
	if err := closed.signal(context.Background()); err != nil {
		t.Fatalf("closed signal = %v", err)
	}

	waitingBus := New()
	waiting := &replayCoordinator[coordinatedEvent]{bus: waitingBus, pending: true, waitCh: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), durableDispatchCtxKey{}, true))
	cancel()
	if err := waiting.signal(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiting signal = %v", err)
	}
	waiting.deactivate()
	waitingBus.Wait()

	durableBus := New()
	durable := &replayCoordinator[coordinatedEvent]{
		bus: durableBus, active: true, running: true, waitCh: make(chan struct{}),
	}
	durableCtx := context.WithValue(context.Background(), durableDispatchCtxKey{}, true)
	durableDone := make(chan error, 1)
	go func() { durableDone <- durable.signal(durableCtx) }()
	select {
	case err := <-durableDone:
		t.Fatalf("durable signal returned before active drain completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	durable.mu.Lock()
	durable.running = false
	durable.mu.Unlock()
	durable.deactivate()
	if err := <-durableDone; err != nil {
		t.Fatalf("closed durable waiter = %v", err)
	}
	durableBus.Wait()

	type strippedKey string
	stripped := valueStrippedContext{Context: context.WithValue(context.Background(), strippedKey("key"), "value")}
	if got := stripped.Value(strippedKey("key")); got != nil {
		t.Fatalf("stripped context leaked value %v", got)
	}
}

func TestSubscribeWithReplayAbortsOnUpcastFailure(t *testing.T) {
	store := NewMemoryStore()
	if _, err := store.Append(context.Background(), &Event{
		Type: "test.coordinated.old", Data: json.RawMessage(`{"id":1}`), Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	bus := New(WithStore(store))
	if err := RegisterUpcastFunc(bus, "test.coordinated.old", "test.coordinated.v1", func(json.RawMessage) (json.RawMessage, string, error) {
		return nil, "", errors.New("migration unavailable")
	}); err != nil {
		t.Fatal(err)
	}
	err := SubscribeWithReplay(context.Background(), bus, "upcast-failure", func(coordinatedEvent) {})
	if err == nil || !strings.Contains(err.Error(), "migration unavailable") {
		t.Fatalf("upcast failure = %v", err)
	}
}
