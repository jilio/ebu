package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gapStore wraps MemoryStore but deliberately does NOT implement
// EventStoreStreamer, forcing Replay down the batched Read path. When the
// first replay pass drains (first empty Read), it injects an event directly
// into the underlying store — simulating an event persisted by another
// publisher in the window between the end of replay and live subscription
// registration.
type gapStore struct {
	inner    *MemoryStore
	injected atomic.Bool
	inject   func(s *gapStore)
	reads    atomic.Int64
	failRead atomic.Bool
}

func (g *gapStore) Append(ctx context.Context, event *Event) (Offset, error) {
	return g.inner.Append(ctx, event)
}

func (g *gapStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	if g.failRead.Load() {
		return nil, from, fmt.Errorf("simulated read failure")
	}
	g.reads.Add(1)
	events, next, err := g.inner.Read(ctx, from, limit)
	if err == nil && len(events) == 0 && g.inject != nil && g.injected.CompareAndSwap(false, true) {
		g.inject(g)
	}
	return events, next, err
}

func (g *gapStore) SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error {
	return g.inner.SaveOffset(ctx, subscriptionID, offset)
}

func (g *gapStore) LoadOffset(ctx context.Context, subscriptionID string) (Offset, error) {
	return g.inner.LoadOffset(ctx, subscriptionID)
}

type gapEvent struct {
	ID int `json:"id"`
}

// TestSubscribeWithReplayCatchUpGap verifies that an event persisted after
// the first replay pass drained but before the live subscription registered
// is still delivered (by the catch-up pass) instead of being lost until the
// next restart.
func TestSubscribeWithReplayCatchUpGap(t *testing.T) {
	store := &gapStore{inner: NewMemoryStore()}
	store.inject = func(s *gapStore) {
		data, _ := json.Marshal(gapEvent{ID: 42})
		s.inner.Append(context.Background(), &Event{
			Type:      "eventbus.gapEvent",
			Data:      data,
			Timestamp: time.Now(),
		})
	}

	bus := New(WithStore(store))

	var mu sync.Mutex
	var received []int
	err := SubscribeWithReplay(context.Background(), bus, "gap-sub", func(e gapEvent) {
		mu.Lock()
		received = append(received, e.ID)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("SubscribeWithReplay: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0] != 42 {
		t.Fatalf("expected catch-up pass to deliver the gap event [42], got %v", received)
	}

	// The gap event's offset must have been saved so it is not replayed again.
	off, _ := store.LoadOffset(context.Background(), "gap-sub")
	if off == OffsetOldest {
		t.Fatal("expected the gap event's offset to be saved")
	}
}

// TestSubscribeWithReplayCatchUpError covers the catch-up pass failing.
func TestSubscribeWithReplayCatchUpError(t *testing.T) {
	store := &gapStore{inner: NewMemoryStore()}
	// Fail all reads after the first replay pass drains.
	store.inject = func(s *gapStore) { s.failRead.Store(true) }

	bus := New(WithStore(store))
	var calls atomic.Int32
	err := SubscribeWithReplay(context.Background(), bus, "gap-err", func(e gapEvent) {
		calls.Add(1)
	})
	if err == nil {
		t.Fatal("expected catch-up replay error")
	}
	if got := HandlerCount[gapEvent](bus); got != 0 {
		t.Fatalf("failed subscription leaked %d live handler(s)", got)
	}

	// A call that reports failure must have no live side effects. Publishing
	// after the failed catch-up would invoke a leaked handler immediately.
	Publish(bus, gapEvent{ID: 99})
	if got := calls.Load(); got != 0 {
		t.Fatalf("failed subscription still received a live event (%d calls)", got)
	}
}

// TestSubscribeWithReplaySubscribeError covers SubscribeContext failing after
// a successful replay (nil subscribe option).
func TestSubscribeWithReplaySubscribeError(t *testing.T) {
	bus := New(WithStore(NewMemoryStore()))
	err := SubscribeWithReplay(context.Background(), bus, "bad-opt", func(e gapEvent) {}, nil)
	if err == nil {
		t.Fatal("expected error from nil subscribe option")
	}
}

// TestWithAsyncHandlerLimit verifies bounded async concurrency and that all
// handlers still run.
func TestWithAsyncHandlerLimit(t *testing.T) {
	const limit = 2
	const events = 8

	bus := New(WithAsyncHandlerLimit(limit))

	var current, max, total atomic.Int64
	Subscribe(bus, func(e gapEvent) {
		c := current.Add(1)
		for {
			m := max.Load()
			if c <= m || max.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		current.Add(-1)
		total.Add(1)
	}, Async())

	for i := 0; i < events; i++ {
		Publish(bus, gapEvent{ID: i})
	}
	bus.Wait()

	if total.Load() != events {
		t.Fatalf("expected %d handler runs, got %d", events, total.Load())
	}
	if max.Load() > limit {
		t.Fatalf("async concurrency %d exceeded limit %d", max.Load(), limit)
	}
}

// TestWithAsyncHandlerLimitZero verifies a non-positive limit means unlimited.
func TestWithAsyncHandlerLimitZero(t *testing.T) {
	bus := New(WithAsyncHandlerLimit(0))
	if bus.asyncSem != nil {
		t.Fatal("expected nil semaphore for limit 0")
	}

	var ran atomic.Bool
	Subscribe(bus, func(e gapEvent) { ran.Store(true) }, Async())
	Publish(bus, gapEvent{ID: 1})
	bus.Wait()
	if !ran.Load() {
		t.Fatal("async handler did not run")
	}
}

// TestUpcastReentrantRegister verifies an upcast function can register
// another upcast without deadlocking (the registry lock is not held while
// user upcast functions run).
func TestUpcastReentrantRegister(t *testing.T) {
	bus := New(WithStore(NewMemoryStore()))

	registered := false
	err := RegisterUpcastFunc(bus, "v1", "v2", func(data json.RawMessage) (json.RawMessage, string, error) {
		if !registered {
			registered = true
			if err := RegisterUpcastFunc(bus, "other.v1", "other.v2", func(d json.RawMessage) (json.RawMessage, string, error) {
				return d, "other.v2", nil
			}); err != nil {
				t.Errorf("reentrant register: %v", err)
			}
		}
		return data, "v2", nil
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		data, typ, err := bus.upcastRegistry.apply(json.RawMessage(`{}`), "v1")
		if err != nil {
			t.Errorf("apply: %v", err)
		}
		if typ != "v2" || data == nil {
			t.Errorf("unexpected result: %s %s", data, typ)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("apply deadlocked on reentrant registration")
	}
	if !registered {
		t.Fatal("reentrant registration did not run")
	}
}

// TestSetUpcastErrorHandlerConcurrent exercises SetUpcastErrorHandler racing
// with a failing upcast; run under -race this verifies the handler is
// synchronized with apply.
func TestSetUpcastErrorHandlerConcurrent(t *testing.T) {
	bus := New()
	if err := RegisterUpcastFunc(bus, "c.v1", "c.v2", func(data json.RawMessage) (json.RawMessage, string, error) {
		return nil, "", fmt.Errorf("always fails")
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				bus.SetUpcastErrorHandler(func(eventType string, data json.RawMessage, err error) {})
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			bus.upcastRegistry.apply(json.RawMessage(`{}`), "c.v1")
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}
