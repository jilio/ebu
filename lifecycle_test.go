package eventbus

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Deliberately does not expose MemoryStore.ReadStream: Read is independently
// blockable, including a driver that ignores cancellation until it returns.
type lifecycleStore struct {
	memory   *MemoryStore
	appendFn func(context.Context)
	readFn   func(context.Context)
	loadFn   func(context.Context)
	closeFn  func() error
	closes   atomic.Int32
}

func (s *lifecycleStore) Append(ctx context.Context, e *Event) (Offset, error) {
	if s.appendFn != nil {
		s.appendFn(ctx)
	}
	return s.memory.Append(ctx, e)
}
func (s *lifecycleStore) Read(ctx context.Context, from Offset, n int) ([]*StoredEvent, Offset, error) {
	if s.readFn != nil {
		s.readFn(ctx)
	}
	return s.memory.Read(ctx, from, n)
}
func (s *lifecycleStore) LoadOffset(ctx context.Context, id string) (Offset, error) {
	if s.loadFn != nil {
		s.loadFn(ctx)
	}
	return s.memory.LoadOffset(ctx, id)
}
func (s *lifecycleStore) SaveOffset(ctx context.Context, id string, o Offset) error {
	return s.memory.SaveOffset(ctx, id, o)
}
func (s *lifecycleStore) Close() error {
	s.closes.Add(1)
	if s.closeFn != nil {
		return s.closeFn()
	}
	return nil
}
func lifecycleContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func lifecycleReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not complete")
		var zero T
		return zero
	}
}

func TestShutdownWaitsForAcceptedOperations(t *testing.T) {
	for _, kind := range []string{"append", "before hook", "sync handler", "async handler", "replay read", "replay handler", "replay setup", "follow read", "follow checkpoint"} {
		t.Run(kind, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			block := func(context.Context) { once.Do(func() { close(entered) }); <-release }
			store := &lifecycleStore{memory: NewMemoryStore()}
			bus := New(WithStore(store))
			var run func() error
			switch kind {
			case "append":
				store.appendFn = block
			case "before hook":
				bus.beforePublish = func(reflect.Type, any) { block(context.Background()) }
			case "sync handler", "async handler":
				var opts []SubscribeOption
				if kind == "async handler" {
					opts = append(opts, Async())
				}
				if err := SubscribeContext(bus, func(ctx context.Context, _ TestEvent) { block(ctx) }, opts...); err != nil {
					t.Fatal(err)
				}
			case "replay read":
				store.readFn = block
				run = func() error {
					return bus.Replay(context.Background(), OffsetOldest, func(*StoredEvent) error { return nil })
				}
			case "replay handler":
				Publish(bus, TestEvent{ID: 1})
				run = func() error {
					return bus.ReplayWithUpcast(context.Background(), OffsetOldest, func(*StoredEvent) error { block(context.Background()); return nil })
				}
			case "replay setup":
				store.loadFn = block
				run = func() error { return SubscribeWithReplay(context.Background(), bus, "sub", func(TestEvent) {}) }
			case "follow read":
				store.readFn = block
				run = func() error { return bus.Follow(context.Background(), FollowFrom(OffsetOldest)) }
			case "follow checkpoint":
				store.loadFn = block
				run = func() error { return bus.Follow(context.Background(), FollowWithSubscriptionID("sub")) }
			}
			if run == nil {
				run = func() error { return TryPublish(bus, TestEvent{ID: 2}) }
			}
			result := make(chan error, 1)
			go func() { result <- run() }()
			lifecycleReceive(t, entered)
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if err := bus.Shutdown(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("Shutdown = %v", err)
			}
			if n := store.closes.Load(); n != 0 {
				t.Fatalf("closed store during %s: %d", kind, n)
			}
			if err := TryPublish(bus, TestEvent{}); !errors.Is(err, ErrClosed) {
				t.Fatalf("late publish = %v", err)
			}
			unblock()
			err := lifecycleReceive(t, result)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if err := bus.Shutdown(lifecycleContext(t)); err != nil {
				t.Fatal(err)
			}
			if n := store.closes.Load(); n != 1 {
				t.Fatalf("Close calls = %d", n)
			}
		})
	}
}

func TestShutdownRejectsNewOperations(t *testing.T) {
	store := &lifecycleStore{memory: NewMemoryStore()}
	bus := New(WithStore(store))
	if err := bus.Shutdown(lifecycleContext(t)); err != nil {
		t.Fatal(err)
	}
	fail := func() { t.Error("closed bus invoked user code or accessed store") }
	store.appendFn = func(context.Context) { fail() }
	store.readFn = store.appendFn
	store.loadFn = store.appendFn
	bus.beforePublish = func(reflect.Type, any) { fail() }
	bus.afterPublish = bus.beforePublish
	bus.persistenceErrorHandler = func(any, reflect.Type, error) { fail() }
	handler := func(TestEvent) { fail() }
	ctxHandler := func(context.Context, TestEvent) { fail() }
	for name, run := range map[string]func() error{
		"TryPublish":                 func() error { return TryPublish(bus, TestEvent{}) },
		"TryPublishContext":          func() error { return TryPublishContext(bus, context.Background(), TestEvent{}) },
		"Subscribe":                  func() error { return Subscribe(bus, handler) },
		"SubscribeContext":           func() error { return SubscribeContext(bus, ctxHandler) },
		"SubscribeWithHandle":        func() error { _, err := SubscribeWithHandle(bus, handler); return err },
		"SubscribeContextWithHandle": func() error { _, err := SubscribeContextWithHandle(bus, ctxHandler); return err },
		"SubscribeWithReplay":        func() error { return SubscribeWithReplay(context.Background(), bus, "s", handler) },
		"SubscribeContextWithReplay": func() error { return SubscribeContextWithReplay(context.Background(), bus, "s", ctxHandler) },
		"Replay": func() error {
			return bus.Replay(context.Background(), OffsetOldest, func(*StoredEvent) error { fail(); return nil })
		},
		"ReplayWithUpcast": func() error {
			return bus.ReplayWithUpcast(context.Background(), OffsetOldest, func(*StoredEvent) error { fail(); return nil })
		},
		"Follow": func() error { return bus.Follow(context.Background()) },
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); !errors.Is(err, ErrClosed) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	Publish(bus, TestEvent{})
	PublishContext(bus, context.Background(), TestEvent{})
}

func TestShutdownConcurrentAndBlockingClose(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	expected := errors.New("close failure")
	store := &lifecycleStore{memory: NewMemoryStore(), closeFn: func() error { close(entered); <-release; return expected }}
	bus := New(WithStore(store))
	const callers = 20
	results := make(chan error, callers)
	ctx := lifecycleContext(t)
	for range callers {
		go func() { results <- bus.Shutdown(ctx) }()
	}
	lifecycleReceive(t, entered)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.Shutdown(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked Close: %v", err)
	}
	unblock()
	for range callers {
		if err := lifecycleReceive(t, results); !errors.Is(err, expected) {
			t.Fatalf("Shutdown = %v", err)
		}
	}
	if err := bus.Shutdown(ctx); !errors.Is(err, expected) {
		t.Fatalf("repeated Shutdown = %v", err)
	}
	if n := store.closes.Load(); n != 1 {
		t.Fatalf("Close calls = %d", n)
	}
}

type lifecycleStreamStore struct {
	*lifecycleStore
	started chan struct{}
	release chan struct{}
}

func (s *lifecycleStreamStore) ReadStream(context.Context, Offset) iter.Seq2[*StoredEvent, error] {
	return func(yield func(*StoredEvent, error) bool) {
		close(s.started)
		<-s.release
		yield(&StoredEvent{Offset: "1"}, nil)
	}
}
func TestShutdownWaitsForStreamingReplay(t *testing.T) {
	store := &lifecycleStreamStore{lifecycleStore: &lifecycleStore{memory: NewMemoryStore()}, started: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(store.release) })
	defer unblock()
	bus := New(WithStore(store))
	result := make(chan error, 1)
	go func() {
		result <- bus.Replay(context.Background(), OffsetOldest, func(*StoredEvent) error { return nil })
	}()
	lifecycleReceive(t, store.started)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if store.closes.Load() != 0 {
		t.Fatal("closed while stream active")
	}
	unblock()
	if err := lifecycleReceive(t, result); err != nil {
		t.Fatal(err)
	}
	if err := bus.Shutdown(lifecycleContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownRejectsNestedPublishDuringDrain(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	bus := New()
	nested := make(chan error, 1)
	if err := Subscribe(bus, func(TestEvent) { close(entered); <-release; nested <- TryPublish(bus, 42) }, Async()); err != nil {
		t.Fatal(err)
	}
	Publish(bus, TestEvent{})
	lifecycleReceive(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	unblock()
	if err := lifecycleReceive(t, nested); !errors.Is(err, ErrClosed) {
		t.Fatalf("nested publish = %v", err)
	}
	if err := bus.Shutdown(lifecycleContext(t)); err != nil {
		t.Fatal(err)
	}
}

type lifecycleTailStore struct {
	*lifecycleStore
	started chan struct{}
	exited  chan struct{}
}

func (s *lifecycleTailStore) Tail(ctx context.Context, _ Offset) iter.Seq2[*StoredEvent, error] {
	return func(func(*StoredEvent, error) bool) {
		close(s.started)
		<-ctx.Done()
		close(s.exited)
	}
}
func TestShutdownCancelsAndJoinsTail(t *testing.T) {
	store := &lifecycleTailStore{lifecycleStore: &lifecycleStore{memory: NewMemoryStore()}, started: make(chan struct{}), exited: make(chan struct{})}
	store.closeFn = func() error {
		select {
		case <-store.exited:
		default:
			t.Error("Close before Tail exited")
		}
		return nil
	}
	bus := New(WithStore(store))
	result := make(chan error, 1)
	go func() { result <- bus.Follow(context.Background(), FollowFrom(OffsetOldest)) }()
	lifecycleReceive(t, store.started)
	if err := bus.Shutdown(lifecycleContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := lifecycleReceive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Follow = %v", err)
	}
}

func TestShutdownWaitsForDeferredReplay(t *testing.T) {
	store := &lifecycleStore{memory: NewMemoryStore()}
	bus := New(WithStore(store))
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var attempts atomic.Int32
	if err := SubscribeWithReplay(context.Background(), bus, "sub", func(TestEvent) {
		if attempts.Add(1) == 1 {
			// A failed durable handler leaves its checkpoint behind and schedules
			// a background retry. The publisher returns before that retry finishes.
			panic("transient handler failure")
		}
		close(entered)
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- TryPublish(bus, TestEvent{ID: 1}) }()
	if err := lifecycleReceive(t, result); err != nil {
		t.Fatal(err)
	}
	lifecycleReceive(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if store.closes.Load() != 0 {
		t.Fatal("Close before deferred replay completed")
	}
	unblock()
	if err := bus.Shutdown(lifecycleContext(t)); err != nil {
		t.Fatal(err)
	}
	saved, err := store.memory.LoadOffset(context.Background(), "sub")
	if err != nil {
		t.Fatal(err)
	}
	_, tail, err := store.memory.Read(context.Background(), OffsetNewest, 0)
	if err != nil {
		t.Fatal(err)
	}
	if saved != tail {
		t.Fatalf("checkpoint %q did not reach tail %q", saved, tail)
	}
}

func TestPublishRacingShutdown(t *testing.T) {
	for range 20 {
		var closed atomic.Bool
		var accepted, handled atomic.Int32
		store := &lifecycleStore{memory: NewMemoryStore()}
		store.appendFn = func(context.Context) {
			if closed.Load() {
				t.Error("Append after Close")
			}
		}
		store.closeFn = func() error { closed.Store(true); return nil }
		bus := New(WithStore(store))
		if err := Subscribe(bus, func(TestEvent) {
			if closed.Load() {
				t.Error("handler after Close")
			}
			handled.Add(1)
		}, Async()); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := TryPublish(bus, TestEvent{})
				if err == nil {
					accepted.Add(1)
				} else if !errors.Is(err, ErrClosed) {
					t.Error(err)
				}
			}()
		}
		close(start)
		if err := bus.Shutdown(lifecycleContext(t)); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if accepted.Load() != handled.Load() {
			t.Fatalf("accepted %d, handled %d", accepted.Load(), handled.Load())
		}
		if store.closes.Load() != 1 {
			t.Fatal("store not closed exactly once")
		}
	}
}
