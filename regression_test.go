package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// --- TypeNamer + SubscribeWithReplay -----------------------------------------

type namedReplayEvent struct {
	ID string
}

func (e namedReplayEvent) EventTypeName() string { return "named.replay.v1" }

type ptrNamedReplayEvent struct {
	ID string
}

func (e *ptrNamedReplayEvent) EventTypeName() string { return "ptr-named.replay.v1" }

type collidingEventA struct{}

func (collidingEventA) EventTypeName() string { return "review.collision" }

type collidingEventB struct{}

func (collidingEventB) EventTypeName() string { return "review.collision" }

// TestSubscribeWithReplayTypeNamer is a regression test: events implementing
// TypeNamer are persisted under their custom name, and SubscribeWithReplay
// must match that same name during replay. Previously it compared against the
// reflection name and silently replayed nothing.
func TestSubscribeWithReplayTypeNamer(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	Publish(bus, namedReplayEvent{ID: "1"})
	Publish(bus, namedReplayEvent{ID: "2"})

	// Simulate restart with a fresh bus on the same store
	bus2 := New(WithStore(store))
	var replayed []string
	err := SubscribeWithReplay(context.Background(), bus2, "typenamer-sub",
		func(e namedReplayEvent) { replayed = append(replayed, e.ID) })
	if err != nil {
		t.Fatal(err)
	}

	if len(replayed) != 2 {
		t.Fatalf("TypeNamer events were not replayed: got %d, want 2", len(replayed))
	}
	if replayed[0] != "1" || replayed[1] != "2" {
		t.Errorf("unexpected replay order: %v", replayed)
	}
}

// TestTypeNameOf covers value receivers, pointer receivers, and plain types.
func TestTypeNameOf(t *testing.T) {
	if got := typeNameOf(reflect.TypeOf(namedReplayEvent{})); got != "named.replay.v1" {
		t.Errorf("value receiver: got %q", got)
	}
	if got := typeNameOf(reflect.TypeOf(ptrNamedReplayEvent{})); got != "eventbus.ptrNamedReplayEvent" {
		t.Errorf("non-pointer value must match EventType(value): got %q", got)
	}
	if got := typeNameOf(reflect.TypeOf(&ptrNamedReplayEvent{})); got != "ptr-named.replay.v1" {
		t.Errorf("pointer event: got %q", got)
	}
	if got := typeNameOf(reflect.TypeOf(TestEvent{})); got != "eventbus.TestEvent" {
		t.Errorf("plain type: got %q", got)
	}
}

// TestSubscribeWithReplayPointerTypeNamer verifies pointer event types derive
// their stable name from a fresh value rather than invoking a promoted
// value-receiver method through a nil pointer.
func TestSubscribeWithReplayPointerTypeNamer(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))
	Publish(bus, &namedReplayEvent{ID: "pointer"})

	bus2 := New(WithStore(store))
	var received []*namedReplayEvent
	if err := SubscribeWithReplay(context.Background(), bus2, "pointer-namer",
		func(event *namedReplayEvent) { received = append(received, event) }); err != nil {
		t.Fatalf("SubscribeWithReplay: %v", err)
	}
	if len(received) != 1 || received[0].ID != "pointer" {
		t.Fatalf("pointer TypeNamer event was not replayed: %+v", received)
	}
}

func TestSubscribeRejectsPersistedTypeNameCollisions(t *testing.T) {
	tests := []struct {
		name      string
		collision func(*EventBus) error
	}{
		{
			name: "Subscribe",
			collision: func(bus *EventBus) error {
				return Subscribe(bus, func(collidingEventB) {})
			},
		},
		{
			name: "SubscribeContext",
			collision: func(bus *EventBus) error {
				return SubscribeContext(bus, func(context.Context, collidingEventB) {})
			},
		},
		{
			name: "SubscribeWithHandle",
			collision: func(bus *EventBus) error {
				_, err := SubscribeWithHandle(bus, func(collidingEventB) {})
				return err
			},
		},
		{
			name: "SubscribeContextWithHandle",
			collision: func(bus *EventBus) error {
				_, err := SubscribeContextWithHandle(bus, func(context.Context, collidingEventB) {})
				return err
			},
		},
		{
			name: "SubscribeWithReplay",
			collision: func(bus *EventBus) error {
				return SubscribeWithReplay(context.Background(), bus, "collision", func(collidingEventB) {})
			},
		},
		{
			name: "SubscribeContextWithReplay",
			collision: func(bus *EventBus) error {
				return SubscribeContextWithReplay(context.Background(), bus, "context-collision", func(context.Context, collidingEventB) {})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bus := New(WithStore(NewMemoryStore()))
			if err := Subscribe(bus, func(collidingEventA) {}); err != nil {
				t.Fatal(err)
			}
			err := tc.collision(bus)
			if err == nil || !strings.Contains(err.Error(), "persisted event type name") ||
				!strings.Contains(err.Error(), "distinct TypeNamer.EventTypeName") {
				t.Fatalf("expected persisted-name collision error, got %v", err)
			}
			if got := HandlerCount[collidingEventB](bus); got != 0 {
				t.Fatalf("collision registered %d handler(s)", got)
			}
		})
	}
}

func TestSubscribeAllowsTypeNameCollisionsWithoutPersistence(t *testing.T) {
	bus := New()
	var aCalls, bCalls int
	if err := Subscribe(bus, func(collidingEventA) { aCalls++ }); err != nil {
		t.Fatal(err)
	}
	if err := Subscribe(bus, func(collidingEventB) { bCalls++ }); err != nil {
		t.Fatalf("in-process subscriptions route by Go type and must not collide: %v", err)
	}

	Publish(bus, collidingEventA{})
	Publish(bus, collidingEventB{})
	if aCalls != 1 || bCalls != 1 {
		t.Fatalf("in-process collision delivery = (%d, %d), want (1, 1)", aCalls, bCalls)
	}
}

// --- Option ordering must not disable persistence -----------------------------

// TestWithStoreSurvivesHookOptions is a regression test: persistence used to
// be chained through the beforePublishCtx hook, so a WithBeforePublishContext
// option applied after WithStore silently disabled persistence.
func TestWithStoreSurvivesHookOptions(t *testing.T) {
	store := NewMemoryStore()
	var hookCalls int
	bus := New(
		WithStore(store),
		WithBeforePublishContext(func(ctx context.Context, tp reflect.Type, ev any) { hookCalls++ }),
	)

	Publish(bus, TestEvent{ID: 1})

	events, _, err := store.Read(context.Background(), OffsetOldest, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("persistence disabled by option ordering: got %d events, want 1", len(events))
	}
	if hookCalls != 1 {
		t.Errorf("user hook not called: got %d calls", hookCalls)
	}
}

// --- Per-event offset tracking -------------------------------------------------

// TestSubscribeWithReplaySavesOwnOffset verifies the live phase saves the
// offset of the exact event a handler processed, not a bus-global "last"
// offset that concurrent publishes of other types can advance.
func TestSubscribeWithReplaySavesOwnOffset(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	type otherEvent struct{ N int }

	if err := SubscribeWithReplay(context.Background(), bus, "own-offset-sub",
		func(e TestEvent) {}); err != nil {
		t.Fatal(err)
	}

	Publish(bus, TestEvent{ID: 1}) // offset 1
	Publish(bus, otherEvent{N: 2}) // offset 2 — must NOT be saved for our sub

	saved, err := store.LoadOffset(context.Background(), "own-offset-sub")
	if err != nil {
		t.Fatal(err)
	}
	if saved != Offset("00000000000000000001") {
		t.Errorf("subscription saved a foreign offset: got %s, want 00000000000000000001", saved)
	}
}

// TestSubscribeWithReplaySaveOffsetErrorReported verifies SaveOffset failures
// are surfaced through the PersistenceErrorHandler rather than dropped.
func TestSubscribeWithReplaySaveOffsetErrorReported(t *testing.T) {
	store := NewMemoryStore()
	failing := &failingSubscriptionStore{}

	var mu sync.Mutex
	var reported []error
	bus := New(
		WithStore(store),
		WithSubscriptionStore(failing),
		WithPersistenceErrorHandler(func(event any, tp reflect.Type, err error) {
			mu.Lock()
			reported = append(reported, err)
			mu.Unlock()
		}),
	)

	if err := SubscribeWithReplay(context.Background(), bus, "failing-sub",
		func(e TestEvent) {}); err != nil {
		t.Fatal(err)
	}

	Publish(bus, TestEvent{ID: 1})

	mu.Lock()
	defer mu.Unlock()
	if len(reported) == 0 {
		t.Fatal("SaveOffset failure was not reported to PersistenceErrorHandler")
	}
	if !strings.Contains(reported[0].Error(), "failing-sub") {
		t.Errorf("error should name the subscription: %v", reported[0])
	}
}

type failingSubscriptionStore struct{}

func (f *failingSubscriptionStore) SaveOffset(ctx context.Context, id string, off Offset) error {
	return fmt.Errorf("disk on fire")
}

func (f *failingSubscriptionStore) LoadOffset(ctx context.Context, id string) (Offset, error) {
	return OffsetOldest, nil
}

// TestOffsetFromContextWithoutStore verifies handlers on a non-persistent bus
// see no offset.
func TestOffsetFromContextWithoutStore(t *testing.T) {
	bus := New()
	var sawOffset bool
	if err := SubscribeContext(bus, func(ctx context.Context, e TestEvent) {
		_, sawOffset = OffsetFromContext(ctx)
	}); err != nil {
		t.Fatal(err)
	}
	Publish(bus, TestEvent{ID: 1})
	if sawOffset {
		t.Error("no store configured, but handler context carried an offset")
	}
}

// TestOffsetFromContextOnPersistFailure verifies a failed Append attaches no
// offset (so SubscribeWithReplay will not save one).
func TestOffsetFromContextOnPersistFailure(t *testing.T) {
	bus := New(WithStore(&errorStore{failAppend: true}))
	var sawOffset bool
	if err := SubscribeContext(bus, func(ctx context.Context, e TestEvent) {
		_, sawOffset = OffsetFromContext(ctx)
	}); err != nil {
		t.Fatal(err)
	}
	Publish(bus, TestEvent{ID: 1})
	if sawOffset {
		t.Error("Append failed, but handler context carried an offset")
	}
}

// --- Filter validation ----------------------------------------------------------

// TestWithFilterTypeMismatchRejected verifies a predicate whose parameter type
// does not match the subscription's event type is rejected at Subscribe time.
// Previously it was silently ignored and the handler received ALL events.
func TestWithFilterTypeMismatchRejected(t *testing.T) {
	bus := New()

	err := Subscribe(bus, func(e TestEvent) {}, WithFilter(func(s string) bool { return false }))
	if err == nil {
		t.Fatal("mismatched filter predicate must be rejected at Subscribe time")
	}
	if !strings.Contains(err.Error(), "filter predicate") {
		t.Errorf("unexpected error: %v", err)
	}

	// Same for SubscribeContext
	err = SubscribeContext(bus, func(ctx context.Context, e TestEvent) {}, WithFilter(func(s string) bool { return false }))
	if err == nil {
		t.Fatal("mismatched filter predicate must be rejected at SubscribeContext time")
	}
}

// --- Nil bus ---------------------------------------------------------------------

func TestPublishNilBusPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Publish on nil bus must panic")
		}
		if !strings.Contains(fmt.Sprint(r), "nil bus") {
			t.Errorf("panic message should mention nil bus: %v", r)
		}
	}()
	Publish(nil, TestEvent{ID: 1})
}

func TestUnsubscribeNilBus(t *testing.T) {
	err := Unsubscribe[TestEvent](nil, func(e TestEvent) {})
	if err == nil {
		t.Fatal("Unsubscribe on nil bus must return an error")
	}
}

// --- WithUpcast panics on invalid registration ------------------------------------

func TestWithUpcastPanicsOnInvalidRegistration(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("WithUpcast with a self-referencing upcast must panic")
		}
	}()
	New(WithUpcast("same.type", "same.type", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "same.type", nil
	}))
}

// --- Once + Async + cancelled context ----------------------------------------------

// TestOnceAsyncNotConsumedByCancelledContext verifies a cancelled context
// cannot consume a Once handler without executing it: either the handler is
// skipped entirely (still armed) or it runs. Previously the CAS consumed the
// handler and the goroutine then dropped it silently.
func TestOnceAsyncNotConsumedByCancelledContext(t *testing.T) {
	bus := New()
	ran := make(chan struct{}, 1)

	if err := Subscribe(bus, func(e TestEvent) {
		ran <- struct{}{}
	}, Once(), Async()); err != nil {
		t.Fatal(err)
	}

	// Publish with an already-cancelled context: the handler must NOT be
	// consumed (top-of-loop ctx check runs before the Once CAS).
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	PublishContext(bus, ctx, TestEvent{ID: 1})
	bus.Wait()

	select {
	case <-ran:
		t.Fatal("handler must not run with a cancelled context")
	default:
	}

	// The Once slot must still be armed: a live publish must run it.
	Publish(bus, TestEvent{ID: 2})
	bus.Wait()

	select {
	case <-ran:
		// executed exactly once
	default:
		t.Fatal("Once handler was consumed by the cancelled publish and never ran")
	}
}
