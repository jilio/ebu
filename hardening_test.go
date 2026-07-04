package eventbus

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- ULID event IDs ---

func TestNewEventIDFormat(t *testing.T) {
	id := NewEventID()
	if len(id) != 26 {
		t.Fatalf("expected 26 characters, got %d (%q)", len(id), id)
	}
	for i, c := range id {
		if !strings.ContainsRune(crockford32, c) {
			t.Errorf("character %d (%q) not in Crockford base32 alphabet", i, c)
		}
	}
	// 128 bits in 130 bits of space: the first character's two high bits are
	// always zero, so it is at most '7'.
	if id[0] > '7' {
		t.Errorf("first character %q exceeds '7'", id[0])
	}
}

func TestNewEventIDTimestamp(t *testing.T) {
	before := time.Now().UnixMilli()
	id := NewEventID()
	after := time.Now().UnixMilli()

	// The first 10 characters encode the 48-bit millisecond timestamp.
	var ms int64
	for _, c := range id[:10] {
		ms = ms<<5 | int64(strings.IndexRune(crockford32, c))
	}
	if ms < before || ms > after {
		t.Errorf("decoded timestamp %d outside publish window [%d, %d]", ms, before, after)
	}
}

func TestNewEventIDUnique(t *testing.T) {
	const goroutines = 4
	const perGoroutine = 2500 // enough to force several entropy buffer refills

	var mu sync.Mutex
	seen := make(map[string]bool, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := make([]string, perGoroutine)
			for i := range ids {
				ids[i] = NewEventID()
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range ids {
				if seen[id] {
					t.Errorf("duplicate ID %q", id)
				}
				seen[id] = true
			}
		}()
	}
	wg.Wait()
}

func TestNewEventIDSortsByTime(t *testing.T) {
	first := NewEventID()
	time.Sleep(2 * time.Millisecond)
	second := NewEventID()
	if !(first < second) {
		t.Errorf("expected %q < %q (later IDs must sort after earlier ones)", first, second)
	}
}

// --- Event envelope: ID, Origin, Metadata ---

type envelopeEvent struct {
	Value int
}

func TestPublishAssignsEnvelope(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	if bus.OriginID() == "" {
		t.Fatal("expected bus to have an origin ID")
	}

	Publish(bus, envelopeEvent{Value: 1})

	events, _, err := store.Read(context.Background(), OffsetOldest, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("expected 1 stored event, got %d (err=%v)", len(events), err)
	}
	if len(events[0].ID) != 26 {
		t.Errorf("expected a 26-char ULID event ID, got %q", events[0].ID)
	}
	if events[0].Origin != bus.OriginID() {
		t.Errorf("expected origin %q, got %q", bus.OriginID(), events[0].Origin)
	}
	if events[0].Metadata != nil {
		t.Errorf("expected no metadata, got %v", events[0].Metadata)
	}
}

func TestDistinctBusesHaveDistinctOrigins(t *testing.T) {
	if New().OriginID() == New().OriginID() {
		t.Error("two buses share an origin ID")
	}
}

func TestContextWithMetadataPersisted(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	md := map[string]string{"correlation_id": "req-42", "tenant": "acme"}
	ctx := ContextWithMetadata(context.Background(), md)

	var handlerMD map[string]string
	var handlerOK bool
	SubscribeContext(bus, func(hctx context.Context, e envelopeEvent) {
		handlerMD, handlerOK = MetadataFromContext(hctx)
	})

	PublishContext(bus, ctx, envelopeEvent{Value: 1})

	events, _, _ := store.Read(context.Background(), OffsetOldest, 0)
	if len(events) != 1 {
		t.Fatalf("expected 1 stored event, got %d", len(events))
	}
	if events[0].Metadata["correlation_id"] != "req-42" || events[0].Metadata["tenant"] != "acme" {
		t.Errorf("stored metadata mismatch: %v", events[0].Metadata)
	}
	if !handlerOK || handlerMD["correlation_id"] != "req-42" {
		t.Errorf("handler metadata mismatch: ok=%v md=%v", handlerOK, handlerMD)
	}
}

func TestContextWithMetadataEmptyIsNoop(t *testing.T) {
	ctx := context.Background()
	if ContextWithMetadata(ctx, nil) != ctx {
		t.Error("nil metadata should return the context unchanged")
	}
	if ContextWithMetadata(ctx, map[string]string{}) != ctx {
		t.Error("empty metadata should return the context unchanged")
	}
	if _, ok := MetadataFromContext(ctx); ok {
		t.Error("expected no metadata on a bare context")
	}
}

func TestEventIDFromContext(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	var handlerID string
	var handlerOK bool
	SubscribeContext(bus, func(hctx context.Context, e envelopeEvent) {
		handlerID, handlerOK = EventIDFromContext(hctx)
	})

	Publish(bus, envelopeEvent{Value: 1})

	events, _, _ := store.Read(context.Background(), OffsetOldest, 0)
	if len(events) != 1 {
		t.Fatalf("expected 1 stored event, got %d", len(events))
	}
	if !handlerOK {
		t.Fatal("expected handler to see the event ID")
	}
	if handlerID != events[0].ID {
		t.Errorf("handler saw ID %q, store recorded %q", handlerID, events[0].ID)
	}
}

func TestEventIDFromContextWithoutStore(t *testing.T) {
	bus := New()
	var ok bool
	SubscribeContext(bus, func(hctx context.Context, e envelopeEvent) {
		_, ok = EventIDFromContext(hctx)
	})
	Publish(bus, envelopeEvent{Value: 1})
	if ok {
		t.Error("expected no event ID on a bus without a store")
	}
}

func TestEventIDPresentWhenPersistFails(t *testing.T) {
	// Best-effort mode: the event is delivered despite the Append failure,
	// and the ID — which identifies the publish, not the storage outcome —
	// must still be visible to handlers.
	bus := New(WithStore(&errorStore{failAppend: true}))
	var id string
	var ok bool
	SubscribeContext(bus, func(hctx context.Context, e envelopeEvent) {
		id, ok = EventIDFromContext(hctx)
	})
	Publish(bus, envelopeEvent{Value: 1})
	if !ok || len(id) != 26 {
		t.Errorf("expected a ULID despite persist failure, got ok=%v id=%q", ok, id)
	}
	if _, offsetOK := OffsetFromContext(context.Background()); offsetOK {
		t.Error("bare context must not carry an offset")
	}
}

// --- Strict persistence and TryPublish ---

func TestStrictPersistenceSkipsDeliveryOnAppendFailure(t *testing.T) {
	var reported error
	bus := New(
		WithStore(&errorStore{failAppend: true}),
		WithStrictPersistence(),
		WithPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
			reported = err
		}),
	)

	called := false
	Subscribe(bus, func(e envelopeEvent) { called = true })

	afterHookFired := false
	bus.SetAfterPublishHook(func(eventType reflect.Type, event any) { afterHookFired = true })

	Publish(bus, envelopeEvent{Value: 1})

	if called {
		t.Error("handler must not run when strict persistence fails")
	}
	if reported == nil {
		t.Error("expected the failure to reach the PersistenceErrorHandler")
	}
	if !afterHookFired {
		t.Error("after-publish hook should still fire on a strict-mode failure")
	}
}

func TestStrictPersistenceDeliversOnSuccess(t *testing.T) {
	bus := New(WithStore(NewMemoryStore()), WithStrictPersistence())
	called := false
	Subscribe(bus, func(e envelopeEvent) { called = true })
	Publish(bus, envelopeEvent{Value: 1})
	if !called {
		t.Error("handler should run when persistence succeeds in strict mode")
	}
}

func TestBestEffortDeliversDespiteAppendFailure(t *testing.T) {
	bus := New(WithStore(&errorStore{failAppend: true}))
	called := false
	Subscribe(bus, func(e envelopeEvent) { called = true })
	Publish(bus, envelopeEvent{Value: 1})
	if !called {
		t.Error("best-effort mode must deliver despite the persist failure")
	}
}

func TestTryPublishReturnsAppendError(t *testing.T) {
	bus := New(WithStore(&errorStore{failAppend: true}))
	if err := TryPublish(bus, envelopeEvent{Value: 1}); err == nil {
		t.Error("expected TryPublish to return the append error")
	}
}

func TestTryPublishContextStrictSkipsDelivery(t *testing.T) {
	bus := New(WithStore(&errorStore{failAppend: true}), WithStrictPersistence())
	called := false
	Subscribe(bus, func(e envelopeEvent) { called = true })
	err := TryPublishContext(bus, context.Background(), envelopeEvent{Value: 1})
	if err == nil {
		t.Error("expected TryPublishContext to return the append error")
	}
	if called {
		t.Error("strict mode must not deliver on persist failure")
	}
}

func TestTryPublishNilOnSuccessAndWithoutStore(t *testing.T) {
	if err := TryPublish(New(), envelopeEvent{Value: 1}); err != nil {
		t.Errorf("expected nil without a store, got %v", err)
	}
	if err := TryPublish(New(WithStore(NewMemoryStore())), envelopeEvent{Value: 1}); err != nil {
		t.Errorf("expected nil on successful persist, got %v", err)
	}
}

type unmarshalableEvent struct {
	Ch chan int // channels cannot be marshaled to JSON
}

func TestTryPublishReturnsMarshalErrorAndStrictSkips(t *testing.T) {
	bus := New(WithStore(NewMemoryStore()), WithStrictPersistence())
	called := false
	Subscribe(bus, func(e unmarshalableEvent) { called = true })
	if err := TryPublish(bus, unmarshalableEvent{Ch: make(chan int)}); err == nil {
		t.Error("expected a marshal error")
	}
	if called {
		t.Error("strict mode must not deliver an event that could not be marshaled")
	}
}

// --- Subscription handles ---

type handleEvent struct {
	Value int
}

func TestSubscribeWithHandle(t *testing.T) {
	bus := New()
	var got []int
	sub, err := SubscribeWithHandle(bus, func(e handleEvent) { got = append(got, e.Value) })
	if err != nil {
		t.Fatalf("SubscribeWithHandle: %v", err)
	}

	Publish(bus, handleEvent{Value: 1})
	sub.Unsubscribe()
	Publish(bus, handleEvent{Value: 2})
	sub.Unsubscribe() // idempotent

	if len(got) != 1 || got[0] != 1 {
		t.Errorf("expected exactly [1], got %v", got)
	}
}

func TestSubscribeWithHandleDistinguishesIdenticalClosures(t *testing.T) {
	// Two closures from the same function literal share a code pointer, so
	// Unsubscribe cannot tell them apart — handles must.
	bus := New()
	counts := make([]int, 2)
	var subs []*Subscription
	for i := 0; i < 2; i++ {
		i := i
		sub, err := SubscribeWithHandle(bus, func(e handleEvent) { counts[i]++ })
		if err != nil {
			t.Fatalf("SubscribeWithHandle: %v", err)
		}
		subs = append(subs, sub)
	}

	// Remove the SECOND registration; code-pointer matching would remove the
	// first one instead.
	subs[1].Unsubscribe()
	Publish(bus, handleEvent{Value: 1})

	if counts[0] != 1 {
		t.Errorf("first subscription should survive, count=%d", counts[0])
	}
	if counts[1] != 0 {
		t.Errorf("second subscription should be removed, count=%d", counts[1])
	}
}

func TestSubscribeContextWithHandle(t *testing.T) {
	bus := New()
	called := false
	sub, err := SubscribeContextWithHandle(bus, func(ctx context.Context, e handleEvent) { called = true })
	if err != nil {
		t.Fatalf("SubscribeContextWithHandle: %v", err)
	}
	Publish(bus, handleEvent{Value: 1})
	if !called {
		t.Error("expected context handler to run")
	}
	sub.Unsubscribe()
	called = false
	Publish(bus, handleEvent{Value: 2})
	if called {
		t.Error("expected handler to be removed")
	}
}

func TestSubscribeWithHandleValidation(t *testing.T) {
	bus := New()

	if _, err := SubscribeWithHandle[handleEvent](nil, func(e handleEvent) {}); err == nil {
		t.Error("expected error for nil bus")
	}
	if _, err := SubscribeWithHandle[handleEvent](bus, nil); err == nil {
		t.Error("expected error for nil handler")
	}
	if _, err := SubscribeWithHandle(bus, func(e handleEvent) {}, nil); err == nil {
		t.Error("expected error for nil option")
	}
	if _, err := SubscribeWithHandle(bus, func(e handleEvent) {},
		WithFilter(func(other envelopeEvent) bool { return true })); err == nil {
		t.Error("expected error for mismatched filter type")
	}

	if _, err := SubscribeContextWithHandle[handleEvent](nil, func(ctx context.Context, e handleEvent) {}); err == nil {
		t.Error("expected error for nil bus (context variant)")
	}
	if _, err := SubscribeContextWithHandle[handleEvent](bus, nil); err == nil {
		t.Error("expected error for nil handler (context variant)")
	}
	if _, err := SubscribeContextWithHandle(bus, func(ctx context.Context, e handleEvent) {}, nil); err == nil {
		t.Error("expected error for nil option (context variant)")
	}
}

func TestUnsubscribeAfterClearIsNoop(t *testing.T) {
	bus := New()
	sub, err := SubscribeWithHandle(bus, func(e handleEvent) {})
	if err != nil {
		t.Fatalf("SubscribeWithHandle: %v", err)
	}
	Clear[handleEvent](bus)
	sub.Unsubscribe() // must not panic or affect other state
	if HasHandlers[handleEvent](bus) {
		t.Error("expected no handlers after Clear")
	}
}
