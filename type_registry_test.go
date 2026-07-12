package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type registryEventA struct{}

func (registryEventA) EventTypeName() string { return "test.registry.collision" }

type registryEventB struct{}

func (registryEventB) EventTypeName() string { return "test.registry.collision" }

type registrySource struct{}

func (registrySource) EventTypeName() string { return "test.registry.source" }

type instanceNamedEvent struct {
	Name string
}

func (e instanceNamedEvent) EventTypeName() string { return e.Name }

type blockingClaimStore struct {
	*MemoryStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingClaimStore) Append(ctx context.Context, event *Event) (Offset, error) {
	s.once.Do(func() {
		close(s.started)
		<-s.release
	})
	return s.MemoryStore.Append(ctx, event)
}

// ambiguousAppendStore models the failure mode of a remote store that commits
// an append but loses the acknowledgement. The caller receives an error even
// though the event is already durable and visible to readers.
type ambiguousAppendStore struct {
	*MemoryStore
	failFirst sync.Once
}

func (s *ambiguousAppendStore) Append(ctx context.Context, event *Event) (Offset, error) {
	offset, err := s.MemoryStore.Append(ctx, event)
	if err != nil {
		return offset, err
	}
	ambiguous := false
	s.failFirst.Do(func() { ambiguous = true })
	if ambiguous {
		return offset, errors.New("append acknowledgement timed out after commit")
	}
	return offset, nil
}

func TestPersistedTypeRegistryReservations(t *testing.T) {
	aType := reflect.TypeOf(registryEventA{})
	bType := reflect.TypeOf(registryEventB{})

	t.Run("request conflicts are atomic", func(t *testing.T) {
		registry := newPersistedTypeRegistry()
		if _, err := registry.reserve(
			persistedTypeSpec{name: "same", eventType: aType},
			persistedTypeSpec{name: "same", eventType: bType},
		); err == nil {
			t.Fatal("same name for distinct request types was accepted")
		}
		if len(registry.byName) != 0 || len(registry.byType) != 0 {
			t.Fatalf("conflicting request partially mutated registry: %v %v", registry.byName, registry.byType)
		}

		if _, err := registry.reserve(
			persistedTypeSpec{name: "one", eventType: aType},
			persistedTypeSpec{name: "two", eventType: aType},
		); err == nil || !strings.Contains(err.Error(), "pure, immutable") {
			t.Fatalf("same type with different names error = %v", err)
		}
	})

	t.Run("pending rollback and sticky commit", func(t *testing.T) {
		registry := newPersistedTypeRegistry()
		first, err := registry.reserve(persistedTypeSpec{name: "same", eventType: aType})
		if err != nil {
			t.Fatal(err)
		}
		same, err := registry.reserve(persistedTypeSpec{name: "same", eventType: aType})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := registry.reserve(persistedTypeSpec{name: "same", eventType: bType}); err == nil {
			t.Fatal("conflict was accepted while the first type was pending")
		}
		first.Rollback()
		first.Rollback()
		if len(registry.byName) != 1 {
			t.Fatal("rollback removed another pending reservation")
		}
		same.Commit()
		same.Rollback()
		if claim := registry.byName["same"]; claim == nil || !claim.sticky || claim.pending != 0 {
			t.Fatalf("committed claim = %#v", claim)
		}
		if _, err := registry.reserve(persistedTypeSpec{name: "other", eventType: aType}); err == nil || !strings.Contains(err.Error(), "pure, immutable") {
			t.Fatalf("reverse type conflict error = %v", err)
		}
	})

	t.Run("identical endpoints deduplicate", func(t *testing.T) {
		registry := newPersistedTypeRegistry()
		reservation, err := registry.reserve(
			persistedTypeSpec{name: "same", eventType: aType},
			persistedTypeSpec{name: "same", eventType: aType},
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(reservation.claims) != 1 || reservation.claims[0].pending != 1 {
			t.Fatalf("duplicate reservation = %#v", reservation.claims)
		}
		reservation.Rollback()
		if len(registry.byName) != 0 {
			t.Fatal("rolled-back registry retained an uncommitted claim")
		}
	})

	t.Run("concurrent same type", func(t *testing.T) {
		registry := newPersistedTypeRegistry()
		var wg sync.WaitGroup
		for range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				reservation, err := registry.reserve(persistedTypeSpec{name: "same", eventType: aType})
				if err != nil {
					t.Errorf("reserve: %v", err)
					return
				}
				reservation.Commit()
			}()
		}
		wg.Wait()
		if claim := registry.byName["same"]; claim == nil || !claim.sticky || claim.pending != 0 {
			t.Fatalf("concurrent claim = %#v", claim)
		}
	})

	// A nil/no-op reservation is deliberately safe for deferred cleanup on a
	// non-persistent bus.
	var noop *persistedTypeReservation
	noop.Commit()
	noop.Rollback()
}

func TestDeferredPersistedTypeRegistryActivation(t *testing.T) {
	aType := reflect.TypeOf(registryEventA{})
	bType := reflect.TypeOf(registryEventB{})

	t.Run("committed and pending claims activate atomically", func(t *testing.T) {
		registry := newDeferredPersistedTypeRegistry()
		committed, err := registry.reserve(persistedTypeSpec{name: "committed", eventType: aType})
		if err != nil {
			t.Fatal(err)
		}
		committed.Commit()
		pending, err := registry.reserve(persistedTypeSpec{name: "pending", eventType: bType})
		if err != nil {
			t.Fatal(err)
		}

		if registry.isActive() {
			t.Fatal("deferred registry activated before persistence")
		}
		if err := registry.activate(); err != nil {
			t.Fatal(err)
		}
		if !registry.isActive() {
			t.Fatal("registry did not activate")
		}
		if err := registry.activate(); err != nil {
			t.Fatalf("idempotent activation: %v", err)
		}
		if claim := registry.byName["committed"]; claim == nil || !claim.sticky || claim.pending != 0 {
			t.Fatalf("committed claim = %#v", claim)
		}
		if claim := registry.byName["pending"]; claim == nil || claim.sticky || claim.pending != 1 {
			t.Fatalf("pending claim = %#v", claim)
		}
		pending.Commit()
		if claim := registry.byName["pending"]; claim == nil || !claim.sticky || claim.pending != 0 {
			t.Fatalf("finished pending claim = %#v", claim)
		}
	})

	t.Run("rollback before activation leaves no claim", func(t *testing.T) {
		registry := newDeferredPersistedTypeRegistry()
		reservation, err := registry.reserve(persistedTypeSpec{name: "rolled-back", eventType: aType})
		if err != nil {
			t.Fatal(err)
		}
		reservation.Rollback()
		if err := registry.activate(); err != nil {
			t.Fatal(err)
		}
		if len(registry.byName) != 0 || len(registry.byType) != 0 {
			t.Fatalf("rollback survived activation: %v %v", registry.byName, registry.byType)
		}
	})

	t.Run("committed provisional claims deduplicate under churn", func(t *testing.T) {
		registry := newDeferredPersistedTypeRegistry()
		for range 1000 {
			reservation, err := registry.reserve(persistedTypeSpec{name: "same", eventType: aType})
			if err != nil {
				t.Fatal(err)
			}
			reservation.Commit()
		}
		if len(registry.deferred) != 1 || len(registry.deferredSet) != 1 {
			t.Fatalf("provisional claims grew with churn: specs=%d set=%d", len(registry.deferred), len(registry.deferredSet))
		}
		if err := registry.activate(); err != nil {
			t.Fatal(err)
		}
		if claim := registry.byName["same"]; claim == nil || !claim.sticky || claim.pending != 0 {
			t.Fatalf("deduplicated claim = %#v", claim)
		}
	})

	t.Run("failed activation is atomic and retryable", func(t *testing.T) {
		registry := newDeferredPersistedTypeRegistry()
		first, err := registry.reserve(persistedTypeSpec{name: "collision", eventType: aType})
		if err != nil {
			t.Fatal(err)
		}
		first.Commit()
		conflict, err := registry.reserve(persistedTypeSpec{name: "collision", eventType: bType})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.activate(); err == nil || !strings.Contains(err.Error(), "persisted event type name") {
			t.Fatalf("activation conflict = %v", err)
		}
		if registry.isActive() || len(registry.byName) != 0 || len(registry.byType) != 0 {
			t.Fatalf("failed activation mutated registry: active=%v names=%v types=%v", registry.isActive(), registry.byName, registry.byType)
		}
		conflict.Rollback()
		if err := registry.activate(); err != nil {
			t.Fatalf("activation after conflicting rollback: %v", err)
		}
		if claim := registry.byName["collision"]; claim == nil || !claim.sticky {
			t.Fatalf("surviving claim = %#v", claim)
		}
	})
}

func TestPersistentCustomOptionTypeRegistrationOrdering(t *testing.T) {
	subscribeA := Option(func(bus *EventBus) {
		if err := Subscribe(bus, func(registryEventA) {}); err != nil {
			panic(err)
		}
	})
	subscribeB := Option(func(bus *EventBus) {
		if err := Subscribe(bus, func(registryEventB) {}); err != nil {
			panic(err)
		}
	})

	for _, tc := range []struct {
		name string
		opts func(EventStore) []Option
	}{
		{
			name: "store before typed registration",
			opts: func(store EventStore) []Option { return []Option{WithStore(store), subscribeA} },
		},
		{
			name: "store after typed registration",
			opts: func(store EventStore) []Option { return []Option{subscribeA, WithStore(store)} },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryStore()
			bus := New(tc.opts(store)...)
			if !bus.persistedTypes.isActive() {
				t.Fatal("persistent bus retained a deferred registry")
			}
			claim := bus.persistedTypes.byName["test.registry.collision"]
			if claim == nil || claim.eventType != reflect.TypeOf(registryEventA{}) || !claim.sticky {
				t.Fatalf("activated claim = %#v", claim)
			}
			if decoderType := bus.followTypes["test.registry.collision"]; decoderType != reflect.TypeOf(registryEventA{}) {
				t.Fatalf("follow decoder type = %v", decoderType)
			}
		})
	}

	for _, tc := range []struct {
		name string
		opts func(EventStore) []Option
	}{
		{
			name: "collision with store first",
			opts: func(store EventStore) []Option {
				return []Option{WithStore(store), subscribeA, subscribeB}
			},
		},
		{
			name: "collision with store last",
			opts: func(store EventStore) []Option {
				return []Option{subscribeA, subscribeB, WithStore(store)}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				panicValue := recover()
				if panicValue == nil || !strings.Contains(fmt.Sprint(panicValue), "cannot enable persistence") {
					t.Fatalf("construction panic = %v", panicValue)
				}
			}()
			_ = New(tc.opts(NewMemoryStore())...)
		})
	}

	t.Run("nonpersistent collisions remain type-routed and decoder is ambiguous", func(t *testing.T) {
		bus := New(subscribeA, subscribeB)
		if bus.persistedTypes.isActive() {
			t.Fatal("nonpersistent registry was activated")
		}
		if decoderType, ok := bus.followTypes["test.registry.collision"]; !ok || decoderType != nil {
			t.Fatalf("ambiguous decoder marker = %v, %v", decoderType, ok)
		}
		if _, ok := bus.followDecoders["test.registry.collision"]; ok {
			t.Fatal("ambiguous decoder silently retained one conflicting type")
		}
		// A later identical registration must not repopulate an ambiguous slot.
		if err := Subscribe(bus, func(registryEventA) {}); err != nil {
			t.Fatal(err)
		}

		var aCount, bCount int
		if err := Subscribe(bus, func(registryEventA) { aCount++ }); err != nil {
			t.Fatal(err)
		}
		if err := Subscribe(bus, func(registryEventB) { bCount++ }); err != nil {
			t.Fatal(err)
		}
		Publish(bus, registryEventA{})
		Publish(bus, registryEventB{})
		if aCount != 1 || bCount != 1 {
			t.Fatalf("nonpersistent delivery counts = %d, %d", aCount, bCount)
		}

		defer func() {
			panicValue := recover()
			if panicValue == nil || !strings.Contains(fmt.Sprint(panicValue), "cannot enable persistence") {
				t.Fatalf("late persistence panic = %v", panicValue)
			}
			if bus.IsPersistent() {
				t.Fatal("failed activation exposed a store")
			}
		}()
		WithStore(NewMemoryStore())(bus)
	})

	t.Run("active decoder install returns a conflict", func(t *testing.T) {
		bus := New(WithStore(NewMemoryStore()))
		if err := installFollowDecoder[registryEventA](bus); err != nil {
			t.Fatal(err)
		}
		if err := registerFollowDecoder[registryEventB](bus); err == nil || !strings.Contains(err.Error(), "persisted event type name") {
			t.Fatalf("decoder conflict = %v", err)
		}
		if decoderType := bus.followTypes["test.registry.collision"]; decoderType != reflect.TypeOf(registryEventA{}) {
			t.Fatalf("conflict replaced first decoder with %v", decoderType)
		}
		if len(bus.persistedTypes.byName) != 0 {
			t.Fatalf("failed decoder install retained type claim: %v", bus.persistedTypes.byName)
		}
	})
}

func TestPersistedTypeClaimsAcrossPublishSubscribeAndUpcast(t *testing.T) {
	t.Run("instance-dependent TypeNamer is rejected before append", func(t *testing.T) {
		store := NewMemoryStore()
		var reported error
		bus := New(
			WithStore(store),
			WithPersistenceErrorHandler(func(_ any, _ reflect.Type, errorValue error) { reported = errorValue }),
		)
		event := instanceNamedEvent{Name: "dynamic.name"}
		if EventType(event) != "dynamic.name" {
			t.Fatal("public EventType should reflect the concrete value")
		}
		if err := TryPublish(bus, event); err == nil || !strings.Contains(err.Error(), "pure, immutable") {
			t.Fatalf("instance-dependent name error = %v", err)
		}
		events, _, _ := store.Read(context.Background(), OffsetOldest, 0)
		if len(events) != 0 {
			t.Fatalf("instance-dependent name appended %d events", len(events))
		}
		if reported == nil {
			t.Fatal("instance-dependent name error was not reported")
		}
	})

	t.Run("publish blocks colliding publish", func(t *testing.T) {
		store := NewMemoryStore()
		var reports int
		bus := New(
			WithStore(store),
			WithPersistenceErrorHandler(func(any, reflect.Type, error) { reports++ }),
		)
		if err := TryPublish(bus, registryEventA{}); err != nil {
			t.Fatal(err)
		}
		if err := TryPublish(bus, registryEventB{}); err == nil || !strings.Contains(err.Error(), "persisted event type name") {
			t.Fatalf("colliding publish error = %v", err)
		}
		events, _, _ := store.Read(context.Background(), OffsetOldest, 0)
		if len(events) != 1 {
			t.Fatalf("colliding publish appended %d events, want 1", len(events))
		}
		if reports != 1 {
			t.Fatalf("colliding publish reports = %d, want 1", reports)
		}
	})

	t.Run("pending append blocks colliding type before storage", func(t *testing.T) {
		store := &blockingClaimStore{
			MemoryStore: NewMemoryStore(),
			started:     make(chan struct{}),
			release:     make(chan struct{}),
		}
		bus := New(WithStore(store))
		firstDone := make(chan error, 1)
		go func() { firstDone <- TryPublish(bus, registryEventA{}) }()
		select {
		case <-store.started:
		case <-time.After(5 * time.Second):
			t.Fatal("first append did not block")
		}
		if err := TryPublish(bus, registryEventB{}); err == nil || !strings.Contains(err.Error(), "persisted event type name") {
			t.Fatalf("pending type conflict error = %v", err)
		}
		close(store.release)
		if err := <-firstDone; err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failed append keeps conservative claim", func(t *testing.T) {
		store := &errorStore{failAppend: true}
		bus := New(WithStore(store))
		if err := TryPublish(bus, registryEventA{}); err == nil {
			t.Fatal("expected append failure")
		}
		store.failAppend = false
		if err := TryPublish(bus, registryEventB{}); err == nil || !strings.Contains(err.Error(), "persisted event type name") {
			t.Fatalf("failed append released its conservative type claim: %v", err)
		}
		if err := TryPublish(bus, registryEventA{}); err != nil {
			t.Fatalf("same type could not retry after failed append: %v", err)
		}
	})

	t.Run("ambiguous append cannot be reinterpreted by colliding type", func(t *testing.T) {
		store := &ambiguousAppendStore{MemoryStore: NewMemoryStore()}
		bus := New(WithStore(store))
		if err := TryPublish(bus, registryEventA{}); err == nil || !strings.Contains(err.Error(), "after commit") {
			t.Fatalf("ambiguous append error = %v", err)
		}

		events, _, err := store.Read(context.Background(), OffsetOldest, 0)
		if err != nil || len(events) != 1 || events[0].Type != "test.registry.collision" {
			t.Fatalf("durable ambiguous event = %#v, err=%v", events, err)
		}
		if err := TryPublish(bus, registryEventB{}); err == nil || !strings.Contains(err.Error(), "persisted event type name") {
			t.Fatalf("colliding type reinterpreted an ambiguously committed append: %v", err)
		}
		if err := TryPublish(bus, registryEventA{}); err != nil {
			t.Fatalf("same type could not retry ambiguous append: %v", err)
		}
		events, _, err = store.Read(context.Background(), OffsetOldest, 0)
		if err != nil || len(events) != 2 {
			t.Fatalf("same-type retry stored %d events, err=%v; want 2", len(events), err)
		}
	})

	for _, tc := range []struct {
		name  string
		first func(*EventBus) error
		then  func(*EventBus) error
	}{
		{
			name:  "subscribe then typed upcast target",
			first: func(bus *EventBus) error { return Subscribe(bus, func(registryEventA) {}) },
			then: func(bus *EventBus) error {
				return RegisterUpcast(bus, func(registrySource) registryEventB { return registryEventB{} })
			},
		},
		{
			name: "typed upcast target then subscribe",
			first: func(bus *EventBus) error {
				return RegisterUpcast(bus, func(registrySource) registryEventB { return registryEventB{} })
			},
			then: func(bus *EventBus) error { return Subscribe(bus, func(registryEventA) {}) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := New(WithStore(NewMemoryStore()))
			if err := tc.first(bus); err != nil {
				t.Fatal(err)
			}
			if err := tc.then(bus); err == nil || !strings.Contains(err.Error(), "persisted event type name") {
				t.Fatalf("collision error = %v", err)
			}
		})
	}

	t.Run("non-persistent bus remains type-routed", func(t *testing.T) {
		bus := New()
		if err := Subscribe(bus, func(registryEventA) {}); err != nil {
			t.Fatal(err)
		}
		if err := Subscribe(bus, func(registryEventB) {}); err != nil {
			t.Fatal(err)
		}
		if err := RegisterUpcast(bus, func(registrySource) registryEventB { return registryEventB{} }); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failed typed upcast registration rolls reservations back", func(t *testing.T) {
		bus := New(WithStore(NewMemoryStore()))
		upcast := func(registrySource) registryEventA { return registryEventA{} }
		if err := RegisterUpcast(bus, upcast); err != nil {
			t.Fatal(err)
		}
		if err := RegisterUpcast(bus, upcast); err == nil || !strings.Contains(err.Error(), "already registered") {
			t.Fatalf("duplicate typed upcast error = %v", err)
		}
	})
}

type switchableLoadStore struct {
	*MemoryStore
	fail bool
}

func (s *switchableLoadStore) LoadOffset(ctx context.Context, id string) (Offset, error) {
	if s.fail {
		return OffsetOldest, errors.New("load unavailable")
	}
	return s.MemoryStore.LoadOffset(ctx, id)
}

func TestSubscribeWithReplayTypeClaimLifecycle(t *testing.T) {
	t.Run("decoder conflict cleans up activated replay setup", func(t *testing.T) {
		bus := New(WithStore(NewMemoryStore()))
		// Create an intentionally inconsistent decoder entry without a matching
		// durable type claim so SubscribeWithReplay reaches its final installer
		// check after the coordinator has activated.
		if err := installFollowDecoder[registryEventA](bus); err != nil {
			t.Fatal(err)
		}
		if err := SubscribeWithReplay(context.Background(), bus, "decoder-cleanup", func(registryEventB) {}); err == nil || !strings.Contains(err.Error(), "install follow decoder") {
			t.Fatalf("decoder conflict = %v", err)
		}
		if HandlerCount[registryEventB](bus) != 0 {
			t.Fatal("failed decoder install left its replay marker registered")
		}
		if len(bus.persistedTypes.byName) != 0 {
			t.Fatalf("failed decoder install retained type claim: %v", bus.persistedTypes.byName)
		}
		if err := SubscribeWithReplay(context.Background(), bus, "decoder-cleanup", func(registryEventA) {}); err != nil {
			t.Fatalf("failed setup did not release its subscription ID: %v", err)
		}
	})

	t.Run("setup failure releases subscription ID for retry", func(t *testing.T) {
		store := &switchableLoadStore{MemoryStore: NewMemoryStore(), fail: true}
		bus := New(WithStore(store))
		if err := SubscribeWithReplay(context.Background(), bus, "retry-setup", func(registryEventA) {}); err == nil {
			t.Fatal("expected load failure")
		}
		store.fail = false
		if err := SubscribeWithReplay(context.Background(), bus, "retry-setup", func(registryEventA) {}); err != nil {
			t.Fatalf("retry with released subscription ID: %v", err)
		}
	})

	t.Run("load failure rolls back claim and decoder", func(t *testing.T) {
		store := &switchableLoadStore{MemoryStore: NewMemoryStore(), fail: true}
		bus := New(WithStore(store))
		if err := SubscribeWithReplay(context.Background(), bus, "claim-load", func(registryEventA) {}); err == nil {
			t.Fatal("expected load failure")
		}
		if _, ok := bus.followDecoders["test.registry.collision"]; ok {
			t.Fatal("failed setup installed a Follow decoder")
		}
		store.fail = false
		if err := Subscribe(bus, func(registryEventB) {}); err != nil {
			t.Fatalf("rolled-back claim blocked type B: %v", err)
		}
	})

	t.Run("decoded history makes failed claim sticky", func(t *testing.T) {
		store := NewMemoryStore()
		valid, _ := json.Marshal(registryEventA{})
		if _, err := store.Append(context.Background(), &Event{
			Type: "test.registry.collision", Data: valid, Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(context.Background(), &Event{
			Type: "test.registry.collision", Data: json.RawMessage(`{`), Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
		bus := New(WithStore(store))
		if err := SubscribeWithReplay(context.Background(), bus, "claim-proof", func(registryEventA) {}); err == nil {
			t.Fatal("expected poison replay failure")
		}
		if _, ok := bus.followDecoders["test.registry.collision"]; ok {
			t.Fatal("failed setup installed a Follow decoder")
		}
		if err := Subscribe(bus, func(registryEventB) {}); err == nil || !strings.Contains(err.Error(), "persisted event type name") {
			t.Fatalf("decoded type proof did not stay sticky: %v", err)
		}
	})
}
