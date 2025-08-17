package eventbus

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Test types
type TestEvent struct {
	ID    int
	Value string
}

type AnotherTestEvent struct {
	Name string
}

// TestWithStoreHookChaining tests that WithStore chains with existing hooks
func TestWithStoreHookChaining(t *testing.T) {
	store := NewMemoryStore()

	// Create bus with existing hook
	var hookCalled bool
	bus := New()
	bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
		hookCalled = true
	})

	// Apply WithStore after setting hook
	WithStore(store)(bus)

	// Publish an event
	Publish(bus, TestEvent{ID: 1, Value: "test"})

	// Both hook and persistence should work
	if !hookCalled {
		t.Error("Existing hook should still be called after WithStore")
	}

	ctx := context.Background()
	pos, _ := store.GetPosition(ctx)
	if pos != 1 {
		t.Error("Event should be persisted even with existing hook")
	}
}

// TestBackwardCompatibility ensures New() still works without options
func TestBackwardCompatibility(t *testing.T) {
	// Should be able to create bus without any options (non-persistent)
	bus := New()

	if bus == nil {
		t.Fatal("Failed to create bus without options")
	}

	if bus.IsPersistent() {
		t.Error("Bus should not be persistent without WithStore option")
	}

	// GetStore should return nil for non-persistent bus
	if bus.GetStore() != nil {
		t.Error("GetStore should return nil for non-persistent bus")
	}

	// Should work normally for publishing and subscribing
	var received bool
	Subscribe(bus, func(e TestEvent) {
		received = true
	})

	Publish(bus, TestEvent{ID: 1, Value: "test"})

	if !received {
		t.Error("Failed to receive event on non-persistent bus")
	}
}

// TestNonPersistentReplay tests that Replay fails gracefully on non-persistent bus
func TestNonPersistentReplay(t *testing.T) {
	bus := New() // No persistence

	ctx := context.Background()
	err := bus.Replay(ctx, 0, func(event *StoredEvent) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error when calling Replay on non-persistent bus")
	}

	if !strings.Contains(err.Error(), "requires persistence") {
		t.Errorf("Expected error message about persistence, got: %v", err)
	}
}

// TestSubscribeWithReplayNoPersistence tests that SubscribeWithReplay fails without persistence
func TestSubscribeWithReplayNoPersistence(t *testing.T) {
	bus := New() // No persistence

	err := SubscribeWithReplay(bus, "test-sub", func(e TestEvent) {})

	if err == nil {
		t.Error("Expected error when calling SubscribeWithReplay on non-persistent bus")
	}

	if !strings.Contains(err.Error(), "requires persistence") {
		t.Errorf("Expected error message about persistence, got: %v", err)
	}
}

// TestPersistentEventBus tests basic persistence functionality
func TestPersistentEventBus(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Test GetStore returns the correct store
	if bus.GetStore() != store {
		t.Error("GetStore should return the store passed to WithStore")
	}

	// Publish some events
	Publish(bus, TestEvent{ID: 1, Value: "first"})
	Publish(bus, TestEvent{ID: 2, Value: "second"})
	Publish(bus, TestEvent{ID: 3, Value: "third"})

	// Check position tracking
	ctx := context.Background()
	pos, err := store.GetPosition(ctx)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}

	if pos != 3 {
		t.Errorf("Expected position 3, got %d", pos)
	}

	// Check stored events
	events, err := store.Load(ctx, 1, 3)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(events) != 3 {
		t.Errorf("Expected 3 events, got %d", len(events))
	}
}

// TestReplay tests event replay functionality
func TestReplay(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Publish events
	for i := 1; i <= 5; i++ {
		Publish(bus, TestEvent{ID: i})
	}

	// Replay from position 2
	var replayed []int
	ctx := context.Background()
	err := bus.Replay(ctx, 2, func(event *StoredEvent) error {
		replayed = append(replayed, int(event.Position))
		return nil
	})

	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Should replay events 2, 3, 4, 5
	if len(replayed) != 4 {
		t.Errorf("Expected 4 replayed events, got %d", len(replayed))
	}

	for i, pos := range replayed {
		expected := i + 2
		if pos != expected {
			t.Errorf("Expected position %d, got %d", expected, pos)
		}
	}
}

// TestReplayWithErrors tests error handling in replay
func TestReplayWithErrors(t *testing.T) {
	// Test with store that fails Load
	store := &errorStore{failLoad: true}
	bus := New(WithStore(store))

	ctx := context.Background()
	err := bus.Replay(ctx, 1, func(event *StoredEvent) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error from Replay when Load fails")
	}

	// Test handler error during replay
	store2 := NewMemoryStore()
	bus2 := New(WithStore(store2))

	// Add some events
	Publish(bus2, TestEvent{ID: 1})
	Publish(bus2, TestEvent{ID: 2})

	// Replay with handler that errors on second event
	callCount := 0
	err = bus2.Replay(ctx, 1, func(event *StoredEvent) error {
		callCount++
		if callCount == 2 {
			return errors.New("handler error")
		}
		return nil
	})

	if err == nil {
		t.Error("Expected error from Replay when handler returns error")
	}
}

// TestSubscribeWithReplay tests subscription with replay
func TestSubscribeWithReplay(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Publish some events before subscription
	for i := 1; i <= 3; i++ {
		Publish(bus, TestEvent{ID: i})
	}

	// Subscribe with replay
	var received []int
	err := SubscribeWithReplay(bus, "test-sub", func(e TestEvent) {
		received = append(received, e.ID)
	})

	if err != nil {
		t.Fatalf("SubscribeWithReplay failed: %v", err)
	}

	// Should have replayed 3 events
	if len(received) != 3 {
		t.Errorf("Expected 3 replayed events, got %d", len(received))
	}

	// Publish more events
	Publish(bus, TestEvent{ID: 4})
	Publish(bus, TestEvent{ID: 5})

	// Give async handlers time to process
	time.Sleep(10 * time.Millisecond)

	// Should have received all 5 events
	if len(received) != 5 {
		t.Errorf("Expected 5 total events, got %d", len(received))
	}

	// Check subscription position was saved
	ctx := context.Background()
	pos, err := store.LoadSubscriptionPosition(ctx, "test-sub")
	if err != nil {
		t.Fatalf("LoadSubscriptionPosition failed: %v", err)
	}

	if pos != 5 {
		t.Errorf("Expected subscription position 5, got %d", pos)
	}
}

// TestSubscribeWithReplayResume tests resuming from saved position
func TestSubscribeWithReplayResume(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Publish initial events
	for i := 1; i <= 3; i++ {
		Publish(bus, TestEvent{ID: i})
	}

	// First subscription
	var received1 []int
	SubscribeWithReplay(bus, "resumable", func(e TestEvent) {
		received1 = append(received1, e.ID)
	})

	if len(received1) != 3 {
		t.Errorf("Expected 3 events in first subscription, got %d", len(received1))
	}

	// Publish more events
	for i := 4; i <= 6; i++ {
		Publish(bus, TestEvent{ID: i})
	}

	// Wait for processing
	time.Sleep(10 * time.Millisecond)

	// Create new bus with same store (simulating restart)
	bus2 := New(WithStore(store))

	// Resume subscription - should only get new events
	var received2 []int
	SubscribeWithReplay(bus2, "resumable", func(e TestEvent) {
		received2 = append(received2, e.ID)
	})

	// Should not replay any events since position is at 6
	if len(received2) != 0 {
		t.Errorf("Expected 0 replayed events on resume, got %d", len(received2))
	}

	// Publish new event
	Publish(bus2, TestEvent{ID: 7})

	// Wait for processing
	time.Sleep(10 * time.Millisecond)

	// Should receive only the new event
	if len(received2) != 1 || (len(received2) > 0 && received2[0] != 7) {
		t.Errorf("Expected only event 7 on resumed subscription, got %v", received2)
	}
}

// TestSubscribeWithReplayUnmarshalError tests unmarshal error handling
func TestSubscribeWithReplayUnmarshalError(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// First publish a valid event to set the position
	Publish(bus, TestEvent{ID: 1, Value: "valid"})

	// Now manually insert an event with invalid JSON that will cause unmarshal to fail
	ctx := context.Background()
	eventType := reflect.TypeOf((*TestEvent)(nil)).Elem()
	// Use consistent type naming
	typeName := eventType.String()

	// This JSON is syntactically invalid
	store.Save(ctx, &StoredEvent{
		Position:  2,
		Type:      typeName,
		Data:      []byte(`{"ID": "unclosed`), // Invalid JSON
		Timestamp: time.Now(),
	})

	// Update the bus position manually since we bypassed Publish
	bus.storeMu.Lock()
	bus.storePosition = 2
	bus.storeMu.Unlock()

	// Subscribe with replay - should fail due to unmarshal error
	var received []TestEvent
	err := SubscribeWithReplay(bus, "unmarshal-test", func(e TestEvent) {
		received = append(received, e)
	})

	// Should get an error from replay due to unmarshal failure
	if err == nil {
		t.Error("Expected unmarshal error from SubscribeWithReplay")
	} else if !strings.Contains(err.Error(), "replay events") {
		t.Errorf("Expected 'replay events' error, got: %v", err)
	}

	// First event should have been processed before the error
	if len(received) != 1 {
		t.Errorf("Expected 1 event before unmarshal error, got %d", len(received))
	}
}

// TestSubscribeWithReplayDifferentEventTypes tests filtering by event type
func TestSubscribeWithReplayDifferentEventTypes(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Publish mixed event types
	Publish(bus, TestEvent{ID: 1})
	Publish(bus, AnotherTestEvent{Name: "ignored"})
	Publish(bus, TestEvent{ID: 2})
	Publish(bus, AnotherTestEvent{Name: "also ignored"})
	Publish(bus, TestEvent{ID: 3})

	// Subscribe to TestEvent only
	var received []int
	err := SubscribeWithReplay(bus, "test-only", func(e TestEvent) {
		received = append(received, e.ID)
	})

	if err != nil {
		t.Fatalf("SubscribeWithReplay failed: %v", err)
	}

	// Should only receive TestEvent instances
	if len(received) != 3 {
		t.Errorf("Expected 3 TestEvents, got %d", len(received))
	}

	for i, id := range received {
		if id != i+1 {
			t.Errorf("Expected ID %d, got %d", i+1, id)
		}
	}
}

// TestMemoryStore tests the memory store implementation
func TestMemoryStore(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Test empty store
	pos, err := store.GetPosition(ctx)
	if err != nil {
		t.Fatalf("GetPosition on empty store failed: %v", err)
	}
	if pos != 0 {
		t.Errorf("Expected position 0 for empty store, got %d", pos)
	}

	// Save events
	for i := 1; i <= 3; i++ {
		event := &StoredEvent{
			Position:  int64(i),
			Type:      "TestEvent",
			Data:      []byte(`{}`),
			Timestamp: time.Now(),
		}
		if err := store.Save(ctx, event); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
	}

	// Test GetPosition
	pos, err = store.GetPosition(ctx)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 3 {
		t.Errorf("Expected position 3, got %d", pos)
	}

	// Test Load range
	events, err := store.Load(ctx, 2, 3)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("Expected 2 events, got %d", len(events))
	}

	// Test subscription positions
	if err := store.SaveSubscriptionPosition(ctx, "sub1", 2); err != nil {
		t.Fatalf("SaveSubscriptionPosition failed: %v", err)
	}

	pos, err = store.LoadSubscriptionPosition(ctx, "sub1")
	if err != nil {
		t.Fatalf("LoadSubscriptionPosition failed: %v", err)
	}
	if pos != 2 {
		t.Errorf("Expected subscription position 2, got %d", pos)
	}

	// Test unknown subscription
	pos, err = store.LoadSubscriptionPosition(ctx, "unknown")
	if err != nil {
		t.Fatalf("LoadSubscriptionPosition for unknown sub failed: %v", err)
	}
	if pos != 0 {
		t.Errorf("Expected position 0 for unknown subscription, got %d", pos)
	}
}

// TestPersistentBusWithFailingStore tests with store that fails GetPosition
func TestPersistentBusWithFailingStore(t *testing.T) {
	store := &errorStore{}
	bus := New(WithStore(store))

	// Should still create bus even if GetPosition fails
	if bus == nil {
		t.Fatal("Expected bus to be created despite GetPosition error")
	}

	// Position should be 0 when GetPosition fails
	if bus.storePosition != 0 {
		t.Errorf("Expected position 0 when GetPosition fails, got %d", bus.storePosition)
	}
}

// TestPersistEventWithNilStore tests defensive check in persistEvent
func TestPersistEventWithNilStore(t *testing.T) {
	// Create bus with store first to set up the hook
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Now manually set store to nil to test defensive check
	// This simulates an edge case where store becomes nil after initialization
	bus.store = nil

	// Publish an event - the hook will call persistEvent with nil store
	Publish(bus, TestEvent{ID: 1})

	// Should not panic, and position should not change
	if bus.storePosition != 0 {
		t.Error("Expected position to remain 0 when store is nil")
	}
}

// TestPersistEventWithUnmarshalableData tests persist with unmarshalable data
func TestPersistEventWithUnmarshalableData(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Create a type that truly cannot be marshaled to JSON
	type UnmarshalableEvent struct {
		Ch chan int // Channels cannot be marshaled to JSON
	}

	event := UnmarshalableEvent{
		Ch: make(chan int),
	}

	// This should not panic, just silently fail
	Publish(bus, event)

	// Check that no event was stored
	ctx := context.Background()
	pos, _ := store.GetPosition(ctx)
	if pos != 0 {
		t.Error("Expected no events to be stored when marshaling fails")
	}
}

// errorStore is a mock store that returns errors
type errorStore struct {
	failLoad bool
}

func (e *errorStore) Save(ctx context.Context, event *StoredEvent) error {
	return nil
}

func (e *errorStore) Load(ctx context.Context, from, to int64) ([]*StoredEvent, error) {
	if e.failLoad {
		return nil, errors.New("load failed")
	}
	return []*StoredEvent{}, nil
}

func (e *errorStore) GetPosition(ctx context.Context) (int64, error) {
	return 0, errors.New("get position failed")
}

func (e *errorStore) SaveSubscriptionPosition(ctx context.Context, subscriptionID string, position int64) error {
	return nil
}

func (e *errorStore) LoadSubscriptionPosition(ctx context.Context, subscriptionID string) (int64, error) {
	return 0, nil
}

// TestTypeNameConsistency verifies that EventType() and persistEvent use the same type naming
func TestTypeNameConsistency(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Test events from different packages and types
	testCases := []struct {
		name  string
		event any
	}{
		{"struct", TestEvent{ID: 1}},
		{"pointer", &TestEvent{ID: 2}},
		{"string", "test string"},
		{"int", 42},
		{"slice", []string{"a", "b"}},
		{"map", map[string]int{"key": 1}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Get expected type name from EventType
			expectedType := EventType(tc.event)

			// Publish the event to trigger persistence
			Publish(bus, tc.event)

			// Retrieve the last stored event
			ctx := context.Background()
			events, err := store.Load(ctx, 0, 100)
			if err != nil {
				t.Fatalf("Failed to load events: %v", err)
			}

			// Find the event we just published (should be the last one)
			if len(events) == 0 {
				t.Fatal("No events were persisted")
			}
			storedEvent := events[len(events)-1]

			// Verify the type name matches
			if storedEvent.Type != expectedType {
				t.Errorf("Type name mismatch: persisted=%q, EventType()=%q",
					storedEvent.Type, expectedType)
			}
		})
	}
}
