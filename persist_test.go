package eventbus

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"strings"
	"sync"
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
	events, _, _ := store.Read(ctx, OffsetOldest, 10)
	if len(events) != 1 {
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
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
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

	ctx := context.Background()
	err := SubscribeWithReplay(ctx, bus, "test-sub", func(e TestEvent) {})

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

	// Check stored events
	ctx := context.Background()
	events, _, err := store.Read(ctx, OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
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

	// Replay from the beginning
	var replayed []string
	ctx := context.Background()
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
		replayed = append(replayed, string(event.Offset))
		return nil
	})

	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Should replay all 5 events
	if len(replayed) != 5 {
		t.Errorf("Expected 5 replayed events, got %d", len(replayed))
	}
}

// TestReplayFromOffset tests replay from a specific offset
func TestReplayFromOffset(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Publish events
	for i := 1; i <= 5; i++ {
		Publish(bus, TestEvent{ID: i})
	}

	// Replay from offset "00000000000000000002" (after the second event)
	var replayed []string
	ctx := context.Background()
	err := bus.Replay(ctx, Offset("00000000000000000002"), func(event *StoredEvent) error {
		replayed = append(replayed, string(event.Offset))
		return nil
	})

	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Should replay events 3, 4, 5 (offsets after "2")
	if len(replayed) != 3 {
		t.Errorf("Expected 3 replayed events, got %d", len(replayed))
	}
}

// TestReplayWithErrors tests error handling in replay
func TestReplayWithErrors(t *testing.T) {
	// Test with store that fails Read
	store := &errorStore{failRead: true}
	bus := New(WithStore(store))

	ctx := context.Background()
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error from Replay when Read fails")
	}

	// Test handler error during replay
	store2 := NewMemoryStore()
	bus2 := New(WithStore(store2))

	// Add some events
	Publish(bus2, TestEvent{ID: 1})
	Publish(bus2, TestEvent{ID: 2})

	// Replay with handler that errors on second event
	callCount := 0
	err = bus2.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
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
	ctx := context.Background()
	var received []int
	err := SubscribeWithReplay(ctx, bus, "test-sub", func(e TestEvent) {
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

	// Check subscription offset was saved
	offset, err := store.LoadOffset(ctx, "test-sub")
	if err != nil {
		t.Fatalf("LoadOffset failed: %v", err)
	}

	if offset != Offset("00000000000000000005") {
		t.Errorf("Expected subscription offset '00000000000000000005', got %s", offset)
	}
}

// TestSubscribeWithReplayResume tests resuming from saved offset
func TestSubscribeWithReplayResume(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))
	ctx := context.Background()

	// Publish initial events
	for i := 1; i <= 3; i++ {
		Publish(bus, TestEvent{ID: i})
	}

	// First subscription
	var received1 []int
	SubscribeWithReplay(ctx, bus, "resumable", func(e TestEvent) {
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
	SubscribeWithReplay(ctx, bus2, "resumable", func(e TestEvent) {
		received2 = append(received2, e.ID)
	})

	// Should not replay any events since offset is at 6
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

	// First publish a valid event
	Publish(bus, TestEvent{ID: 1, Value: "valid"})

	// Now manually insert an event with invalid JSON
	eventType := reflect.TypeOf((*TestEvent)(nil)).Elem()
	typeName := eventType.String()

	// Manually append an event with invalid JSON
	store.mu.Lock()
	store.nextOffset++
	offset := Offset(fmt.Sprintf("%d", store.nextOffset))
	store.events = append(store.events, &StoredEvent{
		Offset:    offset,
		Type:      typeName,
		Data:      []byte(`{"ID": "unclosed`), // Invalid JSON
		Timestamp: time.Now(),
	})
	store.mu.Unlock()

	// Update the bus lastOffset
	bus.storeMu.Lock()
	bus.lastOffset = offset
	bus.storeMu.Unlock()

	// Subscribe with replay - should fail due to unmarshal error
	ctx := context.Background()
	var received []TestEvent
	err := SubscribeWithReplay(ctx, bus, "unmarshal-test", func(e TestEvent) {
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
	ctx := context.Background()
	var received []int
	err := SubscribeWithReplay(ctx, bus, "test-only", func(e TestEvent) {
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
	events, offset, err := store.Read(ctx, OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read on empty store failed: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("Expected 0 events for empty store, got %d", len(events))
	}
	if offset != OffsetOldest {
		t.Errorf("Expected OffsetOldest for empty store, got %s", offset)
	}

	// Append events
	for i := 1; i <= 3; i++ {
		event := &Event{
			Type:      "TestEvent",
			Data:      []byte(`{}`),
			Timestamp: time.Now(),
		}
		_, err := store.Append(ctx, event)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Test Read all
	events, offset, err = store.Read(ctx, OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(events) != 3 {
		t.Errorf("Expected 3 events, got %d", len(events))
	}
	if offset != Offset("00000000000000000003") {
		t.Errorf("Expected offset '00000000000000000003', got %s", offset)
	}

	// Test Read with limit
	events, offset, err = store.Read(ctx, OffsetOldest, 2)
	if err != nil {
		t.Fatalf("Read with limit failed: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("Expected 2 events with limit, got %d", len(events))
	}

	// Test subscription offsets
	if err := store.SaveOffset(ctx, "sub1", Offset("00000000000000000002")); err != nil {
		t.Fatalf("SaveOffset failed: %v", err)
	}

	offset, err = store.LoadOffset(ctx, "sub1")
	if err != nil {
		t.Fatalf("LoadOffset failed: %v", err)
	}
	if offset != Offset("00000000000000000002") {
		t.Errorf("Expected subscription offset '00000000000000000002', got %s", offset)
	}

	// Test unknown subscription
	offset, err = store.LoadOffset(ctx, "unknown")
	if err != nil {
		t.Fatalf("LoadOffset for unknown sub failed: %v", err)
	}
	if offset != OffsetOldest {
		t.Errorf("Expected OffsetOldest for unknown subscription, got %s", offset)
	}
}

// TestPersistEventWithNilStore tests defensive check in persistEvent
func TestPersistEventWithNilStore(t *testing.T) {
	// Create bus with store first to set up the hook
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Now manually set store to nil to test defensive check
	bus.store = nil

	// Publish an event - the hook will call persistEvent with nil store
	Publish(bus, TestEvent{ID: 1})

	// Should not panic
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
	events, _, _ := store.Read(ctx, OffsetOldest, 0)
	if len(events) != 0 {
		t.Error("Expected no events to be stored when marshaling fails")
	}
}

// errorStore is a mock store that returns errors
type errorStore struct {
	failRead   bool
	failAppend bool
}

func (e *errorStore) Append(ctx context.Context, event *Event) (Offset, error) {
	if e.failAppend {
		return "", errors.New("append failed")
	}
	return Offset("1"), nil
}

func (e *errorStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	if e.failRead {
		return nil, "", errors.New("read failed")
	}
	return []*StoredEvent{}, from, nil
}

func (e *errorStore) SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error {
	return nil
}

func (e *errorStore) LoadOffset(ctx context.Context, subscriptionID string) (Offset, error) {
	return OffsetOldest, nil
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
			events, _, err := store.Read(ctx, OffsetOldest, 100)
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

// TestPersistenceErrorHandler tests that persistence errors are handled properly
func TestPersistenceErrorHandler(t *testing.T) {
	var capturedEvent any
	var capturedType reflect.Type
	var capturedError error

	store := &errorStore{failAppend: true}
	bus := New(
		WithStore(store),
		WithPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
			capturedEvent = event
			capturedType = eventType
			capturedError = err
		}),
	)

	// Publish an event that will fail to save
	testEvent := TestEvent{ID: 1, Value: "test"}
	Publish(bus, testEvent)

	// Verify error handler was called
	if capturedEvent == nil {
		t.Fatal("Error handler was not called")
	}

	if e, ok := capturedEvent.(TestEvent); !ok || e.ID != 1 {
		t.Errorf("Wrong event captured: %v", capturedEvent)
	}

	if capturedType.String() != "eventbus.TestEvent" {
		t.Errorf("Wrong type captured: %v", capturedType)
	}

	if !strings.Contains(capturedError.Error(), "append failed") {
		t.Errorf("Wrong error captured: %v", capturedError)
	}
}

// TestPersistenceMarshalError tests handling of marshal errors
func TestPersistenceMarshalError(t *testing.T) {
	var capturedError error

	store := NewMemoryStore()
	bus := New(
		WithStore(store),
		WithPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
			capturedError = err
		}),
	)

	// Create an unmarshalable event (channel)
	type UnmarshalableEvent struct {
		Ch chan int
	}

	Publish(bus, UnmarshalableEvent{Ch: make(chan int)})

	// Verify error handler was called
	if capturedError == nil {
		t.Fatal("Error handler was not called for marshal error")
	}

	if !strings.Contains(capturedError.Error(), "marshal") {
		t.Errorf("Expected marshal error, got: %v", capturedError)
	}
}

// TestPersistenceTimeout tests timeout handling
func TestPersistenceTimeout(t *testing.T) {
	var capturedError error

	// Create a store that sleeps longer than timeout
	slowStore := &slowStore{delay: 100 * time.Millisecond}
	bus := New(
		WithStore(slowStore),
		WithPersistenceTimeout(10*time.Millisecond),
		WithPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
			capturedError = err
		}),
	)

	Publish(bus, TestEvent{ID: 1, Value: "test"})

	// Give it time to timeout
	time.Sleep(50 * time.Millisecond)

	// Verify timeout error was captured
	if capturedError == nil {
		t.Fatal("Error handler was not called for timeout")
	}

	if !strings.Contains(capturedError.Error(), "context deadline exceeded") &&
		!strings.Contains(capturedError.Error(), "context canceled") {
		t.Errorf("Expected timeout error, got: %v", capturedError)
	}
}

// TestSetPersistenceErrorHandler tests runtime configuration
func TestSetPersistenceErrorHandler(t *testing.T) {
	var handlerCalled bool

	store := &errorStore{failAppend: true}
	bus := New(WithStore(store))

	// Set error handler at runtime
	bus.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
		handlerCalled = true
	})

	Publish(bus, TestEvent{ID: 1})

	if !handlerCalled {
		t.Error("Runtime-configured error handler was not called")
	}
}

// TestPersistenceSuccessIncrementsOffset tests that offset is tracked on success
func TestPersistenceSuccessIncrementsOffset(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Publish multiple events successfully
	for i := 1; i <= 3; i++ {
		Publish(bus, TestEvent{ID: i})
	}

	// Verify lastOffset was updated
	bus.storeMu.RLock()
	lastOffset := bus.lastOffset
	bus.storeMu.RUnlock()

	if lastOffset != Offset("00000000000000000003") {
		t.Errorf("Expected lastOffset '00000000000000000003', got %s", lastOffset)
	}

	// Verify all events were stored
	ctx := context.Background()
	events, _, _ := store.Read(ctx, OffsetOldest, 0)
	if len(events) != 3 {
		t.Errorf("Expected 3 events stored, got %d", len(events))
	}
}

// TestPersistenceWithoutErrorHandler tests that errors don't crash when no handler is set
func TestPersistenceWithoutErrorHandler(t *testing.T) {
	store := &errorStore{failAppend: true}
	bus := New(WithStore(store))

	// This should not panic even without error handler
	Publish(bus, TestEvent{ID: 1})
}

// slowStore is a mock store that delays operations
type slowStore struct {
	delay time.Duration
}

func (s *slowStore) Append(ctx context.Context, event *Event) (Offset, error) {
	select {
	case <-time.After(s.delay):
		return Offset("1"), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (s *slowStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	return []*StoredEvent{}, from, nil
}

func (s *slowStore) SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error {
	return nil
}

func (s *slowStore) LoadOffset(ctx context.Context, subscriptionID string) (Offset, error) {
	return OffsetOldest, nil
}

// TestConcurrentPersist tests for race condition in concurrent publishing
func TestConcurrentPersist(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	const numGoroutines = 100
	const eventsPerGoroutine = 10
	totalEvents := numGoroutines * eventsPerGoroutine

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Publish events concurrently from multiple goroutines
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < eventsPerGoroutine; i++ {
				Publish(bus, TestEvent{
					ID:    goroutineID*eventsPerGoroutine + i,
					Value: fmt.Sprintf("goroutine-%d-event-%d", goroutineID, i),
				})
			}
		}(g)
	}

	wg.Wait()

	// Verify all events were persisted
	ctx := context.Background()
	events, _, err := store.Read(ctx, OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Failed to load events: %v", err)
	}

	if len(events) != totalEvents {
		t.Errorf("Expected %d events, got %d", totalEvents, len(events))
	}

	// Check for duplicate offsets (the race condition symptom)
	offsets := make(map[Offset]bool)
	duplicates := []Offset{}

	for _, event := range events {
		if offsets[event.Offset] {
			duplicates = append(duplicates, event.Offset)
		}
		offsets[event.Offset] = true
	}

	if len(duplicates) > 0 {
		t.Errorf("Found duplicate offsets (race condition): %v", duplicates)
	}
}

// contextCapturingStore captures the context passed to Append for testing
type contextCapturingStore struct {
	*MemoryStore
	capturedCtx context.Context
}

func (s *contextCapturingStore) Append(ctx context.Context, event *Event) (Offset, error) {
	s.capturedCtx = ctx
	return s.MemoryStore.Append(ctx, event)
}

func TestPersistenceReceivesPublishContext(t *testing.T) {
	store := &contextCapturingStore{MemoryStore: NewMemoryStore()}
	bus := New(WithStore(store))

	// Create context with a value to verify it's passed through
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "trace-id-123")

	// Publish an event with context
	PublishContext(bus, ctx, TestEvent{ID: 1, Value: "test"})

	// Verify the context was passed to the store
	if store.capturedCtx == nil {
		t.Fatal("store should have received a context")
	}

	if store.capturedCtx.Value(ctxKey{}) != "trace-id-123" {
		t.Error("context value should be passed through to persistence")
	}
}

func TestWithStoreContextHookChaining(t *testing.T) {
	store := NewMemoryStore()

	var customHookCalled bool
	var customHookCtx context.Context

	type ctxKey struct{}

	// Create bus with custom context hook first, then add store
	bus := New(
		WithBeforePublishContext(func(ctx context.Context, eventType reflect.Type, event any) {
			customHookCalled = true
			customHookCtx = ctx
		}),
		WithStore(store),
	)

	// Create context with value
	ctx := context.WithValue(context.Background(), ctxKey{}, "test-value")

	// Publish an event
	PublishContext(bus, ctx, TestEvent{ID: 1, Value: "test"})

	// Custom hook should be called with context
	if !customHookCalled {
		t.Error("custom context hook should have been called")
	}

	if customHookCtx.Value(ctxKey{}) != "test-value" {
		t.Error("custom hook should receive context with value")
	}

	// Event should also be persisted
	events, _, err := store.Read(context.Background(), OffsetOldest, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}
}

// TestMemoryStoreReadStream tests the ReadStream implementation
func TestMemoryStoreReadStream(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Append test events
	for i := 1; i <= 5; i++ {
		event := &Event{
			Type:      "TestEvent",
			Data:      []byte(fmt.Sprintf(`{"id": %d}`, i)),
			Timestamp: time.Now(),
		}
		store.Append(ctx, event)
	}

	t.Run("streams all events from beginning", func(t *testing.T) {
		var received []*StoredEvent
		for event, err := range store.ReadStream(ctx, OffsetOldest) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			received = append(received, event)
		}

		if len(received) != 5 {
			t.Errorf("expected 5 events, got %d", len(received))
		}
	})

	t.Run("streams events from specific offset", func(t *testing.T) {
		var received []*StoredEvent
		for event, err := range store.ReadStream(ctx, Offset("00000000000000000002")) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			received = append(received, event)
		}

		// Should get events 3, 4, 5 (after offset "00000000000000000002")
		if len(received) != 3 {
			t.Errorf("expected 3 events, got %d", len(received))
		}
		if received[0].Offset != Offset("00000000000000000003") {
			t.Errorf("expected first offset '00000000000000000003', got %s", received[0].Offset)
		}
	})

	t.Run("handles early termination", func(t *testing.T) {
		count := 0
		for event, err := range store.ReadStream(ctx, OffsetOldest) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
			if event.Offset == Offset("00000000000000000002") {
				break // Early termination
			}
		}

		if count != 2 {
			t.Errorf("expected 2 events before break, got %d", count)
		}
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		var gotError bool
		for _, err := range store.ReadStream(cancelCtx, OffsetOldest) {
			if err != nil {
				gotError = true
				if !errors.Is(err, context.Canceled) {
					t.Errorf("expected context.Canceled, got: %v", err)
				}
				break
			}
		}

		if !gotError {
			t.Error("expected context cancellation error")
		}
	})

	t.Run("returns empty for out of range", func(t *testing.T) {
		// Use an offset that's lexicographically greater than "00000000000000000004"
		count := 0
		for _, err := range store.ReadStream(ctx, Offset("00000000000000000005")) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
		}

		if count != 0 {
			t.Errorf("expected 0 events, got %d", count)
		}
	})
}

// TestEventStoreStreamerInterface verifies interface compliance
func TestEventStoreStreamerInterface(t *testing.T) {
	var store EventStore = NewMemoryStore()

	// Type assertion to EventStoreStreamer
	streamer, ok := store.(EventStoreStreamer)
	if !ok {
		t.Fatal("MemoryStore should implement EventStoreStreamer")
	}

	// Verify it works
	ctx := context.Background()
	for _, err := range streamer.ReadStream(ctx, OffsetOldest) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

// TestReplayUsesStreaming tests that Replay auto-detects and uses ReadStream
func TestReplayUsesStreaming(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Publish events
	for i := 1; i <= 5; i++ {
		Publish(bus, TestEvent{ID: i})
	}

	// Replay should use streaming internally (MemoryStore implements EventStoreStreamer)
	var replayed []string
	ctx := context.Background()
	err := bus.Replay(ctx, Offset("00000000000000000001"), func(event *StoredEvent) error {
		replayed = append(replayed, string(event.Offset))
		return nil
	})

	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Should replay events after offset "1" (2, 3, 4, 5)
	if len(replayed) != 4 {
		t.Errorf("Expected 4 replayed events, got %d", len(replayed))
	}
}

// streamingErrorStore is a store that implements EventStoreStreamer but returns errors
type streamingErrorStore struct {
	errorStore
}

func (s *streamingErrorStore) ReadStream(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
	return func(yield func(*StoredEvent, error) bool) {
		yield(nil, errors.New("stream error"))
	}
}

// TestReplayWithStreamingError tests error handling when ReadStream returns an error
func TestReplayWithStreamingError(t *testing.T) {
	store := &streamingErrorStore{}
	bus := New(WithStore(store))

	ctx := context.Background()
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error from Replay when ReadStream fails")
	}

	if !strings.Contains(err.Error(), "stream events") {
		t.Errorf("Expected 'stream events' in error, got: %v", err)
	}
}

// nonStreamingStore is a store that does NOT implement EventStoreStreamer
type nonStreamingStore struct {
	errorStore
}

// TestReplayFallsBackToRead tests that Replay falls back to Read for non-streaming stores
func TestReplayFallsBackToRead(t *testing.T) {
	store := &nonStreamingStore{}
	bus := New(WithStore(store))

	ctx := context.Background()
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
		return nil
	})

	// Should succeed with empty result since Read returns empty slice
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestReplayStreamingHandlerError tests handler error when using streaming
func TestReplayStreamingHandlerError(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Publish events
	for i := 1; i <= 3; i++ {
		Publish(bus, TestEvent{ID: i})
	}

	ctx := context.Background()
	handlerErr := errors.New("handler failed")
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
		if event.Offset == Offset("00000000000000000002") {
			return handlerErr
		}
		return nil
	})

	if err == nil {
		t.Error("Expected error from Replay when handler fails")
	}

	if !strings.Contains(err.Error(), "handle event at offset 00000000000000000002") {
		t.Errorf("Expected 'handle event at offset 00000000000000000002' in error, got: %v", err)
	}
}

// nonStreamingStoreWithEvents returns events for testing fallback path
type nonStreamingStoreWithEvents struct {
	events     []*StoredEvent
	nextOffset int64
}

func (s *nonStreamingStoreWithEvents) Append(ctx context.Context, event *Event) (Offset, error) {
	s.nextOffset++
	// Use zero-padded offsets for correct lexicographic ordering
	offset := Offset(fmt.Sprintf("%020d", s.nextOffset))
	stored := &StoredEvent{
		Offset:    offset,
		Type:      event.Type,
		Data:      event.Data,
		Timestamp: event.Timestamp,
	}
	s.events = append(s.events, stored)
	return offset, nil
}

func (s *nonStreamingStoreWithEvents) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	var result []*StoredEvent
	var lastOffset Offset = from
	for _, e := range s.events {
		if from == OffsetOldest || e.Offset > from {
			result = append(result, e)
			lastOffset = e.Offset
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, lastOffset, nil
}

func (s *nonStreamingStoreWithEvents) SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error {
	return nil
}

func (s *nonStreamingStoreWithEvents) LoadOffset(ctx context.Context, subscriptionID string) (Offset, error) {
	return OffsetOldest, nil
}

// TestReplayNonStreamingHandlerError tests handler error for non-streaming stores
func TestReplayNonStreamingHandlerError(t *testing.T) {
	store := &nonStreamingStoreWithEvents{
		events: make([]*StoredEvent, 0),
	}
	bus := New(WithStore(store))

	// Append events directly to the store
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		event := &Event{
			Type:      "TestEvent",
			Data:      []byte(fmt.Sprintf(`{"id": %d}`, i)),
			Timestamp: time.Now(),
		}
		store.Append(ctx, event)
	}

	handlerErr := errors.New("handler failed")
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
		// Offset format is zero-padded: "00000000000000000002"
		if event.Offset == Offset("00000000000000000002") {
			return handlerErr
		}
		return nil
	})

	if err == nil {
		t.Error("Expected error from Replay when handler fails")
	}

	if !strings.Contains(err.Error(), "handle event at offset 00000000000000000002") {
		t.Errorf("Expected 'handle event at offset 00000000000000000002' in error, got: %v", err)
	}
}

// nonStreamingStoreWithReadError returns error on Read
type nonStreamingStoreWithReadError struct{}

func (s *nonStreamingStoreWithReadError) Append(ctx context.Context, event *Event) (Offset, error) {
	return Offset("1"), nil
}

func (s *nonStreamingStoreWithReadError) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	return nil, "", errors.New("read failed")
}

func (s *nonStreamingStoreWithReadError) SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error {
	return nil
}

func (s *nonStreamingStoreWithReadError) LoadOffset(ctx context.Context, subscriptionID string) (Offset, error) {
	return OffsetOldest, nil
}

// TestReplayNonStreamingReadError tests Read error for non-streaming stores
func TestReplayNonStreamingReadError(t *testing.T) {
	store := &nonStreamingStoreWithReadError{}
	bus := New(WithStore(store))

	ctx := context.Background()
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error from Replay when Read fails")
	}

	if !strings.Contains(err.Error(), "read events") {
		t.Errorf("Expected 'read events' in error, got: %v", err)
	}
}

// TestReplayNonStreamingContextCancellation tests context cancellation in non-streaming fallback path
func TestReplayNonStreamingContextCancellation(t *testing.T) {
	store := &nonStreamingStoreWithEvents{
		events: make([]*StoredEvent, 0),
	}
	bus := New(WithStore(store))

	// Append some events directly to the store
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		event := &Event{
			Type:      "TestEvent",
			Data:      []byte(fmt.Sprintf(`{"id": %d}`, i)),
			Timestamp: time.Now(),
		}
		store.Append(ctx, event)
	}

	// Create a cancelled context
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately

	err := bus.Replay(cancelCtx, OffsetOldest, func(event *StoredEvent) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error from Replay when context is cancelled")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled error, got: %v", err)
	}
}

// nonStreamingStoreWithStuckOffset is a store that returns events but never advances the offset
type nonStreamingStoreWithStuckOffset struct {
	events []*StoredEvent
}

func (s *nonStreamingStoreWithStuckOffset) Append(ctx context.Context, event *Event) (Offset, error) {
	stored := &StoredEvent{
		Offset:    Offset("00000000000000000001"),
		Type:      event.Type,
		Data:      event.Data,
		Timestamp: event.Timestamp,
	}
	s.events = append(s.events, stored)
	return stored.Offset, nil
}

func (s *nonStreamingStoreWithStuckOffset) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	// Return events but always return the same offset (stuck/not advancing)
	if from == OffsetOldest && len(s.events) > 0 {
		return s.events, from, nil // Return 'from' as next offset - infinite loop protection should catch this
	}
	return nil, from, nil
}

// TestReplayNonStreamingInfiniteLoopProtection tests the infinite loop protection in non-streaming fallback
func TestReplayNonStreamingInfiniteLoopProtection(t *testing.T) {
	store := &nonStreamingStoreWithStuckOffset{
		events: make([]*StoredEvent, 0),
	}
	bus := New(WithStore(store))

	// Append an event directly to the store
	ctx := context.Background()
	event := &Event{
		Type:      "TestEvent",
		Data:      []byte(`{"id": 1}`),
		Timestamp: time.Now(),
	}
	store.Append(ctx, event)

	// Replay should detect the stuck offset and return an error
	var replayed int
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
		replayed++
		return nil
	})

	// Should error to indicate a buggy EventStore implementation
	if err == nil {
		t.Error("Expected error for stuck offset, got nil")
	}
	if !strings.Contains(err.Error(), "non-advancing offset") {
		t.Errorf("Expected 'non-advancing offset' error, got: %v", err)
	}

	// Should have processed events exactly once before detecting the stuck offset
	if replayed != 1 {
		t.Errorf("Expected 1 replayed event, got %d", replayed)
	}
}

// TestSubscriptionStoreInterface verifies MemoryStore implements SubscriptionStore
func TestSubscriptionStoreInterface(t *testing.T) {
	var store interface{} = NewMemoryStore()

	_, ok := store.(SubscriptionStore)
	if !ok {
		t.Fatal("MemoryStore should implement SubscriptionStore")
	}
}

// TestWithSubscriptionStore tests the WithSubscriptionStore option
func TestWithSubscriptionStore(t *testing.T) {
	eventStore := NewMemoryStore()
	subStore := NewMemoryStore() // Use a separate store for subscriptions

	bus := New(
		WithStore(eventStore),
		WithSubscriptionStore(subStore),
	)

	// Publish some events
	for i := 1; i <= 3; i++ {
		Publish(bus, TestEvent{ID: i})
	}

	// Subscribe with replay
	ctx := context.Background()
	var received []int
	err := SubscribeWithReplay(ctx, bus, "test-sub", func(e TestEvent) {
		received = append(received, e.ID)
	})

	if err != nil {
		t.Fatalf("SubscribeWithReplay failed: %v", err)
	}

	if len(received) != 3 {
		t.Errorf("Expected 3 events, got %d", len(received))
	}

	// Verify offset was saved in the subscription store (not event store)
	offset, _ := subStore.LoadOffset(ctx, "test-sub")
	if offset != Offset("00000000000000000003") {
		t.Errorf("Expected offset '00000000000000000003' in subscription store, got %s", offset)
	}
}

// eventStoreOnly is a store that only implements EventStore, not SubscriptionStore
type eventStoreOnly struct{}

func (s *eventStoreOnly) Append(ctx context.Context, event *Event) (Offset, error) {
	return Offset("1"), nil
}

func (s *eventStoreOnly) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	return nil, from, nil
}

// TestSubscribeWithReplayNoSubscriptionStore tests error when no subscription store available
func TestSubscribeWithReplayNoSubscriptionStore(t *testing.T) {
	// Create a store that doesn't implement SubscriptionStore
	store := &eventStoreOnly{}
	bus := New(WithStore(store))

	ctx := context.Background()
	err := SubscribeWithReplay(ctx, bus, "test-sub", func(e TestEvent) {})

	if err == nil {
		t.Error("Expected error when no SubscriptionStore is available")
	}

	if !strings.Contains(err.Error(), "SubscriptionStore") {
		t.Errorf("Expected error about SubscriptionStore, got: %v", err)
	}
}

// TestReplayNonStreamingPagination tests the pagination path in Replay for non-streaming stores
// This ensures the offset = nextOffset line is covered when reading more than 100 events
func TestReplayNonStreamingPagination(t *testing.T) {
	store := &nonStreamingStoreWithEvents{
		events: make([]*StoredEvent, 0),
	}
	bus := New(WithStore(store))

	// Append more than 100 events to trigger pagination (batch size is 100)
	ctx := context.Background()
	for i := 1; i <= 150; i++ {
		event := &Event{
			Type:      "TestEvent",
			Data:      []byte(fmt.Sprintf(`{"id": %d}`, i)),
			Timestamp: time.Now(),
		}
		store.Append(ctx, event)
	}

	// Replay all events - this will require at least 2 Read calls (100 + 50)
	var replayed []string
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
		replayed = append(replayed, string(event.Offset))
		return nil
	})

	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Should replay all 150 events
	if len(replayed) != 150 {
		t.Errorf("Expected 150 replayed events, got %d", len(replayed))
	}

	// Verify offsets are in order (using 20-digit zero-padded format)
	if len(replayed) >= 150 {
		if replayed[0] != "00000000000000000001" {
			t.Errorf("Expected first offset '00000000000000000001', got %s", replayed[0])
		}
		if replayed[149] != "00000000000000000150" {
			t.Errorf("Expected last offset '00000000000000000150', got %s", replayed[149])
		}
	}
}

// TestWithReplayBatchSize tests the configurable replay batch size option
func TestWithReplayBatchSize(t *testing.T) {
	// Create a non-streaming store to test batch reading
	store := &nonStreamingStoreWithEvents{
		events: make([]*StoredEvent, 0),
	}
	bus := New(WithStore(store), WithReplayBatchSize(10))

	// Append 25 events directly to the store
	ctx := context.Background()
	for i := 1; i <= 25; i++ {
		event := &Event{
			Type:      "TestEvent",
			Data:      []byte(fmt.Sprintf(`{"id": %d}`, i)),
			Timestamp: time.Now(),
		}
		store.Append(ctx, event)
	}

	// Replay all events - this will require 3 Read calls (10 + 10 + 5) with batch size 10
	var replayed int
	err := bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
		replayed++
		return nil
	})

	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Should replay all 25 events
	if replayed != 25 {
		t.Errorf("Expected 25 replayed events, got %d", replayed)
	}
}
