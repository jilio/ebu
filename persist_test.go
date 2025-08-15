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

// TestPersistentEventBus tests basic persistence functionality
func TestPersistentEventBus(t *testing.T) {
	store := NewMemoryStore()
	bus := NewPersistent(store)
	
	// Publish some events
	Publish(bus.EventBus, TestEvent{ID: 1, Value: "first"})
	Publish(bus.EventBus, TestEvent{ID: 2, Value: "second"})
	Publish(bus.EventBus, TestEvent{ID: 3, Value: "third"})
	
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
	bus := NewPersistent(store)
	
	// Publish events
	for i := 1; i <= 5; i++ {
		Publish(bus.EventBus, TestEvent{ID: i})
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
	bus := NewPersistent(store)
	
	ctx := context.Background()
	err := bus.Replay(ctx, 1, func(event *StoredEvent) error {
		return nil
	})
	
	if err == nil {
		t.Error("Expected error from Replay when Load fails")
	}
	
	// Test handler error during replay
	store2 := NewMemoryStore()
	bus2 := NewPersistent(store2)
	
	// Add some events
	Publish(bus2.EventBus, TestEvent{ID: 1})
	Publish(bus2.EventBus, TestEvent{ID: 2})
	
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
	bus := NewPersistent(store)
	
	// Publish some events before subscription
	for i := 1; i <= 3; i++ {
		Publish(bus.EventBus, TestEvent{ID: i})
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
	Publish(bus.EventBus, TestEvent{ID: 4})
	Publish(bus.EventBus, TestEvent{ID: 5})
	
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
	bus := NewPersistent(store)
	
	// Publish initial events
	for i := 1; i <= 3; i++ {
		Publish(bus.EventBus, TestEvent{ID: i})
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
		Publish(bus.EventBus, TestEvent{ID: i})
	}
	
	// Wait for processing
	time.Sleep(10 * time.Millisecond)
	
	// Create new bus with same store (simulating restart)
	bus2 := NewPersistent(store)
	
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
	Publish(bus2.EventBus, TestEvent{ID: 7})
	
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
	bus := NewPersistent(store)
	
	// First publish a valid event to set the position
	Publish(bus.EventBus, TestEvent{ID: 1, Value: "valid"})
	
	// Now manually insert an event with invalid JSON that will cause unmarshal to fail
	ctx := context.Background()
	eventType := reflect.TypeOf((*TestEvent)(nil)).Elem()
	
	// This JSON is syntactically invalid
	store.Save(ctx, &StoredEvent{
		Position:  2,
		Type:      eventType.String(),
		Data:      []byte(`{"ID": "unclosed`), // Invalid JSON
		Timestamp: time.Now(),
	})
	
	// Update the bus position manually since we bypassed Publish
	bus.mu.Lock()
	bus.position = 2
	bus.mu.Unlock()
	
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
	bus := NewPersistent(store)
	
	// Publish mixed event types
	Publish(bus.EventBus, TestEvent{ID: 1})
	Publish(bus.EventBus, AnotherTestEvent{Name: "ignored"})
	Publish(bus.EventBus, TestEvent{ID: 2})
	Publish(bus.EventBus, AnotherTestEvent{Name: "also ignored"})
	Publish(bus.EventBus, TestEvent{ID: 3})
	
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
	bus := NewPersistent(store)
	
	// Should still create bus even if GetPosition fails
	if bus == nil {
		t.Fatal("Expected bus to be created despite GetPosition error")
	}
	
	// Position should be 0 when GetPosition fails
	if bus.position != 0 {
		t.Errorf("Expected position 0 when GetPosition fails, got %d", bus.position)
	}
}

// TestPersistEventWithUnmarshalableData tests persist with unmarshalable data
func TestPersistEventWithUnmarshalableData(t *testing.T) {
	store := NewMemoryStore()
	bus := NewPersistent(store)
	
	// Create a type that causes marshal errors
	type CyclicEvent struct {
		Self *CyclicEvent
	}
	
	event := &CyclicEvent{}
	event.Self = event // Create cycle
	
	// This should not panic, just silently fail
	Publish(bus.EventBus, event)
	
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