package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Test event types for upcasting
type UserCreatedV1 struct {
	UserID string
	Name   string
}

type UserCreatedV2 struct {
	UserID    string
	FirstName string
	LastName  string
}

type UserCreatedV3 struct {
	UserID    string
	FirstName string
	LastName  string
	Email     string
	CreatedAt time.Time
}

type ProductAddedV1 struct {
	ProductID int
	Name      string
	Price     float64
}

type ProductAddedV2 struct {
	ProductID   int
	Name        string
	Price       float64
	Currency    string
	Description string
}

// TestRegisterUpcast tests basic upcast registration
func TestRegisterUpcast(t *testing.T) {
	bus := New()

	// Test successful registration
	err := RegisterUpcast(bus, func(v1 UserCreatedV1) UserCreatedV2 {
		parts := strings.Split(v1.Name, " ")
		firstName := parts[0]
		lastName := ""
		if len(parts) > 1 {
			lastName = strings.Join(parts[1:], " ")
		}
		return UserCreatedV2{
			UserID:    v1.UserID,
			FirstName: firstName,
			LastName:  lastName,
		}
	})

	if err != nil {
		t.Fatalf("Failed to register upcast: %v", err)
	}

	// Test nil bus
	err = RegisterUpcast(nil, func(v1 UserCreatedV1) UserCreatedV2 {
		return UserCreatedV2{}
	})
	if err == nil || !strings.Contains(err.Error(), "bus cannot be nil") {
		t.Error("Expected error for nil bus")
	}

	// Test nil upcast function
	err = RegisterUpcast[UserCreatedV1, UserCreatedV2](bus, nil)
	if err == nil || !strings.Contains(err.Error(), "upcast function cannot be nil") {
		t.Error("Expected error for nil upcast function")
	}
}

// TestRegisterUpcastFunc tests raw upcast function registration
func TestRegisterUpcastFunc(t *testing.T) {
	bus := New()

	upcastFunc := func(data json.RawMessage) (json.RawMessage, string, error) {
		var v1 ProductAddedV1
		if err := json.Unmarshal(data, &v1); err != nil {
			return nil, "", err
		}

		v2 := ProductAddedV2{
			ProductID:   v1.ProductID,
			Name:        v1.Name,
			Price:       v1.Price,
			Currency:    "USD",
			Description: "",
		}

		newData, err := json.Marshal(v2)
		if err != nil {
			return nil, "", err
		}

		return newData, "eventbus.ProductAddedV2", nil
	}

	err := RegisterUpcastFunc(bus, "eventbus.ProductAddedV1", "eventbus.ProductAddedV2", upcastFunc)
	if err != nil {
		t.Fatalf("Failed to register upcast func: %v", err)
	}

	// Test nil bus
	err = RegisterUpcastFunc(nil, "Type1", "Type2", upcastFunc)
	if err == nil || !strings.Contains(err.Error(), "bus cannot be nil") {
		t.Error("Expected error for nil bus")
	}
}

// TestUpcastChain tests chaining multiple upcasts
func TestUpcastChain(t *testing.T) {
	bus := New()

	// Register V1 -> V2
	err := RegisterUpcast(bus, func(v1 UserCreatedV1) UserCreatedV2 {
		parts := strings.Split(v1.Name, " ")
		firstName := parts[0]
		lastName := ""
		if len(parts) > 1 {
			lastName = strings.Join(parts[1:], " ")
		}
		return UserCreatedV2{
			UserID:    v1.UserID,
			FirstName: firstName,
			LastName:  lastName,
		}
	})
	if err != nil {
		t.Fatalf("Failed to register V1->V2 upcast: %v", err)
	}

	// Register V2 -> V3
	err = RegisterUpcast(bus, func(v2 UserCreatedV2) UserCreatedV3 {
		return UserCreatedV3{
			UserID:    v2.UserID,
			FirstName: v2.FirstName,
			LastName:  v2.LastName,
			Email:     fmt.Sprintf("%s.%s@example.com", strings.ToLower(v2.FirstName), strings.ToLower(v2.LastName)),
			CreatedAt: time.Now(),
		}
	})
	if err != nil {
		t.Fatalf("Failed to register V2->V3 upcast: %v", err)
	}

	// Test the chain by applying upcasts
	v1 := UserCreatedV1{
		UserID: "123",
		Name:   "John Doe",
	}
	v1Data, _ := json.Marshal(v1)

	// Apply upcasts
	resultData, resultType, err := bus.upcastRegistry.apply(v1Data, "eventbus.UserCreatedV1")
	if err != nil {
		t.Fatalf("Failed to apply upcast chain: %v", err)
	}

	if resultType != "eventbus.UserCreatedV3" {
		t.Errorf("Expected type eventbus.UserCreatedV3, got %s", resultType)
	}

	var v3 UserCreatedV3
	if err := json.Unmarshal(resultData, &v3); err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	if v3.UserID != "123" || v3.FirstName != "John" || v3.LastName != "Doe" {
		t.Errorf("Unexpected result: %+v", v3)
	}

	if !strings.Contains(v3.Email, "john.doe") {
		t.Errorf("Expected email to contain john.doe, got %s", v3.Email)
	}
}

// TestCircularDependencyDetection tests that circular dependencies are detected
func TestCircularDependencyDetection(t *testing.T) {
	bus := New()

	// Register A -> B
	err := RegisterUpcastFunc(bus, "TypeA", "TypeB", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "TypeB", nil
	})
	if err != nil {
		t.Fatalf("Failed to register A->B: %v", err)
	}

	// Register B -> C
	err = RegisterUpcastFunc(bus, "TypeB", "TypeC", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "TypeC", nil
	})
	if err != nil {
		t.Fatalf("Failed to register B->C: %v", err)
	}

	// Try to register C -> A (should fail due to circular dependency)
	err = RegisterUpcastFunc(bus, "TypeC", "TypeA", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "TypeA", nil
	})
	if err == nil || !strings.Contains(err.Error(), "circular dependency") {
		t.Error("Expected circular dependency error")
	}
}

// TestUpcastErrorHandling tests error handling in upcasts
func TestUpcastErrorHandling(t *testing.T) {
	var capturedType string
	var capturedError error

	bus := New(WithUpcastErrorHandler(func(eventType string, data json.RawMessage, err error) {
		capturedType = eventType
		capturedError = err
	}))

	// Register an upcast that will fail
	err := RegisterUpcastFunc(bus, "FailingType", "TargetType", func(data json.RawMessage) (json.RawMessage, string, error) {
		return nil, "", errors.New("upcast failed")
	})
	if err != nil {
		t.Fatalf("Failed to register upcast: %v", err)
	}

	// Try to apply the failing upcast
	testData := json.RawMessage(`{"test": "data"}`)
	_, _, err = bus.upcastRegistry.apply(testData, "FailingType")

	if err == nil {
		t.Error("Expected error from failing upcast")
	}

	if capturedType != "FailingType" {
		t.Errorf("Expected captured type FailingType, got %s", capturedType)
	}

	if capturedError == nil || !strings.Contains(capturedError.Error(), "upcast failed") {
		t.Errorf("Expected upcast failed error, got %v", capturedError)
	}
}

// TestUpcastWithSubscribeWithReplay tests upcasting in SubscribeWithReplay
func TestUpcastWithSubscribeWithReplay(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Register upcast V1 -> V2
	err := RegisterUpcast(bus, func(v1 UserCreatedV1) UserCreatedV2 {
		parts := strings.Split(v1.Name, " ")
		firstName := parts[0]
		lastName := ""
		if len(parts) > 1 {
			lastName = strings.Join(parts[1:], " ")
		}
		return UserCreatedV2{
			UserID:    v1.UserID,
			FirstName: firstName,
			LastName:  lastName,
		}
	})
	if err != nil {
		t.Fatalf("Failed to register upcast: %v", err)
	}

	// Publish a V1 event to store it properly (this sets up the position correctly)
	v1Event := UserCreatedV1{UserID: "1", Name: "Alice Smith"}

	// Manually store the event with proper setup
	v1Data, _ := json.Marshal(v1Event)
	ctx := context.Background()
	event := &Event{
		Type:      "eventbus.UserCreatedV1",
		Data:      v1Data,
		Timestamp: time.Now(),
	}
	offset, _ := store.Append(ctx, event)

	// Update bus lastOffset to match
	bus.storeMu.Lock()
	bus.lastOffset = offset
	bus.storeMu.Unlock()

	// Subscribe expecting V2 events
	var receivedEvents []UserCreatedV2
	err = SubscribeWithReplay(bus, "test-subscription", func(event UserCreatedV2) {
		receivedEvents = append(receivedEvents, event)
	})
	if err != nil {
		t.Fatalf("Failed to subscribe with replay: %v", err)
	}

	// Verify the V1 event was upcasted to V2 and received
	if len(receivedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(receivedEvents))
	}

	if receivedEvents[0].UserID != "1" || receivedEvents[0].FirstName != "Alice" || receivedEvents[0].LastName != "Smith" {
		t.Errorf("Unexpected event data: %+v", receivedEvents[0])
	}
}

// TestReplayWithUpcast tests the ReplayWithUpcast function
func TestReplayWithUpcast(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Register upcast
	err := RegisterUpcast(bus, func(v1 ProductAddedV1) ProductAddedV2 {
		return ProductAddedV2{
			ProductID:   v1.ProductID,
			Name:        v1.Name,
			Price:       v1.Price,
			Currency:    "USD",
			Description: "Migrated product",
		}
	})
	if err != nil {
		t.Fatalf("Failed to register upcast: %v", err)
	}

	// Store a V1 event
	v1Event := ProductAddedV1{ProductID: 1, Name: "Widget", Price: 9.99}
	v1Data, _ := json.Marshal(v1Event)
	ctx := context.Background()
	event := &Event{
		Type:      "eventbus.ProductAddedV1",
		Data:      v1Data,
		Timestamp: time.Now(),
	}
	offset, _ := store.Append(ctx, event)

	// Update bus lastOffset to match
	bus.storeMu.Lock()
	bus.lastOffset = offset
	bus.storeMu.Unlock()

	// Replay with upcast
	var replayed []*StoredEvent
	err = bus.ReplayWithUpcast(ctx, OffsetOldest, func(event *StoredEvent) error {
		replayed = append(replayed, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayWithUpcast failed: %v", err)
	}

	if len(replayed) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(replayed))
	}

	// Verify the event was upcasted
	if replayed[0].Type != "eventbus.ProductAddedV2" {
		t.Errorf("Expected type ProductAddedV2, got %s", replayed[0].Type)
	}

	var v2 ProductAddedV2
	if err := json.Unmarshal(replayed[0].Data, &v2); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if v2.Currency != "USD" || v2.Description != "Migrated product" {
		t.Errorf("Unexpected upcasted data: %+v", v2)
	}
}

// TestWithUpcastOption tests the WithUpcast option
func TestWithUpcastOption(t *testing.T) {
	upcastFunc := func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "NewType", nil
	}

	bus := New(WithUpcast("OldType", "NewType", upcastFunc))

	// Verify the upcast was registered
	testData := json.RawMessage(`{"test": "data"}`)
	resultData, resultType, err := bus.upcastRegistry.apply(testData, "OldType")
	if err != nil {
		t.Fatalf("Failed to apply upcast: %v", err)
	}

	if resultType != "NewType" {
		t.Errorf("Expected type NewType, got %s", resultType)
	}

	if string(resultData) != string(testData) {
		t.Errorf("Data was unexpectedly modified")
	}
}

// TestSetUpcastErrorHandler tests runtime error handler configuration
func TestSetUpcastErrorHandler(t *testing.T) {
	var handlerCalled bool
	bus := New()

	// Set error handler at runtime
	bus.SetUpcastErrorHandler(func(eventType string, data json.RawMessage, err error) {
		handlerCalled = true
	})

	// Register a failing upcast
	RegisterUpcastFunc(bus, "Fail", "Target", func(data json.RawMessage) (json.RawMessage, string, error) {
		return nil, "", errors.New("test error")
	})

	// Trigger the error
	bus.upcastRegistry.apply(json.RawMessage(`{}`), "Fail")

	if !handlerCalled {
		t.Error("Error handler was not called")
	}
}

// TestClearUpcasts tests clearing all upcasts
func TestClearUpcasts(t *testing.T) {
	bus := New()

	// Register some upcasts
	RegisterUpcastFunc(bus, "Type1", "Type2", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "Type2", nil
	})
	RegisterUpcastFunc(bus, "Type3", "Type4", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "Type4", nil
	})

	// Clear all upcasts
	bus.ClearUpcasts()

	// Verify no upcasts are applied
	testData := json.RawMessage(`{}`)
	_, resultType, _ := bus.upcastRegistry.apply(testData, "Type1")
	if resultType != "Type1" {
		t.Error("Expected no upcast to be applied after clear")
	}
}

// TestClearUpcastsForType tests clearing upcasts for specific type
func TestClearUpcastsForType(t *testing.T) {
	bus := New()

	// Register upcasts for different types
	RegisterUpcastFunc(bus, "Type1", "Type2", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "Type2", nil
	})
	RegisterUpcastFunc(bus, "Type3", "Type4", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "Type4", nil
	})

	// Clear upcasts for Type1 only
	bus.ClearUpcastsForType("Type1")

	// Verify Type1 upcast is cleared
	testData := json.RawMessage(`{}`)
	_, resultType1, _ := bus.upcastRegistry.apply(testData, "Type1")
	if resultType1 != "Type1" {
		t.Error("Expected Type1 upcast to be cleared")
	}

	// Verify Type3 upcast still works
	_, resultType3, _ := bus.upcastRegistry.apply(testData, "Type3")
	if resultType3 != "Type4" {
		t.Error("Expected Type3 upcast to still work")
	}
}

// TestUpcastValidation tests various validation scenarios
func TestUpcastValidation(t *testing.T) {
	bus := New()

	// Test empty from type
	err := RegisterUpcastFunc(bus, "", "Type2", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "Type2", nil
	})
	if err == nil || !strings.Contains(err.Error(), "types cannot be empty") {
		t.Error("Expected error for empty from type")
	}

	// Test empty to type
	err = RegisterUpcastFunc(bus, "Type1", "", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "", nil
	})
	if err == nil || !strings.Contains(err.Error(), "types cannot be empty") {
		t.Error("Expected error for empty to type")
	}

	// Test same from and to type
	err = RegisterUpcastFunc(bus, "SameType", "SameType", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "SameType", nil
	})
	if err == nil || !strings.Contains(err.Error(), "cannot upcast type to itself") {
		t.Error("Expected error for same from and to type")
	}

	// Test nil upcast function
	err = RegisterUpcastFunc(bus, "Type1", "Type2", nil)
	if err == nil || !strings.Contains(err.Error(), "upcast function cannot be nil") {
		t.Error("Expected error for nil upcast function")
	}
}

// TestUpcastMarshalError tests handling of marshal errors in upcast
func TestUpcastMarshalError(t *testing.T) {
	bus := New()

	// Register an upcast that returns unmarshalable data
	err := RegisterUpcast(bus, func(v1 UserCreatedV1) UserCreatedV2 {
		// This will work in the typed function
		return UserCreatedV2{
			UserID:    v1.UserID,
			FirstName: "Test",
			LastName:  "User",
		}
	})
	if err != nil {
		t.Fatalf("Failed to register upcast: %v", err)
	}

	// Test with invalid JSON data
	invalidJSON := json.RawMessage(`{"invalid json`)
	_, _, err = bus.upcastRegistry.apply(invalidJSON, "eventbus.UserCreatedV1")
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

// TestMultipleUpcastsForSameType tests handling multiple upcasts from same source
func TestMultipleUpcastsForSameType(t *testing.T) {
	bus := New()

	// Register first upcast from Type1
	err := RegisterUpcastFunc(bus, "Type1", "Type2A", func(data json.RawMessage) (json.RawMessage, string, error) {
		return append(data, []byte(`"A"`)...), "Type2A", nil
	})
	if err != nil {
		t.Fatalf("Failed to register first upcast: %v", err)
	}

	// Register second upcast from Type1 (different target)
	err = RegisterUpcastFunc(bus, "Type1", "Type2B", func(data json.RawMessage) (json.RawMessage, string, error) {
		return append(data, []byte(`"B"`)...), "Type2B", nil
	})
	if err != nil {
		t.Fatalf("Failed to register second upcast: %v", err)
	}

	// Apply upcast - should use the first one registered
	testData := json.RawMessage(`{"test":`)
	resultData, resultType, err := bus.upcastRegistry.apply(testData, "Type1")
	if err != nil {
		t.Fatalf("Failed to apply upcast: %v", err)
	}

	// Should apply the first upcast registered
	if resultType != "Type2A" {
		t.Errorf("Expected Type2A, got %s", resultType)
	}

	if !strings.Contains(string(resultData), "A") {
		t.Error("Expected data to contain 'A'")
	}
}

// TestNoUpcastForType tests that events without upcasts pass through unchanged
func TestNoUpcastForType(t *testing.T) {
	bus := New()

	// Register an upcast for a different type
	RegisterUpcastFunc(bus, "OtherType", "NewType", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "NewType", nil
	})

	// Try to apply upcast for a type without registered upcasts
	testData := json.RawMessage(`{"original": "data"}`)
	resultData, resultType, err := bus.upcastRegistry.apply(testData, "UnregisteredType")

	if err != nil {
		t.Errorf("Expected no error for unregistered type, got %v", err)
	}

	if resultType != "UnregisteredType" {
		t.Errorf("Expected type to remain unchanged, got %s", resultType)
	}

	if string(resultData) != string(testData) {
		t.Error("Expected data to remain unchanged")
	}
}

// TestReplayWithUpcastNilRegistry tests ReplayWithUpcast with nil registry
func TestReplayWithUpcastNilRegistry(t *testing.T) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	// Set registry to nil to test the path
	bus.upcastRegistry = nil

	// Store an event
	ctx := context.Background()
	testData := json.RawMessage(`{"test": "data"}`)
	event := &Event{
		Type:      "TestType",
		Data:      testData,
		Timestamp: time.Now(),
	}
	offset, _ := store.Append(ctx, event)
	bus.lastOffset = offset

	// Replay with nil registry should still work
	var replayed []*StoredEvent
	err := bus.ReplayWithUpcast(ctx, OffsetOldest, func(event *StoredEvent) error {
		replayed = append(replayed, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayWithUpcast failed: %v", err)
	}

	if len(replayed) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(replayed))
	}

	// Event should be unchanged
	if replayed[0].Type != "TestType" {
		t.Errorf("Expected type TestType, got %s", replayed[0].Type)
	}
}

// TestComplexCycleDFS tests more complex cycle detection scenarios
func TestComplexCycleDFS(t *testing.T) {
	bus := New()

	// Create a more complex graph: A -> B -> C -> D
	RegisterUpcastFunc(bus, "A", "B", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "B", nil
	})
	RegisterUpcastFunc(bus, "B", "C", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "C", nil
	})
	RegisterUpcastFunc(bus, "C", "D", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "D", nil
	})

	// Try to add D -> B (should fail - creates cycle)
	err := RegisterUpcastFunc(bus, "D", "B", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "B", nil
	})
	if err == nil || !strings.Contains(err.Error(), "circular dependency") {
		t.Error("Expected circular dependency error for D->B")
	}

	// Try to add D -> A (should fail - creates cycle)
	err = RegisterUpcastFunc(bus, "D", "A", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "A", nil
	})
	if err == nil || !strings.Contains(err.Error(), "circular dependency") {
		t.Error("Expected circular dependency error for D->A")
	}

	// Adding D -> E should work (no cycle)
	err = RegisterUpcastFunc(bus, "D", "E", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "E", nil
	})
	if err != nil {
		t.Errorf("Expected no error for D->E, got %v", err)
	}
}

// TestUpcastLoopDetection tests loop detection during upcast application
func TestUpcastLoopDetection(t *testing.T) {
	bus := New()

	// Manually create a scenario that could cause a loop if not handled
	// This tests the appliedTypes check in the apply function
	count := 0
	RegisterUpcastFunc(bus, "LoopType", "LoopType2", func(data json.RawMessage) (json.RawMessage, string, error) {
		count++
		if count > 1 {
			// Return the same type to simulate a potential loop
			return data, "LoopType", nil
		}
		return data, "LoopType2", nil
	})

	testData := json.RawMessage(`{"test": "data"}`)
	_, _, err := bus.upcastRegistry.apply(testData, "LoopType")

	// Should handle this gracefully without infinite loop
	if err != nil && strings.Contains(err.Error(), "loop detected") {
		// This is expected if loop is detected
		return
	}
}

// TestRegisterUpcastMarshalError tests marshal error handling in RegisterUpcast
func TestRegisterUpcastMarshalError(t *testing.T) {
	bus := New()

	// Create a type that can't be marshaled
	type BadType struct {
		Ch chan int
	}

	// This should handle marshal errors gracefully
	err := RegisterUpcast(bus, func(v1 UserCreatedV1) BadType {
		return BadType{Ch: make(chan int)}
	})

	// The registration should succeed, but using it will fail
	if err != nil {
		t.Fatalf("Registration should succeed even with unmarshalable type: %v", err)
	}

	// Now try to apply the upcast - this will fail during marshal
	v1 := UserCreatedV1{UserID: "1", Name: "Test"}
	v1Data, _ := json.Marshal(v1)

	_, _, err = bus.upcastRegistry.apply(v1Data, "eventbus.UserCreatedV1")
	if err == nil {
		t.Error("Expected error when marshaling channel type")
	}
}

// TestHasCycleDFSVisitedPath tests the visited path in cycle detection
func TestHasCycleDFSVisitedPath(t *testing.T) {
	bus := New()

	// Create a diamond pattern: A -> B, A -> C, B -> D, C -> D
	RegisterUpcastFunc(bus, "A", "B", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "B", nil
	})
	RegisterUpcastFunc(bus, "A", "C", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "C", nil
	})
	RegisterUpcastFunc(bus, "B", "D", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "D", nil
	})
	RegisterUpcastFunc(bus, "C", "D", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "D", nil
	})

	// D -> A would create a cycle
	err := RegisterUpcastFunc(bus, "D", "A", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "A", nil
	})
	if err == nil || !strings.Contains(err.Error(), "circular dependency") {
		t.Error("Expected circular dependency error")
	}

	// But D -> E should be fine
	err = RegisterUpcastFunc(bus, "D", "E", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "E", nil
	})
	if err != nil {
		t.Errorf("D -> E should not create cycle: %v", err)
	}
}

// TestApplyUpcastWithoutErrorHandler tests apply without error handler set
func TestApplyUpcastWithoutErrorHandler(t *testing.T) {
	bus := New()
	// Don't set error handler to test that path

	// Register a failing upcast
	RegisterUpcastFunc(bus, "Fail", "Target", func(data json.RawMessage) (json.RawMessage, string, error) {
		return nil, "", errors.New("test error")
	})

	// Apply should still return error even without handler
	testData := json.RawMessage(`{}`)
	_, _, err := bus.upcastRegistry.apply(testData, "Fail")
	if err == nil {
		t.Error("Expected error even without error handler")
	}
}

// TestHasCycleDFSWithVisitedNode tests the visited node branch in cycle detection
func TestHasCycleDFSWithVisitedNode(t *testing.T) {
	bus := New()

	// Create a complex graph with multiple paths to the same node
	// A has two upcasters: A->B and A->C
	bus.upcastRegistry.mu.Lock()
	bus.upcastRegistry.upcasters["A"] = []Upcaster{
		{FromType: "A", ToType: "B", Upcast: func(data json.RawMessage) (json.RawMessage, string, error) {
			return data, "B", nil
		}},
		{FromType: "A", ToType: "C", Upcast: func(data json.RawMessage) (json.RawMessage, string, error) {
			return data, "C", nil
		}},
	}
	// Both B and C lead to D
	bus.upcastRegistry.upcasters["B"] = []Upcaster{
		{FromType: "B", ToType: "D", Upcast: func(data json.RawMessage) (json.RawMessage, string, error) {
			return data, "D", nil
		}},
	}
	bus.upcastRegistry.upcasters["C"] = []Upcaster{
		{FromType: "C", ToType: "D", Upcast: func(data json.RawMessage) (json.RawMessage, string, error) {
			return data, "D", nil
		}},
	}
	bus.upcastRegistry.mu.Unlock()

	// Add D -> A to create a cycle
	bus.upcastRegistry.mu.Lock()
	bus.upcastRegistry.upcasters["D"] = []Upcaster{
		{FromType: "D", ToType: "A", Upcast: func(data json.RawMessage) (json.RawMessage, string, error) {
			return data, "A", nil
		}},
	}
	bus.upcastRegistry.mu.Unlock()

	// Now check if the cycle exists by checking from A
	// This will force the DFS to visit nodes through multiple paths
	visited := make(map[string]bool)
	hasCycle := bus.upcastRegistry.hasCycleDFS("A", "A", visited)

	if !hasCycle {
		t.Error("Expected cycle to be detected from A to A")
	}

	// Test the visited branch - when searching for a different target through the same graph
	// The DFS will mark nodes as visited and hit the visited[current] branch
	visited2 := make(map[string]bool)
	// First mark A as visited
	visited2["A"] = true
	// Now check from A again with A already marked - this tests the visited branch
	hasCycle2 := bus.upcastRegistry.hasCycleDFS("A", "NonExistent", visited2)
	if hasCycle2 {
		t.Error("Should not find path to NonExistent")
	}
}

// TestUpcastLoopDetectionInApply tests actual loop detection during apply
func TestUpcastLoopDetectionInApply(t *testing.T) {
	bus := New()

	// Create an upcast that returns a type we've already processed
	// This simulates a corrupted upcast function that creates a loop
	RegisterUpcastFunc(bus, "Type1", "Type2", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "Type2", nil
	})

	// Type2 -> Type3
	RegisterUpcastFunc(bus, "Type2", "Type3", func(data json.RawMessage) (json.RawMessage, string, error) {
		return data, "Type3", nil
	})

	// Type3 -> Type1 (this creates a potential loop)
	// Note: This shouldn't be registered due to cycle detection, but we'll simulate it
	// by manually adding it to test the runtime loop detection
	bus.upcastRegistry.mu.Lock()
	bus.upcastRegistry.upcasters["Type3"] = []Upcaster{
		{
			FromType: "Type3",
			ToType:   "Type1", // Loop back to Type1
			Upcast: func(data json.RawMessage) (json.RawMessage, string, error) {
				return data, "Type1", nil
			},
		},
	}
	bus.upcastRegistry.mu.Unlock()

	// Now try to apply - should detect loop
	testData := json.RawMessage(`{"test": "data"}`)
	_, _, err := bus.upcastRegistry.apply(testData, "Type1")

	if err == nil || !strings.Contains(err.Error(), "upcast loop detected") {
		t.Errorf("Expected loop detection error, got: %v", err)
	}
}
