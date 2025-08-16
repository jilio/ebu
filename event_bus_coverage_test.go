package eventbus

import (
	"context"
	"reflect"
	"testing"
)

// Tests for achieving 100% coverage

// TestExtensions tests the SetExtension and GetExtension functions
func TestExtensions(t *testing.T) {
	bus := New()

	// Test setting and getting an extension
	SetExtension(bus, "test-ext", "test-value")

	val, ok := GetExtension(bus, "test-ext")
	if !ok {
		t.Error("Expected extension to be found")
	}
	if val != "test-value" {
		t.Errorf("Expected 'test-value', got %v", val)
	}

	// Test getting non-existent extension
	val, ok = GetExtension(bus, "non-existent")
	if ok {
		t.Error("Expected extension not to be found")
	}
	if val != nil {
		t.Errorf("Expected nil, got %v", val)
	}

	// Test overwriting extension
	SetExtension(bus, "test-ext", "new-value")
	val, ok = GetExtension(bus, "test-ext")
	if !ok {
		t.Error("Expected extension to be found")
	}
	if val != "new-value" {
		t.Errorf("Expected 'new-value', got %v", val)
	}

	// Test with complex types
	type customData struct {
		ID   int
		Name string
	}
	data := customData{ID: 1, Name: "test"}
	SetExtension(bus, "complex", data)

	val, ok = GetExtension(bus, "complex")
	if !ok {
		t.Error("Expected complex extension to be found")
	}
	if retrieved, ok := val.(customData); !ok || retrieved != data {
		t.Errorf("Expected %v, got %v", data, val)
	}
}

// TestWithBeforePublish tests the WithBeforePublish option
func TestWithBeforePublish(t *testing.T) {
	var hookCalled bool
	var capturedType reflect.Type
	var capturedEvent any

	bus := New(WithBeforePublish(func(eventType reflect.Type, event any) {
		hookCalled = true
		capturedType = eventType
		capturedEvent = event
	}))

	// Subscribe to ensure the event is handled
	Subscribe(bus, func(e UserEvent) {})

	// Publish an event
	event := UserEvent{UserID: "test", Action: "action"}
	Publish(bus, event)

	if !hookCalled {
		t.Error("Before publish hook was not called")
	}
	if capturedType != reflect.TypeOf(event) {
		t.Errorf("Expected event type %v, got %v", reflect.TypeOf(event), capturedType)
	}
	if capturedEvent != event {
		t.Errorf("Expected event %v, got %v", event, capturedEvent)
	}
}

// TestWithAfterPublish tests the WithAfterPublish option
func TestWithAfterPublish(t *testing.T) {
	var hookCalled bool
	var capturedType reflect.Type
	var capturedEvent any

	bus := New(WithAfterPublish(func(eventType reflect.Type, event any) {
		hookCalled = true
		capturedType = eventType
		capturedEvent = event
	}))

	// Subscribe to ensure the event is handled
	Subscribe(bus, func(e UserEvent) {})

	// Publish an event
	event := UserEvent{UserID: "test", Action: "action"}
	Publish(bus, event)

	if !hookCalled {
		t.Error("After publish hook was not called")
	}
	if capturedType != reflect.TypeOf(event) {
		t.Errorf("Expected event type %v, got %v", reflect.TypeOf(event), capturedType)
	}
	if capturedEvent != event {
		t.Errorf("Expected event %v, got %v", event, capturedEvent)
	}
}

// TestCallHandlerWithContextEdgeCases tests additional branches in callHandlerWithContext
func TestCallHandlerWithContextEdgeCases(t *testing.T) {
	// Test with func(any) handler
	var called1 bool

	// Test with func(context.Context, any) error handler
	var called2 bool
	h := &internalHandler{
		handler: func(ctx context.Context, event any) error {
			called2 = true
			return nil
		},
		handlerType: reflect.TypeOf(func(ctx context.Context, event any) error { return nil }),
		eventType:   reflect.TypeOf(UserEvent{}),
	}

	// Call the handler directly to test the edge case
	callHandlerWithContext(h, context.Background(), UserEvent{UserID: "test"}, nil)

	// The func(any) handler is tested separately with a simple type assertion test
	anyHandler := func(e any) {
		if _, ok := e.(UserEvent); ok {
			called1 = true
		}
	}

	h2 := &internalHandler{
		handler:     anyHandler,
		handlerType: reflect.TypeOf(anyHandler),
		eventType:   reflect.TypeOf(UserEvent{}),
	}

	callHandlerWithContext(h2, context.Background(), UserEvent{UserID: "test"}, nil)

	if !called1 {
		t.Error("Handler 1 was not called")
	}
	if !called2 {
		t.Error("Handler 2 was not called")
	}
}

// TestReflectionBasedHandlers tests reflection-based handler calls
func TestReflectionBasedHandlers(t *testing.T) {
	// Test handler with single parameter via reflection
	var called1 bool
	customHandler1 := func(e UserEvent) {
		called1 = true
	}

	h1 := &internalHandler{
		handler:     customHandler1,
		handlerType: reflect.TypeOf(customHandler1),
		eventType:   reflect.TypeOf(UserEvent{}),
	}

	// Test handler with two parameters via reflection
	var called2 bool
	customHandler2 := func(ctx context.Context, e UserEvent) {
		called2 = true
		// Verify context is passed correctly
		if ctx == nil {
			t.Error("Context should not be nil")
		}
	}

	h2 := &internalHandler{
		handler:        customHandler2,
		handlerType:    reflect.TypeOf(customHandler2),
		eventType:      reflect.TypeOf(UserEvent{}),
		acceptsContext: true,
	}

	// Call handlers directly
	ctx := context.Background()
	callHandlerWithContext(h1, ctx, UserEvent{UserID: "test"}, nil)
	callHandlerWithContext(h2, ctx, UserEvent{UserID: "test"}, nil)

	if !called1 {
		t.Error("Reflection-based handler 1 was not called")
	}
	if !called2 {
		t.Error("Reflection-based handler 2 was not called")
	}
}

// TestSequentialHandlerWithReflection tests sequential handler with reflection
func TestSequentialHandlerWithReflection(t *testing.T) {
	var callCount int
	customHandler := func(e UserEvent) {
		callCount++
	}

	h := &internalHandler{
		handler:     customHandler,
		handlerType: reflect.TypeOf(customHandler),
		eventType:   reflect.TypeOf(UserEvent{}),
		sequential:  true,
	}

	// Call handler multiple times to ensure sequential execution
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		callHandlerWithContext(h, ctx, UserEvent{UserID: "test"}, nil)
	}

	if callCount != 3 {
		t.Errorf("Expected 3 calls, got %d", callCount)
	}
}
