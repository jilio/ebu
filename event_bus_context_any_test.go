package eventbus

import (
	"context"
	"reflect"
	"testing"
)

// TestContextAnyHandlers tests handlers with context and any type signatures
func TestContextAnyHandlers(t *testing.T) {
	t.Run("func(context.Context, any) handler", func(t *testing.T) {
		called := false
		var receivedCtx context.Context
		var receivedEvent any
		
		// Create a handler with func(context.Context, any) signature
		handler := func(ctx context.Context, event any) {
			called = true
			receivedCtx = ctx
			receivedEvent = event
		}
		
		// Create internal handler
		h := &internalHandler{
			handler:     handler,
			handlerType: reflect.TypeOf(handler),
			eventType:   reflect.TypeOf(""),
		}
		
		// Test event
		testEvent := "test event"
		ctx := context.WithValue(context.Background(), "key", "value")
		
		// Call the handler
		callHandlerWithContext(h, ctx, testEvent, nil)
		
		// Verify
		if !called {
			t.Error("Handler was not called")
		}
		if receivedCtx.Value("key") != "value" {
			t.Error("Context was not passed correctly")
		}
		if receivedEvent != testEvent {
			t.Errorf("Event was not passed correctly: got %v, want %v", receivedEvent, testEvent)
		}
	})
	
	t.Run("func(context.Context, any) error handler", func(t *testing.T) {
		called := false
		var receivedCtx context.Context
		var receivedEvent any
		
		// Create a handler with func(context.Context, any) error signature
		handler := func(ctx context.Context, event any) error {
			called = true
			receivedCtx = ctx
			receivedEvent = event
			return nil
		}
		
		// Create internal handler
		h := &internalHandler{
			handler:     handler,
			handlerType: reflect.TypeOf(handler),
			eventType:   reflect.TypeOf(0),
		}
		
		// Test event
		testEvent := 42
		ctx := context.WithValue(context.Background(), "test", "data")
		
		// Call the handler
		callHandlerWithContext(h, ctx, testEvent, nil)
		
		// Verify
		if !called {
			t.Error("Handler was not called")
		}
		if receivedCtx.Value("test") != "data" {
			t.Error("Context was not passed correctly")
		}
		if receivedEvent != testEvent {
			t.Errorf("Event was not passed correctly: got %v, want %v", receivedEvent, testEvent)
		}
	})
	
	t.Run("func(context.Context, any) error handler with struct event", func(t *testing.T) {
		type TestEvent struct {
			Message string
		}
		
		called := false
		var receivedEvent any
		
		// Create a handler with func(context.Context, any) error signature
		handler := func(ctx context.Context, event any) error {
			called = true
			receivedEvent = event
			return nil
		}
		
		// Create internal handler
		h := &internalHandler{
			handler:     handler,
			handlerType: reflect.TypeOf(handler),
			eventType:   reflect.TypeOf(TestEvent{}),
		}
		
		// Test event
		testEvent := TestEvent{Message: "hello"}
		
		// Call the handler
		callHandlerWithContext(h, context.Background(), testEvent, nil)
		
		// Verify
		if !called {
			t.Error("Handler was not called")
		}
		if evt, ok := receivedEvent.(TestEvent); !ok || evt.Message != "hello" {
			t.Errorf("Event was not passed correctly: got %v", receivedEvent)
		}
	})
}