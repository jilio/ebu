package eventbus

import (
	"context"
	"reflect"
	"testing"
)

// TestReflectionHandlerEdgeCases tests the reflection-based handler code paths
func TestReflectionHandlerEdgeCases(t *testing.T) {
	// Test handler with no parameters (edge case that should not execute)
	noParamHandler := func() {}
	h1 := &internalHandler{
		handler:     noParamHandler,
		handlerType: reflect.TypeOf(noParamHandler),
		eventType:   reflect.TypeOf(UserEvent{}),
	}

	// This should not panic, just not execute
	callHandlerWithContext(h1, context.Background(), UserEvent{UserID: "test"}, nil)

	// Test handler with wrong number of parameters (3 params)
	threeParamHandler := func(ctx context.Context, e UserEvent, extra string) {}
	h2 := &internalHandler{
		handler:     threeParamHandler,
		handlerType: reflect.TypeOf(threeParamHandler),
		eventType:   reflect.TypeOf(UserEvent{}),
	}

	// This should not panic, just not execute
	callHandlerWithContext(h2, context.Background(), UserEvent{UserID: "test"}, nil)

	// Test non-function handler type (should hit default case)
	nonFuncHandler := "not a function"
	h3 := &internalHandler{
		handler:     nonFuncHandler,
		handlerType: reflect.TypeOf(nonFuncHandler),
		eventType:   reflect.TypeOf(UserEvent{}),
	}

	// This should not panic, just not execute
	callHandlerWithContext(h3, context.Background(), UserEvent{UserID: "test"}, nil)

	// Test reflection-based single param handler
	var called1 bool
	reflectHandler1 := func(e TestEvent) {
		called1 = true
	}
	h4 := &internalHandler{
		handler:     reflectHandler1,
		handlerType: reflect.TypeOf(reflectHandler1),
		eventType:   reflect.TypeOf(TestEvent{}),
	}

	callHandlerWithContext(h4, context.Background(), TestEvent{ID: 1}, nil)
	if !called1 {
		t.Error("Reflection single param handler should have been called")
	}

	// Test reflection-based two param handler
	var called2 bool
	reflectHandler2 := func(ctx context.Context, e TestEvent) {
		called2 = true
		if ctx == nil {
			t.Error("Context should not be nil")
		}
	}
	h5 := &internalHandler{
		handler:        reflectHandler2,
		handlerType:    reflect.TypeOf(reflectHandler2),
		eventType:      reflect.TypeOf(TestEvent{}),
		acceptsContext: true,
	}

	callHandlerWithContext(h5, context.Background(), TestEvent{ID: 2}, nil)
	if !called2 {
		t.Error("Reflection two param handler should have been called")
	}

	// Test handler with invalid signature (0 params) through reflection path
	zeroParamHandler := func() {}
	h6 := &internalHandler{
		handler:     zeroParamHandler,
		handlerType: reflect.TypeOf(zeroParamHandler),
		eventType:   reflect.TypeOf(TestEvent{}),
	}
	// Force reflection path by using a wrapper
	wrappedHandler := &internalHandler{
		handler:     h6.handler,
		handlerType: h6.handlerType,
		eventType:   h6.eventType,
	}
	// This should hit the reflection default case where numIn == 0
	callHandlerWithContext(wrappedHandler, context.Background(), TestEvent{ID: 3}, nil)
}

// TestStartEventProcessor tests the startEventProcessor method
func TestStartEventProcessor(t *testing.T) {
	bus := New()
	// Just call the method to get coverage - it's currently a no-op
	bus.startEventProcessor()
}

// TestReflectionHandlerNumInBranches tests all branches for numIn checks
func TestReflectionHandlerNumInBranches(t *testing.T) {
	// Test handler with 3 parameters (numIn > 2, should not execute)
	threeParamHandler := func(a, b, c int) {}
	h := &internalHandler{
		handler:     threeParamHandler,
		handlerType: reflect.TypeOf(threeParamHandler),
		eventType:   reflect.TypeOf(UserEvent{}),
	}

	// This should not panic, just not execute (numIn = 3, so neither branch taken)
	callHandlerWithContext(h, context.Background(), UserEvent{UserID: "test"}, nil)
}
