package eventbus

import (
	"context"
	"reflect"
	"testing"
)

// TestDirectReflectionPath tests the reflection path directly
func TestDirectReflectionPath(t *testing.T) {
	// Test the numIn == 1 case directly
	type CustomStruct struct {
		Value string
	}

	called := false

	// Create a raw function that won't match any type assertions
	rawHandler := interface{}(func(c CustomStruct) {
		called = true
		if c.Value != "test" {
			t.Errorf("expected Value='test', got %s", c.Value)
		}
	})

	h := &internalHandler{
		handler:     rawHandler,
		handlerType: reflect.TypeOf(rawHandler),
	}

	// Call directly with a context to ensure we go through callHandlerWithContext
	callHandlerWithContext(h, context.Background(), CustomStruct{Value: "test"}, nil)

	if !called {
		t.Error("handler was not called")
	}
}

// TestReflectionWithRawInterface tests with a completely raw interface handler
func TestReflectionWithRawInterface(t *testing.T) {
	bus := New()

	// Create a raw handler as interface{} to bypass type assertions
	type MyEvent struct {
		ID int
	}

	called := false
	var rawHandler interface{} = func(e MyEvent) {
		called = true
		if e.ID != 123 {
			t.Errorf("expected ID=123, got %d", e.ID)
		}
	}

	// Manually register the handler to bypass Subscribe's type constraints
	eventType := reflect.TypeOf(MyEvent{})
	h := &internalHandler{
		handler:     rawHandler,
		handlerType: reflect.TypeOf(rawHandler),
		eventType:   eventType,
	}

	shard := bus.getShard(eventType)
	shard.mu.Lock()
	shard.handlers[eventType] = append(shard.handlers[eventType], h)
	shard.mu.Unlock()

	// Publish the event
	Publish(bus, MyEvent{ID: 123})

	if !called {
		t.Error("handler was not called")
	}
}

// TestReflectionNumInTwoDirect tests the numIn == 2 reflection path (line 371-372)
func TestReflectionNumInTwoDirect(t *testing.T) {
	// Test handler with 2 parameters (context + event) via reflection
	// We need to ensure T doesn't match the event type
	type MyContextEvent struct {
		Message string
	}

	called := false
	// Use a handler with different event type than what we pass to callHandlerWithContext
	// This will force the reflection path since the type assertion won't match
	var rawHandler interface{} = func(ctx context.Context, e MyContextEvent) {
		called = true
		if e.Message != "hello" {
			t.Errorf("expected Message='hello', got %s", e.Message)
		}
		if ctx == nil {
			t.Error("expected non-nil context")
		}
	}

	h := &internalHandler{
		handler:     rawHandler,
		handlerType: reflect.TypeOf(rawHandler),
	}

	// Call with interface{} type parameter to bypass type assertions
	// This simulates the generic T being different from MyContextEvent
	callHandlerWithContext[interface{}](h, context.Background(), MyContextEvent{Message: "hello"}, nil)

	if !called {
		t.Error("handler was not called")
	}
}

// TestReflectionNumInOneDirect specifically tests line 369
func TestReflectionNumInOneDirect(t *testing.T) {
	// Create various handlers that will fall through to reflection
	testCases := []struct {
		name    string
		handler interface{}
		event   interface{}
	}{
		{
			name: "struct_handler",
			handler: func(s struct{ X int }) {
				if s.X != 42 {
					t.Errorf("expected X=42, got %d", s.X)
				}
			},
			event: struct{ X int }{X: 42},
		},
		{
			name: "array_handler",
			handler: func(a [3]int) {
				if a[0] != 1 || a[1] != 2 || a[2] != 3 {
					t.Errorf("unexpected array: %v", a)
				}
			},
			event: [3]int{1, 2, 3},
		},
		{
			name: "complex_handler",
			handler: func(c complex128) {
				if real(c) != 3.14 || imag(c) != 2.71 {
					t.Errorf("unexpected complex: %v", c)
				}
			},
			event: complex(3.14, 2.71),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := &internalHandler{
				handler:     tc.handler,
				handlerType: reflect.TypeOf(tc.handler),
			}

			// This should go through the reflection path
			callHandlerWithContext(h, context.Background(), tc.event, nil)
		})
	}
}
