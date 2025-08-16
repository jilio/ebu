package eventbus

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

// TestExtensions tests the extensions functionality
func TestExtensions(t *testing.T) {
	bus := New()

	// Test SetExtension and GetExtension functions
	SetExtension(bus, "key1", "value1")
	SetExtension(bus, "key2", 42)

	val1, ok := GetExtension(bus, "key1")
	if !ok || val1 != "value1" {
		t.Errorf("expected 'value1', got %v", val1)
	}

	val2, ok := GetExtension(bus, "key2")
	if !ok || val2 != 42 {
		t.Errorf("expected 42, got %v", val2)
	}

	// Test non-existent key
	_, ok = GetExtension(bus, "nonexistent")
	if ok {
		t.Error("expected false for non-existent key")
	}

	// Also test direct access to extensions
	bus.extensions.Store("direct", "access")
	val, ok := bus.extensions.Load("direct")
	if !ok || val != "access" {
		t.Errorf("expected 'access', got %v", val)
	}
}

// TestWithBeforePublish tests the before publish hook
func TestWithBeforePublish(t *testing.T) {
	var hookCalled bool
	var hookEvent interface{}

	hook := func(eventType reflect.Type, event interface{}) {
		hookCalled = true
		hookEvent = event
	}

	bus := New(WithBeforePublish(hook))

	type TestEvent struct {
		Message string
	}

	event := TestEvent{Message: "test"}
	Publish(bus, event)

	if !hookCalled {
		t.Error("before publish hook was not called")
	}
	if hookEvent != event {
		t.Error("hook received wrong event")
	}
}

// TestWithAfterPublish tests the after publish hook
func TestWithAfterPublish(t *testing.T) {
	var hookCalled bool
	var hookEvent interface{}

	hook := func(eventType reflect.Type, event interface{}) {
		hookCalled = true
		hookEvent = event
	}

	bus := New(WithAfterPublish(hook))

	type TestEvent struct {
		Message string
	}

	event := TestEvent{Message: "test"}
	Publish(bus, event)

	if !hookCalled {
		t.Error("after publish hook was not called")
	}
	if hookEvent != event {
		t.Error("hook received wrong event")
	}
}

// TestDirectHandlerCalls tests direct handler type assertion paths
func TestDirectHandlerCalls(t *testing.T) {
	t.Run("func_T_handler", func(t *testing.T) {
		// Test the func(T) case directly
		called := false
		var handler interface{} = func(s string) {
			called = true
			if s != "test" {
				t.Errorf("expected 'test', got %s", s)
			}
		}

		h := &internalHandler{
			handler:     handler,
			handlerType: reflect.TypeOf(handler),
		}

		callHandlerWithContext(h, context.Background(), "test", nil)

		if !called {
			t.Error("handler was not called")
		}
	})

	t.Run("func_context_T_handler", func(t *testing.T) {
		// Test the func(context.Context, T) case directly
		called := false
		var handler interface{} = func(ctx context.Context, s string) {
			called = true
			if s != "test" {
				t.Errorf("expected 'test', got %s", s)
			}
			if ctx == nil {
				t.Error("context should not be nil")
			}
		}

		h := &internalHandler{
			handler:     handler,
			handlerType: reflect.TypeOf(handler),
		}

		callHandlerWithContext(h, context.Background(), "test", nil)

		if !called {
			t.Error("handler was not called")
		}
	})

	t.Run("func_any_handler", func(t *testing.T) {
		// Test the func(any) case directly
		called := false
		var handler interface{} = func(event any) {
			called = true
			if s, ok := event.(string); !ok || s != "test" {
				t.Errorf("expected string 'test', got %v", event)
			}
		}

		h := &internalHandler{
			handler:     handler,
			handlerType: reflect.TypeOf(handler),
		}

		callHandlerWithContext(h, context.Background(), "test", nil)

		if !called {
			t.Error("handler was not called")
		}
	})
}

// TestReflectionHandlers tests various reflection-based handler paths
func TestReflectionHandlers(t *testing.T) {
	t.Run("single_parameter_reflection", func(t *testing.T) {
		// Test the numIn == 1 reflection path
		type CustomStruct struct {
			Value string
		}

		called := false
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

		callHandlerWithContext(h, context.Background(), CustomStruct{Value: "test"}, nil)

		if !called {
			t.Error("handler was not called")
		}
	})

	t.Run("two_parameter_reflection", func(t *testing.T) {
		// Test the numIn == 2 reflection path
		type MyContextEvent struct {
			Message string
		}

		called := false
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
		callHandlerWithContext[interface{}](h, context.Background(), MyContextEvent{Message: "hello"}, nil)

		if !called {
			t.Error("handler was not called")
		}
	})

	t.Run("non_function_handler", func(t *testing.T) {
		// Test with a non-function handler (should not panic, just not call anything)
		h := &internalHandler{
			handler:     "not a function",
			handlerType: reflect.TypeOf("not a function"),
		}

		// Should not panic
		callHandlerWithContext(h, context.Background(), "event", nil)
	})

	t.Run("zero_parameter_handler", func(t *testing.T) {
		// Test handler with no parameters
		called := false
		var rawHandler interface{} = func() {
			called = true
		}

		h := &internalHandler{
			handler:     rawHandler,
			handlerType: reflect.TypeOf(rawHandler),
		}

		callHandlerWithContext(h, context.Background(), "event", nil)

		// Should not be called since it has 0 parameters
		if called {
			t.Error("handler with 0 parameters should not be called")
		}
	})

	t.Run("three_parameter_handler", func(t *testing.T) {
		// Test handler with 3 parameters
		called := false
		var rawHandler interface{} = func(ctx context.Context, event string, extra int) {
			called = true
		}

		h := &internalHandler{
			handler:     rawHandler,
			handlerType: reflect.TypeOf(rawHandler),
		}

		callHandlerWithContext(h, context.Background(), "event", nil)

		// Should not be called since it has 3 parameters
		if called {
			t.Error("handler with 3 parameters should not be called")
		}
	})

	t.Run("sequential_with_reflection", func(t *testing.T) {
		// Test sequential handler with reflection
		type SeqEvent struct{ ID int }

		var mu sync.Mutex
		var callOrder []int

		h := &internalHandler{
			handler: interface{}(func(e SeqEvent) {
				mu.Lock()
				callOrder = append(callOrder, e.ID)
				mu.Unlock()
			}),
			handlerType: reflect.TypeOf(func(e SeqEvent) {}),
			sequential:  true,
		}

		// Call multiple times concurrently
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				callHandlerWithContext(h, context.Background(), SeqEvent{ID: id}, nil)
			}(i)
		}
		wg.Wait()

		// Check all were called
		if len(callOrder) != 5 {
			t.Errorf("expected 5 calls, got %d", len(callOrder))
		}
	})
}

// TestEdgeCases tests various edge cases
func TestEdgeCases(t *testing.T) {
	t.Run("raw_interface_handler", func(t *testing.T) {
		bus := New()

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

		Publish(bus, MyEvent{ID: 123})

		if !called {
			t.Error("handler was not called")
		}
	})

	t.Run("various_event_types", func(t *testing.T) {
		// Test with different types to ensure reflection paths work
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

				callHandlerWithContext(h, context.Background(), tc.event, nil)
			})
		}
	})
}

// TestReflectionNumInBranches tests all branches in reflection handler calls
func TestReflectionNumInBranches(t *testing.T) {
	t.Run("numIn_0", func(t *testing.T) {
		var called atomic.Bool
		h := &internalHandler{
			handler:     func() { called.Store(true) },
			handlerType: reflect.TypeOf(func() {}),
		}
		callHandlerWithContext(h, context.Background(), "event", nil)
		if called.Load() {
			t.Error("handler with 0 parameters should not be called")
		}
	})

	t.Run("numIn_1", func(t *testing.T) {
		var called atomic.Bool
		// Use a custom type to force reflection path
		type CustomType struct{ Value string }
		h := &internalHandler{
			handler:     interface{}(func(c CustomType) { called.Store(true) }),
			handlerType: reflect.TypeOf(func(c CustomType) {}),
		}
		callHandlerWithContext(h, context.Background(), CustomType{Value: "test"}, nil)
		if !called.Load() {
			t.Error("handler with 1 parameter should be called")
		}
	})

	t.Run("numIn_2", func(t *testing.T) {
		var called atomic.Bool
		h := &internalHandler{
			handler: interface{}(func(ctx context.Context, s string) {
				called.Store(true)
				if ctx == nil {
					t.Error("context should not be nil")
				}
			}),
			handlerType: reflect.TypeOf(func(context.Context, string) {}),
		}
		callHandlerWithContext[interface{}](h, context.Background(), "test", nil)
		if !called.Load() {
			t.Error("handler with 2 parameters should be called")
		}
	})

	t.Run("numIn_3", func(t *testing.T) {
		var called atomic.Bool
		h := &internalHandler{
			handler:     interface{}(func(ctx context.Context, s string, extra int) { called.Store(true) }),
			handlerType: reflect.TypeOf(func(context.Context, string, int) {}),
		}
		callHandlerWithContext(h, context.Background(), "test", nil)
		if called.Load() {
			t.Error("handler with 3 parameters should not be called")
		}
	})
}
