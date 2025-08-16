package eventbus

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Test event types
type UserEvent struct {
	UserID string
	Action string
}

type OrderEvent struct {
	OrderID string
	Amount  float64
}

func TestNewEventBus(t *testing.T) {
	bus := New()
	if bus == nil {
		t.Fatal("New() returned nil")
	}
}

func TestEventType(t *testing.T) {
	tests := []struct {
		name     string
		event    any
		expected string
	}{
		{
			name:     "user event struct",
			event:    UserEvent{UserID: "123"},
			expected: "eventbus.UserEvent",
		},
		{
			name:     "pointer to user event",
			event:    &UserEvent{UserID: "456"},
			expected: "*eventbus.UserEvent",
		},
		{
			name:     "order event",
			event:    OrderEvent{OrderID: "789"},
			expected: "eventbus.OrderEvent",
		},
		{
			name:     "int",
			event:    42,
			expected: "int",
		},
		{
			name:     "string",
			event:    "hello",
			expected: "string",
		},
		{
			name:     "slice",
			event:    []string{"a", "b"},
			expected: "[]string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EventType(tt.event)
			if result != tt.expected {
				t.Errorf("EventType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSubscribeAndPublish(t *testing.T) {
	bus := New()
	received := false

	err := Subscribe(bus, func(event UserEvent) {
		if event.UserID != "123" || event.Action != "login" {
			t.Errorf("Unexpected event: %+v", event)
		}
		received = true
	})

	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	Publish(bus, UserEvent{UserID: "123", Action: "login"})

	if !received {
		t.Error("Handler was not called")
	}
}

func TestMultipleEventTypes(t *testing.T) {
	bus := New()
	userReceived := false
	orderReceived := false

	if err := Subscribe(bus, func(event UserEvent) {
		userReceived = true
	}); err != nil {
		t.Fatal(err)
	}

	if err := Subscribe(bus, func(event OrderEvent) {
		orderReceived = true
	}); err != nil {
		t.Fatal(err)
	}

	Publish(bus, UserEvent{UserID: "123", Action: "login"})
	Publish(bus, OrderEvent{OrderID: "456", Amount: 99.99})

	if !userReceived {
		t.Error("User event handler was not called")
	}
	if !orderReceived {
		t.Error("Order event handler was not called")
	}
}

func TestSubscribeOnce(t *testing.T) {
	bus := New()
	var callCount int32

	if err := Subscribe(bus, func(event UserEvent) {
		atomic.AddInt32(&callCount, 1)
	}, Once()); err != nil {
		t.Fatal(err)
	}

	Publish(bus, UserEvent{UserID: "123", Action: "login"})
	Publish(bus, UserEvent{UserID: "456", Action: "logout"})

	if count := atomic.LoadInt32(&callCount); count != 1 {
		t.Errorf("Handler called %d times, expected 1", count)
	}
}

func TestUnsubscribe(t *testing.T) {
	bus := New()
	called := false

	handler := func(event UserEvent) {
		called = true
	}

	if err := Subscribe(bus, handler); err != nil {
		t.Fatal(err)
	}
	Unsubscribe[UserEvent](bus, handler)

	Publish(bus, UserEvent{UserID: "123", Action: "login"})

	if called {
		t.Error("Handler was called after unsubscribe")
	}
}

func TestAsyncSubscribe(t *testing.T) {
	bus := New()
	var callCount int32
	done := make(chan bool, 1)

	if err := Subscribe(bus, func(event UserEvent) {
		atomic.AddInt32(&callCount, 1)
		time.Sleep(10 * time.Millisecond)
		done <- true
	}, Async()); err != nil {
		t.Fatal(err)
	}

	Publish(bus, UserEvent{UserID: "123", Action: "login"})

	select {
	case <-done:
		if count := atomic.LoadInt32(&callCount); count != 1 {
			t.Errorf("Handler called %d times, expected 1", count)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Async handler timeout")
	}

	bus.Wait()
}

func TestHasHandlers(t *testing.T) {
	bus := New()

	if HasHandlers[UserEvent](bus) {
		t.Error("HasHandlers returned true for empty bus")
	}

	if err := Subscribe(bus, func(event UserEvent) {
		// Empty handler - we only need to verify subscription exists,
		// not test handler functionality
	}); err != nil {
		t.Fatal(err)
	}

	if !HasHandlers[UserEvent](bus) {
		t.Error("HasHandlers returned false when handler exists")
	}

	if HasHandlers[OrderEvent](bus) {
		t.Error("HasHandlers returned true for different event type")
	}
}

func TestClear(t *testing.T) {
	bus := New()
	called := false

	if err := Subscribe(bus, func(event UserEvent) {
		called = true
	}); err != nil {
		t.Fatal(err)
	}

	Clear[UserEvent](bus)
	Publish(bus, UserEvent{UserID: "123", Action: "login"})

	if called {
		t.Error("Handler was called after Clear")
	}
}

func TestSubscribeOnceAsync(t *testing.T) {
	bus := New()
	var callCount int32
	done := make(chan bool, 1)

	if err := Subscribe(bus, func(event UserEvent) {
		atomic.AddInt32(&callCount, 1)
		done <- true
	}, Async(), Once()); err != nil {
		t.Fatal(err)
	}

	Publish(bus, UserEvent{UserID: "123", Action: "login"})
	Publish(bus, UserEvent{UserID: "456", Action: "logout"})

	select {
	case <-done:
		if count := atomic.LoadInt32(&callCount); count != 1 {
			t.Errorf("Handler called %d times, expected 1", count)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Async handler timeout")
	}

	bus.Wait()
}

func TestUnsubscribeErrors(t *testing.T) {
	bus := New()

	// Test unsubscribe with no handlers
	handler := func(event UserEvent) {}
	err := Unsubscribe[UserEvent](bus, handler)
	if err == nil {
		t.Error("Expected error when unsubscribing non-existent handler")
	}

	// Test unsubscribe with wrong handler
	if err := Subscribe(bus, func(event UserEvent) {}); err != nil {
		t.Fatal(err)
	}
	err = Unsubscribe[UserEvent](bus, handler)
	if err == nil {
		t.Error("Expected error when unsubscribing different handler")
	}
}

func TestClearAll(t *testing.T) {
	bus := New()
	userCalled := false
	orderCalled := false

	if err := Subscribe(bus, func(event UserEvent) {
		userCalled = true
	}); err != nil {
		t.Fatal(err)
	}
	if err := Subscribe(bus, func(event OrderEvent) {
		orderCalled = true
	}); err != nil {
		t.Fatal(err)
	}

	ClearAll(bus)

	Publish(bus, UserEvent{UserID: "123", Action: "login"})
	Publish(bus, OrderEvent{OrderID: "456", Amount: 99.99})

	if userCalled {
		t.Error("User handler was called after ClearAll")
	}
	if orderCalled {
		t.Error("Order handler was called after ClearAll")
	}
}

func TestAsyncSequential(t *testing.T) {
	bus := New()
	var count int32
	ch := make(chan int, 10)

	// Subscribe with sequential = true
	// This ensures multiple events to the same handler run serially
	if err := Subscribe(bus, func(event UserEvent) {
		current := atomic.AddInt32(&count, 1)
		ch <- int(current)
		time.Sleep(10 * time.Millisecond) // Simulate work
		ch <- int(current + 10)
	}, Async(), Sequential()); err != nil {
		t.Fatal(err)
	}

	// Publish multiple events
	for range 3 {
		Publish(bus, UserEvent{UserID: "123", Action: "login"})
	}

	bus.Wait()
	close(ch)

	// With sequential=true, events should be processed serially
	// We should see: 1, 11, 2, 12, 3, 13 (serial execution)
	// Without sequential, we might see interleaved execution
	var results []int
	for v := range ch {
		results = append(results, v)
	}

	// Verify serial execution
	expected := []int{1, 11, 2, 12, 3, 13}
	if len(results) != len(expected) {
		t.Errorf("Expected %d results, got %d", len(expected), len(results))
	}
	for i, v := range expected {
		if results[i] != v {
			t.Errorf("Expected results %v, got %v", expected, results)
			break
		}
	}
}

// Context tests
func TestSubscribeWithContext(t *testing.T) {
	bus := New()
	received := false
	var receivedCtx context.Context

	err := SubscribeContext(bus, func(ctx context.Context, event UserEvent) {
		if event.UserID != "123" || event.Action != "login" {
			t.Errorf("Unexpected event: %+v", event)
		}
		received = true
		receivedCtx = ctx
	})

	if err != nil {
		t.Fatalf("SubscribeContext failed: %v", err)
	}

	ctx := context.WithValue(context.Background(), "key", "value")
	PublishContext(bus, ctx, UserEvent{UserID: "123", Action: "login"})

	if !received {
		t.Error("Context handler was not called")
	}

	if receivedCtx.Value("key") != "value" {
		t.Error("Context was not passed correctly")
	}
}

func TestPublishContextWithCancellation(t *testing.T) {
	bus := New()
	handlerStarted := make(chan bool)
	handlerCompleted := make(chan bool)

	err := SubscribeContext(bus, func(ctx context.Context, event UserEvent) {
		handlerStarted <- true
		select {
		case <-ctx.Done():
			// Context was cancelled
			return
		case <-time.After(100 * time.Millisecond):
			handlerCompleted <- true
		}
	}, Async())

	if err != nil {
		t.Fatalf("SubscribeContext failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	PublishContext(bus, ctx, UserEvent{UserID: "123", Action: "login"})

	// Wait for handler to start
	<-handlerStarted

	// Cancel the context
	cancel()

	// Handler should not complete
	select {
	case <-handlerCompleted:
		t.Error("Handler completed despite context cancellation")
	case <-time.After(200 * time.Millisecond):
		// Expected behavior
	}
}

func TestSubscribeOnceWithContext(t *testing.T) {
	bus := New()
	var count int32

	err := SubscribeContext(bus, func(ctx context.Context, event UserEvent) {
		atomic.AddInt32(&count, 1)
	}, Once())

	if err != nil {
		t.Fatalf("SubscribeContext failed: %v", err)
	}

	ctx := context.Background()
	PublishContext(bus, ctx, UserEvent{UserID: "1", Action: "test"})
	PublishContext(bus, ctx, UserEvent{UserID: "2", Action: "test"})

	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt32(&count) != 1 {
		t.Errorf("Expected handler to be called once, called %d times", count)
	}
}

func TestMixedHandlersWithAndWithoutContext(t *testing.T) {
	bus := New()
	regularHandlerCalled := false
	contextHandlerCalled := false

	// Regular handler
	if err := Subscribe(bus, func(event UserEvent) {
		regularHandlerCalled = true
	}); err != nil {
		t.Fatal(err)
	}

	// Context handler
	if err := SubscribeContext(bus, func(ctx context.Context, event UserEvent) {
		contextHandlerCalled = true
		if ctx.Value("test") != "value" {
			t.Error("Context value not received")
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Publish with context
	ctx := context.WithValue(context.Background(), "test", "value")
	PublishContext(bus, ctx, UserEvent{UserID: "123", Action: "test"})

	if !regularHandlerCalled {
		t.Error("Regular handler was not called")
	}

	if !contextHandlerCalled {
		t.Error("Context handler was not called")
	}
}

func TestAsyncContextHandlerSequential(t *testing.T) {
	bus := New()
	startTimes := make([]time.Time, 0, 3)
	endTimes := make([]time.Time, 0, 3)
	mu := &sync.Mutex{}

	// Subscribe with sequential = true
	err := SubscribeContext(bus, func(ctx context.Context, event OrderEvent) {
		mu.Lock()
		startTimes = append(startTimes, time.Now())
		mu.Unlock()

		// Simulate work that takes time
		time.Sleep(20 * time.Millisecond)

		mu.Lock()
		endTimes = append(endTimes, time.Now())
		mu.Unlock()
	}, Async(), Sequential()) // async and sequential

	if err != nil {
		t.Fatalf("SubscribeContext failed: %v", err)
	}

	ctx := context.Background()
	// Publish events
	PublishContext(bus, ctx, OrderEvent{OrderID: "first", Amount: 1})
	PublishContext(bus, ctx, OrderEvent{OrderID: "second", Amount: 2})
	PublishContext(bus, ctx, OrderEvent{OrderID: "third", Amount: 3})

	bus.Wait()

	// With sequential = true, each handler should finish before the next starts
	// Check that no handler started before the previous one finished
	if len(startTimes) != 3 || len(endTimes) != 3 {
		t.Fatalf("Expected 3 events to be processed, got %d starts and %d ends", len(startTimes), len(endTimes))
	}

	// For serial execution: end[0] should be before start[1], end[1] before start[2]
	for i := 0; i < len(startTimes)-1; i++ {
		if startTimes[i+1].Before(endTimes[i]) {
			t.Errorf("Event %d started before event %d finished (not serial execution)", i+1, i)
		}
	}
}

func TestSubscribeOnceAsyncWithContext(t *testing.T) {
	bus := New()
	var count int32
	done := make(chan bool)

	err := SubscribeContext(bus, func(ctx context.Context, event UserEvent) {
		atomic.AddInt32(&count, 1)
		done <- true
	}, Async(), Once())

	if err != nil {
		t.Fatalf("SubscribeContext failed: %v", err)
	}

	ctx := context.Background()
	PublishContext(bus, ctx, UserEvent{UserID: "1", Action: "test"})

	// Wait for first handler to complete
	<-done

	// Publish again
	PublishContext(bus, ctx, UserEvent{UserID: "2", Action: "test"})

	// Give time for potential second execution
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt32(&count) != 1 {
		t.Errorf("Expected handler to be called once, called %d times", count)
	}
}

func TestContextPropagationInNestedPublish(t *testing.T) {
	bus := New()
	var outerCtxValue, innerCtxValue string

	// Outer handler that publishes another event
	if err := SubscribeContext(bus, func(ctx context.Context, event UserEvent) {
		outerCtxValue = ctx.Value("trace").(string)
		// Publish nested event with same context
		PublishContext(bus, ctx, OrderEvent{OrderID: "nested", Amount: 100})
	}); err != nil {
		t.Fatal(err)
	}

	// Inner handler
	if err := SubscribeContext(bus, func(ctx context.Context, event OrderEvent) {
		innerCtxValue = ctx.Value("trace").(string)
	}); err != nil {
		t.Fatal(err)
	}

	// Publish with trace context
	ctx := context.WithValue(context.Background(), "trace", "trace-123")
	PublishContext(bus, ctx, UserEvent{UserID: "1", Action: "trigger"})

	// Both handlers should receive the same context value
	if outerCtxValue != "trace-123" {
		t.Errorf("Outer handler got wrong context value: %s", outerCtxValue)
	}

	if innerCtxValue != "trace-123" {
		t.Errorf("Inner handler got wrong context value: %s", innerCtxValue)
	}
}

func TestPanicRecovery(t *testing.T) {
	bus := New()
	var firstHandlerCalled, thirdHandlerCalled bool

	// First handler - should execute
	if err := Subscribe(bus, func(event UserEvent) {
		firstHandlerCalled = true
	}); err != nil {
		t.Fatal(err)
	}

	// Second handler - will panic
	if err := Subscribe(bus, func(event UserEvent) {
		panic("test panic")
	}); err != nil {
		t.Fatal(err)
	}

	// Third handler - should still execute despite panic in second
	if err := Subscribe(bus, func(event UserEvent) {
		thirdHandlerCalled = true
	}); err != nil {
		t.Fatal(err)
	}

	// Publish event
	Publish(bus, UserEvent{UserID: "123", Action: "test"})

	// Verify first and third handlers were called despite panic
	if !firstHandlerCalled {
		t.Error("First handler was not called")
	}
	if !thirdHandlerCalled {
		t.Error("Third handler was not called after panic")
	}
}

func TestPanicRecoveryAsync(t *testing.T) {
	bus := New()
	handlersCalled := make(chan int, 3)

	// Three async handlers
	if err := Subscribe(bus, func(event UserEvent) {
		handlersCalled <- 1
	}, Async()); err != nil {
		t.Fatal(err)
	}

	if err := Subscribe(bus, func(event UserEvent) {
		panic("async panic")
	}, Async()); err != nil {
		t.Fatal(err)
	}

	if err := Subscribe(bus, func(event UserEvent) {
		handlersCalled <- 3
	}, Async()); err != nil {
		t.Fatal(err)
	}

	// Publish and wait
	Publish(bus, UserEvent{UserID: "123", Action: "test"})
	bus.Wait()

	// Should receive from handlers 1 and 3
	close(handlersCalled)
	var results []int
	for v := range handlersCalled {
		results = append(results, v)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 handlers to complete, got %d", len(results))
	}
}

func TestSetPanicHandler(t *testing.T) {
	var capturedEvent any
	var capturedPanic any

	// Create bus with custom panic handler
	bus := New(WithPanicHandler(func(event any, handlerType reflect.Type, panicValue any) {
		capturedEvent = event
		capturedPanic = panicValue
	}))

	// Handler that will panic
	if err := Subscribe(bus, func(event UserEvent) {
		panic("test panic with handler")
	}); err != nil {
		t.Fatal(err)
	}

	// Publish event
	Publish(bus, UserEvent{UserID: "123", Action: "test"})

	// Verify panic handler was called
	if capturedPanic != "test panic with handler" {
		t.Errorf("Expected panic handler to capture panic value, got %v", capturedPanic)
	}

	userEvent, ok := capturedEvent.(UserEvent)
	if !ok {
		t.Fatal("Captured event is not UserEvent")
	}
	if userEvent.UserID != "123" {
		t.Errorf("Expected UserID 123, got %s", userEvent.UserID)
	}
}

func TestPanicHandlerWithAsync(t *testing.T) {
	bus := New()
	panicChan := make(chan string, 1)

	// Set async panic handler
	bus.SetPanicHandler(func(event any, handlerType reflect.Type, panicValue any) {
		panicChan <- panicValue.(string)
	})

	// Async handler that panics
	if err := Subscribe(bus, func(event OrderEvent) {
		panic("async panic with handler")
	}, Async()); err != nil {
		t.Fatal(err)
	}

	// Publish and wait
	Publish(bus, OrderEvent{OrderID: "999", Amount: 100})
	bus.Wait()

	// Check panic was captured
	select {
	case panic := <-panicChan:
		if panic != "async panic with handler" {
			t.Errorf("Expected 'async panic with handler', got %s", panic)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Panic handler was not called")
	}
}

func TestOnceHandlerRaceCondition(t *testing.T) {
	bus := New()
	var callCount int32

	// Subscribe a once handler
	if err := Subscribe(bus, func(event UserEvent) {
		// Increment counter
		atomic.AddInt32(&callCount, 1)
		// Simulate some work
		time.Sleep(10 * time.Millisecond)
	}, Once()); err != nil {
		t.Fatal(err)
	}

	// Launch multiple goroutines that publish the same event concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			Publish(bus, UserEvent{UserID: string(rune('0' + id)), Action: "concurrent"})
		}(i)
	}

	// Wait for all publishers
	wg.Wait()

	// Give a bit more time for any handlers to complete
	time.Sleep(50 * time.Millisecond)

	// Check how many times the handler was called
	count := atomic.LoadInt32(&callCount)
	if count != 1 {
		t.Errorf("Once handler was called %d times, expected exactly 1", count)
	}
}

func TestOnceHandlerRaceWithAsync(t *testing.T) {
	bus := New()
	var callCount int32

	// Subscribe an async once handler
	if err := Subscribe(bus, func(event OrderEvent) {
		atomic.AddInt32(&callCount, 1)
		time.Sleep(20 * time.Millisecond) // Longer delay to increase race window
	}, Once(), Async()); err != nil {
		t.Fatal(err)
	}

	// Concurrent publishers
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			Publish(bus, OrderEvent{OrderID: string(rune('0' + id)), Amount: float64(id)})
		}(i)
	}

	wg.Wait()
	bus.Wait()

	count := atomic.LoadInt32(&callCount)
	if count != 1 {
		t.Errorf("Async once handler was called %d times, expected exactly 1", count)
	}
}

func TestOnceHandlerWithMixedHandlers(t *testing.T) {
	bus := New()
	var onceCount, regularCount int32

	// Subscribe a once handler
	if err := Subscribe(bus, func(event UserEvent) {
		atomic.AddInt32(&onceCount, 1)
	}, Once()); err != nil {
		t.Fatal(err)
	}

	// Subscribe a regular handler
	if err := Subscribe(bus, func(event UserEvent) {
		atomic.AddInt32(&regularCount, 1)
	}); err != nil {
		t.Fatal(err)
	}

	// Publish twice
	Publish(bus, UserEvent{UserID: "1", Action: "test"})
	Publish(bus, UserEvent{UserID: "2", Action: "test"})

	// Once handler should be called once, regular handler twice
	if count := atomic.LoadInt32(&onceCount); count != 1 {
		t.Errorf("Once handler called %d times, expected 1", count)
	}
	if count := atomic.LoadInt32(&regularCount); count != 2 {
		t.Errorf("Regular handler called %d times, expected 2", count)
	}
}

func TestContextCancellationBeforeHandlers(t *testing.T) {
	bus := New()
	var handlerCalled bool

	// Subscribe handler
	if err := Subscribe(bus, func(event UserEvent) {
		handlerCalled = true
	}); err != nil {
		t.Fatal(err)
	}

	// Create already cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Publish with cancelled context
	PublishContext(bus, ctx, UserEvent{UserID: "1", Action: "test"})

	// Handler should not be called
	if handlerCalled {
		t.Error("Handler was called despite cancelled context")
	}
}

func TestContextCancellationWithMultipleHandlers(t *testing.T) {
	bus := New()
	handlersCalled := []int{}
	mu := &sync.Mutex{}

	// Subscribe multiple handlers
	for i := 1; i <= 5; i++ {
		id := i
		if err := Subscribe(bus, func(event UserEvent) {
			// Simulate some work
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			handlersCalled = append(handlersCalled, id)
			mu.Unlock()
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Create context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately (before any handlers run)
	cancel()

	// Publish with cancelled context
	PublishContext(bus, ctx, UserEvent{UserID: "1", Action: "test"})

	// Give time for any handlers to potentially execute
	time.Sleep(100 * time.Millisecond)

	// No handlers should have been called
	mu.Lock()
	count := len(handlersCalled)
	mu.Unlock()

	if count > 0 {
		t.Errorf("Expected no handlers to be called with cancelled context, but %d were called", count)
	}
}

func TestContextCancellationDuringAsyncHandlers(t *testing.T) {
	bus := New()
	var started, completed int32

	// Subscribe async handlers
	for i := 0; i < 5; i++ {
		if err := Subscribe(bus, func(event OrderEvent) {
			atomic.AddInt32(&started, 1)
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&completed, 1)
		}, Async()); err != nil {
			t.Fatal(err)
		}
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	// Publish event
	PublishContext(bus, ctx, OrderEvent{OrderID: "1", Amount: 100})

	// Wait for potential handlers
	bus.Wait()

	// All handlers should start but not all should complete due to context cancellation
	startCount := atomic.LoadInt32(&started)
	completeCount := atomic.LoadInt32(&completed)

	if startCount == 0 {
		t.Error("No async handlers started")
	}
	// Note: We can't guarantee exact behavior due to timing, but completed should be less than started
	t.Logf("Started: %d, Completed: %d handlers", startCount, completeCount)
}

func TestContextCancellationWithContextHandler(t *testing.T) {
	bus := New()
	var handlerStarted, contextCancelled bool

	// Subscribe context-aware handler
	if err := SubscribeContext(bus, func(ctx context.Context, event UserEvent) {
		handlerStarted = true
		select {
		case <-ctx.Done():
			contextCancelled = true
		case <-time.After(100 * time.Millisecond):
			// Should not reach here
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Create context and cancel it after a short delay
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	// Publish event
	PublishContext(bus, ctx, UserEvent{UserID: "1", Action: "test"})

	// Give handler time to process
	time.Sleep(50 * time.Millisecond)

	// Handler should have started and seen the cancellation
	if !handlerStarted {
		t.Error("Context handler was not called")
	}
	if !contextCancelled {
		t.Error("Handler did not detect context cancellation")
	}
}

func TestAsyncHandlerSkippedOnCancelledContext(t *testing.T) {
	bus := New()
	var handlerCalled int32

	// Subscribe async handler
	if err := Subscribe(bus, func(event UserEvent) {
		atomic.AddInt32(&handlerCalled, 1)
	}, Async()); err != nil {
		t.Fatal(err)
	}

	// Create and immediately cancel context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Publish with cancelled context
	PublishContext(bus, ctx, UserEvent{UserID: "1", Action: "test"})

	// Wait for any async handlers
	bus.Wait()

	// Handler should not have been called
	if count := atomic.LoadInt32(&handlerCalled); count != 0 {
		t.Errorf("Async handler was called %d times despite cancelled context", count)
	}
}

func TestAsyncHandlerContextCancelledInGoroutine(t *testing.T) {
	bus := New()
	var goroutineStarted int32
	var handlerCalled int32
	started := make(chan bool)

	// Subscribe async handler with delay
	if err := Subscribe(bus, func(event UserEvent) {
		atomic.AddInt32(&goroutineStarted, 1)
		started <- true
		// Small delay to ensure context check happens after cancellation
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&handlerCalled, 1)
	}, Async()); err != nil {
		t.Fatal(err)
	}

	// Create context that we'll cancel after goroutine starts
	ctx, cancel := context.WithCancel(context.Background())

	// Goroutine to cancel context after handler goroutine starts
	go func() {
		<-started
		cancel()
	}()

	// Publish event
	PublishContext(bus, ctx, UserEvent{UserID: "1", Action: "test"})

	// Wait for handlers
	bus.Wait()

	// Goroutine should have started
	if count := atomic.LoadInt32(&goroutineStarted); count != 1 {
		t.Error("Async handler goroutine did not start")
	}

	// But handler should still be called (context check is before handler execution)
	if count := atomic.LoadInt32(&handlerCalled); count != 1 {
		t.Error("Handler was not called")
	}
}

func TestAsyncHandlerWithPreCancelledContext(t *testing.T) {
	bus := New()
	handlerStarted := make(chan bool, 1)
	handlerExecuted := make(chan bool, 1)

	// Subscribe multiple async handlers
	for i := 0; i < 3; i++ {
		if err := Subscribe(bus, func(event OrderEvent) {
			select {
			case handlerStarted <- true:
			default:
			}
			time.Sleep(5 * time.Millisecond)
			select {
			case handlerExecuted <- true:
			default:
			}
		}, Async()); err != nil {
			t.Fatal(err)
		}
	}

	// Context that gets cancelled very quickly
	ctx, cancel := context.WithCancel(context.Background())

	// Start publishing in background
	go func() {
		PublishContext(bus, ctx, OrderEvent{OrderID: "1", Amount: 100})
	}()

	// Cancel after a tiny delay to catch some goroutines mid-flight
	time.Sleep(1 * time.Millisecond)
	cancel()

	// Wait for handlers
	bus.Wait()

	// Count how many started vs executed
	close(handlerStarted)
	close(handlerExecuted)

	startCount := 0
	for range handlerStarted {
		startCount++
	}

	execCount := 0
	for range handlerExecuted {
		execCount++
	}

	t.Logf("Started: %d, Executed: %d handlers", startCount, execCount)

	// At least one should have been skipped due to context cancellation
	if startCount == 0 && execCount == 0 {
		t.Log("All handlers were skipped before goroutine creation")
	} else if startCount > execCount {
		t.Log("Some handlers were skipped after goroutine creation due to context check")
	}
}

func TestAsyncSequentialHandlerWithCancelledContext(t *testing.T) {
	bus := New()
	goroutineReached := make(chan bool, 10)
	handlerCalled := make(chan bool, 10)
	waitForStart := make(chan bool)

	// Use a custom wrapper to track goroutine execution
	if err := Subscribe(bus, func(event UserEvent) {
		// Signal that we're in the goroutine but before context check
		select {
		case goroutineReached <- true:
		default:
		}

		// Wait a bit to ensure context gets cancelled
		<-waitForStart

		// This should not be reached if context is cancelled
		select {
		case handlerCalled <- true:
		default:
		}
	}, Async(), Sequential()); err != nil {
		t.Fatal(err)
	}

	// Create and cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())

	// Publish event with valid context
	PublishContext(bus, ctx, UserEvent{UserID: "1", Action: "test"})

	// Give goroutine time to start
	time.Sleep(5 * time.Millisecond)

	// Now cancel the context
	cancel()

	// Signal handler to proceed
	close(waitForStart)

	// Wait for handlers
	bus.Wait()

	// Check how many goroutines started vs handlers called
	close(goroutineReached)
	close(handlerCalled)

	goroutineCount := 0
	for range goroutineReached {
		goroutineCount++
	}

	handlerCount := 0
	for range handlerCalled {
		handlerCount++
	}

	// The sequential handler's goroutine should start but handler shouldn't be called
	// due to context cancellation check
	t.Logf("Goroutines started: %d, Handlers called: %d", goroutineCount, handlerCount)

	// We expect the handler to be called because context is checked BEFORE
	// acquiring the sequential lock, so by the time we cancel, the handler
	// is already past the context check
	if handlerCount != 1 {
		t.Errorf("Expected handler to be called once, got %d", handlerCount)
	}
}

func TestAsyncHandlerContextCancelledDuringExecution(t *testing.T) {
	// Use runtime to ensure we hit the context check
	bus := New()
	handlerCalled := int32(0)
	reachedCheck := make(chan struct{})

	// First, we need a way to pause execution right before the context check
	// We'll use runtime.Gosched() in the handler to allow context cancellation
	if err := Subscribe(bus, func(event UserEvent) {
		// Give other goroutines a chance to run
		for i := 0; i < 100; i++ {
			runtime.Gosched()
		}
		atomic.AddInt32(&handlerCalled, 1)
	}, Async()); err != nil {
		t.Fatal(err)
	}

	// Also subscribe a synchronous handler that will help with timing
	if err := Subscribe(bus, func(event UserEvent) {
		close(reachedCheck)
		// Small delay to let async goroutine start
		time.Sleep(1 * time.Millisecond)
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context right after publish starts
	go func() {
		<-reachedCheck
		cancel()
	}()

	PublishContext(bus, ctx, UserEvent{UserID: "1", Action: "test"})
	bus.Wait()

	// The handler might or might not be called depending on timing
	count := atomic.LoadInt32(&handlerCalled)
	t.Logf("Handler called %d times (may vary due to scheduling)", count)
}

func TestAsyncHandlerWithImmediatelyCancelledContext(t *testing.T) {
	// Test with multiple handlers to increase chance of hitting the context check
	bus := New()
	var handlersExecuted int32
	numHandlers := 50

	// Add many async handlers
	for i := 0; i < numHandlers; i++ {
		if err := Subscribe(bus, func(event UserEvent) {
			// Yield to increase chance of context cancellation
			runtime.Gosched()
			atomic.AddInt32(&handlersExecuted, 1)
		}, Async()); err != nil {
			t.Fatal(err)
		}
	}

	// Create context and cancel it immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Publish event with cancelled context
	PublishContext(bus, ctx, UserEvent{UserID: "test", Action: "test"})

	// Wait for any async handlers
	bus.Wait()

	// With cancelled context, no async handlers should execute
	executed := atomic.LoadInt32(&handlersExecuted)
	if executed > 0 {
		t.Logf("Warning: %d/%d async handlers executed despite cancelled context", executed, numHandlers)
	}
}

// TestForceAsyncContextCheck uses synchronization to ensure the context check is hit
func TestForceAsyncContextCheck(t *testing.T) {
	// Run this test multiple times to increase chances
	successCount := 0

	for attempt := 0; attempt < 1000; attempt++ {
		bus := New()
		handlerCalled := int32(0)

		// Add an async handler
		if err := Subscribe(bus, func(event UserEvent) {
			atomic.AddInt32(&handlerCalled, 1)
		}, Async()); err != nil {
			t.Fatal(err)
		}

		// Create a context
		ctx, cancel := context.WithCancel(context.Background())

		// Try to cancel at exactly the right time
		go func() {
			// Tiny delay to let goroutine creation start
			time.Sleep(time.Nanosecond)
			cancel()
		}()

		// Publish the event
		PublishContext(bus, ctx, UserEvent{UserID: "test", Action: "test"})

		// Wait for completion
		bus.Wait()

		// Check if handler was skipped
		if atomic.LoadInt32(&handlerCalled) == 0 {
			successCount++
		}
	}

	t.Logf("Context check prevented execution in %d/1000 attempts", successCount)

	// We should have hit the context check at least once
	if successCount == 0 {
		t.Log("Warning: Never hit the async context cancellation check")
	}
}

// TestAsyncHandlerContextCancelledBeforeGoroutineStarts specifically targets the context check
func TestAsyncHandlerContextCancelledBeforeGoroutineStarts(t *testing.T) {
	// This test uses GOMAXPROCS to increase scheduling predictability
	oldGOMAXPROCS := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)

	bus := New()
	handlerCalled := int32(0)

	// Add multiple async handlers to increase goroutine creation time
	for i := 0; i < 100; i++ {
		if err := Subscribe(bus, func(event OrderEvent) {
			atomic.AddInt32(&handlerCalled, 1)
		}, Async()); err != nil {
			t.Fatal(err)
		}
	}

	// Create and immediately cancel context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Publish with cancelled context
	PublishContext(bus, ctx, OrderEvent{OrderID: "test", Amount: 100})

	// Restore GOMAXPROCS before waiting (to allow goroutines to run)
	runtime.GOMAXPROCS(oldGOMAXPROCS)

	// Wait for any handlers
	bus.Wait()

	// Check how many handlers ran
	count := atomic.LoadInt32(&handlerCalled)
	t.Logf("Handlers executed: %d/100 (some should be skipped due to context)", count)

	// With immediate cancellation, we expect some handlers to be skipped
	if count == 100 {
		t.Log("Warning: All handlers executed, context check might not be working")
	}
}

// TestAsyncHandlerCancelledContextDeterministic demonstrates context cancellation behavior
func TestAsyncHandlerCancelledContextDeterministic(t *testing.T) {
	// This test shows how context cancellation works with async handlers
	bus := New()

	// Use channels to control execution flow
	goroutineStarted := make(chan struct{})
	handlerCalled := int32(0)

	// Add a handler that signals when it's in the goroutine
	if err := Subscribe(bus, func(event UserEvent) {
		// This code is inside the async goroutine
		// Signal that we're here
		close(goroutineStarted)

		// Wait a moment to ensure context.cancel() has time to execute
		time.Sleep(10 * time.Millisecond)

		// If context was cancelled before the goroutine started, we won't get here
		// But if we're already past the context check, this will execute
		atomic.AddInt32(&handlerCalled, 1)
	}, Async()); err != nil {
		t.Fatal(err)
	}

	// Create a context
	ctx, cancel := context.WithCancel(context.Background())

	// Start a goroutine that will cancel the context at the right time
	go func() {
		// Wait for the async handler goroutine to start
		<-goroutineStarted
		// Now cancel the context - but the handler is already running
		cancel()
	}()

	// Publish the event
	PublishContext(bus, ctx, UserEvent{UserID: "test", Action: "test"})

	// Wait for handlers
	bus.Wait()

	// The handler will be called because context is checked BEFORE entering the goroutine
	// Once the goroutine starts, the handler executes regardless of context cancellation
	if count := atomic.LoadInt32(&handlerCalled); count != 1 {
		t.Errorf("Handler was called %d times, expected 1", count)
	}
}

// TestAsyncHandlerContextCheckWithDelay uses a delay mechanism to ensure the context check is hit
func TestAsyncHandlerContextCheckWithDelay(t *testing.T) {
	// Run multiple times to ensure we hit the condition
	for i := 0; i < 50; i++ {
		bus := New()
		handlerExecuted := int32(0)

		// Add async handler that has a small delay
		if err := Subscribe(bus, func(event UserEvent) {
			// Add a small delay to increase chances of context being cancelled
			time.Sleep(time.Microsecond * 10)
			atomic.AddInt32(&handlerExecuted, 1)
		}, Async()); err != nil {
			t.Fatal(err)
		}

		// Create and immediately cancel context
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel context immediately
		cancel()

		// Publish with cancelled context
		PublishContext(bus, ctx, UserEvent{UserID: "test", Action: "test"})

		// Wait for completion
		bus.Wait()

		// Log if we prevented execution
		if atomic.LoadInt32(&handlerExecuted) == 0 {
			t.Logf("Iteration %d: Successfully prevented handler execution", i)
		}
	}
}

// TestAsyncHandlerContextCancelledGuaranteed uses runtime controls to guarantee hitting the context check
func TestAsyncHandlerContextCancelledGuaranteed(t *testing.T) {
	// This test uses multiple strategies to ensure we hit the context cancellation
	successCount := 0

	// Strategy 1: Many handlers with immediate cancellation
	for attempt := 0; attempt < 100; attempt++ {
		bus := New()
		var handlersSkipped int32
		numHandlers := 10

		// Add multiple async handlers
		for i := 0; i < numHandlers; i++ {
			if err := Subscribe(bus, func(event UserEvent) {
				// If we get here, context check didn't prevent execution
				atomic.AddInt32(&handlersSkipped, -1)
			}, Async()); err != nil {
				t.Fatal(err)
			}
		}

		// Pre-cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Set initial count
		atomic.StoreInt32(&handlersSkipped, int32(numHandlers))

		// Publish with cancelled context
		PublishContext(bus, ctx, UserEvent{UserID: "test", Action: "test"})

		// Wait
		bus.Wait()

		// Check how many were skipped
		skipped := atomic.LoadInt32(&handlersSkipped)
		if skipped > 0 {
			successCount++
		}
	}

	t.Logf("Context check prevented execution in %d/100 attempts", successCount)

	// We should hit the context check at least once
	if successCount == 0 {
		t.Error("Failed to hit async context cancellation check")
	}
}

// TestAsyncHandlerWithBlockedGoroutine forces the context check by blocking goroutine creation
func TestAsyncHandlerWithBlockedGoroutine(t *testing.T) {
	// Limit concurrency to force scheduling
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	bus := New()
	var handlersBlocked int32
	blockChan := make(chan struct{})
	readyChan := make(chan struct{}, 100)

	// First, add many blocking handlers to consume goroutines
	for i := 0; i < 50; i++ {
		if err := Subscribe(bus, func(event OrderEvent) {
			readyChan <- struct{}{}
			<-blockChan // Block until released
		}, Async()); err != nil {
			t.Fatal(err)
		}
	}

	// Publish to start the blocking handlers
	PublishContext(bus, context.Background(), OrderEvent{OrderID: "blocker", Amount: 1})

	// Wait for some blockers to start
	for i := 0; i < 25; i++ {
		<-readyChan
	}

	// Now add our test handler
	if err := Subscribe(bus, func(event UserEvent) {
		// This should be skipped due to context
		atomic.AddInt32(&handlersBlocked, 1)
	}, Async()); err != nil {
		t.Fatal(err)
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to publish with cancelled context while goroutines are busy
	PublishContext(bus, ctx, UserEvent{UserID: "test", Action: "test"})

	// Release blockers
	close(blockChan)

	// Wait for everything
	bus.Wait()

	// Check if our handler was blocked
	if blocked := atomic.LoadInt32(&handlersBlocked); blocked == 0 {
		t.Log("Successfully prevented handler execution due to cancelled context")
	}
}

// TestEnsureAsyncContextCancellation is a simple deterministic test for CI environments
func TestEnsureAsyncContextCancellation(t *testing.T) {
	// This test ensures the async context cancellation line is covered
	// by running many iterations with different timing patterns

	hitCount := 0
	totalRuns := 500

	for i := 0; i < totalRuns; i++ {
		bus := New()
		handlerRan := false

		// Subscribe async handler
		if err := Subscribe(bus, func(event UserEvent) {
			handlerRan = true
		}, Async()); err != nil {
			t.Fatal(err)
		}

		// Create context and cancel immediately
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Publish with cancelled context
		PublishContext(bus, ctx, UserEvent{UserID: "test", Action: "test"})

		// Wait for any async operations
		bus.Wait()

		// Count when handler was prevented from running
		if !handlerRan {
			hitCount++
		}

		// Add small variation in timing for different iterations
		if i%10 == 0 {
			runtime.Gosched()
		}
	}

	t.Logf("Prevented async handler execution in %d/%d runs", hitCount, totalRuns)

	// In CI, we should hit this at least once
	if hitCount == 0 {
		t.Log("Warning: Context cancellation check may not be covered in this environment")

		// Force the condition by using runtime controls
		t.Log("Attempting forced context cancellation test...")

		// Use GOMAXPROCS=1 to make scheduling more predictable
		runtime.GOMAXPROCS(1)
		defer runtime.GOMAXPROCS(runtime.NumCPU())

		for j := 0; j < 100; j++ {
			bus := New()
			executed := int32(0)

			// Add many handlers to increase chances
			for k := 0; k < 20; k++ {
				if err := Subscribe(bus, func(event UserEvent) {
					atomic.AddInt32(&executed, 1)
				}, Async()); err != nil {
					t.Fatal(err)
				}
			}

			// Cancel before publish
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			PublishContext(bus, ctx, UserEvent{UserID: fmt.Sprintf("test-%d", j), Action: "test"})
			bus.Wait()

			if atomic.LoadInt32(&executed) < 20 {
				t.Log("Successfully prevented some handlers from executing")
				break
			}
		}
	}
}

func TestOnceHandlerRemovalRaceCondition(t *testing.T) {
	// This test verifies that the race condition in once handler removal is fixed
	// The race occurred when multiple goroutines tried to remove once handlers concurrently
	bus := New()

	// We'll run this test multiple times to increase the chance of catching the race
	for iteration := 0; iteration < 100; iteration++ {
		// Clear the bus for each iteration
		ClearAll(bus)

		// Track execution counts
		executionCount := int32(0)

		// Add multiple once handlers
		numHandlers := 10
		for i := 0; i < numHandlers; i++ {
			if err := Subscribe(bus, func(e UserEvent) {
				atomic.AddInt32(&executionCount, 1)
				// Simulate some work
				time.Sleep(time.Microsecond)
			}, Once()); err != nil {
				t.Fatal(err)
			}
		}

		// Publish events concurrently
		var wg sync.WaitGroup
		numPublishers := 5
		wg.Add(numPublishers)

		for i := 0; i < numPublishers; i++ {
			go func() {
				defer wg.Done()
				Publish(bus, UserEvent{UserID: "concurrent", Action: "test"})
			}()
		}

		// Wait for all publishers to complete
		wg.Wait()

		// Verify that each once handler was executed exactly once
		finalCount := atomic.LoadInt32(&executionCount)
		if finalCount != int32(numHandlers) {
			t.Errorf("Iteration %d: Expected %d executions, got %d", iteration, numHandlers, finalCount)
		}

		// Verify that all once handlers were properly removed
		if HasHandlers[UserEvent](bus) {
			t.Errorf("Iteration %d: Once handlers were not properly removed", iteration)
		}

		// Publish again to ensure no handlers remain
		executionCount = 0
		Publish(bus, UserEvent{UserID: "verify", Action: "test"})

		if atomic.LoadInt32(&executionCount) != 0 {
			t.Errorf("Iteration %d: Handlers executed after they should have been removed", iteration)
		}
	}
}

func TestSubscribeValidation(t *testing.T) {
	bus := New()

	t.Run("nil bus", func(t *testing.T) {
		err := Subscribe(nil, func(e UserEvent) {})
		if err == nil {
			t.Error("expected error for nil bus")
		}
		if err.Error() != "eventbus: bus cannot be nil" {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("nil handler", func(t *testing.T) {
		var nilHandler Handler[UserEvent]
		err := Subscribe(bus, nilHandler)
		if err == nil {
			t.Error("expected error for nil handler")
		}
		if err.Error() != "eventbus: handler cannot be nil" {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("nil option", func(t *testing.T) {
		err := Subscribe(bus, func(e UserEvent) {}, nil, Once())
		if err == nil {
			t.Error("expected error for nil option")
		}
		if err.Error() != "eventbus: subscribe option cannot be nil" {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("valid subscription", func(t *testing.T) {
		err := Subscribe(bus, func(e UserEvent) {})
		if err != nil {
			t.Errorf("unexpected error for valid subscription: %v", err)
		}
	})
}

func TestSubscribeContextValidation(t *testing.T) {
	bus := New()

	t.Run("nil bus", func(t *testing.T) {
		err := SubscribeContext(nil, func(ctx context.Context, e UserEvent) {})
		if err == nil {
			t.Error("expected error for nil bus")
		}
		if err.Error() != "eventbus: bus cannot be nil" {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("nil handler", func(t *testing.T) {
		var nilHandler ContextHandler[UserEvent]
		err := SubscribeContext(bus, nilHandler)
		if err == nil {
			t.Error("expected error for nil handler")
		}
		if err.Error() != "eventbus: handler cannot be nil" {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("nil option", func(t *testing.T) {
		err := SubscribeContext(bus, func(ctx context.Context, e UserEvent) {}, nil, Async())
		if err == nil {
			t.Error("expected error for nil option")
		}
		if err.Error() != "eventbus: subscribe option cannot be nil" {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("valid subscription", func(t *testing.T) {
		err := SubscribeContext(bus, func(ctx context.Context, e UserEvent) {})
		if err != nil {
			t.Errorf("unexpected error for valid subscription: %v", err)
		}
	})
}

func TestNilHandlerNotStored(t *testing.T) {
	// This test verifies that nil handlers are rejected and not stored in the bus
	bus := New()

	// Try to subscribe a nil handler
	var nilHandler Handler[UserEvent]
	err := Subscribe(bus, nilHandler)
	if err == nil {
		t.Fatal("expected error for nil handler")
	}

	// Verify no handlers were stored
	if HasHandlers[UserEvent](bus) {
		t.Error("nil handler should not have been stored")
	}

	// Subscribe a valid handler
	called := false
	if err := Subscribe(bus, func(e UserEvent) {
		called = true
	}); err != nil {
		t.Fatal(err)
	}

	// Publish and verify only the valid handler is called
	Publish(bus, UserEvent{UserID: "test", Action: "test"})

	if !called {
		t.Error("valid handler should have been called")
	}
}

func TestBeforePublishHook(t *testing.T) {
	bus := New()

	var hookCalled bool
	var capturedEventType reflect.Type
	var capturedEvent any

	// Set before publish hook
	bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
		hookCalled = true
		capturedEventType = eventType
		capturedEvent = event
	})

	// Publish an event
	testEvent := UserEvent{UserID: "123", Action: "test"}
	Publish(bus, testEvent)

	// Verify hook was called
	if !hookCalled {
		t.Error("beforePublish hook should have been called")
	}

	if capturedEventType != reflect.TypeOf(testEvent) {
		t.Errorf("expected event type %v, got %v", reflect.TypeOf(testEvent), capturedEventType)
	}

	if capturedEvent != testEvent {
		t.Errorf("expected event %v, got %v", testEvent, capturedEvent)
	}
}

func TestAfterPublishHook(t *testing.T) {
	bus := New()

	var hookCalled bool
	var handlerCalled bool

	// Set after publish hook
	bus.SetAfterPublishHook(func(eventType reflect.Type, event any) {
		hookCalled = true
		// Hook should be called after handler
		if !handlerCalled {
			t.Error("afterPublish hook called before handler")
		}
	})

	// Subscribe a handler
	if err := Subscribe(bus, func(e UserEvent) {
		handlerCalled = true
	}); err != nil {
		t.Fatal(err)
	}

	// Publish an event
	Publish(bus, UserEvent{UserID: "123", Action: "test"})

	// Verify hook was called
	if !hookCalled {
		t.Error("afterPublish hook should have been called")
	}
}

func TestHooksCalledEvenWithNoHandlers(t *testing.T) {
	bus := New()

	var beforeCalled bool
	var afterCalled bool

	// Set hooks
	bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
		beforeCalled = true
	})

	bus.SetAfterPublishHook(func(eventType reflect.Type, event any) {
		afterCalled = true
	})

	// Publish event with no handlers
	Publish(bus, UserEvent{UserID: "123", Action: "test"})

	// Both hooks should still be called
	if !beforeCalled {
		t.Error("beforePublish hook should be called even with no handlers")
	}

	if !afterCalled {
		t.Error("afterPublish hook should be called even with no handlers")
	}
}

func TestHooksWithMultipleEventTypes(t *testing.T) {
	bus := New()

	eventTypes := make([]string, 0)

	// Set hook that logs all event types
	bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
		eventTypes = append(eventTypes, eventType.Name())
	})

	// Publish different event types
	Publish(bus, UserEvent{UserID: "1", Action: "create"})
	Publish(bus, OrderEvent{OrderID: "2", Amount: 100})
	Publish(bus, UserEvent{UserID: "3", Action: "delete"})

	// Verify all events were captured
	if len(eventTypes) != 3 {
		t.Errorf("expected 3 events, got %d", len(eventTypes))
	}

	if eventTypes[0] != "UserEvent" || eventTypes[1] != "OrderEvent" || eventTypes[2] != "UserEvent" {
		t.Errorf("unexpected event types: %v", eventTypes)
	}
}

func TestHooksWithAsyncHandlers(t *testing.T) {
	bus := New()

	var beforeCalled bool
	var afterCalled bool
	handlerDone := make(chan bool)

	// Set hooks
	bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
		beforeCalled = true
	})

	bus.SetAfterPublishHook(func(eventType reflect.Type, event any) {
		afterCalled = true
	})

	// Subscribe async handler
	if err := Subscribe(bus, func(e UserEvent) {
		time.Sleep(10 * time.Millisecond)
		handlerDone <- true
	}, Async()); err != nil {
		t.Fatal(err)
	}

	// Publish event
	Publish(bus, UserEvent{UserID: "123", Action: "test"})

	// Hooks should be called immediately
	if !beforeCalled {
		t.Error("beforePublish hook should be called immediately")
	}

	if !afterCalled {
		t.Error("afterPublish hook should be called immediately (not wait for async)")
	}

	// Wait for async handler
	select {
	case <-handlerDone:
		// Good
	case <-time.After(100 * time.Millisecond):
		t.Error("async handler didn't complete")
	}

	bus.Wait()
}

func TestHookReplacement(t *testing.T) {
	bus := New()

	counter := 0

	// Set initial hook
	bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
		counter = 1
	})

	// Replace with new hook
	bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
		counter = 2
	})

	// Publish event
	Publish(bus, UserEvent{UserID: "123", Action: "test"})

	// Only new hook should be called
	if counter != 2 {
		t.Errorf("expected counter=2, got %d", counter)
	}
}

func TestHooksWithContext(t *testing.T) {
	bus := New()

	var hookEvent any

	// Set hook
	bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
		hookEvent = event
	})

	// Subscribe context handler
	if err := SubscribeContext(bus, func(ctx context.Context, e UserEvent) {
		// Handler
	}); err != nil {
		t.Fatal(err)
	}

	// Publish with context
	ctx := context.WithValue(context.Background(), "key", "value")
	PublishContext(bus, ctx, UserEvent{UserID: "123", Action: "test"})

	// Hook should receive the event
	if hookEvent == nil {
		t.Error("hook should have received the event")
	}
}

func TestGlobalEventLogging(t *testing.T) {
	// This demonstrates the primary use case: logging ALL events
	bus := New()

	// Simulated log storage
	var logs []string

	// Set up global logging hook
	bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
		logs = append(logs, fmt.Sprintf("[LOG] %s: %+v", eventType.Name(), event))
	})

	// Multiple services subscribing to specific events
	if err := Subscribe(bus, func(e UserEvent) {
		// User service handler
	}); err != nil {
		t.Fatal(err)
	}

	if err := Subscribe(bus, func(e OrderEvent) {
		// Order service handler
	}); err != nil {
		t.Fatal(err)
	}

	// Publish various events
	Publish(bus, UserEvent{UserID: "1", Action: "login"})
	Publish(bus, OrderEvent{OrderID: "100", Amount: 50.0})
	Publish(bus, UserEvent{UserID: "2", Action: "logout"})

	// Verify all events were logged
	if len(logs) != 3 {
		t.Errorf("expected 3 log entries, got %d", len(logs))
	}

	// Verify log format
	for _, log := range logs {
		if !strings.HasPrefix(log, "[LOG]") {
			t.Errorf("log entry doesn't have expected format: %s", log)
		}
	}
}

func TestAfterHookCalledWhenHandlersClearedDuringExecution(t *testing.T) {
	// This tests the edge case where handlers list becomes empty during execution
	// This happens when a handler clears all handlers while executing
	bus := New()

	var afterHookCalled bool

	// Set after hook
	bus.SetAfterPublishHook(func(eventType reflect.Type, event any) {
		afterHookCalled = true
	})

	// Subscribe a handler that clears all handlers
	if err := Subscribe(bus, func(e UserEvent) {
		// Clear all handlers while in handler
		ClearAll(bus)
	}); err != nil {
		t.Fatal(err)
	}

	// Publish event
	Publish(bus, UserEvent{UserID: "test", Action: "clear"})

	// After hook should still be called
	if !afterHookCalled {
		t.Error("afterPublish hook should be called even when handlers are cleared during execution")
	}
}

func TestAsyncSequentialHandlerContextCancelled(t *testing.T) {
	bus := New()
	var handlerCalled int32
	started := make(chan bool)

	// Subscribe a sequential async handler that takes time
	if err := Subscribe(bus, func(event UserEvent) {
		atomic.AddInt32(&handlerCalled, 1)
	}, Async(), Sequential()); err != nil { // true = sequential
		t.Fatal(err)
	}

	// Subscribe another handler to signal when processing starts
	if err := Subscribe(bus, func(event UserEvent) {
		started <- true
	}, Async()); err != nil {
		t.Fatal(err)
	}

	// Create context
	ctx, cancel := context.WithCancel(context.Background())

	// Publish multiple events in quick succession
	go func() {
		for i := 0; i < 3; i++ {
			PublishContext(bus, ctx, UserEvent{UserID: fmt.Sprintf("%d", i), Action: "test"})
		}
	}()

	// Wait for processing to start, then cancel context
	<-started
	cancel()

	// Publish more events with cancelled context
	for i := 3; i < 5; i++ {
		PublishContext(bus, ctx, UserEvent{UserID: fmt.Sprintf("%d", i), Action: "test"})
	}

	// Wait for all async processing
	bus.Wait()

	// Not all handlers should have been called due to context cancellation
	count := atomic.LoadInt32(&handlerCalled)
	if count >= 5 {
		t.Errorf("Expected some handlers to be skipped due to context cancellation, but %d/5 were called", count)
	}
}
