package eventbus

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// Handler is a generic event handler function
type Handler[T any] func(T)

// ContextHandler is a generic event handler function that accepts context
type ContextHandler[T any] func(context.Context, T)

// SubscribeOption configures a subscription
type SubscribeOption func(*internalHandler)

// internalHandler wraps a handler with metadata
type internalHandler struct {
	handler        any
	handlerType    reflect.Type
	eventType      reflect.Type
	once           bool
	async          bool
	sequential     bool
	acceptsContext bool
	mu             sync.Mutex
	executed       uint32 // For once handlers, atomically tracks if executed
}

// PanicHandler is called when a handler panics
type PanicHandler func(event any, handlerType reflect.Type, panicValue any)

// PublishHook is called when an event is published
type PublishHook func(eventType reflect.Type, event any)

// EventBus is a type-safe event bus that can handle multiple event types
type EventBus struct {
	handlers      map[reflect.Type][]*internalHandler
	panicHandler  PanicHandler
	beforePublish PublishHook
	afterPublish  PublishHook
	mu            sync.RWMutex
	wg            sync.WaitGroup

	// Optional persistence fields (nil if not using persistence)
	store         EventStore
	storePosition int64
	storeMu       sync.RWMutex

	// Extensions for additional features (snapshots, etc.)
	extensions map[string]any
}

// BusOption configures an EventBus during creation
type BusOption func(*EventBus)

// New creates a new EventBus with optional configuration
func New(opts ...BusOption) *EventBus {
	bus := &EventBus{
		handlers: make(map[reflect.Type][]*internalHandler),
	}

	// Apply all options
	for _, opt := range opts {
		opt(bus)
	}

	return bus
}

// Once configures the handler to be called only once
func Once() SubscribeOption {
	return func(h *internalHandler) {
		h.once = true
	}
}

// Async configures the handler to run asynchronously
// If sequential is true, events are processed one at a time (no concurrency)
func Async(sequential bool) SubscribeOption {
	return func(h *internalHandler) {
		h.async = true
		h.sequential = sequential
	}
}

// Subscribe registers a handler for events of type T
func Subscribe[T any](bus *EventBus, handler Handler[T], opts ...SubscribeOption) error {
	// Validate bus
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}

	// Validate handler
	if handler == nil {
		return fmt.Errorf("eventbus: handler cannot be nil")
	}

	eventType := reflect.TypeOf((*T)(nil)).Elem()

	h := &internalHandler{
		handler:     handler,
		handlerType: reflect.TypeOf(handler),
		eventType:   eventType,
	}

	// Apply options with validation
	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("eventbus: subscribe option cannot be nil")
		}
		opt(h)
	}

	bus.mu.Lock()
	defer bus.mu.Unlock()

	bus.handlers[eventType] = append(bus.handlers[eventType], h)
	return nil
}

// SubscribeContext registers a context-aware handler for events of type T
func SubscribeContext[T any](bus *EventBus, handler ContextHandler[T], opts ...SubscribeOption) error {
	// Validate bus
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}

	// Validate handler
	if handler == nil {
		return fmt.Errorf("eventbus: handler cannot be nil")
	}

	eventType := reflect.TypeOf((*T)(nil)).Elem()

	h := &internalHandler{
		handler:        handler,
		handlerType:    reflect.TypeOf(handler),
		eventType:      eventType,
		acceptsContext: true,
	}

	// Apply options with validation
	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("eventbus: subscribe option cannot be nil")
		}
		opt(h)
	}

	bus.mu.Lock()
	defer bus.mu.Unlock()

	bus.handlers[eventType] = append(bus.handlers[eventType], h)
	return nil
}

// Unsubscribe removes a specific handler for events of type T
// The handler parameter must be the exact same function reference that was subscribed
func Unsubscribe[T any, H any](bus *EventBus, handler H) error {
	eventType := reflect.TypeOf((*T)(nil)).Elem()
	handlerPtr := reflect.ValueOf(handler).Pointer()

	bus.mu.Lock()
	defer bus.mu.Unlock()

	handlers, exists := bus.handlers[eventType]
	if !exists {
		return fmt.Errorf("no handlers found for event type %v", eventType)
	}

	for i, h := range handlers {
		if reflect.ValueOf(h.handler).Pointer() == handlerPtr {
			// Remove handler
			bus.handlers[eventType] = append(handlers[:i], handlers[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("handler not found for event type %v", eventType)
}

// Publish sends an event to all registered handlers
func Publish[T any](bus *EventBus, event T) {
	PublishContext(bus, context.Background(), event)
}

// PublishContext sends an event with context to all registered handlers
func PublishContext[T any](bus *EventBus, ctx context.Context, event T) {
	eventType := reflect.TypeOf(event)

	// Call beforePublish hook if set
	bus.mu.RLock()
	beforeHook := bus.beforePublish
	bus.mu.RUnlock()

	if beforeHook != nil {
		beforeHook(eventType, event)
	}

	bus.mu.RLock()
	handlers, exists := bus.handlers[eventType]
	if !exists {
		bus.mu.RUnlock()

		// Still call afterPublish hook even if no handlers
		bus.mu.RLock()
		afterHook := bus.afterPublish
		bus.mu.RUnlock()

		if afterHook != nil {
			afterHook(eventType, event)
		}
		return
	}

	// Copy handlers slice to avoid holding lock during execution
	handlersCopy := make([]*internalHandler, len(handlers))
	copy(handlersCopy, handlers)
	bus.mu.RUnlock()

	// Execute handlers
	for _, h := range handlersCopy {
		// Check if context is cancelled before processing
		if ctx.Err() != nil {
			// Context cancelled, skip remaining handlers
			break
		}

		// For once handlers, check if already executed
		if h.once && !atomic.CompareAndSwapUint32(&h.executed, 0, 1) {
			// Already executed, skip
			continue
		}

		if h.async {
			bus.wg.Add(1)
			go func(handler *internalHandler, capturedCtx context.Context) {
				defer bus.wg.Done()
				if handler.sequential {
					handler.mu.Lock()
					defer handler.mu.Unlock()
				}
				// Check context cancellation in goroutine
				if capturedCtx.Err() != nil {
					return
				}
				callHandlerWithContext(handler, capturedCtx, event, bus.panicHandler)
			}(h, ctx)
		} else {
			callHandlerWithContext(h, ctx, event, bus.panicHandler)
		}
	}

	// Remove executed once handlers
	// We do this in a separate pass to avoid race conditions
	bus.mu.Lock()
	handlers = bus.handlers[eventType]
	if len(handlers) == 0 {
		bus.mu.Unlock()

		// Call afterPublish hook
		bus.mu.RLock()
		afterHook := bus.afterPublish
		bus.mu.RUnlock()

		if afterHook != nil {
			afterHook(eventType, event)
		}
		return
	}

	// Check if any once handlers were executed and need removal
	hasExecutedOnce := false
	for _, h := range handlers {
		if h.once && atomic.LoadUint32(&h.executed) == 1 {
			hasExecutedOnce = true
			break
		}
	}

	if hasExecutedOnce {
		// Create a new slice without the executed once handlers
		newHandlers := make([]*internalHandler, 0, len(handlers))
		for _, h := range handlers {
			// Keep handler if it's not a once handler OR if it's a once handler that hasn't been executed
			if !h.once || atomic.LoadUint32(&h.executed) == 0 {
				newHandlers = append(newHandlers, h)
			}
		}
		bus.handlers[eventType] = newHandlers
	}
	bus.mu.Unlock()

	// Call afterPublish hook
	bus.mu.RLock()
	afterHook := bus.afterPublish
	bus.mu.RUnlock()

	if afterHook != nil {
		afterHook(eventType, event)
	}
}

// callHandlerWithContext executes a handler with the given context and event
func callHandlerWithContext[T any](h *internalHandler, ctx context.Context, event T, panicHandler PanicHandler) {
	defer func() {
		if r := recover(); r != nil {
			if panicHandler != nil {
				panicHandler(event, h.handlerType, r)
			}
		}
	}()

	if h.acceptsContext {
		if fn, ok := h.handler.(ContextHandler[T]); ok {
			fn(ctx, event)
		} else if fn, ok := h.handler.(func(context.Context, any)); ok {
			// Handle generic context handler (used by CQRS projections)
			fn(ctx, event)
		} else if fn, ok := h.handler.(func(context.Context, any) error); ok {
			// Handle generic context handler with error (used by CQRS projections)
			_ = fn(ctx, event)
		}
	} else {
		if fn, ok := h.handler.(Handler[T]); ok {
			fn(event)
		}
	}
}

// HasSubscribers returns true if there are any handlers for event type T
func HasSubscribers[T any](bus *EventBus) bool {
	eventType := reflect.TypeOf((*T)(nil)).Elem()

	bus.mu.RLock()
	defer bus.mu.RUnlock()

	handlers, exists := bus.handlers[eventType]
	return exists && len(handlers) > 0
}

// WaitAsync waits for all async handlers to complete
func (bus *EventBus) WaitAsync() {
	bus.wg.Wait()
}

// Clear removes all handlers for event type T
func Clear[T any](bus *EventBus) {
	eventType := reflect.TypeOf((*T)(nil)).Elem()

	bus.mu.Lock()
	defer bus.mu.Unlock()

	delete(bus.handlers, eventType)
}

// ClearAll removes all handlers
func (bus *EventBus) ClearAll() {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	bus.handlers = make(map[reflect.Type][]*internalHandler)
}

// SetPanicHandler sets a function to be called when a handler panics
func (bus *EventBus) SetPanicHandler(handler PanicHandler) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	bus.panicHandler = handler
}

// SetBeforePublishHook sets a hook to be called before handlers are executed
func (bus *EventBus) SetBeforePublishHook(hook PublishHook) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	bus.beforePublish = hook
}

// SetAfterPublishHook sets a hook to be called after all handlers have executed
func (bus *EventBus) SetAfterPublishHook(hook PublishHook) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	bus.afterPublish = hook
}
