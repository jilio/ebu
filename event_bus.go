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

// AsyncHandler is a generic async event handler function
type AsyncHandler[T any] func(T)

// ContextHandler is a generic event handler function that accepts context
type ContextHandler[T any] func(context.Context, T)

// AsyncContextHandler is a generic async event handler function that accepts context
type AsyncContextHandler[T any] func(context.Context, T)

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
	executed       int32 // For once handlers, atomically tracks if executed
}

// PanicHandler is called when a handler panics
type PanicHandler func(event any, handlerType reflect.Type, panicValue any)

// EventBus is a type-safe event bus that can handle multiple event types
type EventBus struct {
	handlers     map[reflect.Type][]*internalHandler
	panicHandler PanicHandler
	mu           sync.RWMutex
	wg           sync.WaitGroup
}

// New creates a new EventBus
func New() *EventBus {
	return &EventBus{
		handlers: make(map[reflect.Type][]*internalHandler),
	}
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
	eventType := reflect.TypeOf((*T)(nil)).Elem()

	h := &internalHandler{
		handler:     handler,
		handlerType: reflect.TypeOf(handler),
		eventType:   eventType,
	}

	// Apply options
	for _, opt := range opts {
		opt(h)
	}

	bus.mu.Lock()
	defer bus.mu.Unlock()

	bus.handlers[eventType] = append(bus.handlers[eventType], h)
	return nil
}

// SubscribeContext registers a context-aware handler for events of type T
func SubscribeContext[T any](bus *EventBus, handler ContextHandler[T], opts ...SubscribeOption) error {
	eventType := reflect.TypeOf((*T)(nil)).Elem()

	h := &internalHandler{
		handler:        handler,
		handlerType:    reflect.TypeOf(handler),
		eventType:      eventType,
		acceptsContext: true,
	}

	// Apply options
	for _, opt := range opts {
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

	bus.mu.RLock()
	handlers, exists := bus.handlers[eventType]
	if !exists {
		bus.mu.RUnlock()
		return
	}

	// Copy handlers slice to avoid holding lock during execution
	handlersCopy := make([]*internalHandler, len(handlers))
	copy(handlersCopy, handlers)
	bus.mu.RUnlock()

	// Track handlers to remove (for once handlers)
	var toRemove []*internalHandler

	for _, h := range handlersCopy {
		// Check if context is cancelled before processing
		if ctx.Err() != nil {
			// Context cancelled, skip remaining handlers
			break
		}

		// For once handlers, check if already executed
		if h.once {
			if !atomic.CompareAndSwapInt32(&h.executed, 0, 1) {
				// Already executed, skip
				continue
			}
			toRemove = append(toRemove, h)
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

	// Remove once handlers
	if len(toRemove) > 0 {
		bus.mu.Lock()
		defer bus.mu.Unlock()

		handlers = bus.handlers[eventType]
		// Create a new slice without the executed once handlers
		newHandlers := make([]*internalHandler, 0, len(handlers))
		for _, h := range handlers {
			// Skip handlers that are in the toRemove list
			shouldRemove := false
			for _, removeHandler := range toRemove {
				if h == removeHandler {
					shouldRemove = true
					break
				}
			}
			if !shouldRemove {
				newHandlers = append(newHandlers, h)
			}
		}
		bus.handlers[eventType] = newHandlers
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
