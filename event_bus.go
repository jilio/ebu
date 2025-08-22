package eventbus

import (
	"context"
	"fmt"
	"hash/fnv"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
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
	filter         any // Predicate function for filtering events
	mu             sync.Mutex
	executed       uint32 // For once handlers, atomically tracks if executed
}

// PanicHandler is called when a handler panics
type PanicHandler func(event any, handlerType reflect.Type, panicValue any)

// PersistenceErrorHandler is called when event persistence fails
type PersistenceErrorHandler func(event any, eventType reflect.Type, err error)

// PublishHook is called when an event is published
type PublishHook func(eventType reflect.Type, event any)

const numShards = 32 // Power of 2 for efficient modulo

// shard represents a single shard with its own mutex
type shard struct {
	mu       sync.RWMutex
	handlers map[reflect.Type][]*internalHandler
}

// EventBus is a high-performance event bus with sharded locks
type EventBus struct {
	shards        [numShards]*shard
	panicHandler  PanicHandler
	beforePublish PublishHook
	afterPublish  PublishHook
	wg            sync.WaitGroup

	// Optional persistence fields (nil if not using persistence)
	store                   EventStore
	storePosition           int64
	storeMu                 sync.RWMutex
	persistenceErrorHandler PersistenceErrorHandler
	persistenceTimeout      time.Duration

	// Upcast registry for event migration
	upcastRegistry *upcastRegistry
}

// EventType returns the fully qualified type name of an event.
// This is useful for comparing with StoredEvent.Type during replay.
//
// Example:
//
//	eventType := EventType(MyEvent{})
//	// Returns: "github.com/mypackage/MyEvent"
//
//	// Usage in replay:
//	bus.Replay(ctx, 0, func(event *StoredEvent) error {
//	    if event.Type == EventType(MyEvent{}) {
//	        // Process MyEvent
//	    }
//	    return nil
//	})
func EventType(event any) string {
	return reflect.TypeOf(event).String()
}

// Option is a function that configures the EventBus
type Option func(*EventBus)

// New creates a new EventBus with sharded locks for better performance
func New(opts ...Option) *EventBus {
	bus := &EventBus{
		upcastRegistry: newUpcastRegistry(),
	}

	// Initialize shards
	for i := 0; i < numShards; i++ {
		bus.shards[i] = &shard{
			handlers: make(map[reflect.Type][]*internalHandler),
		}
	}

	// Apply options
	for _, opt := range opts {
		opt(bus)
	}

	// Initialize persistence if store is provided
	if bus.store != nil {
		go bus.startEventProcessor()
	}

	return bus
}

// getShard returns the shard for a given event type using FNV hash
func (bus *EventBus) getShard(eventType reflect.Type) *shard {
	h := fnv.New32a()
	h.Write([]byte(eventType.String()))
	shardIndex := h.Sum32() & (numShards - 1) // Fast modulo for power of 2
	return bus.shards[shardIndex]
}

// Subscribe registers a handler for events of type T
func Subscribe[T any](bus *EventBus, handler Handler[T], opts ...SubscribeOption) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
	if handler == nil {
		return fmt.Errorf("eventbus: handler cannot be nil")
	}

	eventType := reflect.TypeOf((*T)(nil)).Elem()

	h := &internalHandler{
		handler:        handler,
		handlerType:    reflect.TypeOf(handler),
		eventType:      eventType,
		acceptsContext: false,
	}

	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("eventbus: subscribe option cannot be nil")
		}
		opt(h)
	}

	shard := bus.getShard(eventType)
	shard.mu.Lock()
	shard.handlers[eventType] = append(shard.handlers[eventType], h)
	shard.mu.Unlock()

	return nil
}

// SubscribeContext registers a context-aware handler for events of type T
func SubscribeContext[T any](bus *EventBus, handler ContextHandler[T], opts ...SubscribeOption) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
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

	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("eventbus: subscribe option cannot be nil")
		}
		opt(h)
	}

	shard := bus.getShard(eventType)
	shard.mu.Lock()
	shard.handlers[eventType] = append(shard.handlers[eventType], h)
	shard.mu.Unlock()

	return nil
}

// Unsubscribe removes a handler for events of type T
func Unsubscribe[T any, H any](bus *EventBus, handler H) error {
	eventType := reflect.TypeOf((*T)(nil)).Elem()
	handlerPtr := reflect.ValueOf(handler).Pointer()

	shard := bus.getShard(eventType)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	handlers := shard.handlers[eventType]
	for i, h := range handlers {
		if reflect.ValueOf(h.handler).Pointer() == handlerPtr {
			// Remove handler efficiently
			shard.handlers[eventType] = append(handlers[:i], handlers[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("handler not found")
}

// Publish publishes an event to all registered handlers
func Publish[T any](bus *EventBus, event T) {
	PublishContext(bus, context.Background(), event)
}

// PublishContext publishes an event with context to all registered handlers
func PublishContext[T any](bus *EventBus, ctx context.Context, event T) {
	eventType := reflect.TypeOf(event)

	// Call before publish hook
	if bus.beforePublish != nil {
		bus.beforePublish(eventType, event)
	}

	// Get handlers from appropriate shard
	shard := bus.getShard(eventType)
	shard.mu.RLock()
	handlers := shard.handlers[eventType]
	// Create a copy to avoid holding the lock during handler execution
	handlersCopy := make([]*internalHandler, len(handlers))
	copy(handlersCopy, handlers)
	shard.mu.RUnlock()

	// Execute handlers without holding the lock
	var wg sync.WaitGroup
	var onceHandlersToRemove []*internalHandler

	for _, h := range handlersCopy {
		// Check filter if present
		if h.filter != nil {
			if filterFunc, ok := h.filter.(func(T) bool); ok {
				if !filterFunc(event) {
					continue // Skip this handler as event doesn't match filter
				}
			}
		}

		// For once handlers, use CompareAndSwap to ensure atomic execution
		if h.once {
			if !atomic.CompareAndSwapUint32(&h.executed, 0, 1) {
				continue // Already executed
			}
			// Mark for removal after execution
			onceHandlersToRemove = append(onceHandlersToRemove, h)
		}

		if h.async {
			wg.Add(1)
			bus.wg.Add(1)
			go func(handler *internalHandler) {
				defer wg.Done()
				defer bus.wg.Done()

				// Check context before executing
				select {
				case <-ctx.Done():
					return
				default:
					callHandlerWithContext(handler, ctx, event, bus.panicHandler)
				}
			}(h)
		} else {
			// Check context cancellation for sync handlers too
			select {
			case <-ctx.Done():
				continue // Skip if context cancelled
			default:
				callHandlerWithContext(h, ctx, event, bus.panicHandler)
			}
		}
	}

	// Remove once handlers that were executed
	if len(onceHandlersToRemove) > 0 {
		shard.mu.Lock()
		handlers := shard.handlers[eventType]
		for _, onceHandler := range onceHandlersToRemove {
			for i, h := range handlers {
				if h == onceHandler {
					handlers = append(handlers[:i], handlers[i+1:]...)
					break
				}
			}
		}
		shard.handlers[eventType] = handlers
		shard.mu.Unlock()
	}

	// For async handlers, we don't wait inline to avoid blocking
	// Users can call bus.Wait() if they need to wait for completion

	// Call after publish hook
	if bus.afterPublish != nil {
		bus.afterPublish(eventType, event)
	}
}

// Clear removes all handlers for events of type T
func Clear[T any](bus *EventBus) {
	eventType := reflect.TypeOf((*T)(nil)).Elem()

	shard := bus.getShard(eventType)
	shard.mu.Lock()
	delete(shard.handlers, eventType)
	shard.mu.Unlock()
}

// ClearAll removes all handlers for all event types
func ClearAll(bus *EventBus) {
	for i := 0; i < numShards; i++ {
		bus.shards[i].mu.Lock()
		bus.shards[i].handlers = make(map[reflect.Type][]*internalHandler)
		bus.shards[i].mu.Unlock()
	}
}

// HasHandlers checks if there are handlers for events of type T
func HasHandlers[T any](bus *EventBus) bool {
	eventType := reflect.TypeOf((*T)(nil)).Elem()

	shard := bus.getShard(eventType)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	return len(shard.handlers[eventType]) > 0
}

// HandlerCount returns the number of handlers for events of type T
func HandlerCount[T any](bus *EventBus) int {
	eventType := reflect.TypeOf((*T)(nil)).Elem()

	shard := bus.getShard(eventType)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	return len(shard.handlers[eventType])
}

// Wait blocks until all async handlers complete
func (bus *EventBus) Wait() {
	bus.wg.Wait()
}

// callHandlerWithContext calls a handler with proper type checking and panic recovery
func callHandlerWithContext[T any](h *internalHandler, ctx context.Context, event T, panicHandler PanicHandler) {
	defer func() {
		if r := recover(); r != nil && panicHandler != nil {
			panicHandler(event, h.handlerType, r)
		}
	}()

	// Sequential handlers need locking
	if h.sequential {
		h.mu.Lock()
		defer h.mu.Unlock()
	}

	// Try common handler types first for performance
	switch fn := h.handler.(type) {
	case Handler[T]:
		fn(event)
	case ContextHandler[T]:
		fn(ctx, event)
	case func(T):
		fn(event)
	case func(context.Context, T):
		fn(ctx, event)
	case func(any):
		fn(event)
	case func(context.Context, any):
		fn(ctx, event)
	case func(context.Context, any) error:
		_ = fn(ctx, event)
	default:
		// Fallback to reflection for other types
		handlerValue := reflect.ValueOf(h.handler)
		eventValue := reflect.ValueOf(event)

		if h.handlerType.Kind() == reflect.Func {
			numIn := h.handlerType.NumIn()
			if numIn == 1 {
				handlerValue.Call([]reflect.Value{eventValue})
			} else if numIn == 2 {
				handlerValue.Call([]reflect.Value{reflect.ValueOf(ctx), eventValue})
			}
		}
	}
}

// Subscribe Options

// Once makes the handler execute only once
func Once() SubscribeOption {
	return func(h *internalHandler) {
		h.once = true
	}
}

// Async makes the handler execute asynchronously
func Async() SubscribeOption {
	return func(h *internalHandler) {
		h.async = true
	}
}

// Sequential ensures the handler executes sequentially (with mutex)
func Sequential() SubscribeOption {
	return func(h *internalHandler) {
		h.sequential = true
	}
}

// WithFilter configures the handler to only receive events that match the predicate
func WithFilter[T any](predicate func(T) bool) SubscribeOption {
	return func(h *internalHandler) {
		h.filter = predicate
	}
}

// Bus Options

// WithPanicHandler sets a panic handler for the event bus
func WithPanicHandler(handler PanicHandler) Option {
	return func(bus *EventBus) {
		bus.panicHandler = handler
	}
}

// WithBeforePublish sets a hook that's called before publishing events
func WithBeforePublish(hook PublishHook) Option {
	return func(bus *EventBus) {
		bus.beforePublish = hook
	}
}

// WithAfterPublish sets a hook that's called after publishing events
func WithAfterPublish(hook PublishHook) Option {
	return func(bus *EventBus) {
		bus.afterPublish = hook
	}
}

// WithPersistenceErrorHandler sets the error handler for persistence failures
func WithPersistenceErrorHandler(handler PersistenceErrorHandler) Option {
	return func(bus *EventBus) {
		bus.persistenceErrorHandler = handler
	}
}

// WithPersistenceTimeout sets the timeout for persistence operations
func WithPersistenceTimeout(timeout time.Duration) Option {
	return func(bus *EventBus) {
		bus.persistenceTimeout = timeout
	}
}

// startEventProcessor starts processing persisted events
func (bus *EventBus) startEventProcessor() {
	// This will be implemented when needed for event replay
	return
}

// Backward compatibility methods

// SetPanicHandler sets the panic handler (for backward compatibility)
func (bus *EventBus) SetPanicHandler(handler PanicHandler) {
	bus.panicHandler = handler
}

// SetBeforePublishHook sets the before publish hook (for backward compatibility)
func (bus *EventBus) SetBeforePublishHook(hook PublishHook) {
	bus.beforePublish = hook
}

// SetAfterPublishHook sets the after publish hook (for backward compatibility)
func (bus *EventBus) SetAfterPublishHook(hook PublishHook) {
	bus.afterPublish = hook
}

// SetPersistenceErrorHandler sets the persistence error handler (for runtime configuration)
func (bus *EventBus) SetPersistenceErrorHandler(handler PersistenceErrorHandler) {
	bus.persistenceErrorHandler = handler
}
