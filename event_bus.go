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
	handler     any
	handlerType reflect.Type
	eventType   reflect.Type
	once        bool
	async       bool
	sequential  bool
	filter      any // Predicate function for filtering events
	mu          sync.Mutex
	executed    uint32 // For once handlers, atomically tracks if executed

	// replayErrorPolicy controls how SubscribeWithReplay treats stored
	// events that cannot be decoded; it has no effect on live delivery.
	replayErrorPolicy ReplayErrorPolicy
}

// PanicHandler is called when a handler panics
type PanicHandler func(event any, handlerType reflect.Type, panicValue any)

// PersistenceErrorHandler is called when event persistence fails
type PersistenceErrorHandler func(event any, eventType reflect.Type, err error)

// PublishHook is called when an event is published
type PublishHook func(eventType reflect.Type, event any)

// PublishHookContext is called when an event is published (with context)
type PublishHookContext func(ctx context.Context, eventType reflect.Type, event any)

const numShards = 32 // Power of 2 for efficient modulo

// shard represents a single shard with its own mutex
type shard struct {
	mu       sync.RWMutex
	handlers map[reflect.Type][]*internalHandler
}

// EventBus is a high-performance event bus with sharded locks
type EventBus struct {
	shards           [numShards]*shard
	panicHandler     PanicHandler
	beforePublish    PublishHook
	afterPublish     PublishHook
	beforePublishCtx PublishHookContext
	afterPublishCtx  PublishHookContext

	// Async handler tracking. A plain sync.WaitGroup would be racy here:
	// Publish (Add) may legally run concurrently with Wait/Shutdown, which
	// WaitGroup forbids when the counter is at zero.
	asyncMu    sync.Mutex
	asyncCond  *sync.Cond
	asyncCount int

	// asyncSem, when non-nil, bounds the number of concurrently running
	// async handler goroutines (see WithAsyncHandlerLimit).
	asyncSem chan struct{}

	// Optional persistence fields (nil if not using persistence)
	store                   EventStore
	subscriptionStore       SubscriptionStore
	persistenceErrorHandler PersistenceErrorHandler
	persistenceTimeout      time.Duration
	replayBatchSize         int // Batch size for Replay (default: 100)

	// Upcast registry for event migration
	upcastRegistry *upcastRegistry

	// Optional observability (metrics & tracing)
	observability Observability
}

// TypeNamer is an optional interface that events can implement to provide
// their own type name. This gives explicit control over event type naming,
// which is useful for:
//   - Stable type names across package refactoring
//   - Custom versioning schemes (e.g., "UserCreated.v2")
//   - Compatibility with external event stores
//
// If an event implements TypeNamer, EventType() will use the provided name
// instead of the reflection-based package-qualified name.
//
// Example:
//
//	type UserCreatedEvent struct {
//	    UserID string
//	}
//
//	func (e UserCreatedEvent) EventTypeName() string {
//	    return "user.created.v1"
//	}
type TypeNamer interface {
	EventTypeName() string
}

// EventType returns the type name of an event.
// If the event implements TypeNamer, it returns the custom name.
// Otherwise, it returns the reflection-based package-qualified name.
//
// This is useful for comparing with StoredEvent.Type during replay.
//
// Example with reflection (default):
//
//	eventType := EventType(MyEvent{})
//	// Returns: "github.com/mypackage/MyEvent"
//
// Example with TypeNamer:
//
//	type MyEvent struct{}
//	func (e MyEvent) EventTypeName() string { return "my-event.v1" }
//	eventType := EventType(MyEvent{})
//	// Returns: "my-event.v1"
//
// Usage in replay:
//
//	bus.Replay(ctx, 0, func(event *StoredEvent) error {
//	    if event.Type == EventType(MyEvent{}) {
//	        // Process MyEvent
//	    }
//	    return nil
//	})
func EventType(event any) string {
	// Check if event implements TypeNamer
	if namer, ok := event.(TypeNamer); ok {
		return namer.EventTypeName()
	}
	// Fall back to reflection-based name
	return reflect.TypeOf(event).String()
}

// Observability is an optional interface for metrics and tracing.
// Implementations can track event publishing, handler execution, and errors.
//
// This interface is designed to be zero-cost when not used - if no
// observability is configured, there is no performance overhead.
//
// The context returned from each method can be used to propagate trace
// spans and other context-specific data through the event processing pipeline.
//
// Example implementation: see github.com/jilio/ebu/otel package for
// OpenTelemetry integration.
type Observability interface {
	// OnPublishStart is called when an event is about to be published.
	// Returns a context that will be passed to handlers and subsequent hooks.
	// The event parameter allows implementations to extract custom attributes.
	OnPublishStart(ctx context.Context, eventType string, event any) context.Context

	// OnPublishComplete is called after all synchronous handlers complete.
	// Note: This is called before async handlers complete.
	OnPublishComplete(ctx context.Context, eventType string)

	// OnHandlerStart is called before a handler executes.
	// Returns a context for the handler execution.
	OnHandlerStart(ctx context.Context, eventType string, async bool) context.Context

	// OnHandlerComplete is called after a handler completes.
	// The error parameter is non-nil if the handler panicked.
	OnHandlerComplete(ctx context.Context, eventType string, duration time.Duration, err error)

	// OnPersistStart is called before persisting an event. The assigned
	// offset is not known yet at this point; it is reported to
	// OnPersistComplete instead.
	OnPersistStart(ctx context.Context, eventType string) context.Context

	// OnPersistComplete is called after persisting an event. On success,
	// offset is the offset the store assigned; on failure it is empty and
	// err is non-nil.
	OnPersistComplete(ctx context.Context, eventType string, duration time.Duration, offset Offset, err error)
}

// Option is a function that configures the EventBus
type Option func(*EventBus)

// New creates a new EventBus with sharded locks for better performance
func New(opts ...Option) *EventBus {
	bus := &EventBus{
		upcastRegistry: newUpcastRegistry(),
	}
	bus.asyncCond = sync.NewCond(&bus.asyncMu)

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

	return bus
}

// getShard returns the shard for a given event type using FNV hash
func (bus *EventBus) getShard(eventType reflect.Type) *shard {
	h := fnv.New32a()
	h.Write([]byte(eventType.String()))
	shardIndex := h.Sum32() & (numShards - 1) // Fast modulo for power of 2
	return bus.shards[shardIndex]
}

// buildHandler is the single validation chokepoint for every subscription
// entry point: it constructs the internalHandler, applies each option exactly
// once, and runs all subscription-time validation (event type, options,
// filter). New entry points must go through it so no check can be missed.
func buildHandler(handler any, eventType reflect.Type, opts []SubscribeOption) (*internalHandler, error) {
	if err := validateEventType(eventType); err != nil {
		return nil, err
	}

	h := &internalHandler{
		handler:     handler,
		handlerType: reflect.TypeOf(handler),
		eventType:   eventType,
	}

	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("eventbus: subscribe option cannot be nil")
		}
		opt(h)
	}

	if err := validateFilter(h, eventType); err != nil {
		return nil, err
	}

	return h, nil
}

// addHandler registers a validated handler in the shard for its event type.
func (bus *EventBus) addHandler(eventType reflect.Type, h *internalHandler) {
	shard := bus.getShard(eventType)
	shard.mu.Lock()
	shard.handlers[eventType] = append(shard.handlers[eventType], h)
	shard.mu.Unlock()
}

// Subscribe registers a handler for events of type T.
//
// T must be a concrete type. Interface types are rejected with an error:
// Publish routes events by their dynamic (concrete) type, so a handler
// registered under an interface type could never receive an event — even
// one published through a variable of that interface type.
func Subscribe[T any](bus *EventBus, handler Handler[T], opts ...SubscribeOption) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
	if handler == nil {
		return fmt.Errorf("eventbus: handler cannot be nil")
	}

	eventType := reflect.TypeOf((*T)(nil)).Elem()
	h, err := buildHandler(handler, eventType, opts)
	if err != nil {
		return err
	}

	bus.addHandler(eventType, h)
	return nil
}

// SubscribeContext registers a context-aware handler for events of type T.
//
// T must be a concrete type; interface types are rejected (see Subscribe).
func SubscribeContext[T any](bus *EventBus, handler ContextHandler[T], opts ...SubscribeOption) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
	if handler == nil {
		return fmt.Errorf("eventbus: handler cannot be nil")
	}

	eventType := reflect.TypeOf((*T)(nil)).Elem()
	h, err := buildHandler(handler, eventType, opts)
	if err != nil {
		return err
	}

	bus.addHandler(eventType, h)
	return nil
}

// validateEventType rejects event types that Publish could never route to.
// Publish looks handlers up by the event's dynamic (concrete) type, so a
// subscription registered under an interface type would be silently dead.
func validateEventType(eventType reflect.Type) error {
	if eventType.Kind() == reflect.Interface {
		return fmt.Errorf("eventbus: cannot subscribe to interface type %s: events are routed by their concrete type, so an interface subscription would never receive events; subscribe to each concrete event type instead", eventType)
	}
	return nil
}

// validateFilter ensures a WithFilter predicate matches the subscribed event type.
// Without this check, a mismatched predicate (e.g. WithFilter[U] on Subscribe[T])
// would silently never fire and the handler would receive all events unfiltered.
func validateFilter(h *internalHandler, eventType reflect.Type) error {
	if h.filter == nil {
		return nil
	}
	ft := reflect.TypeOf(h.filter)
	if ft.Kind() != reflect.Func || ft.NumIn() != 1 || ft.In(0) != eventType {
		return fmt.Errorf("eventbus: filter predicate type %s does not match event type %s", ft, eventType)
	}
	return nil
}

// Unsubscribe removes a handler for events of type T.
//
// Handlers are matched by function code pointer. This has two known
// limitations inherent to Go:
//   - Two closures created from the same function literal share a code
//     pointer and are indistinguishable; the first registered one is removed.
//   - Method values on different receivers share a code pointer.
//
// If you need precise unsubscription of closures, keep a reference to the
// exact handler you registered and unsubscribe with it, or use Clear[T].
func Unsubscribe[T any, H any](bus *EventBus, handler H) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
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

// Publish publishes an event to all registered handlers.
// It panics if bus is nil.
func Publish[T any](bus *EventBus, event T) {
	PublishContext(bus, context.Background(), event)
}

// PublishContext publishes an event with context to all registered handlers.
// It panics if bus is nil.
//
// When the bus has a store configured (WithStore), the event is persisted
// before handlers run. Persistence is best-effort: on failure the event is
// still delivered to handlers and the error is reported to the
// PersistenceErrorHandler (see WithPersistenceErrorHandler).
func PublishContext[T any](bus *EventBus, ctx context.Context, event T) {
	if bus == nil {
		panic("eventbus: Publish called with nil bus")
	}

	eventType := reflect.TypeOf(event)
	eventTypeName := EventType(event)

	// Observability: Track publish start
	if bus.observability != nil {
		ctx = bus.observability.OnPublishStart(ctx, eventTypeName, event)
	}

	// Call before publish hooks
	if bus.beforePublish != nil {
		bus.beforePublish(eventType, event)
	}
	if bus.beforePublishCtx != nil {
		bus.beforePublishCtx(ctx, eventType, event)
	}

	// Persist the event before handlers run. On success the assigned offset
	// is attached to ctx and can be retrieved with OffsetFromContext.
	if bus.store != nil {
		ctx = bus.persistEvent(ctx, eventType, event)
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
	var onceHandlersToRemove []*internalHandler

	for _, h := range handlersCopy {
		// Check context cancellation before doing anything with the handler.
		// This must happen before the Once CAS so a cancelled publish never
		// consumes a once-handler without executing it.
		select {
		case <-ctx.Done():
			continue
		default:
		}

		// Check filter if present. The predicate type is validated at
		// Subscribe time; if the assertion still fails, fail closed.
		if h.filter != nil {
			filterFunc, ok := h.filter.(func(T) bool)
			if !ok || !filterFunc(event) {
				continue // Skip this handler as event doesn't match filter
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
			// Bounded async concurrency: acquire a slot before spawning.
			// This intentionally blocks the publisher when the limit is
			// reached so a publish spike cannot create an unbounded number
			// of goroutines (see WithAsyncHandlerLimit).
			if bus.asyncSem != nil {
				bus.asyncSem <- struct{}{}
			}
			bus.asyncStarted()
			go func(handler *internalHandler) {
				defer bus.asyncFinished()
				if bus.asyncSem != nil {
					defer func() { <-bus.asyncSem }()
				}

				// Once handlers always run: their execution slot was already
				// consumed by the CAS above, so skipping here would silently
				// drop them forever. Other handlers re-check the context.
				if !handler.once {
					select {
					case <-ctx.Done():
						return
					default:
					}
				}
				callHandlerWithContext(handler, ctx, event, bus.panicHandler, bus.observability, eventTypeName, true)
			}(h)
		} else {
			callHandlerWithContext(h, ctx, event, bus.panicHandler, bus.observability, eventTypeName, false)
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

	// Call after publish hooks
	if bus.afterPublish != nil {
		bus.afterPublish(eventType, event)
	}
	if bus.afterPublishCtx != nil {
		bus.afterPublishCtx(ctx, eventType, event)
	}

	// Observability: Track publish complete (sync handlers done)
	if bus.observability != nil {
		bus.observability.OnPublishComplete(ctx, eventTypeName)
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

// asyncStarted records the start of an async handler goroutine.
func (bus *EventBus) asyncStarted() {
	bus.asyncMu.Lock()
	bus.asyncCount++
	bus.asyncMu.Unlock()
}

// asyncFinished records the completion of an async handler goroutine and
// wakes Wait callers when the last one finishes.
func (bus *EventBus) asyncFinished() {
	bus.asyncMu.Lock()
	bus.asyncCount--
	if bus.asyncCount == 0 {
		bus.asyncCond.Broadcast()
	}
	bus.asyncMu.Unlock()
}

// Wait blocks until all async handlers complete.
// It is safe to call concurrently with Publish.
func (bus *EventBus) Wait() {
	bus.asyncMu.Lock()
	for bus.asyncCount > 0 {
		bus.asyncCond.Wait()
	}
	bus.asyncMu.Unlock()
}

// callHandlerWithContext calls a handler with proper type checking and panic recovery
func callHandlerWithContext[T any](h *internalHandler, ctx context.Context, event T, panicHandler PanicHandler, obs Observability, eventTypeName string, async bool) {
	start := time.Now()
	var panicErr error

	defer func() {
		duration := time.Since(start)
		if r := recover(); r != nil {
			panicErr = fmt.Errorf("handler panic: %v", r)
			if panicHandler != nil {
				panicHandler(event, h.handlerType, r)
			}
		}
		// Observability: Track handler complete
		if obs != nil {
			obs.OnHandlerComplete(ctx, eventTypeName, duration, panicErr)
		}
	}()

	// Observability: Track handler start
	if obs != nil {
		ctx = obs.OnHandlerStart(ctx, eventTypeName, async)
	}

	// Sequential handlers need locking
	if h.sequential {
		h.mu.Lock()
		defer h.mu.Unlock()
	}

	// Handlers are always stored as Handler[T] (Subscribe) or
	// ContextHandler[T] (SubscribeContext); no other shapes can be registered.
	switch fn := h.handler.(type) {
	case Handler[T]:
		fn(event)
	case ContextHandler[T]:
		fn(ctx, event)
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

// WithBeforePublishContext sets a context-aware hook that's called before publishing events
func WithBeforePublishContext(hook PublishHookContext) Option {
	return func(bus *EventBus) {
		bus.beforePublishCtx = hook
	}
}

// WithAfterPublishContext sets a context-aware hook that's called after publishing events
func WithAfterPublishContext(hook PublishHookContext) Option {
	return func(bus *EventBus) {
		bus.afterPublishCtx = hook
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

// WithReplayBatchSize sets the batch size for Replay operations.
// This controls how many events are read at a time when using a store
// that doesn't implement EventStoreStreamer.
// Default is 100 if not set or set to 0.
func WithReplayBatchSize(size int) Option {
	return func(bus *EventBus) {
		bus.replayBatchSize = size
	}
}

// WithObservability sets the observability implementation for metrics and tracing
func WithObservability(obs Observability) Option {
	return func(bus *EventBus) {
		bus.observability = obs
	}
}

// WithAsyncHandlerLimit bounds the number of concurrently running async
// handler goroutines. When the limit is reached, Publish blocks until a
// running async handler finishes, providing backpressure instead of
// unbounded goroutine growth during publish spikes.
//
// A limit <= 0 means unlimited (the default).
//
// Deadlock warning: do not publish events that have async subscribers from
// inside an async handler when a limit is set. Such a nested Publish blocks
// waiting for a free slot while the publishing handler occupies one; if all
// slots are held by handlers blocked the same way, none can ever be
// released. Publish re-entrantly only from synchronous handlers, or size
// the limit above the maximum possible nesting fan-out.
func WithAsyncHandlerLimit(n int) Option {
	return func(bus *EventBus) {
		if n > 0 {
			bus.asyncSem = make(chan struct{}, n)
		}
	}
}

// Shutdown gracefully shuts down the event bus, waiting for async handlers to complete.
// It respects the context timeout/cancellation.
// If the store implements io.Closer, its Close() method will be called after handlers complete.
func (bus *EventBus) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		bus.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Close store if it implements io.Closer
		if bus.store != nil {
			if closer, ok := bus.store.(interface{ Close() error }); ok {
				if err := closer.Close(); err != nil {
					return fmt.Errorf("failed to close store: %w", err)
				}
			}
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Backward compatibility methods
//
// The Set* methods below write bus fields without synchronization. They are
// intended for configuration before the bus is shared across goroutines;
// calling them concurrently with Publish is a data race.

// SetPanicHandler sets the panic handler.
// Not safe to call concurrently with Publish; configure before use.
//
// Deprecated: use the WithPanicHandler option with New instead.
func (bus *EventBus) SetPanicHandler(handler PanicHandler) {
	bus.panicHandler = handler
}

// SetBeforePublishHook sets the before publish hook.
// Not safe to call concurrently with Publish; configure before use.
//
// Deprecated: use the WithBeforePublish option with New instead.
func (bus *EventBus) SetBeforePublishHook(hook PublishHook) {
	bus.beforePublish = hook
}

// SetAfterPublishHook sets the after publish hook.
// Not safe to call concurrently with Publish; configure before use.
//
// Deprecated: use the WithAfterPublish option with New instead.
func (bus *EventBus) SetAfterPublishHook(hook PublishHook) {
	bus.afterPublish = hook
}

// SetPersistenceErrorHandler sets the persistence error handler (for runtime configuration).
// Not safe to call concurrently with Publish; configure before use.
func (bus *EventBus) SetPersistenceErrorHandler(handler PersistenceErrorHandler) {
	bus.persistenceErrorHandler = handler
}
