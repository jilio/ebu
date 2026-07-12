package eventbus

import (
	"context"
	"errors"
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
	handler      any
	handlerType  reflect.Type
	eventType    reflect.Type
	once         bool
	async        bool
	sequential   bool
	filter       any // Predicate function for filtering events
	invoke       func(context.Context, any)
	filterInvoke func(any) bool
	mu           sync.Mutex
	executed     uint32 // For once handlers, atomically tracks if executed
	onceStateMu  sync.Mutex
	// onceInFlight is set only for durable dispatch. It distinguishes a Once
	// handler consumed by an earlier successful delivery from one whose
	// cancelled async invocation is still finishing and must not be
	// checkpointed past by an immediate follower restart.
	onceInFlight uint32

	// replayErrorPolicy controls how SubscribeWithReplay treats stored
	// events that cannot be decoded; it has no effect on live delivery.
	replayErrorPolicy ReplayErrorPolicy

	// internalDelivery is used by infrastructure subscriptions that need to
	// return a delivery error (currently the log-backed resumable coordinator).
	// suppressObservability prevents that signal handler from double-counting
	// the real user handler invocation performed by the coordinator. onRemove
	// lets infrastructure tear down state before its marker leaves the shard.
	internalDelivery      func(context.Context, any) error
	suppressObservability bool
	onRemove              func()
}

// PanicHandler is called when a handler panics
type PanicHandler func(event any, handlerType reflect.Type, panicValue any)

type handlerPanicError struct {
	value any
}

type durableOnceAttempt struct {
	handler  *internalHandler
	rollback atomic.Bool
}

func (a *durableOnceAttempt) complete() {
	a.handler.onceStateMu.Lock()
	atomic.StoreUint32(&a.handler.onceInFlight, 0)
	if a.rollback.Load() {
		atomic.StoreUint32(&a.handler.executed, 0)
	}
	a.handler.onceStateMu.Unlock()
}

func (a *durableOnceAttempt) requestRollback() {
	a.rollback.Store(true)
	a.handler.onceStateMu.Lock()
	if atomic.LoadUint32(&a.handler.onceInFlight) == 0 {
		atomic.StoreUint32(&a.handler.executed, 0)
	}
	a.handler.onceStateMu.Unlock()
}

func (e *handlerPanicError) Error() string {
	return fmt.Sprintf("handler panic: %v", e.value)
}

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

	// strictPersistence, when set, makes a failed Append stop delivery:
	// handlers never observe an event the log did not record (see
	// WithStrictPersistence).
	strictPersistence bool

	// originID uniquely identifies this bus instance; it is stamped on every
	// persisted event's Origin field so processes sharing a log can tell
	// their own events apart from a peer's.
	originID string

	// logDelivery, when set, makes the store the only delivery path: Publish
	// appends to the log and never dispatches locally; handlers receive
	// events exclusively from a Follow loop (see WithLogDelivery).
	logDelivery bool

	// followDecoders maps a persisted type name to a closure that decodes a
	// stored event into its Go type and dispatches it locally. Entries are
	// registered by the generic Subscribe* entry points, which are the only
	// places the concrete type is statically known. A nil followTypes entry
	// marks a name made ambiguous on a nonpersistent bus; no decoder is retained
	// for that name.
	followMu       sync.RWMutex
	followDecoders map[string]followDecoder
	followTypes    map[string]reflect.Type

	// persistedTypes prevents a persistent bus from assigning the same durable
	// wire name to distinct Go types. configuring defers that audit until every
	// New option has run. replayIDs prevents two live coordinators on this bus
	// from racing the same scalar subscription checkpoint.
	persistedTypes    *persistedTypeRegistry
	configuring       bool
	replayMu          sync.Mutex
	replayIDs         map[string]struct{}
	replayMarkers     map[*internalHandler]struct{}
	replayMarkerCount atomic.Int64

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
// instead of the reflection-based name. EventTypeName must depend only on the
// type, not instance fields: generic replay/follow registration may derive it
// from a fresh zero value.
//
// Persisted or distributed event types should implement TypeNamer. Go's
// reflection fallback uses the declared package name (for example,
// "orders.Created"), not its full import path, and can therefore change after
// refactors or collide with another package that has the same name.
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
// Otherwise, it returns Go's reflection name, which uses the declared package
// name rather than the full import path. It returns "nil" for a nil event.
//
// This is useful for comparing with StoredEvent.Type during replay.
//
// Example with reflection (default):
//
//	eventType := EventType(MyEvent{})
//	// Returns: "mypackage.MyEvent"
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
//	bus.Replay(ctx, OffsetOldest, func(event *StoredEvent) error {
//	    if event.Type == EventType(MyEvent{}) {
//	        // Process MyEvent
//	    }
//	    return nil
//	})
func EventType(event any) string {
	if event == nil {
		return "nil"
	}
	value := reflect.ValueOf(event)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return typeNameOf(value.Type())
	}
	if namer, ok := event.(TypeNamer); ok {
		return namer.EventTypeName()
	}
	return value.Type().String()
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

// New creates a new EventBus with sharded locks for better performance.
// It panics when the final option set is internally inconsistent, including
// when typed registrations made by custom options assign one durable event
// name to distinct Go types on a bus configured with WithStore.
func New(opts ...Option) *EventBus {
	bus := &EventBus{
		upcastRegistry: newUpcastRegistry(),
		originID:       NewEventID(),
		followDecoders: make(map[string]followDecoder),
		followTypes:    make(map[string]reflect.Type),
		persistedTypes: newDeferredPersistedTypeRegistry(),
		configuring:    true,
		replayIDs:      make(map[string]struct{}),
		replayMarkers:  make(map[*internalHandler]struct{}),
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
	bus.configuring = false

	// Options are arbitrary functions and may perform typed registrations.
	// Audit their complete durable-name set only after every option has run so
	// WithStore has identical behavior regardless of its position in opts.
	if bus.store != nil {
		bus.activatePersistenceTypes()
	}

	// Log-delivery mode without a store would silently drop every publish
	// (nothing appends, nothing follows). That is a configuration bug, not a
	// runtime condition — fail loudly at construction, mirroring WithUpcast.
	if bus.logDelivery && bus.store == nil {
		panic("eventbus: WithLogDelivery requires WithStore: without a store there is no log to deliver from")
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

// buildPayloadHandler and buildContextHandler bind typed user functions once
// at subscription time. Dispatch routes by an event's dynamic concrete type,
// which can differ from Publish's static type when a value is passed through
// any or another interface. Keeping these adapters on internalHandler avoids
// both a static-generic type mismatch and reflection in the hot path.
func buildPayloadHandler[T any](handler Handler[T], eventType reflect.Type, opts []SubscribeOption) (*internalHandler, error) {
	h, err := buildHandler(handler, eventType, opts)
	if err != nil {
		return nil, err
	}
	h.invoke = func(_ context.Context, event any) { handler(event.(T)) }
	bindFilter[T](h)
	return h, nil
}

func buildContextHandler[T any](handler ContextHandler[T], eventType reflect.Type, opts []SubscribeOption) (*internalHandler, error) {
	h, err := buildHandler(handler, eventType, opts)
	if err != nil {
		return nil, err
	}
	h.invoke = func(ctx context.Context, event any) { handler(ctx, event.(T)) }
	bindFilter[T](h)
	return h, nil
}

func bindFilter[T any](h *internalHandler) {
	if h.filter == nil {
		return
	}
	predicate := h.filter.(func(T) bool)
	h.filterInvoke = func(event any) bool { return predicate(event.(T)) }
}

// addHandler registers a validated handler in the shard for its event type.
func (bus *EventBus) addHandler(eventType reflect.Type, h *internalHandler) {
	shard := bus.getShard(eventType)
	shard.mu.Lock()
	shard.handlers[eventType] = append(shard.handlers[eventType], h)
	shard.mu.Unlock()
}

// removeHandler unregisters a handler by identity (pointer equality on the
// internalHandler), which — unlike Unsubscribe's code-pointer matching —
// always removes exactly the registration it was given. It is a no-op when
// the handler is already gone (unsubscribed twice, or removed by Clear).
func (bus *EventBus) removeHandler(eventType reflect.Type, h *internalHandler) {
	shard := bus.getShard(eventType)
	shard.mu.Lock()

	handlers := shard.handlers[eventType]
	for i, existing := range handlers {
		if existing == h {
			if existing.onRemove != nil {
				existing.onRemove()
			}
			shard.handlers[eventType] = append(handlers[:i], handlers[i+1:]...)
			shard.mu.Unlock()
			return
		}
	}
	shard.mu.Unlock()
}

// OriginID returns the unique identifier of this bus instance. Every event
// the bus persists carries it as Event.Origin, so consumers of a log shared
// by several processes can tell this instance's events from a peer's.
func (bus *EventBus) OriginID() string {
	return bus.originID
}

// Subscription is a handle to a single registration, returned by
// SubscribeWithHandle and SubscribeContextWithHandle. Unlike Unsubscribe —
// which matches handlers by function code pointer and therefore cannot tell
// two closures from the same function literal apart — a handle identifies
// exactly the registration that created it.
type Subscription struct {
	bus       *EventBus
	eventType reflect.Type
	h         *internalHandler
}

// Unsubscribe removes this subscription's handler from the bus. It is
// idempotent: calling it more than once (or after Clear/ClearAll already
// removed the handler) is a no-op. Safe for concurrent use.
func (s *Subscription) Unsubscribe() {
	s.bus.removeHandler(s.eventType, s.h)
}

// SubscribeWithHandle registers a handler for events of type T and returns a
// Subscription handle for precise removal. Semantics are identical to
// Subscribe otherwise; T must be a concrete type.
//
// Prefer this over Subscribe+Unsubscribe when the handler is a closure:
// handles remove exactly the registration they came from, with none of
// Unsubscribe's code-pointer ambiguity.
func SubscribeWithHandle[T any](bus *EventBus, handler Handler[T], opts ...SubscribeOption) (*Subscription, error) {
	if bus == nil {
		return nil, fmt.Errorf("eventbus: bus cannot be nil")
	}
	if handler == nil {
		return nil, fmt.Errorf("eventbus: handler cannot be nil")
	}

	eventType := reflect.TypeOf((*T)(nil)).Elem()
	h, err := buildPayloadHandler(handler, eventType, opts)
	if err != nil {
		return nil, err
	}

	if err := registerFollowDecoder[T](bus); err != nil {
		return nil, err
	}
	bus.addHandler(eventType, h)
	return &Subscription{bus: bus, eventType: eventType, h: h}, nil
}

// SubscribeContextWithHandle registers a context-aware handler for events of
// type T and returns a Subscription handle for precise removal. Semantics are
// identical to SubscribeContext otherwise; T must be a concrete type.
func SubscribeContextWithHandle[T any](bus *EventBus, handler ContextHandler[T], opts ...SubscribeOption) (*Subscription, error) {
	if bus == nil {
		return nil, fmt.Errorf("eventbus: bus cannot be nil")
	}
	if handler == nil {
		return nil, fmt.Errorf("eventbus: handler cannot be nil")
	}

	eventType := reflect.TypeOf((*T)(nil)).Elem()
	h, err := buildContextHandler(handler, eventType, opts)
	if err != nil {
		return nil, err
	}

	if err := registerFollowDecoder[T](bus); err != nil {
		return nil, err
	}
	bus.addHandler(eventType, h)
	return &Subscription{bus: bus, eventType: eventType, h: h}, nil
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
	h, err := buildPayloadHandler(handler, eventType, opts)
	if err != nil {
		return err
	}

	if err := registerFollowDecoder[T](bus); err != nil {
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
	h, err := buildContextHandler(handler, eventType, opts)
	if err != nil {
		return err
	}

	if err := registerFollowDecoder[T](bus); err != nil {
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
	if reflect.ValueOf(h.filter).IsNil() {
		return fmt.Errorf("eventbus: filter predicate cannot be nil")
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
// If you need precise unsubscription of closures, use SubscribeWithHandle
// and the returned Subscription's Unsubscribe method instead — handles are
// matched by identity, not code pointer. Alternatively keep a reference to
// the exact handler you registered and unsubscribe with it, or use Clear[T].
func Unsubscribe[T any, H any](bus *EventBus, handler H) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
	eventType := reflect.TypeOf((*T)(nil)).Elem()
	handlerValue := reflect.ValueOf(handler)
	if !handlerValue.IsValid() || handlerValue.Kind() != reflect.Func || handlerValue.IsNil() {
		return fmt.Errorf("eventbus: handler must be a non-nil function")
	}
	handlerPtr := handlerValue.Pointer()

	shard := bus.getShard(eventType)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	handlers := shard.handlers[eventType]
	for i, h := range handlers {
		// Infrastructure markers route through internalDelivery and deliberately
		// have no public handler function to compare.
		if h.handler == nil {
			continue
		}
		registered := reflect.ValueOf(h.handler)
		if registered.Kind() == reflect.Func && !registered.IsNil() && registered.Pointer() == handlerPtr {
			if h.onRemove != nil {
				h.onRemove()
			}
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
// before handlers run. Persistence is best-effort by default for ordinary
// Subscribe handlers: on failure they still receive the in-memory event and
// the error is reported to the PersistenceErrorHandler (see
// WithPersistenceErrorHandler). Log-backed resumable subscriptions consume
// only records visible in EventStore. An Append error can be ambiguous: if a
// remote store committed before its acknowledgement was lost, that durable
// record may still be delivered now or by a later replay. WithStrictPersistence
// makes every handler skip immediate delivery after a reported persist error. Use
// TryPublish/TryPublishContext to receive the persistence error directly.
func PublishContext[T any](bus *EventBus, ctx context.Context, event T) {
	publishContext(bus, ctx, event)
}

// TryPublish is Publish with the reported persistence outcome returned: it
// returns the marshal/Append error (nil on success or when no store is
// configured). An Append error may be ambiguous if a remote commit succeeded
// before its acknowledgement was lost. It panics if bus is nil.
func TryPublish[T any](bus *EventBus, event T) error {
	return publishContext(bus, context.Background(), event)
}

// TryPublishContext is PublishContext with the persistence outcome returned.
//
// The returned error does not by itself imply ordinary Subscribe handlers were
// skipped: in best-effort mode (the default) their delivery proceeds despite
// the error, while under WithStrictPersistence it is skipped. Log-backed
// resumable subscriptions consume only records visible in EventStore. Because
// an Append error may arrive after a remote commit, such a record can still be
// delivered now or by a later replay. Either way the error is also reported to
// the PersistenceErrorHandler, which remains the right channel for passive
// monitoring; TryPublishContext is for publishers that must act on the failure
// (e.g. fail the request that caused the publish).
// It panics if bus is nil.
func TryPublishContext[T any](bus *EventBus, ctx context.Context, event T) error {
	return publishContext(bus, ctx, event)
}

// publishContext is the single publish path: it persists (when configured),
// delivers, and returns the persistence error, if any. Publish/PublishContext
// discard the error; TryPublish/TryPublishContext surface it.
func publishContext[T any](bus *EventBus, ctx context.Context, event T) error {
	if bus == nil {
		panic("eventbus: Publish called with nil bus")
	}

	eventType := reflect.TypeOf(event)
	eventTypeName := EventType(event)
	if bus != nil && bus.store != nil {
		// Persistent routing must use the reproducible type-derived name. The
		// persistence step separately rejects an instance-dependent TypeNamer.
		eventTypeName = typeNameOf(eventType)
	}

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
	var persistErr error
	if bus.store != nil {
		ctx, persistErr = bus.persistEvent(ctx, eventType, event)
	}

	// In strict mode a failed persist skips delivery: handlers never observe
	// an event the log did not record, so replay and live handling can never
	// diverge. In log-delivery mode (WithLogDelivery) local dispatch is
	// always skipped: delivery happens exclusively through the follower
	// tailing the store (see Follow), so every process — including this one —
	// observes the same events in the same order. The after-publish hooks and
	// observability below still fire so monitoring sees the attempt.
	deliver := (persistErr == nil || !bus.strictPersistence) && !bus.logDelivery

	if deliver {
		dispatch(bus, ctx, eventType, eventTypeName, event)
	}
	if bus.store != nil && !bus.logDelivery && (persistErr == nil || deliver) {
		// A stored predecessor may upcast into a different concrete subscriber
		// type, whose shard the ordinary in-memory dispatch cannot know to wake.
		// Signal every resumable coordinator so it can scan the durable log and
		// apply the registry. Same-type wake-ups are intentionally idempotent:
		// ordinary dispatch may have skipped its marker when an earlier handler
		// cancelled ctx. Best-effort delivery also wakes after an ambiguous
		// Append error, matching its existing same-type behavior; strict failures
		// remain suppressed and can be recovered by a later Follow/replay.
		signalReplayMarkers(bus, ctx, eventTypeName, event)
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

	return persistErr
}

// signalReplayMarkers wakes log-backed resumable subscriptions whose concrete
// shard was not reached by the ordinary publish dispatch. Markers are tracked
// separately from shards so this path is O(active replay subscriptions), not a
// scan of every handler shard. The snapshot lock is released before any
// coordinator runs, preserving Clear's shard -> replay registry ordering.
func signalReplayMarkers[T any](bus *EventBus, ctx context.Context, eventTypeName string, event T) {
	if bus.replayMarkerCount.Load() == 0 {
		return
	}
	bus.replayMu.Lock()
	markers := make([]*internalHandler, 0, len(bus.replayMarkers))
	for marker := range bus.replayMarkers {
		markers = append(markers, marker)
	}
	bus.replayMu.Unlock()

	for _, marker := range markers {
		_ = callHandlerWithContext(marker, ctx, event, bus.panicHandler,
			bus.observability, eventTypeName, false)
	}
}

// dispatch delivers an event to the handlers registered for eventType. It is
// the delivery half of the publish path, shared by publishContext (live
// publishes) and the follower (events read back from a shared store): all
// subscription options — filters, Once, Async, Sequential — behave
// identically on both paths. It does not run the publish hooks or
// publish-level observability; those belong to the publish, not to delivery.
func dispatch[T any](bus *EventBus, ctx context.Context, eventType reflect.Type, eventTypeName string, event T) {
	_ = dispatchWithMode(bus, ctx, eventType, eventTypeName, event, dispatchNonBlocking)
}

// dispatchMode controls whether delivery returns immediately after launching
// async handlers or waits for the async work belonging to this event. Local
// Publish uses the non-blocking mode; durable Follow uses the waiting mode so
// its checkpoint cannot overtake handler completion.
type dispatchMode uint8

const (
	dispatchNonBlocking dispatchMode = iota
	dispatchWaitForAsync
)

func dispatchWithMode[T any](bus *EventBus, ctx context.Context, eventType reflect.Type, eventTypeName string, event T, mode dispatchMode) error {
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
	var durableOnceAttempts []*durableOnceAttempt
	var handlerErrors []error
	var cancellationErr error
	var asyncDone chan error
	if mode == dispatchWaitForAsync {
		asyncDone = make(chan error, len(handlersCopy))
	}
	asyncCount := 0

handlerLoop:
	for _, h := range handlersCopy {
		// Check context cancellation before doing anything with the handler.
		// This must happen before the Once CAS so a cancelled publish never
		// consumes a once-handler without executing it.
		if err := ctx.Err(); err != nil {
			cancellationErr = err
			break
		}

		// Check filter if present. The predicate type is validated at
		// Subscribe time; if the assertion still fails, fail closed.
		if h.filter != nil {
			matches, err := callFilter(h, event, bus.panicHandler)
			if err != nil {
				handlerErrors = append(handlerErrors, err)
				continue
			}
			if !matches {
				continue // Skip this handler as event doesn't match filter
			}
		}

		// For Once handlers, use CompareAndSwap to ensure atomic execution. A
		// durable dispatch additionally publishes its in-flight state under a
		// small per-handler lock: an immediate follower restart must fail/retry,
		// not skip and checkpoint past an async invocation still unwinding from a
		// cancelled attempt.
		var onceAttempt *durableOnceAttempt
		if h.once {
			if mode == dispatchWaitForAsync {
				h.onceStateMu.Lock()
				claimed := atomic.CompareAndSwapUint32(&h.executed, 0, 1)
				if claimed {
					atomic.StoreUint32(&h.onceInFlight, 1)
					onceAttempt = &durableOnceAttempt{handler: h}
				}
				inFlight := atomic.LoadUint32(&h.onceInFlight) != 0
				h.onceStateMu.Unlock()
				if !claimed {
					if inFlight {
						handlerErrors = append(handlerErrors,
							fmt.Errorf("eventbus: Once handler %v is still completing a previous durable attempt", h.handlerType))
						break handlerLoop
					}
					continue // Already completed and awaiting/removing its registration.
				}
			} else if !atomic.CompareAndSwapUint32(&h.executed, 0, 1) {
				continue // Already executed.
			}
			// Mark for removal after execution
			onceHandlersToRemove = append(onceHandlersToRemove, h)
		}

		// Bounded async capacity remains cancellable. A Once handler is selected
		// before waiting so concurrent publishes can fail its CAS immediately;
		// if cancellation wins before the goroutine starts, roll that selection
		// back so a later durable retry can still execute it.
		asyncSlot := false
		if h.async && bus.asyncSem != nil {
			select {
			case bus.asyncSem <- struct{}{}:
				asyncSlot = true
			case <-ctx.Done():
				if h.once {
					if onceAttempt != nil {
						onceAttempt.requestRollback()
						onceAttempt.complete()
					}
					atomic.StoreUint32(&h.executed, 0)
					onceHandlersToRemove = onceHandlersToRemove[:len(onceHandlersToRemove)-1]
				}
				cancellationErr = ctx.Err()
				break handlerLoop
			}
		}
		if onceAttempt != nil {
			durableOnceAttempts = append(durableOnceAttempts, onceAttempt)
		}

		if h.async {
			if mode == dispatchWaitForAsync {
				asyncCount++
			}
			bus.asyncStarted()
			go func(handler *internalHandler, attempt *durableOnceAttempt, releaseSlot bool, completion chan<- error, waitForCompletion bool) {
				var handlerErr error
				defer func() {
					if attempt != nil {
						attempt.complete()
					}
					if releaseSlot {
						<-bus.asyncSem
					}
					bus.asyncFinished()
					if waitForCompletion {
						// Buffered for every snapshotted handler: cancellation may make
						// dispatch return before this handler, but completion never leaks
						// a goroutine waiting for a receiver.
						completion <- handlerErr
					}
				}()

				// Once handlers always run: their execution slot was already
				// consumed by the CAS above, so skipping here would silently
				// drop them forever. Other handlers re-check the context.
				if !handler.once && ctx.Err() != nil {
					handlerErr = ctx.Err()
					return
				}
				handlerErr = callHandlerWithContext(handler, ctx, event, bus.panicHandler, bus.observability, eventTypeName, true)
			}(h, onceAttempt, asyncSlot, asyncDone, mode == dispatchWaitForAsync)
		} else {
			deliveryCtx := ctx
			if mode == dispatchWaitForAsync && h.internalDelivery != nil {
				// Only infrastructure delivery needs to know that its outer durable
				// caller must wait. Keeping this marker off ordinary handler contexts
				// avoids an allocation on every non-durable publish.
				deliveryCtx = context.WithValue(ctx, durableDispatchCtxKey{}, true)
			}
			if err := callHandlerWithContext(h, deliveryCtx, event, bus.panicHandler, bus.observability, eventTypeName, false); err != nil {
				handlerErrors = append(handlerErrors, err)
			}
			if onceAttempt != nil {
				onceAttempt.complete()
			}
		}
	}

	if cancellationErr != nil {
		if mode == dispatchWaitForAsync {
			// The follower cannot know whether every selected handler completed
			// before cancellation. Keep them retryable; at-least-once duplication
			// is safer than silently checkpointing a failed one on a later run.
			rollbackDurableOnceAttempts(durableOnceAttempts)
		} else {
			removeOnceHandlers(shard, eventType, onceHandlersToRemove)
		}
		return errors.Join(append(handlerErrors, cancellationErr)...)
	}

	if mode == dispatchWaitForAsync {
		for asyncCount > 0 {
			select {
			case err := <-asyncDone:
				asyncCount--
				if err != nil {
					handlerErrors = append(handlerErrors, err)
				}
			case <-ctx.Done():
				rollbackDurableOnceAttempts(durableOnceAttempts)
				return errors.Join(append(handlerErrors, ctx.Err())...)
			}
		}
		// A synchronous handler may cancel the delivery context itself, and an
		// async completion can win the final select at the same instant as
		// cancellation. In either case Follow will refuse to checkpoint this
		// attempt, so keep every selected Once handler eligible for its retry.
		if err := ctx.Err(); err != nil {
			rollbackDurableOnceAttempts(durableOnceAttempts)
			return errors.Join(append(handlerErrors, err)...)
		}
	}

	if len(handlerErrors) == 0 {
		commitDurableOnceAttempts(durableOnceAttempts)
		removeOnceHandlers(shard, eventType, onceHandlersToRemove)
		return nil
	}

	deliveryErr := errors.Join(handlerErrors...)
	if deliveryErr != nil && mode == dispatchWaitForAsync {
		// A durable event is retried as one unit. Keep every Once handler selected
		// for this failed attempt registered and make it eligible again, including
		// handlers that succeeded before a sibling failed.
		rollbackDurableOnceAttempts(durableOnceAttempts)
		return deliveryErr
	}

	commitDurableOnceAttempts(durableOnceAttempts)
	removeOnceHandlers(shard, eventType, onceHandlersToRemove)
	return deliveryErr
}

func rollbackDurableOnceAttempts(attempts []*durableOnceAttempt) {
	for _, attempt := range attempts {
		attempt.requestRollback()
	}
}

func commitDurableOnceAttempts(attempts []*durableOnceAttempt) {
	for _, attempt := range attempts {
		// A successful durable dispatch has waited for every async completion,
		// so complete has already cleared onceInFlight. Keeping executed at one
		// prevents a snapshotted concurrent dispatch from running the handler
		// again before removeOnceHandlers takes the shard lock.
		attempt.handler.onceStateMu.Lock()
		atomic.StoreUint32(&attempt.handler.onceInFlight, 0)
		attempt.handler.onceStateMu.Unlock()
	}
}

func removeOnceHandlers(shard *shard, eventType reflect.Type, onceHandlers []*internalHandler) {
	if len(onceHandlers) == 0 {
		return
	}
	shard.mu.Lock()
	handlers := shard.handlers[eventType]
	for _, onceHandler := range onceHandlers {
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

// Clear removes all handlers for events of type T
func Clear[T any](bus *EventBus) {
	eventType := reflect.TypeOf((*T)(nil)).Elem()

	shard := bus.getShard(eventType)
	shard.mu.Lock()
	for _, handler := range shard.handlers[eventType] {
		if handler.onRemove != nil {
			handler.onRemove()
		}
	}
	delete(shard.handlers, eventType)
	shard.mu.Unlock()
}

// ClearAll removes all handlers for all event types
func ClearAll(bus *EventBus) {
	for i := 0; i < numShards; i++ {
		bus.shards[i].mu.Lock()
		for _, handlers := range bus.shards[i].handlers {
			for _, handler := range handlers {
				if handler.onRemove != nil {
					handler.onRemove()
				}
			}
		}
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

// Wait blocks until all async handlers and deferred resumable-subscription
// drains complete. Ordinary synchronous handlers still complete inline in
// their Publish call and are not independently tracked.
// It is safe to call concurrently with Publish.
func (bus *EventBus) Wait() {
	bus.asyncMu.Lock()
	for bus.asyncCount > 0 {
		bus.asyncCond.Wait()
	}
	bus.asyncMu.Unlock()
}

// callHandlerWithContext calls a handler with proper type checking and panic recovery
func callHandlerWithContext[T any](h *internalHandler, ctx context.Context, event T, panicHandler PanicHandler, obs Observability, eventTypeName string, async bool) (panicErr error) {
	if h.suppressObservability {
		obs = nil
	}
	start := time.Now()

	defer func() {
		duration := time.Since(start)
		if r := recover(); r != nil {
			panicErr = &handlerPanicError{value: r}
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
	if h.internalDelivery != nil {
		return h.internalDelivery(ctx, event)
	}

	// Keep the common concrete-T path monomorphized and allocation-free. The
	// bound adapter is the correctness fallback when Publish's static T is any
	// or another interface while routing selected a concrete handler shard.
	switch fn := h.handler.(type) {
	case Handler[T]:
		fn(event)
	case ContextHandler[T]:
		fn(ctx, event)
	case nil:
		// Infrastructure handlers returned through internalDelivery above.
	default:
		if h.invoke != nil {
			h.invoke(ctx, event)
		}
	}
	return nil
}

// callFilter keeps user predicate panics inside the same isolation contract as
// handler panics while deliberately remaining outside handler observability: a
// rejected event never starts the handler, and neither does a predicate that
// fails before that boundary.
func callFilter[T any](h *internalHandler, event T, panicHandler PanicHandler) (matches bool, panicErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicErr = &handlerPanicError{value: recovered}
			if panicHandler != nil {
				panicHandler(event, h.handlerType, recovered)
			}
		}
	}()
	if filter, ok := h.filter.(func(T) bool); ok {
		return filter(event), nil
	}
	if h.filterInvoke != nil {
		return h.filterInvoke(event), nil
	}
	return false, nil
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

// WithFilter configures the handler to only receive events that match the
// predicate. The predicate runs before handler observability. Its panics are
// isolated like handler panics and sent to PanicHandler; local/non-durable
// delivery continues, while durable Follow/replay retries without checkpointing.
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

// WithStrictPersistence makes persistence a delivery precondition: when a
// publish reports a persistence error (marshal or Append failure), it is NOT
// delivered to handlers immediately. Without this option persistence is
// best-effort — handlers can observe an event the log never recorded, so a
// later replay would diverge from what live handlers saw. Append errors can be
// ambiguous, so a record committed before an acknowledgement failure may
// still appear in a later Replay, Follow, or resumable-subscription delivery.
// Enable this option when the store is the source of truth (event sourcing,
// cross-process delivery).
//
// The failure is still reported to the PersistenceErrorHandler, and
// TryPublish/TryPublishContext return it to the publisher. It has no effect
// on a bus without a store.
func WithStrictPersistence() Option {
	return func(bus *EventBus) {
		bus.strictPersistence = true
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

// Shutdown gracefully shuts down the event bus, waiting for async handlers and
// deferred resumable-subscription drains to complete.
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
