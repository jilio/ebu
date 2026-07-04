package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"reflect"
	"sort"
	"sync"
	"time"
)

// Offset represents an opaque position in an event stream.
// Implementations define the format (e.g., "123", "abc_456", timestamp-based).
//
// Offsets are resumption tokens: the only operations the bus performs on
// them are equality checks and passing them back to the store that issued
// them. Ordering semantics are store-defined — the bundled MemoryStore and
// the sqlite store produce lexicographically ordered offsets, but stores
// backed by external systems (e.g. durablestream) may not. Treat offsets
// from one store as meaningless to any other store.
type Offset string

const (
	// OffsetOldest represents the beginning of the stream.
	// When passed to Read, returns events from the start.
	OffsetOldest Offset = ""

	// OffsetNewest represents the current end of the stream.
	// Useful for subscribing to only new events.
	//
	// Contract (all bundled stores): OffsetNewest resolves, at call time, to
	// the current tail. Read/ReadStream from it return no historical events;
	// Read returns a concrete resumable offset (never "$" itself) as
	// nextOffset, so Replay(ctx, OffsetNewest, ...) terminates immediately
	// and the position it reached can be saved and resumed. SaveOffset
	// resolves it to the concrete tail before persisting.
	OffsetNewest Offset = "$"
)

// EventStore defines the core interface for persisting events.
// This is a minimal interface with just 2 methods for basic event storage.
// Additional capabilities are provided through optional interfaces.
type EventStore interface {
	// Append stores an event and returns its assigned offset.
	// The store is responsible for generating unique, monotonically increasing offsets.
	Append(ctx context.Context, event *Event) (Offset, error)

	// Read returns events starting after the given offset.
	// Use OffsetOldest to read from the beginning.
	// The limit parameter controls max events returned (0 = no limit).
	// Returns the events, the offset to use for the next read, and any error.
	Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error)
}

// EventStoreStreamer is an optional interface for memory-efficient streaming.
// When implemented, the Replay method will automatically use streaming.
//
// Implementation notes:
//   - Database-backed stores should use cursor-based iteration to minimize memory
//   - In-memory stores may need to take a snapshot to avoid holding locks during iteration,
//     trading memory for deadlock safety (see MemoryStore.ReadStream for an example)
type EventStoreStreamer interface {
	// ReadStream returns an iterator yielding events starting after the given offset.
	// Use OffsetOldest to read from the beginning.
	// The iterator checks ctx.Done() before each yield and returns ctx.Err() when cancelled.
	// A yielded error terminates iteration.
	ReadStream(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error]
}

// SubscriptionStore tracks subscription progress separately from event storage.
// This interface is optional and enables resumable subscriptions.
type SubscriptionStore interface {
	// SaveOffset persists the current offset for a subscription.
	SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error

	// LoadOffset retrieves the last saved offset for a subscription.
	// Returns OffsetOldest if the subscription has no saved offset.
	LoadOffset(ctx context.Context, subscriptionID string) (Offset, error)
}

// EventStoreSnapshotter is an optional interface for stores that can persist a
// materialized projection snapshot, keyed by an id and tagged with the Offset the
// snapshot reflects. It lets a caller compact a high-churn log: save the reduced
// state, then (with EventStoreTruncator) drop the events the snapshot subsumes,
// bounding cold-start replay. The blob is opaque to ebu — callers define its
// encoding. A store that does not implement this interface is simply not
// compactable; callers fall back to a full replay from OffsetOldest.
type EventStoreSnapshotter interface {
	// SaveSnapshot upserts the snapshot for snapshotID, recording that blob
	// reflects the projection state as of (and including) atOffset. atOffset MUST
	// be a real, resumable Offset previously returned by Append/Read for THIS
	// store (never OffsetNewest, never synthetic): a caller resumes with
	// Replay(ctx, atOffset, ...), which reads only events strictly after it.
	SaveSnapshot(ctx context.Context, snapshotID string, atOffset Offset, blob json.RawMessage) error

	// LoadSnapshot returns the last saved snapshot for snapshotID. When none
	// exists it returns (OffsetOldest, nil, nil) — "replay from the beginning",
	// never an error — mirroring LoadOffset's OffsetOldest default.
	LoadSnapshot(ctx context.Context, snapshotID string) (atOffset Offset, blob json.RawMessage, err error)
}

// EventStoreTruncator is an optional interface for log compaction: it deletes
// events at or before a given Offset. It is only safe to call once a snapshot
// covering that Offset is durably saved AND no live reader/subscription still
// needs the truncated prefix. Stores whose Offsets are not deletable positions
// (e.g. a remote append-only broker) MUST NOT implement this interface.
type EventStoreTruncator interface {
	// TruncateBefore deletes every event whose Offset <= beforeOffset and returns
	// the number deleted. beforeOffset == OffsetOldest is a no-op. Idempotent:
	// re-running with the same offset deletes nothing more.
	TruncateBefore(ctx context.Context, beforeOffset Offset) (deleted int64, err error)
}

// Event represents an event to be stored (before it has an offset).
type Event struct {
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp time.Time       `json:"timestamp"`
}

// StoredEvent represents an event that has been persisted with an offset.
type StoredEvent struct {
	Offset    Offset          `json:"offset"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp time.Time       `json:"timestamp"`
}

// WithStore enables persistence with the given store.
//
// Events are persisted in the publish path, after the before-publish hooks
// and before handlers run. Persistence is best-effort: a failed Append does
// not prevent delivery to handlers; the error is reported to the
// PersistenceErrorHandler instead.
func WithStore(store EventStore) Option {
	return func(bus *EventBus) {
		bus.store = store
	}
}

// offsetCtxKey is the context key under which the offset assigned to the
// event being published is stored.
type offsetCtxKey struct{}

// OffsetFromContext returns the offset assigned to the event currently being
// handled, if the bus persisted it successfully. It only returns an offset
// inside handlers invoked by a bus configured with WithStore.
func OffsetFromContext(ctx context.Context) (Offset, bool) {
	offset, ok := ctx.Value(offsetCtxKey{}).(Offset)
	return offset, ok
}

// WithSubscriptionStore enables subscription position tracking
func WithSubscriptionStore(store SubscriptionStore) Option {
	return func(bus *EventBus) {
		bus.subscriptionStore = store
	}
}

// persistEvent saves an event to storage and returns a context that carries
// the assigned offset on success (see OffsetFromContext). On failure it
// reports to the PersistenceErrorHandler and returns the context unchanged.
//
// Concurrency note: the bus does not serialize Append calls; stores must be
// safe for concurrent use (all bundled stores are).
func (bus *EventBus) persistEvent(ctx context.Context, eventType reflect.Type, event any) context.Context {
	// Marshal the event first
	data, err := json.Marshal(event)
	if err != nil {
		if bus.persistenceErrorHandler != nil {
			bus.persistenceErrorHandler(event, eventType, fmt.Errorf("failed to marshal event: %w", err))
		}
		return ctx
	}

	// Use EventType() to respect TypeNamer interface if implemented
	typeName := EventType(event)

	toStore := &Event{
		Type:      typeName,
		Data:      data,
		Timestamp: time.Now(),
	}

	// Apply timeout if configured. The timeout applies to Append only; the
	// original context is what gets returned to the publish path.
	appendCtx := ctx
	if bus.persistenceTimeout > 0 {
		var cancel context.CancelFunc
		appendCtx, cancel = context.WithTimeout(ctx, bus.persistenceTimeout)
		defer cancel()
	}

	// Observability: Track persistence start
	if bus.observability != nil {
		appendCtx = bus.observability.OnPersistStart(appendCtx, typeName)
	}

	start := time.Now()

	// Append the event - the store assigns the offset
	offset, saveErr := bus.store.Append(appendCtx, toStore)

	// Observability: Track persistence complete
	if bus.observability != nil {
		bus.observability.OnPersistComplete(appendCtx, typeName, time.Since(start), offset, saveErr)
	}

	if saveErr != nil {
		if bus.persistenceErrorHandler != nil {
			bus.persistenceErrorHandler(event, eventType, fmt.Errorf("failed to save event: %w", saveErr))
		}
		return ctx
	}

	return context.WithValue(ctx, offsetCtxKey{}, offset)
}

// Replay replays events from an offset
func (bus *EventBus) Replay(ctx context.Context, from Offset, handler func(*StoredEvent) error) error {
	if bus.store == nil {
		return fmt.Errorf("replay requires persistence (use WithStore option)")
	}

	// Use streaming if available for memory efficiency
	if streamer, ok := bus.store.(EventStoreStreamer); ok {
		for event, err := range streamer.ReadStream(ctx, from) {
			if err != nil {
				return fmt.Errorf("stream events: %w", err)
			}
			if err := handler(event); err != nil {
				return fmt.Errorf("handle event at offset %s: %w", event.Offset, err)
			}
		}
		return nil
	}

	// Fallback to Read for stores that don't support streaming
	batchSize := bus.replayBatchSize
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}

	offset := from
	for {
		// Check context cancellation before each batch
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		events, nextOffset, err := bus.store.Read(ctx, offset, batchSize)
		if err != nil {
			return fmt.Errorf("read events: %w", err)
		}

		if len(events) == 0 {
			// Zero events does not mean the tail was reached: a store may
			// advance the offset past a stretch it cannot deliver (e.g. a
			// remote chunk whose events were all skipped as undecodable).
			// Only a non-advancing offset marks the end of the stream.
			if nextOffset == offset {
				break
			}
			offset = nextOffset
			continue
		}

		for _, event := range events {
			if err := handler(event); err != nil {
				return fmt.Errorf("handle event at offset %s: %w", event.Offset, err)
			}
		}

		// Protect against infinite loop if offset doesn't advance
		if nextOffset == offset {
			return fmt.Errorf("store returned non-advancing offset %s: possible bug in EventStore.Read implementation", offset)
		}
		offset = nextOffset
	}

	return nil
}

// ReplayWithUpcast replays events from an offset, applying upcasts before passing to handler
func (bus *EventBus) ReplayWithUpcast(ctx context.Context, from Offset, handler func(*StoredEvent) error) error {
	return bus.Replay(ctx, from, func(event *StoredEvent) error {
		// Apply upcasts if available
		if bus.upcastRegistry != nil {
			upcastedData, upcastedType, err := bus.upcastRegistry.apply(event.Data, event.Type)
			if err == nil {
				// Create a new StoredEvent with upcasted data
				upcastedEvent := &StoredEvent{
					Offset:    event.Offset,
					Type:      upcastedType,
					Data:      upcastedData,
					Timestamp: event.Timestamp,
				}
				return handler(upcastedEvent)
			}
			// If upcast fails, pass original event
		}
		return handler(event)
	})
}

// IsPersistent returns true if persistence is enabled
func (bus *EventBus) IsPersistent() bool {
	return bus.store != nil
}

// GetStore returns the event store (or nil if not persistent)
func (bus *EventBus) GetStore() EventStore {
	return bus.store
}

// typeNamerType is the reflect.Type of the TypeNamer interface.
var typeNamerType = reflect.TypeOf((*TypeNamer)(nil)).Elem()

// typeNameOf returns the persisted type name for an event type, honoring the
// TypeNamer interface the same way EventType does for event values.
func typeNameOf(t reflect.Type) string {
	if t.Implements(typeNamerType) {
		return reflect.Zero(t).Interface().(TypeNamer).EventTypeName()
	}
	if reflect.PointerTo(t).Implements(typeNamerType) {
		// EventTypeName has a pointer receiver; call it on a fresh instance
		// rather than a nil pointer.
		return reflect.New(t).Interface().(TypeNamer).EventTypeName()
	}
	return t.String()
}

// ReplayErrorPolicy determines how SubscribeWithReplay handles a stored
// event that cannot be decoded into the subscription's event type.
type ReplayErrorPolicy int

const (
	// ReplayAbort stops the replay and returns the decode error (default).
	// The subscription's saved offset does not advance past the failing
	// event, so the next SubscribeWithReplay hits it again. Use this when a
	// decode failure means a bug that must be fixed before proceeding.
	ReplayAbort ReplayErrorPolicy = iota

	// ReplaySkip reports the decode error to the PersistenceErrorHandler
	// (with the *StoredEvent as the event argument) and continues with the
	// next event. The skip is durable: the poison event's offset is saved,
	// so it is not re-scanned and re-reported on later restarts — recover
	// its payload from the reported *StoredEvent if needed.
	//
	// Scope: the policy fires only when a stored event OF THE SUBSCRIBED
	// TYPE fails to decode (malformed or type-incompatible JSON). It does
	// not cover events stored under a different type name — those are
	// always skipped silently, by design, since streams may carry many
	// event types — nor JSON that decodes leniently despite schema drift
	// (unknown fields are dropped, missing fields zero-filled; use upcasts
	// for schema evolution).
	ReplaySkip
)

// WithReplayErrorPolicy sets how SubscribeWithReplay treats stored events
// that fail to decode. It has no effect on Subscribe/SubscribeContext live
// delivery. The default is ReplayAbort.
func WithReplayErrorPolicy(policy ReplayErrorPolicy) SubscribeOption {
	return func(h *internalHandler) {
		h.replayErrorPolicy = policy
	}
}

// SubscribeWithReplay subscribes and replays missed events.
// Requires both an EventStore (for replay) and a SubscriptionStore (for tracking).
// If the store implements SubscriptionStore, it will be used automatically.
//
// Context usage:
//   - The context is used for the replay phase (loading historical events)
//   - Live-phase offset saves use the per-publish context of each event
//
// Offset tracking: after each handled event the subscription saves that
// event's own offset (replay phase) or the offset assigned during publish
// (live phase, via OffsetFromContext). Delivery is at-least-once: after a
// crash between handling and offset save, the event is redelivered on the
// next SubscribeWithReplay.
//
// Handoff: after the live subscription registers, one catch-up replay pass
// runs from the last replayed position, so events published between the end
// of replay and subscription registration are not missed. Events published
// in that window may be delivered twice (once by the catch-up pass, once
// live) — handlers must be idempotent.
//
// SaveOffset failures do not stop delivery; they are reported to the
// PersistenceErrorHandler.
//
// Validation: all argument and option validation (nil bus/handler/options,
// interface event types, filter shape) happens before the replay pass, so a
// call that returns a validation error has delivered no events and saved no
// offsets.
//
// Filtering: a WithFilter predicate applies to the replay and catch-up
// passes exactly as it does to live delivery — the handler never sees
// non-matching events, and their offsets are not saved.
//
// Note: If the saved offset is OffsetOldest (""), replay starts from the beginning.
// The OffsetNewest ("$") constant is typically not stored and is only used for live subscriptions.
func SubscribeWithReplay[T any](
	ctx context.Context,
	bus *EventBus,
	subscriptionID string,
	handler Handler[T],
	opts ...SubscribeOption,
) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
	if handler == nil {
		return fmt.Errorf("eventbus: handler cannot be nil")
	}
	if bus.store == nil {
		return fmt.Errorf("SubscribeWithReplay requires persistence (use WithStore option)")
	}

	// Get subscription store - either explicit or from the event store
	subStore := bus.subscriptionStore
	if subStore == nil {
		if ss, ok := bus.store.(SubscriptionStore); ok {
			subStore = ss
		} else {
			return fmt.Errorf("SubscribeWithReplay requires a SubscriptionStore (use WithSubscriptionStore option or use a store that implements SubscriptionStore)")
		}
	}

	eventType := reflect.TypeOf((*T)(nil)).Elem()
	// Match the name events were persisted under (TypeNamer-aware).
	typeName := typeNameOf(eventType)

	saveOffset := func(saveCtx context.Context, event T, offset Offset) {
		if err := subStore.SaveOffset(saveCtx, subscriptionID, offset); err != nil && bus.persistenceErrorHandler != nil {
			bus.persistenceErrorHandler(event, eventType,
				fmt.Errorf("failed to save offset for subscription %q: %w", subscriptionID, err))
		}
	}

	// Build the live subscription up front: options are applied exactly once
	// and ALL subscription-time validation (event type, nil options, filter
	// shape) runs before the replay pass can deliver events or save offsets.
	// The handler is registered in the shard only after the first replay pass.
	var wrappedHandler ContextHandler[T] = func(hctx context.Context, event T) {
		handler(event)

		// The publish path attaches the persisted offset of the event being
		// delivered to its context, so each handled event saves its own offset.
		if offset, ok := OffsetFromContext(hctx); ok {
			saveOffset(hctx, event, offset)
		}
	}
	h, err := buildHandler(wrappedHandler, eventType, opts)
	if err != nil {
		return err
	}
	// The filter's shape was validated by buildHandler; honor it during the
	// replay passes the same way PublishContext honors it live.
	filter, _ := h.filter.(func(T) bool)

	// callReplayed delivers a replayed event under the same mutual-exclusion
	// guarantee live delivery provides: once the live subscription registers
	// (before the catch-up pass), a Sequential() handler can be entered by a
	// concurrent Publish, so the replay path must take the same lock.
	callReplayed := func(event T) {
		if h.sequential {
			h.mu.Lock()
			defer h.mu.Unlock()
		}
		handler(event)
	}

	// Load last offset for this subscription
	lastOffset, _ := subStore.LoadOffset(ctx, subscriptionID)

	// lastSeen tracks the stream position of the last event observed during
	// replay (regardless of type), so the catch-up pass below can resume
	// where the first pass left off.
	lastSeen := lastOffset

	replayFn := func(stored *StoredEvent) error {
		lastSeen = stored.Offset

		// Apply upcasts if available
		eventData, eventTypeName := stored.Data, stored.Type
		if bus.upcastRegistry != nil {
			upcastedData, upcastedType, err := bus.upcastRegistry.apply(eventData, eventTypeName)
			if err == nil {
				eventData = upcastedData
				eventTypeName = upcastedType
			}
			// Continue even if upcast fails, let the type check handle it
		}

		// Only replay events of the correct type
		if eventTypeName != typeName {
			return nil
		}

		var event T
		if err := json.Unmarshal(eventData, &event); err != nil {
			if h.replayErrorPolicy == ReplaySkip {
				// Poison event: report and move on rather than wedging the
				// subscription on it forever. The skip is durable — saving the
				// poison event's own offset stops it from being re-scanned and
				// re-reported on every restart. Its payload was just handed to
				// the error handler as a *StoredEvent for out-of-band recovery.
				if bus.persistenceErrorHandler != nil {
					bus.persistenceErrorHandler(stored, eventType,
						fmt.Errorf("skipping undecodable event at offset %s for subscription %q: %w", stored.Offset, subscriptionID, err))
				}
				var zero T
				saveOffset(ctx, zero, stored.Offset)
				return nil
			}
			return err
		}

		// Apply the subscription's filter, mirroring live delivery: the
		// handler never sees non-matching events and their offsets are not
		// saved (the next handled event advances past them, as it does live).
		if filter != nil && !filter(event) {
			return nil
		}

		callReplayed(event)

		// Save this event's own offset after successful handling
		saveOffset(ctx, event, stored.Offset)
		return nil
	}

	if err := bus.Replay(ctx, lastOffset, replayFn); err != nil {
		return fmt.Errorf("replay events: %w", err)
	}

	// Register the pre-built, pre-validated subscription for live events.
	// Options were already applied by buildHandler above — registering the
	// same handler instance keeps each option's effect applied exactly once.
	bus.addHandler(eventType, h)

	// Catch-up pass: an event persisted after the first replay finished but
	// before the live subscription registered would otherwise be missed
	// until the next SubscribeWithReplay. Persistence happens before live
	// delivery in the publish path, so replaying once more after the
	// subscription is registered closes the gap: every event is now seen by
	// this pass and/or the live subscription. An event published in the
	// overlap window may be delivered twice (at-least-once semantics — make
	// handlers idempotent).
	if err := bus.Replay(ctx, lastSeen, replayFn); err != nil {
		return fmt.Errorf("catch-up replay: %w", err)
	}

	return nil
}

// MemoryStore is a simple in-memory implementation of EventStore and SubscriptionStore.
type MemoryStore struct {
	events        []*StoredEvent
	subscriptions map[string]Offset
	nextOffset    int64
	mu            sync.RWMutex
}

// Ensure MemoryStore implements all required interfaces
var _ EventStore = (*MemoryStore)(nil)
var _ EventStoreStreamer = (*MemoryStore)(nil)
var _ SubscriptionStore = (*MemoryStore)(nil)

// NewMemoryStore creates a new in-memory event store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		events:        make([]*StoredEvent, 0),
		subscriptions: make(map[string]Offset),
	}
}

// Append implements EventStore
func (m *MemoryStore) Append(ctx context.Context, event *Event) (Offset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextOffset++
	// Use zero-padded format for correct lexicographic ordering
	offset := Offset(fmt.Sprintf("%020d", m.nextOffset))

	stored := &StoredEvent{
		Offset:    offset,
		Type:      event.Type,
		Data:      event.Data,
		Timestamp: event.Timestamp,
	}
	m.events = append(m.events, stored)
	return offset, nil
}

// searchAfter returns the index of the first event with offset > from.
// Events are stored in ascending, zero-padded offset order, so binary
// search applies. Caller must hold at least a read lock.
func (m *MemoryStore) searchAfter(from Offset) int {
	if from == OffsetOldest {
		return 0
	}
	if from == OffsetNewest {
		// "$" is not a stored offset ("$" sorts before the zero-padded
		// digits, so the binary search would wrongly return 0 = replay
		// everything); the tail means "after every current event".
		return len(m.events)
	}
	return sort.Search(len(m.events), func(i int) bool {
		return m.events[i].Offset > from
	})
}

// tailOffset returns the offset of the last stored event, or OffsetOldest
// when the store is empty. Caller must hold at least a read lock.
func (m *MemoryStore) tailOffset() Offset {
	if len(m.events) == 0 {
		return OffsetOldest
	}
	return m.events[len(m.events)-1].Offset
}

// Read implements EventStore
func (m *MemoryStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	start := m.searchAfter(from)
	end := len(m.events)
	if limit > 0 && start+limit < end {
		end = start + limit
	}

	if start == end {
		if from == OffsetNewest {
			// Resolve "$" to a concrete, resumable position: echoing the
			// symbolic offset back would make the caller chase a
			// perpetually moving tail.
			return nil, m.tailOffset(), nil
		}
		return nil, from, nil
	}

	result := make([]*StoredEvent, end-start)
	copy(result, m.events[start:end])
	return result, result[len(result)-1].Offset, nil
}

// SaveOffset implements SubscriptionStore.
// OffsetNewest is resolved to the current tail at save time, so the stored
// value is always a concrete, resumable position.
func (m *MemoryStore) SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if offset == OffsetNewest {
		offset = m.tailOffset()
	}
	m.subscriptions[subscriptionID] = offset
	return nil
}

// LoadOffset implements SubscriptionStore
func (m *MemoryStore) LoadOffset(ctx context.Context, subscriptionID string) (Offset, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	offset, ok := m.subscriptions[subscriptionID]
	if !ok {
		return OffsetOldest, nil
	}

	return offset, nil
}

// ReadStream implements EventStoreStreamer for memory-efficient event iteration.
// Note: This takes a filtered snapshot of matching events to avoid holding the lock
// during iteration, which could cause deadlocks if handlers call other store methods.
func (m *MemoryStore) ReadStream(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
	return func(yield func(*StoredEvent, error) bool) {
		// Take a snapshot to avoid holding lock during iteration
		m.mu.RLock()
		start := m.searchAfter(from)
		events := make([]*StoredEvent, len(m.events)-start)
		copy(events, m.events[start:])
		m.mu.RUnlock()

		for _, event := range events {
			// Check context cancellation
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
			}

			if !yield(event, nil) {
				return // Consumer stopped iteration
			}
		}
	}
}
