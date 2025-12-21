package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"reflect"
	"strconv"
	"sync"
	"time"
)

// Offset represents an opaque position in an event stream.
// Implementations define the format (e.g., "123", "abc_456", timestamp-based).
// Offsets are lexicographically comparable within the same store.
type Offset string

const (
	// OffsetOldest represents the beginning of the stream.
	// When passed to Read, returns events from the start.
	OffsetOldest Offset = ""

	// OffsetNewest represents the current end of the stream.
	// Useful for subscribing to only new events.
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

// EventStoreSubscriber is an optional interface for stores that support live subscriptions.
// This enables real-time event streaming for remote stores.
type EventStoreSubscriber interface {
	// Subscribe starts a live subscription from the given offset.
	// Returns a channel that yields events as they arrive.
	// Call the returned cancel function to stop the subscription and close the channel.
	Subscribe(ctx context.Context, from Offset) (<-chan *StoredEvent, func(), error)
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

// WithStore enables persistence with the given store
func WithStore(store EventStore) Option {
	return func(bus *EventBus) {
		bus.store = store

		// Chain the persistence hook with any existing context-aware hook
		existingHook := bus.beforePublishCtx
		bus.beforePublishCtx = func(ctx context.Context, eventType reflect.Type, event any) {
			// Call existing hook first if any
			if existingHook != nil {
				existingHook(ctx, eventType, event)
			}
			// Then persist the event
			bus.persistEvent(ctx, eventType, event)
		}
	}
}

// WithSubscriptionStore enables subscription position tracking
func WithSubscriptionStore(store SubscriptionStore) Option {
	return func(bus *EventBus) {
		bus.subscriptionStore = store
	}
}

// persistEvent saves an event to storage (only if store is configured)
func (bus *EventBus) persistEvent(ctx context.Context, eventType reflect.Type, event any) {
	if bus.store == nil {
		return // No persistence configured
	}

	// Marshal the event first
	data, err := json.Marshal(event)
	if err != nil {
		if bus.persistenceErrorHandler != nil {
			bus.persistenceErrorHandler(event, eventType, fmt.Errorf("failed to marshal event: %w", err))
		}
		return
	}

	// Use EventType() to respect TypeNamer interface if implemented
	typeName := EventType(event)

	toStore := &Event{
		Type:      typeName,
		Data:      data,
		Timestamp: time.Now(),
	}

	// Apply timeout if configured
	var cancel context.CancelFunc
	if bus.persistenceTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, bus.persistenceTimeout)
		defer cancel()
	}

	// Observability: Track persistence start
	if bus.observability != nil {
		ctx = bus.observability.OnPersistStart(ctx, typeName, 0)
	}

	start := time.Now()

	// Append the event - the store assigns the offset
	bus.storeMu.Lock()
	offset, saveErr := bus.store.Append(ctx, toStore)
	if saveErr == nil {
		bus.lastOffset = offset
	}
	bus.storeMu.Unlock()

	// Observability: Track persistence complete
	if bus.observability != nil {
		bus.observability.OnPersistComplete(ctx, time.Since(start), saveErr)
	}

	if saveErr != nil {
		if bus.persistenceErrorHandler != nil {
			bus.persistenceErrorHandler(event, eventType, fmt.Errorf("failed to save event: %w", saveErr))
		}
	}
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
	offset := from
	for {
		events, nextOffset, err := bus.store.Read(ctx, offset, 100) // Read in batches of 100
		if err != nil {
			return fmt.Errorf("read events: %w", err)
		}

		if len(events) == 0 {
			break
		}

		for _, event := range events {
			if err := handler(event); err != nil {
				return fmt.Errorf("handle event at offset %s: %w", event.Offset, err)
			}
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

// SubscribeWithReplay subscribes and replays missed events.
// Requires both an EventStore (for replay) and a SubscriptionStore (for tracking).
// If the store implements SubscriptionStore, it will be used automatically.
func SubscribeWithReplay[T any](
	bus *EventBus,
	subscriptionID string,
	handler Handler[T],
	opts ...SubscribeOption,
) error {
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

	ctx := context.Background()

	// Load last offset for this subscription
	lastOffset, _ := subStore.LoadOffset(ctx, subscriptionID)

	// Replay missed events
	var eventType = reflect.TypeOf((*T)(nil)).Elem()
	// Use consistent type naming with EventType() function
	typeName := eventType.String()
	err := bus.Replay(ctx, lastOffset, func(stored *StoredEvent) error {
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
			return err
		}

		handler(event)

		// Update offset
		subStore.SaveOffset(ctx, subscriptionID, stored.Offset)
		return nil
	})

	if err != nil {
		return fmt.Errorf("replay events: %w", err)
	}

	// Subscribe for future events with offset tracking
	wrappedHandler := func(event T) {
		handler(event)

		// Update offset after handling
		bus.storeMu.RLock()
		offset := bus.lastOffset
		bus.storeMu.RUnlock()

		subStore.SaveOffset(ctx, subscriptionID, offset)
	}

	return Subscribe(bus, wrappedHandler, opts...)
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
	offset := Offset(strconv.FormatInt(m.nextOffset, 10))

	stored := &StoredEvent{
		Offset:    offset,
		Type:      event.Type,
		Data:      event.Data,
		Timestamp: event.Timestamp,
	}
	m.events = append(m.events, stored)
	return offset, nil
}

// Read implements EventStore
func (m *MemoryStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*StoredEvent
	var lastOffset Offset = from

	for _, event := range m.events {
		// Include events after the given offset
		if from == OffsetOldest || event.Offset > from {
			result = append(result, event)
			lastOffset = event.Offset
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}

	return result, lastOffset, nil
}

// SaveOffset implements SubscriptionStore
func (m *MemoryStore) SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error {
	m.mu.Lock()
	defer m.mu.Unlock()

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
		// Take a filtered snapshot to avoid holding lock during iteration
		m.mu.RLock()
		var events []*StoredEvent
		for _, event := range m.events {
			if from == OffsetOldest || event.Offset > from {
				events = append(events, event)
			}
		}
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
