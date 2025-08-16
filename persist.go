package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// EventStore defines the interface for persisting events
type EventStore interface {
	// Save an event to storage
	Save(ctx context.Context, event *StoredEvent) error

	// Load events from storage within a range
	Load(ctx context.Context, from, to int64) ([]*StoredEvent, error)

	// Get the current position (highest event number)
	GetPosition(ctx context.Context) (int64, error)

	// Save subscription position for resumable subscriptions
	SaveSubscriptionPosition(ctx context.Context, subscriptionID string, position int64) error

	// Load subscription position
	LoadSubscriptionPosition(ctx context.Context, subscriptionID string) (int64, error)
}

// StoredEvent represents an event in storage
type StoredEvent struct {
	Position  int64           `json:"position"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp time.Time       `json:"timestamp"`
}

// WithEventStore enables persistence with the given store
func WithEventStore(store EventStore) Option {
	return func(bus *EventBus) {
		bus.store = store

		// Load current position
		ctx := context.Background()
		if pos, err := store.GetPosition(ctx); err == nil {
			bus.storePosition = pos
		}

		// Chain the persistence hook with any existing hook
		existingHook := bus.beforePublish
		bus.beforePublish = func(eventType reflect.Type, event any) {
			// Call existing hook first if any
			if existingHook != nil {
				existingHook(eventType, event)
			}
			// Then persist the event
			bus.persistEvent(eventType, event)
		}
	}
}

// persistEvent saves an event to storage (only if store is configured)
func (bus *EventBus) persistEvent(eventType reflect.Type, event any) {
	if bus.store == nil {
		return // No persistence configured
	}

	bus.storeMu.Lock()
	bus.storePosition++
	position := bus.storePosition
	bus.storeMu.Unlock()

	data, err := json.Marshal(event)
	if err != nil {
		// Silent fail for now, could add error handler option
		return
	}

	// Build full type name with package path
	typeName := eventType.String()
	if pkg := eventType.PkgPath(); pkg != "" {
		typeName = pkg + "/" + eventType.Name()
	}

	stored := &StoredEvent{
		Position:  position,
		Type:      typeName,
		Data:      data,
		Timestamp: time.Now(),
	}

	ctx := context.Background()
	bus.store.Save(ctx, stored)
}

// Replay replays events from a position
func (bus *EventBus) Replay(ctx context.Context, from int64, handler func(*StoredEvent) error) error {
	if bus.store == nil {
		return fmt.Errorf("replay requires persistence (use WithStore option)")
	}

	bus.storeMu.RLock()
	to := bus.storePosition
	bus.storeMu.RUnlock()

	events, err := bus.store.Load(ctx, from, to)
	if err != nil {
		return fmt.Errorf("load events: %w", err)
	}

	for _, event := range events {
		if err := handler(event); err != nil {
			return fmt.Errorf("handle event at position %d: %w", event.Position, err)
		}
	}

	return nil
}

// IsPersistent returns true if persistence is enabled
func (bus *EventBus) IsPersistent() bool {
	return bus.store != nil
}

// GetStore returns the event store (or nil if not persistent)
func (bus *EventBus) GetStore() EventStore {
	return bus.store
}

// SubscribeWithReplay subscribes and replays missed events
func SubscribeWithReplay[T any](
	bus *EventBus,
	subscriptionID string,
	handler Handler[T],
	opts ...SubscribeOption,
) error {
	if bus.store == nil {
		return fmt.Errorf("SubscribeWithReplay requires persistence (use WithStore option)")
	}

	ctx := context.Background()

	// Load last position for this subscription
	lastPos, _ := bus.store.LoadSubscriptionPosition(ctx, subscriptionID)

	// Replay missed events
	var eventType = reflect.TypeOf((*T)(nil)).Elem()
	// Build full type name with package path for comparison
	typeName := eventType.String()
	if pkg := eventType.PkgPath(); pkg != "" {
		typeName = pkg + "/" + eventType.Name()
	}
	err := bus.Replay(ctx, lastPos+1, func(stored *StoredEvent) error {
		// Only replay events of the correct type
		if stored.Type != typeName {
			return nil
		}

		var event T
		if err := json.Unmarshal(stored.Data, &event); err != nil {
			return err
		}

		handler(event)

		// Update position
		bus.store.SaveSubscriptionPosition(ctx, subscriptionID, stored.Position)
		return nil
	})

	if err != nil {
		return fmt.Errorf("replay events: %w", err)
	}

	// Subscribe for future events with position tracking
	wrappedHandler := func(event T) {
		handler(event)

		// Update position after handling
		bus.storeMu.RLock()
		pos := bus.storePosition
		bus.storeMu.RUnlock()

		bus.store.SaveSubscriptionPosition(ctx, subscriptionID, pos)
	}

	return Subscribe(bus, wrappedHandler, opts...)
}

// MemoryStore is a simple in-memory implementation of EventStore
type MemoryStore struct {
	events        []*StoredEvent
	subscriptions map[string]int64
	mu            sync.RWMutex
}

// NewMemoryStore creates a new in-memory event store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		events:        make([]*StoredEvent, 0),
		subscriptions: make(map[string]int64),
	}
}

// Save implements EventStore
func (m *MemoryStore) Save(ctx context.Context, event *StoredEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Set position for the event
	event.Position = int64(len(m.events)) + 1
	m.events = append(m.events, event)
	return nil
}

// Load implements EventStore
func (m *MemoryStore) Load(ctx context.Context, from, to int64) ([]*StoredEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*StoredEvent
	for _, event := range m.events {
		if event.Position >= from && (to == -1 || event.Position <= to) {
			result = append(result, event)
		}
	}

	return result, nil
}

// GetPosition implements EventStore
func (m *MemoryStore) GetPosition(ctx context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.events) == 0 {
		return 0, nil
	}

	return m.events[len(m.events)-1].Position, nil
}

// SaveSubscriptionPosition implements EventStore
func (m *MemoryStore) SaveSubscriptionPosition(ctx context.Context, subscriptionID string, position int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.subscriptions[subscriptionID] = position
	return nil
}

// LoadSubscriptionPosition implements EventStore
func (m *MemoryStore) LoadSubscriptionPosition(ctx context.Context, subscriptionID string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pos, ok := m.subscriptions[subscriptionID]
	if !ok {
		return 0, nil
	}

	return pos, nil
}
