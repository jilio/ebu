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

// PersistentEventBus wraps EventBus with persistence
type PersistentEventBus struct {
	*EventBus
	store    EventStore
	position int64
	mu       sync.RWMutex
}

// NewPersistent creates a new EventBus with persistence
func NewPersistent(store EventStore) *PersistentEventBus {
	bus := New()
	peb := &PersistentEventBus{
		EventBus: bus,
		store:    store,
	}
	
	// Load current position
	ctx := context.Background()
	if pos, err := store.GetPosition(ctx); err == nil {
		peb.position = pos
	}
	
	// Hook into publish events to persist them
	bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
		peb.persistEvent(eventType, event)
	})
	
	return peb
}

// persistEvent saves an event to storage
func (peb *PersistentEventBus) persistEvent(eventType reflect.Type, event any) {
	peb.mu.Lock()
	peb.position++
	position := peb.position
	peb.mu.Unlock()
	
	data, err := json.Marshal(event)
	if err != nil {
		return // Silent fail for now, could add error handler
	}
	
	stored := &StoredEvent{
		Position:  position,
		Type:      eventType.String(),
		Data:      data,
		Timestamp: time.Now(),
	}
	
	ctx := context.Background()
	peb.store.Save(ctx, stored)
}

// Replay replays events from a position
func (peb *PersistentEventBus) Replay(ctx context.Context, from int64, handler func(*StoredEvent) error) error {
	peb.mu.RLock()
	to := peb.position
	peb.mu.RUnlock()
	
	events, err := peb.store.Load(ctx, from, to)
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

// SubscribeWithReplay subscribes and replays missed events
func SubscribeWithReplay[T any](
	peb *PersistentEventBus,
	subscriptionID string,
	handler Handler[T],
	opts ...SubscribeOption,
) error {
	ctx := context.Background()
	
	// Load last position for this subscription
	lastPos, _ := peb.store.LoadSubscriptionPosition(ctx, subscriptionID)
	
	// Replay missed events
	var eventType = reflect.TypeOf((*T)(nil)).Elem()
	err := peb.Replay(ctx, lastPos+1, func(stored *StoredEvent) error {
		// Only replay events of the correct type
		if stored.Type != eventType.String() {
			return nil
		}
		
		var event T
		if err := json.Unmarshal(stored.Data, &event); err != nil {
			return err
		}
		
		handler(event)
		
		// Update position
		peb.store.SaveSubscriptionPosition(ctx, subscriptionID, stored.Position)
		return nil
	})
	
	if err != nil {
		return fmt.Errorf("replay events: %w", err)
	}
	
	// Subscribe for future events with position tracking
	wrappedHandler := func(event T) {
		handler(event)
		
		// Update position after handling
		peb.mu.RLock()
		pos := peb.position
		peb.mu.RUnlock()
		
		peb.store.SaveSubscriptionPosition(ctx, subscriptionID, pos)
	}
	
	return Subscribe(peb.EventBus, wrappedHandler, opts...)
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
	
	m.events = append(m.events, event)
	return nil
}

// Load implements EventStore
func (m *MemoryStore) Load(ctx context.Context, from, to int64) ([]*StoredEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	var result []*StoredEvent
	for _, event := range m.events {
		if event.Position >= from && event.Position <= to {
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