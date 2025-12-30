package state

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	eventbus "github.com/jilio/ebu"
)

// Store is a generic interface for state storage.
// Implementations can use any backing store (memory, database, etc.).
type Store[T any] interface {
	// Get retrieves an entity by its composite key.
	Get(compositeKey string) (T, bool)
	// Set stores an entity with the given composite key.
	Set(compositeKey string, value T)
	// Delete removes an entity by its composite key.
	Delete(compositeKey string)
	// Clear removes all entities from the store.
	Clear()
	// All returns a copy of all entities in the store.
	All() map[string]T
}

// MemoryStore is an in-memory implementation of Store.
// It is safe for concurrent access.
type MemoryStore[T any] struct {
	data map[string]T
	mu   sync.RWMutex
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore[T any]() *MemoryStore[T] {
	return &MemoryStore[T]{
		data: make(map[string]T),
	}
}

// Get retrieves an entity by its composite key.
func (s *MemoryStore[T]) Get(key string) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Set stores an entity with the given composite key.
func (s *MemoryStore[T]) Set(key string, value T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Delete removes an entity by its composite key.
func (s *MemoryStore[T]) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Clear removes all entities from the store.
func (s *MemoryStore[T]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]T)
}

// All returns a copy of all entities in the store.
func (s *MemoryStore[T]) All() map[string]T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]T, len(s.data))
	for k, v := range s.data {
		result[k] = v
	}
	return result
}

// TypedCollection provides type-safe access to a collection of entities.
// It wraps a Store and associates it with a specific entity type.
type TypedCollection[T any] struct {
	store      Store[T]
	entityType string
}

// NewTypedCollection creates a collection for a specific entity type.
// The entity type name is determined from the type parameter T.
func NewTypedCollection[T any](store Store[T]) *TypedCollection[T] {
	var zero T
	return &TypedCollection[T]{
		store:      store,
		entityType: EntityType(zero),
	}
}

// NewTypedCollectionWithType creates a collection with an explicit type name.
// Use this when the type name should differ from the Go type name.
func NewTypedCollectionWithType[T any](store Store[T], entityType string) *TypedCollection[T] {
	return &TypedCollection[T]{
		store:      store,
		entityType: entityType,
	}
}

// EntityType returns the entity type name for this collection.
func (c *TypedCollection[T]) EntityType() string {
	return c.entityType
}

// Get retrieves an entity by its key (without the type prefix).
func (c *TypedCollection[T]) Get(key string) (T, bool) {
	return c.store.Get(CompositeKey(c.entityType, key))
}

// All returns all entities in this collection.
// The keys in the returned map are composite keys (type/key).
func (c *TypedCollection[T]) All() map[string]T {
	return c.store.All()
}

// collectionApplier is an internal interface for applying changes to collections.
type collectionApplier interface {
	applyChange(msg *ChangeMessage) error
	clear()
}

// typedCollectionApplier wraps TypedCollection to implement collectionApplier.
type typedCollectionApplier[T any] struct {
	collection *TypedCollection[T]
}

func (a *typedCollectionApplier[T]) applyChange(msg *ChangeMessage) error {
	key := CompositeKey(msg.Type, msg.Key)

	switch msg.Headers.Operation {
	case OperationInsert, OperationUpdate:
		var value T
		if err := json.Unmarshal(msg.Value, &value); err != nil {
			return fmt.Errorf("state: unmarshal value for %s/%s: %w", msg.Type, msg.Key, err)
		}
		a.collection.store.Set(key, value)

	case OperationDelete:
		a.collection.store.Delete(key)
	}

	return nil
}

func (a *typedCollectionApplier[T]) clear() {
	a.collection.store.Clear()
}

// Materializer processes state protocol messages and maintains state.
// It applies change messages to registered collections and handles control messages.
type Materializer struct {
	collections map[string]collectionApplier
	cfg         *materializerConfig
	mu          sync.RWMutex
	lastOffset  eventbus.Offset
}

// NewMaterializer creates a new Materializer.
func NewMaterializer(opts ...MaterializerOption) *Materializer {
	cfg := &materializerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	return &Materializer{
		collections: make(map[string]collectionApplier),
		cfg:         cfg,
	}
}

// RegisterCollection registers a typed collection with the materializer.
// The collection's entity type determines which change messages are routed to it.
func RegisterCollection[T any](m *Materializer, collection *TypedCollection[T]) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.collections[collection.EntityType()] = &typedCollectionApplier[T]{collection: collection}
}

// Apply processes a single StoredEvent containing a state protocol message.
// This is the primary integration point with ebu's Replay functionality.
//
// The event's Data field should contain a JSON-encoded ChangeMessage or ControlMessage.
func (m *Materializer) Apply(event *eventbus.StoredEvent) error {
	// Determine message type by checking for control header
	var raw struct {
		Headers json.RawMessage `json:"headers"`
	}
	if err := json.Unmarshal(event.Data, &raw); err != nil {
		return fmt.Errorf("state: unmarshal event: %w", err)
	}

	// Try to parse as control message first
	var ctrlHeaders ControlHeaders
	if json.Unmarshal(raw.Headers, &ctrlHeaders) == nil && ctrlHeaders.Control != "" {
		m.applyControl(&ControlMessage{Headers: ctrlHeaders})
		m.mu.Lock()
		m.lastOffset = event.Offset
		m.mu.Unlock()
		return nil
	}

	// Parse as change message
	var changeMsg ChangeMessage
	if err := json.Unmarshal(event.Data, &changeMsg); err != nil {
		return fmt.Errorf("state: unmarshal change message: %w", err)
	}

	if err := m.applyChange(&changeMsg); err != nil {
		return err
	}

	m.mu.Lock()
	m.lastOffset = event.Offset
	m.mu.Unlock()
	return nil
}

// ApplyChangeMessage processes a ChangeMessage directly.
// Use this when you have a ChangeMessage that's not wrapped in a StoredEvent.
func (m *Materializer) ApplyChangeMessage(msg *ChangeMessage) error {
	return m.applyChange(msg)
}

// ApplyControlMessage processes a ControlMessage directly.
// Use this when you have a ControlMessage that's not wrapped in a StoredEvent.
func (m *Materializer) ApplyControlMessage(msg *ControlMessage) {
	m.applyControl(msg)
}

// applyChange applies a change message to the appropriate collection.
func (m *Materializer) applyChange(msg *ChangeMessage) error {
	m.mu.RLock()
	collection, ok := m.collections[msg.Type]
	m.mu.RUnlock()

	if !ok {
		if m.cfg.strictSchema {
			return fmt.Errorf("state: unknown entity type: %s", msg.Type)
		}
		return nil // Ignore unknown types in non-strict mode
	}

	if err := collection.applyChange(msg); err != nil {
		if m.cfg.onError != nil {
			m.cfg.onError(err)
		}
		return err
	}

	return nil
}

// applyControl applies a control message.
func (m *Materializer) applyControl(msg *ControlMessage) {
	switch msg.Headers.Control {
	case ControlReset:
		m.mu.Lock()
		for _, c := range m.collections {
			c.clear()
		}
		m.mu.Unlock()
		if m.cfg.onReset != nil {
			m.cfg.onReset()
		}

	case ControlSnapshotStart:
		if m.cfg.onSnapshot != nil {
			m.cfg.onSnapshot(true)
		}

	case ControlSnapshotEnd:
		if m.cfg.onSnapshot != nil {
			m.cfg.onSnapshot(false)
		}
	}
}

// LastOffset returns the offset of the last applied event.
func (m *Materializer) LastOffset() eventbus.Offset {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastOffset
}

// Replay is a convenience method that replays events from an EventBus.
// It calls bus.Replay and applies each event through the materializer.
func (m *Materializer) Replay(ctx context.Context, bus *eventbus.EventBus, from eventbus.Offset) error {
	return bus.Replay(ctx, from, m.Apply)
}
