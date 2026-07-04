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
	// snapshot serializes every entity in the collection, keyed by composite key.
	snapshot() (map[string]json.RawMessage, error)
	// restore clears the collection and repopulates it from serialized entities.
	restore(entities map[string]json.RawMessage) error
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

	default:
		return fmt.Errorf("state: unknown operation %q for %s/%s", msg.Headers.Operation, msg.Type, msg.Key)
	}

	return nil
}

func (a *typedCollectionApplier[T]) clear() {
	a.collection.store.Clear()
}

func (a *typedCollectionApplier[T]) snapshot() (map[string]json.RawMessage, error) {
	all := a.collection.store.All()
	entities := make(map[string]json.RawMessage, len(all))
	for key, value := range all {
		data, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("state: marshal snapshot entity %s: %w", key, err)
		}
		entities[key] = data
	}
	return entities, nil
}

func (a *typedCollectionApplier[T]) restore(entities map[string]json.RawMessage) error {
	a.collection.store.Clear()
	for key, raw := range entities {
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("state: unmarshal snapshot entity %s: %w", key, err)
		}
		a.collection.store.Set(key, value)
	}
	return nil
}

// Materializer processes state protocol messages and maintains state.
// It applies change messages to registered collections and handles control messages.
//
// Message application is serialized: concurrent Apply, ApplyChangeMessage, and
// ApplyControlMessage calls are applied one at a time, so a reset can never be
// overwritten by a logically-earlier change, and LastOffset only advances past
// events that were applied successfully. Callbacks configured via options run
// while an application is in flight and must not call back into Apply,
// ApplyChangeMessage, ApplyControlMessage, SaveSnapshotTo, or LoadSnapshotFrom.
type Materializer struct {
	collections map[string]collectionApplier
	cfg         *materializerConfig

	// applyMu serializes message application. It is held for the entirety of
	// Apply/ApplyChangeMessage/ApplyControlMessage — application plus the
	// lastOffset update happen as one atomic step — and for snapshot
	// save/restore, which need a consistent view across collections.
	applyMu sync.Mutex

	// mu guards the collections map and lastOffset.
	mu         sync.RWMutex
	lastOffset eventbus.Offset
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
// The event's Data field should contain a JSON-encoded ChangeMessage or
// ControlMessage. Events that are not state protocol messages — both message
// kinds always carry a "headers" field on the wire, so an event without one
// (e.g. a regular event on a mixed stream) — are skipped silently in both
// strict and non-strict mode.
//
// The offset reported by LastOffset advances only when the event was applied
// (or skipped) successfully; failed events never advance it, so a resumed
// replay picks them up again. All failures are reported to the WithOnError
// callback before being returned.
func (m *Materializer) Apply(event *eventbus.StoredEvent) error {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	// Determine message type by checking for control header
	var raw struct {
		Headers json.RawMessage `json:"headers"`
	}
	if err := json.Unmarshal(event.Data, &raw); err != nil {
		return m.reportError(fmt.Errorf("state: unmarshal event: %w", err))
	}

	// Not a state protocol message: skip it so materializers work on streams
	// that mix state messages with regular events.
	if raw.Headers == nil {
		m.setLastOffset(event.Offset)
		return nil
	}

	// Try to parse as control message first
	var ctrlHeaders ControlHeaders
	if json.Unmarshal(raw.Headers, &ctrlHeaders) == nil && ctrlHeaders.Control != "" {
		m.applyControl(&ControlMessage{Headers: ctrlHeaders})
		m.setLastOffset(event.Offset)
		return nil
	}

	// Parse as change message
	var changeMsg ChangeMessage
	if err := json.Unmarshal(event.Data, &changeMsg); err != nil {
		return m.reportError(fmt.Errorf("state: unmarshal change message: %w", err))
	}

	if err := m.applyChange(&changeMsg); err != nil {
		return err
	}

	m.setLastOffset(event.Offset)
	return nil
}

// ApplyChangeMessage processes a ChangeMessage directly.
// Use this when you have a ChangeMessage that's not wrapped in a StoredEvent.
func (m *Materializer) ApplyChangeMessage(msg *ChangeMessage) error {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	return m.applyChange(msg)
}

// ApplyControlMessage processes a ControlMessage directly.
// Use this when you have a ControlMessage that's not wrapped in a StoredEvent.
func (m *Materializer) ApplyControlMessage(msg *ControlMessage) {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	m.applyControl(msg)
}

// applyChange applies a change message to the appropriate collection.
func (m *Materializer) applyChange(msg *ChangeMessage) error {
	m.mu.RLock()
	collection, ok := m.collections[msg.Type]
	m.mu.RUnlock()

	if !ok {
		if m.cfg.strictSchema {
			return m.reportError(fmt.Errorf("state: unknown entity type: %s", msg.Type))
		}
		return nil // Ignore unknown types in non-strict mode
	}

	if err := collection.applyChange(msg); err != nil {
		return m.reportError(err)
	}

	return nil
}

// reportError invokes the configured error callback, if any, and returns err.
// Every materialization error passes through here exactly once.
func (m *Materializer) reportError(err error) error {
	if m.cfg.onError != nil {
		m.cfg.onError(err)
	}
	return err
}

// setLastOffset records the offset of the last successfully applied event.
// Callers must hold applyMu.
func (m *Materializer) setLastOffset(offset eventbus.Offset) {
	m.mu.Lock()
	m.lastOffset = offset
	m.mu.Unlock()
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
// It calls bus.ReplayWithUpcast and applies each event through the
// materializer, so events with registered schema migrations are upcasted
// before application.
func (m *Materializer) Replay(ctx context.Context, bus *eventbus.EventBus, from eventbus.Offset) error {
	return bus.ReplayWithUpcast(ctx, from, m.Apply)
}

// SaveSnapshotTo serializes every registered collection and saves the result
// to s under snapshotID, tagged with the offset of the last applied event.
// The blob format is {"entityType": {"compositeKey": <entity JSON>}}.
//
// It returns an error if no event has been applied yet (LastOffset is empty):
// such a snapshot would claim OffsetOldest, and a later TruncateBefore based
// on it could silently discard the whole log.
//
// The intended compaction sequence is:
//
//	if err := mat.SaveSnapshotTo(ctx, snapshotter, "users"); err != nil { ... }
//	// Optionally, once the snapshot is durably saved, compact the log:
//	if tr, ok := bus.GetStore().(eventbus.EventStoreTruncator); ok {
//	    tr.TruncateBefore(ctx, offset) // offset the snapshot was saved at
//	}
//
// Truncation is only safe once the snapshot is durably saved AND no other
// reader or subscription still needs the truncated prefix. The offset to
// truncate at is the snapshot's offset (retrievable via the snapshotter's
// LoadSnapshot, or LastOffset when no events are applied concurrently) —
// never a later one, or events not covered by the snapshot would be lost.
//
// Message application is paused while the snapshot is taken, so the blob is a
// consistent view of all collections at a single offset.
func (m *Materializer) SaveSnapshotTo(ctx context.Context, s eventbus.EventStoreSnapshotter, snapshotID string) error {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	m.mu.RLock()
	offset := m.lastOffset
	collections := make(map[string]collectionApplier, len(m.collections))
	for entityType, c := range m.collections {
		collections[entityType] = c
	}
	m.mu.RUnlock()

	if offset == eventbus.OffsetOldest {
		return fmt.Errorf("state: refusing to save snapshot %q: no events applied yet (snapshot would claim OffsetOldest)", snapshotID)
	}

	blob := make(map[string]map[string]json.RawMessage, len(collections))
	for entityType, c := range collections {
		entities, err := c.snapshot()
		if err != nil {
			return err
		}
		blob[entityType] = entities
	}

	// blob contains only string keys and json.RawMessage values produced by
	// json.Marshal, so encoding cannot fail.
	encoded, _ := json.Marshal(blob)

	if err := s.SaveSnapshot(ctx, snapshotID, offset, encoded); err != nil {
		return fmt.Errorf("state: save snapshot %q: %w", snapshotID, err)
	}
	return nil
}

// LoadSnapshotFrom restores the materializer from the snapshot saved under
// snapshotID: it clears all registered collections, repopulates them from the
// snapshot blob, sets LastOffset to the snapshot's offset, and returns that
// offset. The caller resumes with:
//
//	offset, err := mat.LoadSnapshotFrom(ctx, snapshotter, "users")
//	if err != nil { ... }
//	if err := mat.Replay(ctx, bus, offset); err != nil { ... }
//
// When no snapshot exists it returns OffsetOldest with the collections
// untouched, so the caller's Replay naturally rebuilds from the beginning.
// Snapshot data for entity types with no registered collection is dropped
// silently.
//
// If restoring fails, the materializer is left empty with LastOffset reset to
// OffsetOldest, so a full replay from OffsetOldest rebuilds the state.
func (m *Materializer) LoadSnapshotFrom(ctx context.Context, s eventbus.EventStoreSnapshotter, snapshotID string) (eventbus.Offset, error) {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	offset, blob, err := s.LoadSnapshot(ctx, snapshotID)
	if err != nil {
		return eventbus.OffsetOldest, fmt.Errorf("state: load snapshot %q: %w", snapshotID, err)
	}
	if offset == eventbus.OffsetOldest && blob == nil {
		return eventbus.OffsetOldest, nil // No snapshot: replay from the beginning.
	}

	var decoded map[string]map[string]json.RawMessage
	if err := json.Unmarshal(blob, &decoded); err != nil {
		return eventbus.OffsetOldest, fmt.Errorf("state: unmarshal snapshot %q: %w", snapshotID, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, c := range m.collections {
		c.clear()
	}
	for entityType, entities := range decoded {
		c, ok := m.collections[entityType]
		if !ok {
			continue // Entity type no longer registered: drop its snapshot data.
		}
		if err := c.restore(entities); err != nil {
			// Leave the materializer empty rather than partially restored: a
			// full replay from OffsetOldest rebuilds the state.
			for _, c := range m.collections {
				c.clear()
			}
			m.lastOffset = eventbus.OffsetOldest
			return eventbus.OffsetOldest, err
		}
	}

	m.lastOffset = offset
	return offset, nil
}
