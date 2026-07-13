package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	eventbus "github.com/jilio/ebu"
)

// Store is a generic interface for state storage.
// Implementations can use any backing store (memory, database, etc.).
//
// Every method can report failure: a durable backend that swallowed errors
// would let the materializer advance LastOffset past updates that were never
// applied, silently corrupting snapshots and any log compaction based on
// them. The materializer never advances the offset when a store call fails.
type Store[T any] interface {
	// Get retrieves an entity by its composite key.
	Get(compositeKey string) (T, bool, error)
	// Set stores an entity with the given composite key.
	Set(compositeKey string, value T) error
	// Delete removes an entity by its composite key.
	Delete(compositeKey string) error
	// Clear removes all entities from the store.
	Clear() error
	// All returns a copy of all entities in the store.
	All() (map[string]T, error)
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
func (s *MemoryStore[T]) Get(key string) (T, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok, nil
}

// Set stores an entity with the given composite key.
func (s *MemoryStore[T]) Set(key string, value T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

// Delete removes an entity by its composite key.
func (s *MemoryStore[T]) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}

// Clear removes all entities from the store.
func (s *MemoryStore[T]) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]T)
	return nil
}

// All returns a copy of all entities in the store.
func (s *MemoryStore[T]) All() (map[string]T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]T, len(s.data))
	for k, v := range s.data {
		result[k] = v
	}
	return result, nil
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
	return &TypedCollection[T]{
		store:      store,
		entityType: entityTypeFor[T](),
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
func (c *TypedCollection[T]) Get(key string) (T, bool, error) {
	return c.store.Get(CompositeKey(c.entityType, key))
}

// keyPrefix returns the composite-key prefix that scopes this collection's
// entities within its Store.
func (c *TypedCollection[T]) keyPrefix() string {
	return CompositeKey(c.entityType, "")
}

// All returns all entities in this collection.
// The keys in the returned map are CompositeKey-encoded keys (type/key for
// components without reserved characters).
//
// Only entities under this collection's type prefix are returned: several
// collections may share one Store, and each must see (and mutate — see
// clear/restore) only its own slice of it.
func (c *TypedCollection[T]) All() (map[string]T, error) {
	all, err := c.store.All()
	if err != nil {
		return nil, err
	}
	prefix := c.keyPrefix()
	result := make(map[string]T, len(all))
	for key, value := range all {
		if strings.HasPrefix(key, prefix) {
			result[key] = value
		}
	}
	return result, nil
}

// ErrUndecodable marks materialization failures caused by a message payload
// that cannot be decoded (malformed JSON, an incompatible schema, or an
// unknown operation). It never marks store failures. WithApplyErrorPolicy
// consults this: only decode failures are safely skippable — a store failure
// is transient and must abort so the event is retried.
var ErrUndecodable = errors.New("state: undecodable message")

// collectionApplier is an internal interface for applying changes to collections.
type collectionApplier interface {
	applyChange(msg *ChangeMessage) error
	clear() error
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
			return fmt.Errorf("%w: unmarshal value for %s/%s: %w", ErrUndecodable, msg.Type, msg.Key, err)
		}
		if err := a.collection.store.Set(key, value); err != nil {
			return fmt.Errorf("state: set %s: %w", key, err)
		}

	case OperationDelete:
		if err := a.collection.store.Delete(key); err != nil {
			return fmt.Errorf("state: delete %s: %w", key, err)
		}

	default:
		return fmt.Errorf("%w: unknown operation %q for %s/%s", ErrUndecodable, msg.Headers.Operation, msg.Type, msg.Key)
	}

	return nil
}

// clear removes only this collection's entities (its type-prefixed keys):
// a shared Store may hold other collections' data, which must survive.
func (a *typedCollectionApplier[T]) clear() error {
	all, err := a.collection.All()
	if err != nil {
		return fmt.Errorf("state: clear %s: %w", a.collection.entityType, err)
	}
	for key := range all {
		if err := a.collection.store.Delete(key); err != nil {
			return fmt.Errorf("state: clear %s: %w", key, err)
		}
	}
	return nil
}

func (a *typedCollectionApplier[T]) snapshot() (map[string]json.RawMessage, error) {
	all, err := a.collection.All()
	if err != nil {
		return nil, fmt.Errorf("state: snapshot %s: %w", a.collection.entityType, err)
	}
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
	if err := a.clear(); err != nil {
		return err
	}
	for key, raw := range entities {
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("state: unmarshal snapshot entity %s: %w", key, err)
		}
		if err := a.collection.store.Set(key, value); err != nil {
			return fmt.Errorf("state: restore %s: %w", key, err)
		}
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
	// restore, which needs a consistent view across collections.
	applyMu sync.Mutex

	// snapshotMu serializes SaveSnapshotTo calls end to end, including the
	// SaveSnapshot I/O that runs after applyMu is released: two racing
	// savers could otherwise overwrite a newer snapshot with an older one,
	// and a truncation based on the newer offset would then lose events.
	// Lock order: snapshotMu before applyMu, never the reverse.
	snapshotMu sync.Mutex

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

// changeMessageEventType and controlMessageEventType are the names state
// protocol messages are persisted under when published through ebu (see the
// EventTypeName methods in message.go).
const (
	changeMessageEventType  = "state.ChangeMessage"
	controlMessageEventType = "state.ControlMessage"
)

// Apply processes a single StoredEvent containing a state protocol message.
// This is the primary integration point with ebu's Replay functionality.
//
// Routing is by the event's Type first: events published through ebu are
// stored as "state.ChangeMessage" / "state.ControlMessage" and are decoded
// strictly — a decode failure on a typed event is a real error. Events with
// any other Type fall back to structural detection for interoperability with
// streams written by other State Protocol implementations: an event whose
// data carries a "headers" object is applied if it decodes as a control or
// change message, and skipped as a foreign event otherwise. Events without a
// "headers" field are always skipped, so state messages can share a stream
// with regular events.
//
// The offset reported by LastOffset advances only when the event was applied
// (or skipped) successfully; failed events never advance it, so a resumed
// replay picks them up again — unless WithApplyErrorPolicy(ApplySkip) is
// configured, in which case undecodable typed messages are reported and
// skipped with the offset advancing. All failures are reported to the
// WithOnError callback before being returned.
func (m *Materializer) Apply(event *eventbus.StoredEvent) error {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	err := m.applyEvent(event)
	if err != nil && m.cfg.applyErrorPolicy == ApplySkip && errors.Is(err, ErrUndecodable) {
		// Poison message: it was reported to WithOnError by applyEvent;
		// advancing past it keeps one bad payload from wedging every
		// future replay on the same event. Store failures never take this
		// path — they are transient and the event must be retried.
		m.setLastOffset(event.Offset)
		return nil
	}
	if err != nil {
		return err
	}

	m.setLastOffset(event.Offset)
	return nil
}

// applyEvent routes and applies one event. Caller must hold applyMu.
// Every returned error has been reported to WithOnError exactly once.
func (m *Materializer) applyEvent(event *eventbus.StoredEvent) error {
	switch event.Type {
	case changeMessageEventType:
		var changeMsg ChangeMessage
		if err := json.Unmarshal(event.Data, &changeMsg); err != nil {
			return m.reportError(fmt.Errorf("%w: unmarshal change message: %w", ErrUndecodable, err))
		}
		return m.applyChange(&changeMsg)

	case controlMessageEventType:
		var ctrlMsg ControlMessage
		if err := json.Unmarshal(event.Data, &ctrlMsg); err != nil {
			return m.reportError(fmt.Errorf("%w: unmarshal control message: %w", ErrUndecodable, err))
		}
		return m.applyControl(&ctrlMsg)

	default:
		return m.applyUntyped(event)
	}
}

// applyUntyped structurally detects state messages persisted under other
// type names (streams written by non-ebu State Protocol implementations).
// Anything that does not positively identify as a state message is skipped
// as a foreign event — never an error: on a mixed stream a payload that
// merely resembles a state message (e.g. any event with a "headers" field)
// must not wedge or corrupt the materializer.
func (m *Materializer) applyUntyped(event *eventbus.StoredEvent) error {
	var raw struct {
		Headers json.RawMessage `json:"headers"`
	}
	if err := json.Unmarshal(event.Data, &raw); err != nil {
		// Not JSON at all: a foreign event.
		return nil
	}
	if raw.Headers == nil {
		return nil
	}

	// A headers object with a known control value is a control message.
	// Unknown control values are NOT applied here (unlike the typed route):
	// a foreign event with an unrelated "control" field must not be consumed.
	var ctrlHeaders ControlHeaders
	if json.Unmarshal(raw.Headers, &ctrlHeaders) == nil && knownControl(ctrlHeaders.Control) {
		return m.applyControl(&ControlMessage{Headers: ctrlHeaders})
	}

	// A payload that decodes as a change message with a complete
	// type/key/operation triple is applied; anything else is foreign.
	var changeMsg ChangeMessage
	if err := json.Unmarshal(event.Data, &changeMsg); err != nil {
		return nil
	}
	if changeMsg.Type == "" || changeMsg.Key == "" || changeMsg.Headers.Operation == "" {
		return nil
	}
	return m.applyChange(&changeMsg)
}

// knownControl reports whether c is a control value this package understands.
func knownControl(c Control) bool {
	switch c {
	case ControlReset, ControlSnapshotStart, ControlSnapshotEnd:
		return true
	}
	return false
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
// It returns an error when clearing a collection fails on a reset, or — in
// strict mode — when the control value is unknown.
func (m *Materializer) ApplyControlMessage(msg *ControlMessage) error {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	return m.applyControl(msg)
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
func (m *Materializer) applyControl(msg *ControlMessage) error {
	switch msg.Headers.Control {
	case ControlReset:
		m.mu.Lock()
		for _, c := range m.collections {
			if err := c.clear(); err != nil {
				m.mu.Unlock()
				return m.reportError(fmt.Errorf("state: reset: %w", err))
			}
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

	default:
		// A control this package does not understand: silently dropping it
		// in strict mode could mean missing a reset the stream demanded.
		// Mirrors unknown-entity-type handling: strict errors, lax ignores.
		if m.cfg.strictSchema {
			return m.reportError(fmt.Errorf("%w: unknown control %q", ErrUndecodable, msg.Headers.Control))
		}
	}
	return nil
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

const (
	materializerSnapshotVersion = 1
	snapshotKeyCodecPercentV1   = "percent-v1"
)

// materializerSnapshot versions both the overall snapshot shape and the key
// codec. Composite keys are persistent data: changing their encoding without
// a marker would let a newer materializer accept an older snapshot, restore
// unreachable keys, and then skip the compacted history covered by its offset.
type materializerSnapshot struct {
	Version     int                                   `json:"version"`
	KeyCodec    string                                `json:"key_codec"`
	Collections map[string]map[string]json.RawMessage `json:"collections"`
}

// SaveSnapshotTo serializes every registered collection and saves the result
// to s under snapshotID, tagged with the offset of the last applied event.
// The blob format is
// {"version":1,"key_codec":"percent-v1","collections":{"entityType":{"compositeKey":<entity JSON>}}}.
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
// Message application is paused only while the collections are captured and
// serialized; the SaveSnapshot I/O itself runs without blocking Apply.
// Concurrent SaveSnapshotTo calls are serialized with each other so a slower
// older snapshot can never overwrite a newer one.
func (m *Materializer) SaveSnapshotTo(ctx context.Context, s eventbus.EventStoreSnapshotter, snapshotID string) error {
	m.snapshotMu.Lock()
	defer m.snapshotMu.Unlock()

	offset, encoded, err := m.captureSnapshot(snapshotID)
	if err != nil {
		return err
	}

	if err := s.SaveSnapshot(ctx, snapshotID, offset, encoded); err != nil {
		return fmt.Errorf("state: save snapshot %q: %w", snapshotID, err)
	}
	return nil
}

// captureSnapshot serializes all collections as one consistent view at the
// current LastOffset, holding applyMu so no application runs mid-capture.
func (m *Materializer) captureSnapshot(snapshotID string) (eventbus.Offset, json.RawMessage, error) {
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
		return "", nil, fmt.Errorf("state: refusing to save snapshot %q: no events applied yet (snapshot would claim OffsetOldest)", snapshotID)
	}

	snapshot := materializerSnapshot{
		Version:     materializerSnapshotVersion,
		KeyCodec:    snapshotKeyCodecPercentV1,
		Collections: make(map[string]map[string]json.RawMessage, len(collections)),
	}
	for entityType, c := range collections {
		entities, err := c.snapshot()
		if err != nil {
			return "", nil, err
		}
		snapshot.Collections[entityType] = entities
	}
	if err := validatePercentV1SnapshotKeys(snapshot.Collections); err != nil {
		return "", nil, fmt.Errorf("state: refusing to save snapshot %q: %w", snapshotID, err)
	}

	// snapshot contains only string keys and json.RawMessage values produced by
	// json.Marshal, so encoding cannot fail.
	encoded, _ := json.Marshal(snapshot)
	return offset, encoded, nil
}

// decodeSnapshotCollections validates the snapshot/key-codec marker before
// returning any data to the mutating restore path. Versionless snapshots are
// the legacy type->composite-key map. They remain safe only when every entity
// type and key excludes the reserved '%' and '/' characters, because those
// composite keys are byte-for-byte identical under both codecs.
func decodeSnapshotCollections(blob json.RawMessage) (map[string]map[string]json.RawMessage, error) {
	var topLevel map[string]json.RawMessage
	if err := json.Unmarshal(blob, &topLevel); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot envelope: %w", err)
	}
	if topLevel == nil {
		return nil, fmt.Errorf("snapshot envelope must be a JSON object")
	}

	// A current snapshot has a scalar version. A legacy collection may itself
	// be named "version"; its value is an object, so keep treating that shape as
	// legacy rather than reserving a previously-valid entity type name.
	if rawVersion, ok := topLevel["version"]; ok {
		var legacyVersionCollection map[string]json.RawMessage
		if err := json.Unmarshal(rawVersion, &legacyVersionCollection); err != nil {
			var snapshot materializerSnapshot
			if err := json.Unmarshal(blob, &snapshot); err != nil {
				return nil, fmt.Errorf("unmarshal versioned snapshot: %w", err)
			}
			if snapshot.Version != materializerSnapshotVersion ||
				snapshot.KeyCodec != snapshotKeyCodecPercentV1 || snapshot.Collections == nil {
				return nil, fmt.Errorf("unsupported snapshot format version=%d key_codec=%q", snapshot.Version, snapshot.KeyCodec)
			}
			if err := validatePercentV1SnapshotKeys(snapshot.Collections); err != nil {
				return nil, err
			}
			return snapshot.Collections, nil
		}
	}

	var legacy map[string]map[string]json.RawMessage
	if err := json.Unmarshal(blob, &legacy); err != nil {
		return nil, fmt.Errorf("unmarshal legacy snapshot: %w", err)
	}
	if err := validateLegacySnapshotKeys(legacy); err != nil {
		return nil, err
	}
	return legacy, nil
}

func validatePercentV1SnapshotKeys(collections map[string]map[string]json.RawMessage) error {
	for entityType, entities := range collections {
		if entities == nil {
			return fmt.Errorf("versioned snapshot collection %q must be a JSON object, not null", entityType)
		}
		prefix := CompositeKey(entityType, "")
		for compositeKey := range entities {
			if !strings.HasPrefix(compositeKey, prefix) || strings.Count(compositeKey, "/") != 1 {
				return fmt.Errorf("versioned snapshot composite key %q does not match collection %q under key codec %q", compositeKey, entityType, snapshotKeyCodecPercentV1)
			}

			encodedKey := strings.TrimPrefix(compositeKey, prefix)
			decodedKey, err := url.PathUnescape(encodedKey)
			if err != nil || encodeKeyComponent(decodedKey) != encodedKey {
				return fmt.Errorf("versioned snapshot composite key %q is not canonical under key codec %q", compositeKey, snapshotKeyCodecPercentV1)
			}
		}
	}
	return nil
}

func validateLegacySnapshotKeys(collections map[string]map[string]json.RawMessage) error {
	for entityType, entities := range collections {
		if entities == nil {
			return fmt.Errorf("legacy snapshot collection %q must be a JSON object, not null", entityType)
		}
		if strings.ContainsAny(entityType, "%/") {
			return fmt.Errorf("legacy snapshot entity type %q uses a reserved key-codec character; migrate it explicitly, or discard and rebuild from OffsetOldest only if the complete source history is still available", entityType)
		}
		prefix := entityType + "/"
		for compositeKey := range entities {
			key, ok := strings.CutPrefix(compositeKey, prefix)
			if !ok || strings.ContainsAny(key, "%/") {
				return fmt.Errorf("legacy snapshot composite key %q in collection %q is unsafe under key codec %q; migrate it explicitly, or discard and rebuild from OffsetOldest only if the complete source history is still available", compositeKey, entityType, snapshotKeyCodecPercentV1)
			}
		}
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
// silently. A snapshot that omits any currently registered collection is
// rejected before mutation: accepting its offset would skip the omitted
// projection's earlier history, permanently so if that history was compacted.
// Register every collection before loading, and explicitly migrate an older
// snapshot when adding a collection. Versionless snapshots from the legacy
// composite-key codec are accepted only when all entity types and keys contain
// neither '%' nor '/'; otherwise loading fails before any collection or
// LastOffset is changed. Versioned snapshots are likewise rejected before
// mutation unless every composite key is canonical for its declared key codec
// and collection.
//
// If validation succeeds but mutating restore then fails, the materializer is
// left empty with LastOffset reset to OffsetOldest, so a full replay from
// OffsetOldest rebuilds the state.
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

	decoded, err := decodeSnapshotCollections(blob)
	if err != nil {
		return eventbus.OffsetOldest, fmt.Errorf("state: decode snapshot %q: %w", snapshotID, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for entityType := range m.collections {
		if _, ok := decoded[entityType]; !ok {
			return eventbus.OffsetOldest, fmt.Errorf("state: snapshot %q is missing registered collection %q; rebuild from OffsetOldest only if the complete source history is available, or migrate the snapshot before loading if history was compacted", snapshotID, entityType)
		}
	}

	// clearAll empties every registered collection; on restore failure it
	// leaves the materializer empty rather than partially restored, so a
	// full replay from OffsetOldest rebuilds the state.
	clearAll := func() error {
		for _, c := range m.collections {
			if err := c.clear(); err != nil {
				return err
			}
		}
		return nil
	}

	resetToEmpty := func(cause error) (eventbus.Offset, error) {
		if clearErr := clearAll(); clearErr != nil {
			cause = errors.Join(cause, clearErr)
		}
		m.lastOffset = eventbus.OffsetOldest
		return eventbus.OffsetOldest, cause
	}

	if err := clearAll(); err != nil {
		return resetToEmpty(err)
	}
	for entityType, entities := range decoded {
		c, ok := m.collections[entityType]
		if !ok {
			continue // Entity type no longer registered: drop its snapshot data.
		}
		if err := c.restore(entities); err != nil {
			return resetToEmpty(err)
		}
	}

	m.lastOffset = offset
	return offset, nil
}
