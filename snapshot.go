package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Snapshot represents a point-in-time state of an aggregate
type Snapshot struct {
	AggregateID   string          `json:"aggregate_id"`
	AggregateType string          `json:"aggregate_type"`
	Version       int64           `json:"version"`
	Data          json.RawMessage `json:"data"`
	Timestamp     time.Time       `json:"timestamp"`
}

// SnapshotStore defines the interface for storing and retrieving snapshots
type SnapshotStore interface {
	// Save stores a snapshot
	Save(ctx context.Context, snapshot *Snapshot) error
	// Load retrieves the latest snapshot for an aggregate
	Load(ctx context.Context, aggregateID string) (*Snapshot, error)
	// Delete removes all snapshots for an aggregate (optional cleanup)
	Delete(ctx context.Context, aggregateID string) error
}

// SnapshotPolicy determines when to create snapshots
type SnapshotPolicy interface {
	// ShouldSnapshot returns true if a snapshot should be created
	ShouldSnapshot(version int64, lastSnapshotVersion int64, lastSnapshotTime time.Time) bool
}

// SnapshottableAggregate extends Aggregate with snapshot capabilities
type SnapshottableAggregate[E any] interface {
	Aggregate[E]
	// CreateSnapshot serializes the current state
	CreateSnapshot() ([]byte, error)
	// RestoreFromSnapshot deserializes state from a snapshot
	RestoreFromSnapshot(data []byte, version int64) error
}

// PolicyFunc is a function that implements SnapshotPolicy
type PolicyFunc func(version int64, lastSnapshotVersion int64, lastSnapshotTime time.Time) bool

func (f PolicyFunc) ShouldSnapshot(version int64, lastSnapshotVersion int64, lastSnapshotTime time.Time) bool {
	return f(version, lastSnapshotVersion, lastSnapshotTime)
}

// EveryNEvents creates a policy that snapshots after N events
func EveryNEvents(n int64) SnapshotPolicy {
	if n <= 0 {
		n = 1
	}
	return PolicyFunc(func(version int64, lastSnapshotVersion int64, lastSnapshotTime time.Time) bool {
		return version-lastSnapshotVersion >= n
	})
}

// TimeInterval creates a policy that snapshots after a time interval
func TimeInterval(interval time.Duration) SnapshotPolicy {
	return PolicyFunc(func(version int64, lastSnapshotVersion int64, lastSnapshotTime time.Time) bool {
		return time.Since(lastSnapshotTime) >= interval
	})
}

// Never creates a policy that never takes snapshots
func Never() SnapshotPolicy {
	return PolicyFunc(func(version int64, lastSnapshotVersion int64, lastSnapshotTime time.Time) bool {
		return false
	})
}

// Combined creates a policy that triggers when ANY condition is met
func Combined(policies ...SnapshotPolicy) SnapshotPolicy {
	return PolicyFunc(func(version int64, lastSnapshotVersion int64, lastSnapshotTime time.Time) bool {
		for _, policy := range policies {
			if policy.ShouldSnapshot(version, lastSnapshotVersion, lastSnapshotTime) {
				return true
			}
		}
		return false
	})
}

// SnapshotManager handles snapshot creation and loading
type SnapshotManager struct {
	store  SnapshotStore
	policy SnapshotPolicy
	mu     sync.RWMutex
	// Track last snapshot info per aggregate
	lastSnapshots map[string]*snapshotInfo
}

type snapshotInfo struct {
	version   int64
	timestamp time.Time
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(store SnapshotStore, policy SnapshotPolicy) *SnapshotManager {
	if policy == nil {
		policy = Never()
	}
	return &SnapshotManager{
		store:         store,
		policy:        policy,
		lastSnapshots: make(map[string]*snapshotInfo),
	}
}

// MaybeSnapshot checks if a snapshot should be taken and creates it if needed
func (sm *SnapshotManager) MaybeSnapshot(ctx context.Context, aggregateID string, version int64, createSnapshot func() ([]byte, error)) error {
	if sm.store == nil {
		return nil // No snapshot store configured
	}

	sm.mu.RLock()
	lastInfo, exists := sm.lastSnapshots[aggregateID]
	sm.mu.RUnlock()

	if !exists {
		lastInfo = &snapshotInfo{version: 0, timestamp: time.Time{}}
	}

	if !sm.policy.ShouldSnapshot(version, lastInfo.version, lastInfo.timestamp) {
		return nil
	}

	// Create snapshot
	data, err := createSnapshot()
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	snapshot := &Snapshot{
		AggregateID:   aggregateID,
		AggregateType: "aggregate",
		Version:       version,
		Data:          data,
		Timestamp:     time.Now(),
	}

	if err := sm.store.Save(ctx, snapshot); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	// Update tracking
	sm.mu.Lock()
	sm.lastSnapshots[aggregateID] = &snapshotInfo{
		version:   version,
		timestamp: snapshot.Timestamp,
	}
	sm.mu.Unlock()

	return nil
}

// LoadSnapshot loads the latest snapshot for an aggregate
func (sm *SnapshotManager) LoadSnapshot(ctx context.Context, aggregateID string) (*Snapshot, error) {
	if sm.store == nil {
		return nil, errors.New("no snapshot store configured")
	}
	return sm.store.Load(ctx, aggregateID)
}

// MemorySnapshotStore is an in-memory implementation of SnapshotStore
type MemorySnapshotStore struct {
	snapshots map[string]*Snapshot
	mu        sync.RWMutex
}

// NewMemorySnapshotStore creates a new in-memory snapshot store
func NewMemorySnapshotStore() *MemorySnapshotStore {
	return &MemorySnapshotStore{
		snapshots: make(map[string]*Snapshot),
	}
}

// Save stores a snapshot
func (s *MemorySnapshotStore) Save(ctx context.Context, snapshot *Snapshot) error {
	if snapshot == nil {
		return errors.New("snapshot cannot be nil")
	}
	if snapshot.AggregateID == "" {
		return errors.New("aggregate ID is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Only keep the latest snapshot per aggregate
	s.snapshots[snapshot.AggregateID] = snapshot
	return nil
}

// Load retrieves the latest snapshot for an aggregate
func (s *MemorySnapshotStore) Load(ctx context.Context, aggregateID string) (*Snapshot, error) {
	if aggregateID == "" {
		return nil, errors.New("aggregate ID is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot, exists := s.snapshots[aggregateID]
	if !exists {
		return nil, fmt.Errorf("snapshot not found for aggregate %s", aggregateID)
	}

	// Return a copy to prevent external modifications
	snapshotCopy := *snapshot
	return &snapshotCopy, nil
}

// Delete removes all snapshots for an aggregate
func (s *MemorySnapshotStore) Delete(ctx context.Context, aggregateID string) error {
	if aggregateID == "" {
		return errors.New("aggregate ID is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.snapshots, aggregateID)
	return nil
}

// WithSnapshots is a BusOption that enables aggregate snapshots
func WithSnapshots(store SnapshotStore, policy SnapshotPolicy) BusOption {
	return func(bus *EventBus) {
		// Store snapshot manager in bus for access by CQRS
		if bus.extensions == nil {
			bus.extensions = make(map[string]any)
		}
		bus.extensions["snapshot_manager"] = NewSnapshotManager(store, policy)
	}
}

// GetSnapshotManager retrieves the snapshot manager from the bus
func GetSnapshotManager(bus *EventBus) *SnapshotManager {
	if bus.extensions == nil {
		return nil
	}
	if sm, ok := bus.extensions["snapshot_manager"].(*SnapshotManager); ok {
		return sm
	}
	return nil
}

// LoadAggregate loads an aggregate using snapshot + events since snapshot
func LoadAggregate[A SnapshottableAggregate[E], E any](
	ctx context.Context,
	aggregateID string,
	createAggregate func() A,
	eventStore EventStore,
	snapshotStore SnapshotStore,
) (A, error) {
	var zero A
	if aggregateID == "" {
		return zero, errors.New("aggregate ID is required")
	}

	// Create new aggregate instance
	aggregate := createAggregate()

	var fromVersion int64 = 0

	// Try to load snapshot if store is available
	if snapshotStore != nil {
		snapshot, err := snapshotStore.Load(ctx, aggregateID)
		if err == nil && snapshot != nil {
			// Restore from snapshot
			if err := aggregate.RestoreFromSnapshot(snapshot.Data, snapshot.Version); err != nil {
				return zero, fmt.Errorf("failed to restore from snapshot: %w", err)
			}
			fromVersion = snapshot.Version
		}
		// If no snapshot found, we'll load all events from the beginning
	}

	// Load events after snapshot
	if eventStore != nil {
		events, err := eventStore.Load(ctx, fromVersion, -1)
		if err != nil {
			return zero, fmt.Errorf("failed to load events: %w", err)
		}

		// Filter events for this aggregate and unmarshal them
		var aggregateEvents []E
		for _, storedEvent := range events {
			// We need a way to determine if an event belongs to this aggregate
			// This is typically done by examining the event data
			// For now, we'll unmarshal and check if it has the aggregate ID
			var eventData map[string]any
			if err := json.Unmarshal(storedEvent.Data, &eventData); err != nil {
				continue // Skip events that can't be unmarshaled
			}

			// Check if event belongs to this aggregate (implementation-specific)
			// This assumes events have an "aggregate_id" or similar field
			if aggID, ok := eventData["aggregate_id"].(string); ok && aggID == aggregateID {
				// Reconstruct the actual event type
				// This requires type information to be stored with the event
				var event E
				if err := json.Unmarshal(storedEvent.Data, &event); err == nil {
					aggregateEvents = append(aggregateEvents, event)
				}
			}
		}

		// Apply events to aggregate if there are any
		if len(aggregateEvents) > 0 {
			if err := aggregate.LoadFromHistory(aggregateEvents); err != nil {
				return zero, fmt.Errorf("failed to load from history: %w", err)
			}
		}
	}

	return aggregate, nil
}

// SaveAggregate saves an aggregate's uncommitted events and optionally creates a snapshot
func SaveAggregate[A SnapshottableAggregate[E], E any](
	ctx context.Context,
	aggregate A,
	eventStore EventStore,
	snapshotManager *SnapshotManager,
) error {
	// Save uncommitted events
	events := aggregate.GetUncommittedEvents()
	for _, event := range events {
		// Marshal event
		eventData, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		// Create stored event
		storedEvent := &StoredEvent{
			Type:      fmt.Sprintf("%T", event),
			Data:      eventData,
			Timestamp: time.Now(),
			// Position will be set by the event store
		}

		// Save to event store
		if err := eventStore.Save(ctx, storedEvent); err != nil {
			return fmt.Errorf("failed to save event: %w", err)
		}
	}

	// Mark events as committed
	aggregate.MarkEventsAsCommitted()

	// Check if we should create a snapshot
	if snapshotManager != nil {
		if err := snapshotManager.MaybeSnapshot(ctx, aggregate.GetID(), aggregate.GetVersion(), aggregate.CreateSnapshot); err != nil {
			// Log error but don't fail the save
			// Snapshots are optimization, not required for correctness
			fmt.Printf("Warning: failed to create snapshot: %v\n", err)
		}
	}

	return nil
}
