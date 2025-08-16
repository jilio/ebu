package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

// SnapshotTestEvent is a test event type for snapshot tests
type SnapshotTestEvent any

type DepositEvent struct {
	Amount float64
}

type WithdrawEvent struct {
	Amount float64
}

// TestAggregate is a test aggregate that supports snapshots
type TestAggregate struct {
	BaseAggregate[SnapshotTestEvent]
	Balance float64 `json:"balance"`
	Name    string  `json:"name"`
}

func (a *TestAggregate) CreateSnapshot() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	snapshot := map[string]any{
		"id":      a.ID,
		"version": a.Version,
		"balance": a.Balance,
		"name":    a.Name,
	}

	return json.Marshal(snapshot)
}

func (a *TestAggregate) RestoreFromSnapshot(data []byte, version int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var snapshot map[string]any
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	// Restore fields
	if id, ok := snapshot["id"].(string); ok {
		a.ID = id
	}
	if balance, ok := snapshot["balance"].(float64); ok {
		a.Balance = balance
	}
	if name, ok := snapshot["name"].(string); ok {
		a.Name = name
	}

	a.Version = version
	a.uncommittedEvents = nil

	return nil
}

func (a *TestAggregate) LoadFromHistory(events []SnapshotTestEvent) error {
	for _, event := range events {
		// Apply events to update state
		switch e := event.(type) {
		case DepositEvent:
			a.Balance += e.Amount
		case WithdrawEvent:
			a.Balance -= e.Amount
		case map[string]any:
			// Handle events from snapshot loading
			if eventType, ok := e["type"].(string); ok {
				switch eventType {
				case "deposit":
					if amount, ok := e["amount"].(float64); ok {
						a.Balance += amount
					}
				case "withdraw":
					if amount, ok := e["amount"].(float64); ok {
						a.Balance -= amount
					}
				}
			}
		}
		a.Version++
	}
	return nil
}

func (a *TestAggregate) Apply(event SnapshotTestEvent) error {
	switch e := event.(type) {
	case DepositEvent:
		a.Balance += e.Amount
	case WithdrawEvent:
		a.Balance -= e.Amount
	}
	return nil
}

func TestMemorySnapshotStore(t *testing.T) {
	store := NewMemorySnapshotStore()
	ctx := context.Background()

	// Test Save and Load
	snapshot := &Snapshot{
		AggregateID:   "agg-1",
		AggregateType: "TestAggregate",
		Version:       10,
		Data:          json.RawMessage(`{"balance": 100}`),
		Timestamp:     time.Now(),
	}

	err := store.Save(ctx, snapshot)
	if err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Load snapshot
	loaded, err := store.Load(ctx, "agg-1")
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	if loaded.AggregateID != snapshot.AggregateID {
		t.Errorf("Expected aggregate ID %s, got %s", snapshot.AggregateID, loaded.AggregateID)
	}

	if loaded.Version != snapshot.Version {
		t.Errorf("Expected version %d, got %d", snapshot.Version, loaded.Version)
	}

	// Test load non-existent
	_, err = store.Load(ctx, "non-existent")
	if err == nil {
		t.Error("Expected error loading non-existent snapshot")
	}

	// Test Delete
	err = store.Delete(ctx, "agg-1")
	if err != nil {
		t.Fatalf("Failed to delete snapshot: %v", err)
	}

	_, err = store.Load(ctx, "agg-1")
	if err == nil {
		t.Error("Expected error after deleting snapshot")
	}

	// Test validation
	err = store.Save(ctx, nil)
	if err == nil {
		t.Error("Expected error saving nil snapshot")
	}

	err = store.Save(ctx, &Snapshot{})
	if err == nil {
		t.Error("Expected error saving snapshot without aggregate ID")
	}

	err = store.Delete(ctx, "")
	if err == nil {
		t.Error("Expected error deleting with empty ID")
	}

	_, err = store.Load(ctx, "")
	if err == nil {
		t.Error("Expected error loading with empty ID")
	}
}

func TestSnapshotPolicies(t *testing.T) {
	// Test EveryNEvents
	policy := EveryNEvents(50)
	if !policy.ShouldSnapshot(100, 0, time.Time{}) {
		t.Error("Expected snapshot after 100 events from version 0")
	}

	if !policy.ShouldSnapshot(100, 50, time.Time{}) {
		t.Error("Expected snapshot after 50 events from version 50")
	}

	if policy.ShouldSnapshot(100, 99, time.Time{}) {
		t.Error("Should not snapshot after only 1 event")
	}

	// Test with zero/negative N
	policy = EveryNEvents(0)
	if !policy.ShouldSnapshot(100, 99, time.Time{}) {
		t.Error("Expected snapshot with N=0 (becomes 1)")
	}

	// Test TimeInterval
	policy = TimeInterval(1 * time.Hour)
	oldTime := time.Now().Add(-2 * time.Hour)
	if !policy.ShouldSnapshot(100, 0, oldTime) {
		t.Error("Expected snapshot after 2 hours")
	}

	recentTime := time.Now().Add(-30 * time.Minute)
	if policy.ShouldSnapshot(100, 0, recentTime) {
		t.Error("Should not snapshot after only 30 minutes")
	}

	// Test Never
	policy = Never()
	if policy.ShouldSnapshot(100, 0, time.Time{}) {
		t.Error("Never policy should never snapshot")
	}

	// Test Combined
	policy = Combined(
		EveryNEvents(50),
		TimeInterval(1*time.Hour),
	)

	// Should trigger on event count
	if !policy.ShouldSnapshot(50, 0, time.Now()) {
		t.Error("Combined policy should trigger on event count")
	}

	// Should trigger on time
	oldTime = time.Now().Add(-2 * time.Hour)
	if !policy.ShouldSnapshot(10, 0, oldTime) {
		t.Error("Combined policy should trigger on time")
	}

	// Should not trigger when neither condition met
	if policy.ShouldSnapshot(10, 0, time.Now()) {
		t.Error("Combined policy should not trigger when no condition met")
	}
}

func TestSnapshotManager(t *testing.T) {
	ctx := context.Background()
	store := NewMemorySnapshotStore()
	policy := EveryNEvents(10)
	manager := NewSnapshotManager(store, policy)

	// Create test aggregate
	agg := &TestAggregate{}
	agg.ID = "test-1"
	agg.Version = 15
	agg.Balance = 1500
	agg.Name = "Test Account"

	// Should create snapshot (version 15, last snapshot at 0)
	err := manager.MaybeSnapshot(ctx, agg.ID, agg.Version, agg.CreateSnapshot)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Verify snapshot was saved
	snapshot, err := store.Load(ctx, "test-1")
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	if snapshot.Version != 15 {
		t.Errorf("Expected snapshot version 15, got %d", snapshot.Version)
	}

	// Should not create another snapshot immediately
	agg.Version = 16
	err = manager.MaybeSnapshot(ctx, agg.ID, agg.Version, agg.CreateSnapshot)
	if err != nil {
		t.Fatalf("MaybeSnapshot failed: %v", err)
	}

	// Verify still at version 15
	snapshot, err = store.Load(ctx, "test-1")
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	if snapshot.Version != 15 {
		t.Errorf("Expected snapshot version 15, got %d", snapshot.Version)
	}

	// Should create snapshot after 10 more events
	agg.Version = 25
	err = manager.MaybeSnapshot(ctx, agg.ID, agg.Version, agg.CreateSnapshot)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	snapshot, err = store.Load(ctx, "test-1")
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	if snapshot.Version != 25 {
		t.Errorf("Expected snapshot version 25, got %d", snapshot.Version)
	}

	// Test with nil store
	manager = NewSnapshotManager(nil, policy)
	err = manager.MaybeSnapshot(ctx, agg.ID, agg.Version, agg.CreateSnapshot)
	if err != nil {
		t.Errorf("MaybeSnapshot should not error with nil store: %v", err)
	}

	// Test LoadSnapshot
	manager = NewSnapshotManager(store, policy)
	loaded, err := manager.LoadSnapshot(ctx, "test-1")
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	if loaded.Version != 25 {
		t.Errorf("Expected loaded version 25, got %d", loaded.Version)
	}

	// Test LoadSnapshot with nil store
	manager = NewSnapshotManager(nil, policy)
	_, err = manager.LoadSnapshot(ctx, "test-1")
	if err == nil {
		t.Error("Expected error loading snapshot with nil store")
	}

	// Test with nil policy (should default to Never)
	manager = NewSnapshotManager(store, nil)
	agg.Version = 100
	err = manager.MaybeSnapshot(ctx, agg.ID, agg.Version, agg.CreateSnapshot)
	if err != nil {
		t.Fatalf("MaybeSnapshot failed: %v", err)
	}

	// Should still be at version 25 (no new snapshot)
	snapshot, err = store.Load(ctx, "test-1")
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	if snapshot.Version != 25 {
		t.Errorf("Expected snapshot version 25 (no new snapshot), got %d", snapshot.Version)
	}
}

func TestBaseAggregateSnapshot(t *testing.T) {
	agg := &BaseAggregate[SnapshotTestEvent]{
		ID:      "base-1",
		Version: 42,
	}

	// Test CreateSnapshot
	data, err := agg.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	var snapshot map[string]any
	err = json.Unmarshal(data, &snapshot)
	if err != nil {
		t.Fatalf("Failed to unmarshal snapshot: %v", err)
	}

	if snapshot["id"] != "base-1" {
		t.Errorf("Expected ID base-1, got %v", snapshot["id"])
	}

	if snapshot["version"] != float64(42) {
		t.Errorf("Expected version 42, got %v", snapshot["version"])
	}

	// Test RestoreFromSnapshot
	newAgg := &BaseAggregate[SnapshotTestEvent]{}
	err = newAgg.RestoreFromSnapshot(data, 42)
	if err != nil {
		t.Fatalf("Failed to restore from snapshot: %v", err)
	}

	if newAgg.ID != "base-1" {
		t.Errorf("Expected restored ID base-1, got %s", newAgg.ID)
	}

	if newAgg.Version != 42 {
		t.Errorf("Expected restored version 42, got %d", newAgg.Version)
	}

	// Test restore with invalid JSON
	err = newAgg.RestoreFromSnapshot([]byte("invalid json"), 0)
	if err == nil {
		t.Error("Expected error restoring from invalid JSON")
	}
}

func TestWithSnapshots(t *testing.T) {
	store := NewMemorySnapshotStore()
	policy := EveryNEvents(10)

	bus := New(WithSnapshots(store, policy))

	manager := GetSnapshotManager(bus)
	if manager == nil {
		t.Fatal("Expected snapshot manager to be configured")
	}

	if manager.store != store {
		t.Error("Snapshot manager has wrong store")
	}

	if manager.policy == nil {
		t.Error("Snapshot manager has no policy")
	}

	// Test GetSnapshotManager with no snapshots configured
	bus2 := New()
	manager2 := GetSnapshotManager(bus2)
	if manager2 != nil {
		t.Error("Expected nil snapshot manager when not configured")
	}

	// Test GetSnapshotManager with extensions but no snapshot manager
	bus3 := New()
	bus3.extensions = make(map[string]any)
	bus3.extensions["other"] = "value"
	manager3 := GetSnapshotManager(bus3)
	if manager3 != nil {
		t.Error("Expected nil snapshot manager when not in extensions")
	}
}

// Test error cases
func TestSnapshotErrorCases(t *testing.T) {
	ctx := context.Background()

	// Test aggregate that fails to create snapshot
	failingAgg := &FailingAggregate{}
	failingAgg.ID = "fail-1"
	failingAgg.Version = 10

	store := NewMemorySnapshotStore()
	policy := EveryNEvents(1)
	manager := NewSnapshotManager(store, policy)

	err := manager.MaybeSnapshot(ctx, failingAgg.ID, failingAgg.Version, failingAgg.CreateSnapshot)
	if err == nil {
		t.Error("Expected error when creating snapshot fails")
	}

	// Test store that fails to save
	failStore := &FailingSnapshotStore{}
	manager = NewSnapshotManager(failStore, policy)

	agg := &TestAggregate{}
	agg.ID = "test-1"
	agg.Version = 10

	err = manager.MaybeSnapshot(ctx, agg.ID, agg.Version, agg.CreateSnapshot)
	if err == nil {
		t.Error("Expected error when store fails to save")
	}
}

// FailingAggregate for testing error cases
type FailingAggregate struct {
	BaseAggregate[SnapshotTestEvent]
}

func (a *FailingAggregate) CreateSnapshot() ([]byte, error) {
	return nil, errors.New("snapshot creation failed")
}

func (a *FailingAggregate) LoadFromHistory(events []SnapshotTestEvent) error {
	return errors.New("load from history failed")
}

// FailingSnapshotStore for testing error cases
type FailingSnapshotStore struct{}

func (s *FailingSnapshotStore) Save(ctx context.Context, snapshot *Snapshot) error {
	return errors.New("save failed")
}

func (s *FailingSnapshotStore) Load(ctx context.Context, aggregateID string) (*Snapshot, error) {
	return nil, errors.New("load failed")
}

func (s *FailingSnapshotStore) Delete(ctx context.Context, aggregateID string) error {
	return errors.New("delete failed")
}

// Test LoadAggregate and SaveAggregate functions
func TestLoadSaveAggregate(t *testing.T) {
	ctx := context.Background()
	eventStore := NewMemoryStore()
	snapshotStore := NewMemorySnapshotStore()

	// Test LoadAggregate with empty ID
	_, err := LoadAggregate(ctx, "", func() *TestAggregate {
		return &TestAggregate{}
	}, eventStore, snapshotStore)
	if err == nil {
		t.Error("Expected error loading aggregate with empty ID")
	}

	// Test LoadAggregate with no stores
	agg, err := LoadAggregate(ctx, "agg-1", func() *TestAggregate {
		return &TestAggregate{}
	}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to load aggregate: %v", err)
	}

	if agg.GetID() != "" {
		t.Errorf("Expected empty ID for new aggregate, got %s", agg.GetID())
	}

	// Test SaveAggregate
	testAgg := &TestAggregate{}
	testAgg.ID = "agg-1"
	testAgg.Version = 1
	testAgg.Balance = 100
	testAgg.RaiseEvent(DepositEvent{Amount: 50})

	policy := EveryNEvents(1)
	manager := NewSnapshotManager(snapshotStore, policy)

	err = SaveAggregate(ctx, testAgg, eventStore, manager)
	if err != nil {
		t.Fatalf("Failed to save aggregate: %v", err)
	}

	// Verify events were saved
	pos, err := eventStore.GetPosition(ctx)
	if err != nil {
		t.Fatalf("Failed to get position: %v", err)
	}

	if pos != 1 {
		t.Errorf("Expected 1 event saved, got position %d", pos)
	}

	// Verify snapshot was created
	snapshot, err := snapshotStore.Load(ctx, "agg-1")
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	if snapshot.Version != 2 { // Version incremented after event
		t.Errorf("Expected snapshot version 2, got %d", snapshot.Version)
	}

	// Test SaveAggregate with failing event store
	failEventStore := &FailingEventStore{}
	testAgg.RaiseEvent(DepositEvent{Amount: 25})

	err = SaveAggregate(ctx, testAgg, failEventStore, manager)
	if err == nil {
		t.Error("Expected error when event store fails")
	}

	// Test SaveAggregate with unmarshalable event
	type unmarshalableEvent struct {
		Ch chan int
	}
	testAgg.RaiseEvent(unmarshalableEvent{Ch: make(chan int)}) // Channels can't be marshaled

	err = SaveAggregate(ctx, testAgg, eventStore, manager)
	if err == nil {
		t.Error("Expected error with unmarshalable event")
	}

	// Clear uncommitted events
	testAgg.MarkEventsAsCommitted()

	// Test SaveAggregate with failing snapshot manager (should not fail save)
	testAgg.RaiseEvent(DepositEvent{Amount: 100})

	failManager := NewSnapshotManager(&FailingSnapshotStore{}, EveryNEvents(1))
	err = SaveAggregate(ctx, testAgg, eventStore, failManager)
	if err != nil {
		t.Errorf("Save should not fail when snapshot fails: %v", err)
	}
}

// FailingEventStore for testing
type FailingEventStore struct{}

func (s *FailingEventStore) Save(ctx context.Context, event *StoredEvent) error {
	return errors.New("save failed")
}

func (s *FailingEventStore) Load(ctx context.Context, from, to int64) ([]*StoredEvent, error) {
	return nil, errors.New("load failed")
}

func (s *FailingEventStore) GetPosition(ctx context.Context) (int64, error) {
	return 0, errors.New("get position failed")
}

func (s *FailingEventStore) SaveSubscriptionPosition(ctx context.Context, subscriptionID string, position int64) error {
	return errors.New("save subscription position failed")
}

func (s *FailingEventStore) LoadSubscriptionPosition(ctx context.Context, subscriptionID string) (int64, error) {
	return 0, errors.New("load subscription position failed")
}

// Test LoadAggregate with snapshot and events
func TestLoadAggregateWithSnapshot(t *testing.T) {
	ctx := context.Background()
	eventStore := NewMemoryStore()
	snapshotStore := NewMemorySnapshotStore()

	// Create and save a snapshot
	snapshot := &Snapshot{
		AggregateID:   "agg-1",
		AggregateType: "TestAggregate",
		Version:       10,
		Data:          json.RawMessage(`{"id": "agg-1", "version": 10, "balance": 1000, "name": "Test"}`),
		Timestamp:     time.Now(),
	}

	err := snapshotStore.Save(ctx, snapshot)
	if err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Save some events after the snapshot (positions will be 1-5)
	// But we need events with positions > 10 to be loaded after the snapshot
	// So let's add dummy events first to get to position 10
	for i := 0; i < 10; i++ {
		dummyEvent := map[string]any{
			"aggregate_id": "dummy",
			"type":         "dummy",
		}
		dummyData, _ := json.Marshal(dummyEvent)
		dummyStored := &StoredEvent{
			Type:      "dummy",
			Data:      dummyData,
			Timestamp: time.Now(),
		}
		_ = eventStore.Save(ctx, dummyStored)
	}

	// Now save the actual events after position 10
	for i := 0; i < 5; i++ {
		event := map[string]any{
			"aggregate_id": "agg-1",
			"type":         "deposit",
			"amount":       float64(100),
		}

		eventData, _ := json.Marshal(event)
		storedEvent := &StoredEvent{
			Type:      "deposit",
			Data:      eventData,
			Timestamp: time.Now(),
		}

		err = eventStore.Save(ctx, storedEvent)
		if err != nil {
			t.Fatalf("Failed to save event: %v", err)
		}
	}

	// Load aggregate with snapshot and events
	agg, err := LoadAggregate(ctx, "agg-1", func() *TestAggregate {
		return &TestAggregate{}
	}, eventStore, snapshotStore)
	if err != nil {
		t.Fatalf("Failed to load aggregate: %v", err)
	}

	testAgg := agg
	if testAgg.ID != "agg-1" {
		t.Errorf("Expected ID agg-1, got %s", testAgg.ID)
	}

	if testAgg.Version != 15 { // 10 from snapshot + 5 events
		t.Errorf("Expected version 15, got %d", testAgg.Version)
	}

	if testAgg.Balance != 1500 { // 1000 from snapshot + 5*100
		t.Errorf("Expected balance 1500, got %f", testAgg.Balance)
	}

	// Test loading with snapshot restore failure
	badSnapshot := &Snapshot{
		AggregateID:   "agg-2",
		AggregateType: "TestAggregate",
		Version:       5,
		Data:          json.RawMessage(`invalid json`),
		Timestamp:     time.Now(),
	}

	err = snapshotStore.Save(ctx, badSnapshot)
	if err != nil {
		t.Fatalf("Failed to save bad snapshot: %v", err)
	}

	_, err = LoadAggregate(ctx, "agg-2", func() *TestAggregate {
		return &TestAggregate{}
	}, eventStore, snapshotStore)
	if err == nil {
		t.Error("Expected error with invalid snapshot JSON")
	}

	// Test loading with event store failure
	_, err = LoadAggregate(ctx, "agg-3", func() *TestAggregate {
		return &TestAggregate{}
	}, &FailingEventStore{}, snapshotStore)
	if err == nil {
		t.Error("Expected error with failing event store")
	}

	// Test loading with LoadFromHistory failure
	_, err = LoadAggregate(ctx, "agg-4", func() *FailingAggregate {
		return &FailingAggregate{}
	}, eventStore, nil)
	// This should not fail because there are no events for agg-4
	if err != nil {
		t.Errorf("Should not fail when no events to load: %v", err)
	}

	// Test loading with invalid JSON in events (tests the continue path)
	invalidEvent := &StoredEvent{
		Type:      "test",
		Data:      json.RawMessage(`{invalid json`),
		Timestamp: time.Now(),
	}
	_ = eventStore.Save(ctx, invalidEvent)

	// This should skip the invalid event and continue
	agg5, err := LoadAggregate(ctx, "agg-5", func() *TestAggregate {
		return &TestAggregate{}
	}, eventStore, nil)
	if err != nil {
		t.Fatalf("Should not fail with invalid events in store: %v", err)
	}

	if agg5.GetID() != "" {
		t.Errorf("Expected empty ID for new aggregate, got %s", agg5.GetID())
	}

	// Test loading with FailingAggregate when there ARE matching events
	// This tests the LoadFromHistory error path
	failEvent := map[string]any{
		"aggregate_id": "fail-agg",
		"type":         "test",
	}
	failEventData, _ := json.Marshal(failEvent)
	failStoredEvent := &StoredEvent{
		Type:      "test",
		Data:      failEventData,
		Timestamp: time.Now(),
	}
	_ = eventStore.Save(ctx, failStoredEvent)

	_, err = LoadAggregate(ctx, "fail-agg", func() *FailingAggregate {
		return &FailingAggregate{}
	}, eventStore, nil)
	if err == nil {
		t.Error("Expected error when LoadFromHistory fails with events")
	}
}
