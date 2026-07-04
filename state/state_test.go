package state

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

// Test entity types
type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type Product struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

// TypeNamer implementation for stable type names
type NamedUser struct {
	Name string `json:"name"`
}

func (u NamedUser) StateTypeName() string { return "user" }

// ================== Message Type Tests ==================

func TestEntityType(t *testing.T) {
	t.Run("without TypeNamer", func(t *testing.T) {
		user := User{Name: "Alice"}
		got := EntityType(user)
		want := "state.User"
		if got != want {
			t.Errorf("EntityType() = %q, want %q", got, want)
		}
	})

	t.Run("with TypeNamer", func(t *testing.T) {
		user := NamedUser{Name: "Alice"}
		got := EntityType(user)
		want := "user"
		if got != want {
			t.Errorf("EntityType() = %q, want %q", got, want)
		}
	})
}

func TestCompositeKey(t *testing.T) {
	got := CompositeKey("user", "123")
	want := "user/123"
	if got != want {
		t.Errorf("CompositeKey() = %q, want %q", got, want)
	}
}

func TestEntityTypeWithNil(t *testing.T) {
	got := EntityType(nil)
	want := "nil"
	if got != want {
		t.Errorf("EntityType(nil) = %q, want %q", got, want)
	}
}

func TestChangeMessageEventTypeName(t *testing.T) {
	msg := ChangeMessage{}
	got := msg.EventTypeName()
	want := "state.ChangeMessage"
	if got != want {
		t.Errorf("EventTypeName() = %q, want %q", got, want)
	}
}

func TestControlMessageEventTypeName(t *testing.T) {
	msg := ControlMessage{}
	got := msg.EventTypeName()
	want := "state.ControlMessage"
	if got != want {
		t.Errorf("EventTypeName() = %q, want %q", got, want)
	}
}

// ================== Helper Function Tests ==================

func TestInsert(t *testing.T) {
	user := User{Name: "Alice", Email: "alice@example.com"}
	msg, err := Insert("user:1", user)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	if msg.Type != "state.User" {
		t.Errorf("Type = %q, want %q", msg.Type, "state.User")
	}
	if msg.Key != "user:1" {
		t.Errorf("Key = %q, want %q", msg.Key, "user:1")
	}
	if msg.Headers.Operation != OperationInsert {
		t.Errorf("Operation = %q, want %q", msg.Headers.Operation, OperationInsert)
	}
	if msg.Value == nil {
		t.Error("Value should not be nil")
	}
	if msg.OldValue != nil {
		t.Error("OldValue should be nil")
	}

	// Verify value can be unmarshaled
	var decoded User
	if err := json.Unmarshal(msg.Value, &decoded); err != nil {
		t.Errorf("Unmarshal value: %v", err)
	}
	if decoded.Name != user.Name {
		t.Errorf("decoded.Name = %q, want %q", decoded.Name, user.Name)
	}
}

func TestInsertWithTypeName(t *testing.T) {
	user := NamedUser{Name: "Alice"}
	msg, err := Insert("user:1", user)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	if msg.Type != "user" {
		t.Errorf("Type = %q, want %q", msg.Type, "user")
	}
}

func TestInsertWithOptions(t *testing.T) {
	user := User{Name: "Alice"}
	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	msg, err := Insert("user:1", user,
		WithTxID("tx-123"),
		WithTimestamp(ts),
		WithEntityType("custom.User"),
	)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	if msg.Type != "custom.User" {
		t.Errorf("Type = %q, want %q", msg.Type, "custom.User")
	}
	if msg.Headers.TxID != "tx-123" {
		t.Errorf("TxID = %q, want %q", msg.Headers.TxID, "tx-123")
	}
	if msg.Headers.Timestamp != "2025-01-15T10:30:00Z" {
		t.Errorf("Timestamp = %q, want %q", msg.Headers.Timestamp, "2025-01-15T10:30:00Z")
	}
}

func TestInsertWithAutoTimestamp(t *testing.T) {
	before := time.Now().UTC()
	msg, err := Insert("user:1", User{Name: "Alice"}, WithAutoTimestamp())
	after := time.Now().UTC()

	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	if msg.Headers.Timestamp == "" {
		t.Error("Timestamp should be set")
	}

	ts, err := time.Parse(time.RFC3339Nano, msg.Headers.Timestamp)
	if err != nil {
		t.Errorf("Parse timestamp: %v", err)
	}
	if ts.Before(before) || ts.After(after) {
		t.Errorf("Timestamp %v not between %v and %v", ts, before, after)
	}
}

func TestInsertEmptyKey(t *testing.T) {
	_, err := Insert("", User{Name: "Alice"})
	if err == nil {
		t.Error("Insert() with empty key should return error")
	}
}

func TestUpdate(t *testing.T) {
	user := User{Name: "Alice Smith", Email: "alice.smith@example.com"}
	msg, err := Update("user:1", user)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	if msg.Headers.Operation != OperationUpdate {
		t.Errorf("Operation = %q, want %q", msg.Headers.Operation, OperationUpdate)
	}
	if msg.Value == nil {
		t.Error("Value should not be nil")
	}
	if msg.OldValue != nil {
		t.Error("OldValue should be nil")
	}
}

func TestUpdateWithOldValue(t *testing.T) {
	oldUser := User{Name: "Alice", Email: "alice@example.com"}
	newUser := User{Name: "Alice Smith", Email: "alice.smith@example.com"}
	msg, err := UpdateWithOldValue("user:1", newUser, oldUser)
	if err != nil {
		t.Fatalf("UpdateWithOldValue() error = %v", err)
	}

	if msg.Headers.Operation != OperationUpdate {
		t.Errorf("Operation = %q, want %q", msg.Headers.Operation, OperationUpdate)
	}
	if msg.Value == nil {
		t.Error("Value should not be nil")
	}
	if msg.OldValue == nil {
		t.Error("OldValue should not be nil")
	}

	var decoded User
	if err := json.Unmarshal(msg.OldValue, &decoded); err != nil {
		t.Errorf("Unmarshal old_value: %v", err)
	}
	if decoded.Name != oldUser.Name {
		t.Errorf("decoded.Name = %q, want %q", decoded.Name, oldUser.Name)
	}
}

func TestDelete(t *testing.T) {
	msg, err := Delete[User]("user:1")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if msg.Type != "state.User" {
		t.Errorf("Type = %q, want %q", msg.Type, "state.User")
	}
	if msg.Key != "user:1" {
		t.Errorf("Key = %q, want %q", msg.Key, "user:1")
	}
	if msg.Headers.Operation != OperationDelete {
		t.Errorf("Operation = %q, want %q", msg.Headers.Operation, OperationDelete)
	}
	if msg.Value != nil {
		t.Error("Value should be nil for delete")
	}
	if msg.OldValue != nil {
		t.Error("OldValue should be nil")
	}
}

func TestDeleteWithOldValue(t *testing.T) {
	user := User{Name: "Alice"}
	msg, err := DeleteWithOldValue("user:1", user)
	if err != nil {
		t.Fatalf("DeleteWithOldValue() error = %v", err)
	}

	if msg.Headers.Operation != OperationDelete {
		t.Errorf("Operation = %q, want %q", msg.Headers.Operation, OperationDelete)
	}
	if msg.Value != nil {
		t.Error("Value should be nil for delete")
	}
	if msg.OldValue == nil {
		t.Error("OldValue should not be nil")
	}
}

func TestSnapshotStart(t *testing.T) {
	msg := SnapshotStart("offset-123")
	if msg.Headers.Control != ControlSnapshotStart {
		t.Errorf("Control = %q, want %q", msg.Headers.Control, ControlSnapshotStart)
	}
	if msg.Headers.Offset != "offset-123" {
		t.Errorf("Offset = %q, want %q", msg.Headers.Offset, "offset-123")
	}
}

func TestSnapshotEnd(t *testing.T) {
	msg := SnapshotEnd("offset-456")
	if msg.Headers.Control != ControlSnapshotEnd {
		t.Errorf("Control = %q, want %q", msg.Headers.Control, ControlSnapshotEnd)
	}
	if msg.Headers.Offset != "offset-456" {
		t.Errorf("Offset = %q, want %q", msg.Headers.Offset, "offset-456")
	}
}

func TestReset(t *testing.T) {
	msg := Reset("offset-789")
	if msg.Headers.Control != ControlReset {
		t.Errorf("Control = %q, want %q", msg.Headers.Control, ControlReset)
	}
	if msg.Headers.Offset != "offset-789" {
		t.Errorf("Offset = %q, want %q", msg.Headers.Offset, "offset-789")
	}
}

// ================== MemoryStore Tests ==================

func TestMemoryStore(t *testing.T) {
	store := NewMemoryStore[User]()

	// Test Set and Get
	user := User{Name: "Alice", Email: "alice@example.com"}
	store.Set("user/1", user)

	got, ok := store.Get("user/1")
	if !ok {
		t.Error("Get() should find user")
	}
	if got.Name != user.Name {
		t.Errorf("Name = %q, want %q", got.Name, user.Name)
	}

	// Test Get non-existent
	_, ok = store.Get("user/999")
	if ok {
		t.Error("Get() should not find non-existent user")
	}

	// Test Delete
	store.Delete("user/1")
	_, ok = store.Get("user/1")
	if ok {
		t.Error("Get() should not find deleted user")
	}

	// Test All
	store.Set("user/1", User{Name: "Alice"})
	store.Set("user/2", User{Name: "Bob"})
	all := store.All()
	if len(all) != 2 {
		t.Errorf("All() returned %d items, want 2", len(all))
	}

	// Test Clear
	store.Clear()
	all = store.All()
	if len(all) != 0 {
		t.Errorf("All() returned %d items after Clear, want 0", len(all))
	}
}

// ================== TypedCollection Tests ==================

func TestTypedCollection(t *testing.T) {
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)

	if users.EntityType() != "state.User" {
		t.Errorf("EntityType() = %q, want %q", users.EntityType(), "state.User")
	}

	// Store uses composite keys
	store.Set(CompositeKey("state.User", "1"), User{Name: "Alice"})

	// Collection uses simple keys
	user, ok := users.Get("1")
	if !ok {
		t.Error("Get() should find user")
	}
	if user.Name != "Alice" {
		t.Errorf("Name = %q, want %q", user.Name, "Alice")
	}
}

func TestTypedCollectionWithType(t *testing.T) {
	store := NewMemoryStore[User]()
	users := NewTypedCollectionWithType[User](store, "custom.User")

	if users.EntityType() != "custom.User" {
		t.Errorf("EntityType() = %q, want %q", users.EntityType(), "custom.User")
	}
}

// ================== Materializer Tests ==================

func TestMaterializerApplyInsert(t *testing.T) {
	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	// Create insert message
	insertData, _ := json.Marshal(&ChangeMessage{
		Type:    "state.User",
		Key:     "1",
		Value:   json.RawMessage(`{"name":"Alice","email":"alice@example.com"}`),
		Headers: Headers{Operation: OperationInsert},
	})

	event := &eventbus.StoredEvent{
		Offset: "1",
		Type:   "state.ChangeMessage",
		Data:   insertData,
	}

	if err := mat.Apply(event); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	user, ok := users.Get("1")
	if !ok {
		t.Error("User not found after insert")
	}
	if user.Name != "Alice" {
		t.Errorf("Name = %q, want %q", user.Name, "Alice")
	}
}

func TestMaterializerApplyUpdate(t *testing.T) {
	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	// Insert first
	store.Set(CompositeKey("state.User", "1"), User{Name: "Alice"})

	// Apply update
	updateData, _ := json.Marshal(&ChangeMessage{
		Type:    "state.User",
		Key:     "1",
		Value:   json.RawMessage(`{"name":"Alice Smith","email":"alice.smith@example.com"}`),
		Headers: Headers{Operation: OperationUpdate},
	})

	event := &eventbus.StoredEvent{
		Offset: "2",
		Data:   updateData,
	}

	if err := mat.Apply(event); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	user, ok := users.Get("1")
	if !ok {
		t.Error("User not found after update")
	}
	if user.Name != "Alice Smith" {
		t.Errorf("Name = %q, want %q", user.Name, "Alice Smith")
	}
}

func TestMaterializerApplyDelete(t *testing.T) {
	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	// Insert first
	store.Set(CompositeKey("state.User", "1"), User{Name: "Alice"})

	// Apply delete
	deleteData, _ := json.Marshal(&ChangeMessage{
		Type:    "state.User",
		Key:     "1",
		Headers: Headers{Operation: OperationDelete},
	})

	event := &eventbus.StoredEvent{
		Offset: "2",
		Data:   deleteData,
	}

	if err := mat.Apply(event); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	_, ok := users.Get("1")
	if ok {
		t.Error("User should not exist after delete")
	}
}

func TestMaterializerApplyReset(t *testing.T) {
	resetCalled := false
	mat := NewMaterializer(WithOnReset(func() {
		resetCalled = true
	}))

	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	// Insert some data
	store.Set(CompositeKey("state.User", "1"), User{Name: "Alice"})
	store.Set(CompositeKey("state.User", "2"), User{Name: "Bob"})

	// Apply reset
	resetData, _ := json.Marshal(&ControlMessage{
		Headers: ControlHeaders{Control: ControlReset},
	})

	event := &eventbus.StoredEvent{
		Offset: "3",
		Data:   resetData,
	}

	if err := mat.Apply(event); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	if !resetCalled {
		t.Error("OnReset callback was not called")
	}

	all := users.All()
	if len(all) != 0 {
		t.Errorf("Store should be empty after reset, got %d items", len(all))
	}
}

func TestMaterializerApplySnapshot(t *testing.T) {
	snapshotStarts := 0
	snapshotEnds := 0
	mat := NewMaterializer(WithOnSnapshot(func(start bool) {
		if start {
			snapshotStarts++
		} else {
			snapshotEnds++
		}
	}))

	// Apply snapshot-start
	startData, _ := json.Marshal(&ControlMessage{
		Headers: ControlHeaders{Control: ControlSnapshotStart, Offset: "1"},
	})
	event1 := &eventbus.StoredEvent{Offset: "1", Data: startData}
	if err := mat.Apply(event1); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	// Apply snapshot-end
	endData, _ := json.Marshal(&ControlMessage{
		Headers: ControlHeaders{Control: ControlSnapshotEnd, Offset: "10"},
	})
	event2 := &eventbus.StoredEvent{Offset: "10", Data: endData}
	if err := mat.Apply(event2); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	if snapshotStarts != 1 {
		t.Errorf("snapshotStarts = %d, want 1", snapshotStarts)
	}
	if snapshotEnds != 1 {
		t.Errorf("snapshotEnds = %d, want 1", snapshotEnds)
	}
}

func TestMaterializerUnknownType(t *testing.T) {
	t.Run("non-strict mode ignores unknown types", func(t *testing.T) {
		mat := NewMaterializer()

		insertData, _ := json.Marshal(&ChangeMessage{
			Type:    "unknown.Type",
			Key:     "1",
			Value:   json.RawMessage(`{"foo":"bar"}`),
			Headers: Headers{Operation: OperationInsert},
		})

		event := &eventbus.StoredEvent{Offset: "1", Data: insertData}
		err := mat.Apply(event)
		if err != nil {
			t.Errorf("Apply() should not error in non-strict mode, got %v", err)
		}
	})

	t.Run("strict mode errors on unknown types", func(t *testing.T) {
		mat := NewMaterializer(WithStrictSchema())

		insertData, _ := json.Marshal(&ChangeMessage{
			Type:    "unknown.Type",
			Key:     "1",
			Value:   json.RawMessage(`{"foo":"bar"}`),
			Headers: Headers{Operation: OperationInsert},
		})

		event := &eventbus.StoredEvent{Offset: "1", Data: insertData}
		err := mat.Apply(event)
		if err == nil {
			t.Error("Apply() should error in strict mode for unknown types")
		}
	})
}

func TestMaterializerOnError(t *testing.T) {
	var capturedError error
	mat := NewMaterializer(WithOnError(func(err error) {
		capturedError = err
	}))

	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	// Create event with value that is valid JSON but cannot be unmarshaled to User
	// The value is a string instead of an object, which will fail unmarshal to User struct
	insertData := []byte(`{"type":"state.User","key":"1","value":"not an object","headers":{"operation":"insert"}}`)

	event := &eventbus.StoredEvent{Offset: "1", Data: insertData}
	err := mat.Apply(event)
	if err == nil {
		t.Error("Apply() should error when value cannot be unmarshaled to target type")
	}
	if capturedError == nil {
		t.Error("OnError callback was not called")
	}
}

func TestMaterializerLastOffset(t *testing.T) {
	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	insertData, _ := json.Marshal(&ChangeMessage{
		Type:    "state.User",
		Key:     "1",
		Value:   json.RawMessage(`{"name":"Alice"}`),
		Headers: Headers{Operation: OperationInsert},
	})

	event := &eventbus.StoredEvent{Offset: "42", Data: insertData}
	if err := mat.Apply(event); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	if mat.LastOffset() != "42" {
		t.Errorf("LastOffset() = %q, want %q", mat.LastOffset(), "42")
	}
}

func TestMaterializerApplyChangeMessageDirectly(t *testing.T) {
	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	msg, _ := Insert("1", User{Name: "Alice"})
	if err := mat.ApplyChangeMessage(msg); err != nil {
		t.Fatalf("ApplyChangeMessage() error = %v", err)
	}

	user, ok := users.Get("1")
	if !ok {
		t.Error("User not found after insert")
	}
	if user.Name != "Alice" {
		t.Errorf("Name = %q, want %q", user.Name, "Alice")
	}
}

func TestMaterializerApplyControlMessageDirectly(t *testing.T) {
	resetCalled := false
	mat := NewMaterializer(WithOnReset(func() {
		resetCalled = true
	}))

	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	store.Set(CompositeKey("state.User", "1"), User{Name: "Alice"})

	msg := Reset("offset-1")
	mat.ApplyControlMessage(msg)

	if !resetCalled {
		t.Error("OnReset callback was not called")
	}

	all := users.All()
	if len(all) != 0 {
		t.Errorf("Store should be empty after reset, got %d items", len(all))
	}
}

// ================== Integration Tests ==================

func TestIntegrationWithEventBus(t *testing.T) {
	// Create event bus with memory store
	ebuStore := eventbus.NewMemoryStore()
	bus := eventbus.New(eventbus.WithStore(ebuStore))

	// Publish some state change events
	ctx := context.Background()

	msg1, _ := Insert("1", User{Name: "Alice", Email: "alice@example.com"})
	eventbus.Publish(bus, msg1)

	msg2, _ := Insert("2", User{Name: "Bob", Email: "bob@example.com"})
	eventbus.Publish(bus, msg2)

	msg3, _ := Update("1", User{Name: "Alice Smith", Email: "alice.smith@example.com"})
	eventbus.Publish(bus, msg3)

	msg4, _ := Delete[User]("2")
	eventbus.Publish(bus, msg4)

	// Create materializer and replay
	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	if err := mat.Replay(ctx, bus, eventbus.OffsetOldest); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	// Verify final state
	user1, ok := users.Get("1")
	if !ok {
		t.Error("User 1 should exist")
	}
	if user1.Name != "Alice Smith" {
		t.Errorf("User 1 name = %q, want %q", user1.Name, "Alice Smith")
	}

	_, ok = users.Get("2")
	if ok {
		t.Error("User 2 should not exist (was deleted)")
	}
}

func TestIntegrationMultipleCollections(t *testing.T) {
	ebuStore := eventbus.NewMemoryStore()
	bus := eventbus.New(eventbus.WithStore(ebuStore))

	ctx := context.Background()

	// Publish users
	userMsg, _ := Insert("1", User{Name: "Alice"}, WithEntityType("user"))
	eventbus.Publish(bus, userMsg)

	// Publish products
	productMsg, _ := Insert("p1", Product{ID: "p1", Name: "Widget", Price: 9.99}, WithEntityType("product"))
	eventbus.Publish(bus, productMsg)

	// Create materializer with multiple collections
	mat := NewMaterializer()

	userStore := NewMemoryStore[User]()
	users := NewTypedCollectionWithType[User](userStore, "user")
	RegisterCollection(mat, users)

	productStore := NewMemoryStore[Product]()
	products := NewTypedCollectionWithType[Product](productStore, "product")
	RegisterCollection(mat, products)

	if err := mat.Replay(ctx, bus, eventbus.OffsetOldest); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	// Verify both collections
	user, ok := users.Get("1")
	if !ok {
		t.Error("User should exist")
	}
	if user.Name != "Alice" {
		t.Errorf("User name = %q, want %q", user.Name, "Alice")
	}

	product, ok := products.Get("p1")
	if !ok {
		t.Error("Product should exist")
	}
	if product.Name != "Widget" {
		t.Errorf("Product name = %q, want %q", product.Name, "Widget")
	}
}

// ================== JSON Conformance Tests ==================

func TestJSONConformance(t *testing.T) {
	t.Run("change message serialization", func(t *testing.T) {
		msg, _ := Insert("user:1", User{Name: "Alice"}, WithTxID("tx-1"))
		data, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}

		// Verify expected fields are present
		var raw map[string]interface{}
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if raw["type"] != "state.User" {
			t.Errorf("type = %v, want state.User", raw["type"])
		}
		if raw["key"] != "user:1" {
			t.Errorf("key = %v, want user:1", raw["key"])
		}

		headers, ok := raw["headers"].(map[string]interface{})
		if !ok {
			t.Fatal("headers should be an object")
		}
		if headers["operation"] != "insert" {
			t.Errorf("operation = %v, want insert", headers["operation"])
		}
		if headers["txid"] != "tx-1" {
			t.Errorf("txid = %v, want tx-1", headers["txid"])
		}
	})

	t.Run("control message serialization", func(t *testing.T) {
		msg := Reset("offset-123")
		data, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}

		var raw map[string]interface{}
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		headers, ok := raw["headers"].(map[string]interface{})
		if !ok {
			t.Fatal("headers should be an object")
		}
		if headers["control"] != "reset" {
			t.Errorf("control = %v, want reset", headers["control"])
		}
		if headers["offset"] != "offset-123" {
			t.Errorf("offset = %v, want offset-123", headers["offset"])
		}
	})

	t.Run("old_value uses snake_case", func(t *testing.T) {
		msg, _ := UpdateWithOldValue("user:1",
			User{Name: "New"},
			User{Name: "Old"},
		)
		data, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}

		// Check that old_value (snake_case) is used, not oldValue
		if string(data) == "" {
			t.Fatal("data should not be empty")
		}

		var raw map[string]interface{}
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if _, ok := raw["old_value"]; !ok {
			t.Error("old_value field should be present")
		}
		if _, ok := raw["oldValue"]; ok {
			t.Error("oldValue (camelCase) should not be present")
		}
	})
}

// ================== Concurrent Access Tests ==================

func TestMemoryStoreConcurrentAccess(t *testing.T) {
	store := NewMemoryStore[User]()
	done := make(chan bool)

	// Concurrent writes
	go func() {
		for i := 0; i < 100; i++ {
			store.Set("user/1", User{Name: "Alice"})
		}
		done <- true
	}()

	// Concurrent reads
	go func() {
		for i := 0; i < 100; i++ {
			store.Get("user/1")
		}
		done <- true
	}()

	// Concurrent All()
	go func() {
		for i := 0; i < 100; i++ {
			store.All()
		}
		done <- true
	}()

	// Wait for all goroutines
	<-done
	<-done
	<-done
}

func TestMaterializerConcurrentAccess(t *testing.T) {
	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	done := make(chan bool)
	errChan := make(chan error, 100)

	// Concurrent applies
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				msg, _ := Insert(string(rune('0'+id)), User{Name: "User"})
				if err := mat.ApplyChangeMessage(msg); err != nil {
					errChan <- err
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	close(errChan)
	for err := range errChan {
		t.Errorf("Concurrent apply error: %v", err)
	}
}

// ================== Error Case Tests ==================

type unmarshalableType struct {
	Ch chan int // Channels cannot be marshaled to JSON
}

func TestInsertMarshalError(t *testing.T) {
	_, err := Insert("key", unmarshalableType{Ch: make(chan int)})
	if err == nil {
		t.Error("Insert() should fail for unmarshalable type")
	}
}

func TestUpdateWithOldValueMarshalError(t *testing.T) {
	_, err := UpdateWithOldValue("key",
		unmarshalableType{Ch: make(chan int)},
		unmarshalableType{Ch: make(chan int)},
	)
	if err == nil {
		t.Error("UpdateWithOldValue() should fail for unmarshalable old value")
	}
}

type customError struct{}

func (e customError) Error() string { return "custom error" }

func TestMaterializerErrorPropagation(t *testing.T) {
	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	// Invalid JSON in value
	insertData, _ := json.Marshal(&ChangeMessage{
		Type:    "state.User",
		Key:     "1",
		Value:   json.RawMessage(`{invalid`),
		Headers: Headers{Operation: OperationInsert},
	})

	event := &eventbus.StoredEvent{Offset: "1", Data: insertData}
	err := mat.Apply(event)
	if err == nil {
		t.Error("Apply() should return error for invalid JSON")
	}

	// Verify it's wrapped correctly
	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Errorf("Error should wrap json.SyntaxError, got %T", err)
	}
}

func TestDeleteWithOldValueMarshalError(t *testing.T) {
	_, err := DeleteWithOldValue("key", unmarshalableType{Ch: make(chan int)})
	if err == nil {
		t.Error("DeleteWithOldValue() should fail for unmarshalable old value")
	}
}

func TestApplyInvalidRootJSON(t *testing.T) {
	mat := NewMaterializer()

	// Completely invalid JSON at root level
	event := &eventbus.StoredEvent{Offset: "1", Data: []byte(`{invalid`)}
	err := mat.Apply(event)
	if err == nil {
		t.Error("Apply() should error on invalid root JSON")
	}
}

// ================== Serialized Application Tests ==================

// blockingUserStore wraps MemoryStore so a test can pause inside Set and
// provoke the reset/apply race deterministically. It supports exactly one Set.
type blockingUserStore struct {
	*MemoryStore[User]
	setStarted chan struct{}
	release    chan struct{}
}

func (s *blockingUserStore) Set(key string, value User) {
	close(s.setStarted)
	<-s.release
	s.MemoryStore.Set(key, value)
}

func TestMaterializerResetApplyRace(t *testing.T) {
	store := &blockingUserStore{
		MemoryStore: NewMemoryStore[User](),
		setStarted:  make(chan struct{}),
		release:     make(chan struct{}),
	}
	mat := NewMaterializer()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	msg, _ := Insert("1", User{Name: "Alice"})

	applyDone := make(chan error, 1)
	go func() {
		applyDone <- mat.ApplyChangeMessage(msg)
	}()

	// Wait until the insert is inside Set, i.e. past the collection lookup.
	<-store.setStarted

	// Apply a reset concurrently. Serialized application forces it to wait
	// for the in-flight insert and then clear its result; without
	// serialization the logically-earlier insert would land after the clear.
	resetDone := make(chan struct{})
	go func() {
		mat.ApplyControlMessage(Reset("offset-1"))
		close(resetDone)
	}()

	// Give the reset a chance to run before the insert completes; under the
	// old unserialized behavior it would clear here and be overwritten.
	time.Sleep(20 * time.Millisecond)
	close(store.release)

	if err := <-applyDone; err != nil {
		t.Fatalf("ApplyChangeMessage() error = %v", err)
	}
	<-resetDone

	if got := len(users.All()); got != 0 {
		t.Errorf("store has %d entities after reset, want 0", got)
	}
}

func TestMaterializerUnknownOperation(t *testing.T) {
	errCount := 0
	mat := NewMaterializer(WithOnError(func(err error) {
		errCount++
	}))
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	goodData, _ := json.Marshal(&ChangeMessage{
		Type:    "state.User",
		Key:     "1",
		Value:   json.RawMessage(`{"name":"Alice"}`),
		Headers: Headers{Operation: OperationInsert},
	})
	if err := mat.Apply(&eventbus.StoredEvent{Offset: "1", Data: goodData}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	badData, _ := json.Marshal(&ChangeMessage{
		Type:    "state.User",
		Key:     "1",
		Headers: Headers{Operation: "upsert"},
	})
	err := mat.Apply(&eventbus.StoredEvent{Offset: "2", Data: badData})
	if err == nil {
		t.Fatal("Apply() should error for unknown operation")
	}
	if !strings.Contains(err.Error(), "upsert") {
		t.Errorf("error should name the bad operation, got %v", err)
	}
	if errCount != 1 {
		t.Errorf("onError called %d times, want 1", errCount)
	}
	if mat.LastOffset() != "1" {
		t.Errorf("LastOffset() = %q after failed event, want %q", mat.LastOffset(), "1")
	}
}

func TestMaterializerOnErrorAllPaths(t *testing.T) {
	newCounting := func(opts ...MaterializerOption) (*Materializer, *int) {
		count := 0
		opts = append(opts, WithOnError(func(err error) { count++ }))
		return NewMaterializer(opts...), &count
	}

	t.Run("envelope unmarshal failure", func(t *testing.T) {
		mat, count := newCounting()
		err := mat.Apply(&eventbus.StoredEvent{Offset: "1", Data: []byte(`{invalid`)})
		if err == nil {
			t.Fatal("Apply() should error on invalid root JSON")
		}
		if *count != 1 {
			t.Errorf("onError called %d times, want 1", *count)
		}
	})

	t.Run("change message unmarshal failure", func(t *testing.T) {
		mat, count := newCounting()
		event := &eventbus.StoredEvent{
			Offset: "1",
			Data:   []byte(`{"headers":{"operation":"insert"},"type":123,"key":"1"}`),
		}
		if err := mat.Apply(event); err == nil {
			t.Fatal("Apply() should error on malformed change message")
		}
		if *count != 1 {
			t.Errorf("onError called %d times, want 1", *count)
		}
	})

	t.Run("strict unknown entity type", func(t *testing.T) {
		mat, count := newCounting(WithStrictSchema())
		data, _ := json.Marshal(&ChangeMessage{
			Type:    "unknown.Type",
			Key:     "1",
			Headers: Headers{Operation: OperationInsert},
		})
		if err := mat.Apply(&eventbus.StoredEvent{Offset: "1", Data: data}); err == nil {
			t.Fatal("Apply() should error on unknown type in strict mode")
		}
		if *count != 1 {
			t.Errorf("onError called %d times, want 1", *count)
		}
		if mat.LastOffset() != eventbus.OffsetOldest {
			t.Errorf("LastOffset() = %q after failed event, want empty", mat.LastOffset())
		}
	})
}

// ================== Mixed Stream Tests ==================

func TestMaterializerMixedStreamSkip(t *testing.T) {
	run := func(t *testing.T, opts ...MaterializerOption) {
		mat := NewMaterializer(opts...)
		store := NewMemoryStore[User]()
		users := NewTypedCollection[User](store)
		RegisterCollection(mat, users)

		// A regular (non-state) event: no headers field.
		event := &eventbus.StoredEvent{Offset: "7", Data: []byte(`{"name":"Plain","email":"p@example.com"}`)}
		if err := mat.Apply(event); err != nil {
			t.Fatalf("Apply() should skip non-state events, got %v", err)
		}
		if got := len(users.All()); got != 0 {
			t.Errorf("store has %d entities after skipped event, want 0", got)
		}
		if mat.LastOffset() != "7" {
			t.Errorf("LastOffset() = %q, want %q (skipped events still advance)", mat.LastOffset(), "7")
		}
	}

	t.Run("strict mode", func(t *testing.T) { run(t, WithStrictSchema()) })
	t.Run("non-strict mode", func(t *testing.T) { run(t) })
}

func TestIntegrationMixedStreamStrict(t *testing.T) {
	ebuStore := eventbus.NewMemoryStore()
	bus := eventbus.New(eventbus.WithStore(ebuStore))

	// Interleave regular events with state messages on the same stream.
	eventbus.Publish(bus, User{Name: "Plain"})
	msg, _ := Insert("1", User{Name: "Alice"})
	eventbus.Publish(bus, msg)
	eventbus.Publish(bus, Product{ID: "p1", Name: "Widget"})

	mat := NewMaterializer(WithStrictSchema())
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	if err := mat.Replay(context.Background(), bus, eventbus.OffsetOldest); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	user, ok := users.Get("1")
	if !ok {
		t.Fatal("User 1 should be materialized")
	}
	if user.Name != "Alice" {
		t.Errorf("Name = %q, want %q", user.Name, "Alice")
	}
}

// ================== Upcast Tests ==================

type legacyUserInsert struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (legacyUserInsert) EventTypeName() string { return "legacy.UserInsert" }

func TestMaterializerReplayAppliesUpcasts(t *testing.T) {
	ebuStore := eventbus.NewMemoryStore()
	bus := eventbus.New(eventbus.WithStore(ebuStore))

	// A legacy event persisted before the state protocol was adopted.
	eventbus.Publish(bus, legacyUserInsert{ID: "1", Name: "Alice"})

	// Migrate legacy events into state protocol change messages.
	err := eventbus.RegisterUpcastFunc(bus, "legacy.UserInsert", "state.ChangeMessage",
		func(data json.RawMessage) (json.RawMessage, string, error) {
			var legacy legacyUserInsert
			if err := json.Unmarshal(data, &legacy); err != nil {
				return nil, "", err
			}
			msg, err := Insert(legacy.ID, User{Name: legacy.Name})
			if err != nil {
				return nil, "", err
			}
			out, err := json.Marshal(msg)
			if err != nil {
				return nil, "", err
			}
			return out, "state.ChangeMessage", nil
		})
	if err != nil {
		t.Fatalf("RegisterUpcastFunc() error = %v", err)
	}

	// Strict mode: without upcasting the legacy event would be skipped as a
	// non-state message, so a materialized user proves the upcast ran.
	mat := NewMaterializer(WithStrictSchema())
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	if err := mat.Replay(context.Background(), bus, eventbus.OffsetOldest); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	user, ok := users.Get("1")
	if !ok {
		t.Fatal("user should be materialized from upcasted legacy event")
	}
	if user.Name != "Alice" {
		t.Errorf("Name = %q, want %q", user.Name, "Alice")
	}
}

// ================== Snapshot Tests ==================

// fakeSnapshotStore is an in-memory eventbus.EventStoreSnapshotter for tests.
type fakeSnapshotStore struct {
	offsets map[string]eventbus.Offset
	blobs   map[string]json.RawMessage
	saveErr error
	loadErr error
}

func newFakeSnapshotStore() *fakeSnapshotStore {
	return &fakeSnapshotStore{
		offsets: make(map[string]eventbus.Offset),
		blobs:   make(map[string]json.RawMessage),
	}
}

func (s *fakeSnapshotStore) SaveSnapshot(ctx context.Context, snapshotID string, atOffset eventbus.Offset, blob json.RawMessage) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.offsets[snapshotID] = atOffset
	s.blobs[snapshotID] = blob
	return nil
}

func (s *fakeSnapshotStore) LoadSnapshot(ctx context.Context, snapshotID string) (eventbus.Offset, json.RawMessage, error) {
	if s.loadErr != nil {
		return eventbus.OffsetOldest, nil, s.loadErr
	}
	offset, ok := s.offsets[snapshotID]
	if !ok {
		return eventbus.OffsetOldest, nil, nil
	}
	return offset, s.blobs[snapshotID], nil
}

var _ eventbus.EventStoreSnapshotter = (*fakeSnapshotStore)(nil)

func TestMaterializerSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	ebuStore := eventbus.NewMemoryStore()
	bus := eventbus.New(eventbus.WithStore(ebuStore))

	msg1, _ := Insert("1", User{Name: "Alice"})
	eventbus.Publish(bus, msg1)
	msg2, _ := Insert("2", User{Name: "Bob"})
	eventbus.Publish(bus, msg2)

	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)
	if err := mat.Replay(ctx, bus, eventbus.OffsetOldest); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	snaps := newFakeSnapshotStore()
	if err := mat.SaveSnapshotTo(ctx, snaps, "users"); err != nil {
		t.Fatalf("SaveSnapshotTo() error = %v", err)
	}
	if snaps.offsets["users"] != mat.LastOffset() {
		t.Errorf("snapshot offset = %q, want %q", snaps.offsets["users"], mat.LastOffset())
	}

	// An event published after the snapshot; only this tail should need replay.
	msg3, _ := Update("1", User{Name: "Alice Smith"})
	eventbus.Publish(bus, msg3)

	// Restore into a fresh materializer.
	mat2 := NewMaterializer()
	store2 := NewMemoryStore[User]()
	users2 := NewTypedCollection[User](store2)
	RegisterCollection(mat2, users2)
	// Pre-populate stale state to prove restore clears it.
	store2.Set(CompositeKey("state.User", "stale"), User{Name: "Stale"})

	offset, err := mat2.LoadSnapshotFrom(ctx, snaps, "users")
	if err != nil {
		t.Fatalf("LoadSnapshotFrom() error = %v", err)
	}
	if offset != snaps.offsets["users"] {
		t.Errorf("LoadSnapshotFrom() offset = %q, want %q", offset, snaps.offsets["users"])
	}
	if mat2.LastOffset() != offset {
		t.Errorf("LastOffset() = %q, want %q", mat2.LastOffset(), offset)
	}
	if _, ok := users2.Get("stale"); ok {
		t.Error("restore should clear pre-existing state")
	}
	if u, ok := users2.Get("2"); !ok || u.Name != "Bob" {
		t.Errorf("users2.Get(2) = %+v, %v; want Bob", u, ok)
	}

	if err := mat2.Replay(ctx, bus, offset); err != nil {
		t.Fatalf("Replay() from snapshot offset error = %v", err)
	}
	u1, ok := users2.Get("1")
	if !ok {
		t.Fatal("User 1 should exist after tail replay")
	}
	if u1.Name != "Alice Smith" {
		t.Errorf("User 1 name = %q, want %q", u1.Name, "Alice Smith")
	}
}

func TestMaterializerSnapshotUnregisteredType(t *testing.T) {
	ctx := context.Background()

	// Snapshot from a materializer with two collections.
	mat := NewMaterializer()
	userStore := NewMemoryStore[User]()
	users := NewTypedCollectionWithType[User](userStore, "user")
	RegisterCollection(mat, users)
	productStore := NewMemoryStore[Product]()
	products := NewTypedCollectionWithType[Product](productStore, "product")
	RegisterCollection(mat, products)

	insertUser, _ := Insert("1", User{Name: "Alice"}, WithEntityType("user"))
	insertProduct, _ := Insert("p1", Product{ID: "p1"}, WithEntityType("product"))
	userData, _ := json.Marshal(insertUser)
	productData, _ := json.Marshal(insertProduct)
	if err := mat.Apply(&eventbus.StoredEvent{Offset: "1", Data: userData}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if err := mat.Apply(&eventbus.StoredEvent{Offset: "2", Data: productData}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	snaps := newFakeSnapshotStore()
	if err := mat.SaveSnapshotTo(ctx, snaps, "all"); err != nil {
		t.Fatalf("SaveSnapshotTo() error = %v", err)
	}

	// Restore into a materializer that only registers the user collection:
	// the product snapshot data is dropped silently.
	mat2 := NewMaterializer()
	userStore2 := NewMemoryStore[User]()
	users2 := NewTypedCollectionWithType[User](userStore2, "user")
	RegisterCollection(mat2, users2)

	offset, err := mat2.LoadSnapshotFrom(ctx, snaps, "all")
	if err != nil {
		t.Fatalf("LoadSnapshotFrom() error = %v", err)
	}
	if offset != "2" {
		t.Errorf("offset = %q, want %q", offset, "2")
	}
	if _, ok := users2.Get("1"); !ok {
		t.Error("user collection should be restored")
	}
}

func TestMaterializerSaveSnapshotErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("refuses empty last offset", func(t *testing.T) {
		mat := NewMaterializer()
		snaps := newFakeSnapshotStore()
		err := mat.SaveSnapshotTo(ctx, snaps, "users")
		if err == nil {
			t.Fatal("SaveSnapshotTo() should refuse when nothing was applied")
		}
		if !strings.Contains(err.Error(), "no events applied") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("entity marshal failure", func(t *testing.T) {
		mat := NewMaterializer()
		userStore := NewMemoryStore[User]()
		RegisterCollection(mat, NewTypedCollection[User](userStore))
		badStore := NewMemoryStore[unmarshalableType]()
		RegisterCollection(mat, NewTypedCollectionWithType[unmarshalableType](badStore, "bad"))

		insertData, _ := json.Marshal(&ChangeMessage{
			Type:    "state.User",
			Key:     "1",
			Value:   json.RawMessage(`{"name":"Alice"}`),
			Headers: Headers{Operation: OperationInsert},
		})
		if err := mat.Apply(&eventbus.StoredEvent{Offset: "1", Data: insertData}); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		badStore.Set("bad/1", unmarshalableType{Ch: make(chan int)})

		if err := mat.SaveSnapshotTo(ctx, newFakeSnapshotStore(), "users"); err == nil {
			t.Fatal("SaveSnapshotTo() should fail for unmarshalable entity")
		}
	})

	t.Run("store save failure", func(t *testing.T) {
		mat := NewMaterializer()
		store := NewMemoryStore[User]()
		users := NewTypedCollection[User](store)
		RegisterCollection(mat, users)

		insertData, _ := json.Marshal(&ChangeMessage{
			Type:    "state.User",
			Key:     "1",
			Value:   json.RawMessage(`{"name":"Alice"}`),
			Headers: Headers{Operation: OperationInsert},
		})
		if err := mat.Apply(&eventbus.StoredEvent{Offset: "1", Data: insertData}); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}

		snaps := newFakeSnapshotStore()
		snaps.saveErr = errors.New("disk full")
		err := mat.SaveSnapshotTo(ctx, snaps, "users")
		if err == nil {
			t.Fatal("SaveSnapshotTo() should propagate store error")
		}
		if !errors.Is(err, snaps.saveErr) {
			t.Errorf("error should wrap store error, got %v", err)
		}
	})
}

func TestMaterializerLoadSnapshotErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("store load failure", func(t *testing.T) {
		mat := NewMaterializer()
		snaps := newFakeSnapshotStore()
		snaps.loadErr = errors.New("connection lost")
		offset, err := mat.LoadSnapshotFrom(ctx, snaps, "users")
		if err == nil {
			t.Fatal("LoadSnapshotFrom() should propagate store error")
		}
		if !errors.Is(err, snaps.loadErr) {
			t.Errorf("error should wrap store error, got %v", err)
		}
		if offset != eventbus.OffsetOldest {
			t.Errorf("offset = %q, want OffsetOldest", offset)
		}
	})

	t.Run("corrupt blob", func(t *testing.T) {
		mat := NewMaterializer()
		snaps := newFakeSnapshotStore()
		snaps.offsets["users"] = "5"
		snaps.blobs["users"] = json.RawMessage(`{invalid`)
		if _, err := mat.LoadSnapshotFrom(ctx, snaps, "users"); err == nil {
			t.Fatal("LoadSnapshotFrom() should fail on corrupt blob")
		}
	})

	t.Run("entity restore failure leaves materializer empty", func(t *testing.T) {
		mat := NewMaterializer()
		store := NewMemoryStore[User]()
		users := NewTypedCollection[User](store)
		RegisterCollection(mat, users)

		// Establish a non-empty last offset and some state.
		insertData, _ := json.Marshal(&ChangeMessage{
			Type:    "state.User",
			Key:     "1",
			Value:   json.RawMessage(`{"name":"Alice"}`),
			Headers: Headers{Operation: OperationInsert},
		})
		if err := mat.Apply(&eventbus.StoredEvent{Offset: "1", Data: insertData}); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}

		snaps := newFakeSnapshotStore()
		snaps.offsets["users"] = "5"
		snaps.blobs["users"] = json.RawMessage(`{"state.User":{"state.User/1":"not an object"}}`)

		offset, err := mat.LoadSnapshotFrom(ctx, snaps, "users")
		if err == nil {
			t.Fatal("LoadSnapshotFrom() should fail when an entity cannot be restored")
		}
		if offset != eventbus.OffsetOldest {
			t.Errorf("offset = %q, want OffsetOldest", offset)
		}
		if mat.LastOffset() != eventbus.OffsetOldest {
			t.Errorf("LastOffset() = %q, want OffsetOldest", mat.LastOffset())
		}
		if got := len(users.All()); got != 0 {
			t.Errorf("store has %d entities after failed restore, want 0", got)
		}
	})
}

func TestMaterializerLoadSnapshotMissing(t *testing.T) {
	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	// Pre-existing state must remain untouched when there is no snapshot.
	store.Set(CompositeKey("state.User", "1"), User{Name: "Alice"})

	offset, err := mat.LoadSnapshotFrom(context.Background(), newFakeSnapshotStore(), "users")
	if err != nil {
		t.Fatalf("LoadSnapshotFrom() error = %v", err)
	}
	if offset != eventbus.OffsetOldest {
		t.Errorf("offset = %q, want OffsetOldest", offset)
	}
	if _, ok := users.Get("1"); !ok {
		t.Error("collections should be untouched when no snapshot exists")
	}
}

func TestApplyInvalidChangeMessageJSON(t *testing.T) {
	mat := NewMaterializer()
	store := NewMemoryStore[User]()
	users := NewTypedCollection[User](store)
	RegisterCollection(mat, users)

	// Valid headers structure but invalid change message structure
	// The headers are valid but rest of message is malformed
	event := &eventbus.StoredEvent{
		Offset: "1",
		Data:   []byte(`{"headers":{"operation":"insert"},"type":123,"key":"1"}`),
	}
	err := mat.Apply(event)
	if err == nil {
		t.Error("Apply() should error when type field is wrong type")
	}
}
