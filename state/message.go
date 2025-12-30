package state

import (
	"encoding/json"
	"reflect"
)

// Operation represents the type of change operation per the State Protocol.
type Operation string

const (
	// OperationInsert indicates a new entity is being created.
	OperationInsert Operation = "insert"
	// OperationUpdate indicates an existing entity is being modified.
	OperationUpdate Operation = "update"
	// OperationDelete indicates an entity is being removed.
	OperationDelete Operation = "delete"
)

// Control represents the type of control message per the State Protocol.
type Control string

const (
	// ControlSnapshotStart marks the beginning of a snapshot.
	ControlSnapshotStart Control = "snapshot-start"
	// ControlSnapshotEnd marks the end of a snapshot.
	ControlSnapshotEnd Control = "snapshot-end"
	// ControlReset signals that all state should be cleared.
	ControlReset Control = "reset"
)

// Headers contains metadata for change messages per the State Protocol.
type Headers struct {
	// Operation is the type of change (insert, update, delete).
	Operation Operation `json:"operation"`
	// TxID is an optional transaction identifier for grouping related changes.
	TxID string `json:"txid,omitempty"`
	// Timestamp is an optional RFC 3339 formatted timestamp.
	Timestamp string `json:"timestamp,omitempty"`
}

// ControlHeaders contains metadata for control messages.
type ControlHeaders struct {
	// Control is the type of control message.
	Control Control `json:"control"`
	// Offset is an optional reference to a stream position.
	Offset string `json:"offset,omitempty"`
}

// ChangeMessage represents a state change event per the State Protocol.
// It contains an entity mutation (insert, update, or delete) with a composite key.
type ChangeMessage struct {
	// Type is the entity type discriminator (e.g., "user", "order").
	Type string `json:"type"`
	// Key is the unique identifier within the entity type.
	Key string `json:"key"`
	// Value contains the entity data (required for insert/update).
	Value json.RawMessage `json:"value,omitempty"`
	// OldValue contains the previous entity data (optional, for conflict detection).
	OldValue json.RawMessage `json:"old_value,omitempty"`
	// Headers contains operation metadata.
	Headers Headers `json:"headers"`
}

// ControlMessage represents a control event per the State Protocol.
// Control messages manage stream lifecycle (snapshots, resets).
type ControlMessage struct {
	// Headers contains control message metadata.
	Headers ControlHeaders `json:"headers"`
}

// TypeNamer is an optional interface that entity types can implement to provide
// their own type name. This mirrors ebu's TypeNamer pattern.
//
// Example:
//
//	type User struct { ... }
//	func (u User) StateTypeName() string { return "user" }
type TypeNamer interface {
	StateTypeName() string
}

// EntityType returns the type name for an entity.
// If the entity implements TypeNamer, returns the custom name.
// Otherwise returns the reflect-based package-qualified name.
// Returns "nil" if entity is nil.
func EntityType(entity any) string {
	if entity == nil {
		return "nil"
	}
	if namer, ok := entity.(TypeNamer); ok {
		return namer.StateTypeName()
	}
	return reflect.TypeOf(entity).String()
}

// CompositeKey returns a composite key from type and key components.
// The State Protocol uses type + key as a composite identifier.
func CompositeKey(entityType, key string) string {
	return entityType + "/" + key
}

// EventTypeName implements ebu's TypeNamer interface for ChangeMessage.
// This allows ChangeMessage to be published directly to an EventBus.
func (m ChangeMessage) EventTypeName() string {
	return "state.ChangeMessage"
}

// EventTypeName implements ebu's TypeNamer interface for ControlMessage.
// This allows ControlMessage to be published directly to an EventBus.
func (m ControlMessage) EventTypeName() string {
	return "state.ControlMessage"
}
