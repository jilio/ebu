package state

import (
	"encoding/json"
	"reflect"
	"strings"
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
// StateTypeName must be a pure function of the type (never of instance
// state): the package derives names from zero values and fresh instances.
//
// Example:
//
//	type User struct { ... }
//	func (u User) StateTypeName() string { return "user" }
type TypeNamer interface {
	StateTypeName() string
}

// typeNamerType is the reflect.Type of the TypeNamer interface.
var typeNamerType = reflect.TypeOf((*TypeNamer)(nil)).Elem()

// entityTypeOf returns the entity type name for a reflect.Type, honoring
// TypeNamer without ever invoking it on a nil pointer: for pointer types a
// fresh instance is allocated, since a value-receiver method promoted to the
// pointer type would dereference nil (mirrors typeNameOf in the parent
// package).
func entityTypeOf(t reflect.Type) string {
	if t.Implements(typeNamerType) {
		if t.Kind() == reflect.Pointer {
			return reflect.New(t.Elem()).Interface().(TypeNamer).StateTypeName()
		}
		return reflect.Zero(t).Interface().(TypeNamer).StateTypeName()
	}
	if reflect.PointerTo(t).Implements(typeNamerType) {
		return reflect.New(t).Interface().(TypeNamer).StateTypeName()
	}
	return t.String()
}

// entityTypeFor returns the entity type name for a type parameter without
// needing an instance, so pointer entity types are safe.
func entityTypeFor[T any]() string {
	return entityTypeOf(reflect.TypeOf((*T)(nil)).Elem())
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
		if v := reflect.ValueOf(entity); v.Kind() == reflect.Pointer && v.IsNil() {
			// A typed nil pointer: calling a value-receiver StateTypeName
			// through it would dereference nil. Derive the name from the
			// type instead.
			return entityTypeOf(v.Type())
		}
		return namer.StateTypeName()
	}
	return reflect.TypeOf(entity).String()
}

// CompositeKey returns an unambiguous composite key from type and key
// components. Percent signs and slashes within either component are
// percent-encoded, leaving the sole unescaped slash as the separator.
// Components without those reserved characters retain the familiar type/key
// representation.
func CompositeKey(entityType, key string) string {
	return encodeKeyComponent(entityType) + "/" + encodeKeyComponent(key)
}

func encodeKeyComponent(component string) string {
	component = strings.ReplaceAll(component, "%", "%25")
	return strings.ReplaceAll(component, "/", "%2F")
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
