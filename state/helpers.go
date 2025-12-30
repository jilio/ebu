package state

import (
	"encoding/json"
	"fmt"
	"time"
)

// Insert creates an insert change message for an entity.
// The type parameter T determines the entity type name (unless overridden with WithEntityType).
//
// Example:
//
//	msg, err := state.Insert("user:1", User{Name: "Alice"})
func Insert[T any](key string, value T, opts ...ChangeOption) (*ChangeMessage, error) {
	return newChangeMessage[T](OperationInsert, key, &value, nil, opts...)
}

// Update creates an update change message for an entity.
// The type parameter T determines the entity type name (unless overridden with WithEntityType).
//
// Example:
//
//	msg, err := state.Update("user:1", User{Name: "Alice Smith"})
func Update[T any](key string, value T, opts ...ChangeOption) (*ChangeMessage, error) {
	return newChangeMessage[T](OperationUpdate, key, &value, nil, opts...)
}

// UpdateWithOldValue creates an update change message with the old value for conflict detection.
// The old value can be used by consumers to detect concurrent modifications.
//
// Example:
//
//	msg, err := state.UpdateWithOldValue("user:1", newUser, oldUser)
func UpdateWithOldValue[T any](key string, value, oldValue T, opts ...ChangeOption) (*ChangeMessage, error) {
	return newChangeMessage[T](OperationUpdate, key, &value, &oldValue, opts...)
}

// Delete creates a delete change message for an entity.
// The type parameter T determines the entity type name.
//
// Example:
//
//	msg, err := state.Delete[User]("user:1")
func Delete[T any](key string, opts ...ChangeOption) (*ChangeMessage, error) {
	return newChangeMessage[T](OperationDelete, key, nil, nil, opts...)
}

// DeleteWithOldValue creates a delete change message with the old value preserved.
// This is useful for consumers that need to know what was deleted.
//
// Example:
//
//	msg, err := state.DeleteWithOldValue("user:1", user)
func DeleteWithOldValue[T any](key string, oldValue T, opts ...ChangeOption) (*ChangeMessage, error) {
	return newChangeMessage[T](OperationDelete, key, nil, &oldValue, opts...)
}

// newChangeMessage is the internal constructor for change messages.
func newChangeMessage[T any](op Operation, key string, value, oldValue *T, opts ...ChangeOption) (*ChangeMessage, error) {
	if key == "" {
		return nil, fmt.Errorf("state: key cannot be empty")
	}

	cfg := &changeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	// Determine entity type
	var zero T
	entityType := EntityType(zero)
	if cfg.entityType != "" {
		entityType = cfg.entityType
	}

	msg := &ChangeMessage{
		Type: entityType,
		Key:  key,
		Headers: Headers{
			Operation: op,
			TxID:      cfg.txID,
		},
	}

	// Set timestamp
	if cfg.timestamp != nil {
		msg.Headers.Timestamp = cfg.timestamp.Format(time.RFC3339Nano)
	} else if cfg.autoTimestamp {
		msg.Headers.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}

	// Marshal value for insert/update
	if value != nil {
		valueData, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("state: marshal value: %w", err)
		}
		msg.Value = valueData
	}

	// Marshal old value if provided
	if oldValue != nil {
		oldData, err := json.Marshal(oldValue)
		if err != nil {
			return nil, fmt.Errorf("state: marshal old_value: %w", err)
		}
		msg.OldValue = oldData
	}

	return msg, nil
}

// SnapshotStart creates a snapshot-start control message.
// This marks the beginning of a snapshot in the stream.
func SnapshotStart(offset string) *ControlMessage {
	return &ControlMessage{
		Headers: ControlHeaders{
			Control: ControlSnapshotStart,
			Offset:  offset,
		},
	}
}

// SnapshotEnd creates a snapshot-end control message.
// This marks the end of a snapshot in the stream.
func SnapshotEnd(offset string) *ControlMessage {
	return &ControlMessage{
		Headers: ControlHeaders{
			Control: ControlSnapshotEnd,
			Offset:  offset,
		},
	}
}

// Reset creates a reset control message.
// This signals that all state should be cleared and rebuilt from subsequent events.
func Reset(offset string) *ControlMessage {
	return &ControlMessage{
		Headers: ControlHeaders{
			Control: ControlReset,
			Offset:  offset,
		},
	}
}
