package state

import "time"

// ChangeOption configures a change message.
type ChangeOption func(*changeConfig)

type changeConfig struct {
	txID          string
	timestamp     *time.Time
	autoTimestamp bool
	entityType    string
}

// WithTxID sets the transaction ID for grouping related changes.
// Transaction IDs allow consumers to process related changes atomically.
func WithTxID(txID string) ChangeOption {
	return func(c *changeConfig) {
		c.txID = txID
	}
}

// WithTimestamp sets an explicit timestamp for the change message.
// The timestamp will be formatted as RFC 3339.
func WithTimestamp(t time.Time) ChangeOption {
	return func(c *changeConfig) {
		c.timestamp = &t
	}
}

// WithAutoTimestamp automatically sets the timestamp to the current time.
func WithAutoTimestamp() ChangeOption {
	return func(c *changeConfig) {
		c.autoTimestamp = true
	}
}

// WithEntityType overrides the automatic entity type name.
// Use this when the type name should differ from the Go type name.
func WithEntityType(typeName string) ChangeOption {
	return func(c *changeConfig) {
		c.entityType = typeName
	}
}

// MaterializerOption configures a Materializer.
type MaterializerOption func(*materializerConfig)

type materializerConfig struct {
	onReset      func()
	onSnapshot   func(start bool)
	onError      func(error)
	strictSchema bool
}

// WithOnReset sets a callback invoked when a reset control message is received.
// The callback is called after all collections have been cleared.
func WithOnReset(fn func()) MaterializerOption {
	return func(c *materializerConfig) {
		c.onReset = fn
	}
}

// WithOnSnapshot sets a callback invoked on snapshot-start/end messages.
// The boolean parameter is true for snapshot-start, false for snapshot-end.
func WithOnSnapshot(fn func(start bool)) MaterializerOption {
	return func(c *materializerConfig) {
		c.onSnapshot = fn
	}
}

// WithOnError sets an error handler for materialization errors.
// This is called when applying a change message fails.
func WithOnError(fn func(error)) MaterializerOption {
	return func(c *materializerConfig) {
		c.onError = fn
	}
}

// WithStrictSchema enables strict schema validation.
// When enabled, the materializer returns an error for unknown entity types.
// When disabled (default), unknown types are silently ignored.
func WithStrictSchema() MaterializerOption {
	return func(c *materializerConfig) {
		c.strictSchema = true
	}
}
