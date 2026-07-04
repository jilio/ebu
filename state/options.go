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

// ApplyErrorPolicy determines how the Materializer handles a state message
// that cannot be decoded (see ErrUndecodable). It mirrors ebu's
// ReplayErrorPolicy for durable subscriptions.
type ApplyErrorPolicy int

const (
	// ApplyAbort makes Apply return the decode error without advancing
	// LastOffset (default). The next replay hits the same event again. Use
	// this when an undecodable message means a bug that must be fixed
	// before proceeding.
	ApplyAbort ApplyErrorPolicy = iota

	// ApplySkip reports the decode error to WithOnError and advances
	// LastOffset past the poison message, so one bad payload cannot wedge
	// every future replay. Store failures are never skipped: they are
	// transient, and the event must be retried.
	ApplySkip
)

type materializerConfig struct {
	onReset          func()
	onSnapshot       func(start bool)
	onError          func(error)
	strictSchema     bool
	applyErrorPolicy ApplyErrorPolicy
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
// It is invoked exactly once for every failed application: envelope or
// change-message decode failures, unknown entity types in strict mode,
// unknown operations, and collection apply errors.
func WithOnError(fn func(error)) MaterializerOption {
	return func(c *materializerConfig) {
		c.onError = fn
	}
}

// WithApplyErrorPolicy sets how the materializer treats undecodable state
// messages during Apply. The default is ApplyAbort.
func WithApplyErrorPolicy(policy ApplyErrorPolicy) MaterializerOption {
	return func(c *materializerConfig) {
		c.applyErrorPolicy = policy
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
