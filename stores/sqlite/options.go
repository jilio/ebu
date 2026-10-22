package sqlite

import (
	"fmt"
	"time"
)

// Logger is an interface for logging operations
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// MetricsHook is called after store operations complete
type MetricsHook interface {
	OnAppend(duration time.Duration, err error)
	OnRead(duration time.Duration, count int, err error)
	OnSaveOffset(duration time.Duration, err error)
	OnLoadOffset(duration time.Duration, err error)
}

// Option configures the SQLiteStore
type Option func(*config)

// defaultStreamBatchSize is the ReadStream batch size used when
// WithStreamBatchSize is not given (or given a non-positive size).
const defaultStreamBatchSize = 1000

// config holds all configuration options
type config struct {
	path            string
	busyTimeout     time.Duration
	autoMigrate     bool
	readOnly        bool
	synchronous     Synchronous
	logger          Logger
	metricsHook     MetricsHook
	streamBatchSize int
}

// defaultConfig returns the default configuration
func defaultConfig() *config {
	return &config{
		busyTimeout:     5 * time.Second,
		autoMigrate:     true,
		streamBatchSize: defaultStreamBatchSize,
	}
}

// Synchronous selects how a writable file-backed store syncs commits to stable
// storage: the trade-off between append latency and durability across power
// loss or an operating-system crash. Every store runs in WAL mode, where both
// settings keep the database consistent; they differ in which acknowledged
// commits can be lost.
type Synchronous int

const (
	// SynchronousNormal (the default) syncs the write-ahead log only when it is
	// checkpointed. A process crash loses nothing, but a power loss or OS crash
	// can roll back the most recent acknowledged commits — Append, SaveOffset,
	// SaveSnapshot and TruncateBefore alike. Commits are never reordered: a
	// rollback removes a suffix of commits, never one in the middle.
	SynchronousNormal Synchronous = iota

	// SynchronousFull syncs the write-ahead log before every commit returns, so
	// an acknowledged commit survives power loss and OS crashes, provided the
	// storage stack honors sync requests (on macOS, fsync does not flush the
	// drive's cache). Each write pays for one sync; group writes into fewer
	// commits when throughput matters.
	SynchronousFull
)

// String returns the SQLite pragma value for the mode.
func (s Synchronous) String() string {
	switch s {
	case SynchronousNormal:
		return "NORMAL"
	case SynchronousFull:
		return "FULL"
	default:
		return fmt.Sprintf("Synchronous(%d)", int(s))
	}
}

// WithSynchronous sets the synchronous mode of every pooled connection a
// writable store opens (see Synchronous). The default, SynchronousNormal,
// preserves the behavior of earlier releases; choose SynchronousFull when an
// acknowledged write must survive power loss. Stores opened with OpenReadOnly
// never write and ignore this option; :memory: stores have nothing to sync.
// New rejects values other than the declared constants.
func WithSynchronous(mode Synchronous) Option {
	return func(c *config) {
		c.synchronous = mode
	}
}

// WithBusyTimeout sets the SQLite busy timeout
// Default is 5 seconds
func WithBusyTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.busyTimeout = timeout
	}
}

// WithAutoMigrate enables or disables automatic schema migration
// Default is true for New; OpenReadOnly defaults to false and rejects true.
func WithAutoMigrate(enabled bool) Option {
	return func(c *config) {
		c.autoMigrate = enabled
	}
}

// WithLogger sets the logger for the store
func WithLogger(logger Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

// WithMetricsHook sets the metrics hook for the store
func WithMetricsHook(hook MetricsHook) Option {
	return func(c *config) {
		c.metricsHook = hook
	}
}

// WithStreamBatchSize sets the batch size for ReadStream operations.
// Events are fetched in batches of this size using cursor-based pagination,
// so a stream never pins a pooled connection and its WAL read snapshot for
// the whole iteration. Default is 1000; a size <= 0 also selects the default.
//
// Semantics: batched iteration re-queries the database between batches, so
// it observes events appended while the iteration is in progress. This
// means the iteration is not a point-in-time snapshot of the log.
func WithStreamBatchSize(size int) Option {
	return func(c *config) {
		if size <= 0 {
			size = defaultStreamBatchSize
		}
		c.streamBatchSize = size
	}
}
