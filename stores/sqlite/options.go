package sqlite

import (
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

// WithBusyTimeout sets the SQLite busy timeout
// Default is 5 seconds
func WithBusyTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.busyTimeout = timeout
	}
}

// WithAutoMigrate enables or disables automatic schema migration
// Default is true
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
// differs from an unbatched stream (a single query held open across the
// iteration), which is a point-in-time snapshot of the log; unbatched mode
// is not selectable through this option.
func WithStreamBatchSize(size int) Option {
	return func(c *config) {
		if size <= 0 {
			size = defaultStreamBatchSize
		}
		c.streamBatchSize = size
	}
}
