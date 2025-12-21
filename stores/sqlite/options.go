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
		busyTimeout: 5 * time.Second,
		autoMigrate: true,
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

// WithStreamBatchSize sets the batch size for LoadStream operations.
// When > 0, events are fetched in batches of this size using LIMIT.
// Default is 0 (no batching, fetch all matching rows at once).
func WithStreamBatchSize(size int) Option {
	return func(c *config) {
		c.streamBatchSize = size
	}
}
