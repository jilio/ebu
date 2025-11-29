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
	OnSave(duration time.Duration, err error)
	OnLoad(duration time.Duration, count int, err error)
	OnGetPosition(duration time.Duration, err error)
	OnSaveSubscriptionPosition(duration time.Duration, err error)
	OnLoadSubscriptionPosition(duration time.Duration, err error)
}

// Option configures the SQLiteStore
type Option func(*config)

// config holds all configuration options
type config struct {
	path        string
	busyTimeout time.Duration
	autoMigrate bool
	logger      Logger
	metricsHook MetricsHook
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
