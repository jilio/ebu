package durablestream

import (
	"net/http"
	"time"
)

// Logger is the interface for logging.
type Logger interface {
	Printf(format string, v ...any)
}

// Option configures a Store.
type Option func(*config)

type config struct {
	httpClient    *http.Client
	timeout       time.Duration
	retryAttempts int
	retryBackoff  time.Duration
	contentType   string
	logger        Logger
}

func defaultConfig() *config {
	return &config{
		httpClient:    http.DefaultClient,
		timeout:       30 * time.Second,
		retryAttempts: 3,
		retryBackoff:  100 * time.Millisecond,
		contentType:   "application/json",
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) {
		if client != nil {
			c.httpClient = client
		}
	}
}

// WithTimeout sets the request timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithRetry configures retry behavior.
func WithRetry(attempts int, backoff time.Duration) Option {
	return func(c *config) {
		if attempts >= 0 {
			c.retryAttempts = attempts
		}
		if backoff > 0 {
			c.retryBackoff = backoff
		}
	}
}

// WithContentType sets the stream's content type.
// Default is "application/json".
func WithContentType(contentType string) Option {
	return func(c *config) {
		if contentType != "" {
			c.contentType = contentType
		}
	}
}

// WithLogger sets a logger for debugging and error reporting.
// When set, malformed events and other issues will be logged.
func WithLogger(logger Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}
