package durablestream

import (
	"net/http"
	"time"
)

// Logger is the interface for logging.
type Logger interface {
	Printf(format string, v ...any)
}

// DecodeErrorHandler is invoked when a stored event cannot be decoded and
// is skipped during Read. It receives the decode error and the raw JSON of
// the malformed event.
type DecodeErrorHandler func(err error, raw []byte)

// Option configures a Store.
type Option func(*config)

type config struct {
	httpClient         *http.Client
	timeout            time.Duration
	contentType        string
	logger             Logger
	retryAttempts      int
	retryBaseDelay     time.Duration
	decodeErrorHandler DecodeErrorHandler
}

func defaultConfig() *config {
	return &config{
		httpClient:     http.DefaultClient,
		timeout:        30 * time.Second,
		contentType:    "application/json",
		retryAttempts:  3,
		retryBaseDelay: 100 * time.Millisecond,
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

// WithRetry configures retry behavior for Append and Read on transient
// failures (network errors, HTTP 5xx, 429). attempts is the total number of
// attempts (including the first); baseDelay is the delay before the first
// retry and doubles for each subsequent retry. Retries respect context
// cancellation. Default: 3 attempts, 100ms base delay.
// Non-positive values are ignored.
func WithRetry(attempts int, baseDelay time.Duration) Option {
	return func(c *config) {
		if attempts > 0 {
			c.retryAttempts = attempts
		}
		if baseDelay > 0 {
			c.retryBaseDelay = baseDelay
		}
	}
}

// WithDecodeErrorHandler sets a handler invoked whenever a stored event
// cannot be decoded and is skipped during Read. When set, it takes
// precedence over the logger; when unset, malformed events fall back to the
// logger if one is configured. Skipping behavior is unchanged either way.
func WithDecodeErrorHandler(handler DecodeErrorHandler) Option {
	return func(c *config) {
		c.decodeErrorHandler = handler
	}
}
