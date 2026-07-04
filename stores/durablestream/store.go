// Package durablestream implements the ebu EventStore interface for
// durable-streams servers (https://github.com/durable-streams/durable-streams).
//
// Durable-streams is an HTTP-based protocol for real-time sync to client
// applications. It uses opaque string offsets and supports both catch-up
// reads and live tailing via SSE.
//
// This implementation wraps the conformance-tested ahimsalabs/durable-streams-go
// client library for full protocol compatibility.
//
// # Offset semantics (at-least-once)
//
// Every offset this store emits — Append's return value, each StoredEvent's
// Offset, and Read's nextOffset — is a real server-issued offset that is safe
// to persist and resume from. Reading from an offset returns events strictly
// after it. Because durable-streams reads are chunked and the server only
// reports the chunk's end offset, per-event offsets within a chunk use
// chunk-start semantics: resuming from a saved per-event offset may re-deliver
// events at or before the saved position (duplicates), but never skips a
// later event. Consumers must be tolerant of duplicate delivery.
//
// # Append semantics (at-least-once)
//
// Append retries transient failures, and a retry cannot tell a request that
// failed before commit from one that committed but lost its response: the
// protocol's writer-coordination sequence numbers are stream-global and its
// conflict response carries no offset, so they cannot safely disambiguate
// (see Append). A retried append may therefore store the event twice.
// Combined with the offset semantics above, delivery is at-least-once
// end-to-end; consumers must deduplicate if exactly-once processing is
// required.
package durablestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
	eventbus "github.com/jilio/ebu"
)

// Store implements eventbus.EventStore for durable-streams servers.
type Store struct {
	client *durablestream.Client
	path   string
	cfg    *config

	// appendMu serializes Append calls. The event bus does not serialize
	// store access, and each Append round-trips over HTTP; serializing here
	// keeps writer offsets consistent.
	appendMu sync.Mutex

	// writer is a cached StreamWriter, guarded by appendMu. Creating a
	// writer costs a HEAD request, so it is reused across Append calls and
	// dropped after any send failure (it may be bound to a stale context or
	// stale stream metadata).
	writer *durablestream.StreamWriter
}

// Ensure Store implements the EventStore interface.
var _ eventbus.EventStore = (*Store)(nil)

// New creates a new Store connected to a durable-streams server.
//
// The baseURL should be the base URL of the durable-streams server
// (e.g., "http://localhost:4437/v1/stream").
// The streamPath is the name of the stream (e.g., "my-events").
//
// By default, the store will attempt to create the stream if it doesn't exist.
// Note: Stream creation uses context.Background() to ensure it completes fully.
// Use NewWithContext if you need cancellable initialization.
func New(baseURL string, streamPath string, opts ...Option) (*Store, error) {
	return NewWithContext(context.Background(), baseURL, streamPath, opts...)
}

// NewWithContext creates a new Store with a context for initialization.
// The context is used for the initial stream creation request.
func NewWithContext(ctx context.Context, baseURL string, streamPath string, opts ...Option) (*Store, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("durablestream: baseURL is required")
	}
	if streamPath == "" {
		return nil, fmt.Errorf("durablestream: streamPath is required")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// The client library never applies ClientConfig.Timeout to requests and
	// http.DefaultClient has none, so a hung server would block forever.
	// When no custom client is supplied, build one carrying the configured
	// timeout; it is the only bound on the cached writer's sends, which
	// cannot be limited per-call.
	if cfg.httpClient == nil {
		cfg.httpClient = &http.Client{Timeout: cfg.timeout}
	}

	clientCfg := &durablestream.ClientConfig{
		HTTPClient: cfg.httpClient,
		Timeout:    cfg.timeout,
	}

	client := durablestream.NewClient(baseURL, clientCfg)

	// Try to create the stream (idempotent). Bound the request so a hung
	// server cannot block initialization forever.
	createCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	_, err := client.Create(createCtx, streamPath, &durablestream.CreateOptions{
		ContentType: cfg.contentType,
	})
	if err != nil {
		return nil, fmt.Errorf("durablestream: create stream: %w", err)
	}

	return &Store{
		client: client,
		path:   streamPath,
		cfg:    cfg,
	}, nil
}

// storedEventForWrite represents the event format for writing to the stream.
type storedEventForWrite struct {
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp string          `json:"timestamp,omitempty"`
}

// Append stores an event and returns its assigned offset.
// Safe for concurrent use.
//
// Offset semantics: the returned offset is the server-issued next-offset
// from the append response — the position immediately after the appended
// event. Resuming a Read from it returns events appended strictly after
// this one: the event itself is not re-delivered and no later event is
// skipped. It is safe to persist (e.g., via SaveOffset).
//
// Transient failures (network errors, HTTP 5xx, 429) are retried with
// exponential backoff; see WithRetry. A cached writer is reused across
// calls to avoid a HEAD round trip per append and is recreated after any
// send failure.
//
// Appends are at-least-once: a retry of a request that committed server-side
// but lost its response stores the event again. The protocol's Seq
// writer-coordination cannot safely detect this case — sequence numbers are
// stream-global (a conflict may equally mean another writer advanced the
// sequence while our attempt never committed, so treating it as success
// would silently lose the event) and the conflict response carries no
// offset to recover — so duplicates are documented rather than masked.
//
// Appends are serialized on an internal mutex, so a failing or slow append
// (bounded by the HTTP client's timeout and the retry policy) delays
// concurrent publishers until it resolves.
func (s *Store) Append(ctx context.Context, event *eventbus.Event) (eventbus.Offset, error) {
	writeEvent := storedEventForWrite{
		Type: event.Type,
		Data: event.Data,
	}
	if !event.Timestamp.IsZero() {
		writeEvent.Timestamp = event.Timestamp.Format(time.RFC3339Nano)
	}

	data, err := json.Marshal(writeEvent)
	if err != nil {
		return "", fmt.Errorf("durablestream: marshal event: %w", err)
	}

	s.appendMu.Lock()
	defer s.appendMu.Unlock()

	var lastErr error
	for attempt := 1; attempt <= s.cfg.retryAttempts; attempt++ {
		if attempt > 1 {
			if err := backoff(ctx, s.cfg.retryBaseDelay, attempt-1); err != nil {
				return "", fmt.Errorf("durablestream: append: %w", err)
			}
		}
		if err := ctx.Err(); err != nil {
			return "", fmt.Errorf("durablestream: append: %w", err)
		}

		if s.writer == nil {
			// Detach the writer's context from the caller: the cached
			// writer outlives this Append and reuses its construction
			// context for every later send, so binding it to this caller
			// would let their cancellation abort unrelated future Appends.
			// In-flight requests are bounded by the HTTP client's timeout.
			writer, err := s.client.Writer(context.WithoutCancel(ctx), s.path)
			if err != nil {
				lastErr = fmt.Errorf("durablestream: get writer: %w", err)
				if !isRetryable(err) {
					return "", lastErr
				}
				continue
			}
			s.writer = writer
		}

		if err := s.writer.Send(data, nil); err != nil {
			// The cached writer may be bound to a stale context or stale
			// stream metadata; drop it so the next attempt starts fresh.
			s.writer = nil
			lastErr = fmt.Errorf("durablestream: send: %w", err)
			if ctx.Err() != nil || !isRetryable(err) {
				return "", lastErr
			}
			continue
		}

		return eventbus.Offset(s.writer.Offset()), nil
	}

	return "", fmt.Errorf("durablestream: append: giving up after %d attempts: %w", s.cfg.retryAttempts, lastErr)
}

// storedEventWithOffset is used to parse events that include their own offset.
type storedEventWithOffset struct {
	Offset    string          `json:"offset,omitempty"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp string          `json:"timestamp,omitempty"`
}

// Read returns events appended strictly after the given offset.
//
// Offset semantics (at-least-once): every offset in the returned
// StoredEvents is server-issued and safe to persist and resume from.
// durable-streams reads are chunked and the server only reports the chunk's
// end offset, so per-event offsets use chunk-start semantics:
//
//   - If a stored event embeds its own "offset" field (written by an
//     external producer), that offset is used directly and resumption from
//     it is exact.
//   - Otherwise, every event except the last in a chunk carries the offset
//     the chunk was read from. Resuming from it re-reads the whole chunk:
//     events at or before the saved position may be re-delivered
//     (duplicates), but no later event is ever skipped.
//   - The last event of a chunk carries the server's next-offset, which is
//     exactly the resume point after it (no duplicates, no skips).
//
// Consumers resuming from a saved per-event offset must therefore tolerate
// duplicate delivery of already-handled events; events are never skipped.
//
// The returned nextOffset is always server-issued and advancing: it is the
// exact resume point after the last returned event.
//
// Limit handling: limit is honored only when every event carries a real
// embedded offset, because truncating a chunk requires a resumable
// per-event offset for the returned nextOffset. Otherwise the full chunk is
// returned (limit is best-effort) — truncating would either skip the
// dropped events or stall progress on the next Read.
//
// Transient failures (network errors, HTTP 5xx, 429) are retried with
// exponential backoff; see WithRetry.
func (s *Store) Read(ctx context.Context, from eventbus.Offset, limit int) ([]*eventbus.StoredEvent, eventbus.Offset, error) {
	// OffsetNewest resolves at call time to the current tail: no events,
	// and a concrete server-issued offset to resume from. Passing "$" to
	// the server as a literal offset would be meaningless.
	if from == eventbus.OffsetNewest {
		tail, err := s.resolveTail(ctx)
		if err != nil {
			return nil, from, err
		}
		return nil, tail, nil
	}

	// Map OffsetOldest to durable-streams zero offset
	offset := durablestream.Offset(from)
	if from == eventbus.OffsetOldest {
		offset = durablestream.ZeroOffset
	}

	// chunkStart is the offset the current chunk was read from; it is the
	// resume-safe offset for non-last events without embedded offsets.
	chunkStart := from
	for {
		var result *durablestream.StreamData
		var lastErr error
		for attempt := 1; attempt <= s.cfg.retryAttempts; attempt++ {
			if attempt > 1 {
				if err := backoff(ctx, s.cfg.retryBaseDelay, attempt-1); err != nil {
					return nil, from, fmt.Errorf("durablestream: read: %w", err)
				}
			}
			if err := ctx.Err(); err != nil {
				return nil, from, fmt.Errorf("durablestream: read: %w", err)
			}

			// Each attempt gets its own timeout window so a hung server
			// fails the attempt instead of blocking Read forever.
			attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
			res, err := s.client.Reader(s.path, offset).Read(attemptCtx)
			cancel()
			if err != nil {
				lastErr = fmt.Errorf("durablestream: read: %w", err)
				if ctx.Err() != nil || !isRetryable(err) {
					return nil, from, lastErr
				}
				continue
			}
			result = res
			break
		}
		if result == nil {
			return nil, from, fmt.Errorf("durablestream: read: giving up after %d attempts: %w", s.cfg.retryAttempts, lastErr)
		}

		// Parse JSON array response (an empty body yields no events).
		var rawEvents []json.RawMessage
		if len(result.Data) > 0 {
			if err := json.Unmarshal(result.Data, &rawEvents); err != nil {
				return nil, from, fmt.Errorf("durablestream: unmarshal response: %w", err)
			}
		}

		// Convert to StoredEvents
		events := make([]*eventbus.StoredEvent, 0, len(rawEvents))
		allEmbedded := true
		lastRaw := len(rawEvents) - 1
		for i, raw := range rawEvents {
			// Try to parse as event with embedded offset first
			var eventWithOffset storedEventWithOffset
			if err := json.Unmarshal(raw, &eventWithOffset); err != nil {
				s.handleDecodeError(i, raw, err)
				continue
			}

			// Determine the event's resume offset. Every emitted offset is
			// server-issued and safe to store: resuming from it may re-deliver
			// earlier events (at-least-once) but never skips a later one.
			var eventOffset eventbus.Offset
			switch {
			case eventWithOffset.Offset != "":
				// Embedded per-event offset: exact resumption.
				eventOffset = eventbus.Offset(eventWithOffset.Offset)
			case i == lastRaw:
				// The server's next-offset is exactly the resume point after
				// the chunk's last event.
				eventOffset = eventbus.Offset(result.NextOffset)
				allEmbedded = false
			default:
				// Chunk-start: resuming re-reads this chunk from the start,
				// re-delivering earlier events but never skipping later ones.
				eventOffset = chunkStart
				allEmbedded = false
			}

			events = append(events, &eventbus.StoredEvent{
				Offset:    eventOffset,
				Type:      eventWithOffset.Type,
				Data:      eventWithOffset.Data,
				Timestamp: parseTimestamp(eventWithOffset.Timestamp),
			})
		}

		if len(events) == 0 {
			// A chunk with zero decodable events (empty tail read, empty
			// mid-stream chunk, or every event skipped as malformed).
			// Mid-stream, returning an empty batch with an advanced offset
			// would look like end-of-log to callers, so advance to the next
			// chunk and keep reading. At the tail — or if the server does
			// not advance the offset — this is a genuine empty result.
			if result.UpToDate || result.NextOffset == offset {
				return nil, eventbus.Offset(result.NextOffset), nil
			}
			chunkStart = eventbus.Offset(result.NextOffset)
			offset = result.NextOffset
			continue
		}

		// Apply limit only when truncation is resumable: the returned nextOffset
		// must point at the last event actually returned, otherwise the events
		// beyond limit would be skipped by the caller's next Read.
		if limit > 0 && len(events) > limit && allEmbedded {
			events = events[:limit]
			return events, events[len(events)-1].Offset, nil
		}

		return events, eventbus.Offset(result.NextOffset), nil
	}
}

// resolveTail resolves OffsetNewest to the stream's current tail via a HEAD
// request, with the same retry and per-attempt timeout policy as Read. The
// returned offset is concrete and server-issued: reading from it returns
// only events appended after the call. An empty stream resolves to the
// server's initial offset; a missing stream returns an error.
func (s *Store) resolveTail(ctx context.Context) (eventbus.Offset, error) {
	var lastErr error
	for attempt := 1; attempt <= s.cfg.retryAttempts; attempt++ {
		if attempt > 1 {
			if err := backoff(ctx, s.cfg.retryBaseDelay, attempt-1); err != nil {
				return "", fmt.Errorf("durablestream: resolve tail: %w", err)
			}
		}
		if err := ctx.Err(); err != nil {
			return "", fmt.Errorf("durablestream: resolve tail: %w", err)
		}

		attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
		info, err := s.client.Head(attemptCtx, s.path)
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("durablestream: resolve tail: %w", err)
			if ctx.Err() != nil || !isRetryable(err) {
				return "", lastErr
			}
			continue
		}
		return eventbus.Offset(info.NextOffset), nil
	}
	return "", fmt.Errorf("durablestream: resolve tail: giving up after %d attempts: %w", s.cfg.retryAttempts, lastErr)
}

// handleDecodeError reports a malformed stored event that is being skipped.
// The decode error handler takes precedence; the logger is the fallback.
// When neither is configured the event is skipped silently.
func (s *Store) handleDecodeError(index int, raw []byte, err error) {
	if s.cfg.decodeErrorHandler != nil {
		s.cfg.decodeErrorHandler(err, raw)
		return
	}
	if s.cfg.logger != nil {
		s.cfg.logger.Printf("durablestream: skipping malformed event at index %d: %v", index, err)
	}
}

// isRetryable reports whether an error is worth retrying: network errors
// and server-side failures (HTTP 5xx, 429) are transient; protocol errors
// (not found, conflict, bad request, gone) are permanent.
func isRetryable(err error) bool {
	if errors.Is(err, durablestream.ErrNotFound) ||
		errors.Is(err, durablestream.ErrConflict) ||
		errors.Is(err, durablestream.ErrBadRequest) ||
		errors.Is(err, durablestream.ErrGone) {
		return false
	}
	var tErr *transport.Error
	if errors.As(err, &tErr) {
		return tErr.StatusCode >= 500 ||
			tErr.StatusCode == http.StatusTooManyRequests ||
			tErr.StatusCode == 0
	}
	// Network errors, stale-writer context errors, etc.
	return true
}

// maxBackoffShift caps the exponential backoff doubling so large retry
// counts cannot overflow the shift (base<<16 of the 100ms default is ~2h,
// already far beyond any sensible wait).
const maxBackoffShift = 16

// backoff waits for the exponential backoff delay before the given retry
// (retry is 1-based: the first retry waits base, the second 2*base, ...,
// capped at base<<maxBackoffShift). It returns early with the context's
// error if ctx is done.
func backoff(ctx context.Context, base time.Duration, retry int) error {
	timer := time.NewTimer(base << min(retry-1, maxBackoffShift))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Close is a no-op for HTTP-based stores.
func (s *Store) Close() error {
	return nil
}

// parseTimestamp parses a timestamp string and returns a time.Time.
// Uses RFC3339Nano which is a superset of RFC3339. Returns zero time on failure.
func parseTimestamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// Client returns the underlying durablestream.Client for advanced usage.
// This is exposed for testing and advanced scenarios.
func (s *Store) Client() *durablestream.Client {
	return s.client
}

// Path returns the stream path.
func (s *Store) Path() string {
	return s.path
}

// HTTPClient returns the HTTP client used by the store.
// Exposed for testing.
func (s *Store) HTTPClient() *http.Client {
	return s.cfg.httpClient
}
