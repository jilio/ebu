// Package durablestream implements the ebu EventStore interface for
// durable-streams servers (https://github.com/durable-streams/durable-streams).
//
// Durable-streams is an HTTP-based protocol for real-time sync to client
// applications. It uses opaque string offsets and supports both catch-up
// reads and live tailing.
//
// This implementation wraps the official Go client
// (github.com/durable-streams/durable-streams/packages/client-go).
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
// failed before commit from one that committed but lost its response, so a
// retried append may store the event twice. Combined with the offset
// semantics above, delivery is at-least-once end-to-end; consumers must
// deduplicate (e.g. on Event.ID) if exactly-once processing is required.
// Appends are independent POSTs, safe for concurrent use; the server orders
// them.
//
// # Snapshots and retention
//
// The store implements eventbus.EventStoreSnapshotter on a companion stream
// (`<streamPath>.snap`, created lazily; that path is reserved). Each
// SaveSnapshot appends one record atomically; LoadSnapshot returns the newest
// record for the id (last-write-wins). Protocol offsets stay valid for the
// lifetime of a stream, so a snapshot's offset remains a correct resume point
// even after the server drops older events.
//
// The store deliberately does NOT implement eventbus.EventStoreTruncator:
// the Durable Streams protocol has no client-initiated trim — retention is a
// server-side policy ("servers MAY implement retention policies that drop
// data older than a certain age while the stream continues"), and offsets on
// a remote append-only stream are not deletable positions (see the
// EventStoreTruncator contract).
//
// Retention rules of thumb: the MAIN stream may be head-trimmed by server
// retention once snapshots cover the trimmed prefix — a read below the
// earliest retained position fails with the protocol's 410 Gone (permanent
// here); recover by loading a snapshot and resuming from its offset. The
// COMPANION stream must NOT be head-trimmed: LoadSnapshot reads it from the
// beginning and the protocol offers no earliest-offset discovery, so a
// partially trimmed companion stream makes surviving records unreachable
// (LoadSnapshot fails loudly rather than reporting a false miss). Expiring
// the companion stream whole (stream TTL) is safe — it reads as "no
// snapshot", and the next SaveSnapshot re-creates it — but note it also
// discards the newest snapshot, so the next cold start replays the full
// main stream.
//
// Budget the companion stream deliberately: it grows by one full record per
// save, and LoadSnapshot downloads all of it to return the newest record, so
// cost is save cadence × blob size. Save on meaningful deltas rather than a
// tight timer. A cheap LoadSnapshot needs protocol support (client trim, or
// earliest-offset/last-record discovery); until servers offer that, this
// client keeps LoadSnapshot correct rather than cheap.
package durablestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"strings"
	"sync"
	"time"

	durablestreams "github.com/durable-streams/durable-streams/packages/client-go"
	eventbus "github.com/jilio/ebu"
)

// Store implements eventbus.EventStore for durable-streams servers.
type Store struct {
	client     *durablestreams.Client
	stream     *durablestreams.Stream
	snapStream *durablestreams.Stream
	streamURL  string
	path       string
	cfg        *config

	// snap tracks the lazily created snapshot companion stream (see
	// snapshot.go).
	snap snapState
}

// Ensure Store implements the EventStore and EventStoreTailer interfaces.
var _ eventbus.EventStore = (*Store)(nil)
var _ eventbus.EventStoreTailer = (*Store)(nil)

// New creates a new Store connected to a durable-streams server.
//
// The baseURL should be the base URL under which streams live
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
	if strings.HasSuffix(streamPath, snapStreamSuffix) {
		// The ".snap" suffix names the snapshot companion stream of the store
		// at the base path (see EventStoreSnapshotter in snapshot.go). An
		// event store there would silently interleave its envelopes with that
		// store's snapshot records — neither side would notice, both would be
		// corrupted. Reject at construction instead.
		return nil, fmt.Errorf("durablestream: streamPath %q uses the reserved snapshot companion suffix %q", streamPath, snapStreamSuffix)
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// The client applies no global timeout of its own; a hung server would
	// block forever. When no custom client is supplied, build one carrying
	// the configured timeout. This same timeout is what bounds an idle
	// long-poll in Tail (see there).
	if cfg.httpClient == nil {
		cfg.httpClient = &http.Client{Timeout: cfg.timeout}
	}

	// Retries are owned by this store (see WithRetry): the client's internal
	// retry policy is disabled so one protocol call consumes exactly one of
	// our attempts, keeping retry accounting observable and configurable.
	client := durablestreams.NewClient(
		durablestreams.WithHTTPClient(cfg.httpClient),
		durablestreams.WithRetryPolicy(durablestreams.RetryPolicy{
			MaxRetries:   0,
			InitialDelay: time.Millisecond,
			MaxDelay:     time.Millisecond,
			Multiplier:   1,
		}),
	)

	streamURL := strings.TrimSuffix(baseURL, "/") + "/" + streamPath
	stream := client.Stream(streamURL)
	snapStream := client.Stream(streamURL + snapStreamSuffix)
	snapStream.SetContentType("application/json")

	// Try to create the stream (idempotent). Bound the request so a hung
	// server cannot block initialization forever.
	createCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	if err := stream.Create(createCtx, durablestreams.WithContentType(cfg.contentType)); err != nil {
		return nil, fmt.Errorf("durablestream: create stream: %w", err)
	}

	return &Store{
		client:     client,
		stream:     stream,
		snapStream: snapStream,
		streamURL:  streamURL,
		path:       streamPath,
		cfg:        cfg,
	}, nil
}

// storedEventForWrite represents the event format for writing to the stream.
// The envelope fields (id, origin, metadata) are omitted when empty so
// streams stay byte-compatible with events written by earlier versions.
type storedEventForWrite struct {
	ID        string            `json:"id,omitempty"`
	Origin    string            `json:"origin,omitempty"`
	Type      string            `json:"type"`
	Data      json.RawMessage   `json:"data"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp string            `json:"timestamp,omitempty"`
}

// toWireOffset maps an ebu offset to the protocol client's representation.
// OffsetOldest becomes the protocol's start-of-stream token; server-issued
// offsets pass through opaquely.
func toWireOffset(o eventbus.Offset) durablestreams.Offset {
	if o == eventbus.OffsetOldest {
		return durablestreams.StartOffset
	}
	return durablestreams.Offset(o)
}

// fromWireOffset maps a protocol offset back to the ebu domain: any
// start-of-stream representation becomes OffsetOldest.
func fromWireOffset(o durablestreams.Offset) eventbus.Offset {
	if o.IsStart() {
		return eventbus.OffsetOldest
	}
	return eventbus.Offset(o)
}

// Append stores an event and returns its assigned offset.
// Safe for concurrent use: each append is an independent POST and the server
// orders them.
//
// Offset semantics: the returned offset is the server-issued next-offset
// from the append response — the position immediately after the appended
// event. Resuming a Read from it returns events appended strictly after
// this one: the event itself is not re-delivered and no later event is
// skipped. It is safe to persist (e.g., via SaveOffset).
//
// Transient failures (network errors, HTTP 5xx, 429) are retried with
// exponential backoff; see WithRetry. Appends are at-least-once: a retry of
// a request that committed server-side but lost its response stores the
// event again. Duplicates are documented rather than masked; consumers
// deduplicate on Event.ID.
func (s *Store) Append(ctx context.Context, event *eventbus.Event) (eventbus.Offset, error) {
	writeEvent := storedEventForWrite{
		ID:       event.ID,
		Origin:   event.Origin,
		Type:     event.Type,
		Data:     event.Data,
		Metadata: event.Metadata,
	}
	if !event.Timestamp.IsZero() {
		writeEvent.Timestamp = event.Timestamp.Format(time.RFC3339Nano)
	}

	data, err := json.Marshal(writeEvent)
	if err != nil {
		return "", fmt.Errorf("durablestream: marshal event: %w", err)
	}

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

		attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
		result, err := s.stream.Append(attemptCtx, data)
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("durablestream: append: %w", err)
			if ctx.Err() != nil || !isRetryable(err) {
				return "", lastErr
			}
			continue
		}
		return eventbus.Offset(result.NextOffset), nil
	}

	return "", fmt.Errorf("durablestream: append: giving up after %d attempts: %w", s.cfg.retryAttempts, lastErr)
}

// storedEventWithOffset is used to parse events that include their own offset.
type storedEventWithOffset struct {
	Offset    string            `json:"offset,omitempty"`
	ID        string            `json:"id,omitempty"`
	Origin    string            `json:"origin,omitempty"`
	Type      string            `json:"type"`
	Data      json.RawMessage   `json:"data"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp string            `json:"timestamp,omitempty"`
}

// readChunk fetches one chunk after offset with the store's retry policy.
// It returns (nil, tail, nil) when the stream has no data after offset (the
// protocol's caught-up response), where tail is the concrete resume offset.
func (s *Store) readChunk(ctx context.Context, offset eventbus.Offset) (*durablestreams.Chunk, eventbus.Offset, error) {
	var lastErr error
	for attempt := 1; attempt <= s.cfg.retryAttempts; attempt++ {
		if attempt > 1 {
			if err := backoff(ctx, s.cfg.retryBaseDelay, attempt-1); err != nil {
				return nil, "", err
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}

		// One iterator per attempt, consumed for exactly one chunk: each
		// attempt gets its own timeout window so a hung server fails the
		// attempt instead of blocking Read forever.
		attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
		it := s.stream.Read(attemptCtx, durablestreams.WithOffset(toWireOffset(offset)))
		chunk, err := it.Next()
		resolved := fromWireOffset(it.Offset)
		it.Close()
		cancel()
		if err != nil {
			if errors.Is(err, durablestreams.Done) {
				// Caught up with no data: the iterator's offset is the
				// concrete resume point (the request offset when the server
				// reported none).
				if resolved == eventbus.OffsetOldest && offset != eventbus.OffsetOldest {
					resolved = offset
				}
				return nil, resolved, nil
			}
			lastErr = err
			if ctx.Err() != nil || !isRetryable(err) {
				return nil, "", lastErr
			}
			continue
		}
		return chunk, "", nil
	}
	return nil, "", fmt.Errorf("giving up after %d attempts: %w", s.cfg.retryAttempts, lastErr)
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
	// and a concrete server-issued offset to resume from.
	if from == eventbus.OffsetNewest {
		tail, err := s.resolveTail(ctx)
		if err != nil {
			return nil, from, err
		}
		return nil, tail, nil
	}

	// chunkStart is the offset the current chunk was read from; it is the
	// resume-safe offset for non-last events without embedded offsets.
	chunkStart := from
	offset := from
	for {
		chunk, tail, err := s.readChunk(ctx, offset)
		if err != nil {
			return nil, from, fmt.Errorf("durablestream: read: %w", err)
		}
		if chunk == nil {
			// Caught up with no data after offset.
			return nil, tail, nil
		}

		next := eventbus.Offset(chunk.NextOffset)
		events, allEmbedded, err := s.decodeChunk(chunk.Data, next, chunkStart)
		if err != nil {
			return nil, from, err
		}

		if len(events) == 0 {
			// A chunk with zero decodable events (empty tail read, empty
			// mid-stream chunk, or every event skipped as malformed).
			// Mid-stream, returning an empty batch with an advanced offset
			// would look like end-of-log to callers, so advance to the next
			// chunk and keep reading. At the tail — or if the server does
			// not advance the offset — this is a genuine empty result.
			if chunk.UpToDate || next == offset {
				return nil, next, nil
			}
			chunkStart = next
			offset = next
			continue
		}

		// Apply limit only when truncation is resumable: the returned nextOffset
		// must point at the last event actually returned, otherwise the events
		// beyond limit would be skipped by the caller's next Read.
		if limit > 0 && len(events) > limit && allEmbedded {
			events = events[:limit]
			return events, events[len(events)-1].Offset, nil
		}

		return events, next, nil
	}
}

// CompareOffsets orders two concrete, server-issued offsets using the Durable
// Streams protocol's lexicographic ordering rule (spec §6: offsets are
// lexicographically sortable). OffsetNewest is an ebu query sentinel rather
// than a protocol offset and cannot be compared.
func (*Store) CompareOffsets(left, right eventbus.Offset) (int, error) {
	if left == eventbus.OffsetNewest || right == eventbus.OffsetNewest {
		return 0, fmt.Errorf("durablestream: cannot compare symbolic offset %q", eventbus.OffsetNewest)
	}
	return strings.Compare(string(left), string(right)), nil
}

// decodeChunk converts one chunk body into StoredEvents, assigning each a
// resume-safe offset (see Read's offset semantics). chunkStart is the offset
// the chunk was read from; next is the server's next-offset after the chunk.
// allEmbedded reports whether every event carried its own embedded offset
// (exact resumption possible).
func (s *Store) decodeChunk(data []byte, next, chunkStart eventbus.Offset) (events []*eventbus.StoredEvent, allEmbedded bool, err error) {
	// Parse JSON array response (an empty body yields no events).
	var rawEvents []json.RawMessage
	if s.cfg.strictDecoding && strings.TrimSpace(string(data)) == "null" {
		return nil, false, fmt.Errorf("durablestream: decode chunk: null array")
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &rawEvents); err != nil {
			return nil, false, fmt.Errorf("durablestream: unmarshal response: %w", err)
		}
	}

	// Convert to StoredEvents
	events = make([]*eventbus.StoredEvent, 0, len(rawEvents))
	allEmbedded = true
	lastRaw := len(rawEvents) - 1
	for i, raw := range rawEvents {
		if s.cfg.strictDecoding && strings.TrimSpace(string(raw)) == "null" {
			return nil, false, fmt.Errorf("durablestream: decode event at index %d: null envelope", i)
		}
		// Try to parse as event with embedded offset first
		var eventWithOffset storedEventWithOffset
		if err := json.Unmarshal(raw, &eventWithOffset); err != nil {
			if s.cfg.strictDecoding {
				return nil, false, fmt.Errorf("durablestream: decode event at index %d: %w", i, err)
			}
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
			eventOffset = next
			allEmbedded = false
		default:
			// Chunk-start: resuming re-reads this chunk from the start,
			// re-delivering earlier events but never skipping later ones.
			eventOffset = chunkStart
			allEmbedded = false
		}

		events = append(events, &eventbus.StoredEvent{
			Offset:    eventOffset,
			ID:        eventWithOffset.ID,
			Origin:    eventWithOffset.Origin,
			Type:      eventWithOffset.Type,
			Data:      eventWithOffset.Data,
			Metadata:  eventWithOffset.Metadata,
			Timestamp: parseTimestamp(eventWithOffset.Timestamp),
		})
	}
	return events, allEmbedded, nil
}

// Tail implements eventbus.EventStoreTailer over the durable-streams live
// protocol: the client iterator catches up with plain reads, then switches to
// long-poll, so new events are pushed to the follower within one round trip
// instead of discovered by polling.
//
// Offset semantics match Read: per-event offsets are resume-safe but may
// re-deliver on restart (at-least-once); consumers deduplicate on ID.
//
// Retry policy: transient failures (network errors, HTTP 5xx, 429 — and
// notably client-side timeouts of an idle long-poll, bounded by the
// configured timeout via the HTTP client) are retried forever with capped
// exponential backoff, because an idle stream is indistinguishable from a
// transiently failing one. Permanent protocol errors (not found, gone, bad
// request) and undecodable chunks are yielded and end the tail. The iterator
// ends silently when ctx is cancelled.
func (s *Store) Tail(ctx context.Context, from eventbus.Offset) iter.Seq2[*eventbus.StoredEvent, error] {
	return func(yield func(*eventbus.StoredEvent, error) bool) {
		start := from
		if from == eventbus.OffsetNewest {
			tail, err := s.resolveTail(ctx)
			if err != nil {
				if ctx.Err() == nil {
					yield(nil, err)
				}
				return
			}
			start = tail
		}

		chunkStart := start
		newIterator := func() *durablestreams.ChunkIterator {
			return s.stream.Read(ctx,
				durablestreams.WithOffset(toWireOffset(chunkStart)),
				durablestreams.WithLive(durablestreams.LiveModeLongPoll),
			)
		}
		it := newIterator()
		defer func() { it.Close() }()
		retry := 0
		for {
			if ctx.Err() != nil {
				return
			}

			chunk, err := it.Next()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if !isRetryable(err) {
					yield(nil, fmt.Errorf("durablestream: tail: %w", err))
					return
				}
				// The iterator may hold a stale connection or cursor; rebuild
				// it at the last known chunk boundary and back off (capped —
				// see maxBackoffShift — never giving up: idle long-polls time
				// out client-side and look exactly like transient failures).
				retry++
				if backoff(ctx, s.cfg.retryBaseDelay, retry) != nil {
					return
				}
				it.Close()
				it = newIterator()
				continue
			}
			retry = 0

			if len(chunk.Data) == 0 {
				// A long-poll window that expired (204) or an empty chunk:
				// adopt any advanced resume token and keep waiting.
				if chunk.NextOffset != "" {
					chunkStart = eventbus.Offset(chunk.NextOffset)
				}
				continue
			}

			next := eventbus.Offset(chunk.NextOffset)
			events, _, err := s.decodeChunk(chunk.Data, next, chunkStart)
			if err != nil {
				yield(nil, err)
				return
			}
			for _, event := range events {
				if !yield(event, nil) {
					return
				}
			}
			chunkStart = next
		}
	}
}

// resolveTail resolves OffsetNewest to the stream's current tail via a HEAD
// request, with the same retry and per-attempt timeout policy as Read. The
// returned offset is concrete and server-issued: reading from it returns
// only events appended after the call. An empty stream resolves to
// OffsetOldest; a missing stream returns an error.
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
		meta, err := s.stream.Head(attemptCtx)
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("durablestream: resolve tail: %w", err)
			if ctx.Err() != nil || !isRetryable(err) {
				return "", lastErr
			}
			continue
		}
		return fromWireOffset(meta.NextOffset), nil
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
// (not found, conflict, bad request, gone, closed) are permanent.
func isRetryable(err error) bool {
	if errors.Is(err, durablestreams.ErrStreamNotFound) ||
		errors.Is(err, durablestreams.ErrStreamExists) ||
		errors.Is(err, durablestreams.ErrSeqConflict) ||
		errors.Is(err, durablestreams.ErrContentTypeMismatch) ||
		errors.Is(err, durablestreams.ErrStreamClosed) ||
		errors.Is(err, durablestreams.ErrBadRequest) ||
		errors.Is(err, durablestreams.ErrOffsetGone) {
		return false
	}
	var sErr *durablestreams.StreamError
	if errors.As(err, &sErr) {
		return sErr.StatusCode >= 500 ||
			sErr.StatusCode == http.StatusTooManyRequests ||
			sErr.StatusCode == 0
	}
	// Network errors, context timeouts wrapped by the client, etc.
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

// Client returns the underlying durable-streams client for advanced usage.
// This is exposed for testing and advanced scenarios.
func (s *Store) Client() *durablestreams.Client {
	return s.client
}

// Path returns the stream path.
func (s *Store) Path() string {
	return s.path
}

// StreamURL returns the full URL of the store's main stream.
func (s *Store) StreamURL() string {
	return s.streamURL
}

// HTTPClient returns the HTTP client used by the store.
// Exposed for testing.
func (s *Store) HTTPClient() *http.Client {
	return s.cfg.httpClient
}

// snapState tracks lazy creation of the companion stream, shared by all
// SaveSnapshot calls on this Store (see snapshot.go).
type snapState struct {
	mu      sync.Mutex
	created bool
}
