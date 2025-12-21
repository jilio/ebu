// Package durablestream implements the ebu EventStore interface for
// durable-streams servers (https://github.com/durable-streams/durable-streams).
//
// Durable-streams is an HTTP-based protocol for real-time sync to client
// applications. It uses opaque string offsets and supports both catch-up
// reads and live tailing via SSE.
//
// This implementation wraps the conformance-tested ahimsalabs/durable-streams-go
// client library for full protocol compatibility.
package durablestream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	eventbus "github.com/jilio/ebu"
)

// Store implements eventbus.EventStore for durable-streams servers.
type Store struct {
	client *durablestream.Client
	path   string
	cfg    *config
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

	clientCfg := &durablestream.ClientConfig{
		HTTPClient: cfg.httpClient,
		Timeout:    cfg.timeout,
	}

	client := durablestream.NewClient(baseURL, clientCfg)

	// Try to create the stream (idempotent)
	_, err := client.Create(ctx, streamPath, &durablestream.CreateOptions{
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
func (s *Store) Append(ctx context.Context, event *eventbus.Event) (eventbus.Offset, error) {
	writer, err := s.client.Writer(ctx, s.path)
	if err != nil {
		return "", fmt.Errorf("durablestream: get writer: %w", err)
	}

	// Prepare event for JSON serialization
	writeEvent := storedEventForWrite{
		Type: event.Type,
		Data: event.Data,
	}
	if !event.Timestamp.IsZero() {
		writeEvent.Timestamp = event.Timestamp.Format(time.RFC3339Nano)
	}

	if err := writer.SendJSON(writeEvent, nil); err != nil {
		return "", fmt.Errorf("durablestream: send: %w", err)
	}

	return eventbus.Offset(writer.Offset()), nil
}

// storedEventWithOffset is used to parse events that include their own offset.
type storedEventWithOffset struct {
	Offset    string          `json:"offset,omitempty"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp string          `json:"timestamp,omitempty"`
}

// Read returns events starting after the given offset.
//
// Offset handling:
//   - If events in the JSON include an "offset" field, those are used directly
//   - Otherwise, synthetic offsets are generated in format "nextOffset/index"
//
// IMPORTANT: Synthetic offsets (containing "/") are ephemeral and should NOT
// be stored for resumption. They only ensure uniqueness within a single Read call.
// For reliable resumption, use the nextOffset returned by Read, or ensure your
// durable-streams server includes per-event offsets in the response.
func (s *Store) Read(ctx context.Context, from eventbus.Offset, limit int) ([]*eventbus.StoredEvent, eventbus.Offset, error) {
	// Map OffsetOldest to durable-streams zero offset
	offset := durablestream.Offset(from)
	if from == eventbus.OffsetOldest {
		offset = durablestream.ZeroOffset
	}

	reader := s.client.Reader(s.path, offset)
	result, err := reader.Read(ctx)
	if err != nil {
		return nil, from, fmt.Errorf("durablestream: read: %w", err)
	}

	// Empty response means we're at the tail
	if len(result.Data) == 0 {
		return nil, eventbus.Offset(result.NextOffset), nil
	}

	// Parse JSON array response
	var rawEvents []json.RawMessage
	if err := json.Unmarshal(result.Data, &rawEvents); err != nil {
		return nil, from, fmt.Errorf("durablestream: unmarshal response: %w", err)
	}

	// Convert to StoredEvents
	events := make([]*eventbus.StoredEvent, 0, len(rawEvents))
	for i, raw := range rawEvents {
		// Try to parse as event with embedded offset first
		var eventWithOffset storedEventWithOffset
		if err := json.Unmarshal(raw, &eventWithOffset); err != nil {
			// Log and skip malformed events
			if s.cfg.logger != nil {
				s.cfg.logger.Printf("durablestream: skipping malformed event at index %d: %v", i, err)
			}
			continue
		}

		// Determine offset: use embedded offset if present, otherwise synthesize
		var eventOffset eventbus.Offset
		if eventWithOffset.Offset != "" {
			eventOffset = eventbus.Offset(eventWithOffset.Offset)
		} else {
			// Synthesize unique offset: "nextOffset/index"
			// These are ephemeral - use nextOffset for reliable resumption
			eventOffset = eventbus.Offset(fmt.Sprintf("%s/%d", result.NextOffset, i))
		}

		events = append(events, &eventbus.StoredEvent{
			Offset:    eventOffset,
			Type:      eventWithOffset.Type,
			Data:      eventWithOffset.Data,
			Timestamp: parseTimestamp(eventWithOffset.Timestamp),
		})

		// Apply limit if specified
		if limit > 0 && len(events) >= limit {
			break
		}
	}

	return events, eventbus.Offset(result.NextOffset), nil
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
