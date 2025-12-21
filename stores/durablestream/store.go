// Package durablestream implements the ebu EventStore interface for
// durable-streams servers (https://github.com/durable-streams/durable-streams).
//
// Durable-streams is an HTTP-based protocol for real-time sync to client
// applications. It uses opaque string offsets and supports both catch-up
// reads and live tailing via SSE.
package durablestream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	eventbus "github.com/jilio/ebu"
)

// Store implements eventbus.EventStore for durable-streams servers.
type Store struct {
	client *Client
	cfg    *config
}

// Ensure Store implements the EventStore interface.
var _ eventbus.EventStore = (*Store)(nil)

// New creates a new Store connected to a durable-streams server.
//
// The streamURL should be the full URL to the stream endpoint,
// e.g., "https://server.example.com/v1/stream/my-events".
//
// By default, the store will attempt to create the stream if it doesn't exist.
// Note: Stream creation uses context.Background() to ensure it completes fully.
// Use NewWithContext if you need cancellable initialization.
func New(streamURL string, opts ...Option) (*Store, error) {
	return NewWithContext(context.Background(), streamURL, opts...)
}

// NewWithContext creates a new Store with a context for initialization.
// The context is used for the initial stream creation request.
func NewWithContext(ctx context.Context, streamURL string, opts ...Option) (*Store, error) {
	if streamURL == "" {
		return nil, fmt.Errorf("durablestream: streamURL is required")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	client := newClient(streamURL, cfg)

	// Try to create the stream (idempotent)
	if err := client.Create(ctx); err != nil {
		return nil, fmt.Errorf("durablestream: create stream: %w", err)
	}

	return &Store{
		client: client,
		cfg:    cfg,
	}, nil
}

// Append stores an event and returns its assigned offset.
func (s *Store) Append(ctx context.Context, event *eventbus.Event) (eventbus.Offset, error) {
	// For JSON mode, wrap the event in an array for batch semantics
	data, err := json.Marshal([]*eventbus.Event{event})
	if err != nil {
		return "", fmt.Errorf("durablestream: marshal event: %w", err)
	}

	offset, err := s.client.Append(ctx, data)
	if err != nil {
		return "", fmt.Errorf("durablestream: append: %w", err)
	}

	return eventbus.Offset(offset), nil
}

// storedEventWithOffset is used to parse events that include their own offset
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
	// Map OffsetOldest to durable-streams sentinel value
	offset := string(from)
	if from == eventbus.OffsetOldest {
		offset = "-1"
	}

	resp, err := s.client.Read(ctx, offset, limit)
	if err != nil {
		return nil, from, fmt.Errorf("durablestream: read: %w", err)
	}

	// Empty response means we're at the tail
	if len(resp.Body) == 0 || string(resp.Body) == "[]" {
		return nil, from, nil
	}

	// Parse JSON array response
	var rawEvents []json.RawMessage
	if err := json.Unmarshal(resp.Body, &rawEvents); err != nil {
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
			eventOffset = eventbus.Offset(fmt.Sprintf("%s/%d", resp.NextOffset, i))
		}

		events = append(events, &eventbus.StoredEvent{
			Offset:    eventOffset,
			Type:      eventWithOffset.Type,
			Data:      eventWithOffset.Data,
			Timestamp: parseTimestamp(eventWithOffset.Timestamp),
		})
	}

	// Apply limit if specified
	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	return events, eventbus.Offset(resp.NextOffset), nil
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
