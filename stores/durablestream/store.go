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
func New(streamURL string, opts ...Option) (*Store, error) {
	if streamURL == "" {
		return nil, fmt.Errorf("durablestream: streamURL is required")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	client := newClient(streamURL, cfg)

	// Try to create the stream (idempotent)
	if err := client.Create(context.Background()); err != nil {
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

// Read returns events starting after the given offset.
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
	for _, raw := range rawEvents {
		var event eventbus.Event
		if err := json.Unmarshal(raw, &event); err != nil {
			// Skip malformed events
			continue
		}

		events = append(events, &eventbus.StoredEvent{
			Offset:    eventbus.Offset(resp.NextOffset),
			Type:      event.Type,
			Data:      event.Data,
			Timestamp: event.Timestamp,
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
