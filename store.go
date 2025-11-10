package eventbus

import (
	"context"
	"time"
)

// Store defines the interface for event sourcing storage.
// Unlike EventStore which is used for event bus persistence,
// Store is designed for CQRS/ES patterns with streams and versions.
type Store interface {
	// Append appends events to a stream and returns the new version
	Append(ctx context.Context, streamID string, events []Event) (int64, error)

	// Read reads events from a stream starting from a version
	Read(ctx context.Context, streamID string, fromVersion int64) ([]Event, error)

	// ReadAll reads all events across all streams
	ReadAll(ctx context.Context, fromEventID string, limit int) ([]Event, error)

	// Close closes the store
	Close() error
}

// Event represents an event in an event-sourced system
type Event struct {
	ID        string    `json:"id"`
	StreamID  string    `json:"stream_id"`
	Type      string    `json:"type"`
	Data      []byte    `json:"data"`
	Metadata  []byte    `json:"metadata,omitempty"`
	Version   int64     `json:"version"`
	Timestamp time.Time `json:"timestamp"`
}
