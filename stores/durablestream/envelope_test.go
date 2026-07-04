package durablestream_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
	ds "github.com/jilio/ebu/stores/durablestream"
)

// TestEnvelopeRoundTrip verifies that ID, Origin, and Metadata survive
// Append -> Read through a real durable-streams protocol handler.
func TestEnvelopeRoundTrip(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "envelope-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	want := &eventbus.Event{
		ID:        "01JTESTID0000000000000000A",
		Origin:    "origin-1",
		Type:      "test.event",
		Data:      json.RawMessage(`{"v":1}`),
		Metadata:  map[string]string{"correlation_id": "req-1"},
		Timestamp: time.Now(),
	}
	if _, err := store.Append(ctx, want); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	got := events[0]
	if got.ID != want.ID {
		t.Errorf("ID: got %q, want %q", got.ID, want.ID)
	}
	if got.Origin != want.Origin {
		t.Errorf("Origin: got %q, want %q", got.Origin, want.Origin)
	}
	if got.Metadata["correlation_id"] != "req-1" {
		t.Errorf("Metadata: got %v", got.Metadata)
	}
}

// TestEnvelopeAbsentFieldsOmitted verifies that events without envelope
// fields serialize without the keys (wire compatibility with pre-envelope
// events) and read back empty.
func TestEnvelopeAbsentFieldsOmitted(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "envelope-empty-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	if _, err := store.Append(ctx, &eventbus.Event{
		Type: "bare.event",
		Data: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("Read: %d events, err=%v", len(events), err)
	}
	if events[0].ID != "" || events[0].Origin != "" || events[0].Metadata != nil {
		t.Errorf("expected empty envelope, got ID=%q Origin=%q Metadata=%v",
			events[0].ID, events[0].Origin, events[0].Metadata)
	}
}
