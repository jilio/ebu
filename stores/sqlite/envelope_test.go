package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

// TestEnvelopeRoundTrip verifies that ID, Origin, and Metadata survive
// Append -> Read and Append -> ReadStream unchanged.
func TestEnvelopeRoundTrip(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	want := &eventbus.Event{
		ID:        "01JTESTID0000000000000000A",
		Origin:    "origin-1",
		Type:      "test.event",
		Data:      json.RawMessage(`{"v":1}`),
		Metadata:  map[string]string{"correlation_id": "req-1", "tenant": "acme"},
		Timestamp: time.Now().UTC(),
	}
	if _, err := store.Append(ctx, want); err != nil {
		t.Fatal(err)
	}

	check := func(t *testing.T, got *eventbus.StoredEvent) {
		t.Helper()
		if got.ID != want.ID {
			t.Errorf("ID: got %q, want %q", got.ID, want.ID)
		}
		if got.Origin != want.Origin {
			t.Errorf("Origin: got %q, want %q", got.Origin, want.Origin)
		}
		if got.Metadata["correlation_id"] != "req-1" || got.Metadata["tenant"] != "acme" {
			t.Errorf("Metadata: got %v", got.Metadata)
		}
	}

	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("Read: %d events, err=%v", len(events), err)
	}
	check(t, events[0])

	var streamed *eventbus.StoredEvent
	for event, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
		if err != nil {
			t.Fatal(err)
		}
		streamed = event
	}
	if streamed == nil {
		t.Fatal("ReadStream yielded nothing")
	}
	check(t, streamed)
}

// TestEnvelopeEmptyFieldsStoredAsNull verifies that events published without
// an envelope (or through an older core) read back with empty fields, and
// that the columns are actually NULL rather than empty strings.
func TestEnvelopeEmptyFieldsStoredAsNull(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	if _, err := store.Append(ctx, &eventbus.Event{
		Type: "bare.event", Data: json.RawMessage(`{}`), Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("Read: %d events, err=%v", len(events), err)
	}
	if events[0].ID != "" || events[0].Origin != "" || events[0].Metadata != nil {
		t.Errorf("expected empty envelope, got ID=%q Origin=%q Metadata=%v",
			events[0].ID, events[0].Origin, events[0].Metadata)
	}

	var nulls int
	if err := store.GetDB().QueryRow(
		"SELECT COUNT(*) FROM events WHERE event_id IS NULL AND origin IS NULL AND metadata IS NULL",
	).Scan(&nulls); err != nil {
		t.Fatal(err)
	}
	if nulls != 1 {
		t.Errorf("expected envelope columns to be NULL, got %d matching rows", nulls)
	}
}

// TestEnvelopeLegacyRowsReadBack verifies rows written before schema v4
// (envelope columns absent entirely) read back with empty envelope fields
// after the migration adds the columns.
func TestEnvelopeLegacyRowsReadBack(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	// Simulate a legacy row: write only the pre-v4 columns.
	if _, err := store.GetDB().Exec(
		"INSERT INTO events (type, data, timestamp) VALUES ('legacy.event', x'7b7d', CURRENT_TIMESTAMP)",
	); err != nil {
		t.Fatal(err)
	}

	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("Read: %d events, err=%v", len(events), err)
	}
	if events[0].Type != "legacy.event" {
		t.Errorf("Type: got %q", events[0].Type)
	}
	if events[0].ID != "" || events[0].Origin != "" || events[0].Metadata != nil {
		t.Errorf("legacy row must have an empty envelope, got ID=%q Origin=%q Metadata=%v",
			events[0].ID, events[0].Origin, events[0].Metadata)
	}
}

// TestEnvelopeCorruptMetadataErrors verifies that unreadable metadata JSON
// surfaces as a scan error instead of being silently dropped.
func TestEnvelopeCorruptMetadataErrors(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	if _, err := store.GetDB().Exec(
		"INSERT INTO events (type, data, timestamp, metadata) VALUES ('t', x'7b7d', CURRENT_TIMESTAMP, 'not-json')",
	); err != nil {
		t.Fatal(err)
	}

	if _, _, err := store.Read(ctx, eventbus.OffsetOldest, 0); err == nil {
		t.Error("expected an error for corrupt metadata JSON")
	}
}

// TestAppendUnmarshalableMetadata exercises the marshal-failure guard in
// Append. Metadata is map[string]string, which cannot itself fail to
// marshal, so this documents the guard via the exported test hook instead.
func TestMarshalMetadataEmpty(t *testing.T) {
	v, err := marshalMetadata(nil)
	if err != nil || v != nil {
		t.Errorf("nil metadata must marshal to NULL, got %v err=%v", v, err)
	}
	v, err = marshalMetadata(map[string]string{})
	if err != nil || v != nil {
		t.Errorf("empty metadata must marshal to NULL, got %v err=%v", v, err)
	}
	v, err = marshalMetadata(map[string]string{"k": "v"})
	if err != nil || v != `{"k":"v"}` {
		t.Errorf("metadata must marshal to a JSON object, got %v err=%v", v, err)
	}

	if nullifyEmpty("") != nil {
		t.Error("empty string must map to NULL")
	}
	if nullifyEmpty("x") != "x" {
		t.Error("non-empty string must pass through")
	}
}
