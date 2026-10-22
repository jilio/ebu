package sqlite

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

func TestTimestampsInAnyZoneReadBack(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parsedOffset, err := time.Parse(time.RFC3339Nano, "2026-03-04T05:06:07.123456789+05:30")
	if err != nil {
		t.Fatal(err)
	}
	stamps := []time.Time{
		{},
		time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
		time.Date(2026, 1, 2, 3, 4, 5, 6, time.Local),
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("UTC+5", 5*3600)),
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("", -7*3600)),
		parsedOffset,
	}
	for _, ts := range stamps {
		if _, err := store.Append(t.Context(), &eventbus.Event{Type: "t", Data: json.RawMessage(`1`), Timestamp: ts}); err != nil {
			t.Fatal(err)
		}
	}
	events, _, err := store.Read(t.Context(), eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	for i, event := range events {
		if !event.Timestamp.Equal(stamps[i]) {
			t.Errorf("event %d: timestamp %v, want the instant %v", i, event.Timestamp, stamps[i])
		}
	}
}

func TestStoredTimeScan(t *testing.T) {
	want := time.Date(2026, 1, 2, 3, 4, 5, 600000000, time.FixedZone("", 5*3600))
	for _, value := range []any{
		want,
		"2026-01-02 03:04:05.6 +0500 +0500",
		[]byte("2026-01-02 03:04:05.6 +0500 PKT"),
		"2026-01-01T22:04:05.6Z",
	} {
		var got storedTime
		if err := got.Scan(value); err != nil || !got.Equal(want) {
			t.Errorf("Scan(%v) = %v, %v", value, got.Time, err)
		}
	}
	var got storedTime
	if err := got.Scan("2026-01-02 03:04:05"); err != nil || !got.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("zone-less text = %v, %v", got.Time, err)
	}
	if err := got.Scan(nil); err != nil || !got.IsZero() {
		t.Errorf("Scan(nil) = %v, %v", got.Time, err)
	}
	if err := got.Scan("yesterday"); err == nil || !strings.Contains(err.Error(), "unparsable timestamp") {
		t.Errorf("Scan(garbage) = %v", err)
	}
	if err := got.Scan(42); err == nil || !strings.Contains(err.Error(), "unsupported timestamp type int") {
		t.Errorf("Scan(int) = %v", err)
	}
}
