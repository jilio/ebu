package sqlite

import (
	"context"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

// TestOffsetsAreLexicographic verifies the eventbus.Offset contract: offsets
// must compare lexicographically. Plain decimal strings break at the 999→1000
// boundary ("999" > "1000" as strings), which is why offsets are zero-padded.
func TestOffsetsAreLexicographic(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	var prev eventbus.Offset
	for i := 0; i < 1001; i++ {
		offset, err := store.Append(ctx, &eventbus.Event{
			Type:      "test.event",
			Data:      []byte(`{}`),
			Timestamp: time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if offset <= prev {
			t.Fatalf("offset %q not lexicographically greater than previous %q (append #%d)", offset, prev, i+1)
		}
		prev = offset
	}
}

// TestLegacyUnpaddedOffsetsAccepted verifies offsets stored before the
// zero-padded format (plain "123") still parse and resume correctly.
func TestLegacyUnpaddedOffsetsAccepted(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := store.Append(ctx, &eventbus.Event{Type: "test.event", Data: []byte(`{}`), Timestamp: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}

	events, _, err := store.Read(ctx, eventbus.Offset("3"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 events after legacy offset 3, got %d", len(events))
	}
}

// TestMemoryStoresAreIsolated verifies that two :memory: stores in the same
// process do not share a database through SQLite's shared cache.
func TestMemoryStoresAreIsolated(t *testing.T) {
	ctx := context.Background()

	store1, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store1.Close()

	store2, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	if _, err := store1.Append(ctx, &eventbus.Event{Type: "test.event", Data: []byte(`{}`), Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}

	events, _, err := store2.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf(":memory: stores share state: store2 sees %d events from store1", len(events))
	}
}
