package durablestream_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
	ds "github.com/jilio/ebu/stores/durablestream"
)

// TestTailCatchUpAndLive verifies Tail against a real protocol handler:
// existing events are caught up, and an event appended while the tail is
// live arrives without restarting the iterator.
func TestTailCatchUpAndLive(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "tail-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appendEvent := func(v int) {
		t.Helper()
		if _, err := store.Append(ctx, &eventbus.Event{
			ID:   eventbus.NewEventID(),
			Type: "tail.event",
			Data: json.RawMessage(`{}`),
		}); err != nil {
			t.Fatalf("Append(%d) error = %v", v, err)
		}
	}

	appendEvent(1)
	appendEvent(2)

	var mu sync.Mutex
	var got []*eventbus.StoredEvent
	tailDone := make(chan struct{})
	go func() {
		defer close(tailDone)
		for event, err := range store.Tail(ctx, eventbus.OffsetOldest) {
			if err != nil {
				if ctx.Err() == nil {
					t.Errorf("Tail yielded error: %v", err)
				}
				return
			}
			mu.Lock()
			got = append(got, event)
			mu.Unlock()
		}
	}()

	waitCount := func(n int, what string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			count := len(got)
			mu.Unlock()
			if count >= n {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}

	waitCount(2, "catch-up events")

	appendEvent(3) // arrives via the live long-poll
	waitCount(3, "live event")

	mu.Lock()
	for i, ev := range got {
		if ev.Type != "tail.event" {
			t.Errorf("event %d: unexpected type %q", i, ev.Type)
		}
		if ev.ID == "" {
			t.Errorf("event %d: envelope ID lost in tail path", i)
		}
	}
	mu.Unlock()

	cancel()
	select {
	case <-tailDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Tail did not end after context cancellation")
	}
}

// TestTailFromNewestSkipsHistory verifies OffsetNewest resolution: only
// events appended after the tail starts are yielded.
func TestTailFromNewestSkipsHistory(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "tail-newest-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := store.Append(ctx, &eventbus.Event{Type: "old", Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var got []string
	go func() {
		for event, err := range store.Tail(ctx, eventbus.OffsetNewest) {
			if err != nil {
				return
			}
			mu.Lock()
			got = append(got, event.Type)
			mu.Unlock()
		}
	}()

	// Let the tail resolve the tip and enter live mode, then append.
	time.Sleep(100 * time.Millisecond)
	if _, err := store.Append(ctx, &eventbus.Event{Type: "new", Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(got) >= 1
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "new" {
		t.Errorf("expected only the post-tail event, got %v", got)
	}
}

// TestTailMissingStreamYieldsError verifies permanent errors end the tail.
func TestTailMissingStreamYieldsError(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "tail-error-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	// Point at a stream that was never created by deleting it first.
	if err := store.Client().Delete(context.Background(), "tail-error-test"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var tailErr error
	for _, err := range store.Tail(ctx, eventbus.OffsetOldest) {
		if err != nil {
			tailErr = err
			break
		}
	}
	if tailErr == nil {
		t.Fatal("expected the tail to yield an error for a missing stream")
	}
	if errors.Is(tailErr, context.DeadlineExceeded) {
		t.Fatalf("tail should fail fast on a permanent error, got %v", tailErr)
	}
}
