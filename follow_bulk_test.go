package eventbus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type followBulkEvent struct {
	N int `json:"n"`
}

// TestFollowBulkCatchUpDoesNotSleepBetweenBatches pins the poll loop's
// progress rule for Follow, symmetric with Mirror's: a full batch whose
// resume token advanced is progress, and the next read follows immediately.
// With an hour-long poll interval, catching up over multiple read batches
// within the test deadline is only possible when no per-batch sleep happens.
func TestFollowBulkCatchUpDoesNotSleepBetweenBatches(t *testing.T) {
	store := NewMemoryStore()
	publisher := New(WithStore(store))
	const n = 250 // > 2 poll batches (default replay batch size 100)
	for i := 0; i < n; i++ {
		Publish(publisher, followBulkEvent{N: i})
	}
	publisher.Wait()

	follower := New(WithStore(store))
	var delivered atomic.Int32
	Subscribe(follower, func(followBulkEvent) { delivered.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- follower.Follow(ctx, FollowFrom(OffsetOldest), FollowPollInterval(time.Hour))
	}()

	mirrorWaitFor(t, func() bool { return delivered.Load() == n })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow did not return after cancellation")
	}
}
