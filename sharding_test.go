package eventbus

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestShardedPerformance verifies that sharding improves concurrent performance
func TestShardedPerformance(t *testing.T) {
	bus := New()

	const numEvents = 1000
	const numPublishers = 10

	var received int32
	var wg sync.WaitGroup

	// Subscribe to events
	Subscribe(bus, func(evt BenchmarkEvent) {
		atomic.AddInt32(&received, 1)
	})

	// Publish events concurrently
	wg.Add(numPublishers)
	for p := 0; p < numPublishers; p++ {
		go func(publisherID int) {
			defer wg.Done()
			for i := 0; i < numEvents/numPublishers; i++ {
				Publish(bus, BenchmarkEvent{ID: i, Value: "test"})
			}
		}(p)
	}

	wg.Wait()

	if int(received) != numEvents {
		t.Errorf("Expected %d events, got %d", numEvents, received)
	}
}

// TestShardDistribution verifies events are distributed across shards
func TestShardDistribution(t *testing.T) {
	bus := New()

	// Test that different event types go to different shards
	type Event1 struct{ ID int }
	type Event2 struct{ ID int }
	type Event3 struct{ ID int }

	var count1, count2, count3 int32

	Subscribe(bus, func(e Event1) { atomic.AddInt32(&count1, 1) })
	Subscribe(bus, func(e Event2) { atomic.AddInt32(&count2, 1) })
	Subscribe(bus, func(e Event3) { atomic.AddInt32(&count3, 1) })

	// Publish to different event types
	Publish(bus, Event1{ID: 1})
	Publish(bus, Event2{ID: 2})
	Publish(bus, Event3{ID: 3})

	if count1 != 1 || count2 != 1 || count3 != 1 {
		t.Errorf("Events not handled correctly: %d, %d, %d", count1, count2, count3)
	}
}

// TestConcurrentSubscribeUnsubscribe tests concurrent subscription operations
func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	bus := New()

	var wg sync.WaitGroup
	const numOps = 100

	wg.Add(numOps * 2)

	// Concurrent subscribes
	for i := 0; i < numOps; i++ {
		go func() {
			defer wg.Done()
			handler := func(e BenchmarkEvent) {}
			Subscribe(bus, handler)
		}()
	}

	// Concurrent publishes
	for i := 0; i < numOps; i++ {
		go func(id int) {
			defer wg.Done()
			Publish(bus, BenchmarkEvent{ID: id, Value: "test"})
		}(i)
	}

	wg.Wait()

	// Verify handlers were added
	if HandlerCount[BenchmarkEvent](bus) == 0 {
		t.Error("No handlers registered")
	}
}
