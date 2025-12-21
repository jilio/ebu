package eventbus

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// SlowStore simulates a realistic database with I/O latency
type SlowStore struct {
	events      []*StoredEvent
	nextOffset  int64
	mu          sync.Mutex
	saveLatency time.Duration
	saveCount   atomic.Int64
}

func NewSlowStore(latency time.Duration) *SlowStore {
	return &SlowStore{
		events:      make([]*StoredEvent, 0),
		saveLatency: latency,
	}
}

func (s *SlowStore) Append(ctx context.Context, event *Event) (Offset, error) {
	// Simulate realistic database write latency
	time.Sleep(s.saveLatency)

	s.mu.Lock()
	s.nextOffset++
	offset := Offset(strconv.FormatInt(s.nextOffset, 10))
	stored := &StoredEvent{
		Offset:    offset,
		Type:      event.Type,
		Data:      event.Data,
		Timestamp: event.Timestamp,
	}
	s.events = append(s.events, stored)
	s.mu.Unlock()

	s.saveCount.Add(1)
	return offset, nil
}

func (s *SlowStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]*StoredEvent, 0)
	var lastOffset Offset = from
	for _, e := range s.events {
		if from == OffsetOldest || e.Offset > from {
			result = append(result, e)
			lastOffset = e.Offset
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, lastOffset, nil
}

// Benchmark without persistence - baseline
func BenchmarkPublishWithoutPersistence(b *testing.B) {
	bus := New()

	// Handler that does minimal work
	Subscribe(bus, func(e TestEvent) {
		_ = e.ID
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			Publish(bus, TestEvent{ID: i})
			i++
		}
	})
}

// Benchmark with in-memory persistence (no I/O latency)
func BenchmarkPublishWithFastPersistence(b *testing.B) {
	store := NewMemoryStore()
	bus := New(WithStore(store))

	Subscribe(bus, func(e TestEvent) {
		_ = e.ID
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			Publish(bus, TestEvent{ID: i})
			i++
		}
	})
}

// Benchmark with realistic database latency (1ms)
func BenchmarkPublishWithSlowPersistence_1ms(b *testing.B) {
	store := NewSlowStore(1 * time.Millisecond)
	bus := New(WithStore(store))

	Subscribe(bus, func(e TestEvent) {
		_ = e.ID
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			Publish(bus, TestEvent{ID: i})
			i++
		}
	})

	b.StopTimer()
	b.Logf("Total events persisted: %d", store.saveCount.Load())
	b.Logf("Events/sec: %.0f", float64(b.N)/b.Elapsed().Seconds())
}

// Benchmark with high database latency (10ms) - more realistic for remote DB
func BenchmarkPublishWithSlowPersistence_10ms(b *testing.B) {
	store := NewSlowStore(10 * time.Millisecond)
	bus := New(WithStore(store))

	Subscribe(bus, func(e TestEvent) {
		_ = e.ID
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			Publish(bus, TestEvent{ID: i})
			i++
		}
	})

	b.StopTimer()
	b.Logf("Total events persisted: %d", store.saveCount.Load())
	b.Logf("Events/sec: %.0f", float64(b.N)/b.Elapsed().Seconds())
}

// Test to demonstrate the lock contention
func TestPersistenceLockContention(t *testing.T) {
	store := NewSlowStore(10 * time.Millisecond) // 10ms per save
	bus := New(WithStore(store))

	Subscribe(bus, func(e TestEvent) {})

	// Try to publish 10 events concurrently
	const numEvents = 10
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(numEvents)

	for i := 0; i < numEvents; i++ {
		go func(id int) {
			defer wg.Done()
			Publish(bus, TestEvent{ID: id})
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// If fully parallelized: ~10ms (all saves happen in parallel)
	// With lock contention: ~100ms (all saves serialized)

	t.Logf("Publishing %d events took: %v", numEvents, elapsed)
	t.Logf("Average time per event: %v", elapsed/numEvents)

	if elapsed < 50*time.Millisecond {
		t.Logf("✅ Events were processed in parallel (< 50ms)")
	} else {
		t.Logf("❌ Events were serialized due to lock contention (> 50ms)")
		t.Logf("Expected: ~10ms (parallel), Got: %v (serialized)", elapsed)
	}

	// The test will show serialization due to the global lock
	if elapsed > 80*time.Millisecond {
		t.Logf("⚠️  CONFIRMED: Lock contention detected!")
		t.Logf("    10 events * 10ms = 100ms (sequential)")
		t.Logf("    Actual time: %v", elapsed)
		t.Logf("    This proves the global lock serializes all persistence")
	}
}
