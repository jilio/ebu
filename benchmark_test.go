package eventbus

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Benchmark events
type BenchmarkEvent struct {
	ID    int
	Value string
}

type LargeEvent struct {
	ID     int
	Data   [1024]byte // 1KB payload
	Values []string
}

// Benchmark basic publish/subscribe operations
func BenchmarkPublishSubscribe(b *testing.B) {
	bus := New()
	received := make(chan BenchmarkEvent, 1)

	Subscribe(bus, func(evt BenchmarkEvent) {
		received <- evt
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Publish(bus, BenchmarkEvent{ID: i, Value: "test"})
		<-received
	}
}

// Benchmark publish with context
func BenchmarkPublishContext(b *testing.B) {
	bus := New()
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(b.N)

	SubscribeContext(bus, func(ctx context.Context, evt BenchmarkEvent) {
		wg.Done()
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PublishContext(bus, ctx, BenchmarkEvent{ID: i, Value: "test"})
	}
	wg.Wait()
}

// Benchmark concurrent publishers
// Keep handler work empty and distribute exactly b.N operations, including
// the remainder when b.N is not divisible by the publisher count.
func BenchmarkConcurrentPublish(b *testing.B) {
	for _, numPublishers := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("publishers-%d", numPublishers), func(b *testing.B) {
			bus := New()
			Subscribe(bus, func(BenchmarkEvent) {})
			b.ReportAllocs()
			b.ResetTimer()
			var wg sync.WaitGroup
			for p := 0; p < numPublishers; p++ {
				count := b.N / numPublishers
				if p < b.N%numPublishers {
					count++
				}
				wg.Add(1)
				go func(count int) {
					defer wg.Done()
					for i := 0; i < count; i++ {
						Publish(bus, BenchmarkEvent{ID: i, Value: "test"})
					}
				}(count)
			}
			wg.Wait()
		})
	}
}

// Benchmark multiple subscribers
func BenchmarkMultipleSubscribers(b *testing.B) {
	for _, numSubscribers := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("subscribers-%d", numSubscribers), func(b *testing.B) {
			bus := New()
			for s := 0; s < numSubscribers; s++ {
				Subscribe(bus, func(BenchmarkEvent) {})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Publish(bus, BenchmarkEvent{ID: i, Value: "test"})
			}
		})
	}
}

// Benchmark handler registration/deregistration
func BenchmarkSubscribeUnsubscribe(b *testing.B) {
	bus := New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler := func(evt BenchmarkEvent) {}
		Subscribe(bus, handler)
		Unsubscribe[BenchmarkEvent](bus, handler)
	}
}

// Benchmark with large event payloads
func BenchmarkLargeEvents(b *testing.B) {
	bus := New()
	received := make(chan LargeEvent, 100)

	Subscribe(bus, func(evt LargeEvent) {
		received <- evt
	})

	event := LargeEvent{
		ID:     1,
		Values: make([]string, 100),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Publish(bus, event)
		<-received
	}
}

// Benchmark memory allocation
func BenchmarkMemoryAllocation(b *testing.B) {
	b.Run("small-events", func(b *testing.B) {
		bus := New()
		Subscribe(bus, func(evt BenchmarkEvent) {})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			Publish(bus, BenchmarkEvent{ID: i, Value: "test"})
		}
	})

	b.Run("large-events", func(b *testing.B) {
		bus := New()
		Subscribe(bus, func(evt LargeEvent) {})

		event := LargeEvent{
			ID:     1,
			Values: make([]string, 100),
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			Publish(bus, event)
		}
	})
}

// Benchmark lock contention
func BenchmarkLockContention(b *testing.B) {
	benchmarks := []struct {
		readers int
		writers int
	}{
		{readers: 1, writers: 1},
		{readers: 10, writers: 1},
		{readers: 1, writers: 10},
		{readers: 10, writers: 10},
		{readers: 100, writers: 10},
	}

	for _, bm := range benchmarks {
		name := fmt.Sprintf("r%d-w%d", bm.readers, bm.writers)
		b.Run(name, func(b *testing.B) {
			bus := New()

			// Start readers (subscribers)
			for r := 0; r < bm.readers; r++ {
				Subscribe(bus, func(evt BenchmarkEvent) {
					// Simulate some work
					time.Sleep(time.Nanosecond)
				})
			}

			b.ResetTimer()

			// Start writers (publishers)
			var wg sync.WaitGroup
			eventsPerWriter := b.N / bm.writers

			for w := 0; w < bm.writers; w++ {
				count := eventsPerWriter
				if w < b.N%bm.writers {
					count++
				}
				wg.Add(1)
				go func(count int) {
					defer wg.Done()
					for i := 0; i < count; i++ {
						Publish(bus, BenchmarkEvent{ID: i, Value: "test"})
					}
				}(count)
			}

			wg.Wait()
		})
	}
}

// Benchmark Clear operation
func BenchmarkClear(b *testing.B) {
	b.Run("with-10-handlers", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bus := New()
			for j := 0; j < 10; j++ {
				Subscribe(bus, func(evt BenchmarkEvent) {})
			}
			Clear[BenchmarkEvent](bus)
		}
	})

	b.Run("with-100-handlers", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bus := New()
			for j := 0; j < 100; j++ {
				Subscribe(bus, func(evt BenchmarkEvent) {})
			}
			Clear[BenchmarkEvent](bus)
		}
	})
}
