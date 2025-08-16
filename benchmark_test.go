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
	received := make(chan BenchmarkEvent, b.N)

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
func BenchmarkConcurrentPublish(b *testing.B) {
	benchmarks := []int{1, 10, 100, 1000}

	for _, numPublishers := range benchmarks {
		b.Run(fmt.Sprintf("publishers-%d", numPublishers), func(b *testing.B) {
			bus := New()
			received := make(chan BenchmarkEvent, b.N)

			Subscribe(bus, func(evt BenchmarkEvent) {
				select {
				case received <- evt:
				default:
				}
			})

			b.ResetTimer()
			var wg sync.WaitGroup
			eventsPerPublisher := b.N / numPublishers
			if eventsPerPublisher == 0 {
				eventsPerPublisher = 1
			}

			for p := 0; p < numPublishers; p++ {
				wg.Add(1)
				go func(publisherID int) {
					defer wg.Done()
					for i := 0; i < eventsPerPublisher; i++ {
						Publish(bus, BenchmarkEvent{ID: i, Value: fmt.Sprintf("pub-%d", publisherID)})
					}
				}(p)
			}

			wg.Wait()
		})
	}
}

// Benchmark multiple subscribers
func BenchmarkMultipleSubscribers(b *testing.B) {
	benchmarks := []int{1, 10, 100, 1000}

	for _, numSubscribers := range benchmarks {
		b.Run(fmt.Sprintf("subscribers-%d", numSubscribers), func(b *testing.B) {
			bus := New()
			var wg sync.WaitGroup
			wg.Add(b.N * numSubscribers)

			for s := 0; s < numSubscribers; s++ {
				Subscribe(bus, func(evt BenchmarkEvent) {
					wg.Done()
				})
			}

			b.ResetTimer()
			done := make(chan bool)
			go func() {
				for i := 0; i < b.N; i++ {
					Publish(bus, BenchmarkEvent{ID: i, Value: "test"})
				}
				close(done)
			}()

			// Wait for either completion or timeout
			go func() {
				wg.Wait()
			}()

			<-done
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

// Benchmark CQRS command execution
func BenchmarkCommandBus(b *testing.B) {
	bus := New()
	cmdBus := NewCommandBus[CreateAccountCommand, AccountEvent](bus)

	cmdBus.Register("CreateAccount", func(ctx context.Context, cmd CreateAccountCommand) ([]AccountEvent, error) {
		return []AccountEvent{
			AccountCreated{AccountID: cmd.AccountID, Balance: cmd.InitialBalance},
		}, nil
	})

	ctx := context.Background()
	cmd := CreateAccountCommand{AccountID: "test-123", InitialBalance: 100}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cmdBus.Execute(ctx, "CreateAccount", cmd)
	}
}

// Benchmark CQRS query execution
func BenchmarkQueryBus(b *testing.B) {
	queryBus := NewQueryBus[GetBalanceQuery, float64]()

	queryBus.Register("GetBalance", func(ctx context.Context, query GetBalanceQuery) (float64, error) {
		return 1000.0, nil
	})

	ctx := context.Background()
	query := GetBalanceQuery{AccountID: "test-123"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = queryBus.Execute(ctx, "GetBalance", query)
	}
}

// Benchmark query with cache
func BenchmarkQueryWithCache(b *testing.B) {
	queryBus := NewQueryBus[GetBalanceQuery, float64](
		WithQueryCache[GetBalanceQuery, float64](5 * time.Minute),
	)

	queryBus.Register("GetBalance", func(ctx context.Context, query GetBalanceQuery) (float64, error) {
		// Simulate some work
		time.Sleep(time.Microsecond)
		return 1000.0, nil
	})

	ctx := context.Background()
	query := GetBalanceQuery{AccountID: "test-123"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = queryBus.Execute(ctx, "GetBalance", query)
	}
}

// Benchmark projection handling
func BenchmarkProjectionManager(b *testing.B) {
	bus := New()
	pm := NewProjectionManager[AccountEvent](bus)

	projection := NewAccountBalanceProjection()
	_ = pm.Register(projection)

	event := AccountCreated{AccountID: "test-123", Balance: 100}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pm.HandleEvent(ctx, event)
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
			if eventsPerWriter == 0 {
				eventsPerWriter = 1
			}

			for w := 0; w < bm.writers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < eventsPerWriter; i++ {
						Publish(bus, BenchmarkEvent{ID: i, Value: "test"})
					}
				}()
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

// Benchmark aggregate operations
func BenchmarkAggregateOperations(b *testing.B) {
	b.Run("RaiseEvent", func(b *testing.B) {
		agg := &AccountAggregate{
			BaseAggregate: BaseAggregate[AccountEvent]{
				ID: "test-123",
			},
			Balance: 0,
		}

		agg.BaseAggregate.ApplyFunc = func(event AccountEvent) error {
			return nil
		}

		event := AccountCreated{AccountID: "test-123", Balance: 100}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = agg.RaiseEvent(event)
		}
	})

	b.Run("LoadFromHistory", func(b *testing.B) {
		events := []AccountEvent{
			AccountCreated{AccountID: "test-123", Balance: 100},
			MoneyDeposited{AccountID: "test-123", Amount: 50},
			MoneyWithdrawn{AccountID: "test-123", Amount: 25},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			agg := &AccountAggregate{
				BaseAggregate: BaseAggregate[AccountEvent]{
					ID: "test-123",
				},
			}
			agg.BaseAggregate.ApplyFunc = func(event AccountEvent) error {
				return nil
			}
			_ = agg.LoadFromHistory(events)
		}
	})
}
