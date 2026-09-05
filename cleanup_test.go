package eventbus

import (
	"context"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

// A removed closure must not remain reachable through the bus's slice backing
// array, including when the last subscriber is removed. Inspecting the old
// array makes the retention regression deterministic without GC/finalizers.
func TestUnsubscribeReleasesHandlerReferences(t *testing.T) {
	for _, mode := range []string{"handle", "function", "once"} {
		for _, position := range []string{"only", "first", "middle", "last"} {
			t.Run(mode+"/"+position, func(t *testing.T) {
				bus := New()
				var calls []int
				addSurvivor := func(id int) {
					if err := Subscribe(bus, func(int) { calls = append(calls, id) }); err != nil {
						t.Fatal(err)
					}
				}
				if position == "middle" || position == "last" {
					addSurvivor(1)
				}
				target := func(int) { calls = append(calls, 99) }
				var opts []SubscribeOption
				if mode == "once" {
					opts = append(opts, Once())
				}
				sub, err := SubscribeWithHandle(bus, target, opts...)
				if err != nil {
					t.Fatal(err)
				}
				if position == "first" || position == "middle" {
					addSurvivor(2)
				}
				eventType := reflect.TypeOf(0)
				shard := bus.getShard(eventType)
				backing := shard.handlers[eventType]
				switch mode {
				case "handle":
					sub.Unsubscribe()
				case "function":
					if err := Unsubscribe[int](bus, target); err != nil {
						t.Fatal(err)
					}
				case "once":
					Publish(bus, 0)
				}
				if backing[len(backing)-1] != nil {
					t.Fatal("removed handler is still retained in the slice tail")
				}
				if position == "only" {
					if _, ok := shard.handlers[eventType]; ok {
						t.Fatal("empty subscriber entry remains in the bus")
					}
				}
				calls = nil
				Publish(bus, 0)
				var want []int
				if position == "middle" || position == "last" {
					want = append(want, 1)
				}
				if position == "first" || position == "middle" {
					want = append(want, 2)
				}
				if !slices.Equal(calls, want) {
					t.Fatalf("remaining handlers = %v, want %v", calls, want)
				}
			})
		}
	}
}

type concurrentAppendStore struct {
	*MemoryStore
	entered chan struct{}
	release chan struct{}
}

func (s *concurrentAppendStore) Append(ctx context.Context, event *Event) (Offset, error) {
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return s.MemoryStore.Append(ctx, event)
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Both publishers must enter Append before either is allowed to finish. A
// timeout is only a deadlock guard, not a machine-dependent performance target.
func TestPublishDoesNotSerializeStoreAppends(t *testing.T) {
	store := &concurrentAppendStore{
		MemoryStore: NewMemoryStore(),
		entered:     make(chan struct{}, 2),
		release:     make(chan struct{}),
	}
	bus := New(WithStore(store))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			results <- TryPublishContext(bus, ctx, id)
		}(i)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-store.entered:
		case <-ctx.Done():
			t.Fatal("concurrent publishers could not enter Append together")
		}
	}
	close(store.release)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
