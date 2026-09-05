package eventbus

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// Cancellation must roll back Once when the async slot was never acquired.
// Cancel inside the filter so it deterministically happens after the top-of-loop
// context check and before the capacity select; scheduler timing cannot bypass
// the branch under test.
func TestCanceledAsyncCapacityPreservesOnceHandler(t *testing.T) {
	for name, mode := range map[string]dispatchMode{"live": dispatchNonBlocking, "durable": dispatchWaitForAsync} {
		t.Run(name, func(t *testing.T) {
			bus := New(WithAsyncHandlerLimit(1))
			bus.asyncSem <- struct{}{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := true
			calls := 0
			err := Subscribe(bus, func(TestEvent) { calls++ }, Async(), Once(), WithFilter(func(TestEvent) bool {
				if first {
					first = false
					cancel()
				}
				return true
			}))
			if err != nil {
				t.Fatal(err)
			}
			e := TestEvent{}
			err = dispatchWithMode(bus, ctx, reflect.TypeOf(e), EventType(e), e, mode)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled dispatch: %v", err)
			}
			<-bus.asyncSem
			if calls != 0 || HandlerCount[TestEvent](bus) != 1 {
				t.Fatal("unstarted Once handler was lost")
			}
			if err := dispatchWithMode(bus, context.Background(), reflect.TypeOf(e), EventType(e), e, mode); err != nil {
				t.Fatal(err)
			}
			bus.Wait()
			if calls != 1 {
				t.Fatalf("retry calls = %d", calls)
			}
		})
	}
}
