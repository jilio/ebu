package eventbus

import (
	"context"
	"errors"
	"fmt"
)

// ErrClosed is returned when an operation starts after Shutdown begins.
var ErrClosed = errors.New("eventbus: bus is shutting down or closed")

// The high bit closes admission; lower bits count accepted operations. Keeping
// both in one atomic word makes admission race-free without locking the publish
// path. asyncMu still protects the condition variable and async handler count.
const lifecycleStopping uint64 = 1 << 63

func (bus *EventBus) beginOperation() bool {
	for {
		state := bus.lifecycleState.Load()
		if state&lifecycleStopping != 0 {
			return false
		}
		if bus.lifecycleState.CompareAndSwap(state, state+1) {
			return true
		}
	}
}

func (bus *EventBus) endOperation() {
	if bus.lifecycleState.Add(^uint64(0)) == lifecycleStopping {
		// The final accepted operation exited during shutdown. Take the waiter's
		// mutex before signaling so a transition to zero cannot lose a wakeup.
		bus.asyncMu.Lock()
		bus.asyncCond.Broadcast()
		bus.asyncMu.Unlock()
	}
}

// Shutdown stops admission of publishes, subscriptions, Replay and Follow.
// New operations return ErrClosed; Publish and PublishContext discard it.
// This includes nested publishes from handlers once shutdown has begun.
// Active Follow calls are canceled. Accepted operations, async handlers and
// deferred resumable-subscription drains finish before the store is closed.
// In log-delivery mode this does not guarantee that Follow consumed the log.
//
// Shutdown is terminal and safe to call concurrently or repeatedly. The store's
// Close method, if present, runs once; all successful waits return its result.
// ctx bounds only the caller's wait: after timeout, draining and eventual close
// continue in the background. A later Shutdown can wait for completion.
// Permanent replay failures or operations ignoring cancellation can prevent
// completion. A blocking store Close also remains subject to the caller's wait.
//
// Do not call Shutdown synchronously from a handler, hook or store operation:
// shutdown waits for that operation to return. Stop external users of GetStore
// and shared stores separately; their operations are not tracked by the bus.
func (bus *EventBus) Shutdown(ctx context.Context) error {
	bus.asyncMu.Lock()
	if bus.lifecycleState.Load()&lifecycleStopping == 0 {
		bus.lifecycleState.Or(lifecycleStopping)
		bus.stopFollow()
		go bus.finishShutdown()
	}
	bus.asyncMu.Unlock()

	select {
	case <-bus.shutdownDone:
		return bus.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (bus *EventBus) finishShutdown() {
	bus.asyncMu.Lock()
	for bus.lifecycleState.Load() != lifecycleStopping || bus.asyncCount > 0 {
		bus.asyncCond.Wait()
	}
	bus.asyncMu.Unlock()
	// No accepted operation can start another handler or deferred drain now.
	if closer, ok := bus.store.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			bus.shutdownErr = fmt.Errorf("failed to close store: %w", err)
		}
	}
	close(bus.shutdownDone)
}
