package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"reflect"
	"time"
)

// EventStoreTailer is an optional interface for stores that can push new
// events as they arrive instead of being polled. When the bus's store
// implements it, Follow uses Tail; otherwise it falls back to polling Read
// on the configured interval (see FollowPollInterval).
//
// Contract:
//   - Tail yields events strictly after from, in order, as they become
//     available, blocking between events rather than returning at the tail.
//   - from accepts the same values as Read: OffsetOldest, OffsetNewest
//     (resolved to the tail at call time), or any offset the store issued.
//   - The iterator ends after yielding a non-nil error, or silently when ctx
//     is cancelled. Follow restarts a tail that ends for any other reason.
type EventStoreTailer interface {
	Tail(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error]
}

// followDecoder decodes a stored event's payload into its Go type and
// dispatches it to local handlers. Implementations are closures created by
// registerFollowDecoder, the only place the concrete type is known.
type followDecoder func(ctx context.Context, data json.RawMessage) error

// registerFollowDecoder records how to decode and locally dispatch events of
// type T, keyed by T's persisted type name. Every generic subscribe entry
// point calls it, so by the time a Follow loop runs, each subscribed type
// can be delivered from the store. Idempotent; safe for concurrent use.
func registerFollowDecoder[T any](bus *EventBus) {
	eventType := reflect.TypeOf((*T)(nil)).Elem()
	typeName := typeNameOf(eventType)

	bus.followMu.Lock()
	defer bus.followMu.Unlock()
	if _, ok := bus.followDecoders[typeName]; ok {
		return
	}
	bus.followDecoders[typeName] = func(ctx context.Context, data json.RawMessage) error {
		var event T
		if err := json.Unmarshal(data, &event); err != nil {
			return err
		}
		dispatch(bus, ctx, eventType, typeName, event)
		return nil
	}
}

// WithLogDelivery makes the store the only delivery path: Publish appends to
// the log and returns without dispatching to local handlers; handlers receive
// events exclusively from a Follow loop tailing that log. Because every
// process — including the publisher — then observes the same log, all of them
// see the same events in the same order, and a publish that failed to persist
// is (by construction) never observed anywhere.
//
// Requirements: WithStore is mandatory (New panics without it), and the
// process must run bus.Follow, or published events are stored but never
// handled locally. Publish hooks and publish-level observability still fire
// at publish time; handler execution happens on the Follow goroutine.
func WithLogDelivery() Option {
	return func(bus *EventBus) {
		bus.logDelivery = true
	}
}

// FollowOption configures a Follow loop.
type FollowOption func(*followConfig) error

type followConfig struct {
	from         Offset
	fromSet      bool
	subscription string
	pollInterval time.Duration
	includeOwn   bool
	dedupWindow  int
}

// FollowFrom sets the offset the follower starts reading after. The default
// is OffsetNewest (live-only). With FollowWithSubscriptionID, a saved offset
// takes precedence and FollowFrom applies only to the first run (no saved
// offset yet); without a subscription ID it applies to every call.
func FollowFrom(from Offset) FollowOption {
	return func(cfg *followConfig) error {
		cfg.from = from
		cfg.fromSet = true
		return nil
	}
}

// FollowWithSubscriptionID makes the follower durable: it resumes from the
// offset saved under id and saves its position as it processes events, so a
// restarted process continues where it left off. Requires a SubscriptionStore
// (WithSubscriptionStore, or a store that implements it). On the first run —
// no saved offset — it starts from FollowFrom if given, else OffsetOldest.
//
// Progress is saved after each processed event; delivery is at-least-once
// across restarts. Use FollowDedupWindow and StoredEvent.ID to absorb the
// duplicates.
func FollowWithSubscriptionID(id string) FollowOption {
	return func(cfg *followConfig) error {
		if id == "" {
			return fmt.Errorf("eventbus: follow subscription ID cannot be empty")
		}
		cfg.subscription = id
		return nil
	}
}

// FollowPollInterval sets how long the follower sleeps between Read calls
// when the store does not implement EventStoreTailer, and how long it backs
// off after a read error on either path. Default 200ms.
func FollowPollInterval(d time.Duration) FollowOption {
	return func(cfg *followConfig) error {
		if d <= 0 {
			return fmt.Errorf("eventbus: follow poll interval must be positive")
		}
		cfg.pollInterval = d
		return nil
	}
}

// FollowIncludeOwn delivers events this bus instance itself published
// (matched by Origin). By default the follower skips them, because in the
// default delivery mode they were already dispatched locally at publish time
// and would arrive twice. A bus in WithLogDelivery mode always receives its
// own events from the log regardless of this option — there, the log is the
// only delivery path.
func FollowIncludeOwn() FollowOption {
	return func(cfg *followConfig) error {
		cfg.includeOwn = true
		return nil
	}
}

// FollowDedupWindow sets how many recently seen event IDs the follower
// remembers to drop at-least-once duplicates (retried appends, chunk
// re-reads, replays after a crash). Default 1024; 0 disables deduplication.
// Events without an ID (written by pre-envelope versions or external
// producers) are never deduplicated.
func FollowDedupWindow(n int) FollowOption {
	return func(cfg *followConfig) error {
		if n < 0 {
			return fmt.Errorf("eventbus: follow dedup window cannot be negative")
		}
		cfg.dedupWindow = n
		return nil
	}
}

// dedupRing remembers the last N event IDs seen. Follow runs single-threaded,
// so it needs no locking.
type dedupRing struct {
	ids  map[string]struct{}
	ring []string
	next int
}

func newDedupRing(n int) *dedupRing {
	return &dedupRing{
		ids:  make(map[string]struct{}, n),
		ring: make([]string, n),
	}
}

// observe records id and reports whether it had been seen already.
func (r *dedupRing) observe(id string) bool {
	if _, ok := r.ids[id]; ok {
		return true
	}
	if evicted := r.ring[r.next]; evicted != "" {
		delete(r.ids, evicted)
	}
	r.ring[r.next] = id
	r.next = (r.next + 1) % len(r.ring)
	r.ids[id] = struct{}{}
	return false
}

// Follow tails the bus's event store and delivers each new event to the
// local handlers subscribed to its type. It is the primitive that turns a
// shared store into a cross-process bus: every process appends by
// publishing and receives by following.
//
// Follow blocks until ctx is cancelled (returning ctx.Err()) or startup
// validation fails. Run it on its own goroutine:
//
//	go func() {
//	    if err := bus.Follow(ctx); err != nil && !errors.Is(err, context.Canceled) {
//	        log.Printf("follower stopped: %v", err)
//	    }
//	}()
//
// Delivery semantics:
//   - Events are dispatched with the same semantics as a local publish —
//     filters, Once, Async, Sequential, and panic recovery all apply — but
//     publish hooks and publish-level observability do not fire (they fired
//     in the publishing process). OffsetFromContext, EventIDFromContext, and
//     MetadataFromContext work inside handlers.
//   - Only event types with at least one prior Subscribe* call in this
//     process can be decoded; events of other types are skipped. Subscribe
//     BEFORE calling Follow — types subscribed later are only picked up from
//     that point in the stream onward.
//   - Events this bus itself published are skipped unless FollowIncludeOwn
//     is given or the bus is in WithLogDelivery mode (see those options).
//   - Registered upcasts are applied before decoding, exactly as in Replay.
//   - Delivery is at-least-once end to end. The follower deduplicates by
//     StoredEvent.ID within FollowDedupWindow; handlers that need stronger
//     guarantees must be idempotent.
//
// Failure handling: read errors and undecodable (poison) events are reported
// to the PersistenceErrorHandler; reads are retried after FollowPollInterval,
// poison events are skipped so they cannot wedge the follower. Follow itself
// only returns on ctx cancellation or invalid configuration.
func (bus *EventBus) Follow(ctx context.Context, opts ...FollowOption) error {
	if bus.store == nil {
		return fmt.Errorf("eventbus: Follow requires persistence (use WithStore option)")
	}

	cfg := &followConfig{
		from:         OffsetNewest,
		pollInterval: 200 * time.Millisecond,
		dedupWindow:  1024,
	}
	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("eventbus: follow option cannot be nil")
		}
		if err := opt(cfg); err != nil {
			return err
		}
	}

	// Resolve the durable subscription store, mirroring SubscribeWithReplay.
	var subStore SubscriptionStore
	if cfg.subscription != "" {
		subStore = bus.subscriptionStore
		if subStore == nil {
			if ss, ok := bus.store.(SubscriptionStore); ok {
				subStore = ss
			} else {
				return fmt.Errorf("eventbus: FollowWithSubscriptionID requires a SubscriptionStore (use WithSubscriptionStore option or use a store that implements SubscriptionStore)")
			}
		}
	}

	from := cfg.from
	if subStore != nil {
		saved, err := subStore.LoadOffset(ctx, cfg.subscription)
		if err != nil {
			return fmt.Errorf("eventbus: load follow offset for %q: %w", cfg.subscription, err)
		}
		if saved != OffsetOldest {
			from = saved
		} else if !cfg.fromSet {
			// First run of a durable follower: consume the full log so the
			// subscription's view is complete, like SubscribeWithReplay.
			from = OffsetOldest
		}
	}

	var dedup *dedupRing
	if cfg.dedupWindow > 0 {
		dedup = newDedupRing(cfg.dedupWindow)
	}

	f := &follower{bus: bus, cfg: cfg, subStore: subStore, dedup: dedup, offset: from}
	if tailer, ok := bus.store.(EventStoreTailer); ok {
		return f.runTail(ctx, tailer)
	}
	return f.runPoll(ctx)
}

// follower is the running state of one Follow call.
type follower struct {
	bus      *EventBus
	cfg      *followConfig
	subStore SubscriptionStore
	dedup    *dedupRing
	offset   Offset
}

// runPoll reads batches in a sleep loop; the fallback for stores without
// EventStoreTailer support.
func (f *follower) runPoll(ctx context.Context) error {
	batchSize := f.bus.replayBatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		events, next, err := f.bus.store.Read(ctx, f.offset, batchSize)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			f.reportError(fmt.Errorf("follow: read after offset %s: %w", f.offset, err))
			if err := sleepCtx(ctx, f.cfg.pollInterval); err != nil {
				return err
			}
			continue
		}

		for _, stored := range events {
			f.processEvent(ctx, stored)
		}

		if next == f.offset {
			// At the tail (or the store cannot advance): wait for new events.
			if err := sleepCtx(ctx, f.cfg.pollInterval); err != nil {
				return err
			}
			continue
		}
		f.advance(ctx, next)
	}
}

// runTail consumes a pushing store, restarting the tail after errors.
func (f *follower) runTail(ctx context.Context, tailer EventStoreTailer) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var tailErr error
		for stored, err := range tailer.Tail(ctx, f.offset) {
			if err != nil {
				tailErr = err
				break
			}
			f.processEvent(ctx, stored)
			f.advance(ctx, stored.Offset)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if tailErr != nil {
			f.reportError(fmt.Errorf("follow: tail after offset %s: %w", f.offset, tailErr))
		}
		// Either an error or a tail that ended unexpectedly: back off and
		// re-tail from the last processed position.
		if err := sleepCtx(ctx, f.cfg.pollInterval); err != nil {
			return err
		}
	}
}

// processEvent runs one stored event through dedup, origin filtering,
// upcasting, decoding, and local dispatch. Undeliverable events (duplicates,
// own-origin echoes, unsubscribed types, poison payloads) are skipped — a
// follower must keep up with the stream no matter what is on it.
func (f *follower) processEvent(ctx context.Context, stored *StoredEvent) {
	if f.dedup != nil && stored.ID != "" && f.dedup.observe(stored.ID) {
		return
	}

	// Skip this bus's own events: in the default delivery mode they were
	// already dispatched locally at publish time. In log-delivery mode the
	// log is the only delivery path, so own events are always taken.
	if stored.Origin == f.bus.originID && !f.cfg.includeOwn && !f.bus.logDelivery {
		return
	}

	data, typeName := stored.Data, stored.Type
	if upcasted, upcastedType, err := f.bus.upcastRegistry.apply(data, typeName); err == nil {
		data, typeName = upcasted, upcastedType
	}
	// On upcast failure the original payload proceeds; the type check or
	// decode below decides its fate, mirroring replay behavior.

	f.bus.followMu.RLock()
	decode := f.bus.followDecoders[typeName]
	f.bus.followMu.RUnlock()
	if decode == nil {
		return // No local subscriber for this type.
	}

	// Hand handlers the same context values a live publish would carry.
	dctx := ctx
	if stored.Offset != OffsetOldest {
		dctx = context.WithValue(dctx, offsetCtxKey{}, stored.Offset)
	}
	if stored.ID != "" {
		dctx = context.WithValue(dctx, eventIDCtxKey{}, stored.ID)
	}
	dctx = ContextWithMetadata(dctx, stored.Metadata)

	if err := decode(dctx, data); err != nil {
		// Poison event: report and move on. The payload travels with the
		// report for out-of-band recovery, as in SubscribeWithReplay.
		f.reportEvent(stored, fmt.Errorf("follow: skipping undecodable event at offset %s: %w", stored.Offset, err))
	}
}

// advance records the follower's position, persisting it for durable
// followers. SaveOffset failures are reported and do not stop the follower:
// the position is redundant with the events themselves (at-least-once).
func (f *follower) advance(ctx context.Context, offset Offset) {
	f.offset = offset
	if f.subStore == nil {
		return
	}
	if err := f.subStore.SaveOffset(ctx, f.cfg.subscription, offset); err != nil {
		f.reportError(fmt.Errorf("follow: save offset for %q: %w", f.cfg.subscription, err))
	}
}

// reportError forwards a follower failure to the PersistenceErrorHandler,
// the bus's monitoring channel for storage problems. The event argument is
// nil for failures not tied to a specific event.
func (f *follower) reportError(err error) {
	if f.bus.persistenceErrorHandler != nil {
		f.bus.persistenceErrorHandler(nil, nil, err)
	}
}

// reportEvent forwards a per-event failure, carrying the *StoredEvent so the
// payload can be recovered out of band.
func (f *follower) reportEvent(stored *StoredEvent, err error) {
	if f.bus.persistenceErrorHandler != nil {
		f.bus.persistenceErrorHandler(stored, nil, err)
	}
}

// sleepCtx sleeps for d or until ctx is done, returning ctx.Err() in the
// latter case.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
