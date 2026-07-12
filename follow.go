package eventbus

import (
	"context"
	"encoding/json"
	"errors"
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
//   - Every yielded StoredEvent.Offset is prefix-safe to persist immediately
//     after that event: resuming may redeliver an already yielded event or
//     indivisible unit, but must never skip a later yielded event. A protocol
//     with only chunk tokens must use the chunk-start token for non-last
//     members and the chunk-end token only for the last member.
//   - The iterator ends after yielding a non-nil error, or silently when ctx
//     is cancelled. Follow restarts a tail that ends for any other reason.
type EventStoreTailer interface {
	Tail(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error]
}

// followDecoder decodes a stored event's payload into its Go type and
// dispatches it to local handlers. Implementations are closures created by
// registerFollowDecoder, the only place the concrete type is known.
type followDecoder func(ctx context.Context, data json.RawMessage, mode dispatchMode) error

type followDecodeError struct {
	err error
}

func (e *followDecodeError) Error() string {
	return e.err.Error()
}

// registerFollowDecoder records how to decode and locally dispatch events of
// type T, keyed by T's persisted type name. Every generic subscribe entry
// point calls it, so by the time a Follow loop runs, each subscribed type
// can be delivered from the store. It is idempotent for the same Go type. An
// active persistent bus returns an error when two distinct types claim the
// same name; a nonpersistent bus records the ambiguity for a later activation
// audit without constraining reflect.Type delivery. Safe for concurrent use.
func registerFollowDecoder[T any](bus *EventBus) error {
	eventType := reflect.TypeOf((*T)(nil)).Elem()
	typeName := typeNameOf(eventType)
	reservation, err := bus.reservePersistedTypes(persistedTypeSpec{name: typeName, eventType: eventType})
	if err != nil {
		return err
	}
	defer reservation.Rollback()

	if err := installFollowDecoder[T](bus); err != nil {
		return err
	}
	reservation.Commit()
	return nil
}

// installFollowDecoder installs only the runtime decoder. The caller owns any
// durable type reservation; separating the operations lets
// SubscribeWithReplay roll back failed setup without leaving a decoder behind.
//
// Before persistence is enabled, conflicting names mark the decoder slot
// ambiguous instead of constraining ordinary reflect.Type delivery. Once the
// durable type registry is active, a conflict is always returned explicitly.
func installFollowDecoder[T any](bus *EventBus) error {
	eventType := reflect.TypeOf((*T)(nil)).Elem()
	typeName := typeNameOf(eventType)
	active := bus.persistedTypes.isActive()
	bus.followMu.Lock()
	defer bus.followMu.Unlock()
	if existing, ok := bus.followTypes[typeName]; ok {
		if existing == eventType {
			return nil
		}
		if active {
			return persistedTypeNameConflict(typeName, existing, eventType)
		}

		// An in-process bus never uses this map. Retain an explicit ambiguous
		// marker and remove the first decoder so enabling persistence cannot
		// silently select whichever conflicting subscription ran first.
		bus.followTypes[typeName] = nil
		delete(bus.followDecoders, typeName)
		return nil
	}
	bus.followTypes[typeName] = eventType
	bus.followDecoders[typeName] = func(ctx context.Context, data json.RawMessage, mode dispatchMode) error {
		var event T
		if err := json.Unmarshal(data, &event); err != nil {
			// A resumable subscription is represented by an infrastructure marker
			// in the same concrete-type shard. Wake those markers even though the
			// ordinary typed decoder cannot produce T: each coordinator must apply
			// its own ReplaySkip/ReplayAbort policy to this stored envelope. Valid
			// events continue through dispatchWithMode exactly once below.
			if markerErr := dispatchInternalHandlers(bus, ctx, eventType, typeName, event, mode); markerErr != nil {
				return markerErr
			}
			return &followDecodeError{err: err}
		}
		return dispatchWithMode(bus, ctx, eventType, typeName, event, mode)
	}
	return nil
}

// dispatchInternalHandlers snapshots and invokes only infrastructure markers.
// It is used on a Follow decode failure, where no concrete user value exists
// but log-backed replay coordinators still need a wake-up. The shard lock is
// released before coordinator code runs, preserving Clear's lock ordering.
func dispatchInternalHandlers(
	bus *EventBus,
	ctx context.Context,
	eventType reflect.Type,
	eventTypeName string,
	event any,
	mode dispatchMode,
) error {
	shard := bus.getShard(eventType)
	shard.mu.RLock()
	var handlers []*internalHandler
	for _, handler := range shard.handlers[eventType] {
		if handler.internalDelivery != nil {
			handlers = append(handlers, handler)
		}
	}
	shard.mu.RUnlock()

	var errs []error
	for _, handler := range handlers {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		deliveryCtx := ctx
		if mode == dispatchWaitForAsync {
			deliveryCtx = context.WithValue(ctx, durableDispatchCtxKey{}, true)
		}
		if err := callHandlerWithContext(handler, deliveryCtx, event, bus.panicHandler,
			bus.observability, eventTypeName, false); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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
// at publish time; handler execution happens on the Follow goroutine. Follow
// defaults to OffsetOldest in this mode so events appended before it starts
// are not skipped; use a durable subscription ID to resume across restarts.
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
// is OffsetNewest (live-only), except with WithLogDelivery, where it is
// OffsetOldest so events published before Follow starts cannot be lost. With
// FollowWithSubscriptionID, a saved offset takes precedence and FollowFrom
// applies only to the first run (no saved offset yet); without a subscription
// ID it applies to every call. A custom SubscriptionStore needs the optional
// SubscriptionStoreLookup capability to combine a durable ID with an explicit
// starting offset other than OffsetOldest without confusing a legitimate
// OffsetOldest checkpoint with an absent one.
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
// no saved offset — it starts from FollowFrom if given, else OffsetOldest, and
// saves that concrete initial boundary before consuming. A failure to establish
// that first checkpoint aborts startup; later progress-save failures are
// reported and retain the usual at-least-once retry behavior.
//
// Progress is saved after each processed event; delivery is at-least-once
// across restarts. Use FollowDedupWindow and StoredEvent.ID to absorb the
// duplicates.
//
// One EventBus permits only one active durable owner for an ID across Follow
// and SubscribeWithReplay. Follow releases that ownership when it returns, so
// it can be cancelled and restarted on the same bus. Coordination across bus
// instances/processes still requires an external lease or a single owner.
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

// forget removes an ID whose delivery failed so retrying the same stored
// event is not mistaken for a completed duplicate.
func (r *dedupRing) forget(id string) {
	delete(r.ids, id)
	for i, observed := range r.ring {
		if observed == id {
			r.ring[i] = ""
		}
	}
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
//   - A follower with FollowWithSubscriptionID waits for Async handlers for
//     the current event before saving its checkpoint. Publish itself remains
//     non-blocking for Async handlers.
//
// Failure handling: startup checkpoint errors fail the call. Runtime read
// errors and undecodable (poison) events are reported to the
// PersistenceErrorHandler. Generic poison events are skipped; local resumable
// coordinators first apply their own ReplaySkip/ReplayAbort policy, and an
// abort is a durable delivery failure that keeps the outer follower checkpoint
// unchanged. For durable followers, handler panics and upcast failures are
// likewise reported and retried after FollowPollInterval. A non-durable
// follower reports and skips a failed upcast without dispatching the original
// schema. Follow itself returns on ctx cancellation or a startup/configuration
// failure.
func (bus *EventBus) Follow(ctx context.Context, opts ...FollowOption) error {
	if bus.store == nil {
		return fmt.Errorf("eventbus: Follow requires persistence (use WithStore option)")
	}

	defaultFrom := OffsetNewest
	if bus.logDelivery {
		// Publish has no local delivery path in this mode, so starting at a
		// tail resolved inside Follow would create a startup race: an event
		// appended before the follower goroutine first runs would be skipped.
		// Replaying from the beginning is the only complete default when no
		// durable checkpoint or explicit starting position exists.
		defaultFrom = OffsetOldest
	}
	cfg := &followConfig{
		from:         defaultFrom,
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
		// A scalar checkpoint has exactly one in-process owner. Unlike a
		// successful SubscribeWithReplay reservation (which lasts for the bus's
		// lifetime), a Follow reservation is active only for this blocking call so
		// callers can cancel and restart the follower on the same bus.
		if err := bus.reserveReplayID(cfg.subscription); err != nil {
			return fmt.Errorf("eventbus: follow subscription %q: %w", cfg.subscription, err)
		}
		defer bus.releaseReplayID(cfg.subscription)
	}

	from := cfg.from
	if subStore != nil {
		var saved Offset
		var found bool
		var err error
		if lookup, ok := subStore.(SubscriptionStoreLookup); ok {
			saved, found, err = lookup.LookupOffset(ctx, cfg.subscription)
		} else {
			saved, err = subStore.LoadOffset(ctx, cfg.subscription)
			found = saved != OffsetOldest
			if err == nil && !found && cfg.fromSet && cfg.from != OffsetOldest {
				return fmt.Errorf("eventbus: FollowWithSubscriptionID %q cannot safely combine legacy SubscriptionStore.LoadOffset with FollowFrom(%q): implement SubscriptionStoreLookup to distinguish a missing checkpoint from a saved OffsetOldest", cfg.subscription, cfg.from)
			}
		}
		if err != nil {
			return fmt.Errorf("eventbus: load follow offset for %q: %w", cfg.subscription, err)
		}
		if found {
			if saved == OffsetNewest {
				return fmt.Errorf("eventbus: load follow offset for %q: symbolic offset %q is not a durable checkpoint", cfg.subscription, OffsetNewest)
			}
			from = saved
		} else if !cfg.fromSet {
			// First run of a durable follower: consume the full log so the
			// subscription's view is complete, like SubscribeWithReplay.
			from = OffsetOldest
		}

		// Every first-run boundary must be committed before Tail/Read begins so
		// FollowFrom remains a one-time choice even if no event is yielded. A
		// symbolic live-only boundary is first resolved to one concrete tail;
		// otherwise a restart could resolve "$" later and skip downtime events.
		if !found {
			if from == OffsetNewest {
				events, concrete, err := bus.store.Read(ctx, OffsetNewest, 0)
				if err != nil {
					return fmt.Errorf("eventbus: resolve initial follow tail for %q: %w", cfg.subscription, err)
				}
				if len(events) != 0 {
					return fmt.Errorf("eventbus: resolve initial follow tail for %q: EventStore.Read(OffsetNewest) returned %d event(s), want none", cfg.subscription, len(events))
				}
				if concrete == OffsetNewest {
					return fmt.Errorf("eventbus: resolve initial follow tail for %q: EventStore.Read(OffsetNewest) returned symbolic offset %q, want a concrete checkpoint", cfg.subscription, OffsetNewest)
				}
				from = concrete
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			// Establish the first-run boundary even if no event is ever yielded.
			// This makes FollowFrom a one-time choice and prevents a later call
			// with different options from silently moving the starting point.
			if err := subStore.SaveOffset(ctx, cfg.subscription, from); err != nil {
				return fmt.Errorf("eventbus: save initial follow offset for %q: %w", cfg.subscription, err)
			}
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

pollLoop:
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
			if err := f.processEvent(ctx, stored); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := sleepCtx(ctx, f.cfg.pollInterval); err != nil {
					return err
				}
				continue pollLoop
			}
			// Checkpoint every successfully processed envelope. Stores with
			// per-event offsets avoid replaying an already completed prefix when a
			// later sibling in this batch fails; chunk-start tokens may repeat and
			// safely leave the durable cursor unchanged until the chunk boundary.
			f.advance(ctx, stored.Offset)
		}

		if next == f.offset {
			// At the tail (or the store cannot advance): wait for new events.
			if err := sleepCtx(ctx, f.cfg.pollInterval); err != nil {
				return err
			}
			continue
		}
		// A store may advance beyond the last returned envelope (for example, an
		// undecodable or empty chunk). Persist that concrete resume token too.
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
			if err := f.processEvent(ctx, stored); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				break
			}
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
// own-origin echoes, unsubscribed types, and generic poison payloads) are
// skipped so a follower can keep up with a heterogeneous stream. A poison
// payload still wakes resumable markers first; ReplayAbort and other durable
// delivery failures are returned so the caller retries without advancing.
func (f *follower) processEvent(ctx context.Context, stored *StoredEvent) error {
	if f.dedup != nil && stored.ID != "" && f.dedup.observe(stored.ID) {
		return nil
	}

	// Skip this bus's own events: in the default delivery mode they were
	// already dispatched locally at publish time. In log-delivery mode the
	// log is the only delivery path, so own events are always taken.
	if stored.Origin == f.bus.originID && !f.cfg.includeOwn && !f.bus.logDelivery {
		return nil
	}

	data, typeName := stored.Data, stored.Type
	if upcasted, upcastedType, err := f.bus.upcastRegistry.apply(data, typeName); err == nil {
		data, typeName = upcasted, upcastedType
	} else {
		failure := fmt.Errorf("follow: upcast failed at offset %s: %w", stored.Offset, err)
		// The typed decoder never runs on an upcast failure, so target-type
		// replay markers would otherwise receive no signal. Wake every resumable
		// coordinator before the outer follower chooses retry (durable) or skip
		// (non-durable); each coordinator scans the stored envelope and owns its
		// own autonomous retry policy.
		signalReplayMarkers(f.bus, contextForStoredEvent(ctx, stored), stored.Type, stored)
		f.reportEvent(stored, failure)
		if f.subStore != nil {
			f.forget(stored)
			return failure
		}
		// A non-durable follower has no checkpoint to retain for retry. Skip the
		// failed migration after reporting it, but never dispatch the original
		// schema as though the configured upcast had succeeded.
		return nil
	}

	f.bus.followMu.RLock()
	decode := f.bus.followDecoders[typeName]
	f.bus.followMu.RUnlock()
	if decode == nil {
		return nil // No local subscriber for this type.
	}

	// Hand handlers the same context values a live publish would carry.
	dctx := contextForStoredEvent(ctx, stored)

	mode := dispatchNonBlocking
	if f.subStore != nil {
		// A durable checkpoint is a promise that all work for this event
		// finished. Await only this dispatch's async handlers; bus.Wait would
		// also wait for unrelated publishes and cannot provide that boundary.
		mode = dispatchWaitForAsync
	}
	if err := decode(dctx, data, mode); err != nil {
		if _, ok := err.(*followDecodeError); ok {
			// Poison event: report and move on. The payload travels with the
			// report for out-of-band recovery, as in SubscribeWithReplay.
			f.reportEvent(stored, fmt.Errorf("follow: skipping undecodable event at offset %s: %w", stored.Offset, err))
			return nil
		}
		if f.subStore == nil {
			// Non-durable followers retain live-dispatch panic semantics: recovery
			// isolates the handler and the stream continues. Only a durable
			// checkpoint needs delivery failure to stop advancement.
			return nil
		}

		f.forget(stored)
		failure := fmt.Errorf("follow: handler delivery failed at offset %s: %w", stored.Offset, err)
		var panicErr *handlerPanicError
		if ctx.Err() == nil || errors.As(err, &panicErr) {
			f.reportEvent(stored, failure)
		}
		return failure
	}
	return nil
}

func (f *follower) forget(stored *StoredEvent) {
	if f.dedup != nil && stored.ID != "" {
		f.dedup.forget(stored.ID)
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
