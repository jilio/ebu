package eventbus

import (
	"context"
	"fmt"
	"time"
)

// MirrorOption configures a Mirror loop.
type MirrorOption func(*mirrorConfig) error

type mirrorConfig struct {
	pollInterval  time.Duration
	dedupWindow   int
	resetOnRewind bool
	onForward     func(*StoredEvent)
	onError       func(error)
}

// MirrorPollInterval sets how long the mirror sleeps between Read calls when
// the source store does not implement EventStoreTailer, and how long it backs
// off after any error on either path. Default 200ms.
func MirrorPollInterval(d time.Duration) MirrorOption {
	return func(cfg *mirrorConfig) error {
		if d <= 0 {
			return fmt.Errorf("eventbus: mirror poll interval must be positive")
		}
		cfg.pollInterval = d
		return nil
	}
}

// MirrorDedupWindow sets how many recently forwarded event IDs the mirror
// remembers so that a retried or re-read source batch does not append the same
// event to the destination twice. Default 1024; 0 disables deduplication.
// Events without an ID (written by pre-envelope versions or external
// producers) are never deduplicated. The window only absorbs duplicates seen
// by one Mirror call: a restart resumes from the durable checkpoint, and the
// events between that checkpoint and the crash may be appended again —
// consumers of the destination deduplicate on StoredEvent.ID exactly as they
// would on the source.
func MirrorDedupWindow(n int) MirrorOption {
	return func(cfg *mirrorConfig) error {
		if n < 0 {
			return fmt.Errorf("eventbus: mirror dedup window cannot be negative")
		}
		cfg.dedupWindow = n
		return nil
	}
}

// MirrorOnForward registers an observer invoked after each event is
// successfully appended to the destination, before the checkpoint for it is
// saved. Use it for metrics such as forwarded-event counts or replication lag
// (compare StoredEvent.Timestamp with the clock). The callback runs on the
// mirror goroutine: keep it fast, and treat the event as read-only.
func MirrorOnForward(fn func(*StoredEvent)) MirrorOption {
	return func(cfg *mirrorConfig) error {
		if fn == nil {
			return fmt.Errorf("eventbus: mirror OnForward callback cannot be nil")
		}
		cfg.onForward = fn
		return nil
	}
}

// MirrorOnError registers an observer for runtime failures the mirror absorbs
// and retries: source read errors, destination append errors, checkpoint save
// errors, and reconcile failures. Without it those failures are silent (the
// mirror still retries). The callback runs on the mirror goroutine.
func MirrorOnError(fn func(error)) MirrorOption {
	return func(cfg *mirrorConfig) error {
		if fn == nil {
			return fmt.Errorf("eventbus: mirror OnError callback cannot be nil")
		}
		cfg.onError = fn
		return nil
	}
}

// MirrorResetOnSourceRewind makes the mirror recover when its saved checkpoint
// no longer resolves in the source because the source log was rebuilt or
// restored from an older copy: a checkpoint past the source's current tail
// would otherwise read as "at the tail" forever and the mirror would silently
// stop forwarding. With this option the mirror compares its checkpoint against
// the source tail — at startup, and again whenever the source looks idle — and
// when the checkpoint is ahead it resets to OffsetOldest and re-reads the
// source from the beginning, reporting the reset to MirrorOnError.
//
// Requires the source store to implement EventStoreOffsetComparer; Mirror
// fails at startup otherwise. Off by default because the reset re-appends the
// entire surviving source log to the destination — duplicates that downstream
// consumers must absorb by StoredEvent.ID.
func MirrorResetOnSourceRewind() MirrorOption {
	return func(cfg *mirrorConfig) error {
		cfg.resetOnRewind = true
		return nil
	}
}

// Mirror copies every event from src to dst, preserving the stored envelope
// verbatim: ID, Origin, Type, Data, Metadata, and Timestamp are appended to
// dst exactly as read from src (dst assigns its own offsets). It is the
// primitive for replicating one log into another — a local write-ahead log
// shipped into a shared store, a store migration run alongside live traffic,
// or a backup log rebuilt on another machine.
//
// Mirror tails src forever: it blocks until ctx is cancelled (returning
// ctx.Err()) or startup validation fails, using Tail when src implements
// EventStoreTailer and polling Read otherwise. Run it on its own goroutine.
//
// Progress is durable. The mirror resumes from the offset saved under
// subscriptionID in offsets, and checkpoints after each forwarded event; on
// the first run (no saved checkpoint) it starts from OffsetOldest, because a
// mirror's job is the whole log. The checkpoint is a source offset; offsets
// may be backed by src, dst, or a third store.
//
// Semantics:
//   - Delivery into dst is at-least-once: a crash between Append and the
//     checkpoint save re-forwards that event on restart. Event IDs are
//     preserved, so consumers of dst deduplicate exactly as they would on src
//     (see FollowDedupWindow). Within one call, MirrorDedupWindow absorbs
//     re-reads so transient retries do not duplicate events in dst.
//   - Events are forwarded as raw envelopes: no type registry, no decoding,
//     and no upcasts are involved, so the mirroring process does not need the
//     producers' event types and never skips an unregistered type.
//   - Per-source ordering is preserved: events are appended to dst in source
//     log order, one at a time.
//   - Mirror is one-directional. Two mirrors forming a cycle between stores
//     will copy events forever; nothing filters previously mirrored events.
//   - Runtime failures (read, append, checkpoint save) are reported to
//     MirrorOnError and retried after MirrorPollInterval; the checkpoint does
//     not advance past a failed event.
//
// Mirror does not coordinate writers: run one mirror per subscriptionID, and
// enforce that across processes with an external lease or a single owner.
func Mirror(ctx context.Context, src, dst EventStore, subscriptionID string, offsets SubscriptionStore, opts ...MirrorOption) error {
	if src == nil || dst == nil {
		return fmt.Errorf("eventbus: mirror requires both a source and a destination store")
	}
	if subscriptionID == "" {
		return fmt.Errorf("eventbus: mirror subscription ID cannot be empty")
	}
	if offsets == nil {
		return fmt.Errorf("eventbus: mirror requires a SubscriptionStore for its checkpoint")
	}

	cfg := &mirrorConfig{
		pollInterval: 200 * time.Millisecond,
		dedupWindow:  1024,
	}
	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("eventbus: mirror option cannot be nil")
		}
		if err := opt(cfg); err != nil {
			return err
		}
	}

	var comparer EventStoreOffsetComparer
	if cfg.resetOnRewind {
		c, ok := src.(EventStoreOffsetComparer)
		if !ok {
			return fmt.Errorf("eventbus: MirrorResetOnSourceRewind requires the source store to implement EventStoreOffsetComparer")
		}
		comparer = c
	}

	// Resolve the durable checkpoint. The first-run default is OffsetOldest,
	// which doubles as the legacy LoadOffset "absent" value, so a store without
	// SubscriptionStoreLookup resolves identically.
	from := OffsetOldest
	var saved Offset
	var found bool
	var err error
	if lookup, ok := offsets.(SubscriptionStoreLookup); ok {
		saved, found, err = lookup.LookupOffset(ctx, subscriptionID)
	} else {
		saved, err = offsets.LoadOffset(ctx, subscriptionID)
		found = saved != OffsetOldest
	}
	if err != nil {
		return fmt.Errorf("eventbus: load mirror offset for %q: %w", subscriptionID, err)
	}
	if found {
		if saved == OffsetNewest {
			return fmt.Errorf("eventbus: load mirror offset for %q: symbolic offset %q is not a durable checkpoint", subscriptionID, OffsetNewest)
		}
		from = saved
	}

	var dedup *dedupRing
	if cfg.dedupWindow > 0 {
		dedup = newDedupRing(cfg.dedupWindow)
	}

	m := &mirror{
		src:          src,
		dst:          dst,
		subscription: subscriptionID,
		offsets:      offsets,
		comparer:     comparer,
		cfg:          cfg,
		dedup:        dedup,
		offset:       from,
	}

	// A rebuilt source is detectable before the first read; failing here makes
	// store misbehavior a startup error rather than a silent stall.
	if comparer != nil {
		if err := m.reconcile(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("eventbus: mirror %q: %w", subscriptionID, err)
		}
	}

	if tailer, ok := src.(EventStoreTailer); ok {
		return m.runTail(ctx, tailer)
	}
	return m.runPoll(ctx)
}

// mirror is the running state of one Mirror call.
type mirror struct {
	src          EventStore
	dst          EventStore
	subscription string
	offsets      SubscriptionStore
	comparer     EventStoreOffsetComparer
	cfg          *mirrorConfig
	dedup        *dedupRing
	offset       Offset
}

// runPoll reads batches in a sleep loop; the fallback for sources without
// EventStoreTailer support.
func (m *mirror) runPoll(ctx context.Context) error {
	const batchSize = 100

pollLoop:
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		events, next, err := m.src.Read(ctx, m.offset, batchSize)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			m.report(fmt.Errorf("mirror: read after offset %s: %w", m.offset, err))
			if err := sleepCtx(ctx, m.cfg.pollInterval); err != nil {
				return err
			}
			continue
		}

		for _, stored := range events {
			if err := m.forward(ctx, stored); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := sleepCtx(ctx, m.cfg.pollInterval); err != nil {
					return err
				}
				continue pollLoop
			}
			// Checkpoint every forwarded envelope so a later sibling's failure
			// does not replay an already completed prefix (see follower.runPoll).
			m.advance(ctx, stored.Offset)
		}

		if next == m.offset {
			// At the tail — or holding a checkpoint the source no longer knows.
			// Only an explicit tail comparison tells the two apart.
			if err := m.maybeReconcile(ctx); err != nil {
				return err
			}
			if err := sleepCtx(ctx, m.cfg.pollInterval); err != nil {
				return err
			}
			continue
		}
		// A store may advance beyond the last returned envelope (for example, an
		// empty chunk). Persist that concrete resume token too.
		m.advance(ctx, next)
	}
}

// runTail consumes a pushing source, restarting the tail after errors.
func (m *mirror) runTail(ctx context.Context, tailer EventStoreTailer) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var tailErr error
		for stored, err := range tailer.Tail(ctx, m.offset) {
			if err != nil {
				tailErr = err
				break
			}
			if err := m.forward(ctx, stored); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				break
			}
			m.advance(ctx, stored.Offset)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if tailErr != nil {
			m.report(fmt.Errorf("mirror: tail after offset %s: %w", m.offset, tailErr))
		}
		// Either an error or a tail that ended unexpectedly: a rebuilt source is
		// one cause of both, so reconcile before re-tailing from the checkpoint.
		if err := m.maybeReconcile(ctx); err != nil {
			return err
		}
		if err := sleepCtx(ctx, m.cfg.pollInterval); err != nil {
			return err
		}
	}
}

// forward appends one stored event to the destination with its envelope
// preserved. The destination assigns its own offset; the source offset is
// carried only in the mirror's checkpoint.
func (m *mirror) forward(ctx context.Context, stored *StoredEvent) error {
	if m.dedup != nil && stored.ID != "" && m.dedup.observe(stored.ID) {
		return nil
	}

	event := &Event{
		ID:        stored.ID,
		Origin:    stored.Origin,
		Type:      stored.Type,
		Data:      stored.Data,
		Metadata:  stored.Metadata,
		Timestamp: stored.Timestamp,
	}
	if _, err := m.dst.Append(ctx, event); err != nil {
		// Forget the ID so the retry of this same stored event is not mistaken
		// for a completed duplicate.
		if m.dedup != nil && stored.ID != "" {
			m.dedup.forget(stored.ID)
		}
		failure := fmt.Errorf("mirror: append event at source offset %s: %w", stored.Offset, err)
		if ctx.Err() == nil {
			m.report(failure)
		}
		return failure
	}
	if m.cfg.onForward != nil {
		m.cfg.onForward(stored)
	}
	return nil
}

// advance records the mirror's position durably. SaveOffset failures are
// reported and do not stop the mirror: the position is redundant with the
// events themselves (at-least-once).
func (m *mirror) advance(ctx context.Context, offset Offset) {
	m.offset = offset
	if err := m.offsets.SaveOffset(ctx, m.subscription, offset); err != nil {
		m.report(fmt.Errorf("mirror: save offset for %q: %w", m.subscription, err))
	}
}

// maybeReconcile runs reconcile when MirrorResetOnSourceRewind is active,
// reporting failures instead of returning them: mid-run they are as transient
// as a read error. Returns only ctx.Err().
func (m *mirror) maybeReconcile(ctx context.Context) error {
	if m.comparer == nil {
		return nil
	}
	if err := m.reconcile(ctx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		m.report(fmt.Errorf("mirror: %w", err))
	}
	return nil
}

// reconcile compares the checkpoint with the source's current tail and resets
// to OffsetOldest when the checkpoint is ahead — the signature of a source log
// that was rebuilt or restored from an older copy.
func (m *mirror) reconcile(ctx context.Context) error {
	if m.offset == OffsetOldest {
		return nil // Nothing can be ahead of the beginning.
	}
	events, tail, err := m.src.Read(ctx, OffsetNewest, 0)
	if err != nil {
		return fmt.Errorf("resolve source tail: %w", err)
	}
	if len(events) != 0 {
		return fmt.Errorf("resolve source tail: EventStore.Read(OffsetNewest) returned %d event(s), want none", len(events))
	}
	if tail == OffsetNewest {
		return fmt.Errorf("resolve source tail: EventStore.Read(OffsetNewest) returned symbolic offset %q, want a concrete checkpoint", OffsetNewest)
	}
	cmp, err := m.comparer.CompareOffsets(m.offset, tail)
	if err != nil {
		return fmt.Errorf("compare checkpoint %q with source tail %q: %w", m.offset, tail, err)
	}
	if cmp > 0 {
		m.report(fmt.Errorf("mirror: checkpoint %q is ahead of source tail %q (source log was rebuilt); resetting %q to the start", m.offset, tail, m.subscription))
		m.advance(ctx, OffsetOldest)
	}
	return nil
}

// report forwards a runtime failure to the MirrorOnError observer, if any.
func (m *mirror) report(err error) {
	if m.cfg.onError != nil {
		m.cfg.onError(err)
	}
}
