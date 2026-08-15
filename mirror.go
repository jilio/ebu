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
// remembers so that a re-read source batch (a chunk-token re-read after a
// restart or tail restart) does not append the same event to the destination
// twice. Default 1024; 0 disables deduplication. Events without an ID
// (written by pre-envelope versions or external producers) are never
// deduplicated. Failed appends are retried in place and never re-read the
// batch, so the window does not need to cover a whole batch. The window only
// absorbs duplicates seen by one Mirror call: a restart resumes from the
// durable checkpoint, and the events between that checkpoint and the crash
// may be appended again — consumers of the destination deduplicate on
// StoredEvent.ID exactly as they would on the source.
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
// the source tail and, when the checkpoint is ahead, resets to OffsetOldest
// and re-reads the source from the beginning, reporting the reset to
// MirrorOnError.
//
// When the comparison runs: at startup; on the poll path, on reads that make
// no progress and on read errors; on the tail path, at every tail restart (a
// tail that ends or yields an error — a source whose Tail treats an
// ahead-of-tail offset as a permanently blocked idle long-poll is not
// detected mid-run). Checks are throttled to roughly one per ten poll
// intervals, and the reset itself requires two consecutive checks to agree
// (one transiently stale tail read must not destroy the checkpoint), so
// detection latency is around twenty poll intervals.
//
// What it can and cannot detect: the reset fires only while the restored
// source's tail is still behind the saved checkpoint. A source that was
// restored AND then refilled past the checkpoint is indistinguishable from
// ordinary progress by offsets alone — the mirror resumes at the checkpoint
// and the replaced events below it are not re-forwarded. Detecting that
// requires out-of-band versioning (for example, a per-rebuild log name or
// epoch), which is the caller's design decision, not an offset property.
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
// mirror's job is the whole log. The checkpoint is a SOURCE offset; offsets
// may be backed by src, dst, or a third store, provided that store accepts
// the source's offset tokens verbatim. A SubscriptionStore that parses or
// validates offsets in its own native format (the bundled SQLite store's
// SaveOffset accepts only its integer offsets) cannot checkpoint a source
// with a different token format: the very first save fails and Mirror ends
// with an error, turning the misconfiguration into an immediate failure
// instead of a run that looks healthy while every restart silently starts
// over from OffsetOldest.
//
// Semantics:
//   - Delivery into dst is at-least-once: a crash between Append and the
//     checkpoint save re-forwards that event on restart. Event IDs are
//     preserved, so consumers of dst deduplicate exactly as they would on src
//     (see FollowDedupWindow). Within one call, a failed append is retried in
//     place — the already forwarded prefix of a batch is never re-read — and
//     MirrorDedupWindow absorbs chunk-token re-reads after restarts. When the
//     source implements EventStoreOffsetComparer, the checkpoint also never
//     moves backward, even when a read redelivers earlier events.
//   - Events are forwarded as raw envelopes: no type registry, no decoding,
//     and no upcasts are involved, so the mirroring process does not need the
//     producers' event types and never skips an unregistered type.
//   - Per-source ordering is preserved: events are appended to dst in source
//     log order, one at a time.
//   - Mirror is one-directional. Two mirrors forming a cycle between stores
//     will copy events forever; nothing filters previously mirrored events.
//   - Runtime failures (read, append, checkpoint save) are reported to
//     MirrorOnError and retried after MirrorPollInterval; the checkpoint does
//     not advance past a failed event. One exception is fatal: a failure to
//     persist the very first checkpoint of a fresh subscription ends Mirror
//     with an error, because it is the signature of a checkpoint store that
//     cannot store the source's offset tokens (see below) — continuing would
//     look healthy while every restart re-copies the whole source.
//   - Source retention must not outpace the mirror. If the source drops
//     events past the saved checkpoint (a remote stream head-trimmed by
//     server-side retention), those events are unrecoverable and the mirror
//     will not silently skip the gap: it keeps reporting the failing read
//     and retrying. Recovery is an operator decision — clear or reseed the
//     checkpoint to pick a new starting point.
//
// Mirror does not coordinate writers: run one mirror per subscriptionID, and
// enforce that yourself — unlike Follow, Mirror is not bound to a bus, so it
// cannot detect a second concurrent mirror on the same ID even in the same
// process. Two concurrent mirrors interleave checkpoint saves; the result is
// re-forwarded duplicates in the destination (each mirror only checkpoints a
// prefix it has itself forwarded), never skipped events. Across processes,
// use an external lease or a single owner.
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

	// The comparer is used opportunistically whenever the source can order
	// offsets (the checkpoint monotonicity guard in advance); the rewind
	// option additionally requires it.
	comparer, _ := src.(EventStoreOffsetComparer)
	if cfg.resetOnRewind && comparer == nil {
		return fmt.Errorf("eventbus: MirrorResetOnSourceRewind requires the source store to implement EventStoreOffsetComparer")
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
		// A found checkpoint proves the offsets store accepts the source's
		// tokens; without one, the first save must prove it (see advance).
		saveProven: found,
	}

	// A rebuilt source is detectable before the first read. Like every later
	// reconcile, the check is best-effort: a transient source error here is
	// reported, not fatal — the same error one loop iteration later would be
	// retried forever, and the check reruns on non-progressing polls, read
	// errors, and tail restarts.
	if err := m.maybeReconcile(ctx); err != nil {
		return err
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

	// saveProven records that at least one checkpoint save succeeded (or a
	// saved checkpoint was found at startup): the first save failing is
	// fatal, later failures are tolerated (see advance).
	saveProven bool

	// rewindSeen/rewindFrom implement the two-check confirmation of a source
	// rewind: a reset only fires when two consecutive reconciles observe the
	// same checkpoint ahead of the tail, so one stale tail read (a source
	// behind a cache or replica) cannot destroy the checkpoint.
	rewindSeen bool
	rewindFrom Offset

	// lastReconcile throttles reconcile to roughly one check per ten poll
	// intervals: at the tail the reconcile path runs every poll, and each
	// check costs a tail-resolution round-trip against the source.
	lastReconcile time.Time
}

// runPoll reads batches in a sleep loop; the fallback for sources without
// EventStoreTailer support.
func (m *mirror) runPoll(ctx context.Context) error {
	const batchSize = 100

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		readFrom := m.offset
		events, next, err := m.src.Read(ctx, readFrom, batchSize)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			m.report(fmt.Errorf("mirror: read after offset %s: %w", readFrom, err))
			// A rebuilt source may reject the stale checkpoint with an error
			// rather than answer with an empty read, so the rewind check must
			// run on this path too (runTail already reconciles after tail
			// errors for the same reason).
			if err := m.maybeReconcile(ctx); err != nil {
				return err
			}
			if err := sleepCtx(ctx, m.cfg.pollInterval); err != nil {
				return err
			}
			continue
		}

		for _, stored := range events {
			// A failed append retries THIS event in place rather than
			// re-reading the batch: a re-read redelivers the already forwarded
			// prefix, and a resumable unit larger than the dedup window would
			// then land in the destination again on every retry cycle.
			for {
				err := m.forward(ctx, stored)
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := sleepCtx(ctx, m.cfg.pollInterval); err != nil {
					return err
				}
			}
			// Checkpoint every forwarded envelope so a later sibling's failure
			// does not replay an already completed prefix (see follower.runPoll).
			if err := m.advance(ctx, stored.Offset); err != nil {
				return err
			}
		}

		if next == readFrom {
			// The read made no progress: the mirror is at the tail — or holds a
			// checkpoint the source no longer knows; only an explicit tail
			// comparison tells the two apart. (Compared against the offset the
			// batch was read from, not the per-event advances above: a full
			// batch whose last event carries the batch's resume token is
			// progress, and the next read must follow immediately — sleeping
			// per batch would cap bulk replication at batchSize/pollInterval.)
			if err := m.maybeReconcile(ctx); err != nil {
				return err
			}
			if err := sleepCtx(ctx, m.cfg.pollInterval); err != nil {
				return err
			}
			continue
		}
		if next != m.offset {
			// A store may advance beyond the last returned envelope (for
			// example, an empty chunk). Persist that concrete resume token too.
			if err := m.advance(ctx, next); err != nil {
				return err
			}
		}
	}
}

// runTail consumes a pushing source, restarting the tail after errors.
func (m *mirror) runTail(ctx context.Context, tailer EventStoreTailer) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var tailErr, fatal error
		for stored, err := range tailer.Tail(ctx, m.offset) {
			if err != nil {
				tailErr = err
				break
			}
			// In-place retry, as in runPoll: re-tailing from the checkpoint
			// would redeliver the already forwarded prefix of the chunk.
			for {
				err := m.forward(ctx, stored)
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := sleepCtx(ctx, m.cfg.pollInterval); err != nil {
					return err
				}
			}
			if err := m.advance(ctx, stored.Offset); err != nil {
				fatal = err
				break
			}
		}
		if fatal != nil {
			return fatal
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

// advance records the mirror's position durably. When the source can order
// offsets (EventStoreOffsetComparer), the checkpoint never moves backward:
// the EventStore contract lets reads redeliver events at or before the
// resume point, and a chunk protocol can interleave an exact embedded offset
// with a lower chunk-start token — persisting the lower one would re-append
// an already checkpointed range after a crash.
//
// SaveOffset failures after the first proven save are reported and do not
// stop the mirror (the position is redundant with the events themselves).
// A failure of the FIRST save is fatal: it is the signature of a checkpoint
// store that cannot store the source's offset tokens at all, and continuing
// would look healthy while every restart re-copies the whole source.
func (m *mirror) advance(ctx context.Context, offset Offset) error {
	if m.comparer != nil && m.offset != OffsetOldest {
		if cmp, err := m.comparer.CompareOffsets(offset, m.offset); err == nil && cmp <= 0 {
			return nil
		}
	}
	m.offset = offset
	if err := m.offsets.SaveOffset(ctx, m.subscription, offset); err != nil {
		if !m.saveProven {
			return fmt.Errorf("eventbus: mirror %q: initial checkpoint save failed (does the offsets store accept the source's offset tokens?): %w", m.subscription, err)
		}
		m.report(fmt.Errorf("mirror: save offset for %q: %w", m.subscription, err))
		return nil
	}
	m.saveProven = true
	return nil
}

// maybeReconcile runs reconcile when MirrorResetOnSourceRewind is active,
// reporting failures instead of returning them: mid-run they are as transient
// as a read error. Returns only ctx.Err(). Checks are throttled to roughly
// one per ten poll intervals — at the tail this path runs every poll, and
// each check costs a tail-resolution round-trip against the source.
func (m *mirror) maybeReconcile(ctx context.Context) error {
	if !m.cfg.resetOnRewind {
		return nil
	}
	if !m.lastReconcile.IsZero() && time.Since(m.lastReconcile) < 10*m.cfg.pollInterval {
		return nil
	}
	m.lastReconcile = time.Now()
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
// that was rebuilt or restored from an older copy. The reset is destructive
// (it re-appends the surviving source history), so it requires TWO
// consecutive checks to observe the same checkpoint ahead of the tail: one
// transiently stale tail read (a source answering from a lagging replica or
// cache) must not destroy the checkpoint.
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
	if cmp <= 0 {
		m.rewindSeen = false
		return nil
	}
	if !m.rewindSeen || m.rewindFrom != m.offset {
		m.rewindSeen, m.rewindFrom = true, m.offset
		m.report(fmt.Errorf("mirror: checkpoint %q is ahead of source tail %q; resetting %q to the start if the next check agrees", m.offset, tail, m.subscription))
		return nil
	}
	m.rewindSeen = false
	m.report(fmt.Errorf("mirror: checkpoint %q is ahead of source tail %q (source log was rebuilt); resetting %q to the start", m.offset, tail, m.subscription))
	// Save directly: the reset intentionally moves the checkpoint backward,
	// which advance's monotonicity guard would refuse.
	m.offset = OffsetOldest
	if err := m.offsets.SaveOffset(ctx, m.subscription, OffsetOldest); err != nil {
		m.report(fmt.Errorf("mirror: save offset for %q: %w", m.subscription, err))
	}
	return nil
}

// report forwards a runtime failure to the MirrorOnError observer, if any.
func (m *mirror) report(err error) {
	if m.cfg.onError != nil {
		m.cfg.onError(err)
	}
}
