package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	// ErrReplicationPosition identifies an invalid position or a position from a
	// different replication identity/generation.
	ErrReplicationPosition = errors.New("eventbus: invalid replication position")
	// ErrReplicationHistory means a saved checkpoint is ahead of the source tail.
	// Restore the matching source or start a new generation; do not reuse offsets.
	ErrReplicationHistory = errors.New("eventbus: replication history changed")
	// ErrReplicatorStarted means Run was already called on this object.
	ErrReplicatorStarted = errors.New("eventbus: replicator already started")
	// ErrReplicationStopped means Run ended before the requested point was confirmed.
	ErrReplicationStopped = errors.New("eventbus: replication stopped")
)

// ReplicationPosition identifies a SOURCE boundary, scoped to one immutable
// source/destination pair and generation. It can be serialized across restarts.
// Use Replicator.Position for an Append result or Capture for a group of writes.
// Never use a non-last chunk member's StoredEvent.Offset as proof of that
// member's replication: such offsets may refer to the start of the chunk.
type ReplicationPosition struct {
	ID         string `json:"id"`
	Generation string `json:"generation"`
	Offset     Offset `json:"offset"`
}

// ReplicatorConfig binds one replication relationship. ID and Generation must
// be valid UTF-8 strings for unambiguous persisted/serialized identity. ID identifies the
// source/destination pair. Generation is an application-owned immutable epoch:
// change it after replacing, rebuilding or restoring EITHER log. Epoch changes
// require a new Replicator and checkpoint namespace; they cannot be inferred
// from opaque offsets, especially after a rebuilt log has caught up again.
//
// Source must implement EventStoreOffsetComparer, expose every event without
// filtering/skipping decode failures, and retain all history not yet replicated.
// A fresh generation starts at OffsetOldest and requires the full source history. Source and destination must be different logs with no
// cyclic mirror topology. Destination must preserve all acknowledged records (or
// a recovery-equivalent snapshot). There must be one owner of the checkpoint
// namespace across processes; Replicator does not acquire distributed leases.
//
// Wait inherits Destination.Append's acknowledgement guarantees. For survival
// of source-worker loss, Destination must be outside that worker's failure
// domain and acknowledge only after meeting your storage durability policy.
// An in-memory or buffered destination does not become durable through this API.
// Checkpoints must durably store SOURCE offsets verbatim; losing a checkpoint
// causes redelivery. Both replicas must tolerate duplicate event IDs on restart.
type ReplicatorConfig struct {
	Source      EventStore
	Destination EventStore
	Checkpoints SubscriptionStore
	ID          string
	Generation  string
}

// Replicator runs Mirror's ordered copy loop and exposes acknowledged prefix
// barriers. Construct with NewReplicator. Run is single-use; after it ends,
// construct another instance with the same config to resume the checkpoint.
// It owns no store lifetimes and is independent of EventBus.Shutdown.
type Replicator struct {
	config       ReplicatorConfig
	comparer     EventStoreOffsetComparer
	mirrorConfig *mirrorConfig
	checkpointID string
	mu           sync.Mutex
	changed      chan struct{}
	wake         chan struct{}
	started      bool
	stopped      bool
	runErr       error
	confirmed    Offset
	ready        bool
}

// NewReplicator prepares a replicator without I/O or background goroutines.
// Mirror options configure retry timing, deduplication and observers. Automatic
// rewind reset is rejected: reusing an acknowledged generation after history
// replacement would invalidate its barriers. Ordinary Mirror is unchanged.
func NewReplicator(config ReplicatorConfig, opts ...MirrorOption) (*Replicator, error) {
	if config.Source == nil || config.Destination == nil || config.Checkpoints == nil {
		return nil, fmt.Errorf("eventbus: replication requires source, destination and checkpoint stores")
	}
	if config.ID == "" || config.Generation == "" {
		return nil, fmt.Errorf("eventbus: replication ID and generation are required")
	}
	if !utf8.ValidString(config.ID) || !utf8.ValidString(config.Generation) {
		return nil, fmt.Errorf("eventbus: replication ID and generation must be valid UTF-8")
	}
	comparer, ok := config.Source.(EventStoreOffsetComparer)
	if !ok {
		return nil, fmt.Errorf("eventbus: replication source must implement EventStoreOffsetComparer")
	}
	cfg := &mirrorConfig{pollInterval: 200 * time.Millisecond, dedupWindow: 1024}
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("eventbus: mirror option cannot be nil")
		}
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	if cfg.resetOnRewind {
		return nil, fmt.Errorf("eventbus: confirmed replication cannot reset history; use a new generation")
	}
	// A structured tuple prevents IDs/generations containing separators from
	// aliasing another relationship's checkpoints. Plain Mirror IDs stay separate.
	key, _ := json.Marshal([2]string{config.ID, config.Generation})
	r := &Replicator{config: config, comparer: comparer, mirrorConfig: cfg,
		checkpointID: "ebu:replication:" + string(key), changed: make(chan struct{}), wake: make(chan struct{}, 1)}
	cfg.progress = r
	return r, nil
}

// Position scopes an offset returned by Source.Append to this relationship.
// It delegates token comparisons to the store; this is not an existence or
// provenance check. Pass only offsets actually issued by this source generation.
// OffsetNewest is rejected; use Capture to resolve it.
func (r *Replicator) Position(offset Offset) (ReplicationPosition, error) {
	if offset == OffsetNewest {
		return ReplicationPosition{}, fmt.Errorf("%w: newest is symbolic", ErrReplicationPosition)
	}
	cmp, err := r.comparer.CompareOffsets(offset, OffsetOldest)
	if err != nil {
		return ReplicationPosition{}, fmt.Errorf("%w: %w", ErrReplicationPosition, err)
	}
	if cmp < 0 {
		return ReplicationPosition{}, fmt.Errorf("%w: offset precedes the start", ErrReplicationPosition)
	}
	return ReplicationPosition{ID: r.config.ID, Generation: r.config.Generation, Offset: offset}, nil
}

// Capture resolves the current source tail once. Call it after successful local
// writes, then Wait for the returned position to establish a prefix barrier.
// Concurrent later appends do not extend that barrier. This method does not
// itself wait for replication or start Run.
func (r *Replicator) Capture(ctx context.Context) (ReplicationPosition, error) {
	if err := ctx.Err(); err != nil {
		return ReplicationPosition{}, err
	}
	events, offset, err := r.config.Source.Read(ctx, OffsetNewest, 0)
	if err != nil {
		return ReplicationPosition{}, fmt.Errorf("replication: capture source tail: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return ReplicationPosition{}, err
	}
	if len(events) != 0 {
		return ReplicationPosition{}, fmt.Errorf("replication: tail lookup returned historical events")
	}
	return r.Position(offset)
}

// Run copies until canceled or a fatal startup/checkpoint error. Transient
// mirror errors are retried in place. Run uses batched Read so cursor-only
// advances (empty chunks) are visible; Wait wakes idle polling without bypassing
// retry backoff. Concurrent/repeated Run calls fail with
// ErrReplicatorStarted; use a new instance to resume after this call returns.
// Observers run on this goroutine: never call Wait on this replicator from one.
func (r *Replicator) Run(ctx context.Context) (err error) {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return ErrReplicatorStarted
	}
	r.started = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.stopped = true
		r.runErr = errors.Join(ErrReplicationStopped, err)
		close(r.changed)
		r.mu.Unlock()
	}()
	return mirrorRun(ctx, r.config.Source, r.config.Destination, r.checkpointID, r.config.Checkpoints, r.mirrorConfig)
}

// Confirmed returns the latest confirmed SOURCE prefix and whether startup has
// established one. The empty prefix is valid. A previously confirmed prefix
// remains confirmed after Run stops, subject to the configured storage contract.
func (r *Replicator) Confirmed() (ReplicationPosition, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return ReplicationPosition{ID: r.config.ID, Generation: r.config.Generation, Offset: r.confirmed}, r.ready
}

// Wait waits until every source event through target has been acknowledged by
// Destination.Append and its SOURCE checkpoint has been saved. It never treats
// the local append, a read attempt, or an observer callback as acknowledgement.
// This is a replication barrier, not evidence that a destination projection has
// applied the events. Configure destination durability separately (see config).
//
// Wait may start before Run, and multiple waiters are independent. Canceling a
// wait neither cancels replication nor rolls back any writes: a timeout has an
// ambiguous outcome, so retry the same position instead of reissuing the write.
// Positions from another ID/generation fail immediately. If Run ends before
// target is confirmed, the error matches ErrReplicationStopped and Run's error.
func (r *Replicator) Wait(ctx context.Context, target ReplicationPosition) error {
	if target.ID != r.config.ID || target.Generation != r.config.Generation {
		return fmt.Errorf("%w: ID or generation mismatch", ErrReplicationPosition)
	}
	if _, err := r.Position(target.Offset); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mu.Lock()
		ready, confirmed, stopped, runErr, changed := r.ready, r.confirmed, r.stopped, r.runErr, r.changed
		r.mu.Unlock()
		if ready {
			cmp, err := r.comparer.CompareOffsets(confirmed, target.Offset)
			if err != nil {
				return fmt.Errorf("replication: compare wait target: %w", err)
			}
			if cmp >= 0 {
				return nil
			}
		}
		if stopped {
			return runErr
		}
		select {
		case r.wake <- struct{}{}:
		default:
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Replicator) initialize(ctx context.Context, from Offset) error {
	point, err := r.Position(from)
	if err != nil {
		return fmt.Errorf("replication: invalid saved checkpoint: %w", err)
	}
	tail, err := r.Capture(ctx)
	if err != nil {
		return err
	}
	cmp, err := r.comparer.CompareOffsets(point.Offset, tail.Offset)
	if err != nil {
		return fmt.Errorf("replication: compare saved checkpoint: %w", err)
	}
	if cmp > 0 {
		return fmt.Errorf("%w: checkpoint %q is ahead of tail %q", ErrReplicationHistory, from, tail.Offset)
	}
	r.confirm(from)
	return nil
}

func (r *Replicator) confirm(offset Offset) {
	r.mu.Lock()
	r.confirmed = offset
	r.ready = true
	close(r.changed)
	r.changed = make(chan struct{})
	r.mu.Unlock()
}
