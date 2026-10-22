package state

import (
	"context"
	"errors"
	"fmt"

	eventbus "github.com/jilio/ebu"
)

// CompactResult reports what Materializer.Compact did.
type CompactResult struct {
	// Snapshot is the offset the saved snapshot covers (LastOffset at capture).
	Snapshot eventbus.Offset

	// Through is the offset the log was trimmed through, or OffsetOldest when
	// nothing was trimmed (a keep offset of OffsetOldest, or a store that
	// cannot remove events).
	Through eventbus.Offset

	// Removed is the number of events the store reported deleting.
	Removed int64
}

// Compact saves a snapshot of every registered collection under snapshotID and
// then removes the log prefix that the snapshot covers, bounding both the log
// and the replay a cold start needs (LoadSnapshotFrom, then Replay from the
// returned offset).
//
// store must implement eventbus.EventStoreSnapshotter. keep lists offsets that
// other readers of the same log still resume from — their checkpoints. The
// log is trimmed only through the oldest of the snapshot offset and every keep
// offset, so those readers lose nothing; a keep offset of OffsetOldest skips
// trimming. Comparing keep offsets requires eventbus.EventStoreOffsetComparer.
//
// Removal uses eventbus.EventStoreTrimmer (RetentionPolicy.Through) when the
// store implements it, so a reader that still resumes from an older offset
// gets eventbus.ErrOffsetTrimmed instead of silently skipping events.
// Otherwise it uses eventbus.EventStoreTruncator. A store with neither keeps
// its events; only the snapshot is saved.
//
// Compact is serialized with SaveSnapshotTo, and the snapshot is durably
// saved before anything is removed: when Compact fails after saving, the log
// still holds every event the snapshot does not cover. Like SaveSnapshotTo it
// refuses to run before any event was applied.
func (m *Materializer) Compact(ctx context.Context, store eventbus.EventStore, snapshotID string, keep ...eventbus.Offset) (CompactResult, error) {
	snapshotter, ok := store.(eventbus.EventStoreSnapshotter)
	if !ok {
		return CompactResult{}, errors.New("state: compact: store does not implement eventbus.EventStoreSnapshotter")
	}
	comparer, _ := store.(eventbus.EventStoreOffsetComparer)
	for _, offset := range keep {
		if offset == eventbus.OffsetNewest {
			return CompactResult{}, fmt.Errorf("state: compact: keep offset cannot be the symbolic %q", eventbus.OffsetNewest)
		}
		if offset != eventbus.OffsetOldest && comparer == nil {
			return CompactResult{}, errors.New("state: compact: keep offsets require eventbus.EventStoreOffsetComparer")
		}
	}

	m.snapshotMu.Lock()
	defer m.snapshotMu.Unlock()

	offset, encoded, err := m.captureSnapshot(snapshotID)
	if err != nil {
		return CompactResult{}, err
	}
	if err := snapshotter.SaveSnapshot(ctx, snapshotID, offset, encoded); err != nil {
		return CompactResult{}, fmt.Errorf("state: save snapshot %q: %w", snapshotID, err)
	}
	result := CompactResult{Snapshot: offset, Through: offset}

	for _, k := range keep {
		if k == eventbus.OffsetOldest {
			result.Through = eventbus.OffsetOldest
			return result, nil
		}
		cmp, err := comparer.CompareOffsets(k, result.Through)
		if err != nil {
			return result, fmt.Errorf("state: compact: compare keep offset %q: %w", k, err)
		}
		if cmp < 0 {
			result.Through = k
		}
	}

	switch remover := store.(type) {
	case eventbus.EventStoreTrimmer:
		trimmed, err := remover.Trim(ctx, eventbus.RetentionPolicy{Through: result.Through})
		if err != nil {
			return result, fmt.Errorf("state: compact: trim through %q: %w", result.Through, err)
		}
		result.Removed = trimmed.Deleted
	case eventbus.EventStoreTruncator:
		deleted, err := remover.TruncateBefore(ctx, result.Through)
		if err != nil {
			return result, fmt.Errorf("state: compact: truncate through %q: %w", result.Through, err)
		}
		result.Removed = deleted
	default:
		result.Through = eventbus.OffsetOldest
	}
	return result, nil
}
