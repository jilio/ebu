package eventbus

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrOffsetTrimmed reports a read whose starting offset precedes events that
// retention removed: continuing from that offset would silently skip them.
// The condition is permanent for that offset. Readers decide how to recover:
// rebuild from a snapshot, or acknowledge the gap and resume from
// OffsetTrimmedError.ResumeFrom. Match it with errors.Is.
var ErrOffsetTrimmed = errors.New("eventbus: offset precedes the retained log")

// OffsetTrimmedError details an ErrOffsetTrimmed read. errors.Is matches both
// ErrOffsetTrimmed and Err.
type OffsetTrimmedError struct {
	// Offset is the offset the read started from.
	Offset Offset

	// ResumeFrom is an offset from which reading succeeds: every event
	// retained after the trimmed prefix follows it. It is OffsetOldest when
	// the store cannot name the boundary; reading from OffsetOldest returns
	// the retained log from its start in either case.
	ResumeFrom Offset

	// Err is the store's underlying error, if any.
	Err error
}

// Error implements error.
func (e *OffsetTrimmedError) Error() string {
	msg := fmt.Sprintf("eventbus: offset %q precedes the retained log (resume from %q)", e.Offset, e.ResumeFrom)
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

// Unwrap returns ErrOffsetTrimmed and, when set, the underlying store error.
func (e *OffsetTrimmedError) Unwrap() []error {
	if e.Err != nil {
		return []error{ErrOffsetTrimmed, e.Err}
	}
	return []error{ErrOffsetTrimmed}
}

// RetentionPolicy selects the events an EventStoreTrimmer removes. Every rule
// is optional; a zero policy removes nothing. Offsets of retained events never
// change and are never reused.
//
// Through, OlderThan and MaxEvents remove a prefix of the log. The store
// records how far its prefix has been trimmed, and reading from an older
// concrete offset fails with ErrOffsetTrimmed instead of silently skipping the
// removed events. Reading from OffsetOldest is never an error: it starts at
// the oldest retained event.
//
// MaxPerPartition instead removes events from inside the log, like key-based
// log compaction: it keeps the newest events of each partition. Readers are
// not told about those removals; they observe only what remains.
//
// The rules combine: an event is removed when any rule selects it, so the
// strictest prefix rule decides how far the prefix goes, and the partition
// cap then applies to what remains.
type RetentionPolicy struct {
	// Through removes every event at or before this offset. It must be a
	// concrete offset issued by the store; OffsetOldest disables the rule.
	// An offset beyond the current tail trims only up to the tail.
	Through Offset

	// OlderThan removes the oldest events, in log order, whose Timestamp is
	// before this time. It stops at the first event that is not older, so an
	// event with a skewed timestamp can delay trimming but never punches a
	// hole into the log. Events without a timestamp count as oldest. The zero
	// time disables the rule.
	OlderThan time.Time

	// MaxEvents keeps at most this many of the newest events. Zero disables
	// the rule.
	MaxEvents int

	// PartitionKey names the event Metadata key whose value partitions the
	// log for MaxPerPartition. Events without the key, or with an empty
	// value, form one partition with the empty value.
	PartitionKey string

	// MaxPerPartition keeps at most this many of the newest events in each
	// partition. Zero disables the rule; it requires PartitionKey.
	MaxPerPartition int
}

// Validate reports a policy no store can apply.
func (p RetentionPolicy) Validate() error {
	switch {
	case p.Through == OffsetNewest:
		return fmt.Errorf("eventbus: retention Through cannot be the symbolic offset %q", OffsetNewest)
	case p.MaxEvents < 0:
		return fmt.Errorf("eventbus: retention MaxEvents must not be negative, got %d", p.MaxEvents)
	case p.MaxPerPartition < 0:
		return fmt.Errorf("eventbus: retention MaxPerPartition must not be negative, got %d", p.MaxPerPartition)
	case p.MaxPerPartition > 0 && p.PartitionKey == "":
		return errors.New("eventbus: retention MaxPerPartition requires PartitionKey")
	}
	return nil
}

// TrimResult reports what an EventStoreTrimmer.Trim call did.
type TrimResult struct {
	// Deleted is the number of events removed by this call.
	Deleted int64

	// TrimmedThrough is the store's prefix boundary after the call: every
	// event at or before it has been removed by Trim, and reads from older
	// concrete offsets fail with ErrOffsetTrimmed. OffsetOldest when no
	// prefix has been trimmed.
	TrimmedThrough Offset
}

// EventStoreTrimmer is an optional capability for stores that can enforce a
// RetentionPolicy while keeping offsets stable: it bounds a log by offset,
// age, total count and per-partition count, and gives readers whose position
// fell behind the trimmed prefix a clear ErrOffsetTrimmed signal (see
// RetentionPolicy). Implementations must treat a zero policy as a query: it
// removes nothing and reports the current TrimmedThrough, and callers rely on
// that to read the boundary.
//
// Retention never consults subscriptions or snapshots. Trimming past a
// checkpoint that a reader still needs makes that reader fail with
// ErrOffsetTrimmed rather than lose events silently; bound Through by such
// checkpoints when they must stay readable. EventStoreTruncator remains the
// lower-level compaction primitive and does not record a trimmed prefix.
type EventStoreTrimmer interface {
	Trim(ctx context.Context, policy RetentionPolicy) (TrimResult, error)
}
