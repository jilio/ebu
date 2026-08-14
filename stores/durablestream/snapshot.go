package durablestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	eventbus "github.com/jilio/ebu"
)

// Ensure Store implements the optional snapshot capability.
var _ eventbus.EventStoreSnapshotter = (*Store)(nil)

// snapStreamSuffix names the companion stream that holds snapshots for a
// store's main stream. The companion path (`<streamPath>.snap`) is reserved:
// do not create an ordinary event stream there.
const snapStreamSuffix = ".snap"

// snapshotRecord is the wire format of one snapshot on the companion stream.
type snapshotRecord struct {
	SnapshotID string          `json:"snapshot_id"`
	AtOffset   string          `json:"at_offset"`
	Blob       json.RawMessage `json:"blob"`
}

// snapState tracks lazy creation of the companion stream, shared by all
// SaveSnapshot calls on this Store.
type snapState struct {
	mu      sync.Mutex
	created bool
}

// SaveSnapshot implements eventbus.EventStoreSnapshotter by appending a
// snapshot record to the companion stream `<streamPath>.snap` (created lazily,
// content type application/json). Each save is one protocol append — atomic by
// the protocol's atomicity requirement — and the record appended last wins:
// LoadSnapshot returns the newest record for the id, so semantics are
// last-write-wins per snapshotID, like the SQLite store's upsert.
//
// atOffset must be a concrete offset of THIS store's main stream (an Append
// return value or a StoredEvent/nextOffset from Read); OffsetNewest is
// rejected. Offsets stay valid for the lifetime of the stream (protocol §6),
// including across server-side retention of the main stream, so a loaded
// snapshot remains a correct resume point.
//
// The companion stream is append-only like any other: it grows by one record
// per save and this client never deletes from it (see the package
// documentation on retention). Transient failures are retried with the
// store's WithRetry policy.
func (s *Store) SaveSnapshot(ctx context.Context, snapshotID string, atOffset eventbus.Offset, blob json.RawMessage) error {
	if atOffset == eventbus.OffsetNewest {
		return fmt.Errorf("durablestream: save snapshot %q: symbolic offset %q is not a durable position", snapshotID, eventbus.OffsetNewest)
	}

	data, err := json.Marshal(snapshotRecord{
		SnapshotID: snapshotID,
		AtOffset:   string(atOffset),
		Blob:       blob,
	})
	if err != nil {
		return fmt.Errorf("durablestream: marshal snapshot %q: %w", snapshotID, err)
	}

	// Serialize saves: concurrent appends would still be individually atomic,
	// but a single writer keeps "last record wins" aligned with call order in
	// this process.
	s.snap.mu.Lock()
	defer s.snap.mu.Unlock()

	if err := s.ensureSnapStream(ctx); err != nil {
		return fmt.Errorf("durablestream: create snapshot stream: %w", err)
	}

	snapPath := s.path + snapStreamSuffix
	var lastErr error
	for attempt := 1; attempt <= s.cfg.retryAttempts; attempt++ {
		if attempt > 1 {
			if err := backoff(ctx, s.cfg.retryBaseDelay, attempt-1); err != nil {
				return fmt.Errorf("durablestream: save snapshot %q: %w", snapshotID, err)
			}
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("durablestream: save snapshot %q: %w", snapshotID, err)
		}

		// A fresh writer per attempt: snapshots are infrequent, and a cached
		// writer could be bound to stale stream metadata. The attempt context
		// bounds both the writer's HEAD and the send.
		attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
		writer, err := s.client.Writer(attemptCtx, snapPath)
		if err == nil {
			err = writer.Send(data, nil)
		}
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("durablestream: save snapshot %q: %w", snapshotID, err)
			if errors.Is(err, durablestream.ErrNotFound) && ctx.Err() == nil {
				// The companion stream disappeared after we cached its
				// creation (expired or deleted server-side). Re-create it and
				// retry within the attempt budget instead of failing every
				// save until process restart.
				s.snap.created = false
				if cerr := s.ensureSnapStream(ctx); cerr != nil {
					return fmt.Errorf("durablestream: create snapshot stream: %w", cerr)
				}
				continue
			}
			if ctx.Err() != nil || !isRetryable(err) {
				return lastErr
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("durablestream: save snapshot %q: giving up after %d attempts: %w", snapshotID, s.cfg.retryAttempts, lastErr)
}

// LoadSnapshot implements eventbus.EventStoreSnapshotter: it reads the
// companion stream from the beginning and returns the newest record saved
// under snapshotID. When the companion stream does not exist, or holds no
// record for the id, it returns (OffsetOldest, nil, nil) — "replay from the
// beginning", never an error.
//
// The read is proportional to the number of snapshots ever saved on this
// stream (the companion log is append-only); periodic snapshotting keeps that
// small. Do not bound the companion stream by head-trimming retention — a
// trimmed head makes the surviving records unreachable (no earliest-offset
// discovery in the protocol) and LoadSnapshot fails loudly; whole-stream
// expiry is safe and reads as "no snapshot".
func (s *Store) LoadSnapshot(ctx context.Context, snapshotID string) (eventbus.Offset, json.RawMessage, error) {
	snapPath := s.path + snapStreamSuffix

	var (
		found    bool
		atOffset eventbus.Offset
		blob     json.RawMessage
	)

	offset := durablestream.ZeroOffset
	for {
		var result *durablestream.StreamData
		var lastErr error
		for attempt := 1; attempt <= s.cfg.retryAttempts; attempt++ {
			if attempt > 1 {
				if err := backoff(ctx, s.cfg.retryBaseDelay, attempt-1); err != nil {
					return eventbus.OffsetOldest, nil, fmt.Errorf("durablestream: load snapshot %q: %w", snapshotID, err)
				}
			}
			if err := ctx.Err(); err != nil {
				return eventbus.OffsetOldest, nil, fmt.Errorf("durablestream: load snapshot %q: %w", snapshotID, err)
			}

			attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
			res, err := s.client.Reader(snapPath, offset).Read(attemptCtx)
			cancel()
			if err != nil {
				if errors.Is(err, durablestream.ErrNotFound) {
					// The companion stream is absent: never created, expired,
					// or deleted while we were paging through it. A record
					// already found on an earlier page is still the best
					// available snapshot; with nothing found, this is a miss.
					if found {
						return atOffset, blob, nil
					}
					return eventbus.OffsetOldest, nil, nil
				}
				if errors.Is(err, durablestream.ErrGone) {
					// The companion stream's head was trimmed by server-side
					// retention. The protocol offers no way to discover the
					// earliest retained offset, so records that survived the
					// trim are unreachable from here; a record from an earlier
					// page is still usable, otherwise fail loudly rather than
					// report "no snapshot" while newer records survive.
					if found {
						return atOffset, blob, nil
					}
					return eventbus.OffsetOldest, nil, fmt.Errorf("durablestream: load snapshot %q: companion stream %q was head-trimmed by server retention and its surviving records are unreachable (the protocol has no earliest-offset discovery); do not head-trim snapshot companion streams — expire them whole instead: %w", snapshotID, snapPath, err)
				}
				lastErr = fmt.Errorf("durablestream: load snapshot %q: %w", snapshotID, err)
				if ctx.Err() != nil || !isRetryable(err) {
					return eventbus.OffsetOldest, nil, lastErr
				}
				continue
			}
			result = res
			break
		}
		if result == nil {
			return eventbus.OffsetOldest, nil, fmt.Errorf("durablestream: load snapshot %q: giving up after %d attempts: %w", snapshotID, s.cfg.retryAttempts, lastErr)
		}

		var rawRecords []json.RawMessage
		if len(result.Data) > 0 {
			if err := json.Unmarshal(result.Data, &rawRecords); err != nil {
				return eventbus.OffsetOldest, nil, fmt.Errorf("durablestream: load snapshot %q: unmarshal response: %w", snapshotID, err)
			}
		}
		for i, raw := range rawRecords {
			var record snapshotRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				s.handleSnapshotDecodeError(i, raw, err)
				continue
			}
			if record.SnapshotID != snapshotID {
				continue
			}
			if eventbus.Offset(record.AtOffset) == eventbus.OffsetNewest {
				// A symbolic offset is never a durable position (SaveSnapshot
				// rejects it on write, and every sibling checkpoint-load path
				// rejects it on read). The companion stream is externally
				// writable, so treat such a record as malformed and keep the
				// previous good one rather than let a caller resume at "$"
				// and silently skip history.
				s.handleSnapshotDecodeError(i, raw, fmt.Errorf("symbolic at_offset %q is not a durable position", eventbus.OffsetNewest))
				continue
			}
			// Later records supersede earlier ones: last write wins.
			found = true
			atOffset = eventbus.Offset(record.AtOffset)
			blob = record.Blob
		}

		if result.UpToDate || result.NextOffset == offset {
			break
		}
		offset = result.NextOffset
	}

	if !found {
		return eventbus.OffsetOldest, nil, nil
	}
	return atOffset, blob, nil
}

// ensureSnapStream lazily creates the companion stream (idempotent on the
// server) with the store's retry policy. Caller holds s.snap.mu.
func (s *Store) ensureSnapStream(ctx context.Context) error {
	if s.snap.created {
		return nil
	}
	snapPath := s.path + snapStreamSuffix
	var lastErr error
	for attempt := 1; attempt <= s.cfg.retryAttempts; attempt++ {
		if attempt > 1 {
			if err := backoff(ctx, s.cfg.retryBaseDelay, attempt-1); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
		_, err := s.client.Create(attemptCtx, snapPath, &durablestream.CreateOptions{
			ContentType: "application/json",
		})
		cancel()
		if err != nil {
			lastErr = err
			if ctx.Err() != nil || !isRetryable(err) {
				return lastErr
			}
			continue
		}
		s.snap.created = true
		return nil
	}
	return fmt.Errorf("giving up after %d attempts: %w", s.cfg.retryAttempts, lastErr)
}

// handleSnapshotDecodeError reports a malformed snapshot record that is being
// skipped, with the same precedence as event decode errors: the decode error
// handler first, the logger as fallback, silence otherwise.
func (s *Store) handleSnapshotDecodeError(index int, raw []byte, err error) {
	if s.cfg.decodeErrorHandler != nil {
		s.cfg.decodeErrorHandler(err, raw)
		return
	}
	if s.cfg.logger != nil {
		s.cfg.logger.Printf("durablestream: skipping malformed snapshot record at index %d: %v", index, err)
	}
}
