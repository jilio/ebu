package durablestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	durablestreams "github.com/durable-streams/durable-streams/packages/client-go"
	eventbus "github.com/jilio/ebu"
)

// Ensure Store implements the optional snapshot capability.
var _ eventbus.EventStoreSnapshotter = (*Store)(nil)

// snapStreamSuffix names the companion stream that holds snapshots for a
// store's main stream. The companion path (`<streamPath>.snap`) is reserved:
// do not create an ordinary event stream there. The reservation cannot be
// verified server-side — stream creation is idempotent per media type
// (content-type parameters are ignored by conforming servers), so a
// pre-existing event stream at the companion path is indistinguishable from
// a snapshot log at creation time. The constructor's suffix rejection
// prevents this library from creating the collision, and LoadSnapshot
// reports any foreign records it skips (see handleSnapshotDecodeError).
const snapStreamSuffix = ".snap"

// snapshotRecord is the wire format of one snapshot on the companion stream.
type snapshotRecord struct {
	SnapshotID string          `json:"snapshot_id"`
	AtOffset   string          `json:"at_offset"`
	Blob       json.RawMessage `json:"blob"`
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
// snapshotID must be non-empty: on this store the id is the discriminator
// that separates snapshot records from foreign data on the externally
// writable companion stream, so an empty id would be indistinguishable from
// a stray event envelope.
//
// The companion stream is append-only like any other: it grows by one record
// per save and this client never deletes from it (see the package
// documentation on retention). Transient failures are retried with the
// store's WithRetry policy; lazy creation of the companion stream shares the
// same per-attempt budget as the send. If the companion stream disappears
// server-side after it was created (TTL, operator cleanup), the save
// re-creates it once and retries instead of failing until process restart.
func (s *Store) SaveSnapshot(ctx context.Context, snapshotID string, atOffset eventbus.Offset, blob json.RawMessage) error {
	if atOffset == eventbus.OffsetNewest {
		return fmt.Errorf("durablestream: save snapshot %q: symbolic offset %q is not a durable position", snapshotID, eventbus.OffsetNewest)
	}
	if snapshotID == "" {
		return fmt.Errorf("durablestream: save snapshot: snapshot ID cannot be empty (indistinguishable from a foreign record on the companion stream)")
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

	snapPath := s.path + snapStreamSuffix
	recreated := false
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

		// Lazy creation inside the attempt loop: creation and send share one
		// attempt budget (one network call still consumes one attempt — no
		// nested retry loops), and a companion stream that disappeared
		// server-side is re-created on the next save.
		if !s.snap.created {
			if err := s.createSnapStream(ctx); err != nil {
				lastErr = fmt.Errorf("create companion stream %q: %w", snapPath, err)
				if ctx.Err() != nil || !isRetryable(err) {
					return fmt.Errorf("durablestream: save snapshot %q: %w", snapshotID, lastErr)
				}
				continue
			}
			s.snap.created = true
		}

		// One append per attempt, bounded by the per-attempt timeout.
		err := s.sendSnapshotRecord(ctx, data)
		if err != nil && errors.Is(err, durablestreams.ErrStreamNotFound) && !recreated && ctx.Err() == nil {
			// The companion stream disappeared after we cached its creation
			// (expired or deleted server-side). Re-create and resend inline —
			// no attempt consumed, no repeated backoff — so recovery also
			// works when the 404 lands on the final attempt. At most once: a
			// second 404 after a successful re-create is a real failure.
			recreated = true
			s.snap.created = false
			if cerr := s.createSnapStream(ctx); cerr != nil {
				lastErr = fmt.Errorf("create companion stream %q: %w", snapPath, cerr)
				if ctx.Err() != nil || !isRetryable(cerr) {
					return fmt.Errorf("durablestream: save snapshot %q: %w", snapshotID, lastErr)
				}
				continue
			}
			s.snap.created = true
			err = s.sendSnapshotRecord(ctx, data)
		}
		if err != nil {
			lastErr = err
			if ctx.Err() != nil || !isRetryable(err) {
				return fmt.Errorf("durablestream: save snapshot %q: %w", snapshotID, lastErr)
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("durablestream: save snapshot %q: giving up after %d attempts: %w", snapshotID, s.cfg.retryAttempts, lastErr)
}

// sendSnapshotRecord performs one append round trip, bounded by the
// per-attempt timeout.
func (s *Store) sendSnapshotRecord(ctx context.Context, data []byte) error {
	attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
	defer cancel()
	_, err := s.snapStream.Append(attemptCtx, data)
	return err
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

	offset := eventbus.OffsetOldest
	for {
		var result *durablestreams.Chunk
		var lastErr error
		done := false
		for attempt := 1; attempt <= s.cfg.retryAttempts; attempt++ {
			if attempt > 1 {
				if err := backoff(ctx, s.cfg.retryBaseDelay, attempt-1); err != nil {
					return eventbus.OffsetOldest, nil, fmt.Errorf("durablestream: load snapshot %q: %w", snapshotID, err)
				}
			}
			if err := ctx.Err(); err != nil {
				return eventbus.OffsetOldest, nil, fmt.Errorf("durablestream: load snapshot %q: %w", snapshotID, err)
			}

			// One iterator per attempt, consumed for one chunk, with its own
			// timeout window.
			attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
			it := s.snapStream.Read(attemptCtx, durablestreams.WithOffset(toWireOffset(offset)))
			res, err := it.Next()
			it.Close()
			cancel()
			if err != nil {
				if errors.Is(err, durablestreams.Done) {
					// Caught up: nothing after offset.
					done = true
					break
				}
				if errors.Is(err, durablestreams.ErrStreamNotFound) {
					// The companion stream is absent: never created, expired,
					// or deleted while we were paging through it. A record
					// already found on an earlier page is still the best
					// available snapshot; with nothing found, this is a miss.
					if found {
						return atOffset, blob, nil
					}
					return eventbus.OffsetOldest, nil, nil
				}
				if errors.Is(err, durablestreams.ErrOffsetGone) {
					// The companion stream's head was trimmed by server-side
					// retention. The protocol offers no way to discover the
					// earliest retained offset, so any records that survived
					// the trim — including ones NEWER than what earlier pages
					// yielded — are unreachable from here. Returning an
					// earlier-page record would silently serve a stale
					// snapshot while a newer one survives; fail loudly
					// instead, whether or not something was found.
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
		if done {
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
			if record.SnapshotID == "" {
				// SaveSnapshot never writes an empty id, so this is foreign
				// data — most likely an event envelope from a stream that
				// predates the constructor's ".snap" reservation or was
				// written by a raw protocol client. Skipping is correct
				// (LoadSnapshot must return the newest genuine record), but
				// silently skipping would hide the misconfiguration, so
				// report it like a malformed record.
				s.handleSnapshotDecodeError(i, raw, fmt.Errorf("record has no snapshot_id: foreign data on the companion stream"))
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
			// A nil blob marshals as the JSON literal null; normalize it back
			// so blob == nil discrimination behaves like the SQLite store's
			// (which stores and returns nil).
			if len(blob) == 0 || string(blob) == "null" {
				blob = nil
			}
		}

		next := eventbus.Offset(result.NextOffset)
		if result.UpToDate || next == offset {
			break
		}
		offset = next
	}

	if !found {
		return eventbus.OffsetOldest, nil, nil
	}
	return atOffset, blob, nil
}

// createSnapStream performs one idempotent Create of the companion stream,
// bounded by the per-attempt timeout. Retry policy belongs to the caller's
// attempt loop so creation and send share one attempt budget. Caller holds
// s.snap.mu.
func (s *Store) createSnapStream(ctx context.Context) error {
	attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
	defer cancel()
	return s.snapStream.Create(attemptCtx, durablestreams.WithContentType("application/json"))
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
