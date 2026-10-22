package durablestream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	durablestreams "github.com/durable-streams/durable-streams/packages/client-go"
	eventbus "github.com/jilio/ebu"
)

// Idempotent producer headers (Durable Streams protocol, section 5.2.1).
const (
	headerProducerID          = "Producer-Id"
	headerProducerEpoch       = "Producer-Epoch"
	headerProducerSeq         = "Producer-Seq"
	headerProducerExpectedSeq = "Producer-Expected-Seq"
	headerStreamNextOffset    = "Stream-Next-Offset"
	headerStreamClosed        = "Stream-Closed"
)

// maxProducerEpochOrSeq is the protocol's upper bound for Producer-Epoch and
// Producer-Seq (2^53-1, for JavaScript interoperability).
const maxProducerEpochOrSeq = 1<<53 - 1

// maxDrainedBody bounds how much of an append response body is read: the
// producer protocol carries everything in headers.
const maxDrainedBody = 64 << 10

// maxProducerCorrections bounds the protocol corrections (epoch claims and
// sequence renumbering) a single Append may make. One of each is all a
// correctly behaving server ever requires; the bound only stops a
// misbehaving or racing peer from spinning one Append forever.
const maxProducerCorrections = 16

var (
	// ErrProducerFenced reports that the server holds a newer epoch for this
	// store's producer ID: another instance has taken over the producer
	// (zombie fencing). The rejected request appended nothing; as with any
	// Append error, an earlier attempt of the same call whose acknowledgement
	// was lost may still have been committed. The condition is permanent for
	// the Store: every later Append fails with the same error without
	// contacting the server. Use errors.As with *ProducerFencedError for the
	// epochs involved.
	ErrProducerFenced = errors.New("durablestream: producer fenced by a newer epoch")

	// ErrProducerConflict reports that the server already holds the producer
	// sequence number this store was about to use, and the duplicate is not
	// this event: another writer is using the same producer ID and epoch (for
	// example a restarted instance that reused its predecessor's epoch), or
	// an abandoned request of this store arrived late. This event was not
	// appended: it is reported only when no attempt of this event can have
	// landed. Give every live writer its own epoch. A store that meets the
	// conflict before it owns its epoch never takes that epoch over: every
	// later Append fails with the same error (an auto-claim store claims a
	// fresh epoch instead of reaching this error).
	ErrProducerConflict = errors.New("durablestream: producer sequence already used by another writer")

	// ErrProducerIndeterminate reports an Append whose outcome cannot be
	// established: the server answered a retry with a duplicate that cannot
	// be attributed (the same sequence number may also have been used by
	// another writer, before this store owns its epoch, or by an earlier,
	// abandoned request of this store), or it asked for a different sequence
	// number while an earlier attempt of this event may still arrive (re-sending
	// the event under the new number could store it twice). The event may or
	// may not be stored; like any indeterminate Append error, retrying may
	// store it twice, so consumers deduplicate on Event.ID.
	ErrProducerIndeterminate = errors.New("durablestream: producer append outcome is indeterminate")
)

// ProducerFencedError details an ErrProducerFenced rejection.
type ProducerFencedError struct {
	// ProducerID is the producer ID this store writes as.
	ProducerID string
	// Epoch is the epoch this store wrote with.
	Epoch int64
	// CurrentEpoch is the newer epoch the server holds for ProducerID.
	CurrentEpoch int64
}

// Error implements error.
func (e *ProducerFencedError) Error() string {
	return fmt.Sprintf("durablestream: producer %q epoch %d fenced by epoch %d", e.ProducerID, e.Epoch, e.CurrentEpoch)
}

// Unwrap makes errors.Is(err, ErrProducerFenced) true.
func (e *ProducerFencedError) Unwrap() error { return ErrProducerFenced }

// WithProducer makes Append use the Durable Streams idempotent producer
// protocol (section 5.2.1): every append carries the producer ID, epoch and a
// per-epoch sequence number, so the server
//
//   - deduplicates a retried append: an attempt whose acknowledgement was lost
//     is recognized on retry and is not written twice, and
//   - fences stale writers: once any append with a higher epoch for the same
//     ID is accepted, appends with an older epoch fail with ErrProducerFenced.
//
// id must be non-empty and stable across restarts of the logical writer; it
// is sent as an HTTP header, so it may contain neither control characters
// nor leading or trailing spaces.
// epoch must be within [0, 2^53-1]. Each Store instance needs its own epoch,
// strictly greater than that of every earlier instance using the same ID — for
// example a lease generation or a durable counter incremented at startup — or
// use WithProducerAutoClaim to claim the next epoch from the server. Reusing a
// previous instance's epoch makes the first Append fail with
// ErrProducerConflict (or ErrProducerIndeterminate when an earlier attempt of
// it may have landed); it never reports success for an event the server
// discarded as a duplicate. Until the store owns its epoch, a deduplicated
// retry cannot be told apart from another writer's event under the same
// number and is reported as ErrProducerIndeterminate.
//
// Appends of a producer Store are serialized: concurrent Append calls wait for
// one another (honoring their contexts), because the sequence numbers must
// reach the server in order. When a retry is deduplicated the server reports
// no offset: Append reads from its previous acknowledged offset to recover
// the exact offset after the event. If that read fails, or another writer's
// events follow the event in the same read chunk (no offset then covers the
// event without skipping later ones), Append returns an error although the
// event is stored — like any indeterminate Append error, consumers still
// deduplicate on Event.ID. Ownership of the epoch comes only from an
// acknowledged sequence 0; a store whose first acknowledged number is later
// stops after that append (or, with auto-claim, claims a fresh epoch).
// Read, Tail and snapshots are unaffected; snapshots on the companion stream
// are not fenced. Without this option Append keeps its at-least-once behavior.
func WithProducer(id string, epoch int64) Option {
	return func(c *config) {
		c.producer = &producerConfig{id: id, epoch: epoch}
	}
}

// WithProducerAutoClaim lets a WithProducer store claim its epoch from the
// server instead of requiring a fresh one: while the store has not yet had an
// append accepted, a rejection because the server holds the configured epoch
// or a newer one makes the store retry the same append with the next free
// epoch (protocol section 5.2.1 "auto-claim"). The newest instance therefore
// wins and fences every older one.
//
// Claiming happens only before the store's first accepted append. A store that
// already owns its epoch never re-claims: a later stale-epoch rejection means a
// newer instance took over, and Append fails with ErrProducerFenced. Two live
// instances therefore cannot keep taking the producer from each other.
// ProducerEpoch reports the claimed epoch. New rejects this option without
// WithProducer.
func WithProducerAutoClaim() Option {
	return func(c *config) {
		c.producerAutoClaim = true
	}
}

// producerConfig is the WithProducer configuration.
type producerConfig struct {
	id    string
	epoch int64
}

// validateProducer checks the producer options before the store is created.
func validateProducer(cfg *config) error {
	if cfg.producer == nil {
		if cfg.producerAutoClaim {
			return errors.New("durablestream: WithProducerAutoClaim requires WithProducer")
		}
		return nil
	}
	if cfg.producer.id == "" {
		return errors.New("durablestream: producer ID is required")
	}
	if strings.ContainsFunc(cfg.producer.id, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		// HTTP header values cannot carry control characters; fail at New
		// rather than on every append.
		return fmt.Errorf("durablestream: producer ID %q contains control characters", cfg.producer.id)
	}
	if strings.Trim(cfg.producer.id, " ") != cfg.producer.id {
		// HTTP trims spaces around header values, so " w " would share
		// producer state with "w" (and " " would arrive empty).
		return fmt.Errorf("durablestream: producer ID %q must not start or end with spaces", cfg.producer.id)
	}
	if cfg.producer.epoch < 0 || cfg.producer.epoch > maxProducerEpochOrSeq {
		return fmt.Errorf("durablestream: producer epoch %d is outside [0, 2^53-1]", cfg.producer.epoch)
	}
	return nil
}

// producerState is the live sequence state of a producer Store. The turn
// token is held for an entire Append, including retries and backoff, so
// sequence numbers reach the server in order. Every field except id,
// autoClaim and epoch (an atomic, read by ProducerEpoch) is guarded by the
// token: only appendAsProducer, while holding it, may touch them. Waiting
// for the token honors the caller's context.
type producerState struct {
	turn chan struct{}
	id   string
	// epoch is written only under turn but read lock-free by ProducerEpoch, so
	// a status query never waits for an Append's retries.
	epoch     atomic.Int64
	nextSeq   int64
	autoClaim bool
	// owned is set by the first accepted append: the epoch is then this
	// store's, and auto-claim is over.
	owned bool
	// sentThrough is the highest sequence number this store has sent in the
	// current epoch (-1: none). A number at or below it may still reach the
	// server from an abandoned request, so a duplicate for it cannot be
	// attributed to the current Append.
	sentThrough int64
	// fenced is the sticky ErrProducerFenced once a newer epoch was seen.
	fenced error
	// conflict is the sticky error of a store that must never take over its
	// epoch (without auto-claim): it found another writer's event under it
	// before owning it, or could not establish that the epoch's first
	// numbers were its own.
	conflict error
	// lastAck is the exact offset after this store's latest acknowledged
	// append (OffsetOldest before the first): a lower bound for any later
	// event of the store, from which its exact offset can be recovered.
	lastAck eventbus.Offset
	// claimNext makes the next Append claim a fresh epoch first: an
	// auto-claim store found its epoch in use but could not claim at once.
	claimNext bool
}

func newProducerState(cfg *config) *producerState {
	if cfg.producer == nil {
		return nil
	}
	p := &producerState{turn: make(chan struct{}, 1), id: cfg.producer.id, autoClaim: cfg.producerAutoClaim, sentThrough: -1}
	p.epoch.Store(cfg.producer.epoch)
	return p
}

// canClaimAfter reports whether an epoch above epoch exists to claim.
func (p *producerState) canClaimAfter(epoch int64) error {
	if epoch >= maxProducerEpochOrSeq {
		return fmt.Errorf("durablestream: append: producer %q cannot claim an epoch beyond %d", p.id, epoch)
	}
	return nil
}

// ProducerEpoch returns the epoch Append writes with — after auto-claim, the
// claimed epoch — and false when the store was created without WithProducer.
func (s *Store) ProducerEpoch() (int64, bool) {
	if s.producer == nil {
		return 0, false
	}
	return s.producer.epoch.Load(), true
}

// producerResponse is the part of an append response the producer protocol
// needs.
type producerResponse struct {
	status      int
	nextOffset  string
	epoch       string
	seq         string // Producer-Seq: on success, the highest accepted sequence
	expectedSeq string
	closed      bool
}

// appendAsProducer appends data under the idempotent producer protocol.
//
// Sequence numbers: each call takes the next number; retries of the call reuse
// it with the same body. A call that gives up leaves its number unused or
// committed; if it never landed, the next call's number runs past what the
// server expects and the server's 409 names the expected number (or, before
// the epoch is established, a 400 rejects any number but 0). Those answers
// mean nothing was appended, so the current event is renumbered.
//
// A duplicate answer (204) proves this Append's event was stored only when an
// earlier attempt of it may have landed, the store owns its epoch (no other
// instance writes it), and the number was never sent by an earlier request
// that might still arrive. A duplicate with no possibly landed attempt is
// another writer's event; any other duplicate is reported as
// ErrProducerIndeterminate, never as success (even a higher Producer-Seq
// does not prove rejection: the attempt may have landed before another
// writer's later numbers).
func (s *Store) appendAsProducer(ctx context.Context, eventID string, data []byte) (eventbus.Offset, error) {
	p := s.producer
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("durablestream: append: %w", err)
	}
	select {
	case p.turn <- struct{}{}:
	case <-ctx.Done():
		return "", fmt.Errorf("durablestream: append: waiting for the producer: %w", ctx.Err())
	}
	defer func() { <-p.turn }()
	if p.fenced != nil {
		return "", p.fenced
	}
	if p.conflict != nil {
		return "", p.conflict
	}
	if p.claimNext {
		epoch := p.epoch.Load()
		if err := p.canClaimAfter(epoch); err != nil {
			return "", err
		}
		p.claimNext = false
		p.epoch.Store(epoch + 1)
		p.nextSeq, p.sentThrough = 0, -1
	}

	// Taken even if this call later fails without sending (a non-retryable
	// status, a cancelled context): the next call's number then runs ahead,
	// and the server's 409 or 400 renumbers it, at the cost of one round trip.
	seq := p.nextSeq
	p.nextSeq++
	// sentBefore is the highest number earlier calls sent in this epoch; only
	// numbers above it are attributable to this call.
	sentBefore := p.sentThrough
	// mayHaveLanded records that an earlier attempt of this exact
	// (epoch, seq, body) may have been committed although no acknowledgement
	// arrived. anyMayLand records the same for any number or epoch this event
	// was sent under: such a request can still arrive later, so the event is
	// never re-sent under another number or epoch (it would be stored twice).
	mayHaveLanded, anyMayLand := false, false
	corrections := 0
	correct := func() error {
		if corrections++; corrections > maxProducerCorrections {
			return fmt.Errorf("durablestream: append: producer %q gave up after %d epoch or sequence corrections", p.id, maxProducerCorrections)
		}
		return nil
	}
	// conflicted reports another writer's event under this store's epoch.
	// Before the store owns its epoch that epoch is someone else's, so the
	// store stops for good. (An auto-claim store never gets here unowned: it
	// claims instead, or reports an indeterminate duplicate and claims next.)
	conflicted := func(err error) error {
		if !p.owned {
			p.conflict = err
		}
		return err
	}
	indeterminate := func(epoch int64, why string) error {
		return fmt.Errorf("durablestream: append: producer %q epoch %d sequence %d: %s: %w", p.id, epoch, seq, why, ErrProducerIndeterminate)
	}
	// claim moves to a new epoch whose sequence starts at 0.
	claim := func(epoch int64) {
		p.epoch.Store(epoch)
		seq, p.nextSeq = 0, 1
		p.sentThrough, sentBefore = -1, -1
		mayHaveLanded = false
	}
	var lastErr error
	for attempt := 1; attempt <= s.cfg.retryAttempts; {
		if attempt > 1 {
			if err := backoff(ctx, s.cfg.retryBaseDelay, attempt-1); err != nil {
				return "", fmt.Errorf("durablestream: append: %w", err)
			}
		}
		// A context that ends now fails the request below without sending
		// it, and the error path returns because ctx.Err() is set.
		epoch := p.epoch.Load()
		p.sentThrough = max(p.sentThrough, seq)
		resp, err := s.postAsProducer(ctx, data, epoch, seq)
		if err == nil && resp.status == http.StatusBadRequest && !p.owned && seq > 0 {
			// A new epoch must start at sequence 0 (protocol section 5.2.1):
			// an earlier call that gave up never established it. Nothing was
			// appended; start the epoch with this event — unless an earlier
			// attempt of it may still arrive, which would then be accepted
			// as sequence 1 and store the event twice.
			if anyMayLand {
				return "", indeterminate(epoch, "the epoch is not established, and an earlier attempt may still arrive")
			}
			if err := correct(); err != nil {
				return "", err
			}
			seq, p.nextSeq = 0, 1
			mayHaveLanded = false
			continue
		}
		if err == nil {
			err = producerStatusError(s.streamURL, resp)
		}
		if err != nil {
			lastErr = fmt.Errorf("durablestream: append: %w", err)
			if ctx.Err() != nil || !isRetryable(err) {
				return "", lastErr
			}
			mayHaveLanded, anyMayLand = true, true
			attempt++
			continue
		}

		switch resp.status {
		case http.StatusOK:
			if !p.owned {
				if seq == 0 {
					// Only an acknowledged sequence 0 makes the epoch this
					// store's: nobody else wrote in it before.
					p.owned = true
				} else {
					// The epoch already held numbers this store never had
					// acknowledged (an earlier append's outcome was lost):
					// they may be another writer's. This event is written,
					// but the store must not keep writing in this epoch.
					p.conflict = fmt.Errorf("durablestream: append: producer %q cannot establish ownership of epoch %d (its first sequence numbers may be another writer's); open a new Store with a newer epoch: %w",
						p.id, epoch, ErrProducerIndeterminate)
					if p.autoClaim {
						p.conflict, p.claimNext = nil, true
					}
				}
			}
			if resp.nextOffset != "" {
				p.lastAck = eventbus.Offset(resp.nextOffset)
				return p.lastAck, nil
			}
			return s.producerAckOffset(ctx, p, eventID, epoch, seq)

		case http.StatusNoContent:
			highest, parseErr := strconv.ParseInt(resp.seq, 10, 64)
			known := parseErr == nil
			switch {
			case mayHaveLanded && p.owned && seq > sentBefore && (!known || highest >= seq):
				// Only this store writes its epoch and nothing else ever
				// carried this number: an earlier attempt landed and only its
				// acknowledgement was lost. Written once.
				return s.producerAckOffset(ctx, p, eventID, epoch, seq)
			case mayHaveLanded:
				// An earlier attempt may have landed, but the duplicate is
				// not attributable to it: a higher server sequence does not
				// prove rejection either (the attempt may have landed before
				// another writer's later numbers).
				if !p.owned && p.autoClaim {
					// The epoch may be another writer's: take a fresh one
					// for the next event rather than ever owning this one.
					p.claimNext = true
				}
				return "", indeterminate(epoch, "duplicate after a lost acknowledgement")
			case p.autoClaim && !p.owned && !anyMayLand:
				// The configured epoch is already in use: claim the next.
				if err := correct(); err != nil {
					return "", err
				}
				if err := p.canClaimAfter(epoch); err != nil {
					return "", err
				}
				claim(epoch + 1)
				continue
			default:
				// The server holds another writer's event under this
				// number (its highest accepted number differs, or no attempt
				// of this event can have landed).
				return "", conflicted(fmt.Errorf("durablestream: append: producer %q epoch %d sequence %d (server holds through %q): %w",
					p.id, epoch, seq, resp.seq, ErrProducerConflict))
			}

		case http.StatusForbidden:
			current, parseErr := strconv.ParseInt(resp.epoch, 10, 64)
			if parseErr != nil || current <= epoch {
				// Not a fencing answer: the protocol fences only with a newer
				// epoch (a 403 from an authorizing proxy carries none).
				// Report it without fencing the store.
				return "", &durablestreams.StreamError{Op: "append", URL: s.streamURL, StatusCode: http.StatusForbidden,
					Err: fmt.Errorf("403 without a newer %s (got %q): not a producer fencing response", headerProducerEpoch, resp.epoch)}
			}
			// Claim only while no attempt of this event can have landed:
			// re-sending it under a new epoch could otherwise write it twice.
			if p.autoClaim && !p.owned && !anyMayLand && current < maxProducerEpochOrSeq {
				if err := correct(); err != nil {
					return "", err
				}
				claim(current + 1)
				continue
			}
			p.fenced = &ProducerFencedError{ProducerID: p.id, Epoch: epoch, CurrentEpoch: current}
			return "", p.fenced

		default: // http.StatusConflict with Producer-Expected-Seq (see producerStatusError)
			expected, parseErr := strconv.ParseInt(resp.expectedSeq, 10, 64)
			if parseErr != nil || expected < 0 || expected >= seq {
				return "", fmt.Errorf("durablestream: append: producer %q epoch %d sequence %d: server expects sequence %q: %w",
					p.id, epoch, seq, resp.expectedSeq, durablestreams.ErrSeqConflict)
			}
			if anyMayLand {
				// Renumbering would let a delayed earlier attempt store the
				// event a second time under its old number.
				return "", indeterminate(epoch, fmt.Sprintf("the server expects sequence %d, and an earlier attempt may still arrive", expected))
			}
			if err := correct(); err != nil {
				return "", err
			}
			// Nothing was appended, and the server has not committed
			// sequence numbers >= expected in this epoch: earlier calls
			// that gave up never landed. Reuse the expected number.
			// sentBefore stays as it was: the expected number was carried by
			// an earlier request, so a duplicate for it after a lost
			// acknowledgement is reported as indeterminate, not success.
			seq, p.nextSeq = expected, expected+1
			mayHaveLanded = false
		}
	}
	return "", fmt.Errorf("durablestream: append: giving up after %d attempts: %w", s.cfg.retryAttempts, lastErr)
}

// producerAckOffset returns the exact offset after an event the server
// acknowledged without one (a deduplicated retry, or a server that omitted
// Stream-Next-Offset). It reads from the store's previous acknowledged offset
// and finds the event by its ID. The offset after the event is exact only
// when the event ends a read chunk; when another writer's events follow it in
// the same chunk, no offset satisfies the Append contract (it must cover the
// event and skip nothing later), so the outcome is reported as
// ErrProducerIndeterminate although the event is stored.
func (s *Store) producerAckOffset(ctx context.Context, p *producerState, eventID string, epoch, seq int64) (eventbus.Offset, error) {
	offset, exact, err := s.locate(ctx, p.lastAck, eventID)
	switch {
	case err != nil:
		return "", fmt.Errorf("durablestream: append committed, but its offset is unknown: %w", err)
	case !exact:
		return "", fmt.Errorf("durablestream: append: producer %q epoch %d sequence %d: the event is stored, but no exact offset follows it: %w",
			p.id, epoch, seq, ErrProducerIndeterminate)
	}
	p.lastAck = offset
	return offset, nil
}

// locate scans the stream from from, which must not lie after the event, for
// the last record carrying eventID. It returns the offset after that record
// and whether that offset is exact (the record ended its chunk).
func (s *Store) locate(ctx context.Context, from eventbus.Offset, eventID string) (offset eventbus.Offset, exact bool, err error) {
	if eventID == "" {
		return "", false, nil
	}
	// The event was committed before the duplicate answer, so it lies before
	// the tail captured now. Scanning stops there: other writers extending
	// the stream cannot keep the scan (and the producer) busy forever.
	tail, err := s.resolveTail(ctx)
	if err != nil {
		return "", false, err
	}
	matches := 0
	// complete records that the scan reached the captured tail; a scan that
	// stops earlier (a premature caught-up answer, a chunk that does not
	// advance) may have missed a later copy of the ID, so it proves nothing.
	complete := from >= tail
	for at := from; at < tail; {
		chunk, _, err := s.readChunk(ctx, at)
		if err != nil {
			return "", false, err
		}
		if chunk == nil {
			break
		}
		next := eventbus.Offset(chunk.NextOffset)
		events, _, err := s.decodeChunk(chunk.Data, next, at)
		if err != nil {
			return "", false, err
		}
		for _, event := range events {
			if event.ID == eventID {
				// The record that ends a chunk carries the chunk's next
				// offset; any other carries the chunk start (the same value
				// when a chunk fails to advance, so that is never exact).
				matches++
				offset, exact = next, event.Offset == next && next != at
			}
		}
		if next == at {
			break
		}
		at = next
		complete = at >= tail
	}
	// An ID identifies a publish, not a physical record: with more than one
	// copy (an earlier, retried publish of the same event) the appended
	// record cannot be told apart, and either offset could skip events.
	return offset, complete && matches == 1 && exact, nil
}

// postAsProducer sends one append attempt with producer headers. Transport
// failures are returned as errors; every HTTP status is returned as a
// response for producerStatusError to classify.
func (s *Store) postAsProducer(ctx context.Context, data []byte, epoch, seq int64) (producerResponse, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, s.streamURL, bytes.NewReader(data))
	if err != nil {
		return producerResponse{}, &durablestreams.StreamError{Op: "append", URL: s.streamURL, Err: err}
	}
	req.Header.Set("Content-Type", s.cfg.contentType)
	req.Header.Set(headerProducerID, s.producer.id)
	req.Header.Set(headerProducerEpoch, strconv.FormatInt(epoch, 10))
	req.Header.Set(headerProducerSeq, strconv.FormatInt(seq, 10))

	resp, err := s.cfg.httpClient.Do(req)
	if err != nil {
		return producerResponse{}, &durablestreams.StreamError{Op: "append", URL: s.streamURL, Err: err}
	}
	defer resp.Body.Close()
	// Drain a bounded amount so the connection can be reused; a server or
	// proxy sending a large error body cannot make each attempt read it all.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainedBody))
	return producerResponse{
		status:      resp.StatusCode,
		nextOffset:  resp.Header.Get(headerStreamNextOffset),
		epoch:       resp.Header.Get(headerProducerEpoch),
		seq:         resp.Header.Get(headerProducerSeq),
		expectedSeq: resp.Header.Get(headerProducerExpectedSeq),
		closed:      resp.Header.Get(headerStreamClosed) == "true",
	}, nil
}

// producerStatusError maps a response that is not part of the producer
// protocol's success/fencing/sequence vocabulary to the client's error
// sentinels, so isRetryable classifies it like a plain append failure.
func producerStatusError(url string, resp producerResponse) error {
	var sentinel error
	switch resp.status {
	case http.StatusOK, http.StatusNoContent, http.StatusForbidden:
		return nil
	case http.StatusConflict:
		switch {
		case resp.closed:
			sentinel = durablestreams.ErrStreamClosed
		case resp.expectedSeq != "":
			return nil
		default:
			sentinel = durablestreams.ErrContentTypeMismatch
		}
	case http.StatusBadRequest:
		sentinel = durablestreams.ErrBadRequest
	case http.StatusNotFound:
		sentinel = durablestreams.ErrStreamNotFound
	case http.StatusGone:
		sentinel = durablestreams.ErrOffsetGone
	case http.StatusTooManyRequests:
		sentinel = durablestreams.ErrRateLimited
	default:
		sentinel = fmt.Errorf("unexpected status code: %d", resp.status)
	}
	return &durablestreams.StreamError{Op: "append", URL: url, StatusCode: resp.status, Err: sentinel}
}
