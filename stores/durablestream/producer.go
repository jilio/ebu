package durablestream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
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
	// sequence number this store was about to use, although this store never
	// sent that event under it: another writer is using the same producer ID
	// and epoch (or an abandoned request of this store arrived late). The event
	// was not appended. Give every concurrently live writer its own epoch.
	ErrProducerConflict = errors.New("durablestream: producer sequence already used by another writer")
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
// id must be non-empty and stable across restarts of the logical writer.
// epoch must be within [0, 2^53-1]. Each Store instance needs its own epoch,
// strictly greater than that of every earlier instance using the same ID — for
// example a lease generation or a durable counter incremented at startup — or
// use WithProducerAutoClaim to claim the next epoch from the server. Reusing a
// previous instance's epoch makes the first Append fail with
// ErrProducerConflict instead of being silently discarded as a duplicate.
//
// Appends of a producer Store are serialized: concurrent Append calls wait for
// one another, because the sequence numbers must reach the server in order.
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
	if cfg.producer.epoch < 0 || cfg.producer.epoch > maxProducerEpochOrSeq {
		return fmt.Errorf("durablestream: producer epoch %d is outside [0, 2^53-1]", cfg.producer.epoch)
	}
	return nil
}

// producerState is the live sequence state of a producer Store. mu is held
// for an entire Append, including retries and backoff, so sequence numbers
// reach the server in order.
type producerState struct {
	mu sync.Mutex
	id string
	// epoch is written only under mu but read lock-free by ProducerEpoch, so
	// a status query never waits for an Append's retries.
	epoch     atomic.Int64
	nextSeq   int64
	autoClaim bool
	// owned is set by the first accepted append: the epoch is then this
	// store's, and auto-claim is over.
	owned bool
	// fenced is the sticky ErrProducerFenced once a newer epoch was seen.
	fenced error
}

func newProducerState(cfg *config) *producerState {
	if cfg.producer == nil {
		return nil
	}
	p := &producerState{id: cfg.producer.id, autoClaim: cfg.producerAutoClaim}
	p.epoch.Store(cfg.producer.epoch)
	return p
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
	expectedSeq string
	closed      bool
}

// appendAsProducer appends data under the idempotent producer protocol.
//
// Sequence numbers: each call takes the next number; retries of the call reuse
// it with the same body, so a duplicate (204) after a failed attempt proves the
// event was committed by that attempt. A call that gives up leaves its number
// unused-or-committed; the next call's number then runs one past what the server
// expects if the abandoned attempt never landed, and the server's 409 names the
// expected number. A 409 means nothing was appended, so renumbering the current
// event down to the expected number cannot duplicate or lose anything.
func (s *Store) appendAsProducer(ctx context.Context, data []byte) (eventbus.Offset, error) {
	p := s.producer
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fenced != nil {
		return "", p.fenced
	}

	seq := p.nextSeq
	p.nextSeq++
	// mayHaveLanded records that an earlier attempt of this exact
	// (epoch, seq, body) may have been committed although no acknowledgement
	// arrived. Only then is a duplicate response this store's own event.
	mayHaveLanded := false
	corrections := 0
	var lastErr error
	for attempt := 1; attempt <= s.cfg.retryAttempts; {
		if attempt > 1 {
			if err := backoff(ctx, s.cfg.retryBaseDelay, attempt-1); err != nil {
				return "", fmt.Errorf("durablestream: append: %w", err)
			}
		}
		if err := ctx.Err(); err != nil {
			return "", fmt.Errorf("durablestream: append: %w", err)
		}

		epoch := p.epoch.Load()
		resp, err := s.postAsProducer(ctx, data, epoch, seq)
		if err == nil {
			err = producerStatusError(s.streamURL, resp)
		}
		if err != nil {
			lastErr = fmt.Errorf("durablestream: append: %w", err)
			if ctx.Err() != nil || !isRetryable(err) {
				return "", lastErr
			}
			mayHaveLanded = true
			attempt++
			continue
		}

		switch resp.status {
		case http.StatusOK:
			p.owned = true
			if resp.nextOffset != "" {
				return eventbus.Offset(resp.nextOffset), nil
			}
			return s.producerAckOffset(ctx)

		case http.StatusNoContent:
			if mayHaveLanded {
				// An earlier attempt was committed and only its
				// acknowledgement was lost: success, written once.
				p.owned = true
				return s.producerAckOffset(ctx)
			}
			if p.autoClaim && !p.owned {
				// The configured epoch is already in use: claim the next.
				if corrections++; corrections > maxProducerCorrections {
					return "", producerCorrectionsError(p)
				}
				if epoch >= maxProducerEpochOrSeq {
					return "", fmt.Errorf("durablestream: append: producer %q cannot claim an epoch beyond %d", p.id, epoch)
				}
				p.epoch.Store(epoch + 1)
				seq, p.nextSeq = 0, 1
				continue
			}
			return "", fmt.Errorf("durablestream: append: producer %q epoch %d sequence %d: %w", p.id, epoch, seq, ErrProducerConflict)

		case http.StatusForbidden:
			current, parseErr := strconv.ParseInt(resp.epoch, 10, 64)
			if parseErr != nil || current < epoch {
				return "", fmt.Errorf("durablestream: append: stale-epoch response carries invalid %s %q", headerProducerEpoch, resp.epoch)
			}
			// Claim only while no attempt of this event can have landed:
			// re-sending it under a new epoch could otherwise write it twice.
			if p.autoClaim && !p.owned && !mayHaveLanded && current < maxProducerEpochOrSeq {
				if corrections++; corrections > maxProducerCorrections {
					return "", producerCorrectionsError(p)
				}
				p.epoch.Store(current + 1)
				seq, p.nextSeq = 0, 1
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
			if corrections++; corrections > maxProducerCorrections {
				return "", producerCorrectionsError(p)
			}
			// Nothing was appended, and the server has not committed
			// sequence numbers >= expected in this epoch: earlier calls
			// that gave up never landed. Reuse the expected number.
			seq, p.nextSeq = expected, expected+1
			mayHaveLanded = false
		}
	}
	return "", fmt.Errorf("durablestream: append: giving up after %d attempts: %w", s.cfg.retryAttempts, lastErr)
}

func producerCorrectionsError(p *producerState) error {
	return fmt.Errorf("durablestream: append: producer %q gave up after %d epoch or sequence corrections", p.id, maxProducerCorrections)
}

// producerAckOffset returns the offset for an append the server acknowledged
// without a resume offset (a deduplicated retry). The stream tail covers the
// committed event, so it is a valid replication barrier; it may also cover
// events other writers appended since.
func (s *Store) producerAckOffset(ctx context.Context) (eventbus.Offset, error) {
	tail, err := s.resolveTail(ctx)
	if err != nil {
		return "", fmt.Errorf("durablestream: append committed, but its offset is unknown: %w", err)
	}
	return tail, nil
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
	_, _ = io.Copy(io.Discard, resp.Body)
	return producerResponse{
		status:      resp.StatusCode,
		nextOffset:  resp.Header.Get(headerStreamNextOffset),
		epoch:       resp.Header.Get(headerProducerEpoch),
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
