package durablestream_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	durablestreams "github.com/durable-streams/durable-streams/packages/client-go"
	eventbus "github.com/jilio/ebu"
	ds "github.com/jilio/ebu/stores/durablestream"
)

func producerEvent(n int) *eventbus.Event {
	return &eventbus.Event{ID: "event-" + strconv.Itoa(n), Type: "produced", Data: json.RawMessage(strconv.Itoa(n))}
}

// storedMessages returns the raw messages the server holds for a stream.
func (ps *protoServer) storedMessages(path string) []string {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	st := ps.streams[path]
	if st == nil {
		return nil
	}
	out := make([]string, len(st.messages))
	for i, m := range st.messages {
		out[i] = string(m)
	}
	return out
}

// producerOf returns the server's producer state for a stream.
func (ps *protoServer) producerOf(path, id string) (epoch, lastSeq int64, ok bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	st := ps.streams[path]
	if st == nil || st.producers[id] == nil {
		return 0, 0, false
	}
	return st.producers[id].epoch, st.producers[id].lastSeq, true
}

// recordPosts records the producer headers of every POST the server receives.
func (ps *protoServer) recordPosts() *postLog {
	log := &postLog{}
	ps.reject = func(r *http.Request) int {
		log.mu.Lock()
		defer log.mu.Unlock()
		log.headers = append(log.headers, [3]string{r.Header.Get("Producer-Id"), r.Header.Get("Producer-Epoch"), r.Header.Get("Producer-Seq")})
		return 0
	}
	return log
}

type postLog struct {
	mu      sync.Mutex
	headers [][3]string
}

func (l *postLog) snapshot() [][3]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([][3]string(nil), l.headers...)
}

func newProducerStore(t *testing.T, ps *protoServer, path string, opts ...ds.Option) *ds.Store {
	t.Helper()
	store, err := ds.New(ps.URL+"/v1/stream", path, append([]ds.Option{ds.WithRetry(3, time.Millisecond)}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

func TestProducer_AppendsCarryProducerHeaders(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	log := ps.recordPosts()
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer-a", 4))

	var offsets []eventbus.Offset
	for i := range 3 {
		offset, err := store.Append(t.Context(), producerEvent(i))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		offsets = append(offsets, offset)
	}
	want := [][3]string{{"writer-a", "4", "0"}, {"writer-a", "4", "1"}, {"writer-a", "4", "2"}}
	if got := log.snapshot(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("producer headers = %v, want %v", got, want)
	}
	if offsets[0] != eventbus.Offset(padOffset(1)) || offsets[2] != eventbus.Offset(padOffset(3)) {
		t.Fatalf("offsets = %v", offsets)
	}
	if epoch, ok := store.ProducerEpoch(); !ok || epoch != 4 {
		t.Fatalf("ProducerEpoch = %d, %v", epoch, ok)
	}
	if events := readAllFrom(t, store, eventbus.OffsetOldest); len(events) != 3 || events[1].ID != "event-1" {
		t.Fatalf("events = %v", events)
	}
}

func TestProducer_PlainStoreSendsNoProducerHeaders(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	log := ps.recordPosts()
	store := newProducerStore(t, ps, "events")
	if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}
	if got := log.snapshot(); len(got) != 1 || got[0] != [3]string{} {
		t.Fatalf("plain append sent producer headers: %v", got)
	}
	if _, ok := store.ProducerEpoch(); ok {
		t.Fatal("ProducerEpoch reported a producer for a plain store")
	}
}

func TestProducer_RetryAfterLostAckIsWrittenOnce(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
	if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}

	var lost atomic.Bool
	ps.loseAck = func(*http.Request) bool { return lost.CompareAndSwap(false, true) }
	offset, err := store.Append(t.Context(), producerEvent(1))
	if err != nil {
		t.Fatalf("retried append: %v", err)
	}
	if !lost.Load() {
		t.Fatal("the acknowledgement was never dropped")
	}
	if got := ps.storedMessages("events"); len(got) != 2 {
		t.Fatalf("retried append wrote %d messages, want 2: %v", len(got), got)
	}
	// The deduplicated retry carries no offset; the tail covers the event.
	if offset != eventbus.Offset(padOffset(2)) {
		t.Fatalf("offset = %q, want the tail %q", offset, padOffset(2))
	}
	if _, lastSeq, _ := ps.producerOf("events", "writer"); lastSeq != 1 {
		t.Fatalf("server lastSeq = %d", lastSeq)
	}
	// The sequence continues after the deduplicated append.
	if _, err := store.Append(t.Context(), producerEvent(2)); err != nil {
		t.Fatal(err)
	}
	if got := ps.storedMessages("events"); len(got) != 3 {
		t.Fatalf("messages = %v", got)
	}
}

func TestProducer_DuplicateTailLookupFailureIsReported(t *testing.T) {
	var posts atomic.Int32
	ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch posts.Add(1) {
		case 1:
			return false // accepted: the store now owns its epoch
		case 2:
			w.WriteHeader(http.StatusServiceUnavailable) // may have landed
		default:
			w.Header().Set("Producer-Seq", r.Header.Get("Producer-Seq"))
			w.WriteHeader(http.StatusNoContent) // ...and it had
		}
		return true
	})
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
	if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}
	inner := ps.Config.Handler
	var headFails atomic.Bool
	ps.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && headFails.Load():
			http.Error(w, "not found", http.StatusNotFound)
		case r.Method == http.MethodHead:
			w.Header().Set("Stream-Next-Offset", padOffset(9)) // the event lies before this tail
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			http.Error(w, "not found", http.StatusNotFound)
		default:
			inner.ServeHTTP(w, r)
		}
	})
	_, err := store.Append(t.Context(), producerEvent(1))
	if err == nil || !errors.Is(err, durablestreams.ErrStreamNotFound) || !strings.Contains(err.Error(), "append committed, but its offset is unknown") {
		t.Fatalf("expected a committed-but-unknown-offset error, got %v", err)
	}
	// The tail lookup that bounds the scan can fail too.
	headFails.Store(true)
	posts.Store(1) // the next append: 503, then the duplicate
	if _, err := store.Append(t.Context(), producerEvent(2)); err == nil || !strings.Contains(err.Error(), "append committed, but its offset is unknown") {
		t.Fatalf("tail lookup failure = %v", err)
	}
}

func TestProducer_MissingOffsetFallsBackToTail(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	ps.omitProducerOffset = true
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
	offset, err := store.Append(t.Context(), producerEvent(0))
	if err != nil || offset != eventbus.Offset(padOffset(1)) {
		t.Fatalf("Append = %q, %v", offset, err)
	}
}

func TestProducer_StaleEpochIsFencedPermanently(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	old := newProducerStore(t, ps, "events", ds.WithProducer("writer", 1))
	if _, err := old.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}
	successor := newProducerStore(t, ps, "events", ds.WithProducer("writer", 2))
	if _, err := successor.Append(t.Context(), producerEvent(1)); err != nil {
		t.Fatal(err)
	}

	log := ps.recordPosts()
	_, err := old.Append(t.Context(), producerEvent(2))
	var fenced *ds.ProducerFencedError
	if !errors.Is(err, ds.ErrProducerFenced) || !errors.As(err, &fenced) {
		t.Fatalf("expected ErrProducerFenced, got %v", err)
	}
	if fenced.ProducerID != "writer" || fenced.Epoch != 1 || fenced.CurrentEpoch != 2 ||
		!strings.Contains(fenced.Error(), `producer "writer" epoch 1 fenced by epoch 2`) {
		t.Fatalf("fenced error = %+v (%v)", fenced, fenced)
	}
	if _, err := old.Append(t.Context(), producerEvent(3)); !errors.Is(err, ds.ErrProducerFenced) {
		t.Fatalf("fencing must be sticky, got %v", err)
	}
	if got := log.snapshot(); len(got) != 1 {
		t.Fatalf("a fenced store contacted the server again: %v", got)
	}
	if got := ps.storedMessages("events"); len(got) != 2 {
		t.Fatalf("the fenced writer appended: %v", got)
	}
}

func TestProducer_ReusedEpochIsAConflictNotASilentDrop(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	first := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
	for i := range 2 {
		if _, err := first.Append(t.Context(), producerEvent(i)); err != nil {
			t.Fatal(err)
		}
	}
	// A restart that forgot to advance the epoch starts again at sequence 0.
	restarted := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
	if _, err := restarted.Append(t.Context(), producerEvent(9)); !errors.Is(err, ds.ErrProducerConflict) {
		t.Fatalf("expected ErrProducerConflict, got %v", err)
	}
	if got := ps.storedMessages("events"); len(got) != 2 {
		t.Fatalf("messages = %v", got)
	}
}

func TestProducer_AutoClaimTakesOverAndFencesOlderInstance(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	first := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0), ds.WithProducerAutoClaim())
	if _, err := first.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}
	if epoch, _ := first.ProducerEpoch(); epoch != 0 {
		t.Fatalf("first instance claimed epoch %d, want 0", epoch)
	}

	// Same configured epoch: the server holds epoch 0 already (duplicate).
	second := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0), ds.WithProducerAutoClaim())
	if _, err := second.Append(t.Context(), producerEvent(1)); err != nil {
		t.Fatal(err)
	}
	if epoch, _ := second.ProducerEpoch(); epoch != 1 {
		t.Fatalf("second instance claimed epoch %d, want 1", epoch)
	}
	if _, err := first.Append(t.Context(), producerEvent(2)); !errors.Is(err, ds.ErrProducerFenced) {
		t.Fatalf("older instance must be fenced, got %v", err)
	}

	// Older configured epoch: the server answers 403 and names its epoch.
	third := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0), ds.WithProducerAutoClaim())
	if _, err := third.Append(t.Context(), producerEvent(3)); err != nil {
		t.Fatal(err)
	}
	if epoch, _ := third.ProducerEpoch(); epoch != 2 {
		t.Fatalf("third instance claimed epoch %d, want 2", epoch)
	}
	if got := ps.storedMessages("events"); len(got) != 3 {
		t.Fatalf("messages = %v", got)
	}

	// An owner never re-claims: once superseded it stays fenced.
	if _, err := second.Append(t.Context(), producerEvent(4)); !errors.Is(err, ds.ErrProducerFenced) {
		t.Fatalf("an owning instance must not re-claim, got %v", err)
	}
}

func TestProducer_RenumbersAfterAnAbandonedSequence(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0), ds.WithRetry(1, time.Millisecond))

	// Sequence 0 never reaches the log: the caller gets an error.
	var failures atomic.Int32
	failures.Store(1)
	ps.reject = func(*http.Request) int {
		if failures.Add(-1) >= 0 {
			return http.StatusServiceUnavailable
		}
		return 0
	}
	if _, err := store.Append(t.Context(), producerEvent(0)); err == nil {
		t.Fatal("expected the injected failure")
	}
	// The next event is offered as sequence 1, the server expects 0, and the
	// store renumbers it: written once, nothing lost or duplicated.
	if _, err := store.Append(t.Context(), producerEvent(1)); err != nil {
		t.Fatalf("Append after an abandoned sequence: %v", err)
	}
	if got := ps.storedMessages("events"); len(got) != 1 || !strings.Contains(got[0], "event-1") {
		t.Fatalf("messages = %v", got)
	}
	if _, lastSeq, _ := ps.producerOf("events", "writer"); lastSeq != 0 {
		t.Fatalf("lastSeq = %d", lastSeq)
	}

	// An abandoned sequence that did land is simply skipped over.
	ps.reject = nil
	var lost atomic.Bool
	ps.loseAck = func(*http.Request) bool { return lost.CompareAndSwap(false, true) }
	if _, err := store.Append(t.Context(), producerEvent(2)); err == nil {
		t.Fatal("expected the lost acknowledgement to fail the single attempt")
	}
	if _, err := store.Append(t.Context(), producerEvent(3)); err != nil {
		t.Fatal(err)
	}
	if got := ps.storedMessages("events"); len(got) != 3 {
		t.Fatalf("messages = %v", got)
	}
}

func TestProducer_ConcurrentAppendsAreSerialized(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
	const writers, each = 8, 10
	var wg sync.WaitGroup
	errs := make(chan error, writers*each)
	for w := range writers {
		wg.Go(func() {
			for i := range each {
				if _, err := store.Append(context.Background(), producerEvent(w*each+i)); err != nil {
					errs <- err
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := ps.storedMessages("events"); len(got) != writers*each {
		t.Fatalf("stored %d messages, want %d", len(got), writers*each)
	}
	if _, lastSeq, _ := ps.producerOf("events", "writer"); lastSeq != writers*each-1 {
		t.Fatalf("lastSeq = %d", lastSeq)
	}
}

func TestProducer_OptionValidation(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	for _, tc := range []struct {
		name string
		opts []ds.Option
		want string
	}{
		{"empty id", []ds.Option{ds.WithProducer("", 0)}, "producer ID is required"},
		{"negative epoch", []ds.Option{ds.WithProducer("w", -1)}, "outside [0, 2^53-1]"},
		{"epoch too large", []ds.Option{ds.WithProducer("w", 1<<53)}, "outside [0, 2^53-1]"},
		{"auto-claim alone", []ds.Option{ds.WithProducerAutoClaim()}, "requires WithProducer"},
		{"control characters", []ds.Option{ds.WithProducer("w\r\nX-Injected: 1", 0)}, "contains control characters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ds.New(ps.URL+"/v1/stream", "events", tc.opts...); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New error = %v, want %q", err, tc.want)
			}
		})
	}
}

// scriptedServer answers producer POSTs from a script while delegating
// stream creation and reads to a protocol server.
func scriptedServer(t *testing.T, script func(w http.ResponseWriter, r *http.Request) bool) *protoServer {
	t.Helper()
	ps := newProtoServer()
	t.Cleanup(ps.Close)
	inner := ps.Config.Handler
	ps.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.Header.Get("Producer-Id") != "" && script(w, r) {
			return
		}
		inner.ServeHTTP(w, r)
	})
	return ps
}

func TestProducer_PermanentStatuses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		headers map[string]string
		want    error
	}{
		{"closed stream", http.StatusConflict, map[string]string{"Stream-Closed": "true"}, durablestreams.ErrStreamClosed},
		{"content type", http.StatusConflict, nil, durablestreams.ErrContentTypeMismatch},
		{"bad request", http.StatusBadRequest, nil, durablestreams.ErrBadRequest},
		{"not found", http.StatusNotFound, nil, durablestreams.ErrStreamNotFound},
		{"gone", http.StatusGone, nil, durablestreams.ErrOffsetGone},
		{"too large", http.StatusRequestEntityTooLarge, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var posts atomic.Int32
			ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				posts.Add(1)
				for k, v := range tc.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
				return true
			})
			store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
			_, err := store.Append(t.Context(), producerEvent(0))
			var streamErr *durablestreams.StreamError
			if err == nil || !errors.As(err, &streamErr) || streamErr.StatusCode != tc.status {
				t.Fatalf("Append error = %v", err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("Append error = %v, want %v", err, tc.want)
			}
			if posts.Load() != 1 {
				t.Fatalf("a permanent status was retried %d times", posts.Load())
			}
		})
	}
}

func TestProducer_RateLimitIsRetried(t *testing.T) {
	var posts atomic.Int32
	ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if posts.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return true
		}
		return false
	})
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
	if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}
	if got := ps.storedMessages("events"); len(got) != 1 || posts.Load() != 2 {
		t.Fatalf("messages = %v after %d posts", got, posts.Load())
	}
}

func TestProducer_MalformedProtocolResponses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		headers map[string]string
		want    string
	}{
		{"403 without epoch", http.StatusForbidden, nil, "not a producer fencing response"},
		{"403 with older epoch", http.StatusForbidden, map[string]string{"Producer-Epoch": "2"}, "not a producer fencing response"},
		{"403 with the same epoch", http.StatusForbidden, map[string]string{"Producer-Epoch": "3"}, "not a producer fencing response"},
		{"409 expecting a later sequence", http.StatusConflict, map[string]string{"Producer-Expected-Seq": "7"}, "server expects sequence \"7\""},
		{"409 with garbage", http.StatusConflict, map[string]string{"Producer-Expected-Seq": "x"}, "server expects sequence \"x\""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				for k, v := range tc.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
				return true
			})
			store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 3))
			if _, err := store.Append(t.Context(), producerEvent(0)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Append error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestProducer_CorrectionsAreBounded(t *testing.T) {
	t.Run("claims", func(t *testing.T) {
		ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			// A racing claimant that is always one epoch ahead.
			sent, _ := strconv.ParseInt(r.Header.Get("Producer-Epoch"), 10, 64)
			w.Header().Set("Producer-Epoch", strconv.FormatInt(sent+1, 10))
			w.WriteHeader(http.StatusForbidden)
			return true
		})
		store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0), ds.WithProducerAutoClaim())
		if _, err := store.Append(t.Context(), producerEvent(0)); err == nil || !strings.Contains(err.Error(), "corrections") {
			t.Fatalf("Append error = %v", err)
		}
	})
	t.Run("duplicates while claiming", func(t *testing.T) {
		ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			w.WriteHeader(http.StatusNoContent)
			return true
		})
		store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0), ds.WithProducerAutoClaim())
		if _, err := store.Append(t.Context(), producerEvent(0)); err == nil || !strings.Contains(err.Error(), "corrections") {
			t.Fatalf("Append error = %v", err)
		}
	})
	t.Run("renumbering", func(t *testing.T) {
		const burned = 40
		var posts atomic.Int32
		ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			if posts.Add(1) <= burned {
				// Abandon the first sequence numbers.
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			// Then keep asking for an ever smaller sequence number.
			seq, _ := strconv.Atoi(r.Header.Get("Producer-Seq"))
			w.Header().Set("Producer-Expected-Seq", strconv.Itoa(seq-1))
			w.WriteHeader(http.StatusConflict)
			return true
		})
		store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0), ds.WithRetry(1, time.Millisecond))
		for range burned {
			if _, err := store.Append(t.Context(), producerEvent(0)); err == nil {
				t.Fatal("expected the injected failure")
			}
		}
		if _, err := store.Append(t.Context(), producerEvent(1)); err == nil || !strings.Contains(err.Error(), "corrections") {
			t.Fatalf("Append error = %v", err)
		}
	})
}

func TestProducer_EpochCeiling(t *testing.T) {
	t.Run("duplicate at the ceiling", func(t *testing.T) {
		ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			w.WriteHeader(http.StatusNoContent)
			return true
		})
		store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 1<<53-1), ds.WithProducerAutoClaim())
		if _, err := store.Append(t.Context(), producerEvent(0)); err == nil || !strings.Contains(err.Error(), "cannot claim an epoch beyond") {
			t.Fatalf("Append error = %v", err)
		}
	})
	t.Run("stale at the ceiling", func(t *testing.T) {
		ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			w.Header().Set("Producer-Epoch", strconv.FormatInt(1<<53-1, 10))
			w.WriteHeader(http.StatusForbidden)
			return true
		})
		store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0), ds.WithProducerAutoClaim())
		if _, err := store.Append(t.Context(), producerEvent(0)); !errors.Is(err, ds.ErrProducerFenced) {
			t.Fatalf("Append error = %v", err)
		}
	})
}

func TestProducer_ContextEndsRetries(t *testing.T) {
	ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	})
	store, err := ds.New(ps.URL+"/v1/stream", "events", ds.WithProducer("writer", 0), ds.WithRetry(5, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := store.Append(ctx, producerEvent(0)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Append error = %v", err)
	}
	cancelled, stop := context.WithCancel(t.Context())
	stop()
	if _, err := store.Append(cancelled, producerEvent(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Append error = %v", err)
	}
}

func TestProducer_RetryExhaustion(t *testing.T) {
	ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.WriteHeader(http.StatusBadGateway)
		return true
	})
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
	if _, err := store.Append(t.Context(), producerEvent(0)); err == nil || !strings.Contains(err.Error(), "giving up after 3 attempts") {
		t.Fatalf("Append error = %v", err)
	}
}

func TestProducer_NoClaimAfterAPossiblyLandedAttempt(t *testing.T) {
	var posts atomic.Int32
	ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if posts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable) // may have landed at epoch 0
			return true
		}
		w.Header().Set("Producer-Epoch", "5") // a newer instance took over meanwhile
		w.WriteHeader(http.StatusForbidden)
		return true
	})
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0), ds.WithProducerAutoClaim())
	var fenced *ds.ProducerFencedError
	if _, err := store.Append(t.Context(), producerEvent(0)); !errors.As(err, &fenced) || fenced.Epoch != 0 || fenced.CurrentEpoch != 5 {
		t.Fatalf("Append error = %v", err)
	}
	if posts.Load() != 2 {
		t.Fatalf("re-sent a possibly committed event under a claimed epoch (%d posts)", posts.Load())
	}
}

func TestProducer_WaitingForTheProducerHonorsContext(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		select {
		case entered <- struct{}{}:
			<-release // hold the first append in flight
		default:
		}
		return false
	})
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
	first := make(chan error, 1)
	go func() {
		_, err := store.Append(context.Background(), producerEvent(0))
		first <- err
	}()
	<-entered

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := store.Append(ctx, producerEvent(1)); !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "waiting for the producer") {
		t.Fatalf("queued Append = %v", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("queued Append ignored its deadline for %v", waited)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	// The abandoned caller took no sequence number.
	if _, err := store.Append(t.Context(), producerEvent(2)); err != nil {
		t.Fatal(err)
	}
	if _, lastSeq, _ := ps.producerOf("events", "writer"); lastSeq != 1 {
		t.Fatalf("lastSeq = %d, want 1", lastSeq)
	}
}

// failFirstPosts makes the first n POSTs fail with status before the server
// processes them (nothing is committed).
func failFirstPosts(ps *protoServer, n int32, status int) *atomic.Int32 {
	var remaining atomic.Int32
	remaining.Store(n)
	ps.reject = func(*http.Request) int {
		if remaining.Add(-1) >= 0 {
			return status
		}
		return 0
	}
	return &remaining
}

// writeAsPredecessor appends n events as a previous instance owning epoch.
func writeAsPredecessor(t *testing.T, ps *protoServer, epoch int64, n int) {
	t.Helper()
	old := newProducerStore(t, ps, "events", ds.WithProducer("w", epoch))
	for i := range n {
		if _, err := old.Append(t.Context(), producerEvent(i)); err != nil {
			t.Fatal(err)
		}
	}
}

// A restarted auto-claim instance whose first attempt fails transiently gets
// a duplicate on retry for the predecessor's numbers. It must not report
// success for an event that was never stored (review probe).
func TestProducer_AutoClaimTransientThenForeignDuplicateIsNotSuccess(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	writeAsPredecessor(t, ps, 0, 3)
	failFirstPosts(ps, 1, http.StatusServiceUnavailable)
	fresh := newProducerStore(t, ps, "events", ds.WithProducer("w", 0), ds.WithProducerAutoClaim())
	if _, err := fresh.Append(t.Context(), producerEvent(99)); !errors.Is(err, ds.ErrProducerIndeterminate) {
		t.Fatalf("Append = %v, want ErrProducerIndeterminate, never success", err)
	}
	if got := ps.storedMessages("events"); len(got) != 3 {
		t.Fatalf("stored %d messages", len(got))
	}
	// The next append claims a new epoch and is stored.
	if _, err := fresh.Append(t.Context(), producerEvent(100)); err != nil {
		t.Fatal(err)
	}
	if epoch, _ := fresh.ProducerEpoch(); epoch != 1 || len(ps.storedMessages("events")) != 4 {
		t.Fatalf("epoch %d, stored %d", epoch, len(ps.storedMessages("events")))
	}
}

// The same without auto-claim: a reused epoch is never a silent success when
// a transient failure preceded the duplicate (review probe). Whether the
// first attempt landed is unknown, so the outcome is indeterminate.
func TestProducer_ReusedEpochTransientThenDuplicateIsNotSuccess(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests} {
		ps := newProtoServer()
		writeAsPredecessor(t, ps, 0, 3)
		failFirstPosts(ps, 1, status)
		fresh := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
		if _, err := fresh.Append(t.Context(), producerEvent(99)); !errors.Is(err, ds.ErrProducerIndeterminate) {
			t.Fatalf("status %d: Append = %v, want ErrProducerIndeterminate", status, err)
		}
		// The next append is a definite conflict: no attempt of it landed.
		if _, err := fresh.Append(t.Context(), producerEvent(100)); !errors.Is(err, ds.ErrProducerConflict) {
			t.Fatalf("status %d: next Append = %v, want ErrProducerConflict", status, err)
		}
		if got := ps.storedMessages("events"); len(got) != 3 {
			t.Fatalf("status %d: stored %d messages", status, len(got))
		}
		ps.Close()
	}
}

// Before the store owns its epoch, a duplicate whose highest accepted number
// equals ours cannot be attributed: it is indeterminate, not success.
func TestProducer_UnownedLostAckIsIndeterminate(t *testing.T) {
	t.Run("own attempt landed", func(t *testing.T) {
		ps := newProtoServer()
		defer ps.Close()
		var lost atomic.Bool
		ps.loseAck = func(*http.Request) bool { return lost.CompareAndSwap(false, true) }
		store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
		if _, err := store.Append(t.Context(), producerEvent(0)); !errors.Is(err, ds.ErrProducerIndeterminate) {
			t.Fatalf("Append = %v, want ErrProducerIndeterminate", err)
		}
		if got := ps.storedMessages("events"); len(got) != 1 {
			t.Fatalf("stored %d messages", len(got))
		}
	})
	t.Run("predecessor wrote only sequence 0", func(t *testing.T) {
		ps := newProtoServer()
		defer ps.Close()
		writeAsPredecessor(t, ps, 0, 1)
		failFirstPosts(ps, 1, http.StatusServiceUnavailable)
		store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
		if _, err := store.Append(t.Context(), producerEvent(99)); !errors.Is(err, ds.ErrProducerIndeterminate) {
			t.Fatalf("Append = %v, want ErrProducerIndeterminate", err)
		}
		if got := ps.storedMessages("events"); len(got) != 1 {
			t.Fatalf("stored %d messages", len(got))
		}
	})
	t.Run("no Producer-Seq header", func(t *testing.T) {
		var posts atomic.Int32
		ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			if posts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
			return true
		})
		store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
		if _, err := store.Append(t.Context(), producerEvent(0)); !errors.Is(err, ds.ErrProducerIndeterminate) {
			t.Fatalf("Append = %v, want ErrProducerIndeterminate", err)
		}
	})
}

// An owning store whose renumbered append (reusing a number an abandoned
// request carried) gets a duplicate after a lost acknowledgement cannot know
// whose request landed: indeterminate, never success.
func TestProducer_RenumberedLostAckIsIndeterminate(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0), ds.WithRetry(2, time.Millisecond))
	if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}
	// Sequence 1 is abandoned without landing.
	failFirstPosts(ps, 2, http.StatusServiceUnavailable)
	if _, err := store.Append(t.Context(), producerEvent(1)); err == nil {
		t.Fatal("expected the abandoned append to fail")
	}
	// Sequence 2 is renumbered to 1; its acknowledgement is lost.
	ps.reject = nil
	var lost atomic.Bool
	ps.loseAck = func(*http.Request) bool { return lost.CompareAndSwap(false, true) }
	if _, err := store.Append(t.Context(), producerEvent(2)); !errors.Is(err, ds.ErrProducerIndeterminate) {
		t.Fatalf("Append = %v, want ErrProducerIndeterminate", err)
	}
	if got := ps.storedMessages("events"); len(got) != 2 {
		t.Fatalf("stored %d messages", len(got))
	}
	// The store keeps working.
	if _, err := store.Append(t.Context(), producerEvent(3)); err != nil {
		t.Fatal(err)
	}
}

// A new instance with a fresh, greater epoch whose very first Append gives up
// must recover once the server is back: the epoch was never established, so
// the server rejects any sequence but 0 with 400 (review probe).
func TestProducer_FailedFirstAppendDoesNotWedgeANewEpoch(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	writeAsPredecessor(t, ps, 0, 2)
	var down atomic.Bool
	down.Store(true)
	ps.reject = func(*http.Request) int {
		if down.Load() {
			return http.StatusServiceUnavailable
		}
		return 0
	}
	fresh := newProducerStore(t, ps, "events", ds.WithProducer("w", 1))
	if _, err := fresh.Append(t.Context(), producerEvent(10)); err == nil {
		t.Fatal("expected the first append to give up")
	}
	down.Store(false)
	for i := range 3 {
		if _, err := fresh.Append(t.Context(), producerEvent(11+i)); err != nil {
			t.Fatalf("append %d after recovery: %v", i, err)
		}
	}
	if got := ps.storedMessages("events"); len(got) != 5 {
		t.Fatalf("stored %d messages, want 5", len(got))
	}
	if epoch, lastSeq, _ := ps.producerOf("events", "w"); epoch != 1 || lastSeq != 2 {
		t.Fatalf("server producer state epoch %d lastSeq %d", epoch, lastSeq)
	}
}

func TestProducer_BadRequestAtSequenceZeroIsPermanent(t *testing.T) {
	var posts atomic.Int32
	ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		posts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		return true
	})
	store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0), ds.WithRetry(1, time.Millisecond))
	// A 400 at sequence 0 is permanent and consumes the number.
	if _, err := store.Append(t.Context(), producerEvent(0)); !errors.Is(err, durablestreams.ErrBadRequest) || posts.Load() != 1 {
		t.Fatalf("Append = %v after %d requests", err, posts.Load())
	}
	before := posts.Load()
	if _, err := store.Append(t.Context(), producerEvent(1)); !errors.Is(err, durablestreams.ErrBadRequest) {
		t.Fatalf("Append = %v, want ErrBadRequest", err)
	}
	if sent := posts.Load() - before; sent != 2 {
		t.Fatalf("sent %d requests, want 2 (seq 1, then seq 0)", sent)
	}
}

func TestProducer_EpochRestartCountsAsACorrection(t *testing.T) {
	const burned = 40
	var posts, conflicts atomic.Int32
	ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if posts.Add(1) <= burned {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		if conflicts.Add(1) <= 16 {
			seq, _ := strconv.Atoi(r.Header.Get("Producer-Seq"))
			w.Header().Set("Producer-Expected-Seq", strconv.Itoa(seq-1))
			w.WriteHeader(http.StatusConflict)
			return true
		}
		w.WriteHeader(http.StatusBadRequest)
		return true
	})
	store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0), ds.WithRetry(1, time.Millisecond))
	for range burned {
		if _, err := store.Append(t.Context(), producerEvent(0)); err == nil {
			t.Fatal("expected the injected failure")
		}
	}
	if _, err := store.Append(t.Context(), producerEvent(1)); err == nil || !strings.Contains(err.Error(), "corrections") {
		t.Fatalf("Append = %v", err)
	}
}

func TestProducer_RequestConstructionFailure(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0), ds.WithRetry(1, time.Millisecond))
	ds.SetStreamURLForTest(store, "http://[::1") // unparsable
	_, err := store.Append(t.Context(), producerEvent(0))
	var streamErr *durablestreams.StreamError
	if !errors.As(err, &streamErr) || streamErr.StatusCode != 0 || !strings.Contains(err.Error(), "missing ']'") {
		t.Fatalf("Append = %v", err)
	}
}

// holdingServer delays chosen POSTs: their body is read at once, but the
// request is only processed when released, after the client gave up on it —
// a request that arrives late.
type holdingServer struct {
	*protoServer
	hold        atomic.Bool   // hold the next POST
	release     chan struct{} // closes to process the held POST
	releaseOnce sync.Once
	done        chan struct{} // closed once the held POST was processed
}

// releaseHeld processes the held POST and waits until it was handled.
func (hs *holdingServer) releaseHeld() {
	hs.releaseOnce.Do(func() { close(hs.release) })
	<-hs.done
}

func newHoldingServer(t *testing.T) *holdingServer {
	t.Helper()
	hs := &holdingServer{protoServer: newProtoServer(), release: make(chan struct{}), done: make(chan struct{})}
	t.Cleanup(hs.Close)
	// Runs first (cleanups are LIFO): a failing test must not leave the held
	// handler blocking the server's shutdown.
	t.Cleanup(func() { hs.releaseOnce.Do(func() { close(hs.release) }) })
	inner := hs.Config.Handler
	hs.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && hs.hold.CompareAndSwap(true, false) {
			body, _ := io.ReadAll(r.Body)
			<-hs.release
			r.Body = io.NopCloser(bytes.NewReader(body))
			inner.ServeHTTP(httptest.NewRecorder(), r) // the client is long gone
			close(hs.done)
			return
		}
		inner.ServeHTTP(w, r)
	})
	return hs
}

// Codex review finding: an event whose earlier attempt may still arrive must
// not be re-sent under another number, or the late request stores it twice.
func TestProducer_NeverResendsAnEventThatMayStillArrive(t *testing.T) {
	opts := []ds.Option{ds.WithRetry(2, time.Millisecond), ds.WithTimeout(150 * time.Millisecond)}
	t.Run("409 renumbering", func(t *testing.T) {
		hs := newHoldingServer(t)
		store := newProducerStore(t, hs.protoServer, "events", append(opts, ds.WithProducer("w", 0))...)
		if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
			t.Fatal(err)
		}
		failFirstPosts(hs.protoServer, 2, http.StatusServiceUnavailable) // sequence 1 is abandoned
		if _, err := store.Append(t.Context(), producerEvent(1)); err == nil {
			t.Fatal("expected the abandoned append to fail")
		}
		hs.hold.Store(true) // the first attempt of sequence 2 arrives late
		if _, err := store.Append(t.Context(), producerEvent(2)); !errors.Is(err, ds.ErrProducerIndeterminate) {
			t.Fatalf("Append = %v, want ErrProducerIndeterminate instead of renumbering", err)
		}
		hs.releaseHeld()
		if got := hs.storedMessages("events"); len(got) != 1 {
			t.Fatalf("stored %d messages: %v", len(got), got)
		}
		// The next event is renumbered normally and stored once.
		if _, err := store.Append(t.Context(), producerEvent(3)); err != nil {
			t.Fatal(err)
		}
		if got := hs.storedMessages("events"); len(got) != 2 || !strings.Contains(got[1], "event-3") {
			t.Fatalf("messages = %v", got)
		}
	})
	t.Run("400 epoch restart", func(t *testing.T) {
		hs := newHoldingServer(t)
		writeAsPredecessor(t, hs.protoServer, 0, 1)
		store := newProducerStore(t, hs.protoServer, "events", append(opts, ds.WithProducer("w", 1))...)
		failFirstPosts(hs.protoServer, 2, http.StatusServiceUnavailable) // sequence 0 never establishes epoch 1
		if _, err := store.Append(t.Context(), producerEvent(1)); err == nil {
			t.Fatal("expected the first append to give up")
		}
		hs.hold.Store(true) // the first attempt of sequence 1 arrives late
		if _, err := store.Append(t.Context(), producerEvent(2)); !errors.Is(err, ds.ErrProducerIndeterminate) {
			t.Fatalf("Append = %v, want ErrProducerIndeterminate instead of restarting the epoch", err)
		}
		hs.releaseHeld()
		if got := hs.storedMessages("events"); len(got) != 1 {
			t.Fatalf("stored %d messages: %v", len(got), got)
		}
		if _, err := store.Append(t.Context(), producerEvent(3)); err != nil {
			t.Fatal(err)
		}
		if got := hs.storedMessages("events"); len(got) != 2 || !strings.Contains(got[1], "event-3") {
			t.Fatalf("messages = %v", got)
		}
	})
}

// Codex review finding: a store that found its epoch in use before owning it
// must never take that epoch over later (a later 200 would let it attribute
// the other writer's duplicates to itself).
func TestProducer_ConflictBeforeOwnershipIsSticky(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	owner := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
	if _, err := owner.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}
	reused := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
	if _, err := reused.Append(t.Context(), producerEvent(10)); !errors.Is(err, ds.ErrProducerConflict) {
		t.Fatalf("first Append = %v", err)
	}
	log := ps.recordPosts()
	// Sequence 1 would be accepted in epoch 0; the store must not try.
	if _, err := reused.Append(t.Context(), producerEvent(11)); !errors.Is(err, ds.ErrProducerConflict) {
		t.Fatalf("second Append = %v, want the sticky conflict", err)
	}
	if got := log.snapshot(); len(got) != 0 {
		t.Fatalf("a conflicted store contacted the server: %v", got)
	}
	ps.reject = nil
	if _, err := owner.Append(t.Context(), producerEvent(1)); err != nil {
		t.Fatalf("the owner must keep working: %v", err)
	}
	if got := ps.storedMessages("events"); len(got) != 2 {
		t.Fatalf("messages = %v", got)
	}
}

// With auto-claim, an unattributable duplicate before owning the epoch makes
// the next append claim a fresh epoch instead of joining the old one.
func TestProducer_AutoClaimAfterAnUnattributableDuplicate(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	writeAsPredecessor(t, ps, 0, 1) // the predecessor holds only sequence 0
	failFirstPosts(ps, 1, http.StatusServiceUnavailable)
	store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0), ds.WithProducerAutoClaim())
	if _, err := store.Append(t.Context(), producerEvent(99)); !errors.Is(err, ds.ErrProducerIndeterminate) {
		t.Fatalf("Append = %v", err)
	}
	if _, err := store.Append(t.Context(), producerEvent(100)); err != nil {
		t.Fatal(err)
	}
	if epoch, _ := store.ProducerEpoch(); epoch != 1 {
		t.Fatalf("epoch = %d, want a freshly claimed 1 (sequence 1 of epoch 0 would have been accepted)", epoch)
	}
	if e, lastSeq, _ := ps.producerOf("events", "w"); e != 1 || lastSeq != 0 {
		t.Fatalf("server producer state epoch %d lastSeq %d", e, lastSeq)
	}
}

func TestProducer_PendingClaimAtTheEpochCeiling(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	writeAsPredecessor(t, ps, 1<<53-1, 1)
	failFirstPosts(ps, 1, http.StatusServiceUnavailable)
	store := newProducerStore(t, ps, "events", ds.WithProducer("w", 1<<53-1), ds.WithProducerAutoClaim())
	if _, err := store.Append(t.Context(), producerEvent(1)); !errors.Is(err, ds.ErrProducerIndeterminate) {
		t.Fatalf("Append = %v", err)
	}
	if _, err := store.Append(t.Context(), producerEvent(2)); err == nil || !strings.Contains(err.Error(), "cannot claim an epoch beyond") {
		t.Fatalf("Append = %v", err)
	}
}

func TestProducer_LargeResponseBodiesAreNotReadInFull(t *testing.T) {
	var written atomic.Int64
	ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.Header().Set("Stream-Next-Offset", padOffset(1))
		w.WriteHeader(http.StatusOK)
		chunk := bytes.Repeat([]byte("x"), 32<<10)
		for range 64 { // 2 MiB, far beyond what the client drains
			n, err := w.Write(chunk)
			written.Add(int64(n))
			if err != nil {
				return true
			}
		}
		return true
	})
	store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
	if offset, err := store.Append(t.Context(), producerEvent(0)); err != nil || offset != eventbus.Offset(padOffset(1)) {
		t.Fatalf("Append = %q, %v", offset, err)
	}
}

// Codex review (second round): Append must return the exact offset after a
// deduplicated event. When another writer's event follows it in the same
// read chunk no such offset exists, and the stream tail would skip that
// later event for a resuming reader: the outcome is indeterminate instead.
func TestProducer_DeduplicatedAppendReturnsTheExactOffset(t *testing.T) {
	interleave := func(ps *protoServer) func(*http.Request) bool {
		var lost atomic.Bool
		return func(*http.Request) bool {
			if !lost.CompareAndSwap(false, true) {
				return false
			}
			// Another writer appends before the retry.
			ps.mu.Lock()
			st := ps.streams["events"]
			st.messages = append(st.messages, []byte(`{"id":"other","type":"other","data":1}`))
			ps.mu.Unlock()
			return true
		}
	}
	t.Run("another writer's event follows in the chunk", func(t *testing.T) {
		ps := newProtoServer()
		defer ps.Close()
		store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
		if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
			t.Fatal(err)
		}
		ps.loseAck = interleave(ps)
		if _, err := store.Append(t.Context(), producerEvent(1)); !errors.Is(err, ds.ErrProducerIndeterminate) || !strings.Contains(err.Error(), "the event is stored") {
			t.Fatalf("Append = %v, want ErrProducerIndeterminate (the tail would skip the other event)", err)
		}
		if got := ps.storedMessages("events"); len(got) != 3 {
			t.Fatalf("messages = %v", got)
		}
		// The store keeps working, with exact offsets.
		ps.loseAck = nil
		if offset, err := store.Append(t.Context(), producerEvent(2)); err != nil || offset != eventbus.Offset(padOffset(4)) {
			t.Fatalf("next Append = %q, %v", offset, err)
		}
	})
	t.Run("the event ends its chunk", func(t *testing.T) {
		ps := newProtoServer()
		defer ps.Close()
		ps.chunkLimit = 1 // every record is its own chunk
		store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
		if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
			t.Fatal(err)
		}
		ps.loseAck = interleave(ps)
		offset, err := store.Append(t.Context(), producerEvent(1))
		if err != nil || offset != eventbus.Offset(padOffset(2)) {
			t.Fatalf("Append = %q, %v; want the exact %q before the other writer's event", offset, err, padOffset(2))
		}
	})
	t.Run("the event cannot be found", func(t *testing.T) {
		ps := newProtoServer()
		defer ps.Close()
		ps.omitProducerOffset = true
		store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
		event := producerEvent(0)
		event.ID = "" // nothing to look for
		if _, err := store.Append(t.Context(), event); !errors.Is(err, ds.ErrProducerIndeterminate) {
			t.Fatalf("Append = %v", err)
		}
	})
}

// Codex review (second round): only an acknowledged sequence 0 makes an
// epoch the store's. A store whose first acknowledged number is later cannot
// tell whether the epoch's earlier numbers were its own.
func TestProducer_OwnershipComesOnlyFromSequenceZero(t *testing.T) {
	t.Run("without auto-claim the store stops after the ambiguous append", func(t *testing.T) {
		ps := newProtoServer()
		defer ps.Close()
		writeAsPredecessor(t, ps, 0, 1) // a live writer holds epoch 0 with sequence 0
		failFirstPosts(ps, 1, http.StatusServiceUnavailable)
		reused := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
		if _, err := reused.Append(t.Context(), producerEvent(10)); !errors.Is(err, ds.ErrProducerIndeterminate) {
			t.Fatalf("first Append = %v", err)
		}
		// Sequence 1 is accepted, but the epoch is not provably this store's.
		if _, err := reused.Append(t.Context(), producerEvent(11)); err != nil {
			t.Fatalf("second Append = %v (its event is written)", err)
		}
		log := ps.recordPosts()
		if _, err := reused.Append(t.Context(), producerEvent(12)); !errors.Is(err, ds.ErrProducerIndeterminate) || !strings.Contains(err.Error(), "cannot establish ownership") {
			t.Fatalf("third Append = %v, want the sticky ownership error", err)
		}
		if got := log.snapshot(); len(got) != 0 {
			t.Fatalf("a store without ownership contacted the server: %v", got)
		}
	})
	t.Run("with auto-claim the next append claims a fresh epoch", func(t *testing.T) {
		ps := newProtoServer()
		defer ps.Close()
		var lost atomic.Bool
		ps.loseAck = func(*http.Request) bool { return lost.CompareAndSwap(false, true) }
		store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0), ds.WithProducerAutoClaim(), ds.WithRetry(1, time.Millisecond))
		if _, err := store.Append(t.Context(), producerEvent(0)); err == nil {
			t.Fatal("expected the lost acknowledgement to fail the single attempt")
		}
		if _, err := store.Append(t.Context(), producerEvent(1)); err != nil {
			t.Fatal(err) // sequence 1 of epoch 0: written, ownership ambiguous
		}
		if _, err := store.Append(t.Context(), producerEvent(2)); err != nil {
			t.Fatal(err)
		}
		if epoch, _ := store.ProducerEpoch(); epoch != 1 {
			t.Fatalf("epoch = %d, want a fresh claim", epoch)
		}
	})
}

// locate's defensive paths: an undecodable chunk, and a server whose chunk
// does not advance the offset.
func TestProducer_LocateDefensivePaths(t *testing.T) {
	for _, tc := range []struct {
		name string
		read func(w http.ResponseWriter, r *http.Request)
		want string
	}{
		{"undecodable chunk", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Stream-Next-Offset", padOffset(5))
			_, _ = w.Write([]byte("not json"))
		}, "append committed, but its offset is unknown"},
		{"caught up before the captured tail", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Stream-Next-Offset", r.URL.Query().Get("offset"))
			w.Header().Set("Stream-Up-To-Date", "true")
			w.WriteHeader(http.StatusNoContent)
		}, "no exact offset follows it"},
		{"a copy, then caught up before the captured tail", func(w http.ResponseWriter, r *http.Request) {
			// The first read returns one copy of the ID, ending its chunk;
			// the next answers caught up although the tail is later. A
			// later copy could have been missed, so nothing is proven.
			if r.URL.Query().Get("offset") == padOffset(1) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Stream-Next-Offset", padOffset(2))
				_, _ = w.Write([]byte(`[{"id":"event-1","type":"produced","data":1}]`))
				return
			}
			w.Header().Set("Stream-Next-Offset", r.URL.Query().Get("offset"))
			w.Header().Set("Stream-Up-To-Date", "true")
			w.WriteHeader(http.StatusNoContent)
		}, "no exact offset follows it"},
		{"chunk that does not advance", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Stream-Next-Offset", r.URL.Query().Get("offset"))
			_, _ = w.Write([]byte(`[{"id":"event-1","type":"produced","data":1},{"id":"x","type":"t","data":1}]`))
		}, "no exact offset follows it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var posts atomic.Int32
			ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				switch posts.Add(1) {
				case 1:
					return false // accepted: the store owns its epoch
				case 2:
					w.WriteHeader(http.StatusServiceUnavailable) // may have landed
				default:
					w.Header().Set("Producer-Seq", "1")
					w.WriteHeader(http.StatusNoContent)
				}
				return true
			})
			store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
			if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
				t.Fatal(err)
			}
			inner := ps.Config.Handler
			ps.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					w.Header().Set("Stream-Next-Offset", padOffset(9))
					w.WriteHeader(http.StatusOK)
				case http.MethodGet:
					tc.read(w, r)
				default:
					inner.ServeHTTP(w, r)
				}
			})
			if _, err := store.Append(t.Context(), producerEvent(1)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Append = %v, want %q", err, tc.want)
			}
		})
	}
}

// Codex review (third round): a higher server sequence does not prove that a
// possibly landed attempt was rejected. Here sequence 0 lands with its
// acknowledgement lost, then another writer in the same epoch appends
// sequence 1 before the retry.
func TestProducer_LandedAttemptBeforeAnotherWritersNumber(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	var lost atomic.Bool
	ps.loseAck = func(*http.Request) bool {
		if !lost.CompareAndSwap(false, true) {
			return false
		}
		ps.mu.Lock()
		st := ps.streams["events"]
		st.messages = append(st.messages, []byte(`{"id":"other","type":"other","data":1}`))
		st.producers["w"].lastSeq = 1 // the other writer's sequence 1
		ps.mu.Unlock()
		return true
	}
	store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
	_, err := store.Append(t.Context(), producerEvent(0))
	if errors.Is(err, ds.ErrProducerConflict) || !errors.Is(err, ds.ErrProducerIndeterminate) {
		t.Fatalf("Append = %v, want ErrProducerIndeterminate (the event is stored)", err)
	}
	if got := ps.storedMessages("events"); len(got) != 2 || !strings.Contains(got[0], "event-0") {
		t.Fatalf("messages = %v", got)
	}
}

// Codex review (third round): an ID identifies a publish, so it can occur
// more than once; the appended copy cannot then be told apart.
func TestProducer_RepeatedEventIDIsIndeterminate(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
	if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}
	// An earlier publish of event-1 already reached the stream, after the
	// store's last acknowledged offset (for example an indeterminate append
	// that the caller retried).
	ps.mu.Lock()
	ps.streams["events"].messages = append(ps.streams["events"].messages, []byte(`{"id":"event-1","type":"produced","data":1}`))
	ps.mu.Unlock()
	ps.chunkLimit = 1 // each copy ends its own chunk
	var lost atomic.Bool
	ps.loseAck = func(*http.Request) bool { return lost.CompareAndSwap(false, true) }
	if _, err := store.Append(t.Context(), producerEvent(1)); !errors.Is(err, ds.ErrProducerIndeterminate) {
		t.Fatalf("Append = %v, want ErrProducerIndeterminate", err)
	}
}

// Codex review (third round): offset recovery stops at the tail captured
// before scanning, so writers extending the stream cannot keep it running.
func TestProducer_OffsetRecoveryIsBoundedOnABusyStream(t *testing.T) {
	ps := newProtoServer()
	defer ps.Close()
	ps.chunkLimit = 1
	inner := ps.Config.Handler
	var busy atomic.Bool
	ps.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && busy.Load() {
			// Another writer appends before every read.
			ps.mu.Lock()
			st := ps.streams["events"]
			st.messages = append(st.messages, []byte(`{"id":"busy","type":"other","data":1}`))
			ps.mu.Unlock()
		}
		inner.ServeHTTP(w, r)
	})
	store := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
	if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}
	var lost atomic.Bool
	ps.loseAck = func(*http.Request) bool {
		if lost.CompareAndSwap(false, true) {
			busy.Store(true)
			return true
		}
		return false
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	offset, err := store.Append(ctx, producerEvent(1))
	if err != nil || offset != eventbus.Offset(padOffset(2)) {
		t.Fatalf("Append = %q, %v; want the exact %q, found within the captured tail", offset, err, padOffset(2))
	}
}
