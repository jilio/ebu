package durablestream_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
			w.Header().Set("Producer-Seq", "1")
			w.WriteHeader(http.StatusNoContent) // ...and it had
		}
		return true
	})
	store := newProducerStore(t, ps, "events", ds.WithProducer("writer", 0))
	if _, err := store.Append(t.Context(), producerEvent(0)); err != nil {
		t.Fatal(err)
	}
	inner := ps.Config.Handler
	ps.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		inner.ServeHTTP(w, r)
	})
	_, err := store.Append(t.Context(), producerEvent(1))
	if err == nil || !errors.Is(err, durablestreams.ErrStreamNotFound) || !strings.Contains(err.Error(), "append committed, but its offset is unknown") {
		t.Fatalf("expected a committed-but-unknown-offset error, got %v", err)
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
		var epoch atomic.Int64
		ps := scriptedServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			// A racing claimant that is always one epoch ahead.
			w.Header().Set("Producer-Epoch", strconv.FormatInt(epoch.Add(1), 10))
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
	if _, err := fresh.Append(t.Context(), producerEvent(99)); !errors.Is(err, ds.ErrProducerConflict) {
		t.Fatalf("Append = %v, want ErrProducerConflict (the duplicate is the predecessor's)", err)
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

// The same without auto-claim: a reused epoch stays a loud conflict even when
// a transient failure preceded the duplicate (review probe).
func TestProducer_ReusedEpochTransientThenDuplicateIsAConflict(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests} {
		ps := newProtoServer()
		writeAsPredecessor(t, ps, 0, 3)
		failFirstPosts(ps, 1, status)
		fresh := newProducerStore(t, ps, "events", ds.WithProducer("w", 0))
		if _, err := fresh.Append(t.Context(), producerEvent(99)); !errors.Is(err, ds.ErrProducerConflict) {
			t.Fatalf("status %d: Append = %v, want ErrProducerConflict", status, err)
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
