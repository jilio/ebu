package durablestream_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
	ds "github.com/jilio/ebu/stores/durablestream"
)

func newSnapshotStore(t *testing.T, baseURL, path string, opts ...ds.Option) *ds.Store {
	t.Helper()
	opts = append([]ds.Option{ds.WithRetry(3, time.Millisecond)}, opts...)
	store, err := ds.New(baseURL+"/v1/stream", path, opts...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return store
}

func TestSnapshotSaveLoadRoundTrip(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-roundtrip")
	ctx := context.Background()

	blob := json.RawMessage(`{"users":{"u1":{"name":"ada"}}}`)
	if err := store.SaveSnapshot(ctx, "users", "42_0", blob); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	atOffset, got, err := store.LoadSnapshot(ctx, "users")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if atOffset != "42_0" {
		t.Errorf("atOffset = %q, want %q", atOffset, "42_0")
	}
	if string(got) != string(blob) {
		t.Errorf("blob = %s, want %s", got, blob)
	}
}

func TestSnapshotLoadMissing(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	ctx := context.Background()

	t.Run("companion stream absent", func(t *testing.T) {
		store := newSnapshotStore(t, srv.URL, "snap-missing-stream")
		atOffset, blob, err := store.LoadSnapshot(ctx, "users")
		if err != nil {
			t.Fatalf("LoadSnapshot() error = %v", err)
		}
		if atOffset != eventbus.OffsetOldest || blob != nil {
			t.Errorf("got (%q, %s), want (OffsetOldest, nil)", atOffset, blob)
		}
	})

	t.Run("id absent on existing companion stream", func(t *testing.T) {
		store := newSnapshotStore(t, srv.URL, "snap-missing-id")
		if err := store.SaveSnapshot(ctx, "orders", "1_0", json.RawMessage(`{}`)); err != nil {
			t.Fatalf("SaveSnapshot() error = %v", err)
		}
		atOffset, blob, err := store.LoadSnapshot(ctx, "users")
		if err != nil {
			t.Fatalf("LoadSnapshot() error = %v", err)
		}
		if atOffset != eventbus.OffsetOldest || blob != nil {
			t.Errorf("got (%q, %s), want (OffsetOldest, nil)", atOffset, blob)
		}
	})
}

func TestSnapshotLastWriteWins(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-lww")
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatalf("SaveSnapshot(1) error = %v", err)
	}
	if err := store.SaveSnapshot(ctx, "users", "9_0", json.RawMessage(`{"v":2}`)); err != nil {
		t.Fatalf("SaveSnapshot(2) error = %v", err)
	}
	// Last write wins even when the offset moves backward, like SQLite's
	// unconditional upsert.
	if err := store.SaveSnapshot(ctx, "users", "5_0", json.RawMessage(`{"v":3}`)); err != nil {
		t.Fatalf("SaveSnapshot(3) error = %v", err)
	}

	atOffset, blob, err := store.LoadSnapshot(ctx, "users")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if atOffset != "5_0" || string(blob) != `{"v":3}` {
		t.Errorf("got (%q, %s), want (5_0, {\"v\":3})", atOffset, blob)
	}
}

func TestSnapshotIDIsolation(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-isolation")
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{"k":"users"}`)); err != nil {
		t.Fatalf("SaveSnapshot(users) error = %v", err)
	}
	if err := store.SaveSnapshot(ctx, "orders", "2_0", json.RawMessage(`{"k":"orders"}`)); err != nil {
		t.Fatalf("SaveSnapshot(orders) error = %v", err)
	}

	for id, wantOffset := range map[string]eventbus.Offset{"users": "1_0", "orders": "2_0"} {
		atOffset, blob, err := store.LoadSnapshot(ctx, id)
		if err != nil {
			t.Fatalf("LoadSnapshot(%q) error = %v", id, err)
		}
		if atOffset != wantOffset || !strings.Contains(string(blob), id) {
			t.Errorf("LoadSnapshot(%q) = (%q, %s), want offset %q", id, atOffset, blob, wantOffset)
		}
	}
}

func TestSnapshotSaveAtOffsetOldest(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-oldest")
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "empty", eventbus.OffsetOldest, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	atOffset, blob, err := store.LoadSnapshot(ctx, "empty")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if atOffset != eventbus.OffsetOldest || string(blob) != `{}` {
		t.Errorf("got (%q, %s), want (OffsetOldest, {})", atOffset, blob)
	}
}

func TestSnapshotSaveRejectsOffsetNewest(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-newest")

	err := store.SaveSnapshot(context.Background(), "users", eventbus.OffsetNewest, json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "not a durable position") {
		t.Fatalf("got %v, want symbolic-offset rejection", err)
	}
}

func TestSnapshotSaveRejectsInvalidBlob(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-badblob")

	err := store.SaveSnapshot(context.Background(), "users", "1_0", json.RawMessage(`{not json`))
	if err == nil || !strings.Contains(err.Error(), "marshal snapshot") {
		t.Fatalf("got %v, want marshal error", err)
	}
}

// TestSnapshotPlusTailEqualsFullReplay is the equivalence property behind
// snapshot-based cold starts: for any prefix boundary k, the events covered
// by a snapshot taken at the k-th append offset plus the events read after
// that offset must equal the full history. Append-returned offsets are exact
// resume points on this store, so the tail starts precisely after event k.
func TestSnapshotPlusTailEqualsFullReplay(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-equivalence")
	ctx := context.Background()

	const n = 12
	offsets := make([]eventbus.Offset, 0, n) // exact next-offset after each event
	for i := 0; i < n; i++ {
		offset, err := store.Append(ctx, &eventbus.Event{
			ID:   fmt.Sprintf("ev-%02d", i),
			Type: "counter.incremented",
			Data: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
		})
		if err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
		offsets = append(offsets, offset)
	}

	full := readAllFrom(t, store, eventbus.OffsetOldest)
	if len(full) != n {
		t.Fatalf("full replay returned %d events, want %d", len(full), n)
	}

	for _, k := range []int{1, n / 2, n} {
		k := k
		t.Run(fmt.Sprintf("boundary at event %d", k), func(t *testing.T) {
			// The "projection" is simply the set of IDs folded in. Snapshot it
			// at the exact offset returned for event k-1.
			var foldedIDs []string
			for _, stored := range full[:k] {
				foldedIDs = append(foldedIDs, stored.ID)
			}
			blob, err := json.Marshal(foldedIDs)
			if err != nil {
				t.Fatalf("marshal fold: %v", err)
			}
			snapID := fmt.Sprintf("fold-%d", k)
			if err := store.SaveSnapshot(ctx, snapID, offsets[k-1], blob); err != nil {
				t.Fatalf("SaveSnapshot() error = %v", err)
			}

			// Cold start: load snapshot, then read the tail from its offset.
			atOffset, loaded, err := store.LoadSnapshot(ctx, snapID)
			if err != nil {
				t.Fatalf("LoadSnapshot() error = %v", err)
			}
			var restored []string
			if err := json.Unmarshal(loaded, &restored); err != nil {
				t.Fatalf("unmarshal snapshot: %v", err)
			}
			for _, stored := range readAllFrom(t, store, atOffset) {
				restored = append(restored, stored.ID)
			}

			if len(restored) != n {
				t.Fatalf("snapshot+tail folded %d events, want %d (%v)", len(restored), n, restored)
			}
			for i, id := range restored {
				if want := fmt.Sprintf("ev-%02d", i); id != want {
					t.Fatalf("snapshot+tail order mismatch at %d: got %q, want %q", i, id, want)
				}
			}
		})
	}
}

// corruptSnapStream appends a record that is valid JSON but cannot decode
// into a snapshot record (a bare string), directly via the protocol client.
func corruptSnapStream(t *testing.T, store *ds.Store, snapPath string) {
	t.Helper()
	writer, err := store.Client().Writer(context.Background(), snapPath)
	if err != nil {
		t.Fatalf("companion writer: %v", err)
	}
	if err := writer.Send([]byte(`"junk-record"`), nil); err != nil {
		t.Fatalf("append junk record: %v", err)
	}
}

func TestSnapshotSkipsMalformedRecords(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	var decodeErrs int
	store := newSnapshotStore(t, srv.URL, "snap-malformed",
		ds.WithDecodeErrorHandler(func(err error, raw []byte) { decodeErrs++ }))
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	corruptSnapStream(t, store, "snap-malformed.snap")
	if err := store.SaveSnapshot(ctx, "users", "2_0", json.RawMessage(`{"v":2}`)); err != nil {
		t.Fatalf("SaveSnapshot(2) error = %v", err)
	}

	atOffset, blob, err := store.LoadSnapshot(ctx, "users")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if atOffset != "2_0" || string(blob) != `{"v":2}` {
		t.Errorf("got (%q, %s), want (2_0, {\"v\":2})", atOffset, blob)
	}
	if decodeErrs == 0 {
		t.Error("decode error handler was not invoked for the malformed record")
	}
}

func TestSnapshotLoggerFallbackForMalformed(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	logger := &testLogger{}
	store := newSnapshotStore(t, srv.URL, "snap-logger", ds.WithLogger(logger))
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "3_0", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	corruptSnapStream(t, store, "snap-logger.snap")

	atOffset, _, err := store.LoadSnapshot(ctx, "users")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if atOffset != "3_0" {
		t.Errorf("atOffset = %q, want 3_0", atOffset)
	}
	found := false
	for _, line := range logger.messages {
		if strings.Contains(line, "malformed snapshot record") {
			found = true
		}
	}
	if !found {
		t.Errorf("logger did not record the malformed snapshot record, got %v", logger.messages)
	}
}

func TestSnapshotSaveRetriesTransientFailures(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-save-retry")
	ctx := context.Background()

	// One failed append POST; the retry succeeds. (Creation and send share
	// one attempt budget: each network call consumes one attempt.)
	srv.failPost.Store(1)
	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("SaveSnapshot() should succeed after retry, got %v", err)
	}
	atOffset, _, err := store.LoadSnapshot(ctx, "users")
	if err != nil || atOffset != "1_0" {
		t.Fatalf("LoadSnapshot() = (%q, _, %v), want (1_0, _, nil)", atOffset, err)
	}
}

func TestSnapshotSaveGivesUpOnPersistentFailure(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-save-fail")

	srv.failPost.Store(100)
	srv.failHead.Store(100)
	err := store.SaveSnapshot(context.Background(), "users", "1_0", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "save snapshot") {
		t.Fatalf("got %v, want save snapshot failure", err)
	}
}

func TestSnapshotSavePermanentFailureDoesNotRetry(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-save-perm")

	// After the companion stream exists, a 400 on append is permanent.
	if err := store.SaveSnapshot(context.Background(), "users", "1_0", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("initial SaveSnapshot() error = %v", err)
	}
	posts := srv.posts.Load()
	srv.failStatus.Store(http.StatusBadRequest)
	srv.failPost.Store(100)
	srv.failHead.Store(100)
	err := store.SaveSnapshot(context.Background(), "users", "2_0", json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected permanent failure")
	}
	if got := srv.posts.Load() - posts; got > 1 {
		t.Errorf("expected no POST retries on 400, got %d attempts", got)
	}
}

func TestSnapshotCreateStreamFailure(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-create-fail")

	// The companion stream is created lazily on the first save; fail it.
	srv.failStatus.Store(http.StatusBadRequest)
	srv.failPut.Store(100)
	err := store.SaveSnapshot(context.Background(), "users", "1_0", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "create companion stream") {
		t.Fatalf("got %v, want create companion stream failure", err)
	}

	// Transient create failures retry and then succeed.
	srv.failStatus.Store(0)
	srv.failPut.Store(1)
	if err := store.SaveSnapshot(context.Background(), "users", "1_0", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("SaveSnapshot() after transient create failure = %v", err)
	}
}

func TestSnapshotLoadRetriesTransientFailures(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-load-retry")
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	srv.failGet.Store(1)
	atOffset, _, err := store.LoadSnapshot(ctx, "users")
	if err != nil || atOffset != "1_0" {
		t.Fatalf("LoadSnapshot() = (%q, _, %v), want (1_0, _, nil)", atOffset, err)
	}
}

func TestSnapshotLoadGivesUpOnPersistentFailure(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-load-fail")
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	srv.failGet.Store(100)
	_, _, err := store.LoadSnapshot(ctx, "users")
	if err == nil || !strings.Contains(err.Error(), "load snapshot") {
		t.Fatalf("got %v, want load snapshot failure", err)
	}
}

func TestNewRejectsReservedSnapSuffix(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	_, err := ds.New(srv.URL+"/v1/stream", "orders.snap")
	if err == nil || !strings.Contains(err.Error(), "reserved snapshot companion suffix") {
		t.Fatalf("got %v, want reserved-suffix rejection", err)
	}
}

// newSnapPageServer serves a paginated companion stream: page 1 returns the
// given records, later pages answer with failStatus. The main-stream PUT
// succeeds so ds.New works.
func newSnapPageServer(page1 string, failStatus int) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			if r.URL.Query().Get("offset") == "" {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Stream-Next-Offset", "p2")
				// Deliberately not up-to-date: the loader must fetch page 2.
				w.Write([]byte(page1))
				return
			}
			http.Error(w, http.StatusText(failStatus), failStatus)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	return httptest.NewServer(mux)
}

func TestSnapshotLoadKeepsFoundRecordWhenLaterPageFails(t *testing.T) {
	page1 := `[{"snapshot_id":"users","at_offset":"1_0","blob":{"v":1}}]`

	t.Run("stream deleted mid-read", func(t *testing.T) {
		srv := newSnapPageServer(page1, http.StatusNotFound)
		defer srv.Close()
		store := newSnapshotStore(t, srv.URL, "snap-page-404")
		atOffset, blob, err := store.LoadSnapshot(context.Background(), "users")
		if err != nil {
			t.Fatalf("LoadSnapshot() error = %v", err)
		}
		if atOffset != "1_0" || string(blob) != `{"v":1}` {
			t.Errorf("got (%q, %s), want the page-1 record", atOffset, blob)
		}
	})

	t.Run("head trimmed mid-read fails loudly despite an earlier record", func(t *testing.T) {
		// A 410 mid-pagination means records NEWER than the earlier pages may
		// survive beyond the trim point, unreachable; returning the earlier
		// (stale) record silently would serve outdated state forever.
		srv := newSnapPageServer(page1, http.StatusGone)
		defer srv.Close()
		store := newSnapshotStore(t, srv.URL, "snap-page-410")
		_, _, err := store.LoadSnapshot(context.Background(), "users")
		if err == nil || !strings.Contains(err.Error(), "head-trimmed") {
			t.Fatalf("got %v, want loud head-trimmed error", err)
		}
	})
}

func TestSnapshotLoadFailsLoudlyOnTrimmedHead(t *testing.T) {
	// 410 on the very first page with nothing found: surviving records are
	// unreachable, so this must be a loud error, not a false miss.
	srv := newFlakyServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-trimmed")
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	srv.failStatus.Store(http.StatusGone)
	srv.failGet.Store(100)
	_, _, err := store.LoadSnapshot(ctx, "users")
	if err == nil || !strings.Contains(err.Error(), "head-trimmed") {
		t.Fatalf("got %v, want loud head-trimmed error", err)
	}
}

func TestSnapshotSaveRecreatesDeletedCompanionStream(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-recreate")
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatalf("SaveSnapshot(1) error = %v", err)
	}
	// The companion stream disappears server-side (operator cleanup or TTL).
	if err := store.Client().Delete(ctx, "snap-recreate.snap"); err != nil {
		t.Fatalf("delete companion stream: %v", err)
	}
	// The next save must re-create it instead of failing until restart.
	if err := store.SaveSnapshot(ctx, "users", "2_0", json.RawMessage(`{"v":2}`)); err != nil {
		t.Fatalf("SaveSnapshot(2) after companion deletion error = %v", err)
	}

	atOffset, blob, err := store.LoadSnapshot(ctx, "users")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if atOffset != "2_0" || string(blob) != `{"v":2}` {
		t.Errorf("got (%q, %s), want (2_0, {\"v\":2})", atOffset, blob)
	}
}

func TestSnapshotSaveRejectsEmptyID(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-empty-id")

	err := store.SaveSnapshot(context.Background(), "", "1_0", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "snapshot ID cannot be empty") {
		t.Fatalf("got %v, want empty-id rejection", err)
	}
}

func TestSnapshotLoadReportsForeignRecords(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	var decodeErrs int
	store := newSnapshotStore(t, srv.URL, "snap-foreign",
		ds.WithDecodeErrorHandler(func(err error, raw []byte) { decodeErrs++ }))
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	// An event envelope on the companion stream (pre-reservation stream or a
	// raw protocol writer): decodes as a record with an empty snapshot_id.
	// It must be skipped AND reported, never silently ignored.
	writer, err := store.Client().Writer(ctx, "snap-foreign.snap")
	if err != nil {
		t.Fatalf("companion writer: %v", err)
	}
	if err := writer.Send([]byte(`{"type":"user.created","data":{"id":"u1"}}`), nil); err != nil {
		t.Fatalf("append foreign envelope: %v", err)
	}

	atOffset, blob, err := store.LoadSnapshot(ctx, "users")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if atOffset != "1_0" || string(blob) != `{"v":1}` {
		t.Errorf("got (%q, %s), want the genuine (1_0) record", atOffset, blob)
	}
	if decodeErrs == 0 {
		t.Error("foreign record was not reported to the decode error handler")
	}
}

// TestSnapshotSaveRecreatesWithSingleAttempt pins the recovery when the 404
// lands on the final (here: only) attempt — the re-create must not consume a
// send attempt, or WithRetry(1) could never recover.
func TestSnapshotSaveRecreatesWithSingleAttempt(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-recreate-one", ds.WithRetry(1, time.Millisecond))
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatalf("SaveSnapshot(1) error = %v", err)
	}
	if err := store.Client().Delete(ctx, "snap-recreate-one.snap"); err != nil {
		t.Fatalf("delete companion stream: %v", err)
	}
	if err := store.SaveSnapshot(ctx, "users", "2_0", json.RawMessage(`{"v":2}`)); err != nil {
		t.Fatalf("SaveSnapshot(2) with WithRetry(1) error = %v", err)
	}

	atOffset, _, err := store.LoadSnapshot(ctx, "users")
	if err != nil || atOffset != "2_0" {
		t.Fatalf("LoadSnapshot() = (%q, _, %v), want (2_0, _, nil)", atOffset, err)
	}
}

func TestSnapshotSaveRecreateFailureSurfaces(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-recreate-fail")
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("SaveSnapshot(1) error = %v", err)
	}
	if err := store.Client().Delete(ctx, "snap-recreate-fail.snap"); err != nil {
		t.Fatalf("delete companion stream: %v", err)
	}
	// Re-creation itself fails permanently: the save must surface that.
	srv.failStatus.Store(http.StatusBadRequest)
	srv.failPut.Store(100)
	err := store.SaveSnapshot(ctx, "users", "2_0", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "create companion stream") {
		t.Fatalf("got %v, want create companion stream failure", err)
	}
}

func TestSnapshotLoadSkipsSymbolicOffsetRecord(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	var decodeErrs int
	store := newSnapshotStore(t, srv.URL, "snap-symbolic",
		ds.WithDecodeErrorHandler(func(err error, raw []byte) { decodeErrs++ }))
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	// A corrupt/foreign record claiming the symbolic tail as its offset: it
	// must be skipped like any malformed record, keeping the good one.
	writer, err := store.Client().Writer(ctx, "snap-symbolic.snap")
	if err != nil {
		t.Fatalf("companion writer: %v", err)
	}
	if err := writer.Send([]byte(`{"snapshot_id":"users","at_offset":"$","blob":{"v":666}}`), nil); err != nil {
		t.Fatalf("append symbolic record: %v", err)
	}

	atOffset, blob, err := store.LoadSnapshot(ctx, "users")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if atOffset != "1_0" || string(blob) != `{"v":1}` {
		t.Errorf("got (%q, %s), want the good (1_0) record", atOffset, blob)
	}
	if decodeErrs == 0 {
		t.Error("symbolic-offset record was not reported to the decode error handler")
	}
}

func TestSnapshotLoadPermanentFailure(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-load-perm")
	ctx := context.Background()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	// A 400 on the companion read is permanent: no retries, immediate error.
	srv.failStatus.Store(http.StatusBadRequest)
	srv.failGet.Store(100)
	gets := srv.gets.Load()
	_, _, err := store.LoadSnapshot(ctx, "users")
	if err == nil || !strings.Contains(err.Error(), "load snapshot") {
		t.Fatalf("got %v, want load snapshot failure", err)
	}
	if got := srv.gets.Load() - gets; got != 1 {
		t.Errorf("expected 1 GET attempt on 400, got %d", got)
	}
}

func TestSnapshotCreateGivesUpOnTransientFailures(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-create-giveup")

	// Every create attempt fails 503: retries exhaust and the save reports it.
	srv.failPut.Store(100)
	err := store.SaveSnapshot(context.Background(), "users", "1_0", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "create companion stream") || !strings.Contains(err.Error(), "giving up") {
		t.Fatalf("got %v, want create giving-up failure", err)
	}
}

func TestSnapshotLoadMultiChunk(t *testing.T) {
	// Two records served one per chunk: the loader must follow NextOffset
	// across chunks and keep the newest record for the id.
	records := []string{
		`{"snapshot_id":"users","at_offset":"1_0","blob":{"v":1}}`,
		`{"snapshot_id":"other","at_offset":"7_0","blob":{"v":7}}`,
		`{"snapshot_id":"users","at_offset":"2_0","blob":{"v":2}}`,
	}
	srv := newChunkedServer(records, 1)
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-chunky")

	atOffset, blob, err := store.LoadSnapshot(context.Background(), "users")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if atOffset != "2_0" || string(blob) != `{"v":2}` {
		t.Errorf("got (%q, %s), want (2_0, {\"v\":2})", atOffset, blob)
	}
}

func TestSnapshotLoadInvalidJSONBody(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			w.Header().Set("Stream-Next-Offset", "1")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("invalid json"))
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store := newSnapshotStore(t, srv.URL, "snap-invalid-json")
	_, _, err := store.LoadSnapshot(context.Background(), "users")
	if err == nil || !strings.Contains(err.Error(), "unmarshal response") {
		t.Fatalf("got %v, want unmarshal response error", err)
	}
}

func TestSnapshotBackoffInterruptedByContext(t *testing.T) {
	newCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 50*time.Millisecond)
	}
	// A large base delay guarantees cancellation lands inside the backoff.
	slowRetry := ds.WithRetry(3, time.Hour)

	t.Run("save send backoff", func(t *testing.T) {
		srv := newFlakyServer()
		defer srv.Close()
		store := newSnapshotStore(t, srv.URL, "snap-save-backoff", slowRetry)
		// Create the companion stream first so the send loop is reached.
		if err := store.SaveSnapshot(context.Background(), "users", "1_0", json.RawMessage(`{}`)); err != nil {
			t.Fatalf("initial SaveSnapshot() error = %v", err)
		}
		srv.failPost.Store(100)
		srv.failHead.Store(100)
		ctx, cancel := newCtx()
		defer cancel()
		err := store.SaveSnapshot(ctx, "users", "2_0", json.RawMessage(`{}`))
		if err == nil || !strings.Contains(err.Error(), "save snapshot") {
			t.Fatalf("got %v, want interrupted save", err)
		}
	})

	t.Run("create backoff", func(t *testing.T) {
		srv := newFlakyServer()
		defer srv.Close()
		store := newSnapshotStore(t, srv.URL, "snap-create-backoff", slowRetry)
		srv.failPut.Store(100)
		ctx, cancel := newCtx()
		defer cancel()
		err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{}`))
		if err == nil || !strings.Contains(err.Error(), "save snapshot") {
			t.Fatalf("got %v, want interrupted save", err)
		}
	})

	t.Run("load backoff", func(t *testing.T) {
		srv := newFlakyServer()
		defer srv.Close()
		store := newSnapshotStore(t, srv.URL, "snap-load-backoff", slowRetry)
		if err := store.SaveSnapshot(context.Background(), "users", "1_0", json.RawMessage(`{}`)); err != nil {
			t.Fatalf("SaveSnapshot() error = %v", err)
		}
		srv.failGet.Store(100)
		ctx, cancel := newCtx()
		defer cancel()
		_, _, err := store.LoadSnapshot(ctx, "users")
		if err == nil || !strings.Contains(err.Error(), "load snapshot") {
			t.Fatalf("got %v, want interrupted load", err)
		}
	})
}

func TestSnapshotSaveCancelledAfterCreate(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-cancel-created")

	// Create the companion stream with a healthy context first.
	if err := store.SaveSnapshot(context.Background(), "users", "1_0", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("initial SaveSnapshot() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.SaveSnapshot(ctx, "users", "2_0", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "save snapshot") {
		t.Fatalf("got %v, want cancelled save", err)
	}
}

func TestSnapshotContextCancellation(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	store := newSnapshotStore(t, srv.URL, "snap-cancel")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := store.SaveSnapshot(ctx, "users", "1_0", json.RawMessage(`{}`)); err == nil {
		t.Fatal("SaveSnapshot() with cancelled context should fail")
	}
	if _, _, err := store.LoadSnapshot(ctx, "users"); err == nil {
		t.Fatal("LoadSnapshot() with cancelled context should fail")
	}
}
