package durablestream_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	durablestreams "github.com/durable-streams/durable-streams/packages/client-go"
	eventbus "github.com/jilio/ebu"
	ds "github.com/jilio/ebu/stores/durablestream"
)

// retentionServer answers reads from any offset but the start of the stream
// with 410 Gone, like a server whose retention dropped the stream's head.
func retentionServer(t *testing.T) *protoServer {
	t.Helper()
	ps := newProtoServer()
	t.Cleanup(ps.Close)
	inner := ps.Config.Handler
	ps.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && !durablestreams.Offset(r.URL.Query().Get("offset")).IsStart() && r.URL.Query().Get("offset") != "" {
			http.Error(w, "offset before retention window", http.StatusGone)
			return
		}
		inner.ServeHTTP(w, r)
	})
	return ps
}

func TestReadFromTrimmedOffsetReportsTheGap(t *testing.T) {
	ps := retentionServer(t)
	store, err := ds.New(ps.URL+"/v1/stream", "events", ds.WithRetry(2, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), &eventbus.Event{Type: "retained", Data: json.RawMessage(`1`)}); err != nil {
		t.Fatal(err)
	}

	_, next, err := store.Read(t.Context(), eventbus.Offset(padOffset(0)), 0)
	var trimmed *eventbus.OffsetTrimmedError
	if !errors.As(err, &trimmed) || !errors.Is(err, eventbus.ErrOffsetTrimmed) || !errors.Is(err, durablestreams.ErrOffsetGone) {
		t.Fatalf("Read from a trimmed offset = %v", err)
	}
	if trimmed.Offset != eventbus.Offset(padOffset(0)) || trimmed.ResumeFrom != eventbus.OffsetOldest || next != eventbus.Offset(padOffset(0)) {
		t.Fatalf("trimmed = %+v, next %q", trimmed, next)
	}
	if events, _, err := store.Read(t.Context(), trimmed.ResumeFrom, 0); err != nil || len(events) != 1 {
		t.Fatalf("Read from ResumeFrom = %d events, %v", len(events), err)
	}

	for event, err := range store.Tail(t.Context(), eventbus.Offset(padOffset(0))) {
		if event != nil || !errors.Is(err, eventbus.ErrOffsetTrimmed) {
			t.Fatalf("Tail from a trimmed offset = %v, %v", event, err)
		}
	}
}

func TestReadGoneFromTheStartIsNotATrim(t *testing.T) {
	ps := newProtoServer()
	t.Cleanup(ps.Close)
	inner := ps.Config.Handler
	ps.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.Error(w, "soft-deleted", http.StatusGone)
			return
		}
		inner.ServeHTTP(w, r)
	})
	store, err := ds.New(ps.URL+"/v1/stream", "events", ds.WithRetry(1, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Read(t.Context(), eventbus.OffsetOldest, 0)
	if !errors.Is(err, durablestreams.ErrOffsetGone) || errors.Is(err, eventbus.ErrOffsetTrimmed) {
		t.Fatalf("Read(OffsetOldest) on a gone stream = %v", err)
	}
}
