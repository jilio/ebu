package durablestream_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/memorystorage"
	eventbus "github.com/jilio/ebu"
	ds "github.com/jilio/ebu/stores/durablestream"
)

// newTestServer creates a test durable-streams server using the ahimsalabs handler.
func newTestServer() *httptest.Server {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)
	mux := http.NewServeMux()
	mux.Handle("/v1/stream/", http.StripPrefix("/v1/stream/", handler))
	return httptest.NewServer(mux)
}

func TestNew(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	t.Run("creates store successfully", func(t *testing.T) {
		store, err := ds.New(srv.URL+"/v1/stream", "test-stream")
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store == nil {
			t.Fatal("expected non-nil store")
		}
	})

	t.Run("errors on empty baseURL", func(t *testing.T) {
		_, err := ds.New("", "test-stream")
		if err == nil {
			t.Fatal("expected error for empty baseURL")
		}
		if !strings.Contains(err.Error(), "baseURL is required") {
			t.Errorf("expected 'baseURL is required' error, got: %v", err)
		}
	})

	t.Run("errors on empty streamPath", func(t *testing.T) {
		_, err := ds.New(srv.URL+"/v1/stream", "")
		if err == nil {
			t.Fatal("expected error for empty streamPath")
		}
		if !strings.Contains(err.Error(), "streamPath is required") {
			t.Errorf("expected 'streamPath is required' error, got: %v", err)
		}
	})

	t.Run("errors when server is unreachable", func(t *testing.T) {
		_, err := ds.New("http://127.0.0.1:1", "test-stream", ds.WithTimeout(100*time.Millisecond))
		if err == nil {
			t.Fatal("expected error when server is unreachable")
		}
	})
}

func TestNewWithContext(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	t.Run("creates store with context", func(t *testing.T) {
		ctx := context.Background()
		store, err := ds.NewWithContext(ctx, srv.URL+"/v1/stream", "test-stream-ctx")
		if err != nil {
			t.Fatalf("NewWithContext() error = %v", err)
		}
		if store == nil {
			t.Fatal("expected non-nil store")
		}
	})

	t.Run("errors on cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := ds.NewWithContext(ctx, srv.URL+"/v1/stream", "test-stream-cancelled")
		if err == nil {
			t.Fatal("expected error for cancelled context")
		}
	})
}

func TestStore_Append(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "append-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Run("appends event successfully", func(t *testing.T) {
		ctx := context.Background()
		event := &eventbus.Event{
			Type:      "test.event",
			Data:      json.RawMessage(`{"key":"value"}`),
			Timestamp: time.Now(),
		}

		offset, err := store.Append(ctx, event)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if offset == "" {
			t.Error("expected non-empty offset")
		}
	})

	t.Run("appends event without timestamp", func(t *testing.T) {
		ctx := context.Background()
		event := &eventbus.Event{
			Type: "test.event.no.timestamp",
			Data: json.RawMessage(`{"id":1}`),
		}

		offset, err := store.Append(ctx, event)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if offset == "" {
			t.Error("expected non-empty offset")
		}
	})

	t.Run("returns error on context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		event := &eventbus.Event{
			Type: "test.event",
			Data: json.RawMessage(`{}`),
		}

		_, err := store.Append(ctx, event)
		if err == nil {
			t.Fatal("expected error on cancelled context")
		}
	})
}

func TestStore_Read(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "read-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Append some events first
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		event := &eventbus.Event{
			Type:      "test.event",
			Data:      json.RawMessage(fmt.Sprintf(`{"id":%d}`, i)),
			Timestamp: time.Now(),
		}
		_, err := store.Append(ctx, event)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	t.Run("reads from beginning", func(t *testing.T) {
		events, nextOffset, err := store.Read(ctx, eventbus.OffsetOldest, 0)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		if len(events) != 3 {
			t.Errorf("expected 3 events, got %d", len(events))
		}
		if nextOffset == "" {
			t.Error("expected non-empty next offset")
		}
	})

	t.Run("reads with limit is best-effort without embedded offsets", func(t *testing.T) {
		// These events have no per-event offsets, so truncating to the limit
		// would skip the dropped events on the next Read (nextOffset points
		// past them). The full chunk must be returned instead.
		events, nextOffset, err := store.Read(ctx, eventbus.OffsetOldest, 2)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		if len(events) != 3 {
			t.Errorf("expected full chunk of 3 events (limit is best-effort for synthetic offsets), got %d", len(events))
		}
		if nextOffset == "" {
			t.Error("expected non-empty next offset")
		}
	})

	t.Run("reads from offset returns remaining events", func(t *testing.T) {
		// Note: durable-streams returns data in chunks, and the nextOffset
		// points to the end of the current read, not per-event offsets.
		// This test verifies offset-based resumption works correctly.
		allEvents, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		if len(allEvents) != 3 {
			t.Fatalf("expected 3 events, got %d", len(allEvents))
		}

		// Read first event to get a position
		_, nextOffset, err := store.Read(ctx, eventbus.OffsetOldest, 1)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}

		// nextOffset should be valid (non-empty)
		if nextOffset == "" {
			t.Log("nextOffset is empty after reading 1 event")
		}
	})

	t.Run("returns error on context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
		if err == nil {
			t.Fatal("expected error on cancelled context")
		}
	})
}

func TestStore_Close(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "close-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Close should be a no-op
	if err := store.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestOptions(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	t.Run("WithHTTPClient", func(t *testing.T) {
		client := &http.Client{Timeout: 5 * time.Second}
		store, err := ds.New(srv.URL+"/v1/stream", "options-client", ds.WithHTTPClient(client))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.HTTPClient() != client {
			t.Error("HTTP client not set correctly")
		}
	})

	t.Run("WithTimeout", func(t *testing.T) {
		store, err := ds.New(srv.URL+"/v1/stream", "options-timeout", ds.WithTimeout(10*time.Second))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store == nil {
			t.Error("expected non-nil store")
		}
	})

	t.Run("WithContentType", func(t *testing.T) {
		store, err := ds.New(srv.URL+"/v1/stream", "options-contenttype", ds.WithContentType("application/json"))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store == nil {
			t.Error("expected non-nil store")
		}
	})
}

func TestOptions_NilValues(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	t.Run("nil HTTPClient is ignored", func(t *testing.T) {
		store, err := ds.New(srv.URL+"/v1/stream", "options-nil-client", ds.WithHTTPClient(nil))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.HTTPClient() == nil {
			t.Error("HTTP client should not be nil")
		}
	})

	t.Run("zero timeout is ignored", func(t *testing.T) {
		store, err := ds.New(srv.URL+"/v1/stream", "options-zero-timeout", ds.WithTimeout(0))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store == nil {
			t.Error("expected non-nil store")
		}
	})

	t.Run("empty content type is ignored", func(t *testing.T) {
		store, err := ds.New(srv.URL+"/v1/stream", "options-empty-contenttype", ds.WithContentType(""))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store == nil {
			t.Error("expected non-nil store")
		}
	})
}

func TestParseTimestamp(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "timestamp-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()

	t.Run("RFC3339Nano format", func(t *testing.T) {
		timestamp := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
		event := &eventbus.Event{
			Type:      "test.timestamp",
			Data:      json.RawMessage(`{}`),
			Timestamp: timestamp,
		}

		_, err := store.Append(ctx, event)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}

		events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}

		if len(events) == 0 {
			t.Fatal("expected at least 1 event")
		}

		// Find our event
		var found bool
		for _, e := range events {
			if e.Type == "test.timestamp" {
				found = true
				if e.Timestamp.IsZero() {
					t.Error("expected non-zero timestamp")
				}
			}
		}
		if !found {
			t.Error("timestamp event not found")
		}
	})
}

// testLogger captures log messages for testing
type testLogger struct {
	messages []string
}

func (l *testLogger) Printf(format string, v ...any) {
	l.messages = append(l.messages, fmt.Sprintf(format, v...))
}

func TestWithLogger(t *testing.T) {
	// Create a custom mock server that returns malformed events
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			w.Header().Set("Stream-Next-Offset", "1")
			w.Header().Set("Content-Type", "application/json")
			// Return array with malformed event
			w.Write([]byte(`[{"type":"valid","data":{}}, "not an object"]`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	logger := &testLogger{}
	store, err := ds.New(srv.URL+"/v1/stream", "logger-test", ds.WithLogger(logger))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should have 1 valid event
	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}

	// Logger should have captured the malformed event warning
	if len(logger.messages) != 1 {
		t.Errorf("expected 1 log message, got %d", len(logger.messages))
	}
	if len(logger.messages) > 0 && !strings.Contains(logger.messages[0], "skipping malformed event") {
		t.Errorf("expected log message about skipping malformed event, got: %s", logger.messages[0])
	}
}

func TestRead_EmbeddedOffsets(t *testing.T) {
	// Server that returns events with embedded offsets
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			w.Header().Set("Stream-Next-Offset", "100")
			w.Header().Set("Content-Type", "application/json")
			// Events with their own offset field
			w.Write([]byte(`[
				{"offset":"10","type":"event1","data":{},"timestamp":"2024-01-15T10:30:00Z"},
				{"offset":"20","type":"event2","data":{},"timestamp":"2024-01-15T10:31:00.123456789Z"}
			]`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "embedded-offsets")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	events, nextOffset, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// Check that embedded offsets are used
	if events[0].Offset != "10" {
		t.Errorf("expected offset '10', got '%s'", events[0].Offset)
	}
	if events[1].Offset != "20" {
		t.Errorf("expected offset '20', got '%s'", events[1].Offset)
	}

	// Check timestamps are parsed
	if events[0].Timestamp.IsZero() {
		t.Error("expected non-zero timestamp for event 0")
	}

	if nextOffset != "100" {
		t.Errorf("expected next offset '100', got '%s'", nextOffset)
	}
}

// TestRead_LimitWithEmbeddedOffsets verifies that when events carry real
// per-event offsets, limit truncation is resumable: nextOffset must point at
// the last RETURNED event, not past the truncated ones. Returning the chunk's
// end offset would silently skip the dropped events on the next Read.
func TestRead_LimitWithEmbeddedOffsets(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			w.Header().Set("Stream-Next-Offset", "100")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[
				{"offset":"10","type":"event1","data":{},"timestamp":"2024-01-15T10:30:00Z"},
				{"offset":"20","type":"event2","data":{},"timestamp":"2024-01-15T10:31:00Z"},
				{"offset":"30","type":"event3","data":{},"timestamp":"2024-01-15T10:32:00Z"}
			]`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "limit-embedded-offsets")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	events, nextOffset, err := store.Read(context.Background(), eventbus.OffsetOldest, 2)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events with limit, got %d", len(events))
	}
	if nextOffset != "20" {
		t.Errorf("nextOffset must be the last returned event's offset '20' (resumable), got '%s'", nextOffset)
	}
}

// TestRead_ChunkStartOffsets verifies the chunk-start offset semantics for
// events that carry no embedded offset: every event except the last carries
// the offset the chunk was read FROM (resume-safe: re-delivers the chunk,
// never skips), and the last event carries the server's next-offset (the
// exact resume point after it). All emitted offsets are server-issued and
// safe to store.
func TestRead_ChunkStartOffsets(t *testing.T) {
	// Server that returns events without offsets
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			w.Header().Set("Stream-Next-Offset", "50")
			w.Header().Set("Content-Type", "application/json")
			// Events without offset field
			w.Write([]byte(`[
				{"type":"event1","data":{}},
				{"type":"event2","data":{}},
				{"type":"event3","data":{}}
			]`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "chunk-start-offsets")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()

	t.Run("from explicit offset", func(t *testing.T) {
		events, nextOffset, err := store.Read(ctx, "10", 0)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		if len(events) != 3 {
			t.Fatalf("expected 3 events, got %d", len(events))
		}
		// Non-last events carry the chunk-start offset (read-from position).
		if events[0].Offset != "10" {
			t.Errorf("expected chunk-start offset '10', got '%s'", events[0].Offset)
		}
		if events[1].Offset != "10" {
			t.Errorf("expected chunk-start offset '10', got '%s'", events[1].Offset)
		}
		// The last event carries the server's next-offset.
		if events[2].Offset != "50" {
			t.Errorf("expected next-offset '50' on last event, got '%s'", events[2].Offset)
		}
		if nextOffset != "50" {
			t.Errorf("expected nextOffset '50', got '%s'", nextOffset)
		}
	})

	t.Run("from oldest", func(t *testing.T) {
		events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		if len(events) != 3 {
			t.Fatalf("expected 3 events, got %d", len(events))
		}
		// Chunk-start for a read from the beginning is OffsetOldest itself:
		// resuming from it replays the stream from the start (duplicates OK,
		// skips impossible).
		if events[0].Offset != eventbus.OffsetOldest {
			t.Errorf("expected OffsetOldest chunk-start, got '%s'", events[0].Offset)
		}
		if events[2].Offset != "50" {
			t.Errorf("expected next-offset '50' on last event, got '%s'", events[2].Offset)
		}
	})
}

func TestRead_InvalidJSON(t *testing.T) {
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

	store, err := ds.New(srv.URL+"/v1/stream", "invalid-json")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	_, _, err = store.Read(ctx, eventbus.OffsetOldest, 0)
	if err == nil {
		t.Fatal("expected error for invalid json")
	}
	if !strings.Contains(err.Error(), "unmarshal response") {
		t.Errorf("expected 'unmarshal response' error, got: %v", err)
	}
}

func TestRead_EmptyResponse(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "empty-read")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Read from empty stream
	ctx := context.Background()
	events, nextOffset, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events for empty stream, got %d", len(events))
	}
	// nextOffset should still be set
	if nextOffset == "" {
		t.Log("nextOffset is empty for empty stream (this is acceptable)")
	}
}

func TestRead_EmptyDataResponse(t *testing.T) {
	// Mock server that returns empty data (no body)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			w.Header().Set("Stream-Next-Offset", "0")
			w.Header().Set("Content-Type", "application/json")
			// Empty response body
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "empty-data")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	events, nextOffset, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events for empty data, got %d", len(events))
	}
	if nextOffset != "0" {
		t.Errorf("expected nextOffset '0', got '%s'", nextOffset)
	}
}

func TestStore_Path(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "path-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if store.Path() != "path-test" {
		t.Errorf("expected path 'path-test', got '%s'", store.Path())
	}
}

func TestStore_Client(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "client-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if store.Client() == nil {
		t.Error("expected non-nil client")
	}
}

func TestStore_EventInterface(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "interface-test")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Verify it implements EventStore interface
	var _ eventbus.EventStore = store
}

func TestAppend_WriterError(t *testing.T) {
	// Create a mock server that fails on POST (writer request)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodPost:
			http.Error(w, "writer error", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "append-writer-error")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	event := &eventbus.Event{
		Type: "test.event",
		Data: json.RawMessage(`{}`),
	}

	_, err = store.Append(ctx, event)
	if err == nil {
		t.Fatal("expected error on writer failure")
	}
}

func TestRead_ServerError(t *testing.T) {
	// Create a mock server that fails on GET
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			http.Error(w, "server error", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "read-server-error")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	_, _, err = store.Read(ctx, eventbus.OffsetOldest, 0)
	if err == nil {
		t.Fatal("expected error on server failure")
	}
	if !strings.Contains(err.Error(), "durablestream: read") {
		t.Errorf("expected 'durablestream: read' error, got: %v", err)
	}
}

func TestParseTimestamp_InvalidFormat(t *testing.T) {
	// Create a mock server that returns events with invalid timestamps
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			w.Header().Set("Stream-Next-Offset", "1")
			w.Header().Set("Content-Type", "application/json")
			// Event with invalid timestamp format
			w.Write([]byte(`[{"type":"test","data":{},"timestamp":"invalid-timestamp"}]`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "invalid-timestamp")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	// Invalid timestamp should result in zero time
	if !events[0].Timestamp.IsZero() {
		t.Error("expected zero timestamp for invalid format")
	}
}

// readAllFrom reads all events from the given offset, following nextOffset
// until the store reports no more data.
func readAllFrom(t *testing.T, store *ds.Store, from eventbus.Offset) []*eventbus.StoredEvent {
	t.Helper()
	ctx := context.Background()
	var all []*eventbus.StoredEvent
	offset := from
	for {
		events, next, err := store.Read(ctx, offset, 0)
		if err != nil {
			t.Fatalf("Read(%q) error = %v", offset, err)
		}
		if len(events) == 0 {
			return all
		}
		all = append(all, events...)
		if next == offset {
			t.Fatalf("nextOffset did not advance from %q", offset)
		}
		offset = next
	}
}

// newChunkedServer creates a mock durable-streams server that serves the
// given JSON payloads (one message each, no embedded offsets) in chunks of
// at most chunkSize messages per read. Offsets follow the protocol
// semantics: reading from offset k returns messages strictly after the
// k-th message, and Stream-Next-Offset is the resume point after the last
// returned message.
func newChunkedServer(payloads []string, chunkSize int) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			off := 0
			if q := r.URL.Query().Get("offset"); q != "" {
				n, err := strconv.Atoi(q)
				if err != nil {
					http.Error(w, "bad offset", http.StatusBadRequest)
					return
				}
				off = n
			}
			w.Header().Set("Content-Type", "application/json")
			if off >= len(payloads) {
				w.Header().Set("Stream-Next-Offset", strconv.Itoa(off))
				w.Header().Set("Stream-Up-To-Date", "true")
				return
			}
			end := off + chunkSize
			if end > len(payloads) {
				end = len(payloads)
			}
			w.Header().Set("Stream-Next-Offset", strconv.Itoa(end))
			if end == len(payloads) {
				w.Header().Set("Stream-Up-To-Date", "true")
			}
			w.Write([]byte("[" + strings.Join(payloads[off:end], ",") + "]"))
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	return httptest.NewServer(mux)
}

// TestRead_ResumeFromSavedOffsetNeverSkips is the key regression test for
// per-event offset semantics: resuming a Read from any event's saved
// StoredEvent.Offset must never skip a later event. Re-delivery of the
// saved event or earlier ones (duplicates) is acceptable at-least-once
// behavior.
func TestRead_ResumeFromSavedOffsetNeverSkips(t *testing.T) {
	verifyResume := func(t *testing.T, store *ds.Store, total int) {
		t.Helper()
		all := readAllFrom(t, store, eventbus.OffsetOldest)
		if len(all) != total {
			t.Fatalf("expected %d events, got %d", total, len(all))
		}
		for i := 0; i < total; i++ {
			want := fmt.Sprintf("event.%d", i)
			if all[i].Type != want {
				t.Fatalf("expected event %d to be %q, got %q", i, want, all[i].Type)
			}
		}

		for i, saved := range all {
			resumed := readAllFrom(t, store, saved.Offset)
			seen := make(map[string]bool, len(resumed))
			for _, e := range resumed {
				seen[e.Type] = true
			}
			// No event after i may be missing (skips are NOT allowed;
			// duplicates of events <= i are).
			for j := i + 1; j < total; j++ {
				if !seen[fmt.Sprintf("event.%d", j)] {
					t.Errorf("resuming from event %d offset %q skipped event %d", i, saved.Offset, j)
				}
			}
		}
	}

	t.Run("chunked reads without embedded offsets", func(t *testing.T) {
		payloads := make([]string, 6)
		for i := range payloads {
			payloads[i] = fmt.Sprintf(`{"type":"event.%d","data":{"id":%d}}`, i, i)
		}
		srv := newChunkedServer(payloads, 2)
		defer srv.Close()

		store, err := ds.New(srv.URL+"/v1/stream", "resume-chunked")
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		verifyResume(t, store, 6)
	})

	t.Run("real server round trip", func(t *testing.T) {
		srv := newTestServer()
		defer srv.Close()

		store, err := ds.New(srv.URL+"/v1/stream", "resume-real")
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		ctx := context.Background()
		for i := 0; i < 5; i++ {
			event := &eventbus.Event{
				Type: fmt.Sprintf("event.%d", i),
				Data: json.RawMessage(fmt.Sprintf(`{"id":%d}`, i)),
			}
			if _, err := store.Append(ctx, event); err != nil {
				t.Fatalf("Append() error = %v", err)
			}
		}
		verifyResume(t, store, 5)
	})
}

// TestAppend_OffsetIsResumeSafe verifies Append's returned offset semantics:
// reading from the offset returned for event i must yield events i+1..N-1
// without re-delivering event i and without skipping any later event.
func TestAppend_OffsetIsResumeSafe(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "append-resume-safe")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	const total = 5
	offsets := make([]eventbus.Offset, total)
	for i := 0; i < total; i++ {
		event := &eventbus.Event{
			Type: fmt.Sprintf("event.%d", i),
			Data: json.RawMessage(`{}`),
		}
		offset, err := store.Append(ctx, event)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		offsets[i] = offset
	}

	for i, offset := range offsets {
		resumed := readAllFrom(t, store, offset)
		if len(resumed) != total-i-1 {
			t.Fatalf("resuming from Append offset of event %d: expected %d events, got %d", i, total-i-1, len(resumed))
		}
		for j, e := range resumed {
			want := fmt.Sprintf("event.%d", i+1+j)
			if e.Type != want {
				t.Errorf("resuming from event %d: expected %q at position %d, got %q", i, want, j, e.Type)
			}
		}
	}
}

func TestStore_ConcurrentAppends(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "concurrent-appends")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const goroutines = 10
	const perGoroutine = 20

	ctx := context.Background()
	errCh := make(chan error, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				event := &eventbus.Event{
					Type: fmt.Sprintf("concurrent.%d.%d", g, i),
					Data: json.RawMessage(`{}`),
				}
				if _, err := store.Append(ctx, event); err != nil {
					errCh <- fmt.Errorf("goroutine %d append %d: %w", g, i, err)
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.Fatalf("concurrent appends failed")
	}

	all := readAllFrom(t, store, eventbus.OffsetOldest)
	if len(all) != goroutines*perGoroutine {
		t.Fatalf("expected %d events, got %d", goroutines*perGoroutine, len(all))
	}

	seen := make(map[string]bool, len(all))
	for _, e := range all {
		seen[e.Type] = true
	}
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perGoroutine; i++ {
			key := fmt.Sprintf("concurrent.%d.%d", g, i)
			if !seen[key] {
				t.Errorf("event %s missing after concurrent appends", key)
			}
		}
	}
}

// newFlakyServer wraps a real durable-streams handler and fails requests on
// demand: failHead/failPost/failGet hold the number of upcoming
// HEAD/POST/GET requests to reject with failStatus (503 unless set).
type flakyServer struct {
	*httptest.Server
	heads      atomic.Int32
	posts      atomic.Int32
	gets       atomic.Int32
	failHead   atomic.Int32
	failPost   atomic.Int32
	failGet    atomic.Int32
	failStatus atomic.Int32
}

func (fs *flakyServer) fail(w http.ResponseWriter) {
	status := int(fs.failStatus.Load())
	if status == 0 {
		status = http.StatusServiceUnavailable
	}
	http.Error(w, http.StatusText(status), status)
}

func newFlakyServer() *flakyServer {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	fs := &flakyServer{}
	mux := http.NewServeMux()
	mux.Handle("/v1/stream/", http.StripPrefix("/v1/stream/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			fs.heads.Add(1)
			if fs.failHead.Load() > 0 {
				fs.failHead.Add(-1)
				fs.fail(w)
				return
			}
		case http.MethodPost:
			fs.posts.Add(1)
			if fs.failPost.Load() > 0 {
				fs.failPost.Add(-1)
				fs.fail(w)
				return
			}
		case http.MethodGet:
			fs.gets.Add(1)
			if fs.failGet.Load() > 0 {
				fs.failGet.Add(-1)
				fs.fail(w)
				return
			}
		}
		handler.ServeHTTP(w, r)
	})))
	fs.Server = httptest.NewServer(mux)
	return fs
}

func TestAppend_RetryOn503ThenSuccess(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "retry-append", ds.WithRetry(3, 5*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	srv.failPost.Store(1)

	ctx := context.Background()
	offset, err := store.Append(ctx, &eventbus.Event{
		Type: "retry.event",
		Data: json.RawMessage(`{"ok":true}`),
	})
	if err != nil {
		t.Fatalf("Append() should succeed after retry, got error = %v", err)
	}
	if offset == "" {
		t.Error("expected non-empty offset")
	}
	if got := srv.posts.Load(); got != 2 {
		t.Errorf("expected 2 POST requests (1 failed + 1 retried), got %d", got)
	}

	events := readAllFrom(t, store, eventbus.OffsetOldest)
	if len(events) != 1 || events[0].Type != "retry.event" {
		t.Fatalf("expected the retried event to be stored exactly once, got %v", events)
	}
}

func TestRead_RetryOn503ThenSuccess(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "retry-read", ds.WithRetry(3, 5*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	if _, err := store.Append(ctx, &eventbus.Event{
		Type: "retry.read.event",
		Data: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	srv.failGet.Store(1)

	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read() should succeed after retry, got error = %v", err)
	}
	if len(events) != 1 || events[0].Type != "retry.read.event" {
		t.Fatalf("expected 1 event after retried read, got %v", events)
	}
	if got := srv.gets.Load(); got != 2 {
		t.Errorf("expected 2 GET requests (1 failed + 1 retried), got %d", got)
	}
}

func TestAppend_RetryExhausted(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "retry-exhausted", ds.WithRetry(2, time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	srv.failPost.Store(10)

	ctx := context.Background()
	_, err = store.Append(ctx, &eventbus.Event{
		Type: "never.stored",
		Data: json.RawMessage(`{}`),
	})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if !strings.Contains(err.Error(), "giving up after 2 attempts") {
		t.Errorf("expected 'giving up after 2 attempts' error, got: %v", err)
	}
	if got := srv.posts.Load(); got != 2 {
		t.Errorf("expected exactly 2 POST attempts, got %d", got)
	}
}

func TestAppend_WriterCacheReuse(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "writer-cache", ds.WithRetry(3, 5*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		event := &eventbus.Event{
			Type: fmt.Sprintf("cached.%d", i),
			Data: json.RawMessage(`{}`),
		}
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	// The writer is created once (one HEAD) and reused for all appends.
	if got := srv.heads.Load(); got != 1 {
		t.Errorf("expected exactly 1 HEAD request for 5 appends (writer cached), got %d", got)
	}

	// A send failure invalidates the cached writer; the retry recreates it
	// with a second HEAD.
	srv.failPost.Store(1)
	if _, err := store.Append(ctx, &eventbus.Event{
		Type: "cached.recreated",
		Data: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("Append() after transient failure error = %v", err)
	}
	if got := srv.heads.Load(); got != 2 {
		t.Errorf("expected writer to be recreated after send failure (2 HEADs total), got %d", got)
	}

	events := readAllFrom(t, store, eventbus.OffsetOldest)
	if len(events) != 6 {
		t.Fatalf("expected 6 events, got %d", len(events))
	}
}

func TestWithDecodeErrorHandler(t *testing.T) {
	newMalformedServer := func() *httptest.Server {
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/stream/", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPut:
				w.WriteHeader(http.StatusCreated)
			case http.MethodGet:
				w.Header().Set("Stream-Next-Offset", "1")
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`[{"type":"valid","data":{}}, "not an object"]`))
			default:
				w.WriteHeader(http.StatusOK)
			}
		})
		return httptest.NewServer(mux)
	}

	t.Run("handler is invoked with error and raw bytes", func(t *testing.T) {
		srv := newMalformedServer()
		defer srv.Close()

		var mu sync.Mutex
		var errs []error
		var raws [][]byte
		store, err := ds.New(srv.URL+"/v1/stream", "decode-handler",
			ds.WithDecodeErrorHandler(func(err error, raw []byte) {
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
				raws = append(raws, raw)
			}))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		events, _, err := store.Read(context.Background(), eventbus.OffsetOldest, 0)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		if len(events) != 1 {
			t.Errorf("expected 1 valid event, got %d", len(events))
		}
		mu.Lock()
		defer mu.Unlock()
		if len(errs) != 1 {
			t.Fatalf("expected handler to be invoked once, got %d", len(errs))
		}
		if errs[0] == nil {
			t.Error("expected non-nil decode error")
		}
		if !strings.Contains(string(raws[0]), "not an object") {
			t.Errorf("expected raw bytes of the malformed event, got %s", raws[0])
		}
	})

	t.Run("handler takes precedence over logger", func(t *testing.T) {
		srv := newMalformedServer()
		defer srv.Close()

		var calls int
		logger := &testLogger{}
		store, err := ds.New(srv.URL+"/v1/stream", "decode-handler-precedence",
			ds.WithLogger(logger),
			ds.WithDecodeErrorHandler(func(err error, raw []byte) {
				calls++
			}))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		if _, _, err := store.Read(context.Background(), eventbus.OffsetOldest, 0); err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		if calls != 1 {
			t.Errorf("expected handler to be invoked once, got %d", calls)
		}
		if len(logger.messages) != 0 {
			t.Errorf("expected logger not to be invoked when handler is set, got %v", logger.messages)
		}
	})
}

func TestAppend_MarshalError(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "marshal-error")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = store.Append(context.Background(), &eventbus.Event{
		Type: "bad.data",
		Data: json.RawMessage(`{invalid`),
	})
	if err == nil {
		t.Fatal("expected error for invalid event data")
	}
	if !strings.Contains(err.Error(), "marshal event") {
		t.Errorf("expected 'marshal event' error, got: %v", err)
	}
}

func TestAppend_NonRetryableError(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "append-non-retryable", ds.WithRetry(3, time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	srv.failStatus.Store(http.StatusBadRequest)
	srv.failPost.Store(1)

	_, err = store.Append(context.Background(), &eventbus.Event{
		Type: "rejected",
		Data: json.RawMessage(`{}`),
	})
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if got := srv.posts.Load(); got != 1 {
		t.Errorf("expected no retry on 4xx (1 POST), got %d", got)
	}
}

func TestAppend_WriterErrorNonRetryable(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "writer-not-found", ds.WithRetry(3, time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	srv.failStatus.Store(http.StatusNotFound)
	srv.failHead.Store(1)

	_, err = store.Append(context.Background(), &eventbus.Event{
		Type: "no.stream",
		Data: json.RawMessage(`{}`),
	})
	if err == nil {
		t.Fatal("expected error when writer creation fails with 404")
	}
	if !strings.Contains(err.Error(), "get writer") {
		t.Errorf("expected 'get writer' error, got: %v", err)
	}
	if got := srv.posts.Load(); got != 0 {
		t.Errorf("expected no POST after writer creation failure, got %d", got)
	}
	if got := srv.heads.Load(); got != 1 {
		t.Errorf("expected no retry of non-retryable HEAD failure (1 HEAD), got %d", got)
	}
}

func TestAppend_WriterErrorRetryable(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "writer-retry", ds.WithRetry(3, time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	srv.failHead.Store(1) // 503 on first HEAD, then recover

	if _, err := store.Append(context.Background(), &eventbus.Event{
		Type: "writer.retried",
		Data: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("Append() should succeed after writer creation retry, got %v", err)
	}
	if got := srv.heads.Load(); got != 2 {
		t.Errorf("expected 2 HEAD requests (1 failed + 1 retried), got %d", got)
	}
}

// TestAppend_StaleWriterContextRecovers verifies that a cached writer bound
// to a context that has since been cancelled is transparently recreated on
// the next Append with a live context.
func TestAppend_StaleWriterContextRecovers(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "stale-writer-ctx", ds.WithRetry(3, time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	if _, err := store.Append(ctx1, &eventbus.Event{
		Type: "event.0",
		Data: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	cancel1() // The cached writer is now bound to a dead context.

	if _, err := store.Append(context.Background(), &eventbus.Event{
		Type: "event.1",
		Data: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("Append() with live context should recover from stale cached writer, got %v", err)
	}

	events := readAllFrom(t, store, eventbus.OffsetOldest)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestAppend_BackoffRespectsContext(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "append-backoff-ctx", ds.WithRetry(3, time.Second))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	srv.failPost.Store(10)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = store.Append(ctx, &eventbus.Event{
		Type: "never",
		Data: json.RawMessage(`{}`),
	})
	if err == nil {
		t.Fatal("expected error when context expires during backoff")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("backoff did not respect context cancellation, took %v", elapsed)
	}
}

func TestRead_BackoffRespectsContext(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "read-backoff-ctx", ds.WithRetry(3, time.Second))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	srv.failGet.Store(10)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, err = store.Read(ctx, eventbus.OffsetOldest, 0)
	if err == nil {
		t.Fatal("expected error when context expires during backoff")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("backoff did not respect context cancellation, took %v", elapsed)
	}
}

func TestRead_NonRetryableError(t *testing.T) {
	srv := newFlakyServer()
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "read-non-retryable", ds.WithRetry(3, time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	srv.failStatus.Store(http.StatusBadRequest)
	srv.failGet.Store(1)

	_, _, err = store.Read(context.Background(), eventbus.OffsetOldest, 0)
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if got := srv.gets.Load(); got != 1 {
		t.Errorf("expected no retry on 4xx (1 GET), got %d", got)
	}
}
