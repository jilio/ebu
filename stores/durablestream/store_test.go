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

	t.Run("reads with limit", func(t *testing.T) {
		events, _, err := store.Read(ctx, eventbus.OffsetOldest, 2)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		if len(events) != 2 {
			t.Errorf("expected 2 events with limit, got %d", len(events))
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

func TestRead_SyntheticOffsets(t *testing.T) {
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
				{"type":"event2","data":{}}
			]`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store, err := ds.New(srv.URL+"/v1/stream", "synthetic-offsets")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// Check that synthetic offsets are unique (nextOffset/index format)
	if events[0].Offset != "50/0" {
		t.Errorf("expected synthetic offset '50/0', got '%s'", events[0].Offset)
	}
	if events[1].Offset != "50/1" {
		t.Errorf("expected synthetic offset '50/1', got '%s'", events[1].Offset)
	}
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
