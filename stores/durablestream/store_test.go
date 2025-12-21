package durablestream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

// mockServer creates a test server that simulates a durable-streams server.
type mockServer struct {
	events     []json.RawMessage
	nextOffset int64
	createErr  bool
	appendErr  bool
	readErr    bool
}

func newMockServer() *mockServer {
	return &mockServer{
		events: make([]json.RawMessage, 0),
	}
}

func (m *mockServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		m.handleCreate(w, r)
	case http.MethodPost:
		m.handleAppend(w, r)
	case http.MethodGet:
		m.handleRead(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *mockServer) handleCreate(w http.ResponseWriter, r *http.Request) {
	if m.createErr {
		http.Error(w, "create error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Stream-Next-Offset", "0")
	w.WriteHeader(http.StatusCreated)
}

func (m *mockServer) handleAppend(w http.ResponseWriter, r *http.Request) {
	if m.appendErr {
		http.Error(w, "append error", http.StatusInternalServerError)
		return
	}

	var events []json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	m.events = append(m.events, events...)
	atomic.AddInt64(&m.nextOffset, int64(len(events)))

	w.Header().Set("Stream-Next-Offset", offsetString(atomic.LoadInt64(&m.nextOffset)))
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockServer) handleRead(w http.ResponseWriter, r *http.Request) {
	if m.readErr {
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}

	offset := r.URL.Query().Get("offset")

	// Parse offset to determine starting position
	startIdx := 0
	if offset != "" && offset != "-1" {
		// Simple numeric offset parsing for tests
		for i, c := range offset {
			if c < '0' || c > '9' {
				break
			}
			startIdx = startIdx*10 + int(c-'0')
			if i == len(offset)-1 {
				startIdx++ // Start after this offset
			}
		}
	}

	// Get events from start index
	var result []json.RawMessage
	if startIdx < len(m.events) {
		result = m.events[startIdx:]
	}

	w.Header().Set("Stream-Next-Offset", offsetString(atomic.LoadInt64(&m.nextOffset)))
	w.Header().Set("Content-Type", "application/json")

	if len(result) == 0 {
		w.Header().Set("Stream-Up-To-Date", "true")
		w.Write([]byte("[]"))
		return
	}

	data, _ := json.Marshal(result)
	w.Write(data)
}

func offsetString(n int64) string {
	if n == 0 {
		return "0"
	}
	result := make([]byte, 0, 20)
	for n > 0 {
		result = append([]byte{byte(n%10) + '0'}, result...)
		n /= 10
	}
	return string(result)
}

func TestNew(t *testing.T) {
	mock := newMockServer()
	srv := httptest.NewServer(mock)
	defer srv.Close()

	t.Run("creates store successfully", func(t *testing.T) {
		store, err := New(srv.URL)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store == nil {
			t.Fatal("expected non-nil store")
		}
	})

	t.Run("errors on empty URL", func(t *testing.T) {
		_, err := New("")
		if err == nil {
			t.Fatal("expected error for empty URL")
		}
	})

	t.Run("errors when server fails to create stream", func(t *testing.T) {
		mock.createErr = true
		defer func() { mock.createErr = false }()

		_, err := New(srv.URL)
		if err == nil {
			t.Fatal("expected error when create fails")
		}
	})
}

func TestStore_Append(t *testing.T) {
	mock := newMockServer()
	srv := httptest.NewServer(mock)
	defer srv.Close()

	store, err := New(srv.URL)
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

	t.Run("returns error on server failure", func(t *testing.T) {
		mock.appendErr = true
		defer func() { mock.appendErr = false }()

		ctx := context.Background()
		event := &eventbus.Event{
			Type: "test.event",
			Data: json.RawMessage(`{}`),
		}

		_, err := store.Append(ctx, event)
		if err == nil {
			t.Fatal("expected error on server failure")
		}
	})
}

func TestStore_Read(t *testing.T) {
	mock := newMockServer()
	srv := httptest.NewServer(mock)
	defer srv.Close()

	store, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Append some events first
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		event := &eventbus.Event{
			Type:      "test.event",
			Data:      json.RawMessage(`{"id":` + offsetString(int64(i)) + `}`),
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

	t.Run("returns empty for out of range offset", func(t *testing.T) {
		events, _, err := store.Read(ctx, eventbus.Offset("999"), 0)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		if len(events) != 0 {
			t.Errorf("expected 0 events for out of range offset, got %d", len(events))
		}
	})

	t.Run("returns error on server failure", func(t *testing.T) {
		mock.readErr = true
		defer func() { mock.readErr = false }()

		_, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
		if err == nil {
			t.Fatal("expected error on server failure")
		}
	})
}

func TestStore_Close(t *testing.T) {
	mock := newMockServer()
	srv := httptest.NewServer(mock)
	defer srv.Close()

	store, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Close should be a no-op
	if err := store.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestOptions(t *testing.T) {
	mock := newMockServer()
	srv := httptest.NewServer(mock)
	defer srv.Close()

	t.Run("WithHTTPClient", func(t *testing.T) {
		client := &http.Client{Timeout: 5 * time.Second}
		store, err := New(srv.URL, WithHTTPClient(client))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.client.httpClient != client {
			t.Error("HTTP client not set correctly")
		}
	})

	t.Run("WithTimeout", func(t *testing.T) {
		store, err := New(srv.URL, WithTimeout(10*time.Second))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.cfg.timeout != 10*time.Second {
			t.Error("timeout not set correctly")
		}
	})

	t.Run("WithRetry", func(t *testing.T) {
		store, err := New(srv.URL, WithRetry(5, 200*time.Millisecond))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.cfg.retryAttempts != 5 {
			t.Error("retry attempts not set correctly")
		}
		if store.cfg.retryBackoff != 200*time.Millisecond {
			t.Error("retry backoff not set correctly")
		}
	})

	t.Run("WithContentType", func(t *testing.T) {
		store, err := New(srv.URL, WithContentType("text/plain"))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.cfg.contentType != "text/plain" {
			t.Error("content type not set correctly")
		}
	})
}

func TestClient_Retry(t *testing.T) {
	failCount := 0
	maxFails := 2

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			if failCount < maxFails {
				failCount++
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store, err := New(srv.URL, WithRetry(3, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v, expected success after retries", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store after retries")
	}
	if failCount != maxFails {
		t.Errorf("expected %d failures before success, got %d", maxFails, failCount)
	}
}

func TestClient_ContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Slow response
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := defaultConfig()
	cfg.timeout = 10 * time.Millisecond
	client := newClient(srv.URL, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_, err := client.Read(ctx, "-1", 0)
	if err == nil {
		t.Fatal("expected error on context cancellation")
	}
}

func TestOptions_NilValues(t *testing.T) {
	mock := newMockServer()
	srv := httptest.NewServer(mock)
	defer srv.Close()

	t.Run("nil HTTPClient is ignored", func(t *testing.T) {
		store, err := New(srv.URL, WithHTTPClient(nil))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.client.httpClient == nil {
			t.Error("HTTP client should not be nil")
		}
	})

	t.Run("zero timeout is ignored", func(t *testing.T) {
		store, err := New(srv.URL, WithTimeout(0))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.cfg.timeout == 0 {
			t.Error("timeout should not be zero")
		}
	})

	t.Run("negative retry attempts is ignored", func(t *testing.T) {
		store, err := New(srv.URL, WithRetry(-1, time.Millisecond))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.cfg.retryAttempts < 0 {
			t.Error("retry attempts should not be negative")
		}
	})

	t.Run("zero backoff is ignored", func(t *testing.T) {
		store, err := New(srv.URL, WithRetry(1, 0))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.cfg.retryBackoff == 0 {
			t.Error("backoff should not be zero")
		}
	})

	t.Run("empty content type is ignored", func(t *testing.T) {
		store, err := New(srv.URL, WithContentType(""))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if store.cfg.contentType == "" {
			t.Error("content type should not be empty")
		}
	})
}

func TestClient_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	store, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	_, _, err = store.Read(ctx, eventbus.OffsetOldest, 0)
	if err == nil {
		t.Fatal("expected error for not found")
	}
}

func TestClient_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.Header().Set("Stream-Next-Offset", "1")
		w.Write([]byte("invalid json"))
	}))
	defer srv.Close()

	store, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	_, _, err = store.Read(ctx, eventbus.OffsetOldest, 0)
	if err == nil {
		t.Fatal("expected error for invalid json")
	}
}

func TestClient_MalformedEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.Header().Set("Stream-Next-Offset", "1")
		// Return array with valid and malformed events
		w.Write([]byte(`[{"type":"valid","data":{}}, "not an event object"]`))
	}))
	defer srv.Close()

	store, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	// Should skip malformed events
	if len(events) != 1 {
		t.Errorf("expected 1 valid event, got %d", len(events))
	}
}

func TestStore_ReadWithLimit(t *testing.T) {
	mock := newMockServer()
	srv := httptest.NewServer(mock)
	defer srv.Close()

	store, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Append 5 events
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		event := &eventbus.Event{
			Type:      "test.event",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		_, err := store.Append(ctx, event)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	// Read with limit
	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 2)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(events) > 2 {
		t.Errorf("expected at most 2 events with limit, got %d", len(events))
	}
}

func TestClient_InvalidURL(t *testing.T) {
	// Test with URL that will fail parsing in Read
	cfg := defaultConfig()
	cfg.timeout = 100 * time.Millisecond

	// URL with control character that will fail url.Parse
	client := newClient("http://[::1]:namedport", cfg)

	ctx := context.Background()
	_, err := client.Read(ctx, "0", 0)
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestClient_CreateRequestError(t *testing.T) {
	cfg := defaultConfig()
	cfg.timeout = 100 * time.Millisecond

	// URL with invalid scheme will fail NewRequestWithContext
	client := newClient("://invalid", cfg)

	ctx := context.Background()
	err := client.Create(ctx)
	if err == nil {
		t.Fatal("expected error for invalid URL in Create")
	}
}

func TestClient_AppendRequestError(t *testing.T) {
	cfg := defaultConfig()
	cfg.timeout = 100 * time.Millisecond

	// URL with invalid scheme
	client := newClient("://invalid", cfg)

	ctx := context.Background()
	_, err := client.Append(ctx, []byte("test"))
	if err == nil {
		t.Fatal("expected error for invalid URL in Append")
	}
}

func TestClient_ReadRequestError(t *testing.T) {
	cfg := defaultConfig()
	cfg.timeout = 100 * time.Millisecond

	// Valid URL for parsing but invalid for request creation
	client := newClient("://invalid", cfg)

	ctx := context.Background()
	_, err := client.Read(ctx, "0", 0)
	if err == nil {
		t.Fatal("expected error for invalid URL in Read")
	}
}

func TestClient_CreateDoWithRetryError(t *testing.T) {
	// Server that never responds
	cfg := defaultConfig()
	cfg.timeout = 10 * time.Millisecond
	cfg.retryAttempts = 0

	client := newClient("http://192.0.2.1:1", cfg) // Non-routable IP

	ctx := context.Background()
	err := client.Create(ctx)
	if err == nil {
		t.Fatal("expected error from doWithRetry")
	}
}

func TestClient_AppendBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}
		// Return error status for POST
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	store, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	event := &eventbus.Event{
		Type:      "test.event",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	_, err = store.Append(ctx, event)
	if err == nil {
		t.Fatal("expected error for bad status on append")
	}
}

func TestClient_ReadBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}
		// Return error status for GET (not 404, not 200)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	store, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	_, _, err = store.Read(ctx, eventbus.OffsetOldest, 0)
	if err == nil {
		t.Fatal("expected error for bad status on read")
	}
}

// errorReader always returns an error on Read
type errorReader struct{}

func (e errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read error")
}

func (e errorReader) Close() error {
	return nil
}

// errorTransport returns a response with an errorReader body
type errorTransport struct {
	callCount int
}

func (t *errorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.callCount++
	// First call is for Create (PUT), return success
	if req.Method == http.MethodPut {
		return &http.Response{
			StatusCode: http.StatusCreated,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	}
	// For Read (GET), return 200 with an error reader
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       errorReader{},
		Header:     make(http.Header),
	}, nil
}

func TestClient_ReadBodyError(t *testing.T) {
	transport := &errorTransport{}
	client := &http.Client{Transport: transport}

	store, err := New("http://example.com", WithHTTPClient(client))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	_, _, err = store.Read(ctx, eventbus.OffsetOldest, 0)
	if err == nil {
		t.Fatal("expected error for read body error")
	}
	if !strings.Contains(err.Error(), "read body") {
		t.Errorf("expected 'read body' error, got: %v", err)
	}
}

// controlCharTransport makes Create succeed but all operations use URL with control char
type controlCharTransport struct{}

func (t *controlCharTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusCreated,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
	}, nil
}

func TestClient_ReadWithControlCharInURL(t *testing.T) {
	// URL with a control character that passes url.Parse but fails NewRequestWithContext
	cfg := defaultConfig()
	cfg.httpClient = &http.Client{Transport: &controlCharTransport{}}

	// Control character in path - url.Parse succeeds but http.NewRequestWithContext fails
	client := newClient("http://example.com/path\x00with\x00null", cfg)

	ctx := context.Background()
	_, err := client.Read(ctx, "0", 0)
	if err == nil {
		t.Fatal("expected error for URL with control character")
	}
}

func TestStore_AppendMarshalError(t *testing.T) {
	mock := newMockServer()
	srv := httptest.NewServer(mock)
	defer srv.Close()

	store, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	// Event with invalid JSON in Data field causes json.Marshal to fail
	event := &eventbus.Event{
		Type:      "test.event",
		Data:      json.RawMessage(`invalid json`),
		Timestamp: time.Now(),
	}

	_, err = store.Append(ctx, event)
	if err == nil {
		t.Fatal("expected error for marshal failure")
	}
	if !strings.Contains(err.Error(), "marshal event") {
		t.Errorf("expected 'marshal event' error, got: %v", err)
	}
}

func TestNewWithContext(t *testing.T) {
	mock := newMockServer()
	srv := httptest.NewServer(mock)
	defer srv.Close()

	t.Run("creates store with context", func(t *testing.T) {
		ctx := context.Background()
		store, err := NewWithContext(ctx, srv.URL)
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

		_, err := NewWithContext(ctx, srv.URL)
		if err == nil {
			t.Fatal("expected error for cancelled context")
		}
	})
}

func TestParseTimestamp(t *testing.T) {
	t.Run("empty string returns zero time", func(t *testing.T) {
		result := parseTimestamp("")
		if !result.IsZero() {
			t.Errorf("expected zero time, got %v", result)
		}
	})

	t.Run("RFC3339Nano format", func(t *testing.T) {
		ts := "2024-01-15T10:30:00.123456789Z"
		result := parseTimestamp(ts)
		if result.IsZero() {
			t.Error("expected non-zero time for RFC3339Nano")
		}
		if result.Year() != 2024 {
			t.Errorf("expected year 2024, got %d", result.Year())
		}
	})

	t.Run("RFC3339 format without nanoseconds", func(t *testing.T) {
		ts := "2024-01-15T10:30:00+05:30"
		result := parseTimestamp(ts)
		if result.IsZero() {
			t.Error("expected non-zero time for RFC3339")
		}
		if result.Year() != 2024 {
			t.Errorf("expected year 2024, got %d", result.Year())
		}
	})

	t.Run("invalid format returns zero time", func(t *testing.T) {
		result := parseTimestamp("invalid-timestamp")
		if !result.IsZero() {
			t.Errorf("expected zero time for invalid format, got %v", result)
		}
	})
}

func TestRead_EmbeddedOffsets(t *testing.T) {
	// Server that returns events with embedded offsets
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.Header().Set("Stream-Next-Offset", "100")
		// Events with their own offset field
		w.Write([]byte(`[
			{"offset":"10","type":"event1","data":{},"timestamp":"2024-01-15T10:30:00Z"},
			{"offset":"20","type":"event2","data":{},"timestamp":"2024-01-15T10:31:00.123456789Z"}
		]`))
	}))
	defer srv.Close()

	store, err := New(srv.URL)
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.Header().Set("Stream-Next-Offset", "50")
		// Events without offset field
		w.Write([]byte(`[
			{"type":"event1","data":{}},
			{"type":"event2","data":{}}
		]`))
	}))
	defer srv.Close()

	store, err := New(srv.URL)
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

	// Check that synthetic offsets are unique (batchOffset:index format)
	if events[0].Offset != "50:0" {
		t.Errorf("expected synthetic offset '50:0', got '%s'", events[0].Offset)
	}
	if events[1].Offset != "50:1" {
		t.Errorf("expected synthetic offset '50:1', got '%s'", events[1].Offset)
	}
}
