package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
	_ "modernc.org/sqlite"
)

// testLogger implements Logger for testing
type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debug(msg string, args ...any) {
	l.t.Logf("DEBUG: %s %v", msg, args)
}

func (l *testLogger) Info(msg string, args ...any) {
	l.t.Logf("INFO: %s %v", msg, args)
}

func (l *testLogger) Error(msg string, args ...any) {
	l.t.Logf("ERROR: %s %v", msg, args)
}

// testMetricsHook implements MetricsHook for testing
type testMetricsHook struct {
	mu              sync.Mutex
	appendCount     int
	readCount       int
	saveOffsetCount int
	loadOffsetCount int
	lastAppendErr   error
	lastReadErr     error
	lastSaveOffErr  error
	lastLoadOffErr  error
}

func (h *testMetricsHook) OnAppend(duration time.Duration, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.appendCount++
	h.lastAppendErr = err
}

func (h *testMetricsHook) OnRead(duration time.Duration, count int, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.readCount++
	h.lastReadErr = err
}

func (h *testMetricsHook) OnSaveOffset(duration time.Duration, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.saveOffsetCount++
	h.lastSaveOffErr = err
}

func (h *testMetricsHook) OnLoadOffset(duration time.Duration, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.loadOffsetCount++
	h.lastLoadOffErr = err
}

func TestNew(t *testing.T) {
	t.Run("requires path", func(t *testing.T) {
		_, err := New("")
		if err == nil {
			t.Fatal("expected error when path not provided")
		}
	})

	t.Run("rejects path with query params", func(t *testing.T) {
		_, err := New("/tmp/test.db?mode=ro")
		if err == nil {
			t.Fatal("expected error for path with '?' character")
		}
	})

	t.Run("rejects path with fragment", func(t *testing.T) {
		_, err := New("/tmp/test.db#fragment")
		if err == nil {
			t.Fatal("expected error for path with '#' character")
		}
	})

	t.Run("creates in-memory store", func(t *testing.T) {
		store, err := New(":memory:")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer store.Close()
	})

	t.Run("creates file-based store", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		store, err := New(dbPath)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer store.Close()

		// Check file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			t.Error("database file was not created")
		}
	})

	t.Run("with all options", func(t *testing.T) {
		logger := &testLogger{t: t}
		metrics := &testMetricsHook{}

		store, err := New(":memory:",
			WithBusyTimeout(10*time.Second),
			WithAutoMigrate(true),
			WithLogger(logger),
			WithMetricsHook(metrics),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer store.Close()
	})

	t.Run("handles db open error", func(t *testing.T) {
		SetDBOpener(func(driverName, dataSourceName string) (*sql.DB, error) {
			return nil, errors.New("mock open error")
		})
		defer ResetDBOpener()

		_, err := New(":memory:")
		if err == nil {
			t.Fatal("expected error when db open fails")
		}
		if !strings.Contains(err.Error(), "open database") {
			t.Errorf("expected 'open database' in error, got: %v", err)
		}
	})
}

func TestAppend(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	t.Run("appends event and returns offset", func(t *testing.T) {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{"id": 1}`),
			Timestamp: time.Now(),
		}

		offset, err := store.Append(ctx, event)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != "1" {
			t.Errorf("expected offset '1', got %s", offset)
		}
	})

	t.Run("assigns sequential offsets", func(t *testing.T) {
		for i := 2; i <= 5; i++ {
			event := &eventbus.Event{
				Type:      "TestEvent",
				Data:      json.RawMessage(`{"id": 1}`),
				Timestamp: time.Now(),
			}

			offset, err := store.Append(ctx, event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			expected := eventbus.Offset(string(rune('0' + i)))
			if offset != expected {
				t.Errorf("expected offset %s, got %s", expected, offset)
			}
		}
	})
}

func TestRead(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save test events
	for i := 1; i <= 10; i++ {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{"id": ` + string(rune('0'+i)) + `}`),
			Timestamp: time.Now(),
		}
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	t.Run("reads from beginning", func(t *testing.T) {
		events, nextOffset, err := store.Read(ctx, eventbus.OffsetOldest, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(events) != 10 {
			t.Errorf("expected 10 events, got %d", len(events))
		}

		if nextOffset != "10" {
			t.Errorf("expected next offset '10', got %s", nextOffset)
		}
	})

	t.Run("reads with limit", func(t *testing.T) {
		events, _, err := store.Read(ctx, eventbus.OffsetOldest, 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(events) != 5 {
			t.Errorf("expected 5 events, got %d", len(events))
		}
	})

	t.Run("reads from offset", func(t *testing.T) {
		events, _, err := store.Read(ctx, eventbus.Offset("5"), 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(events) != 5 {
			t.Errorf("expected 5 events (6-10), got %d", len(events))
		}

		if events[0].Offset != "6" {
			t.Errorf("expected first offset '6', got %s", events[0].Offset)
		}
	})

	t.Run("returns empty for out of range", func(t *testing.T) {
		events, _, err := store.Read(ctx, eventbus.Offset("100"), 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(events) != 0 {
			t.Errorf("expected 0 events, got %d", len(events))
		}
	})

	t.Run("returns error for invalid offset", func(t *testing.T) {
		_, _, err := store.Read(ctx, eventbus.Offset("invalid"), 0)
		if err == nil {
			t.Fatal("expected error for invalid offset")
		}
	})
}

// mockRows implements RowScanner for testing
type mockRows struct {
	index    int
	data     [][]any
	scanErr  error
	iterErr  error
	closeErr error
	closed   bool
}

func (m *mockRows) Next() bool {
	if m.index >= len(m.data) {
		return false
	}
	m.index++
	return true
}

func (m *mockRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	row := m.data[m.index-1]
	for i, d := range dest {
		switch ptr := d.(type) {
		case *int64:
			*ptr = row[i].(int64)
		case *string:
			*ptr = row[i].(string)
		case *[]byte:
			*ptr = row[i].([]byte)
		case *time.Time:
			*ptr = row[i].(time.Time)
		}
	}
	return nil
}

func (m *mockRows) Err() error {
	return m.iterErr
}

func (m *mockRows) Close() error {
	m.closed = true
	return m.closeErr
}

func TestScanEventsRowsErr(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	mock := &mockRows{
		data:    [][]any{},
		iterErr: errors.New("iteration error"),
	}

	_, err = store.ScanEvents(mock)
	if err == nil {
		t.Fatal("expected error from rows.Err()")
	}
	if !strings.Contains(err.Error(), "iterate events") {
		t.Errorf("expected 'iterate events' in error, got: %v", err)
	}
	if !mock.closed {
		t.Error("expected rows to be closed")
	}
}

func TestScanEventsScanErr(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	mock := &mockRows{
		data:    [][]any{{int64(1), "test", []byte(`{}`), time.Now()}},
		scanErr: errors.New("scan error"),
	}

	_, err = store.ScanEvents(mock)
	if err == nil {
		t.Fatal("expected error from Scan()")
	}
	if !strings.Contains(err.Error(), "scan event") {
		t.Errorf("expected 'scan event' in error, got: %v", err)
	}
}

func TestReadDBClosed(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()

	// Save some events
	for i := 0; i < 10; i++ {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{"id": 1}`),
			Timestamp: time.Now(),
		}
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	store.GetDB().Close()

	_, _, err = store.Read(ctx, eventbus.OffsetOldest, 0)
	if err == nil {
		t.Fatal("expected error when database is closed")
	}
}

func TestSubscriptionOffset(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	t.Run("returns OffsetOldest for unknown subscription", func(t *testing.T) {
		offset, err := store.LoadOffset(ctx, "unknown")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != eventbus.OffsetOldest {
			t.Errorf("expected OffsetOldest, got %s", offset)
		}
	})

	t.Run("saves and loads offset", func(t *testing.T) {
		err := store.SaveOffset(ctx, "sub1", eventbus.Offset("42"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		offset, err := store.LoadOffset(ctx, "sub1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != "42" {
			t.Errorf("expected offset '42', got %s", offset)
		}
	})

	t.Run("updates existing offset", func(t *testing.T) {
		err := store.SaveOffset(ctx, "sub1", eventbus.Offset("100"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		offset, err := store.LoadOffset(ctx, "sub1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if offset != "100" {
			t.Errorf("expected offset '100', got %s", offset)
		}
	})

	t.Run("handles multiple subscriptions", func(t *testing.T) {
		store.SaveOffset(ctx, "sub2", eventbus.Offset("50"))
		store.SaveOffset(ctx, "sub3", eventbus.Offset("75"))

		offset2, _ := store.LoadOffset(ctx, "sub2")
		offset3, _ := store.LoadOffset(ctx, "sub3")

		if offset2 != "50" {
			t.Errorf("expected sub2 offset '50', got %s", offset2)
		}
		if offset3 != "75" {
			t.Errorf("expected sub3 offset '75', got %s", offset3)
		}
	})

	t.Run("returns error for invalid offset", func(t *testing.T) {
		err := store.SaveOffset(ctx, "sub4", eventbus.Offset("invalid"))
		if err == nil {
			t.Fatal("expected error for invalid offset")
		}
	})
}

func TestConcurrentAccess(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	const numGoroutines = 10
	const eventsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				event := &eventbus.Event{
					Type:      "ConcurrentEvent",
					Data:      json.RawMessage(`{"test": true}`),
					Timestamp: time.Now(),
				}
				if _, err := store.Append(ctx, event); err != nil {
					t.Errorf("concurrent append failed: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	// Verify all events were saved
	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("failed to read events: %v", err)
	}

	expectedTotal := numGoroutines * eventsPerGoroutine
	if len(events) != expectedTotal {
		t.Errorf("expected %d events, got %d", expectedTotal, len(events))
	}

	// Verify all offsets are unique
	offsets := make(map[eventbus.Offset]bool)
	for _, e := range events {
		if offsets[e.Offset] {
			t.Errorf("duplicate offset: %s", e.Offset)
		}
		offsets[e.Offset] = true
	}
}

func TestMetricsHook(t *testing.T) {
	metrics := &testMetricsHook{}

	store, err := New(":memory:", WithMetricsHook(metrics))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Append some events
	for i := 0; i < 5; i++ {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Append(ctx, event)
	}

	// Read events
	store.Read(ctx, eventbus.OffsetOldest, 0)
	store.Read(ctx, eventbus.OffsetOldest, 3)

	// Subscription offset operations
	store.SaveOffset(ctx, "sub1", eventbus.Offset("5"))
	store.SaveOffset(ctx, "sub2", eventbus.Offset("10"))
	store.LoadOffset(ctx, "sub1")
	store.LoadOffset(ctx, "unknown")

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if metrics.appendCount != 5 {
		t.Errorf("expected 5 append calls, got %d", metrics.appendCount)
	}

	if metrics.readCount != 2 {
		t.Errorf("expected 2 read calls, got %d", metrics.readCount)
	}

	if metrics.saveOffsetCount != 2 {
		t.Errorf("expected 2 save offset calls, got %d", metrics.saveOffsetCount)
	}

	if metrics.loadOffsetCount != 2 {
		t.Errorf("expected 2 load offset calls, got %d", metrics.loadOffsetCount)
	}
}

func TestContextCancellation(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	event := &eventbus.Event{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}

	_, err = store.Append(ctx, event)
	if err == nil {
		t.Error("expected error with cancelled context")
	}

	_, _, err = store.Read(ctx, eventbus.OffsetOldest, 10)
	if err == nil {
		t.Error("expected error with cancelled context")
	}
}

func TestClose(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	err = store.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}

	ctx := context.Background()
	event := &eventbus.Event{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}

	_, err = store.Append(ctx, event)
	if err == nil {
		t.Error("expected error after close")
	}
}

func TestIntegrationWithEventBus(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	bus := eventbus.New(eventbus.WithStore(store))
	defer bus.Shutdown(context.Background())

	type TestEvent struct {
		ID      int    `json:"id"`
		Message string `json:"message"`
	}

	var received []TestEvent
	var mu sync.Mutex

	eventbus.Subscribe(bus, func(e TestEvent) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, e)
	})

	for i := 1; i <= 5; i++ {
		eventbus.Publish(bus, TestEvent{ID: i, Message: "test"})
	}

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(received) != 5 {
		t.Errorf("expected 5 events, received %d", len(received))
	}
	mu.Unlock()

	ctx := context.Background()
	events, _, err := store.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("failed to read events: %v", err)
	}

	if len(events) != 5 {
		t.Errorf("expected 5 persisted events, got %d", len(events))
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "persist.db")

	store1, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{"id": ` + string(rune('0'+i)) + `}`),
			Timestamp: time.Now(),
		}
		if _, err := store1.Append(ctx, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}
	store1.SaveOffset(ctx, "sub1", eventbus.Offset("3"))
	store1.Close()

	store2, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store2.Close()

	events, _, err := store2.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("failed to read events: %v", err)
	}
	if len(events) != 5 {
		t.Errorf("expected 5 events, got %d", len(events))
	}

	subOffset, err := store2.LoadOffset(ctx, "sub1")
	if err != nil {
		t.Fatalf("failed to load offset: %v", err)
	}
	if subOffset != "3" {
		t.Errorf("expected offset '3', got %s", subOffset)
	}
}

func TestAutoMigrateDisabled(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "nomigrate.db")

	store, err := New(dbPath, WithAutoMigrate(false))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	event := &eventbus.Event{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}

	_, err = store.Append(ctx, event)
	if err == nil {
		t.Error("expected error when tables don't exist")
	}
}

func TestLoggerCoverage(t *testing.T) {
	logger := &testLogger{t: t}

	store, err := New(":memory:", WithLogger(logger))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	event := &eventbus.Event{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Append(ctx, event)
	store.Read(ctx, eventbus.OffsetOldest, 0)
	store.SaveOffset(ctx, "sub1", eventbus.Offset("1"))
}

func TestMetricsHookErrors(t *testing.T) {
	metrics := &testMetricsHook{}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "metrics_error.db")

	store, err := New(dbPath, WithAutoMigrate(false), WithMetricsHook(metrics))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	event := &eventbus.Event{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Append(ctx, event)
	store.Read(ctx, eventbus.OffsetOldest, 0)
	store.SaveOffset(ctx, "sub1", eventbus.Offset("1"))
	store.LoadOffset(ctx, "sub1")

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if metrics.lastAppendErr == nil {
		t.Error("expected append error to be recorded")
	}
	if metrics.lastReadErr == nil {
		t.Error("expected read error to be recorded")
	}
	if metrics.lastSaveOffErr == nil {
		t.Error("expected save offset error to be recorded")
	}
	if metrics.lastLoadOffErr == nil {
		t.Error("expected load offset error to be recorded")
	}
}

func TestMigrateErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("migrate with cancelled context", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "migrate_cancel.db")

		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}
		defer db.Close()

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		err = RunMigrate(cancelCtx, db)
		if err == nil {
			t.Error("expected error with cancelled context")
		}
	})

	t.Run("migrateV1 with cancelled context", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "migratev1_cancel.db")

		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}
		defer db.Close()

		db.Exec("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		err = RunMigrateV1(cancelCtx, db)
		if err == nil {
			t.Error("expected error with cancelled context")
		}
	})
}

func TestMigrateIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "idempotent.db")

	store1, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	store1.Close()

	store2, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store2.Close()

	ctx := context.Background()
	events, _, err := store2.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestNewFromDB_PrepareStatementsError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	db.Close()

	_, err = NewFromDB(db)
	if err == nil {
		t.Fatal("expected error when db is closed")
	}
}

func TestReadStream(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	for i := 1; i <= 10; i++ {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{"id": ` + string(rune('0'+i)) + `}`),
			Timestamp: time.Now(),
		}
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	t.Run("streams all events", func(t *testing.T) {
		var events []*eventbus.StoredEvent
		for event, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			events = append(events, event)
		}

		if len(events) != 10 {
			t.Errorf("expected 10 events, got %d", len(events))
		}
	})

	t.Run("streams from offset", func(t *testing.T) {
		var events []*eventbus.StoredEvent
		for event, err := range store.ReadStream(ctx, eventbus.Offset("5")) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			events = append(events, event)
		}

		if len(events) != 5 {
			t.Errorf("expected 5 events (6-10), got %d", len(events))
		}
	})

	t.Run("handles early termination", func(t *testing.T) {
		count := 0
		for event, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
			if event.Offset == "3" {
				break
			}
		}

		if count != 3 {
			t.Errorf("expected 3 events, got %d", count)
		}
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		var gotError bool
		for _, err := range store.ReadStream(cancelCtx, eventbus.OffsetOldest) {
			if err != nil {
				gotError = true
				break
			}
		}

		if !gotError {
			t.Error("expected error with cancelled context")
		}
	})

	t.Run("returns error for invalid offset", func(t *testing.T) {
		var gotError bool
		for _, err := range store.ReadStream(ctx, eventbus.Offset("invalid")) {
			if err != nil {
				gotError = true
				break
			}
		}

		if !gotError {
			t.Error("expected error for invalid offset")
		}
	})
}

func TestReadStreamBatched(t *testing.T) {
	store, err := New(":memory:", WithStreamBatchSize(3))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	for i := 1; i <= 10; i++ {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	t.Run("batched streaming all events", func(t *testing.T) {
		var events []*eventbus.StoredEvent
		for event, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			events = append(events, event)
		}

		if len(events) != 10 {
			t.Errorf("expected 10 events, got %d", len(events))
		}
	})

	t.Run("batched streaming early termination", func(t *testing.T) {
		count := 0
		for event, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
			if event.Offset == "5" {
				break
			}
		}

		if count != 5 {
			t.Errorf("expected 5 events before break, got %d", count)
		}
	})
}

func TestReadStreamDBClosed(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()

	event := &eventbus.Event{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Append(ctx, event)
	store.Close()

	var gotError bool
	for _, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
		if err != nil {
			gotError = true
			break
		}
	}

	if !gotError {
		t.Error("expected error when database is closed")
	}
}

func TestStreamRowsIterErr(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	mock := &mockRows{
		data:    [][]any{},
		iterErr: errors.New("iteration error"),
	}

	var eventCount int
	var iterErr error
	var gotError bool

	store.StreamRows(ctx, mock, &eventCount, &iterErr, func(event *eventbus.StoredEvent, err error) bool {
		if err != nil {
			gotError = true
			return false
		}
		return true
	})

	if !gotError {
		t.Error("expected error from rows.Err()")
	}
	if iterErr == nil {
		t.Error("expected iterErr to be set")
	}
	if !strings.Contains(iterErr.Error(), "iterate events") {
		t.Errorf("expected 'iterate events' in error, got: %v", iterErr)
	}
}

func TestStreamBatchCloseErr(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	mock := &mockRows{
		data: [][]any{
			{int64(1), "test", []byte(`{}`), time.Now()},
		},
		closeErr: errors.New("close error"),
	}

	var eventCount int
	var iterErr error
	var gotError bool

	_, _, cont := store.StreamBatch(mock, &eventCount, &iterErr, func(event *eventbus.StoredEvent, err error) bool {
		if err != nil {
			gotError = true
			return false
		}
		return true
	})

	if !gotError {
		t.Error("expected error from rows.Close()")
	}
	if cont {
		t.Error("expected cont to be false due to close error")
	}
}

func TestStreamRowsScanErr(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	mock := &mockRows{
		data: [][]any{
			{int64(1), "test", []byte(`{}`), time.Now()},
		},
		scanErr: errors.New("scan error"),
	}

	var eventCount int
	var iterErr error
	var gotError bool

	store.StreamRows(ctx, mock, &eventCount, &iterErr, func(event *eventbus.StoredEvent, err error) bool {
		if err != nil {
			gotError = true
			return false
		}
		return true
	})

	if !gotError {
		t.Error("expected error from rows.Scan()")
	}
	if iterErr == nil {
		t.Error("expected iterErr to be set")
	}
	if !strings.Contains(iterErr.Error(), "scan event") {
		t.Errorf("expected 'scan event' in error, got: %v", iterErr)
	}
}

func TestStreamBatchScanErr(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	mock := &mockRows{
		data: [][]any{
			{int64(1), "test", []byte(`{}`), time.Now()},
		},
		scanErr: errors.New("scan error"),
	}

	var eventCount int
	var iterErr error
	var gotError bool

	batchCount, lastPos, cont := store.StreamBatch(mock, &eventCount, &iterErr, func(event *eventbus.StoredEvent, err error) bool {
		if err != nil {
			gotError = true
			return false
		}
		return true
	})

	if !gotError {
		t.Error("expected error from rows.Scan()")
	}
	if cont {
		t.Error("expected cont to be false due to scan error")
	}
	if batchCount != 0 {
		t.Errorf("expected batchCount 0, got %d", batchCount)
	}
	if lastPos != 0 {
		t.Errorf("expected lastPos 0, got %d", lastPos)
	}
	if !strings.Contains(iterErr.Error(), "scan event") {
		t.Errorf("expected 'scan event' in error, got: %v", iterErr)
	}
}

func TestReadStreamBatchedContextCancellation(t *testing.T) {
	store, err := New(":memory:", WithStreamBatchSize(3))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	var gotError bool
	for _, err := range store.ReadStream(cancelCtx, eventbus.OffsetOldest) {
		if err != nil {
			gotError = true
			break
		}
	}

	if !gotError {
		t.Error("expected error with cancelled context in batched mode")
	}
}

func TestReadStreamBatchedDBClosed(t *testing.T) {
	store, err := New(":memory:", WithStreamBatchSize(3))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()

	event := &eventbus.Event{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Append(ctx, event)
	store.Close()

	var gotError bool
	for _, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
		if err != nil {
			gotError = true
			break
		}
	}

	if !gotError {
		t.Error("expected error when database is closed in batched mode")
	}
}

func TestReadStreamBatchedWithLogger(t *testing.T) {
	logger := &testLogger{t: t}
	store, err := New(":memory:", WithStreamBatchSize(3), WithLogger(logger))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	var events []*eventbus.StoredEvent
	for event, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		events = append(events, event)
	}

	if len(events) != 5 {
		t.Errorf("expected 5 events, got %d", len(events))
	}
}

func BenchmarkAppend(b *testing.B) {
	store, _ := New(":memory:")
	defer store.Close()

	ctx := context.Background()
	event := &eventbus.Event{
		Type:      "BenchEvent",
		Data:      json.RawMessage(`{"id": 1, "data": "benchmark test data"}`),
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := &eventbus.Event{
			Type:      event.Type,
			Data:      event.Data,
			Timestamp: event.Timestamp,
		}
		store.Append(ctx, e)
	}
}

func BenchmarkRead(b *testing.B) {
	store, _ := New(":memory:")
	defer store.Close()

	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		event := &eventbus.Event{
			Type:      "BenchEvent",
			Data:      json.RawMessage(`{"id": 1}`),
			Timestamp: time.Now(),
		}
		store.Append(ctx, event)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Read(ctx, eventbus.OffsetOldest, 100)
	}
}

func BenchmarkConcurrentAppend(b *testing.B) {
	store, _ := New(":memory:")
	defer store.Close()

	ctx := context.Background()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			event := &eventbus.Event{
				Type:      "BenchEvent",
				Data:      json.RawMessage(`{"id": 1}`),
				Timestamp: time.Now(),
			}
			store.Append(ctx, event)
		}
	})
}

func TestApplyPragmasError(t *testing.T) {
	// To test applyPragmas error, we need a database that rejects pragmas
	// This is typically a read-only database or one with issues

	// Use a mock dbOpener that returns a db that fails on exec
	SetDBOpener(func(driverName, dataSourceName string) (*sql.DB, error) {
		// Open a real connection but close it immediately so pragmas fail
		db, err := sql.Open(driverName, dataSourceName)
		if err != nil {
			return nil, err
		}
		db.Close() // Close it so all operations fail
		return db, nil
	})
	defer ResetDBOpener()

	_, err := New(":memory:")
	if err == nil {
		t.Fatal("expected error when applying pragmas")
	}
	if !strings.Contains(err.Error(), "apply pragmas") {
		t.Errorf("expected 'apply pragmas' in error, got: %v", err)
	}
}

func TestMigrateError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "migrate_error.db")

	// Create a database with a corrupted schema_version table
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	// Create schema_version as a non-table object to cause migration to fail
	// Actually, create a schema_version table but with the wrong schema
	_, err = db.Exec("CREATE TABLE schema_version (bad_column TEXT)")
	if err != nil {
		t.Fatalf("failed to create bad table: %v", err)
	}
	db.Close()

	// Now try to open with New - migration should fail due to bad schema
	_, err = New(dbPath)
	if err == nil {
		t.Fatal("expected error when migration fails")
	}
	if !strings.Contains(err.Error(), "migrate") {
		t.Errorf("expected 'migrate' in error, got: %v", err)
	}
}

func TestMigrateV1ExecSchemaError(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "v1_exec_error.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Create schema_version table
	_, err = db.Exec("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create the events table with wrong schema to cause the CREATE TABLE to fail
	// Actually, SQLite's IF NOT EXISTS won't fail on existing table.
	// Let's create a conflicting index name
	_, err = db.Exec("CREATE TABLE idx_events_type (x INTEGER)")
	if err != nil {
		t.Fatalf("failed to create conflicting object: %v", err)
	}

	err = RunMigrateV1(ctx, db)
	if err == nil {
		t.Fatal("expected error when exec schema fails")
	}
	if !strings.Contains(err.Error(), "exec schema") {
		t.Errorf("expected 'exec schema' in error, got: %v", err)
	}
}

func TestMigrateGetVersionError(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "version_error.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Create schema_version but with columns that cause the query to fail
	// The query is: SELECT COALESCE(MAX(version), 0) FROM schema_version
	// If 'version' column doesn't exist, it will fail
	_, err = db.Exec("CREATE TABLE schema_version (bad_column TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	err = RunMigrate(ctx, db)
	if err == nil {
		t.Fatal("expected error when getting schema version fails")
	}
	if !strings.Contains(err.Error(), "get schema version") {
		t.Errorf("expected 'get schema version' in error, got: %v", err)
	}
}

func TestReadWithScanEventsError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "scan_error.db")

	// Create a database with proper schema
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	// Create schema
	_, err = db.Exec(`
		CREATE TABLE events (
			position INTEGER PRIMARY KEY AUTOINCREMENT,
			type TEXT NOT NULL,
			data BLOB NOT NULL,
			timestamp DATETIME NOT NULL
		);
		CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
		INSERT INTO schema_version (version) VALUES (1);
		CREATE TABLE subscription_positions (
			subscription_id TEXT PRIMARY KEY,
			position INTEGER NOT NULL,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Insert data with a string timestamp that can't be parsed into time.Time
	// The Go driver tries to scan a string into time.Time which fails
	_, err = db.Exec("INSERT INTO events (type, data, timestamp) VALUES (?, ?, ?)",
		"test", []byte("{}"), "not-a-valid-timestamp")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	db.Close()

	// Open with New (no migration needed since version is 1)
	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	// This should fail during scan due to invalid timestamp
	_, _, err = store.Read(ctx, eventbus.OffsetOldest, 0)
	if err == nil {
		t.Fatal("expected error when scan fails due to invalid timestamp")
	}
	if !strings.Contains(err.Error(), "scan event") {
		t.Errorf("expected 'scan event' in error, got: %v", err)
	}
}

func TestReadStreamWithLoggerNonBatched(t *testing.T) {
	logger := &testLogger{t: t}
	store, err := New(":memory:", WithLogger(logger))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Add events
	for i := 1; i <= 3; i++ {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Append(ctx, event)
	}

	// Stream them all (non-batched mode) to trigger the logger path at line 396-398
	var events []*eventbus.StoredEvent
	for event, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		events = append(events, event)
	}

	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}
}

func TestReadStreamWithMetricsHook(t *testing.T) {
	metrics := &testMetricsHook{}
	store, err := New(":memory:", WithMetricsHook(metrics))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Add events
	for i := 1; i <= 3; i++ {
		event := &eventbus.Event{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Append(ctx, event)
	}

	// Stream them to trigger metrics hook in ReadStream deferred (line 369-371)
	var events []*eventbus.StoredEvent
	for event, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		events = append(events, event)
	}

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	// The ReadStream uses OnRead hook
	if metrics.readCount == 0 {
		t.Error("expected readCount > 0 from ReadStream")
	}
}

func TestStreamRowsContextCancellation(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Create mock rows that will keep returning Next() = true
	// but context will be cancelled mid-iteration
	mock := &mockRowsWithCancel{
		data: [][]any{
			{int64(1), "test", []byte(`{}`), time.Now()},
			{int64(2), "test", []byte(`{}`), time.Now()},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	var eventCount int
	var iterErr error
	var gotContextError bool

	// Cancel context during first iteration
	store.StreamRows(ctx, mock, &eventCount, &iterErr, func(event *eventbus.StoredEvent, err error) bool {
		if event != nil {
			cancel() // Cancel after first event
		}
		if err != nil && errors.Is(err, context.Canceled) {
			gotContextError = true
			return false
		}
		return true
	})

	if !gotContextError {
		t.Error("expected context.Canceled error in streamRows")
	}
}

// mockRowsWithCancel is similar to mockRows but allows testing context cancellation
type mockRowsWithCancel struct {
	index  int
	data   [][]any
	closed bool
}

func (m *mockRowsWithCancel) Next() bool {
	if m.index >= len(m.data) {
		return false
	}
	m.index++
	return true
}

func (m *mockRowsWithCancel) Scan(dest ...any) error {
	row := m.data[m.index-1]
	for i, d := range dest {
		switch ptr := d.(type) {
		case *int64:
			*ptr = row[i].(int64)
		case *string:
			*ptr = row[i].(string)
		case *[]byte:
			*ptr = row[i].([]byte)
		case *time.Time:
			*ptr = row[i].(time.Time)
		}
	}
	return nil
}

func (m *mockRowsWithCancel) Err() error {
	return nil
}

func (m *mockRowsWithCancel) Close() error {
	m.closed = true
	return nil
}

func BenchmarkReadVsReadStream(b *testing.B) {
	store, _ := New(":memory:")
	defer store.Close()

	ctx := context.Background()

	for i := 0; i < 10000; i++ {
		event := &eventbus.Event{
			Type:      "BenchEvent",
			Data:      json.RawMessage(`{"id": 1}`),
			Timestamp: time.Now(),
		}
		store.Append(ctx, event)
	}

	b.Run("Read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			events, _, _ := store.Read(ctx, eventbus.OffsetOldest, 0)
			_ = len(events)
		}
	})

	b.Run("ReadStream", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count := 0
			for _, err := range store.ReadStream(ctx, eventbus.OffsetOldest) {
				if err != nil {
					break
				}
				count++
			}
		}
	})
}
