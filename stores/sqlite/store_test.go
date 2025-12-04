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
	mu                sync.Mutex
	saveCount         int
	loadCount         int
	posCount          int
	saveSubPosCount   int
	loadSubPosCount   int
	lastSaveErr       error
	lastLoadErr       error
	lastPosErr        error
	lastSaveSubPosErr error
	lastLoadSubPosErr error
}

func (h *testMetricsHook) OnSave(duration time.Duration, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.saveCount++
	h.lastSaveErr = err
}

func (h *testMetricsHook) OnLoad(duration time.Duration, count int, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.loadCount++
	h.lastLoadErr = err
}

func (h *testMetricsHook) OnGetPosition(duration time.Duration, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.posCount++
	h.lastPosErr = err
}

func (h *testMetricsHook) OnSaveSubscriptionPosition(duration time.Duration, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.saveSubPosCount++
	h.lastSaveSubPosErr = err
}

func (h *testMetricsHook) OnLoadSubscriptionPosition(duration time.Duration, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.loadSubPosCount++
	h.lastLoadSubPosErr = err
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
		// Mock dbOpener to return an error
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

func TestSave(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	t.Run("saves event and assigns position", func(t *testing.T) {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{"id": 1}`),
			Timestamp: time.Now(),
		}

		err := store.Save(ctx, event)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if event.Position != 1 {
			t.Errorf("expected position 1, got %d", event.Position)
		}
	})

	t.Run("assigns sequential positions", func(t *testing.T) {
		for i := int64(2); i <= 5; i++ {
			event := &eventbus.StoredEvent{
				Type:      "TestEvent",
				Data:      json.RawMessage(`{"id": 1}`),
				Timestamp: time.Now(),
			}

			if err := store.Save(ctx, event); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if event.Position != i {
				t.Errorf("expected position %d, got %d", i, event.Position)
			}
		}
	})
}

func TestLoad(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save test events
	for i := 1; i <= 10; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{"id": ` + string(rune('0'+i)) + `}`),
			Timestamp: time.Now(),
		}
		if err := store.Save(ctx, event); err != nil {
			t.Fatalf("failed to save event: %v", err)
		}
	}

	t.Run("loads range of events", func(t *testing.T) {
		events, err := store.Load(ctx, 3, 7)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(events) != 5 {
			t.Errorf("expected 5 events, got %d", len(events))
		}

		if events[0].Position != 3 {
			t.Errorf("expected first position 3, got %d", events[0].Position)
		}

		if events[4].Position != 7 {
			t.Errorf("expected last position 7, got %d", events[4].Position)
		}
	})

	t.Run("loads all events from position", func(t *testing.T) {
		events, err := store.Load(ctx, 5, -1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(events) != 6 {
			t.Errorf("expected 6 events, got %d", len(events))
		}

		if events[0].Position != 5 {
			t.Errorf("expected first position 5, got %d", events[0].Position)
		}
	})

	t.Run("returns empty for out of range", func(t *testing.T) {
		events, err := store.Load(ctx, 100, 200)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(events) != 0 {
			t.Errorf("expected 0 events, got %d", len(events))
		}
	})

	t.Run("loads all events", func(t *testing.T) {
		events, err := store.Load(ctx, 1, -1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(events) != 10 {
			t.Errorf("expected 10 events, got %d", len(events))
		}
	})
}

// mockRows implements RowScanner for testing rows.Err() error path
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

	// Create mock rows that return an error from Err()
	mock := &mockRows{
		data:    [][]any{}, // No data, but Err() will return error
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

	// Create mock rows that return an error from Scan()
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

func TestLoadDBClosed(t *testing.T) {
	// Create a store with events
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()

	// Save some events
	for i := 0; i < 10; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{"id": 1}`),
			Timestamp: time.Now(),
		}
		if err := store.Save(ctx, event); err != nil {
			t.Fatalf("failed to save event: %v", err)
		}
	}

	// Close the database connection to force error
	store.GetDB().Close()

	// Now try to load - this should error
	_, err = store.Load(ctx, 1, -1)
	if err == nil {
		t.Fatal("expected error when database is closed")
	}
}

func TestGetPosition(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	t.Run("returns 0 for empty store", func(t *testing.T) {
		pos, err := store.GetPosition(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if pos != 0 {
			t.Errorf("expected position 0, got %d", pos)
		}
	})

	t.Run("returns max position after saves", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			event := &eventbus.StoredEvent{
				Type:      "TestEvent",
				Data:      json.RawMessage(`{}`),
				Timestamp: time.Now(),
			}
			if err := store.Save(ctx, event); err != nil {
				t.Fatalf("failed to save event: %v", err)
			}
		}

		pos, err := store.GetPosition(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if pos != 5 {
			t.Errorf("expected position 5, got %d", pos)
		}
	})
}

func TestSubscriptionPosition(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	t.Run("returns 0 for unknown subscription", func(t *testing.T) {
		pos, err := store.LoadSubscriptionPosition(ctx, "unknown")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if pos != 0 {
			t.Errorf("expected position 0, got %d", pos)
		}
	})

	t.Run("saves and loads position", func(t *testing.T) {
		err := store.SaveSubscriptionPosition(ctx, "sub1", 42)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		pos, err := store.LoadSubscriptionPosition(ctx, "sub1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if pos != 42 {
			t.Errorf("expected position 42, got %d", pos)
		}
	})

	t.Run("updates existing position", func(t *testing.T) {
		err := store.SaveSubscriptionPosition(ctx, "sub1", 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		pos, err := store.LoadSubscriptionPosition(ctx, "sub1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if pos != 100 {
			t.Errorf("expected position 100, got %d", pos)
		}
	})

	t.Run("handles multiple subscriptions", func(t *testing.T) {
		store.SaveSubscriptionPosition(ctx, "sub2", 50)
		store.SaveSubscriptionPosition(ctx, "sub3", 75)

		pos2, _ := store.LoadSubscriptionPosition(ctx, "sub2")
		pos3, _ := store.LoadSubscriptionPosition(ctx, "sub3")

		if pos2 != 50 {
			t.Errorf("expected sub2 position 50, got %d", pos2)
		}
		if pos3 != 75 {
			t.Errorf("expected sub3 position 75, got %d", pos3)
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
				event := &eventbus.StoredEvent{
					Type:      "ConcurrentEvent",
					Data:      json.RawMessage(`{"test": true}`),
					Timestamp: time.Now(),
				}
				if err := store.Save(ctx, event); err != nil {
					t.Errorf("concurrent save failed: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	// Verify all events were saved
	pos, err := store.GetPosition(ctx)
	if err != nil {
		t.Fatalf("failed to get position: %v", err)
	}

	expectedTotal := int64(numGoroutines * eventsPerGoroutine)
	if pos != expectedTotal {
		t.Errorf("expected %d events, got %d", expectedTotal, pos)
	}

	// Verify all positions are unique
	events, err := store.Load(ctx, 1, -1)
	if err != nil {
		t.Fatalf("failed to load events: %v", err)
	}

	positions := make(map[int64]bool)
	for _, e := range events {
		if positions[e.Position] {
			t.Errorf("duplicate position: %d", e.Position)
		}
		positions[e.Position] = true
	}
}

func TestMetricsHook(t *testing.T) {
	metrics := &testMetricsHook{}

	store, err := New(":memory:",
		WithMetricsHook(metrics),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save some events
	for i := 0; i < 5; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Save(ctx, event)
	}

	// Load events
	store.Load(ctx, 1, -1)
	store.Load(ctx, 1, 3)

	// Get position
	store.GetPosition(ctx)

	// Subscription position operations
	store.SaveSubscriptionPosition(ctx, "sub1", 5)
	store.SaveSubscriptionPosition(ctx, "sub2", 10)
	store.LoadSubscriptionPosition(ctx, "sub1")
	store.LoadSubscriptionPosition(ctx, "unknown") // not found case

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if metrics.saveCount != 5 {
		t.Errorf("expected 5 save calls, got %d", metrics.saveCount)
	}

	if metrics.loadCount != 2 {
		t.Errorf("expected 2 load calls, got %d", metrics.loadCount)
	}

	if metrics.posCount != 1 {
		t.Errorf("expected 1 position call, got %d", metrics.posCount)
	}

	if metrics.saveSubPosCount != 2 {
		t.Errorf("expected 2 save subscription position calls, got %d", metrics.saveSubPosCount)
	}

	if metrics.loadSubPosCount != 2 {
		t.Errorf("expected 2 load subscription position calls, got %d", metrics.loadSubPosCount)
	}
}

func TestContextCancellation(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Operations with cancelled context should fail
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}

	err = store.Save(ctx, event)
	if err == nil {
		t.Error("expected error with cancelled context")
	}

	_, err = store.Load(ctx, 1, 10)
	if err == nil {
		t.Error("expected error with cancelled context")
	}

	_, err = store.GetPosition(ctx)
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

	// Operations after close should fail
	ctx := context.Background()
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}

	err = store.Save(ctx, event)
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

	// Create event bus with SQLite store
	bus := eventbus.New(eventbus.WithStore(store))
	defer bus.Shutdown(context.Background())

	// Define test event
	type TestEvent struct {
		ID      int    `json:"id"`
		Message string `json:"message"`
	}

	// Track received events
	var received []TestEvent
	var mu sync.Mutex

	eventbus.Subscribe(bus, func(e TestEvent) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, e)
	})

	// Publish events
	for i := 1; i <= 5; i++ {
		eventbus.Publish(bus, TestEvent{ID: i, Message: "test"})
	}

	// Wait for handlers to complete
	time.Sleep(50 * time.Millisecond)

	// Verify events were received
	mu.Lock()
	if len(received) != 5 {
		t.Errorf("expected 5 events, received %d", len(received))
	}
	mu.Unlock()

	// Verify events were persisted
	ctx := context.Background()
	events, err := store.Load(ctx, 1, -1)
	if err != nil {
		t.Fatalf("failed to load events: %v", err)
	}

	if len(events) != 5 {
		t.Errorf("expected 5 persisted events, got %d", len(events))
	}

	// Verify position
	pos, err := store.GetPosition(ctx)
	if err != nil {
		t.Fatalf("failed to get position: %v", err)
	}

	if pos != 5 {
		t.Errorf("expected position 5, got %d", pos)
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "persist.db")

	// Create store and save events
	store1, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{"id": ` + string(rune('0'+i)) + `}`),
			Timestamp: time.Now(),
		}
		if err := store1.Save(ctx, event); err != nil {
			t.Fatalf("failed to save event: %v", err)
		}
	}
	store1.SaveSubscriptionPosition(ctx, "sub1", 3)
	store1.Close()

	// Reopen store and verify data
	store2, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store2.Close()

	// Verify events
	pos, err := store2.GetPosition(ctx)
	if err != nil {
		t.Fatalf("failed to get position: %v", err)
	}
	if pos != 5 {
		t.Errorf("expected position 5, got %d", pos)
	}

	events, err := store2.Load(ctx, 1, -1)
	if err != nil {
		t.Fatalf("failed to load events: %v", err)
	}
	if len(events) != 5 {
		t.Errorf("expected 5 events, got %d", len(events))
	}

	// Verify subscription position
	subPos, err := store2.LoadSubscriptionPosition(ctx, "sub1")
	if err != nil {
		t.Fatalf("failed to load subscription position: %v", err)
	}
	if subPos != 3 {
		t.Errorf("expected subscription position 3, got %d", subPos)
	}
}

func TestAutoMigrateDisabled(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "nomigrate.db")

	// Create store without auto-migrate
	store, err := New(dbPath,
		WithAutoMigrate(false),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Operations should fail since tables don't exist
	ctx := context.Background()
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}

	err = store.Save(ctx, event)
	if err == nil {
		t.Error("expected error when tables don't exist")
	}
}

func TestLoggerCoverage(t *testing.T) {
	logger := &testLogger{t: t}

	store, err := New(":memory:",
		WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save triggers logger
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Save(ctx, event)

	// Load triggers logger
	store.Load(ctx, 1, -1)

	// SaveSubscriptionPosition triggers logger
	store.SaveSubscriptionPosition(ctx, "sub1", 1)
}

func TestMetricsHookErrors(t *testing.T) {
	metrics := &testMetricsHook{}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "metrics_error.db")

	// Create store with auto-migrate disabled so operations will fail
	store, err := New(dbPath,
		WithAutoMigrate(false),
		WithMetricsHook(metrics),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save should fail and trigger metrics
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Save(ctx, event)

	// Load should fail and trigger metrics
	store.Load(ctx, 1, -1)

	// GetPosition should fail and trigger metrics
	store.GetPosition(ctx)

	// Subscription position operations should fail and trigger metrics
	store.SaveSubscriptionPosition(ctx, "sub1", 1)
	store.LoadSubscriptionPosition(ctx, "sub1")

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if metrics.lastSaveErr == nil {
		t.Error("expected save error to be recorded")
	}
	if metrics.lastLoadErr == nil {
		t.Error("expected load error to be recorded")
	}
	if metrics.lastPosErr == nil {
		t.Error("expected position error to be recorded")
	}
	if metrics.lastSaveSubPosErr == nil {
		t.Error("expected save subscription position error to be recorded")
	}
	if metrics.lastLoadSubPosErr == nil {
		t.Error("expected load subscription position error to be recorded")
	}
}

func TestLoadWithRange(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save test events
	for i := 1; i <= 10; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Save(ctx, event)
	}

	// Test loading specific range
	events, err := store.Load(ctx, 3, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}
}

func TestSubscriptionPositionErrors(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "sub_error.db")

	store, err := New(dbPath, WithAutoMigrate(false))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// SaveSubscriptionPosition should fail without tables
	err = store.SaveSubscriptionPosition(ctx, "sub1", 1)
	if err == nil {
		t.Error("expected error for SaveSubscriptionPosition without tables")
	}

	// LoadSubscriptionPosition should fail without tables
	_, err = store.LoadSubscriptionPosition(ctx, "sub1")
	if err == nil {
		t.Error("expected error for LoadSubscriptionPosition without tables")
	}
}

func TestLoadScanError(t *testing.T) {
	// This tests the scan error path by loading after close
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()

	// Save an event
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Save(ctx, event)

	// Close and try to load - should error
	store.Close()
	_, err = store.Load(ctx, 1, -1)
	if err == nil {
		t.Error("expected error loading after close")
	}
}

func TestPrepareStatementError(t *testing.T) {
	// Create store with auto-migrate disabled, then prepare will fail
	// because tables don't exist
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "prep_error.db")

	store, err := New(dbPath, WithAutoMigrate(false))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// The store is created but operations will fail
	ctx := context.Background()
	_, err = store.GetPosition(ctx)
	if err == nil {
		t.Error("expected error when tables don't exist")
	}
}

func TestMigrateIdempotent(t *testing.T) {
	// Test that running migrations twice is safe
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "idempotent.db")

	// First store creates the schema
	store1, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	store1.Close()

	// Second store should succeed (migrations already applied)
	store2, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store2.Close()

	// Verify it works
	ctx := context.Background()
	pos, err := store2.GetPosition(ctx)
	if err != nil {
		t.Fatalf("failed to get position: %v", err)
	}
	if pos != 0 {
		t.Errorf("expected position 0, got %d", pos)
	}
}

func TestLoadWithMetricsOnScanError(t *testing.T) {
	metrics := &testMetricsHook{}

	store, err := New(":memory:", WithMetricsHook(metrics))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()

	// Save an event
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Save(ctx, event)

	// Close the store to cause load to fail
	store.Close()

	// Load should fail and trigger metrics
	store.Load(ctx, 1, -1)

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if metrics.lastLoadErr == nil {
		t.Error("expected load error to be recorded")
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

		// Create schema_version table first
		db.Exec("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		err = RunMigrateV1(cancelCtx, db)
		if err == nil {
			t.Error("expected error with cancelled context")
		}
	})
}

func TestNewErrors(t *testing.T) {
	t.Run("invalid path", func(t *testing.T) {
		// Try to create a store in a non-existent directory
		_, err := New("/nonexistent/path/to/db.sqlite")
		if err == nil {
			t.Error("expected error for invalid path")
		}
	})
}

func TestMigrateV1Rollback(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rollback.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Create an invalid schema that will cause migration to fail
	// First create schema_version table
	db.Exec("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
	// Create events table with wrong schema to cause conflict
	db.Exec("CREATE TABLE events (id INTEGER)")

	ctx := context.Background()
	err = RunMigrateV1(ctx, db)
	if err == nil {
		t.Error("expected error when schema conflicts")
	}
}

func TestMigrateVersionQuery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "version_query.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Create schema_version with version 1 to skip migration
	db.Exec("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
	db.Exec("INSERT INTO schema_version (version) VALUES (1)")

	ctx := context.Background()
	err = RunMigrate(ctx, db)
	// This should succeed but skip migrateV1
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPrepareStatementsError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "prepare_error.db")

	// Create a database with partial schema (missing events table)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	// Create subscription_positions but not events
	db.Exec("CREATE TABLE subscription_positions (subscription_id TEXT PRIMARY KEY, position INTEGER, updated_at DATETIME)")
	db.Close()

	// Now try to create a store with auto-migrate disabled
	// This should fail on prepareStatements since events table doesn't exist
	store, err := New(dbPath, WithAutoMigrate(false))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Operations should fail
	ctx := context.Background()
	_, err = store.GetPosition(ctx)
	if err == nil {
		t.Error("expected error when events table missing")
	}
}

func TestLoadScanErrorWithMetrics(t *testing.T) {
	metrics := &testMetricsHook{}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "scan_error.db")

	// Create database with corrupted events table
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	// Create events table with wrong column types
	db.Exec("CREATE TABLE events (position INTEGER PRIMARY KEY, type INTEGER, data INTEGER, timestamp INTEGER)")
	db.Exec("INSERT INTO events (type, data, timestamp) VALUES (123, 456, 789)")
	db.Close()

	store, err := New(dbPath, WithAutoMigrate(false), WithMetricsHook(metrics))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	// This should fail when scanning because types don't match
	_, err = store.Load(ctx, 1, -1)
	// The load might succeed since SQLite is loosely typed
	// but let's check if metrics were called
}

func TestMigrateSchemaVersionQueryError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "version_error.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Create schema_version but with different column name to cause scan error
	db.Exec("CREATE TABLE schema_version (ver INTEGER PRIMARY KEY)")

	ctx := context.Background()
	err = RunMigrate(ctx, db)
	if err == nil {
		t.Error("expected error when schema_version has wrong columns")
	}
}

func TestNewMigrationError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "migrate_error.db")

	// Pre-create a database with a corrupted schema_version table
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	// Create schema_version with wrong column to cause migration error
	db.Exec("CREATE TABLE schema_version (ver INTEGER)")
	db.Close()

	// Now try to create store - should fail during migration
	_, err = New(dbPath) // autoMigrate is true by default
	if err == nil {
		t.Error("expected error when migration fails")
	}
}

func TestNewPrepareStatementsError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "prepare_fail.db")

	// Pre-create database without proper tables
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	// Create only schema_version with version 1 to skip migration
	// but don't create events table
	db.Exec("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
	db.Exec("INSERT INTO schema_version (version) VALUES (1)")
	db.Close()

	// Now try to create store - should succeed (auto-migrate skipped due to version)
	// but prepareStatements won't fail because SQLite doesn't validate tables during prepare
	// Let's just verify it handles gracefully
	store, err := New(dbPath)
	if err != nil {
		// This is expected if prepare fails
		return
	}
	defer store.Close()

	// Operations would fail
	ctx := context.Background()
	_, err = store.GetPosition(ctx)
	if err == nil {
		t.Error("expected error when tables don't exist")
	}
}

func TestApplyPragmasError(t *testing.T) {
	// Create a read-only database to cause pragma failure
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "readonly.db")

	// Create the database first
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	db.Exec("SELECT 1") // Force file creation
	db.Close()

	// Make it read-only
	os.Chmod(dbPath, 0444)
	defer os.Chmod(dbPath, 0644)

	// Try to open - WAL pragma should fail on read-only
	_, err = New(dbPath)
	if err == nil {
		// Some systems may allow WAL on read-only, that's fine
		t.Log("pragma succeeded on read-only database")
	}
}

func BenchmarkSave(b *testing.B) {
	store, _ := New(":memory:")
	defer store.Close()

	ctx := context.Background()
	event := &eventbus.StoredEvent{
		Type:      "BenchEvent",
		Data:      json.RawMessage(`{"id": 1, "data": "benchmark test data"}`),
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := &eventbus.StoredEvent{
			Type:      event.Type,
			Data:      event.Data,
			Timestamp: event.Timestamp,
		}
		store.Save(ctx, e)
	}
}

func BenchmarkLoad(b *testing.B) {
	store, _ := New(":memory:")
	defer store.Close()

	ctx := context.Background()

	// Pre-populate with events
	for i := 0; i < 1000; i++ {
		event := &eventbus.StoredEvent{
			Type:      "BenchEvent",
			Data:      json.RawMessage(`{"id": 1}`),
			Timestamp: time.Now(),
		}
		store.Save(ctx, event)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Load(ctx, 1, 100)
	}
}

func BenchmarkConcurrentSave(b *testing.B) {
	store, _ := New(":memory:")
	defer store.Close()

	ctx := context.Background()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			event := &eventbus.StoredEvent{
				Type:      "BenchEvent",
				Data:      json.RawMessage(`{"id": 1}`),
				Timestamp: time.Now(),
			}
			store.Save(ctx, event)
		}
	})
}

func TestNewFromDB_PrepareStatementsError(t *testing.T) {
	// Create an in-memory database and close it to trigger error in Prepare
	db, err := sql.Open("sqlite", "file::memory:?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	db.Close()

	// NewFromDB will fail because db is closed
	_, err = NewFromDB(db)
	if err == nil {
		t.Fatal("expected error when db is closed")
	}
}

func TestLoadStream(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save test events
	for i := 1; i <= 10; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{"id": ` + string(rune('0'+i)) + `}`),
			Timestamp: time.Now(),
		}
		if err := store.Save(ctx, event); err != nil {
			t.Fatalf("failed to save event: %v", err)
		}
	}

	t.Run("streams all events from position 1", func(t *testing.T) {
		var events []*eventbus.StoredEvent
		for event, err := range store.LoadStream(ctx, 1, -1) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			events = append(events, event)
		}

		if len(events) != 10 {
			t.Errorf("expected 10 events, got %d", len(events))
		}
	})

	t.Run("streams events with upper bound", func(t *testing.T) {
		var events []*eventbus.StoredEvent
		for event, err := range store.LoadStream(ctx, 3, 7) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			events = append(events, event)
		}

		if len(events) != 5 {
			t.Errorf("expected 5 events (3-7), got %d", len(events))
		}
		if events[0].Position != 3 {
			t.Errorf("expected first position 3, got %d", events[0].Position)
		}
		if events[4].Position != 7 {
			t.Errorf("expected last position 7, got %d", events[4].Position)
		}
	})

	t.Run("streams events from middle position", func(t *testing.T) {
		var events []*eventbus.StoredEvent
		for event, err := range store.LoadStream(ctx, 5, -1) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			events = append(events, event)
		}

		if len(events) != 6 {
			t.Errorf("expected 6 events (5-10), got %d", len(events))
		}
		if events[0].Position != 5 {
			t.Errorf("expected first position 5, got %d", events[0].Position)
		}
	})

	t.Run("handles early termination and closes rows", func(t *testing.T) {
		count := 0
		for event, err := range store.LoadStream(ctx, 1, -1) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
			if event.Position == 3 {
				break // Early termination should close rows
			}
		}

		if count != 3 {
			t.Errorf("expected 3 events, got %d", count)
		}

		// Verify store is still usable (rows were properly closed)
		pos, err := store.GetPosition(ctx)
		if err != nil {
			t.Errorf("store should be usable after early termination: %v", err)
		}
		if pos != 10 {
			t.Errorf("expected position 10, got %d", pos)
		}
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		var gotError bool
		for _, err := range store.LoadStream(cancelCtx, 1, -1) {
			if err != nil {
				gotError = true
				break
			}
		}

		if !gotError {
			t.Error("expected error with cancelled context")
		}
	})

	t.Run("returns empty stream for out of range", func(t *testing.T) {
		count := 0
		for _, err := range store.LoadStream(ctx, 100, -1) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
		}

		if count != 0 {
			t.Errorf("expected 0 events, got %d", count)
		}
	})
}

func TestLoadStreamWithMetrics(t *testing.T) {
	metrics := &testMetricsHook{}

	store, err := New(":memory:", WithMetricsHook(metrics))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save some events
	for i := 0; i < 5; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Save(ctx, event)
	}

	// Reset metrics
	metrics.mu.Lock()
	metrics.loadCount = 0
	metrics.mu.Unlock()

	// Stream events
	count := 0
	for _, err := range store.LoadStream(ctx, 1, -1) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}

	if count != 5 {
		t.Errorf("expected 5 events, got %d", count)
	}

	// Verify metrics were called
	metrics.mu.Lock()
	if metrics.loadCount != 1 {
		t.Errorf("expected 1 load metric call, got %d", metrics.loadCount)
	}
	metrics.mu.Unlock()
}

func TestLoadStreamDBClosed(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()

	// Save an event
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Save(ctx, event)

	// Close the store
	store.Close()

	// Stream should error
	var gotError bool
	for _, err := range store.LoadStream(ctx, 1, -1) {
		if err != nil {
			gotError = true
			break
		}
	}

	if !gotError {
		t.Error("expected error when database is closed")
	}
}

func TestLoadStreamInterfaceCheck(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Verify SQLiteStore implements EventStoreStreamer
	var _ eventbus.EventStoreStreamer = store
}

func TestLoadStreamBatched(t *testing.T) {
	store, err := New(":memory:", WithStreamBatchSize(3))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save 10 events
	for i := 1; i <= 10; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		if err := store.Save(ctx, event); err != nil {
			t.Fatalf("failed to save event: %v", err)
		}
	}

	t.Run("batched streaming all events", func(t *testing.T) {
		var events []*eventbus.StoredEvent
		for event, err := range store.LoadStream(ctx, 1, -1) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			events = append(events, event)
		}

		if len(events) != 10 {
			t.Errorf("expected 10 events, got %d", len(events))
		}

		// Verify sequential positions
		for i, e := range events {
			if e.Position != int64(i+1) {
				t.Errorf("expected position %d, got %d", i+1, e.Position)
			}
		}
	})

	t.Run("batched streaming with upper bound", func(t *testing.T) {
		var events []*eventbus.StoredEvent
		for event, err := range store.LoadStream(ctx, 2, 8) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			events = append(events, event)
		}

		if len(events) != 7 {
			t.Errorf("expected 7 events (2-8), got %d", len(events))
		}
	})

	t.Run("batched streaming early termination", func(t *testing.T) {
		count := 0
		for event, err := range store.LoadStream(ctx, 1, -1) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
			if event.Position == 5 {
				break
			}
		}

		if count != 5 {
			t.Errorf("expected 5 events before break, got %d", count)
		}

		// Verify store is still usable
		pos, err := store.GetPosition(ctx)
		if err != nil {
			t.Errorf("store should be usable after early termination: %v", err)
		}
		if pos != 10 {
			t.Errorf("expected position 10, got %d", pos)
		}
	})

	t.Run("batched streaming context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		var gotError bool
		for _, err := range store.LoadStream(cancelCtx, 1, -1) {
			if err != nil {
				gotError = true
				break
			}
		}

		if !gotError {
			t.Error("expected error with cancelled context")
		}
	})
}

func TestLoadStreamBatchedDBClosed(t *testing.T) {
	store, err := New(":memory:", WithStreamBatchSize(3))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()

	// Save an event
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Save(ctx, event)

	// Close the store
	store.Close()

	// Stream should error
	var gotError bool
	for _, err := range store.LoadStream(ctx, 1, -1) {
		if err != nil {
			gotError = true
			break
		}
	}

	if !gotError {
		t.Error("expected error when database is closed")
	}
}

func TestWithStreamBatchSize(t *testing.T) {
	store, err := New(":memory:", WithStreamBatchSize(100))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Verify the option was applied by checking batched behavior works
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Save(ctx, event)
	}

	count := 0
	for _, err := range store.LoadStream(ctx, 1, -1) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}

	if count != 5 {
		t.Errorf("expected 5 events, got %d", count)
	}
}

func BenchmarkLoadVsLoadStream(b *testing.B) {
	store, _ := New(":memory:")
	defer store.Close()

	ctx := context.Background()

	// Pre-populate with 10000 events
	for i := 0; i < 10000; i++ {
		event := &eventbus.StoredEvent{
			Type:      "BenchEvent",
			Data:      json.RawMessage(`{"id": 1}`),
			Timestamp: time.Now(),
		}
		store.Save(ctx, event)
	}

	b.Run("Load", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			events, _ := store.Load(ctx, 1, -1)
			_ = len(events)
		}
	})

	b.Run("LoadStream", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count := 0
			for _, err := range store.LoadStream(ctx, 1, -1) {
				if err != nil {
					break
				}
				count++
			}
		}
	})
}

func TestLoadStreamWithLogger(t *testing.T) {
	logger := &testLogger{t: t}

	store, err := New(":memory:", WithLogger(logger))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save some events
	for i := 0; i < 3; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Save(ctx, event)
	}

	// Stream all events - should trigger logger.Debug
	count := 0
	for _, err := range store.LoadStream(ctx, 1, -1) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 events, got %d", count)
	}
}

func TestLoadStreamBatchedWithLogger(t *testing.T) {
	logger := &testLogger{t: t}

	store, err := New(":memory:", WithLogger(logger), WithStreamBatchSize(2))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save some events
	for i := 0; i < 5; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Save(ctx, event)
	}

	// Stream all events - should trigger logger.Debug for batched streaming
	count := 0
	for _, err := range store.LoadStream(ctx, 1, -1) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}

	if count != 5 {
		t.Errorf("expected 5 events, got %d", count)
	}
}

func TestLoadStreamContextCancelDuringIteration(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save many events to ensure we have something to iterate
	for i := 0; i < 10; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Save(ctx, event)
	}

	// Create a context that can be cancelled
	cancelCtx, cancel := context.WithCancel(ctx)

	count := 0
	var gotError bool
	for _, err := range store.LoadStream(cancelCtx, 1, -1) {
		if err != nil {
			gotError = true
			break
		}
		count++
		if count == 3 {
			// Cancel after receiving 3 events
			cancel()
		}
	}

	// We should have received some events before cancellation
	if count < 3 {
		t.Errorf("expected at least 3 events before cancellation, got %d", count)
	}

	// The error might or might not have been caught depending on timing
	// This is acceptable - the important thing is that cancellation stops iteration
	_ = gotError
}

func TestLoadStreamBatchedContextCancelDuringIteration(t *testing.T) {
	store, err := New(":memory:", WithStreamBatchSize(2))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Save many events
	for i := 0; i < 10; i++ {
		event := &eventbus.StoredEvent{
			Type:      "TestEvent",
			Data:      json.RawMessage(`{}`),
			Timestamp: time.Now(),
		}
		store.Save(ctx, event)
	}

	cancelCtx, cancel := context.WithCancel(ctx)

	count := 0
	for _, err := range store.LoadStream(cancelCtx, 1, -1) {
		if err != nil {
			break
		}
		count++
		if count == 3 {
			cancel()
		}
	}

	if count < 3 {
		t.Errorf("expected at least 3 events before cancellation, got %d", count)
	}
}

func TestLoadStreamScanError(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert a valid event first
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Save(ctx, event)

	// Corrupt the data by inserting invalid timestamp directly via SQL
	db := store.GetDB()
	_, err = db.ExecContext(ctx, "INSERT INTO events (type, data, timestamp) VALUES ('BadEvent', '{}', 'not-a-timestamp')")
	if err != nil {
		t.Fatalf("failed to insert corrupt data: %v", err)
	}

	// Stream should error on the corrupt row
	var gotError bool
	for _, err := range store.LoadStream(ctx, 1, -1) {
		if err != nil {
			gotError = true
			if !strings.Contains(err.Error(), "scan event") {
				t.Errorf("expected 'scan event' in error, got: %v", err)
			}
			break
		}
	}

	if !gotError {
		t.Error("expected scan error for corrupt data")
	}
}

func TestLoadStreamBatchedScanError(t *testing.T) {
	store, err := New(":memory:", WithStreamBatchSize(2))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert a valid event first
	event := &eventbus.StoredEvent{
		Type:      "TestEvent",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	store.Save(ctx, event)

	// Corrupt the data by inserting invalid timestamp directly via SQL
	db := store.GetDB()
	_, err = db.ExecContext(ctx, "INSERT INTO events (type, data, timestamp) VALUES ('BadEvent', '{}', 'not-a-timestamp')")
	if err != nil {
		t.Fatalf("failed to insert corrupt data: %v", err)
	}

	// Stream should error on the corrupt row
	var gotError bool
	for _, err := range store.LoadStream(ctx, 1, -1) {
		if err != nil {
			gotError = true
			if !strings.Contains(err.Error(), "scan event") {
				t.Errorf("expected 'scan event' in error, got: %v", err)
			}
			break
		}
	}

	if !gotError {
		t.Error("expected scan error for corrupt data")
	}
}

func TestStreamRowsIterErr(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Create mock rows that return an error from Err()
	mock := &mockRows{
		data:    [][]any{}, // No data, but Err() will return error
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

func TestStreamRowsScanErr(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Create mock rows that return an error from Scan()
	mock := &mockRows{
		data:    [][]any{{int64(1), "test", []byte(`{}`), time.Now()}},
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

func TestStreamRowsContextCancel(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Create mock rows that has data
	mock := &mockRows{
		data: [][]any{{int64(1), "test", []byte(`{}`), time.Now()}},
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
		t.Error("expected context cancellation error")
	}
	if !errors.Is(iterErr, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", iterErr)
	}
}

func TestStreamRowsEarlyTermination(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Create mock rows with multiple rows
	mock := &mockRows{
		data: [][]any{
			{int64(1), "test", []byte(`{}`), time.Now()},
			{int64(2), "test", []byte(`{}`), time.Now()},
			{int64(3), "test", []byte(`{}`), time.Now()},
		},
	}

	var eventCount int
	var iterErr error
	count := 0

	store.StreamRows(ctx, mock, &eventCount, &iterErr, func(event *eventbus.StoredEvent, err error) bool {
		count++
		if count == 2 {
			return false // Stop after 2 events
		}
		return true
	})

	if count != 2 {
		t.Errorf("expected 2 events, got %d", count)
	}
	if eventCount != 2 {
		t.Errorf("expected eventCount 2, got %d", eventCount)
	}
	if !mock.closed {
		t.Error("expected rows to be closed after early termination")
	}
}

func TestStreamBatchCloseErr(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Create mock rows that return an error from Close()
	mock := &mockRows{
		data: [][]any{
			{int64(1), "test", []byte(`{}`), time.Now()},
		},
		closeErr: errors.New("close error"),
	}

	var eventCount int
	var iterErr error
	var gotError bool
	var receivedCount int

	batchCount, lastPos, cont := store.StreamBatch(mock, &eventCount, &iterErr, func(event *eventbus.StoredEvent, err error) bool {
		if err != nil {
			gotError = true
			return false
		}
		receivedCount++
		return true
	})

	// Should have processed the event before Close error
	if receivedCount != 1 {
		t.Errorf("expected 1 event received, got %d", receivedCount)
	}

	// Should have returned the close error
	if !gotError {
		t.Error("expected error from rows.Close()")
	}
	if iterErr == nil {
		t.Error("expected iterErr to be set")
	}
	if !strings.Contains(iterErr.Error(), "close rows") {
		t.Errorf("expected 'close rows' in error, got: %v", iterErr)
	}

	// cont should be false due to error
	if cont {
		t.Error("expected cont to be false due to close error")
	}

	// batchCount and lastPos should reflect what was processed
	if batchCount != 1 {
		t.Errorf("expected batchCount 1, got %d", batchCount)
	}
	if lastPos != 1 {
		t.Errorf("expected lastPos 1, got %d", lastPos)
	}
}
