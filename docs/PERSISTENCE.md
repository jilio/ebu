# Event Persistence Guide

Complete guide to event persistence, storage, and replay in ebu.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Event Stores](#event-stores)
- [Replay Patterns](#replay-patterns)
- [Resumable Subscriptions](#resumable-subscriptions)
- [Custom Stores](#custom-stores)
- [Best Practices](#best-practices)

## Overview

ebu includes built-in support for event persistence, enabling:

- **Event Sourcing** - Store all state changes as events
- **Audit Logging** - Keep an immutable log of all actions
- **Message Recovery** - Resume processing after crashes
- **Event Replay** - Rebuild state or reprocess events
- **Debugging** - Analyze event flow in production
- **Integration** - Share events between services via storage

## Quick Start

### Basic Persistence

```go
import (
    "context"
    "fmt"

    eventbus "github.com/jilio/ebu"
)

type UserCreatedEvent struct {
    UserID   string
    Username string
    Email    string
}

func main() {
    // Create a persistent event bus with in-memory store
    store := eventbus.NewMemoryStore()
    bus := eventbus.New(eventbus.WithStore(store))

    // Use the bus normally - persistence is transparently handled
    eventbus.Publish(bus, UserCreatedEvent{
        UserID:   "123",
        Username: "john_doe",
        Email:    "john@example.com",
    })

    // All events are automatically persisted!
}
```

### Checking if Persistence is Enabled

```go
if bus.IsPersistent() {
    fmt.Println("Events are being persisted")
}

// Get the underlying store
store := bus.GetStore()
```

## Event Stores

### MemoryStore

Built-in in-memory store for development and testing:

```go
store := eventbus.NewMemoryStore()
bus := eventbus.New(eventbus.WithStore(store))

// Events are stored in memory
// Note: All data is lost when the process exits
```

**When to use:**
- Development and testing
- Short-lived processes
- Non-critical event buffering

**NOT for:**
- Production persistence
- Long-term storage
- Critical data

### StoredEvent Structure

Events are stored with metadata:

```go
type StoredEvent struct {
    Position  int64           // Sequential position number
    Type      string          // Event type name (e.g., "main.UserCreatedEvent")
    Data      json.RawMessage // JSON-encoded event data
    Timestamp time.Time       // When the event was stored
}
```

## Replay Patterns

### Basic Replay

Replay all events from a specific position:

```go
ctx := context.Background()

err := bus.Replay(ctx, 0, func(event *eventbus.StoredEvent) error {
    fmt.Printf("Position %d: %s at %v\n",
        event.Position, event.Type, event.Timestamp)

    // Process the event
    return nil
})

if err != nil {
    log.Fatal(err)
}
```

### Type-Safe Replay

Use `EventType` helper for type-safe event handling:

```go
import (
    "encoding/json"
    eventbus "github.com/jilio/ebu"
)

bus.Replay(ctx, 0, func(event *eventbus.StoredEvent) error {
    switch event.Type {
    case eventbus.EventType(UserCreatedEvent{}):
        var user UserCreatedEvent
        if err := json.Unmarshal(event.Data, &user); err != nil {
            return err
        }
        // Process user event
        fmt.Printf("User created: %s\n", user.Username)

    case eventbus.EventType(OrderCreatedEvent{}):
        var order OrderCreatedEvent
        if err := json.Unmarshal(event.Data, &order); err != nil {
            return err
        }
        // Process order event
        fmt.Printf("Order created: %s\n", order.OrderID)
    }

    return nil
})
```

### Replay with Upcasting

Automatically transform old event versions during replay:

```go
// Old V1 events are automatically upcasted to V2
bus.ReplayWithUpcast(ctx, 0, func(event *eventbus.StoredEvent) error {
    // event.Type and event.Data are already transformed
    if event.Type == eventbus.EventType(UserCreatedV2{}) {
        var user UserCreatedV2
        json.Unmarshal(event.Data, &user)
        // Process as V2, even if originally V1!
    }
    return nil
})
```

### Ranged Replay

Replay a specific range of events:

```go
// Get current position
pos, _ := store.GetPosition(ctx)

// Replay only recent events (last 100)
from := pos - 100
if from < 0 {
    from = 0
}

bus.Replay(ctx, from, func(event *eventbus.StoredEvent) error {
    // Process only recent events
    return nil
})
```

## Resumable Subscriptions

Subscribe with automatic position tracking - never miss or duplicate events:

```go
type EmailNotification struct {
    To      string
    Subject string
    Body    string
}

func main() {
    store := eventbus.NewMemoryStore()
    bus := eventbus.New(eventbus.WithStore(store))

    // Subscribe with position tracking
    err := eventbus.SubscribeWithReplay(bus, "email-sender",
        func(event EmailNotification) {
            sendEmail(event.To, event.Subject, event.Body)
            // Position is automatically saved after successful handling
        })

    if err != nil {
        log.Fatal(err)
    }

    // After restart, the subscription resumes from last position
    // No events are missed or duplicated!
}
```

### How Resumable Subscriptions Work

1. **First Run**: Subscription starts from position 0, replays all historical events
2. **Normal Operation**: New events are handled as they're published
3. **After Crash**: On restart, resumes from last successfully processed position
4. **Position Tracking**: Position is saved after each successful event handling

### Multiple Subscriptions

Different subscriptions track positions independently:

```go
// Email sender tracks its own position
eventbus.SubscribeWithReplay(bus, "email-sender", emailHandler)

// Analytics tracker tracks separately
eventbus.SubscribeWithReplay(bus, "analytics", analyticsHandler)

// If email-sender crashes, it resumes from its last position
// Analytics continues unaffected
```

## Custom Stores

### EventStore Interface

Implement this interface for custom storage backends:

```go
type EventStore interface {
    // Save a single event
    Save(ctx context.Context, event *StoredEvent) error

    // Load events from position 'from' to 'to' (-1 for unlimited)
    Load(ctx context.Context, from, to int64) ([]*StoredEvent, error)

    // Get the current highest position
    GetPosition(ctx context.Context) (int64, error)

    // Save subscription position for resumable subscriptions
    SaveSubscriptionPosition(ctx context.Context, subscriptionID string, position int64) error

    // Load subscription position
    LoadSubscriptionPosition(ctx context.Context, subscriptionID string) (int64, error)
}
```

### PostgreSQL Example

```go
import (
    "context"
    "database/sql"
    "encoding/json"
    "time"

    eventbus "github.com/jilio/ebu"
    _ "github.com/lib/pq"
)

type PostgresStore struct {
    db *sql.DB
}

func NewPostgresStore(connectionString string) (*PostgresStore, error) {
    db, err := sql.Open("postgres", connectionString)
    if err != nil {
        return nil, err
    }

    // Create tables
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS events (
            position BIGSERIAL PRIMARY KEY,
            type TEXT NOT NULL,
            data JSONB NOT NULL,
            timestamp TIMESTAMP NOT NULL
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            subscription_id TEXT PRIMARY KEY,
            position BIGINT NOT NULL
        );
    `)
    if err != nil {
        return nil, err
    }

    return &PostgresStore{db: db}, nil
}

func (p *PostgresStore) Save(ctx context.Context, event *eventbus.StoredEvent) error {
    _, err := p.db.ExecContext(ctx, `
        INSERT INTO events (type, data, timestamp)
        VALUES ($1, $2, $3)
        RETURNING position
    `, event.Type, event.Data, event.Timestamp)

    return err
}

func (p *PostgresStore) Load(ctx context.Context, from, to int64) ([]*eventbus.StoredEvent, error) {
    var query string
    var args []interface{}

    if to == -1 {
        query = "SELECT position, type, data, timestamp FROM events WHERE position >= $1 ORDER BY position"
        args = []interface{}{from}
    } else {
        query = "SELECT position, type, data, timestamp FROM events WHERE position >= $1 AND position <= $2 ORDER BY position"
        args = []interface{}{from, to}
    }

    rows, err := p.db.QueryContext(ctx, query, args...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var events []*eventbus.StoredEvent
    for rows.Next() {
        event := &eventbus.StoredEvent{}
        if err := rows.Scan(&event.Position, &event.Type, &event.Data, &event.Timestamp); err != nil {
            return nil, err
        }
        events = append(events, event)
    }

    return events, rows.Err()
}

func (p *PostgresStore) GetPosition(ctx context.Context) (int64, error) {
    var position int64
    err := p.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(position), 0) FROM events").Scan(&position)
    return position, err
}

func (p *PostgresStore) SaveSubscriptionPosition(ctx context.Context, subscriptionID string, position int64) error {
    _, err := p.db.ExecContext(ctx, `
        INSERT INTO subscriptions (subscription_id, position)
        VALUES ($1, $2)
        ON CONFLICT (subscription_id) DO UPDATE SET position = $2
    `, subscriptionID, position)

    return err
}

func (p *PostgresStore) LoadSubscriptionPosition(ctx context.Context, subscriptionID string) (int64, error) {
    var position int64
    err := p.db.QueryRowContext(ctx,
        "SELECT position FROM subscriptions WHERE subscription_id = $1",
        subscriptionID).Scan(&position)

    if err == sql.ErrNoRows {
        return 0, nil
    }

    return position, err
}

// Usage
func main() {
    store, err := NewPostgresStore("postgres://user:pass@localhost/eventstore")
    if err != nil {
        log.Fatal(err)
    }

    bus := eventbus.New(eventbus.WithStore(store))
    // Events are now persisted to PostgreSQL!
}
```

### SQLite Example

SQLite is an excellent choice for single-node event stores - simple, reliable, and fast:

```go
import (
    "context"
    "database/sql"
    "encoding/json"

    eventbus "github.com/jilio/ebu"
    _ "github.com/mattn/go-sqlite3"
)

type SQLiteStore struct {
    db *sql.DB
}

func NewSQLiteStore(filepath string) (*SQLiteStore, error) {
    db, err := sql.Open("sqlite3", filepath)
    if err != nil {
        return nil, err
    }

    // Create tables with optimal indexes
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS events (
            position INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            data BLOB NOT NULL,
            timestamp DATETIME NOT NULL
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            subscription_id TEXT PRIMARY KEY,
            position INTEGER NOT NULL
        );

        -- Index for efficient range queries
        CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
    `)
    if err != nil {
        return nil, err
    }

    // Enable WAL mode for better concurrency
    db.Exec("PRAGMA journal_mode=WAL")

    return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Save(ctx context.Context, event *eventbus.StoredEvent) error {
    result, err := s.db.ExecContext(ctx, `
        INSERT INTO events (type, data, timestamp)
        VALUES (?, ?, ?)
    `, event.Type, event.Data, event.Timestamp)

    if err != nil {
        return err
    }

    // Get the auto-generated position
    pos, err := result.LastInsertId()
    if err != nil {
        return err
    }

    event.Position = pos
    return nil
}

func (s *SQLiteStore) Load(ctx context.Context, from, to int64) ([]*eventbus.StoredEvent, error) {
    var query string
    var args []interface{}

    if to == -1 {
        query = "SELECT position, type, data, timestamp FROM events WHERE position >= ? ORDER BY position"
        args = []interface{}{from}
    } else {
        query = "SELECT position, type, data, timestamp FROM events WHERE position >= ? AND position <= ? ORDER BY position"
        args = []interface{}{from, to}
    }

    rows, err := s.db.QueryContext(ctx, query, args...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var events []*eventbus.StoredEvent
    for rows.Next() {
        event := &eventbus.StoredEvent{}
        if err := rows.Scan(&event.Position, &event.Type, &event.Data, &event.Timestamp); err != nil {
            return nil, err
        }
        events = append(events, event)
    }

    return events, rows.Err()
}

func (s *SQLiteStore) GetPosition(ctx context.Context) (int64, error) {
    var position int64
    err := s.db.QueryRowContext(ctx,
        "SELECT COALESCE(MAX(position), 0) FROM events").Scan(&position)
    return position, err
}

func (s *SQLiteStore) SaveSubscriptionPosition(ctx context.Context, subscriptionID string, position int64) error {
    _, err := s.db.ExecContext(ctx, `
        INSERT OR REPLACE INTO subscriptions (subscription_id, position)
        VALUES (?, ?)
    `, subscriptionID, position)
    return err
}

func (s *SQLiteStore) LoadSubscriptionPosition(ctx context.Context, subscriptionID string) (int64, error) {
    var position int64
    err := s.db.QueryRowContext(ctx,
        "SELECT position FROM subscriptions WHERE subscription_id = ?",
        subscriptionID).Scan(&position)

    if err == sql.ErrNoRows {
        return 0, nil
    }

    return position, err
}

// Usage
func main() {
    store, err := NewSQLiteStore("./events.db")
    if err != nil {
        log.Fatal(err)
    }

    bus := eventbus.New(eventbus.WithStore(store))
    // Events are now persisted to SQLite!
}
```

## Persistence Configuration

### Error Handling

Set a custom error handler for persistence failures:

```go
bus := eventbus.New(
    eventbus.WithStore(store),
    eventbus.WithPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
        log.Printf("Failed to persist %v: %v", eventType, err)
        // Send to error tracking service, retry queue, etc.
    }),
)

// Can also be set at runtime
bus.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
    metrics.Increment("persistence.errors")
})
```

### Persistence Timeout

Set a timeout for store operations:

```go
bus := eventbus.New(
    eventbus.WithStore(store),
    eventbus.WithPersistenceTimeout(5 * time.Second),
)
```

## Best Practices

### 1. Handle Persistence Errors

```go
bus.SetPersistenceErrorHandler(func(event any, eventType reflect.Type, err error) {
    // Log the error
    log.Printf("Persistence error for %v: %v", eventType, err)

    // Send to dead letter queue
    dlq.Send(event, err)

    // Alert monitoring
    alerts.Send("Event persistence failed", err)
})
```

### 2. Use Resumable Subscriptions

For critical event processing:

```go
// BAD - events missed after crash
eventbus.Subscribe(bus, criticalHandler)

// GOOD - resumes after crash
eventbus.SubscribeWithReplay(bus, "critical-processor", criticalHandler)
```

### 3. Implement Proper Indexing

For database stores, create indexes on frequently queried fields:

```sql
-- PostgreSQL
CREATE INDEX idx_events_position ON events(position);
CREATE INDEX idx_events_type ON events(type);
CREATE INDEX idx_events_timestamp ON events(timestamp);
```

### 4. Consider Event Retention

Implement archival or cleanup for old events:

```go
// Archive events older than 90 days
func archiveOldEvents(store *PostgresStore) error {
    cutoff := time.Now().AddDate(0, 0, -90)
    _, err := store.db.Exec(`
        INSERT INTO events_archive SELECT * FROM events WHERE timestamp < $1;
        DELETE FROM events WHERE timestamp < $1;
    `, cutoff)
    return err
}
```

### 5. Test Recovery Scenarios

```go
func TestSubscriptionResumption(t *testing.T) {
    store := eventbus.NewMemoryStore()
    bus := eventbus.New(eventbus.WithStore(store))

    // Publish some events
    for i := 0; i < 10; i++ {
        eventbus.Publish(bus, TestEvent{ID: i})
    }

    // Subscribe and process 5 events
    count := 0
    eventbus.SubscribeWithReplay(bus, "test",
        func(event TestEvent) {
            count++
            if count == 5 {
                panic("simulate crash")
            }
        })

    // Resume subscription - should start from position 5
    eventbus.SubscribeWithReplay(bus, "test",
        func(event TestEvent) {
            // Processes events 5-9
        })
}
```

## Use Cases

### Event Sourcing

```go
type AccountOpened struct{ AccountID, CustomerID string }
type MoneyDeposited struct{ AccountID string; Amount float64 }
type MoneyWithdrawn struct{ AccountID string; Amount float64 }

// Rebuild account state from events
func rebuildAccount(bus *eventbus.EventBus, accountID string) (*Account, error) {
    account := &Account{ID: accountID}

    err := bus.Replay(ctx, 0, func(event *eventbus.StoredEvent) error {
        switch event.Type {
        case eventbus.EventType(MoneyDeposited{}):
            var e MoneyDeposited
            json.Unmarshal(event.Data, &e)
            if e.AccountID == accountID {
                account.Balance += e.Amount
            }
        case eventbus.EventType(MoneyWithdrawn{}):
            var e MoneyWithdrawn
            json.Unmarshal(event.Data, &e)
            if e.AccountID == accountID {
                account.Balance -= e.Amount
            }
        }
        return nil
    })

    return account, err
}
```

### Audit Trail

```go
// All events are automatically stored for audit purposes
bus := eventbus.New(eventbus.WithStore(postgresStore))

// Query audit trail
events, _ := postgresStore.Load(ctx, 0, -1)
for _, event := range events {
    fmt.Printf("%v: %s - %s\n", event.Timestamp, event.Type, event.Data)
}
```

### Event Replay for Testing

```go
// Capture production events
productionStore := NewPostgresStore(prodConnection)

// Replay in test environment
testBus := eventbus.New()
productionStore.Load(ctx, 0, -1)

// Replay events to test new handler logic
testBus.Replay(ctx, 0, func(event *eventbus.StoredEvent) error {
    // Test new behavior against real events
    return nil
})
```

## Summary

Event persistence in ebu provides:

- ✅ Simple API - Just add `WithStore(store)` option
- ✅ Flexible - Implement `EventStore` for any backend
- ✅ Reliable - Resumable subscriptions never miss events
- ✅ Powerful - Full replay and event sourcing support
- ✅ Production-ready - Error handling, timeouts, and monitoring

For more examples, see [EXAMPLES.md](EXAMPLES.md).
