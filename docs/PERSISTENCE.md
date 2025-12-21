# Event Persistence Guide

Complete guide to event persistence, storage, and replay in ebu.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Event Stores](#event-stores)
- [Available Storage Backends](#available-storage-backends)
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
    Offset    Offset          // Opaque position in the stream
    Type      string          // Event type name (e.g., "main.UserCreatedEvent")
    Data      json.RawMessage // JSON-encoded event data
    Timestamp time.Time       // When the event was stored
}

// Special offset constants
const (
    OffsetOldest Offset = ""   // Beginning of stream
    OffsetNewest Offset = "$"  // Current end of stream
)
```

## Available Storage Backends

ebu provides several storage backends:

| Backend | Package | Description |
|---------|---------|-------------|
| MemoryStore | `github.com/jilio/ebu` | In-memory store for development/testing |
| SQLite | `github.com/jilio/ebu/stores/sqlite` | Local persistent storage with WAL mode |
| Durable-Streams | `github.com/jilio/ebu/stores/durablestream` | Remote HTTP-based storage |

### SQLite Store

Production-ready local storage with excellent performance:

```go
import (
    eventbus "github.com/jilio/ebu"
    "github.com/jilio/ebu/stores/sqlite"
)

// Create SQLite store with WAL mode and optimizations
store, err := sqlite.New("./events.db",
    sqlite.WithBusyTimeout(5 * time.Second),
    sqlite.WithStreamBatchSize(1000), // For efficient streaming
)
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Use with event bus
bus := eventbus.New(eventbus.WithStore(store))
```

SQLite store features:
- WAL mode for better concurrency
- Prepared statements for performance
- Memory-efficient streaming with `ReadStream`
- Automatic schema migrations
- Optional logging and metrics hooks

### Durable-Streams Store

Remote storage using the [Durable Streams](https://electric-sql.com/blog/2025/12/09/announcing-durable-streams) protocol - an HTTP-based persistent stream primitive for reliable, resumable, real-time data streaming developed by [Electric](https://electric-sql.com):

```go
import (
    eventbus "github.com/jilio/ebu"
    "github.com/jilio/ebu/stores/durablestream"
)

// Connect to durable-streams server
store, err := durablestream.New("http://localhost:8080/v1/stream/events",
    durablestream.WithRetry(3, 100*time.Millisecond),
    durablestream.WithTimeout(30 * time.Second),
)
if err != nil {
    log.Fatal(err)
}

// Use with event bus
bus := eventbus.New(eventbus.WithStore(store))

// Events are persisted to the remote server
eventbus.Publish(bus, UserCreatedEvent{UserID: "123"})
```

Durable-streams features:
- HTTP-based protocol for distributed systems
- Automatic retries with exponential backoff
- Configurable timeouts
- Server-assigned opaque offsets

## Replay Patterns

### Basic Replay

Replay all events from a specific offset:

```go
ctx := context.Background()

err := bus.Replay(ctx, eventbus.OffsetOldest, func(event *eventbus.StoredEvent) error {
    fmt.Printf("Offset %s: %s at %v\n",
        event.Offset, event.Type, event.Timestamp)

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

bus.Replay(ctx, eventbus.OffsetOldest, func(event *eventbus.StoredEvent) error {
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
bus.ReplayWithUpcast(ctx, eventbus.OffsetOldest, func(event *eventbus.StoredEvent) error {
    // event.Type and event.Data are already transformed
    if event.Type == eventbus.EventType(UserCreatedV2{}) {
        var user UserCreatedV2
        json.Unmarshal(event.Data, &user)
        // Process as V2, even if originally V1!
    }
    return nil
})
```

### Replay from Specific Offset

Replay events starting from a specific offset:

```go
// Read events to find a starting point
events, nextOffset, _ := store.Read(ctx, eventbus.OffsetOldest, 100)

// Continue reading from where we left off
bus.Replay(ctx, nextOffset, func(event *eventbus.StoredEvent) error {
    // Process events after the first 100
    return nil
})
```

## Resumable Subscriptions

Subscribe with automatic offset tracking - never miss or duplicate events:

```go
type EmailNotification struct {
    To      string
    Subject string
    Body    string
}

func main() {
    store := eventbus.NewMemoryStore()
    bus := eventbus.New(eventbus.WithStore(store))

    // Subscribe with offset tracking
    err := eventbus.SubscribeWithReplay(bus, "email-sender",
        func(event EmailNotification) {
            sendEmail(event.To, event.Subject, event.Body)
            // Offset is automatically saved after successful handling
        })

    if err != nil {
        log.Fatal(err)
    }

    // After restart, the subscription resumes from last offset
    // No events are missed or duplicated!
}
```

### How Resumable Subscriptions Work

1. **First Run**: Subscription starts from `OffsetOldest`, replays all historical events
2. **Normal Operation**: New events are handled as they're published
3. **After Crash**: On restart, resumes from last successfully processed offset
4. **Offset Tracking**: Offset is saved after each successful event handling

### Multiple Subscriptions

Different subscriptions track offsets independently:

```go
// Email sender tracks its own offset
eventbus.SubscribeWithReplay(bus, "email-sender", emailHandler)

// Analytics tracker tracks separately
eventbus.SubscribeWithReplay(bus, "analytics", analyticsHandler)

// If email-sender crashes, it resumes from its last offset
// Analytics continues unaffected
```

## Custom Stores

### EventStore Interface

Implement this interface for custom storage backends:

```go
// EventStore is the core interface (2 methods)
type EventStore interface {
    // Append stores an event and returns its assigned offset
    Append(ctx context.Context, event *Event) (Offset, error)

    // Read returns events starting after the given offset
    // Use OffsetOldest to read from the beginning
    // Use limit > 0 to limit results, or 0 for no limit
    Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error)
}

// SubscriptionStore is optional, for tracking subscription offsets
type SubscriptionStore interface {
    SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error
    LoadOffset(ctx context.Context, subscriptionID string) (Offset, error)
}

// EventStoreStreamer is optional, for memory-efficient streaming
type EventStoreStreamer interface {
    ReadStream(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error]
}
```

The new interface uses opaque string offsets instead of integer positions, allowing
for better compatibility with remote storage backends that may use non-sequential
identifiers.

### PostgreSQL Example

```go
import (
    "context"
    "database/sql"
    "strconv"

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

func (p *PostgresStore) Append(ctx context.Context, event *eventbus.Event) (eventbus.Offset, error) {
    var position int64
    err := p.db.QueryRowContext(ctx, `
        INSERT INTO events (type, data, timestamp)
        VALUES ($1, $2, $3)
        RETURNING position
    `, event.Type, event.Data, event.Timestamp).Scan(&position)

    if err != nil {
        return "", err
    }
    return eventbus.Offset(strconv.FormatInt(position, 10)), nil
}

func (p *PostgresStore) Read(ctx context.Context, from eventbus.Offset, limit int) ([]*eventbus.StoredEvent, eventbus.Offset, error) {
    var fromPos int64
    if from != eventbus.OffsetOldest {
        fromPos, _ = strconv.ParseInt(string(from), 10, 64)
    }

    var query string
    var args []interface{}

    if limit <= 0 {
        query = "SELECT position, type, data, timestamp FROM events WHERE position > $1 ORDER BY position"
        args = []interface{}{fromPos}
    } else {
        query = "SELECT position, type, data, timestamp FROM events WHERE position > $1 ORDER BY position LIMIT $2"
        args = []interface{}{fromPos, limit}
    }

    rows, err := p.db.QueryContext(ctx, query, args...)
    if err != nil {
        return nil, from, err
    }
    defer rows.Close()

    var events []*eventbus.StoredEvent
    var lastOffset eventbus.Offset = from

    for rows.Next() {
        var position int64
        event := &eventbus.StoredEvent{}
        if err := rows.Scan(&position, &event.Type, &event.Data, &event.Timestamp); err != nil {
            return nil, from, err
        }
        event.Offset = eventbus.Offset(strconv.FormatInt(position, 10))
        events = append(events, event)
        lastOffset = event.Offset
    }

    return events, lastOffset, rows.Err()
}

func (p *PostgresStore) SaveOffset(ctx context.Context, subscriptionID string, offset eventbus.Offset) error {
    position, _ := strconv.ParseInt(string(offset), 10, 64)
    _, err := p.db.ExecContext(ctx, `
        INSERT INTO subscriptions (subscription_id, position)
        VALUES ($1, $2)
        ON CONFLICT (subscription_id) DO UPDATE SET position = $2
    `, subscriptionID, position)

    return err
}

func (p *PostgresStore) LoadOffset(ctx context.Context, subscriptionID string) (eventbus.Offset, error) {
    var position int64
    err := p.db.QueryRowContext(ctx,
        "SELECT position FROM subscriptions WHERE subscription_id = $1",
        subscriptionID).Scan(&position)

    if err == sql.ErrNoRows {
        return eventbus.OffsetOldest, nil
    }
    if err != nil {
        return eventbus.OffsetOldest, err
    }

    return eventbus.Offset(strconv.FormatInt(position, 10)), nil
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

For SQLite storage, use the official `stores/sqlite` package:

```go
import (
    eventbus "github.com/jilio/ebu"
    "github.com/jilio/ebu/stores/sqlite"
)

func main() {
    // Create SQLite store with default options
    store, err := sqlite.New("./events.db")
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Or with custom options
    store, err := sqlite.New("./events.db",
        sqlite.WithBusyTimeout(10 * time.Second),
        sqlite.WithAutoMigrate(true),
        sqlite.WithStreamBatchSize(1000),
        sqlite.WithLogger(myLogger),
        sqlite.WithMetricsHook(myMetrics),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    bus := eventbus.New(eventbus.WithStore(store))
    // Events are now persisted to SQLite!
}
```

The SQLite store includes:
- Automatic schema migrations
- WAL mode for better concurrency
- Prepared statements for performance
- Memory-efficient streaming via `ReadStream`
- Configurable batch sizes for large datasets

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

    err := bus.Replay(ctx, eventbus.OffsetOldest, func(event *eventbus.StoredEvent) error {
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
events, _, _ := postgresStore.Read(ctx, eventbus.OffsetOldest, 0)
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
events, _, _ := productionStore.Read(ctx, eventbus.OffsetOldest, 0)

// Replay events to test new handler logic
testBus.Replay(ctx, eventbus.OffsetOldest, func(event *eventbus.StoredEvent) error {
    // Test new behavior against real events
    return nil
})
```

## Summary

Event persistence in ebu provides:

- ✅ Simple API - Just add `WithStore(store)` option
- ✅ Multiple backends - MemoryStore, SQLite, Durable-Streams, or custom
- ✅ Remote storage - Native support for distributed event storage
- ✅ Flexible - Implement `EventStore` interface for any backend
- ✅ Reliable - Resumable subscriptions never miss events
- ✅ Powerful - Full replay and event sourcing support
- ✅ Production-ready - Error handling, timeouts, and monitoring

For more examples, see [EXAMPLES.md](EXAMPLES.md).
