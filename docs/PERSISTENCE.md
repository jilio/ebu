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

`MemoryStore` owns its event envelopes: it copies mutable `Data` and `Metadata`
on append and returns fresh copies from both `Read` and `ReadStream`. Reusing an
input event or modifying a read result cannot rewrite its stored history.

**When to use:**
- Development and testing
- Short-lived processes
- Non-critical event buffering

**NOT for:**
- Production persistence
- Long-term storage
- Critical data

### StoredEvent Structure

Events are stored with an envelope:

```go
type StoredEvent struct {
    Offset    Offset            // Opaque position in the stream
    ID        string            // ULID assigned per publish (dedup/idempotency key)
    Origin    string            // ID of the bus instance that published it
    Type      string            // Event type name (e.g., "main.UserCreatedEvent")
    Data      json.RawMessage   // JSON-encoded event data
    Metadata  map[string]string // Publisher-supplied metadata (see ContextWithMetadata)
    Timestamp time.Time         // When the event was stored
}

// Special offset constants
const (
    OffsetOldest Offset = ""   // Beginning of stream
    OffsetNewest Offset = "$"  // Current end of stream
)
```

The envelope makes at-least-once delivery workable in practice:

- **`ID`** is a ULID minted once per publish, *before* the first `Append`
  attempt — a store that retries a failed append writes the same ID, so
  consumers can deduplicate on it. Handlers of live events can read it with
  `EventIDFromContext(ctx)`; context-aware resumable handlers get the same ID
  through `SubscribeContextWithReplay`.
- **`Origin`** is the publishing bus instance's unique ID (`bus.OriginID()`),
  letting consumers of a shared log tell their own events apart from a
  peer process's.
- **`Metadata`** carries correlation/causation IDs or any other tags:
  attach with `ContextWithMetadata(ctx, map[string]string{...})` at publish,
  read back via `StoredEvent.Metadata` or `MetadataFromContext(ctx)` in
  live and context-aware replay handlers.

All three fields are optional on the wire (`omitempty`, nullable columns), so
streams written by earlier ebu versions or external producers read back with
empty envelope fields.

`OffsetNewest` is a query sentinel, not a durable offset. Every `EventStore`
must resolve it efficiently at call time: `Read` returns no historical events
and returns the current concrete tail as `nextOffset` (or `OffsetOldest` for an
empty stream), without scanning or returning the stream's history. Persist that
concrete `nextOffset`, never the symbolic `"$"`. `ReadStream` likewise yields
no events and terminates; live following belongs to `EventStoreTailer`.

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

Remote storage using the [Durable Streams](https://electric-sql.com/blog/2025/12/09/announcing-durable-streams) protocol - an HTTP-based persistent stream primitive for reliable, resumable, real-time data streaming developed by [Electric](https://electric-sql.com).

This implementation uses the [ahimsalabs/durable-streams-go](https://github.com/ahimsalabs/durable-streams-go) client library, which passes the official durable-streams conformance test suite:

```go
import (
    eventbus "github.com/jilio/ebu"
    "github.com/jilio/ebu/stores/durablestream"
)

// Connect to durable-streams server
store, err := durablestream.New(
    "http://localhost:4437/v1/stream",  // server base URL
    "events",                            // stream name
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
- Full protocol conformance via tested client library
- Compatible with any durable-streams server implementation
- Configurable timeouts and HTTP client
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

For any event written to durable storage, implement `TypeNamer` with an
immutable, globally unique name. The default reflection name includes only
the declared package name, not its full import path, so it may change during a
refactor or collide with an unrelated package. A persistent bus rejects
conflicting Go types across append attempts, subscriptions, and typed upcast
endpoints. An `Append` error can be ambiguous—a remote store may commit before
its acknowledgement is lost—so the bus retains that type's wire-name claim
once the call is attempted. The same Go type can retry, but a colliding type is
rejected for the bus's lifetime. This is per bus; producers in other processes
must still coordinate globally unique names.

### Replay with Upcasting

Automatically transform old event versions during replay:

```go
// Old V1 events are automatically upcasted to V2
bus.ReplayWithUpcast(ctx, eventbus.OffsetOldest, func(event *eventbus.StoredEvent) error {
    // event.Type and event.Data are transformed. ID, Origin, Metadata,
    // Offset, and Timestamp are preserved from the original envelope.
    if event.Type == eventbus.EventType(UserCreatedV2{}) {
        var user UserCreatedV2
        json.Unmarshal(event.Data, &user)
        // Process as V2, even if originally V1!
    }
    return nil
})
```

For raw, string-keyed migrations registered with `RegisterUpcastFunc` or
`WithUpcast`, the callback must return exactly the `toType` declared at
registration. The registry owns the chain topology; returning a different or
empty type fails replay with `*UpcastContractError` instead of silently
rewriting the chain or delivering the original schema:

```go
err := eventbus.RegisterUpcastFunc(bus, "user.created.v1", "user.created.v2",
    func(data json.RawMessage) (json.RawMessage, string, error) {
        migrated, err := migrateUserCreated(data)
        return migrated, "user.created.v2", err // Must match the declared toType.
    })
if err != nil {
    log.Fatal(err)
}

err = bus.ReplayWithUpcast(ctx, eventbus.OffsetOldest, replayHandler)
var contractErr *eventbus.UpcastContractError
if errors.As(err, &contractErr) {
    log.Printf("invalid upcast %s -> %s: returned %s",
        contractErr.FromType, contractErr.DeclaredType, contractErr.ReturnedType)
}
```

Typed `RegisterUpcast` derives both endpoints and the returned type from its Go
signature, so this runtime contract check is specific to raw callbacks.

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

Subscribe with automatic offset tracking and at-least-once delivery:

```go
type EmailNotification struct {
    To      string
    Subject string
    Body    string
}

func main() {
    store := eventbus.NewMemoryStore()
    bus := eventbus.New(eventbus.WithStore(store))

    // Subscribe with offset tracking and the persisted event envelope.
    err := eventbus.SubscribeContextWithReplay(ctx, bus, "email-sender",
        func(deliveryCtx context.Context, event EmailNotification) {
            eventID, _ := eventbus.EventIDFromContext(deliveryCtx)
            sendEmailOnce(eventID, event.To, event.Subject, event.Body)
            // Offset is automatically saved after successful handling
        })

    if err != nil {
        log.Fatal(err)
    }

    // After restart, the subscription resumes from its last checkpoint.
    // A crash between the side effect and checkpoint can redeliver one or
    // more events, so the handler must be idempotent.
}
```

### How Resumable Subscriptions Work

1. **First Run**: Subscription starts from `OffsetOldest`, replays all historical events
2. **Normal Operation**: New events are handled as they're published
3. **After Crash**: On restart, resumes from last successfully processed offset
4. **Offset Tracking**: Offset is saved after each successful event handling

Resumable subscriptions consume `EventStore` toward a concrete, bounded tail
barrier in their live phase too, using finite requested batch targets. A store
may exceed a target only when it must return an indivisible chunk to preserve a
safe resume token. When such a chunk crosses the captured barrier without
exposing that exact token, `EventStoreOffsetComparer` lets the coordinator stop
at the chunk's actual resume token. The handler may receive that one indivisible
chunk's concurrent post-barrier events, but the drain will not chase later
chunks. The handler receives a fresh decode of the persisted JSON plus its
stored envelope—not pointer identity, unexported/`json:"-"` fields, or other
transient properties of the object passed to `Publish`. An append failure that
leaves no durable record therefore produces no resumable delivery. An `Append`
error may instead follow a successful remote commit; that record can still be
delivered now or during a later replay. Use ordinary `Subscribe` when the exact
in-memory, best-effort callback contract is required.

In default delivery mode, every persisted publish wakes active resumable
coordinators across concrete handler shards. This is necessary when a stored V1
event upcasts into a V2 subscription (and when a publish context was cancelled
after a store accepted the record): the coordinator scans the durable envelope
rather than relying on the publisher's original Go type. `WithLogDelivery`
keeps this wake behind `Follow`, preserving its log-only delivery boundary.

The coordinator will not let a newer offset pass a blocked or panicking older
event. Concurrent and same-type reentrant publishes are coalesced to avoid
deadlock, so under contention a publisher may return before this particular
resumable handler drains its event; `bus.Wait()` and `Shutdown` wait for the
scheduled drain. Setup completes its fixed initial tail synchronously. When a
local signal is already pending at handoff, the tracked background drain
performs the second catch-up pass, so continuous local publishing cannot starve
`SubscribeWithReplay` from returning; call `bus.Wait()` when that concurrent
catch-up must be complete before the next operation. A live drain failure keeps
its wake-up pending and retries with capped exponential backoff; clearing the
subscription cancels any delayed retry.

`Async()` and `Once()` are rejected for resumable subscriptions: safe durable
checkpointing requires synchronous handler completion, and one-time delivery
has no coherent restart behavior. `LoadOffset` errors abort subscription setup
instead of being mistaken for a new subscription. Handler panics are recovered,
reported through the configured panic/observability hooks, and abort replay
without advancing the failing event's offset.

Subscription IDs must be non-empty. After setup succeeds, an ID remains
reserved for the `EventBus`'s lifetime; create a new bus to restart it. Failed
setup releases the reservation so the same ID can be retried on that bus. The
same bus also rejects an overlapping durable `Follow` owner; `Follow` releases
its active reservation when the call returns. The bus cannot coordinate another
process, so use one process per ID unless the store adds an external lease.

### Multiple Subscriptions

Different subscriptions track offsets independently:

```go
// Email sender tracks its own offset
eventbus.SubscribeWithReplay(ctx, bus, "email-sender", emailHandler)

// Analytics tracker tracks separately
eventbus.SubscribeWithReplay(ctx, bus, "analytics", analyticsHandler)

// If email-sender crashes, it resumes from its last offset
// Analytics continues unaffected
```

## Custom Stores

### EventStore Interface

Implement this interface for custom storage backends. Persist the whole
envelope — `ID`, `Origin`, and `Metadata` alongside `Type`/`Data`/`Timestamp`
— and copy it back onto the `StoredEvent`s you return, or consumers lose
their deduplication and correlation keys (the bundled stores all do this;
the Postgres example below predates the envelope and stores only the core
fields):

```go
// EventStore is the core interface (2 methods)
type EventStore interface {
    // Append stores an event and returns a store-issued resume token.
    // Tokens need not be numeric or unique per event; a chunk may share one.
    // An error can be ambiguous: the remote commit may already exist.
    // Use Event.ID as the idempotency key for internal retries.
    Append(ctx context.Context, event *Event) (Offset, error)

    // Read returns events starting after the given offset
    // Use OffsetOldest to read from the beginning. OffsetNewest must
    // efficiently return no events and the current concrete tail.
    // A positive limit is a requested batch target; a store may exceed it
    // only to keep an indivisible resume-token unit intact. Zero means no limit.
    Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error)
}

// SubscriptionStore is optional, for tracking subscription offsets
type SubscriptionStore interface {
    SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error
    LoadOffset(ctx context.Context, subscriptionID string) (Offset, error)
}

// SubscriptionStoreLookup is optional but required when a durable Follow
// combines a custom checkpoint store with a first-run FollowFrom value other
// than OffsetOldest. OffsetOldest is itself a valid saved token, so LoadOffset
// alone cannot distinguish it from an absent checkpoint.
type SubscriptionStoreLookup interface {
    LookupOffset(ctx context.Context, subscriptionID string) (offset Offset, found bool, err error)
}

// EventStoreStreamer is optional, for memory-efficient streaming
type EventStoreStreamer interface {
    ReadStream(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error]
}

// EventStoreOffsetComparer is required only when one indivisible Read unit
// can cross a concrete tail without exposing that exact token in its result.
type EventStoreOffsetComparer interface {
    // Compare concrete, same-store tokens: negative, zero, or positive.
    // OffsetOldest is valid; OffsetNewest must return an error.
    CompareOffsets(left, right Offset) (int, error)
}
```

The interface uses opaque string offsets instead of integer positions, allowing
for remote storage backends with non-sequential identifiers. A custom store must
still implement the `OffsetNewest` sentinel contract above; resumable
subscriptions use that concrete tail as a bounded replay handoff. Do not compare
opaque tokens inside the bus or generic application code. When a store's
smallest resume-safe unit can cross the handoff without returning its exact
token, implement `EventStoreOffsetComparer` with that store protocol's ordering
rule. The coordinator will finish that one crossing unit and stop at its actual
resume token. Stores whose reads always expose the exact captured token do not
need the optional interface.

Every `StoredEvent.Offset` returned by `Read` or yielded by `Tail` must also be
safe to checkpoint immediately after that event. Resuming from it may redeliver
an already completed event or chunk, but must never skip a later event. If a
protocol exposes only chunk boundaries, give non-last members the read-from
(chunk-start) token and only the last member the chunk-end token. Do not attach
the chunk-end token to every member: a crash after the first handler would then
skip the unfinished suffix.

### PostgreSQL Example

```go
import (
    "context"
    "database/sql"
    "fmt"
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

func (p *PostgresStore) tailOffset(ctx context.Context) (eventbus.Offset, error) {
    var position int64
    err := p.db.QueryRowContext(ctx,
        "SELECT COALESCE(MAX(position), 0) FROM events").Scan(&position)
    if err != nil {
        return eventbus.OffsetOldest, err
    }
    if position == 0 {
        return eventbus.OffsetOldest, nil
    }
    return eventbus.Offset(strconv.FormatInt(position, 10)), nil
}

func (p *PostgresStore) Read(ctx context.Context, from eventbus.Offset, limit int) ([]*eventbus.StoredEvent, eventbus.Offset, error) {
    if from == eventbus.OffsetNewest {
        tail, err := p.tailOffset(ctx)
        return nil, tail, err
    }

    var fromPos int64
    if from != eventbus.OffsetOldest {
        parsed, err := strconv.ParseInt(string(from), 10, 64)
        if err != nil {
            return nil, from, fmt.Errorf("parse event offset %q: %w", from, err)
        }
        fromPos = parsed
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
    if offset == eventbus.OffsetNewest {
        tail, err := p.tailOffset(ctx)
        if err != nil {
            return err
        }
        offset = tail
    }

    var position int64
    if offset != eventbus.OffsetOldest {
        parsed, err := strconv.ParseInt(string(offset), 10, 64)
        if err != nil {
            return fmt.Errorf("parse subscription offset %q: %w", offset, err)
        }
        position = parsed
    }
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
eventbus.SubscribeWithReplay(ctx, bus, "critical-processor", criticalHandler)
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
    err := eventbus.SubscribeWithReplay(ctx, bus, "test",
        func(event TestEvent) {
            count++
            if count == 5 {
                panic("simulate crash")
            }
        })
    if err == nil {
        t.Fatal("expected recovered handler panic")
    }

    // Resume subscription. The event whose handler panicked was not
    // checkpointed, so it is delivered again (at-least-once).
    err = eventbus.SubscribeWithReplay(ctx, bus, "test",
        func(event TestEvent) {
            // Process the remaining history idempotently.
        })
    if err != nil {
        t.Fatal(err)
    }
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
- ✅ Reliable - Resumable subscriptions provide at-least-once delivery
- ✅ Powerful - Full replay and event sourcing support
- ✅ Production-ready - Error handling, timeouts, and monitoring

For more examples, see [EXAMPLES.md](EXAMPLES.md).
