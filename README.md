# ebu (Event BUs)

[![GoDoc](https://godoc.org/github.com/jilio/ebu?status.svg)](https://godoc.org/github.com/jilio/ebu)
[![Test and Coverage](https://github.com/jilio/ebu/actions/workflows/test.yml/badge.svg)](https://github.com/jilio/ebu/actions/workflows/test.yml)
[![Go Coverage](https://github.com/jilio/ebu/wiki/coverage.svg)](https://raw.githack.com/wiki/jilio/ebu/coverage.html)
[![Go Report Card](https://goreportcard.com/badge/github.com/jilio/ebu)](https://goreportcard.com/report/github.com/jilio/ebu)

A lightweight, type-safe event bus for Go with generics support. Build decoupled applications with compile-time type safety.

**ebu** stands for **Event BUs** - a simple, powerful event bus implementation for Go.

## Features

- 🔒 **Type-safe** - Full compile-time type safety with generics
- ⚡ **Fast** - Zero allocations in hot paths, optimized for performance
- 🔄 **Async support** - Built-in async handlers with optional sequential processing
- 🎯 **Simple API** - Clean, intuitive API with options pattern
- 🧵 **Thread-safe** - Safe for concurrent use across goroutines
- 🌐 **Context support** - First-class context support for cancellation and tracing
- 🛡️ **Panic recovery** - Handlers are isolated from each other's panics
- 🚀 **Zero dependencies** - Pure Go standard library (core package)
- 💾 **Event persistence** - Built-in support for event storage and replay
- 🌍 **Remote storage** - Native support for remote backends like [durable-streams](https://github.com/durable-streams/durable-streams)
- 🔄 **Event upcasting** - Seamless event schema migration and versioning
- ✅ **100% test coverage** - Thoroughly tested for reliability

## Installation

```bash
go get github.com/jilio/ebu
```

## Quick Start

```go
package main

import (
    "fmt"
    "time"

    eventbus "github.com/jilio/ebu"
)

// Define your event types
type UserLoginEvent struct {
    UserID    string
    Timestamp time.Time
}

type OrderCreatedEvent struct {
    OrderID string
    Amount  float64
}

func main() {
    // Create a new event bus
    bus := eventbus.New()

    // Subscribe to events with type-safe handlers
    eventbus.Subscribe(bus, func(event UserLoginEvent) {
        fmt.Printf("User %s logged in at %v\n", event.UserID, event.Timestamp)
    })

    eventbus.Subscribe(bus, func(event OrderCreatedEvent) {
        fmt.Printf("Order %s created for $%.2f\n", event.OrderID, event.Amount)
    })

    // Publish events - compile-time type safety!
    eventbus.Publish(bus, UserLoginEvent{
        UserID:    "user123",
        Timestamp: time.Now(),
    })

    eventbus.Publish(bus, OrderCreatedEvent{
        OrderID: "order456",
        Amount:  99.99,
    })
}
```

## Core API

### Subscribe and Publish

```go
// Simple subscription
eventbus.Subscribe(bus, func(event UserEvent) {
    // Handle event
})

// With options
eventbus.Subscribe(bus, func(event EmailEvent) {
    sendEmail(event)
}, eventbus.Async(), eventbus.Once())

// Publish events
eventbus.Publish(bus, UserEvent{UserID: "123"})
```

### Async Processing

```go
// Parallel async processing (default)
eventbus.Subscribe(bus, func(event EmailEvent) {
    sendEmail(event) // Each email sent in parallel
}, eventbus.Async())

// Sequential async processing (preserves order)
eventbus.Subscribe(bus, func(event PaymentEvent) {
    processPayment(event) // Processed one at a time
}, eventbus.Async(), eventbus.Sequential())

// Wait for all async handlers
bus.Wait()
```


### Graceful Shutdown

Shutdown the event bus gracefully, waiting for async handlers to complete with timeout support:

```go
// Shutdown with timeout
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := bus.Shutdown(ctx); err != nil {
    log.Printf("Shutdown timed out: %v", err)
}
```

The `Shutdown` method:
- Waits for all async handlers to complete
- Respects context timeout and cancellation
- Returns `context.DeadlineExceeded` if handlers don't finish in time
- Returns `nil` on successful graceful shutdown

### Context Support

```go
// Context-aware handlers
eventbus.SubscribeContext(bus, func(ctx context.Context, event RequestEvent) {
    traceID := ctx.Value("traceID")
    // Handle with context
})

// Publish with context
ctx := context.WithTimeout(context.Background(), 5*time.Second)
eventbus.PublishContext(bus, ctx, RequestEvent{Path: "/api/users"})
```

### Event Filtering

```go
eventbus.Subscribe(bus, func(event PriceEvent) {
    fmt.Printf("Alert: Price changed %.2f%%\n", event.Change)
}, eventbus.WithFilter(func(event PriceEvent) bool {
    return math.Abs(event.Change) > 5.0 // Only large changes
}))
```

### Handler Management

```go
// Check for handlers
if eventbus.HasHandlers[UserEvent](bus) {
    eventbus.Publish(bus, UserEvent{})
}

// Unsubscribe a handler
handler := func(event UserEvent) { /* ... */ }
eventbus.Subscribe(bus, handler)
eventbus.Unsubscribe(bus, handler)

// Clear all handlers for a type
eventbus.Clear[UserEvent](bus)

// Clear all handlers
bus.ClearAll()
```

## Advanced Features

### Event Persistence

Store and replay events for event sourcing, audit logs, and resumable subscriptions:

```go
// Create persistent bus with in-memory store
store := eventbus.NewMemoryStore()
bus := eventbus.New(eventbus.WithStore(store))

// Events are automatically persisted
eventbus.Publish(bus, UserCreatedEvent{UserID: "123"})

// Replay events from the beginning
bus.Replay(ctx, eventbus.OffsetOldest, func(event *eventbus.StoredEvent) error {
    // Process stored event
    return nil
})

// Subscribe with automatic offset tracking
eventbus.SubscribeWithReplay(bus, "email-sender",
    func(event EmailEvent) {
        sendEmail(event)
        // Offset saved automatically after success
    })
```

See [**Persistence Guide**](docs/PERSISTENCE.md) for custom stores and advanced patterns.

### Remote Storage

Use remote storage backends for distributed event persistence. ebu supports [Durable Streams](https://electric-sql.com/blog/2025/12/09/announcing-durable-streams) - an HTTP protocol for reliable, resumable, real-time data streaming developed by [Electric](https://electric-sql.com):

```go
import (
    eventbus "github.com/jilio/ebu"
    "github.com/jilio/ebu/stores/durablestream"
)

// Connect to a durable-streams server
store, err := durablestream.New(
    "http://localhost:4437/v1/stream",  // server base URL
    "mystream",                          // stream name
    durablestream.WithTimeout(30*time.Second),
)
if err != nil {
    log.Fatal(err)
}

// Use with event bus - same API as local storage
bus := eventbus.New(eventbus.WithStore(store))

// Events are now persisted to the remote durable-streams server
eventbus.Publish(bus, OrderCreatedEvent{OrderID: "123", Amount: 99.99})
```

Available storage backends:
- **MemoryStore** - Built-in in-memory store for development
- **SQLite** - `stores/sqlite` - Persistent local storage
- **Durable-Streams** - `stores/durablestream` - Remote HTTP-based storage ([protocol spec](https://electric-sql.com/blog/2025/12/09/announcing-durable-streams))

See [**Persistence Guide**](docs/PERSISTENCE.md) for all storage options.

### Observability

Add metrics and distributed tracing with OpenTelemetry:

```go
import (
    eventbus "github.com/jilio/ebu"
    "github.com/jilio/ebu/otel"
)

// Create observability implementation
obs, err := otel.New(
    otel.WithTracerProvider(tracerProvider),
    otel.WithMeterProvider(meterProvider),
)

// Create bus with observability
bus := eventbus.New(eventbus.WithObservability(obs))

// Events, handlers, and persistence are automatically tracked
eventbus.Publish(bus, UserCreatedEvent{UserID: "123"})
```

The `otel` package provides:
- **Metrics**: Event counts, handler duration, error rates, persistence metrics
- **Tracing**: Distributed tracing with spans for publish, handlers, and persistence
- **Zero overhead**: Optional - no performance impact if not used
- **Vendor-neutral**: Built on OpenTelemetry standards

See [**examples/observability**](examples/observability) for a complete example.

### Event Upcasting

Migrate event schemas seamlessly without breaking existing handlers:

```go
// V1 event
type UserCreatedV1 struct {
    UserID string
    Name   string
}

// V2 event with split name
type UserCreatedV2 struct {
    UserID    string
    FirstName string
    LastName  string
}

// Register upcast transformation
eventbus.RegisterUpcast(bus, func(v1 UserCreatedV1) UserCreatedV2 {
    parts := strings.Split(v1.Name, " ")
    return UserCreatedV2{
        UserID:    v1.UserID,
        FirstName: parts[0],
        LastName:  strings.Join(parts[1:], " "),
    }
})

// Old events automatically transformed when replayed
eventbus.SubscribeWithReplay(bus, "processor", func(event UserCreatedV2) {
    // Receives V2 format even for old V1 events
})
```

Upcasting supports:

- Automatic chain resolution (V1→V2→V3)
- Circular dependency detection
- Type-safe transformations
- Error handling hooks

### Panic Recovery

Handlers are isolated - one panic won't affect others:

```go
bus.SetPanicHandler(func(event any, handlerType reflect.Type, panicValue any) {
    log.Printf("Handler panic: %v", panicValue)
})

eventbus.Subscribe(bus, func(e Event) { panic("error") })
eventbus.Subscribe(bus, func(e Event) { /* Still runs! */ })
```

### Global Hooks

Intercept all events for logging, metrics, or tracing:

```go
bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
    log.Printf("Publishing %s", eventType.Name())
})

bus.SetAfterPublishHook(func(eventType reflect.Type, event any) {
    metrics.Increment("events." + eventType.Name())
})
```

### Custom Event Type Names

Control event type naming explicitly with the `TypeNamer` interface for stable names across refactoring:

```go
type UserCreatedEvent struct {
    UserID string
}

// Implement TypeNamer for explicit type control
func (e UserCreatedEvent) EventTypeName() string {
    return "user.created.v1"
}

// Now EventType() returns "user.created.v1" instead of package-qualified name
eventbus.Publish(bus, UserCreatedEvent{UserID: "123"})
```

Benefits:
- **Stable event names** across package reorganization
- **Version control** for event schema evolution
- **External compatibility** with other event systems

See [TypeNamer examples](docs/EXAMPLES.md#custom-event-type-names-typenamer) for versioning and migration patterns.

## Documentation

- 📖 [**Complete Examples**](docs/EXAMPLES.md) - Comprehensive usage examples
- 💾 [**Persistence Guide**](docs/PERSISTENCE.md) - Event storage and replay patterns
- 📚 [**API Reference**](https://godoc.org/github.com/jilio/ebu) - Complete API documentation

## Storage Backends

| Backend | Package | Description |
|---------|---------|-------------|
| MemoryStore | `github.com/jilio/ebu` | In-memory store for development/testing |
| SQLite | `github.com/jilio/ebu/stores/sqlite` | Local persistent storage with WAL mode |
| Durable-Streams | `github.com/jilio/ebu/stores/durablestream` | Remote HTTP-based storage |

See [**Persistence Guide**](docs/PERSISTENCE.md) for detailed usage.

## State Protocol

The optional `state` package implements the [Durable Streams State Protocol](https://github.com/durable-streams/durable-streams) for database-style state synchronization:

```go
import (
    eventbus "github.com/jilio/ebu"
    "github.com/jilio/ebu/state"
)

// Define entity type
type User struct {
    Name  string `json:"name"`
    Email string `json:"email"`
}

// Create and publish state changes
bus := eventbus.New(eventbus.WithStore(eventbus.NewMemoryStore()))

insertMsg, _ := state.Insert("user:1", User{Name: "Alice", Email: "alice@example.com"})
eventbus.Publish(bus, insertMsg)

updateMsg, _ := state.Update("user:1", User{Name: "Alice Smith"}, state.WithTxID("tx-123"))
eventbus.Publish(bus, updateMsg)

// Materialize state from events
mat := state.NewMaterializer()
users := state.NewTypedCollection[User](state.NewMemoryStore[User]())
state.RegisterCollection(mat, users)

mat.Replay(ctx, bus, eventbus.OffsetOldest)

// Access materialized state
user, ok := users.Get("user:1")  // User{Name: "Alice Smith", ...}
```

Features:
- **Type-safe helpers**: `Insert`, `Update`, `Delete` with Go generics
- **Options pattern**: `WithTxID`, `WithTimestamp`, `WithEntityType`
- **Materializer**: Build typed state from event streams
- **Control messages**: `SnapshotStart`, `SnapshotEnd`, `Reset`
- **JSON interoperability**: Compatible with durable-streams ecosystem

## Best Practices

1. **Define clear event types** - Use descriptive structs with meaningful fields
2. **Keep events immutable** - Don't modify events after publishing
3. **Handle errors gracefully** - Prefer returning errors over panicking
4. **Use async for I/O** - Keep synchronous handlers fast
5. **Leverage context** - Use `PublishContext` for cancellable operations
6. **Set panic handlers** - Monitor and log handler failures in production
7. **Test concurrency** - The bus is thread-safe, but test your handlers

## Performance

- Type-based routing with zero reflection for direct handlers
- Zero allocations in hot paths
- Efficient sharding reduces lock contention
- Async handlers run in separate goroutines

## Contributing

Contributions are welcome! Submit a Pull Request or open an Issue for bugs, features, or improvements.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support

- 📖 [Documentation](https://godoc.org/github.com/jilio/ebu)
- 🐛 [Issues](https://github.com/jilio/ebu/issues)
- 💬 [Discussions](https://github.com/jilio/ebu/discussions)
