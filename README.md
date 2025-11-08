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
- 🚀 **Zero dependencies** - Pure Go standard library
- 💾 **Event persistence** - Built-in support for event storage and replay
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
// Create persistent bus
store := eventbus.NewMemoryStore()
bus := eventbus.New(eventbus.WithStore(store))

// Events are automatically persisted
eventbus.Publish(bus, UserCreatedEvent{UserID: "123"})

// Replay events
bus.Replay(ctx, 0, func(event *eventbus.StoredEvent) error {
    // Process stored event
    return nil
})

// Subscribe with automatic position tracking
eventbus.SubscribeWithReplay(bus, "email-sender",
    func(event EmailEvent) {
        sendEmail(event)
        // Position saved automatically after success
    })
```

See [**Persistence Guide**](docs/PERSISTENCE.md) for custom stores and advanced patterns.

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

## Documentation

- 📖 [**Complete Examples**](docs/EXAMPLES.md) - Comprehensive usage examples
- 💾 [**Persistence Guide**](docs/PERSISTENCE.md) - Event storage and replay patterns
- 📚 [**API Reference**](https://godoc.org/github.com/jilio/ebu) - Complete API documentation

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
