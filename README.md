# ebu (Event BUs)

[![GoDoc](https://godoc.org/github.com/jilio/ebu?status.svg)](https://godoc.org/github.com/jilio/ebu)
[![Test and Coverage](https://github.com/jilio/ebu/actions/workflows/test.yml/badge.svg)](https://github.com/jilio/ebu/actions/workflows/test.yml)
[![Go Coverage](https://github.com/jilio/ebu/wiki/coverage.svg)](https://raw.githack.com/wiki/jilio/ebu/coverage.html)
[![Go Report Card](https://goreportcard.com/badge/github.com/jilio/ebu)](https://goreportcard.com/report/github.com/jilio/ebu)

A lightweight, type-safe event bus for Go with generics support. Build decoupled applications with compile-time type safety and zero runtime reflection for handlers.

**ebu** stands for **Event BUs** - a simple, powerful event bus implementation for Go.

## Features

- 🔒 **Type-safe** - Full compile-time type safety with generics
- ⚡ **Fast** - Zero allocations in hot paths, minimal reflection
- 🔄 **Async support** - Built-in async handlers with optional sequential processing
- 🎯 **Simple API** - Just 2 main functions with clean options pattern
- 🧵 **Thread-safe** - Safe for concurrent use across goroutines
- 🌐 **Context support** - First-class context support for cancellation and request tracing
- 🛡️ **Panic recovery** - Handlers are isolated from each other's panics
- ⏹️ **Context cancellation** - Stop processing handlers when context is cancelled
- 🏃 **Race-safe** - Guaranteed once-only execution for once handlers
- 💾 **Event persistence** - Built-in support for event storage and replay
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
    err := eventbus.Subscribe(bus, func(event UserLoginEvent) {
        fmt.Printf("User %s logged in at %v\n", event.UserID, event.Timestamp)
    })
    if err != nil {
        panic(err)
    }
    
    err = eventbus.Subscribe(bus, func(event OrderCreatedEvent) {
        fmt.Printf("Order %s created for $%.2f\n", event.OrderID, event.Amount)
    })
    if err != nil {
        panic(err)
    }
    
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

## Advanced Examples

### Async Event Processing

```go
type EmailNotification struct {
    To      string
    Subject string
    Body    string
}

func main() {
    bus := eventbus.New()
    
    // Async handler for sending emails
    err := eventbus.Subscribe(bus, func(event EmailNotification) {
        // Simulate email sending
        time.Sleep(100 * time.Millisecond)
        fmt.Printf("Email sent to %s: %s\n", event.To, event.Subject)
    }, eventbus.Async(false)) // false = parallel processing
    if err != nil {
        panic(err)
    }
    
    // Publish multiple notifications
    for i := 0; i < 5; i++ {
        eventbus.Publish(bus, EmailNotification{
            To:      fmt.Sprintf("user%d@example.com", i),
            Subject: "Welcome!",
            Body:    "Thanks for signing up!",
        })
    }
    
    // Wait for all async handlers to complete
    bus.WaitAsync()
}
```

### One-time Event Handlers

```go
type AppStartedEvent struct{}

func main() {
    bus := eventbus.New()
    
    // This handler will only be called once
    err := eventbus.Subscribe(bus, func(event AppStartedEvent) {
        fmt.Println("App initialization completed!")
    }, eventbus.Once())
    if err != nil {
        panic(err)
    }
    
    // First publish - handler will be called
    eventbus.Publish(bus, AppStartedEvent{})
    
    // Second publish - handler won't be called
    eventbus.Publish(bus, AppStartedEvent{})
}
```

### Sequential Async Processing

```go
type PaymentEvent struct {
    ID     string
    Amount float64
}

func main() {
    bus := eventbus.New()
    
    // Sequential async handler - events processed one at a time
    err := eventbus.Subscribe(bus, func(event PaymentEvent) {
        fmt.Printf("Processing payment %s...\n", event.ID)
        time.Sleep(200 * time.Millisecond) // Simulate processing
        fmt.Printf("Payment %s completed\n", event.ID)
    }, eventbus.Async(true)) // true = sequential processing
    if err != nil {
        panic(err)
    }
    
    // Publish multiple payments
    eventbus.Publish(bus, PaymentEvent{ID: "PAY-001", Amount: 100.00})
    eventbus.Publish(bus, PaymentEvent{ID: "PAY-002", Amount: 200.00})
    eventbus.Publish(bus, PaymentEvent{ID: "PAY-003", Amount: 300.00})
    
    bus.WaitAsync()
}
```

### Dynamic Handler Management

```go
type MetricEvent struct {
    Name  string
    Value float64
}

func main() {
    bus := eventbus.New()
    
    // Define a handler we can reference later
    metricHandler := func(event MetricEvent) {
        fmt.Printf("Metric: %s = %.2f\n", event.Name, event.Value)
    }
    
    // Subscribe the handler
    err := eventbus.Subscribe(bus, metricHandler)
    if err != nil {
        panic(err)
    }
    
    // Check if there are subscribers
    if eventbus.HasSubscribers[MetricEvent](bus) {
        eventbus.Publish(bus, MetricEvent{Name: "cpu_usage", Value: 45.5})
    }
    
    // Unsubscribe the handler
    err = eventbus.Unsubscribe(bus, metricHandler)
    if err != nil {
        panic(err)
    }
    
    // This won't be handled
    eventbus.Publish(bus, MetricEvent{Name: "memory_usage", Value: 62.3})
}
```

### Context Support

ebu provides context-aware variants of all subscription functions, allowing you to pass context for cancellation, deadlines, and request-scoped values.

```go
import (
    "context"
    "fmt"
    
    eventbus "github.com/jilio/ebu"
)

type RequestEvent struct {
    Path   string
    Method string
}

func main() {
    bus := eventbus.New()
    
    // Context-aware handler for distributed tracing
    err := eventbus.SubscribeContext(bus, func(ctx context.Context, event RequestEvent) {
        // Extract trace ID from context
        traceID := ctx.Value("traceID")
        fmt.Printf("[%s] Processing %s %s\n", traceID, event.Method, event.Path)
    })
    if err != nil {
        panic(err)
    }
    
    // Publish with context
    ctx := context.WithValue(context.Background(), "traceID", "abc-123")
    eventbus.PublishContext(bus, ctx, RequestEvent{
        Path:   "/api/users",
        Method: "GET",
    })
}
```

### Context with Cancellation

Context cancellation works at multiple levels in ebu:

1. **Early termination**: When context is cancelled, remaining handlers are skipped
2. **Async safety**: Async handlers check context before execution
3. **Handler awareness**: Context-aware handlers can respond to cancellation

```go
import (
    "context"
    "fmt"
    "time"
    
    eventbus "github.com/jilio/ebu"
)

type LongRunningTask struct {
    ID   string
    Data []byte
}

func main() {
    bus := eventbus.New()
    
    // Handler 1: Will be skipped if context is already cancelled
    err := eventbus.Subscribe(bus, func(event LongRunningTask) {
        fmt.Printf("Processing task %s\n", event.ID)
    })
    if err != nil {
        panic(err)
    }
    
    // Handler 2: Context-aware handler that respects cancellation
    err = eventbus.SubscribeContext(bus, func(ctx context.Context, event LongRunningTask) {
        for i := 0; i < 10; i++ {
            select {
            case <-ctx.Done():
                fmt.Printf("Task %s cancelled at step %d\n", event.ID, i)
                return
            case <-time.After(100 * time.Millisecond):
                fmt.Printf("Processing step %d for task %s\n", i+1, event.ID)
            }
        }
        fmt.Printf("Task %s completed\n", event.ID)
    }, eventbus.Async(false))
    if err != nil {
        panic(err)
    }
    
    // Handler 3: Will not execute if context was cancelled before reaching it
    err = eventbus.Subscribe(bus, func(event LongRunningTask) {
        fmt.Printf("Post-processing task %s\n", event.ID)
    })
    if err != nil {
        panic(err)
    }
    
    // Example 1: Cancel during execution
    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        time.Sleep(250 * time.Millisecond)
        cancel() // This will stop handler 2 and prevent handler 3 from running
    }()
    
    eventbus.PublishContext(bus, ctx, LongRunningTask{ID: "task-1", Data: []byte("data")})
    bus.WaitAsync()
    
    // Example 2: Pre-cancelled context (no handlers will execute)
    ctx2, cancel2 := context.WithCancel(context.Background())
    cancel2() // Cancel before publishing
    
    eventbus.PublishContext(bus, ctx2, LongRunningTask{ID: "task-2", Data: []byte("data")})
    // No output - all handlers skipped
}
```

### Panic Recovery

ebu isolates handlers from each other - if one handler panics, other handlers continue to execute normally:

```go
import (
    "fmt"
    "reflect"
    
    eventbus "github.com/jilio/ebu"
)

type UserEvent struct {
    UserID string
    Action string
}

func main() {
    bus := eventbus.New()
    
    // Optional: Set a custom panic handler to log or monitor panics
    bus.SetPanicHandler(func(event any, handlerType reflect.Type, panicValue any) {
        fmt.Printf("Handler %v panicked with: %v (event: %+v)\n", 
            handlerType, panicValue, event)
    })
    
    // Handler 1: Normal handler
    err := eventbus.Subscribe(bus, func(event UserEvent) {
        fmt.Printf("Handler 1 processing user %s\n", event.UserID)
    })
    if err != nil {
        panic(err)
    }
    
    // Handler 2: This handler will panic
    err = eventbus.Subscribe(bus, func(event UserEvent) {
        panic("something went wrong!")
    })
    if err != nil {
        panic(err)
    }
    
    // Handler 3: Will still execute despite handler 2's panic
    err = eventbus.Subscribe(bus, func(event UserEvent) {
        fmt.Printf("Handler 3 processing user %s\n", event.UserID)
    })
    if err != nil {
        panic(err)
    }
    
    // Publish event
    eventbus.Publish(bus, UserEvent{UserID: "123", Action: "test"})
    
    // Output:
    // Handler 1 processing user 123
    // Handler panic detected with: something went wrong! (event: {UserID:123 Action:test})
    // Handler 3 processing user 123
}
```

Panic recovery works for both synchronous and asynchronous handlers, ensuring your application remains stable even when individual handlers fail.

### Global Event Hooks

Hooks allow you to intercept ALL events without subscribing to each type individually. This is perfect for cross-cutting concerns like logging, metrics, and tracing.

```go
import (
    "log"
    "reflect"
    
    eventbus "github.com/jilio/ebu"
)

func setupGlobalLogging(bus *eventbus.EventBus) {
    // Called before any handlers execute
    bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
        log.Printf("[EVENT] Publishing %s: %+v", eventType.Name(), event)
    })
    
    // Called after all handlers complete
    bus.SetAfterPublishHook(func(eventType reflect.Type, event any) {
        log.Printf("[EVENT] Completed %s", eventType.Name())
    })
}

func main() {
    bus := eventbus.New()
    setupGlobalLogging(bus)
    
    // All events are now automatically logged
    eventbus.Publish(bus, UserCreatedEvent{UserID: "123"})
    eventbus.Publish(bus, OrderPlacedEvent{OrderID: "456"})
    // No need to subscribe to each event type for logging!
}
```

#### Use Cases for Hooks

1. **Global Logging**: Log all events across your application
2. **Metrics Collection**: Track event counts, latency, and patterns
3. **Distributed Tracing**: Add trace IDs to all events
4. **Event Store**: Persist all events for audit or event sourcing
5. **Service Bridge**: Forward events between microservices
6. **Rate Limiting**: Implement global rate limits
7. **Circuit Breaking**: Detect and handle failure patterns

#### Hook Behavior

- Hooks are called synchronously before/after handler execution
- `BeforePublishHook` is called before any handlers execute
- `AfterPublishHook` is called after all sync handlers complete (doesn't wait for async)
- Hooks are called even if there are no handlers for an event
- Only one hook of each type can be set (setting a new one replaces the old)
- Hooks receive the event type and event value, allowing for type-based routing

### Event Persistence

ebu includes built-in support for event persistence, enabling event sourcing patterns, audit logs, and resumable subscriptions. The persistence layer is designed to be simple and pluggable.

```go
import (
    "context"
    
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
    bus := eventbus.NewPersistent(store)
    
    // Events are automatically persisted when published
    eventbus.Publish(bus.EventBus, UserCreatedEvent{
        UserID:   "123",
        Username: "john_doe",
        Email:    "john@example.com",
    })
    
    // Replay events from a specific position
    ctx := context.Background()
    err := bus.Replay(ctx, 0, func(event *eventbus.StoredEvent) error {
        fmt.Printf("Replaying event at position %d: %s\n", 
            event.Position, event.Type)
        return nil
    })
    if err != nil {
        panic(err)
    }
}
```

#### Resumable Subscriptions

Subscriptions can automatically resume from where they left off after a restart:

```go
func main() {
    store := eventbus.NewMemoryStore()
    bus := eventbus.NewPersistent(store)
    
    // Subscribe with automatic position tracking
    err := eventbus.SubscribeWithReplay(bus, "email-sender", 
        func(event EmailNotification) {
            sendEmail(event)
            // Position is automatically saved after successful handling
        })
    if err != nil {
        panic(err)
    }
    
    // After restart, the subscription resumes from last position
    // No events are missed or duplicated!
}
```

#### Custom Event Stores

Implement the `EventStore` interface to use your own storage backend:

```go
type EventStore interface {
    Save(ctx context.Context, event *StoredEvent) error
    Load(ctx context.Context, from, to int64) ([]*StoredEvent, error)
    GetPosition(ctx context.Context) (int64, error)
    SaveSubscriptionPosition(ctx context.Context, subscriptionID string, position int64) error
    LoadSubscriptionPosition(ctx context.Context, subscriptionID string) (int64, error)
}

// Example: PostgreSQL implementation
type PostgresStore struct {
    db *sql.DB
}

func (p *PostgresStore) Save(ctx context.Context, event *StoredEvent) error {
    _, err := p.db.ExecContext(ctx, `
        INSERT INTO events (position, type, data, timestamp)
        VALUES ($1, $2, $3, $4)
    `, event.Position, event.Type, event.Data, event.Timestamp)
    return err
}

// ... implement other methods

func main() {
    db, _ := sql.Open("postgres", connectionString)
    store := &PostgresStore{db: db}
    bus := eventbus.NewPersistent(store)
    // Use the bus normally - events are now persisted to PostgreSQL
}
```

#### Use Cases for Persistence

1. **Event Sourcing**: Store all state changes as events
2. **Audit Logging**: Keep an immutable log of all actions
3. **Message Recovery**: Resume processing after crashes
4. **Event Replay**: Rebuild state or reprocess events
5. **Debugging**: Analyze event flow in production
6. **CQRS**: Separate write and read models with events
7. **Integration**: Share events between services via storage

#### The 100% Coverage Story

The persistence module was initially 99.1% covered. That missing 0.9% was a JSON unmarshal error path that seemed impossible to trigger. By pushing for 100% coverage, we discovered that the test wasn't properly setting the bus position, which meant the error path was unreachable in production too! This could have led to silent failures where corrupted events would be skipped without any error reporting. **The lesson: that last 1% of coverage often reveals the most interesting bugs.**

## API Reference

### Core Functions

#### `New() *EventBus`

Creates a new EventBus instance.

```go
bus := eventbus.New()
```

#### `Subscribe[T any](bus *EventBus, handler Handler[T], opts ...SubscribeOption) error`

Subscribes a handler to events of type T with optional configuration.

```go
// Simple subscription
err := eventbus.Subscribe(bus, func(event UserLoginEvent) {
    // Handle user login
})
if err != nil {
    // Handle error
}

// With options
err = eventbus.Subscribe(bus, func(event EmailEvent) {
    // Process email
}, eventbus.Once())
if err != nil {
    // Handle error
}

err = eventbus.Subscribe(bus, func(event OrderEvent) {
    // Process order asynchronously
}, eventbus.Async(false))
if err != nil {
    // Handle error
}

// Combine options
err = eventbus.Subscribe(bus, func(event InitEvent) {
    // One-time async initialization
}, eventbus.Async(true), eventbus.Once())
if err != nil {
    // Handle error
}
```

#### `SubscribeContext[T any](bus *EventBus, handler ContextHandler[T], opts ...SubscribeOption) error`

Subscribes a context-aware handler to events of type T.

```go
// Simple context subscription
err := eventbus.SubscribeContext(bus, func(ctx context.Context, event PaymentEvent) {
    // Handle payment with context
})
if err != nil {
    // Handle error
}

// With options
err = eventbus.SubscribeContext(bus, func(ctx context.Context, event JobEvent) {
    // Process job asynchronously with context
}, eventbus.Async(true))
if err != nil {
    // Handle error
}
```

#### `Publish[T any](bus *EventBus, event T)`

Publishes an event to all registered handlers.

```go
eventbus.Publish(bus, UserLoginEvent{
    UserID:    "user123",
    Timestamp: time.Now(),
})
```

#### `Unsubscribe[T any](bus *EventBus, handler Handler[T]) error`

Removes a specific handler.

```go
handler := func(event UserEvent) { ... }
err := eventbus.Subscribe(bus, handler)
if err != nil {
    // Handle error
}
// Later...
err = eventbus.Unsubscribe(bus, handler)
if err != nil {
    // Handle error
}
```

#### `HasSubscribers[T any](bus *EventBus) bool`

Returns true if there are any handlers for event type T.

```go
if eventbus.HasSubscribers[UserLoginEvent](bus) {
    // There are handlers for UserLoginEvent
}
```

#### `WaitAsync()`

Waits for all async handlers to complete.

```go
bus.WaitAsync()
```

#### `Clear[T any](bus *EventBus)`

Removes all handlers for event type T.

```go
eventbus.Clear[UserLoginEvent](bus)
```

#### `ClearAll()`

Removes all handlers from the event bus.

```go
bus.ClearAll()
```

#### `SetPanicHandler(handler PanicHandler)`

Sets a custom function to be called when a handler panics. This is useful for logging, monitoring, or error reporting.

```go
bus.SetPanicHandler(func(event any, handlerType reflect.Type, panicValue any) {
    log.Printf("Handler panic: type=%v, panic=%v, event=%+v", 
        handlerType, panicValue, event)
})
```

#### `SetBeforePublishHook(hook PublishHook)`

Sets a hook to be called before any handlers are executed for an event. Useful for logging, metrics, or pre-processing.

```go
bus.SetBeforePublishHook(func(eventType reflect.Type, event any) {
    log.Printf("Publishing event: %s", eventType.Name())
})
```

#### `SetAfterPublishHook(hook PublishHook)`

Sets a hook to be called after all synchronous handlers have completed. The hook is called immediately after sync handlers, not waiting for async handlers.

```go
bus.SetAfterPublishHook(func(eventType reflect.Type, event any) {
    metrics.Increment("events." + eventType.Name())
})
```

### Subscription Options

#### `Once() SubscribeOption`

Configures the handler to be called only once.

```go
err := eventbus.Subscribe(bus, func(event WelcomeEvent) {
    fmt.Println("Welcome! This message appears only once.")
}, eventbus.Once())
if err != nil {
    // Handle error
}
```

#### `Async(sequential bool) SubscribeOption`

Configures the handler to run asynchronously. If sequential is true, multiple events are processed one at a time.

```go
// Parallel processing (good for I/O-bound tasks)
err := eventbus.Subscribe(bus, func(event EmailEvent) {
    sendEmail(event) // Each email sent in parallel
}, eventbus.Async(false))
if err != nil {
    // Handle error
}

// Sequential processing (preserves order, prevents concurrency issues)
err = eventbus.Subscribe(bus, func(event DatabaseEvent) {
    updateDatabase(event) // Updates happen one at a time
}, eventbus.Async(true))
if err != nil {
    // Handle error
}
```

### Publishing Events

#### `PublishContext[T any](bus *EventBus, ctx context.Context, event T)`

Publishes an event with context to all registered handlers.

```go
ctx := context.WithTimeout(context.Background(), 5*time.Second)
eventbus.PublishContext(bus, ctx, ProcessingEvent{ID: "123"})
```

## Best Practices

1. **Define clear event types**: Use structs with descriptive names and fields
2. **Keep events immutable**: Don't modify events after publishing
3. **Handle errors gracefully**: While panic recovery protects the bus, prefer returning errors over panicking
4. **Use async handlers for I/O**: Keep synchronous handlers fast
5. **Clean up handlers**: Use `Unsubscribe` or `Clear` when handlers are no longer needed
6. **Leverage context cancellation**: Use `PublishContext` for operations that might need cancellation
7. **Set panic handlers in production**: Use `SetPanicHandler` to monitor and log handler failures
8. **Test concurrent scenarios**: The bus is thread-safe, but test your handlers for race conditions

## Performance Tips

- The event bus uses type information for routing, making it very efficient
- Async handlers run in separate goroutines, so be mindful of goroutine creation
- Use sequential async handlers when event processing order matters
- The `WaitAsync()` method is useful for graceful shutdowns

## Contributing

Contributions are welcome! Feel free to submit a Pull Request or open an Issue for bugs, feature requests, or improvements.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- 📖 Documentation: [godoc.org](https://godoc.org/github.com/jilio/ebu)
- 🐛 Issues: [GitHub Issues](https://github.com/jilio/ebu/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/jilio/ebu/discussions)
