# ebu Examples

Comprehensive examples demonstrating all features of the ebu event bus library.

## Table of Contents

- [Basic Usage](#basic-usage)
- [Async Processing](#async-processing)
- [Event Filtering](#event-filtering)
- [Context Support](#context-support)
- [Panic Recovery](#panic-recovery)
- [Global Hooks](#global-hooks)
- [Dynamic Handler Management](#dynamic-handler-management)

## Basic Usage

### Simple Pub/Sub

```go
package main

import (
    "fmt"
    "time"

    eventbus "github.com/jilio/ebu"
)

type UserLoginEvent struct {
    UserID    string
    Timestamp time.Time
}

func main() {
    bus := eventbus.New()

    // Subscribe
    eventbus.Subscribe(bus, func(event UserLoginEvent) {
        fmt.Printf("User %s logged in at %v\n", event.UserID, event.Timestamp)
    })

    // Publish
    eventbus.Publish(bus, UserLoginEvent{
        UserID:    "user123",
        Timestamp: time.Now(),
    })
}
```

### One-time Event Handlers

```go
type AppStartedEvent struct{}

func main() {
    bus := eventbus.New()

    // This handler will only be called once
    eventbus.Subscribe(bus, func(event AppStartedEvent) {
        fmt.Println("App initialization completed!")
    }, eventbus.Once())

    // First publish - handler will be called
    eventbus.Publish(bus, AppStartedEvent{})

    // Second publish - handler won't be called
    eventbus.Publish(bus, AppStartedEvent{})
}
```

## Async Processing

### Parallel Async Handlers

```go
type EmailNotification struct {
    To      string
    Subject string
    Body    string
}

func main() {
    bus := eventbus.New()

    // Async handler for sending emails (parallel processing)
    eventbus.Subscribe(bus, func(event EmailNotification) {
        time.Sleep(100 * time.Millisecond) // Simulate email sending
        fmt.Printf("Email sent to %s: %s\n", event.To, event.Subject)
    }, eventbus.Async())

    // Publish multiple notifications
    for i := 0; i < 5; i++ {
        eventbus.Publish(bus, EmailNotification{
            To:      fmt.Sprintf("user%d@example.com", i),
            Subject: "Welcome!",
            Body:    "Thanks for signing up!",
        })
    }

    // Wait for all async handlers to complete
    bus.Wait()
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
    eventbus.Subscribe(bus, func(event PaymentEvent) {
        fmt.Printf("Processing payment %s...\n", event.ID)
        time.Sleep(200 * time.Millisecond) // Simulate processing
        fmt.Printf("Payment %s completed\n", event.ID)
    }, eventbus.Async(), eventbus.Sequential())

    // Publish multiple payments
    eventbus.Publish(bus, PaymentEvent{ID: "PAY-001", Amount: 100.00})
    eventbus.Publish(bus, PaymentEvent{ID: "PAY-002", Amount: 200.00})
    eventbus.Publish(bus, PaymentEvent{ID: "PAY-003", Amount: 300.00})

    bus.Wait()
}
```

## Event Filtering

### Filter by Event Fields

```go
import (
    "fmt"
    "math"

    eventbus "github.com/jilio/ebu"
)

type PriceUpdateEvent struct {
    Symbol string
    Price  float64
    Change float64
}

func main() {
    bus := eventbus.New()

    // Only handle price updates with significant changes
    eventbus.Subscribe(bus, func(event PriceUpdateEvent) {
        fmt.Printf("ALERT: %s moved %.2f%% to $%.2f\n",
            event.Symbol, event.Change, event.Price)
    }, eventbus.WithFilter(func(event PriceUpdateEvent) bool {
        // Only process events where price changed more than 5%
        return math.Abs(event.Change) > 5.0
    }))

    // Only handle specific symbols
    eventbus.Subscribe(bus, func(event PriceUpdateEvent) {
        fmt.Printf("Tech stock update: %s at $%.2f\n",
            event.Symbol, event.Price)
    }, eventbus.WithFilter(func(event PriceUpdateEvent) bool {
        techStocks := []string{"AAPL", "GOOGL", "MSFT", "AMZN"}
        for _, stock := range techStocks {
            if event.Symbol == stock {
                return true
            }
        }
        return false
    }))

    // Publish various price updates
    eventbus.Publish(bus, PriceUpdateEvent{Symbol: "AAPL", Price: 150.00, Change: 0.5})
    eventbus.Publish(bus, PriceUpdateEvent{Symbol: "GOOGL", Price: 2800.00, Change: 10.2})
    eventbus.Publish(bus, PriceUpdateEvent{Symbol: "TSLA", Price: 850.00, Change: -7.5})
}
```

### Combining Filters with Other Options

```go
type UserEvent struct {
    UserID    string
    IsNewUser bool
}

// Filter + Async + Once
eventbus.Subscribe(bus, func(event UserEvent) {
    sendWelcomeEmail(event.UserID)
},
    eventbus.WithFilter(func(event UserEvent) bool {
        return event.IsNewUser
    }),
    eventbus.Async(),
    eventbus.Once(), // Only send welcome email once per app lifetime
)
```

## Context Support

### Basic Context Usage

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
    eventbus.SubscribeContext(bus, func(ctx context.Context, event RequestEvent) {
        traceID := ctx.Value("traceID")
        fmt.Printf("[%s] Processing %s %s\n", traceID, event.Method, event.Path)
    })

    // Publish with context
    ctx := context.WithValue(context.Background(), "traceID", "abc-123")
    eventbus.PublishContext(bus, ctx, RequestEvent{
        Path:   "/api/users",
        Method: "GET",
    })
}
```

### Context with Cancellation

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
    eventbus.Subscribe(bus, func(event LongRunningTask) {
        fmt.Printf("Processing task %s\n", event.ID)
    })

    // Handler 2: Context-aware handler that respects cancellation
    eventbus.SubscribeContext(bus, func(ctx context.Context, event LongRunningTask) {
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
    }, eventbus.Async())

    // Handler 3: Will not execute if context was cancelled before reaching it
    eventbus.Subscribe(bus, func(event LongRunningTask) {
        fmt.Printf("Post-processing task %s\n", event.ID)
    })

    // Cancel during execution
    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        time.Sleep(250 * time.Millisecond)
        cancel() // Stops handler 2 and prevents handler 3
    }()

    eventbus.PublishContext(bus, ctx, LongRunningTask{ID: "task-1", Data: []byte("data")})
    bus.Wait()
}
```

## Panic Recovery

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
    eventbus.Subscribe(bus, func(event UserEvent) {
        fmt.Printf("Handler 1 processing user %s\n", event.UserID)
    })

    // Handler 2: This handler will panic
    eventbus.Subscribe(bus, func(event UserEvent) {
        panic("something went wrong!")
    })

    // Handler 3: Will still execute despite handler 2's panic
    eventbus.Subscribe(bus, func(event UserEvent) {
        fmt.Printf("Handler 3 processing user %s\n", event.UserID)
    })

    // Publish event
    eventbus.Publish(bus, UserEvent{UserID: "123", Action: "test"})

    // Output:
    // Handler 1 processing user 123
    // Handler panic detected with: something went wrong! (event: {UserID:123 Action:test})
    // Handler 3 processing user 123
}
```

## Global Hooks

### Logging and Metrics

```go
import (
    "log"
    "reflect"

    eventbus "github.com/jilio/ebu"
)

type UserCreatedEvent struct {
    UserID string
}

type OrderPlacedEvent struct {
    OrderID string
}

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
}
```

### Use Cases for Hooks

1. **Global Logging** - Log all events across your application
2. **Metrics Collection** - Track event counts, latency, and patterns
3. **Distributed Tracing** - Add trace IDs to all events
4. **Event Store** - Persist all events for audit or event sourcing
5. **Service Bridge** - Forward events between microservices
6. **Rate Limiting** - Implement global rate limits
7. **Circuit Breaking** - Detect and handle failure patterns

## Dynamic Handler Management

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
    eventbus.Subscribe(bus, metricHandler)

    // Check if there are handlers
    if eventbus.HasHandlers[MetricEvent](bus) {
        eventbus.Publish(bus, MetricEvent{Name: "cpu_usage", Value: 45.5})
    }

    // Unsubscribe the handler
    eventbus.Unsubscribe(bus, metricHandler)

    // This won't be handled
    eventbus.Publish(bus, MetricEvent{Name: "memory_usage", Value: 62.3})
}
```

## Event Type Helper

```go
package main

import (
    "encoding/json"
    eventbus "github.com/jilio/ebu"
)

type OrderCreatedEvent struct {
    OrderID string
    Amount  float64
}

type UserCreatedEvent struct {
    UserID string
}

// Get the event type string for comparisons
eventType := eventbus.EventType(OrderCreatedEvent{})
// Returns: "main.OrderCreatedEvent"

// Useful in replay scenarios
bus.Replay(ctx, 0, func(event *eventbus.StoredEvent) error {
    switch event.Type {
    case eventbus.EventType(OrderCreatedEvent{}):
        var order OrderCreatedEvent
        json.Unmarshal(event.Data, &order)
        // Process order event
    case eventbus.EventType(UserCreatedEvent{}):
        var user UserCreatedEvent
        json.Unmarshal(event.Data, &user)
        // Process user event
    }
    return nil
})
```

## Custom Event Type Names (TypeNamer)

The `TypeNamer` interface provides explicit control over event type naming, which is useful for:
- Stable type names across package refactoring
- Custom versioning schemes
- Compatibility with external event stores

### Basic TypeNamer Usage

```go
import eventbus "github.com/jilio/ebu"

// Define an event that implements TypeNamer
type UserCreatedEvent struct {
    UserID   string
    Username string
    Email    string
}

// Implement the TypeNamer interface
func (e UserCreatedEvent) EventTypeName() string {
    return "user.created.v1"
}

func main() {
    bus := eventbus.New()

    // The event type will use the custom name
    typeName := eventbus.EventType(UserCreatedEvent{})
    // Returns: "user.created.v1" (not "main.UserCreatedEvent")

    // Works seamlessly with publishing and persistence
    eventbus.Publish(bus, UserCreatedEvent{
        UserID:   "123",
        Username: "john_doe",
        Email:    "john@example.com",
    })
}
```

### Versioned Events

Use TypeNamer to implement event versioning:

```go
// Version 1 of the event
type UserCreatedV1 struct {
    UserID string
    Name   string // Full name in one field
}

func (e UserCreatedV1) EventTypeName() string {
    return "user.created.v1"
}

// Version 2 with split name fields
type UserCreatedV2 struct {
    UserID    string
    FirstName string
    LastName  string
}

func (e UserCreatedV2) EventTypeName() string {
    return "user.created.v2"
}

func main() {
    store := eventbus.NewMemoryStore()
    bus := eventbus.New(eventbus.WithStore(store))

    // Both versions can coexist
    eventbus.Publish(bus, UserCreatedV1{UserID: "1", Name: "John Doe"})
    eventbus.Publish(bus, UserCreatedV2{
        UserID:    "2",
        FirstName: "Jane",
        LastName:  "Smith",
    })

    bus.Wait()

    // Replay and handle different versions
    bus.Replay(ctx, 0, func(event *eventbus.StoredEvent) error {
        switch event.Type {
        case "user.created.v1":
            var v1 UserCreatedV1
            json.Unmarshal(event.Data, &v1)
            // Handle V1 format
        case "user.created.v2":
            var v2 UserCreatedV2
            json.Unmarshal(event.Data, &v2)
            // Handle V2 format
        }
        return nil
    })
}
```

### Stable Names Across Refactoring

TypeNamer prevents breaking changes when you reorganize code:

```go
// Before refactoring: package.OrderCreated
// After refactoring: newpackage.OrderCreatedEvent
// Without TypeNamer: Type name changes, breaks replays!

// With TypeNamer: Type name stays stable
type OrderCreatedEvent struct {
    OrderID string
    Amount  float64
}

func (e OrderCreatedEvent) EventTypeName() string {
    // This name remains stable regardless of package/type renaming
    return "order.created"
}
```

### External System Compatibility

Use TypeNamer to match external event naming conventions:

```go
// Events from Kafka or other systems with specific naming
type PaymentProcessedEvent struct {
    PaymentID string
    Status    string
}

// Match the external system's event type format
func (e PaymentProcessedEvent) EventTypeName() string {
    return "com.example.payment.ProcessedEvent"
}
```

### Best Practices

1. **Use semantic versioning** in type names (e.g., "user.created.v1")
2. **Keep names immutable** once events are persisted
3. **Document type name changes** in migration guides
4. **Use dot notation** for hierarchical namespacing (e.g., "domain.entity.action.version")

## Best Practices Summary

1. **Define clear event types** - Use structs with descriptive names
2. **Keep events immutable** - Don't modify events after publishing
3. **Handle errors gracefully** - Prefer returning errors over panicking
4. **Use async for I/O** - Keep synchronous handlers fast
5. **Leverage context** - Use `PublishContext` for cancellable operations
6. **Clean up handlers** - Use `Unsubscribe` or `Clear` when done
7. **Set panic handlers** - Monitor and log handler failures
8. **Test concurrency** - The bus is thread-safe, test your handlers too
