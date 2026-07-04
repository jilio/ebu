# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.11.0] - 2026-07-04

### Fixed

- **SubscribeWithReplay now replays `TypeNamer` events.** The replay filter
  compared stored type names against the reflection name, so events with a
  custom `EventTypeName()` were silently skipped — replayed data loss.
- **Persistence can no longer be silently disabled by option ordering.**
  `WithStore` used to install persistence by chaining onto the
  `beforePublishCtx` hook; a `WithBeforePublishContext` option applied after
  it overwrote the chain. Persistence now runs directly in the publish path.
- **SubscribeWithReplay saves the offset of the exact event it handled.** It
  previously saved a bus-global "last persisted offset" that concurrent
  publishes (of any event type) could advance, so a crash could skip events
  on the next start. Each delivery now carries its own offset via the
  handler context (see `OffsetFromContext`), and `SaveOffset` failures are
  reported to the `PersistenceErrorHandler` instead of being dropped.
- **durablestream: `Read` no longer skips events when `limit` truncates.**
  It returned the chunk-end offset with a truncated result, so the events
  beyond the limit were never delivered on the next read. Limit truncation
  now returns the last returned event's offset when events carry real
  offsets, and is best-effort (full chunk) when offsets are synthetic.
- **sqlite: offsets are zero-padded** so they compare lexicographically, as
  the `Offset` contract requires (`"999" < "1000"` fails as plain strings).
  Legacy unpadded offsets are still accepted as input.
- **sqlite: two `:memory:` stores no longer share a database** through
  SQLite's shared cache; each store gets a unique in-memory database.
- **`Wait`/`Shutdown` are now safe to call concurrently with `Publish`.**
  Async-handler tracking used a `sync.WaitGroup`, which forbids `Add` racing
  `Wait` at counter zero (a real data race under `-race`).
- **A cancelled context can no longer consume a `Once` handler without
  executing it.** Cancellation is checked before the handler's once-slot is
  claimed; once claimed, the handler always runs.
- **Mismatched `WithFilter` predicates are rejected at Subscribe time.**
  Previously a predicate whose parameter type didn't match the subscription
  compiled fine and was silently ignored — the handler received *all* events.
- **`WithUpcast` panics on invalid registration** (self-upcast, cycle, nil
  function) instead of silently ignoring the error.
- Fixed a flaky test that hung the suite under `-race`; the race detector now
  runs (and passes) in CI for all modules.

### Added

- `OffsetFromContext(ctx)` — inside handlers on a persistent bus, returns the
  offset the event being handled was persisted at.
- Delivery-semantics documentation: persistence is best-effort by default;
  `SubscribeWithReplay` is at-least-once (make replay handlers idempotent).
- `Publish`/`PublishContext` panic with a clear message on a nil bus;
  `Unsubscribe` returns an error instead of panicking.

### Changed

- **Module packaging repaired.** `stores/sqlite`, `stores/durablestream`, and
  `otel` referenced ebu versions that were never published (or used a
  `replace` directive), making them uninstallable outside this repo. All
  sub-modules now require a real, tagged ebu version and are covered by CI,
  each tagged per Go multi-module convention (e.g. `stores/sqlite/v0.11.0`).
- Publishes are no longer globally serialized around `EventStore.Append`;
  stores handle their own concurrency (all bundled stores do).
- `MemoryStore.Read`/`ReadStream` use binary search instead of scanning the
  log from the start.
- `SetPanicHandler`, `SetBeforePublishHook`, and `SetAfterPublishHook` are
  deprecated in favor of the equivalent `New` options.

### Removed

- **`EventStoreSubscriber` interface.** It was never consumed by the bus and
  no bundled store implemented it; keeping it implied live-subscription
  support that did not exist.
- Unreachable handler-dispatch branches (raw `func(T)`, `func(any)`, and
  reflection fallbacks): handlers can only be registered as `Handler[T]` or
  `ContextHandler[T]` through the public API.

## [0.10.0] - 2025-12-28

### Breaking Changes

- **EventStore interface redesigned** to use opaque `Offset` type (string) instead of int64 positions
- **SubscribeWithReplay** now requires `context.Context` as first parameter
- **SQLite store** method renames:
  - `Save` → `Append`
  - `Load` → `Read`
  - `LoadStream` → `ReadStream`
  - `SaveSubscriptionPosition` → `SaveOffset`
  - `LoadSubscriptionPosition` → `LoadOffset`
  - `GetPosition` removed (no longer needed with opaque offsets)
  - `MetricsHook` interface updated to match new method names
- **Durable-streams store** API changed:
  - `New(streamURL)` → `New(baseURL, streamPath, opts...)`
  - `WithRetry()` option removed (handled by underlying client library)

### Added

- New `Offset` type with `OffsetOldest` ("") and `OffsetNewest` ("$") constants
- `Event` struct for events before storage assignment
- Optional `EventStoreStreamer` interface for memory-efficient streaming
- Optional `EventStoreSubscriber` interface for live subscriptions
- Separate `SubscriptionStore` interface for subscription position tracking
- `WithSubscriptionStore` option for configuring subscription storage
- `WithReplayBatchSize` option for configuring replay batch size
- Durable-streams store now uses conformance-tested `ahimsalabs/durable-streams-go` client library

### Changed

- `StoredEvent` now uses `Offset` instead of `Position`
- `EventStore` reduced to 2 core methods: `Append` and `Read`
- MemoryStore uses zero-padded offsets (20 digits) for correct lexicographic ordering
- Replay returns error on stuck offset instead of silent break

## [0.9.2] - 2025-12-04

### Changed

- Optimize streaming with filtered snapshots and boundary check for improved performance

## [0.9.1] - 2025-11-29

### Fixed

- Propagate context to persistence for proper trace hierarchy in OpenTelemetry

## [0.9.0] - 2025-11-29

### Added

- SpanAttributer interface for enriching OpenTelemetry spans with custom attributes
- SQLite event store implementation (`stores/sqlite` module)
- Streaming/iterator support for EventStore

### Fixed

- Improve SpanAttributer test coverage

## [0.8.6] - 2025-11-16

### Added

- Call `store.Close()` in Shutdown if the store implements `io.Closer`

## [0.8.5] - 2025-11-12

### Added

- Graceful `Shutdown(context.Context)` method for clean application termination
- Waits for all async handlers to complete with context timeout support

### Fixed

- Remove empty event processor goroutine that was causing resource leaks

## [0.8.4] - 2025-11-11

### Added

- Store interface for event sourcing patterns (later reverted in v0.8.5)

## [0.8.3] - 2025-11-09

### Added

- OpenTelemetry observability support (`otel` module)
- Metrics for event publish, handler execution, and persistence
- Distributed tracing with context propagation across async handlers
- Per-event-type metric labeling
- Example with Docker Compose, Prometheus, Grafana, and Jaeger

## [0.8.2] - 2025-11-08

### Added

- TypeNamer interface for explicit event type control

## [0.8.1] - 2025-11-08

### Changed

- Remove prescriptive store recommendations from persistence guide

## [0.8.0] - 2025-08-22

### Added

- **Event Upcasting** for seamless event migration
  - Type-safe `RegisterUpcast` with generics
  - Support for upcast chains (V1 → V2 → V3)
  - Circular dependency detection
  - Lazy evaluation for optimal performance
  - Integration with SubscribeWithReplay
- **Persistence Error Handling**
  - `PersistenceErrorHandler` for handling save/marshal failures
  - Configurable timeouts for storage operations
  - Position management ensures consistency on failures
  - Runtime configuration support

### Changed

- Remove unused Extensions API for cleaner codebase

## [0.7.1] - 2025-08-17

### Fixed

- Ensure consistent type naming between `EventType()` and persistence layer

## [0.7.0] - 2025-08-17

### Added

- Predicate-based event filtering with `WithFilter` option

## [0.6.0] - 2025-08-17

### Breaking Changes

- Remove CQRS implementation (CommandBus, QueryBus, ProjectionManager)
- Remove Aggregate interfaces and implementations
- Remove Snapshot functionality

### Changed

- Library now focuses purely on event bus functionality
- Consolidate test files for better organization

## [0.5.0] - 2025-08-16

### Added

- Sharded EventBus architecture with 32 shards for improved performance
- FNV-1a hashing for distributed locking
- Lock-free extensions using sync.Map
- Extensive benchmarks for performance validation

### Performance

- Concurrent publishers: 150% faster (1.68ms → 673µs)
- Concurrent subscribe/unsubscribe: 36% faster (339µs → 217µs)

## [0.4.0] - 2025-08-16

### Breaking Changes

- All CQRS types now require generic type parameters
- Event type storage format includes package path
- `ProjectionManager.Register` no longer auto-subscribes to events

### Added

- Complete CQRS module refactor with Go generics for type safety
- Options Pattern for all CQRS components:
  - `WithCommandPreHandler` / `WithCommandPostHandler`
  - `WithQueryCache` / `WithQueryLogger`
  - `WithAsyncProjections` / `WithErrorHandler`
- Helper functions: `SubscribeProjection`, `SetupCQRSProjections`
- `AggregateCommandHandler` for simplified command handling
- Type-safe aggregate stores with generics

### Changed

- Replace all `interface{}` with `any` keyword

## [0.3.0] - 2025-08-15

### Added

- Complete CQRS implementation with commands, queries, and projections
- Event sourcing support with aggregates
- Projection builder for read models
- Synchronous projection handlers for event ordering

## [0.2.1] - 2025-08-12

### Fixed

- Fix gofmt -s formatting throughout the codebase
- Remove all trailing whitespace

## [0.2.0] - 2025-08-12

### Added

- Global event interception through publish hooks
- `SetBeforePublishHook`: Intercept events before handler execution
- `SetAfterPublishHook`: Intercept events after handler execution
- Zero performance impact when hooks not used

## [0.1.0] - 2025-08-02

Initial release of ebu (Event BUs) - a lightweight, type-safe event bus for Go.

### Added

- Type-safe event handling with Go generics
- Synchronous and asynchronous event handlers
- Context support for cancellation and tracing
- One-time event handlers with `Once` option
- Sequential async processing option
- Thread-safe operations
- Panic recovery with custom handlers
- 100% test coverage
- Zero dependencies

### API

- `Subscribe`: Register event handlers
- `SubscribeContext`: Register context-aware handlers
- `Publish`: Send events to handlers
- `PublishContext`: Send events with context
- `Unsubscribe`: Remove specific handlers
- `HasSubscribers`: Check for registered handlers
- `Clear`: Remove all handlers for an event type
- `ClearAll`: Remove all handlers
- `WaitAsync`: Wait for async handlers to complete

[Unreleased]: https://github.com/jilio/ebu/compare/v0.11.0...HEAD
[0.11.0]: https://github.com/jilio/ebu/compare/v0.9.2...v0.11.0
[0.10.0]: https://github.com/jilio/ebu/compare/v0.9.2...v0.10.0
[0.9.2]: https://github.com/jilio/ebu/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/jilio/ebu/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/jilio/ebu/compare/v0.8.6...v0.9.0
[0.8.6]: https://github.com/jilio/ebu/compare/v0.8.5...v0.8.6
[0.8.5]: https://github.com/jilio/ebu/compare/v0.8.4...v0.8.5
[0.8.4]: https://github.com/jilio/ebu/compare/v0.8.3...v0.8.4
[0.8.3]: https://github.com/jilio/ebu/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/jilio/ebu/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/jilio/ebu/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/jilio/ebu/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/jilio/ebu/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/jilio/ebu/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/jilio/ebu/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/jilio/ebu/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/jilio/ebu/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jilio/ebu/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/jilio/ebu/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/jilio/ebu/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jilio/ebu/releases/tag/v0.1.0
