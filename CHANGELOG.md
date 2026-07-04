# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.16.0] - 2026-07-05

### Added

- **Cross-process delivery: `Follow`.** A follower tails the bus's event
  store and dispatches new events to local subscribers with full option
  semantics (filters, `Once`, `Async`, `Sequential`, panic recovery, context
  values). Every process appends by publishing and receives by following —
  the shared log becomes the bus. Events this bus itself published are
  skipped by `Origin` (override with `FollowIncludeOwn`); at-least-once
  duplicates are dropped by event ID (`FollowDedupWindow`, default 1024);
  `FollowWithSubscriptionID` makes the follower durable across restarts;
  read errors retry and poison events are reported and skipped, so the
  follower never wedges. See docs/DISTRIBUTED.md.
- **`WithLogDelivery`.** Makes the store the only delivery path: `Publish`
  appends and returns, and all delivery — including in the publishing
  process — happens through `Follow`. Every process then observes the same
  events in the same order, and an event that failed to persist is never
  observed anywhere. Requires `WithStore` (New panics otherwise).
- **`EventStoreTailer`.** Optional store capability for push-based tailing;
  `Follow` uses it when available and falls back to polling `Read`
  otherwise.
- **durablestream: `Tail`** implements `EventStoreTailer` over the
  protocol's live long-poll/SSE modes, so followers receive new events
  within one round trip. Transient failures (including client-side timeouts
  of idle long-polls) retry forever with capped backoff; permanent protocol
  errors end the tail.

- **Event envelope: `ID`, `Origin`, `Metadata`.** Every persisted event now
  carries a ULID `ID` minted once per publish (store-level retries reuse it,
  so it is a reliable deduplication key for at-least-once delivery), the
  publishing bus instance's `Origin` (`bus.OriginID()`), and optional
  publisher-supplied `Metadata` attached with `ContextWithMetadata`. Live
  handlers can read the ID and metadata with `EventIDFromContext` /
  `MetadataFromContext`. All fields are optional on the wire, so existing
  streams and external producers remain compatible. `NewEventID` (a
  dependency-free ULID generator) is exported.
- **`WithStrictPersistence`.** Makes persistence a delivery precondition: a
  failed marshal or `Append` skips handler delivery, so handlers never
  observe an event the log did not record and replay cannot diverge from
  live handling. Default behavior (best-effort) is unchanged.
- **`TryPublish` / `TryPublishContext`.** Publish variants that return the
  persistence error, for publishers that must act on failure (e.g. fail the
  originating request). Nil when persistence succeeded or no store is
  configured.
- **Subscription handles.** `SubscribeWithHandle` /
  `SubscribeContextWithHandle` return a `*Subscription` whose `Unsubscribe`
  removes exactly that registration by identity — closing the long-standing
  `Unsubscribe` footgun where two closures from the same function literal
  share a code pointer and cannot be told apart. Idempotent and safe for
  concurrent use.
- **sqlite: schema v4** adds nullable `event_id`, `origin`, and `metadata`
  columns for the envelope. The migration tolerates concurrent migrators
  (duplicate-column errors are treated as already-applied) and rows written
  by earlier versions read back with empty envelope fields.
- **durablestream: envelope fields** are written and read as optional JSON
  keys (`id`, `origin`, `metadata`), byte-compatible with pre-envelope
  streams.

### Documented

- `Async()` + `SubscribeWithReplay` offset saves can complete out of publish
  order; delivery stays at-least-once but a crash may redeliver more history.
  Prefer synchronous replay handlers or idempotency on `StoredEvent.ID`.

## [0.15.0] - 2026-07-05

### Fixed

- **`Replay` no longer mistakes an empty batch for the end of the stream.**
  A store may return zero events while still advancing the offset (e.g. a
  remote chunk whose events were all skipped as undecodable). The fallback
  read loop treated any empty batch as the tail and stopped, silently
  dropping every event after the gap. The tail is now detected only by a
  non-advancing offset.
- **`Sequential()` mutual exclusion holds during replay.**
  `SubscribeWithReplay`'s replay passes called the handler without taking
  the handler mutex, so once the live subscription registered (before the
  catch-up pass) a concurrent `Publish` could enter a `Sequential()` handler
  while the replay pass was inside it. The replay path now takes the same
  lock as live delivery.
- **sqlite: batched `ReadStream` no longer swallows mid-iteration errors.**
  `database/sql` surfaces mid-iteration failures — including context
  cancellation — only through `rows.Err()`, which the batched stream path
  never checked. A cancelled or failing replay could therefore terminate
  cleanly after a fraction of the log, and a projection built from it was
  silently stale. Batched streaming (the default) now yields the error, and
  responds to cancellation per row rather than per batch.
- **durablestream: `WithTimeout` is honored.** The pinned client stores but
  never applies its configured timeout, and the previous default HTTP client
  had none, so a remote that accepted the connection and never responded
  blocked `Append` forever — while holding the append lock, wedging every
  publisher. The default HTTP client now carries the configured timeout, and
  `Read`/`Create` wrap each attempt in a timeout context.
- **durablestream: chunks with no decodable events no longer end `Replay`
  early.** `Read` now advances chunk by chunk (bounded by the server's
  `UpToDate` signal) until it can return at least one event or the tail is
  reached, instead of returning an empty advancing result the bus's read
  loop used to misread as end-of-stream.
- **durablestream: the cached stream writer is detached from its creator's
  context.** Cancelling the first `Append` caller's context could abort
  later, unrelated `Append`s (and with retries exhausted, fail them) because
  the writer captured that context for all subsequent sends.
- **state: events are routed by stored type, and foreign events can no
  longer wedge or corrupt the materializer.** `Apply` previously classified
  by JSON shape alone: any event carrying a `headers` field — an HTTP log,
  say — was force-decoded as a state message; failure aborted replay without
  advancing the offset, wedging every future replay on the same event, and a
  foreign `headers.control` string was consumed as a control message.
  Events typed `state.ChangeMessage`/`state.ControlMessage` are now decoded
  strictly; everything else is detected structurally and skipped as foreign
  unless it positively identifies as a state protocol message.
- **state: pointer entity types with a `TypeNamer` no longer panic.**
  `Insert[*T]`, `NewTypedCollection[*T]`, and `EntityType` on a typed nil
  pointer derived the name by calling the method on a nil zero value.
- **state: collections sharing one `Store` are isolated.** `All`, snapshot,
  restore, and reset were unscoped, so one collection could read, snapshot,
  or clear another's entities. Every collection operation is now scoped to
  its `entityType/` key prefix.
- **state: `SaveSnapshotTo` no longer blocks message application during
  snapshot I/O.** Application pauses only while collections are captured;
  the `SaveSnapshot` call runs outside the apply lock. Concurrent savers are
  serialized with each other so an older snapshot can never overwrite a
  newer one.

### Added

- **`OffsetNewest` has a defined, uniform contract across all stores**: it
  resolves at call time to the concrete current tail. Previously the three
  bundled stores disagreed — sqlite returned a parse error, `MemoryStore`
  replayed from the beginning (`"$"` sorts before zero-padded digits), and
  durablestream sent the literal `"$"` to the server.
- **state: `WithApplyErrorPolicy(ApplySkip)`** — the materializer equivalent
  of `ReplayErrorPolicy`: reports undecodable (poison) state messages to
  `WithOnError` and advances past them instead of aborting every future
  replay on the same event. Store failures are never skipped. New
  `ErrUndecodable` sentinel distinguishes the two.
- **state: strict mode reports unknown control values** instead of silently
  dropping them (a missed `reset` means knowingly stale state).
- **otel: explicit sub-millisecond histogram buckets** for the handler and
  persist duration metrics (SDK defaults start at 5ms, collapsing typical
  in-process handler latencies into one bucket), plus an `error` attribute
  on duration records so failure latencies are separable.

### Changed

- **state (breaking): `Store` methods return errors** (`Get` returns
  `(T, bool, error)`, `All` returns `(map[string]T, error)`, `Set`/`Delete`/
  `Clear` return `error`), and `TypedCollection.Get`/`All` and
  `ApplyControlMessage` propagate them. A durable backend that could only
  swallow failures let the materializer advance past updates that were never
  applied — and a snapshot plus truncation then made the loss permanent. The
  materializer never advances its offset when a store call fails.
- **Registering a second upcaster for the same source type now returns an
  error.** Upcasts follow a single chain per type, so the duplicate could
  never run; it was accepted silently and never applied.
- **durablestream: append semantics documented as at-least-once.** A retried
  append whose first attempt committed but lost its response is duplicated;
  the pinned protocol's sequence tokens cannot distinguish that case safely
  (treating the conflict as success could silently lose events instead).
  Consumers should be idempotent; retry backoff is now capped.

## [0.14.0] - 2026-07-04

### Added

- **`WithReplayErrorPolicy` subscribe option.** By default an undecodable
  stored event aborts `SubscribeWithReplay` and, because its offset is never
  saved, aborts it again on every restart — a single poison event could
  permanently wedge a subscription. `ReplaySkip` instead reports the event to
  the `PersistenceErrorHandler` (as its `*StoredEvent`, so the payload can be
  recovered out of band) and continues. The skip is durable: the poison
  event's own offset is saved, so it is not re-scanned and re-reported on
  every restart even when it is the last event in the stream. The default
  (`ReplayAbort`) is unchanged. Scope note: the policy covers decode failures
  of the subscribed type only — events stored under other type names are
  always skipped silently (streams may carry many types), and JSON that
  decodes leniently despite schema drift is delivered (use upcasts for
  schema evolution).

### Fixed

- **Interface-typed registrations are rejected instead of silently dead.**
  Publish routes by the event's dynamic (concrete) type and stores events
  under concrete type names, so a `Subscribe[I]` handler or a
  `RegisterUpcast` keyed by an interface type could never fire — with no
  error. `Subscribe`, `SubscribeContext`, `SubscribeWithReplay`, and
  `RegisterUpcast` now return an error for interface types.
- **`SubscribeWithReplay` runs all validation before the replay pass.**
  Previously a nil option or a `WithFilter` predicate of the wrong type was
  only rejected when the live subscription registered — after the replay
  pass had already delivered events and saved offsets, permanently consuming
  them despite the call returning an error. A nil bus or nil handler
  panicked (nil handler mid-replay) instead of returning an error like the
  other subscribe entry points. All argument and option validation now
  happens first; a failing call delivers nothing and saves nothing.
- **`WithFilter` now applies to replayed events.** The replay and catch-up
  passes delivered every stored event of the subscribed type to the handler
  regardless of the subscription's filter (and saved offsets for them),
  while live delivery filtered correctly. Replay now honors the predicate
  exactly like live delivery: filtered events are not delivered and their
  offsets are not saved.
- **Subscribe options are applied exactly once per subscription.**
  `SubscribeWithReplay` briefly applied options twice (once to read
  replay-affecting options, once when registering); it now builds the
  subscription once, up front, and registers that same instance. All
  subscription entry points share one validation/registration chokepoint
  (`buildHandler`/`addHandler`) so no entry point can miss a check.

### Documented

- **`WithAsyncHandlerLimit` re-entrancy deadlock.** With a limit set, a
  nested `Publish` of async-handled events from inside an async handler
  blocks waiting for a slot the publishing handler occupies; if all slots are
  held by handlers blocked this way, none can be released.

## [0.13.0] - 2026-07-04

### Fixed

- **sqlite: concurrent appends to file-backed databases no longer fail with
  SQLITE_BUSY.** Pragmas were applied via `db.Exec`, reaching only one
  pooled connection, and the `_busy_timeout=` DSN parameter is mattn syntax
  that modernc.org/sqlite silently ignores — so under concurrent publishing
  ~84% of appends failed and, because persistence is best-effort, silently
  vanished from the log. All connection-scoped pragmas now travel in the DSN
  as `_pragma=` parameters (plus `_txlock=immediate`), so every pooled
  connection gets them. A file-backed concurrency test (10×100 appends, zero
  tolerated failures) guards the fix; the old test used `:memory:`, which
  takes a different lock path and could not catch it.
- **sqlite: concurrent `New()` across processes no longer races.** Schema
  version seeding uses `INSERT OR IGNORE`, and the one-time WAL conversion —
  which SQLite reports as SQLITE_BUSY without consulting the busy handler —
  is retried with backoff.
- **sqlite: `:memory:` stores pin a dedicated connection** so pool churn can
  no longer silently destroy the shared-cache in-memory database.
- **durablestream: every offset the store emits is now server-issued and
  safe to store for resumption.** Previously events read without embedded
  offsets got synthetic `"nextOffset/i"` offsets that the bus saved as
  resume positions and the server would not recognize, structurally breaking
  `SubscribeWithReplay` with this store. Events within a chunk now carry the
  chunk-start offset (resume re-reads the chunk: duplicates possible, skips
  impossible), the last event of a chunk carries the server's next-offset,
  and `Append`'s return value is documented as the exact resume point after
  the appended event.
- **state: message application is serialized.** A concurrent `Reset` control
  message could be overwritten by a logically-earlier change that was applied
  after the reset cleared collections; `LastOffset` could also advance past a
  concurrently failed event, permanently skipping it on resume. Apply,
  ApplyChangeMessage, and ApplyControlMessage now run one at a time, and
  `LastOffset` only advances past successfully applied events.
- **state: unknown operations are errors.** A change message with a missing
  or misspelled `operation` (e.g. `"Insert"`) was silently ignored while the
  offset still advanced — a producer bug produced zero signal. It now
  returns an error, fires `WithOnError`, and does not advance the offset.
- **state: `WithOnError` fires on every error path** (envelope unmarshal,
  change unmarshal, strict-mode unknown type, collection apply), not just
  collection apply failures.
- **state: non-state events on mixed streams are skipped cleanly** in both
  strict and non-strict mode (previously strict mode aborted replay with
  `unknown entity type: ` on the first foreign event).
- **state: `Materializer.Replay` applies upcasts** (it used `bus.Replay`
  instead of `bus.ReplayWithUpcast`, bypassing schema migration).
- **SubscribeWithReplay no longer has a replay→live gap.** An event persisted
  after the replay pass drained but before the live subscription registered
  was previously missed until the next restart. A catch-up replay pass now
  runs after subscription registration; events in the overlap window may be
  delivered twice (at-least-once — keep handlers idempotent).
- **Upcast functions no longer run under the registry lock.** An upcast
  function that called back into the registry (e.g. `RegisterUpcast`)
  deadlocked; registration during an in-flight chain is now safe.
- **`SetUpcastErrorHandler` is now synchronized** with concurrent
  replay/publish (previously a data race).
- **otel: durations are recorded with sub-millisecond precision.**
  `duration.Milliseconds()` truncated, so typical in-process handlers all
  recorded 0ms and the histograms were useless.
- **otel: the persist span no longer carries an always-0 `position`
  attribute.** The store-assigned offset is recorded on the span at
  completion instead (`event.offset`).

### Changed

- **`Observability` interface (breaking).** `OnHandlerComplete` and
  `OnPersistComplete` now receive the event type, and `OnPersistComplete`
  receives the store-assigned `Offset`; `OnPersistStart` loses its
  meaningless `position` parameter. This removes per-handler context-value
  allocations in the otel implementation and gives error metrics correct
  attributes.
- **`Offset` ordering contract relaxed.** Offsets are documented as opaque
  resumption tokens; ordering is store-defined. The bundled MemoryStore and
  sqlite store still produce lexicographically ordered offsets.
- Deprecated `Set*` configuration methods are documented as not safe to call
  concurrently with `Publish`.
- **sqlite: streaming reads are batched by default** (batch size 1000).
  Unbatched `ReadStream` pinned a connection and WAL read snapshot for the
  entire iteration — a slow consumer meant unbounded WAL growth. Batched
  iteration observes concurrently appended events; the previous point-in-time
  snapshot behavior remains available via `WithStreamBatchSize` (documented
  there).
- **sqlite: dropped the unused `idx_events_type` index** (schema v3) — no
  query filters by type; it was pure write amplification on every append.

### Added

- `WithAsyncHandlerLimit(n)` bounds concurrently running async handler
  goroutines; `Publish` blocks when the limit is reached (backpressure
  instead of unbounded goroutine growth).
- **state: snapshot orchestration.** `Materializer.SaveSnapshotTo` and
  `LoadSnapshotFrom` bridge the materializer to `EventStoreSnapshotter`, so
  compacting a projection is now: load snapshot → replay tail → periodically
  save snapshot → optionally `TruncateBefore` the snapshot offset.
- **durablestream: `WithRetry(attempts, baseDelay)`** — bounded
  exponential-backoff retry for transient failures (network errors, 5xx,
  429) on Append and Read; defaults to 3 attempts / 100ms. A transient blip
  is no longer a permanent gap in the event log.
- **durablestream: `WithDecodeErrorHandler`** — malformed stored events were
  skipped silently unless a logger was configured; now a dedicated callback
  can observe them.
- **durablestream: cached stream writer.** Append no longer performs a HEAD
  request per call (2 HTTP round trips → 1 after the first append); the
  cached writer is invalidated and recreated on failure.

## [0.12.0] - 2026-07-04

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
  each tagged per Go multi-module convention (e.g. `stores/sqlite/v0.12.0`).
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

[Unreleased]: https://github.com/jilio/ebu/compare/v0.13.0...HEAD
[0.13.0]: https://github.com/jilio/ebu/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/jilio/ebu/compare/v0.11.0...v0.12.0
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
