# Distributed ebu: scaling beyond a single process

ebu's core is an in-process bus. Add a shared `EventStore` and a `Follow`
loop, and it becomes a cross-process one: **every process appends to the
shared log by publishing, and receives by following.**

```
process A                    shared log                    process B
─────────                 (sqlite / durable-streams / …)   ─────────
Publish ──append──▶  ┌────┬────┬────┬────┬────┐
                     │ e1 │ e2 │ e3 │ e4 │ e5 │ ◀──append── Publish
handlers ◀──Follow── └────┴────┴────┴────┴────┘ ──Follow──▶ handlers
```

## Quick start

```go
// Both processes: same store, same setup.
store, _ := durablestream.New("http://streams:4437/v1/stream", "orders")
bus := eventbus.New(eventbus.WithStore(store))

// Subscribe BEFORE following: only subscribed types can be decoded.
eventbus.Subscribe(bus, func(e OrderCreated) { ... })

// Tail the log; new events from any process are dispatched locally.
go func() {
    if err := bus.Follow(ctx); err != nil && !errors.Is(err, context.Canceled) {
        log.Printf("follower stopped: %v", err)
    }
}()

// Publish as usual — peers receive it via their followers.
eventbus.Publish(bus, OrderCreated{ID: "o-1"})
```

`Follow` blocks until its context is cancelled, so run it on a goroutine.
When the store implements `EventStoreTailer` (durable-streams does — it uses
the protocol's live long-poll/SSE modes), events are pushed within one round
trip; otherwise `Follow` polls `Read` on `FollowPollInterval` (default 200ms).

## Delivery modes

### Default: local-first + follower

`Publish` persists and dispatches to local handlers immediately, exactly as
before. The follower delivers *other* processes' events and skips this bus's
own (matched by the envelope's `Origin`), so nothing arrives twice. This mode
adds cross-process delivery without changing local latency or semantics.

### `WithLogDelivery`: the log is the only delivery path

```go
bus := eventbus.New(
    eventbus.WithStore(store),
    eventbus.WithLogDelivery(), // requires WithStore; New panics without it
)
```

`Publish` appends and returns; **all** delivery — including to the publishing
process — happens through `Follow`. Consequences:

- Every process observes **the same events in the same order** (the log's
  order), which makes cross-process state machines and projections sane.
- An event that failed to persist is never observed anywhere (strict
  persistence by construction).
- Local delivery latency becomes the follower's latency; the process MUST
  run `bus.Follow` or nothing is ever handled.
- Unless `FollowFrom` or a saved durable offset says otherwise, the follower
  starts at `OffsetOldest`. That complete default closes the startup window in
  which an event could be appended after `Publish` stopped delivering locally
  but before `Follow` resolved the tail. Use `FollowWithSubscriptionID` in
  production so restarts resume instead of replaying the whole log.

## Durable followers

```go
err := bus.Follow(ctx,
    eventbus.FollowWithSubscriptionID("billing-projector"),
)
```

With a subscription ID, the follower saves its position after each processed
event (via the `SubscriptionStore`) and resumes there after a restart. The
first run consumes the log from the beginning (or from `FollowFrom` if
given). It persists that concrete first-run boundary before starting `Read` or
`Tail`; an initialization save failure aborts startup, while later progress
save failures are reported and remain at-least-once. This means an explicit
`FollowFrom(OffsetNewest)` is resolved once and cannot skip events appended
between cancellation and restart. Custom checkpoint stores should implement
`SubscriptionStoreLookup` when combining a durable ID with a first-run offset
other than `OffsetOldest`, because `LoadOffset` alone cannot distinguish an
absent checkpoint from a legitimately saved `OffsetOldest`. Without a
subscription ID, a default-mode follower starts at the tail
(live events only); a `WithLogDelivery` follower starts at the beginning.
Within one `EventBus`, an ID has one active durable owner across `Follow` and
resumable subscriptions. A `Follow` call releases that ownership when it
returns, so cancellation followed by restart on the same bus is supported.

For `Async()` handlers, a durable follower waits for every async handler for
the current stored event to finish before advancing its checkpoint. Ordinary
in-process `Publish` remains non-blocking. A crash can still happen after the
handler finishes but before the checkpoint is saved, so delivery remains
at-least-once.

A recovered sync/async handler panic or an upcast failure is also a failed
durable attempt: it is reported, its event ID is removed from the dedup window,
and polling/tailing retries from the unchanged offset after backoff. `Once()`
registrations selected for a failed event are restored so the complete set can
run again; they are removed only when the event attempt succeeds. Cancellation
interrupts async-capacity waits and completion waits without checkpointing.

## At-least-once, and what to do about it

Every layer of a shared log is at-least-once: appends may be retried, chunked
reads may re-deliver on resume, a crash between handling and offset save
replays. ebu absorbs most duplicates for you:

- Every publish gets a ULID `Event.ID`, minted **before** the first append
  attempt — retried appends carry the same ID.
- The follower drops IDs it has recently seen (`FollowDedupWindow`, default
  1024; `0` disables).

For stronger guarantees (e.g. duplicates arriving outside the window, or two
followers of the same subscription), make handlers idempotent keyed on the
event ID — `EventIDFromContext(ctx)` in live/followed handlers,
`EventIDFromContext(ctx)` with `SubscribeContextWithReplay`, or
`StoredEvent.ID` with the raw `Replay` API.

## Semantics inside followed handlers

Dispatch is identical to a local publish: filters, `Once()`, `Async()`,
`Sequential()`, and panic recovery all apply, and `OffsetFromContext`,
`EventIDFromContext`, and `MetadataFromContext` work. Publish *hooks* and
publish-level observability do not re-fire (they fired in the publishing
process); handler-level observability does.

Registered upcasts are applied before decoding, so old events on the stream
arrive in their current schema — same as `Replay`.

Every persisted/distributed event type should implement `TypeNamer` with an
immutable, globally unique name. The reflection fallback uses only Go's
declared package name, not the full import path. If two subscribed Go types on
a persistent bus claim the same wire name, or a publish/typed upcast conflicts
with an existing claim, the operation fails rather than letting `Follow`
decode the stream according to registration order. The registry is per bus,
so independent producers must still coordinate globally unique names.

## Failure behavior

- **Read/tail errors**: reported to the `PersistenceErrorHandler`, retried
  after `FollowPollInterval`. The follower does not die.
- **Poison events** (payload does not decode into the subscribed type):
  reported with the `*StoredEvent` for out-of-band recovery, then normally
  skipped. A local `SubscribeWithReplay` coordinator sees the stored envelope
  first and applies its own policy: `ReplaySkip` advances that projection;
  `ReplayAbort` keeps both it and a durable outer follower uncheckpointed.
- **Upcast failure**: reported with the `*StoredEvent`; a durable follower
  retries from the unchanged checkpoint, while a non-durable follower skips
  that envelope and continues. Neither path dispatches the original schema as
  though migration had succeeded.
- **Durable handler panic**: reported and retried after `FollowPollInterval`,
  never checkpointed until successful. Non-durable followers keep ordinary
  panic isolation and continue.
- **Unsubscribed types**: skipped silently — streams may carry many types.
- `Follow` itself returns on context cancellation or a startup/configuration
  failure; runtime delivery/read failures retry as described above.

## Current limitations (roadmap)

- **Competing consumers**: the bus rejects overlapping owners of one durable
  subscription ID within that bus, but there is no cross-process lease/lock
  yet. Two processes (or independent bus instances) can still deliver
  duplicates and interleave offset saves. Planned: offset leases and key-based
  partitioning for ordered scale-out.
- **Backends**: sqlite works cross-process on one machine (WAL) and
  durable-streams over the network. A Postgres store (table +
  LISTEN/NOTIFY tail) is the natural next backend; the `EventStore` +
  `EventStoreTailer` interfaces are all it needs to implement.
