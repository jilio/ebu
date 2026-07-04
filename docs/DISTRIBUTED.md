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

## Durable followers

```go
err := bus.Follow(ctx,
    eventbus.FollowWithSubscriptionID("billing-projector"),
)
```

With a subscription ID, the follower saves its position after each processed
event (via the `SubscriptionStore`) and resumes there after a restart. The
first run consumes the log from the beginning (or from `FollowFrom` if
given). Without a subscription ID, `Follow` starts at the tail: live events
only.

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
`StoredEvent.ID` in replay.

## Semantics inside followed handlers

Dispatch is identical to a local publish: filters, `Once()`, `Async()`,
`Sequential()`, and panic recovery all apply, and `OffsetFromContext`,
`EventIDFromContext`, and `MetadataFromContext` work. Publish *hooks* and
publish-level observability do not re-fire (they fired in the publishing
process); handler-level observability does.

Registered upcasts are applied before decoding, so old events on the stream
arrive in their current schema — same as `Replay`.

## Failure behavior

- **Read/tail errors**: reported to the `PersistenceErrorHandler`, retried
  after `FollowPollInterval`. The follower does not die.
- **Poison events** (payload does not decode into the subscribed type):
  reported with the `*StoredEvent` for out-of-band recovery, then skipped.
- **Unsubscribed types**: skipped silently — streams may carry many types.
- `Follow` itself returns only on context cancellation or invalid
  configuration.

## Current limitations (roadmap)

- **Competing consumers**: one durable subscription ID should be followed by
  one process at a time; there is no lease/lock yet. Running two followers
  on the same ID delivers duplicates (the dedup window absorbs some) and
  interleaves offset saves. Planned: offset leases and key-based
  partitioning for ordered scale-out.
- **Backends**: sqlite works cross-process on one machine (WAL) and
  durable-streams over the network. A Postgres store (table +
  LISTEN/NOTIFY tail) is the natural next backend; the `EventStore` +
  `EventStoreTailer` interfaces are all it needs to implement.
