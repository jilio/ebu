# Confirmed replication

`Replicator` combines asynchronous log copying with explicit prefix barriers.
Use it when ordinary writes may return after local storage, while selected writes
must wait for an independently stored copy. It uses the same ordered copy loop as
`Mirror`; existing `Mirror` callers keep their behavior.

## What a successful wait means

For a position returned by `r.Position(sourceAppendOffset)` or `r.Capture(ctx)`,
`r.Wait(ctx, position)` returns nil only after the destination has acknowledged
all source events through that boundary and the source checkpoint has been saved.
An important write therefore protects its preceding ordinary writes too. Events
appended later do not move an already captured boundary.

This is a **destination-acknowledgement guarantee**. To survive source-worker loss,
the destination must be outside that worker's failure domain. Its successful
`Append` must mean the storage durability you require: ebu cannot turn an
in-memory store, buffered write, or HTTP server that acknowledges before durable
storage into a durable replica. `MemoryStore` examples demonstrate the API only.
SQLite uses WAL with `synchronous=NORMAL`; this is not a promise that the latest
commit survives power loss on the destination. A Durable Streams acknowledgement
inherits the server/backend's configured guarantees. Do not silently downgrade
an important operation when that destination is unavailable.

A wait does not mean a destination projection has applied the events. It also
does not acquire a lease, fence an old writer, elect a replacement worker, or
replicate snapshots. Those responsibilities belong to the application/storage
system. Source and destination offsets are different token spaces; every public
replication position contains a **source** offset.

## Start one replication relationship

```go
local, err := sqlite.New("documents.db")
if err != nil { return err }
remote, err := durablestream.New("https://streams.example/v1/stream", "documents-g7")
if err != nil { return err }

r, err := eventbus.NewReplicator(eventbus.ReplicatorConfig{
    Source:      local,
    Destination: remote,
    Checkpoints: local,
    ID:          "documents-to-backup",
    Generation:  "source-g7-destination-g3",
}, eventbus.MirrorOnError(func(err error) { log.Print(err) }))
if err != nil { return err }

runCtx, stopReplication := context.WithCancel(context.Background())
done := make(chan error, 1)
go func() { done <- r.Run(runCtx) }()
// Keep r, stopReplication and done in the owning service.
```

`NewReplicator` performs no I/O and starts no goroutine. The source must implement
`EventStoreOffsetComparer`. The bundled memory, SQLite and Durable Streams stores
do. The checkpoint store must accept the source's tokens verbatim: SQLite source
checkpoints can live in SQLite; opaque remote tokens cannot necessarily do so.
The tuple `(ID, Generation)` gets its own checkpoint namespace, separate from
ordinary `Mirror` IDs. Constructing an object does not import an older plain
Mirror checkpoint: a new relationship copies from `OffsetOldest`.

Run exactly one owner of a relationship across processes. `Run` is single-use on
one object; concurrent or repeated calls return `ErrReplicatorStarted`. After a
process restart or a completed Run, construct another object with the same
configuration to resume. `MirrorPollInterval`, `MirrorDedupWindow`,
`MirrorOnForward` and `MirrorOnError` also configure a Replicator. Observers execute
on the copying goroutine; never wait for that replicator from an observer.

Replicator uses batched `Read`, including for sources with `Tail`: Read exposes
cursor-only advances across empty chunks, which Tail's event iterator cannot
report. `MirrorPollInterval` sets the idle polling period. A waiting operation
wakes an idle copier immediately; it does not bypass retry backoff after errors.
Plain Mirror continues to prefer Tail when available.

## Mix ordinary and important writes

Both modes use the same local append. Important operations add a wait:

```go
func appendWithReplication(
    ctx context.Context,
    local eventbus.EventStore,
    r *eventbus.Replicator,
    event *eventbus.Event,
    requireReplica bool,
) (eventbus.ReplicationPosition, error) {
    offset, err := local.Append(ctx, event)
    if err != nil {
        return eventbus.ReplicationPosition{}, err
    }
    position, err := r.Position(offset)
    if err != nil {
        return eventbus.ReplicationPosition{}, err
    }
    if requireReplica {
        return position, r.Wait(ctx, position)
    }
    return position, nil
}
```

The application chooses its default and can override it per operation without
restarting the copier. Assign an event ID unique to that logical write before attempting the local write
and make retries of the write idempotent. If the append succeeded but the wait
timed out, keep the returned position and retry **the wait**, not the write.
An append error can also be ambiguous; that case requires the source store's
idempotency/reconciliation mechanism.

`Position` checks the offset using the source comparer; it cannot prove that an
arbitrary token was actually issued by this log. Concrete boundaries must name
stable prefixes: later appends cannot reuse a tail token with a larger meaning. Pass only a source Append result
whose boundary covers that append, or use `Capture`. Do not use the offset on an
arbitrary read event as evidence that the event was copied: a non-last member of
a chunk can carry the chunk's start token. The copier confirms the end only after
the complete chunk is acknowledged.

To confirm a group of successful writes, including publishes through EventBus:

```go
// Check every local write's result before creating the barrier.
if err := eventbus.TryPublishContext(bus, ctx, DocumentSaved{ID: "doc-1"}); err != nil {
    return err
}
position, err := r.Capture(ctx)
if err != nil { return err }
waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
if err := r.Wait(waitCtx, position); err != nil { return err }
```

The bus's store must be this replicator's source. `Capture` resolves the current
tail once and performs no waiting. Concurrent writes can be included up to that
resolution, but writes after it cannot extend the barrier. To switch an
application default from asynchronous to confirmed, establish the required
boundary, wait for it, and use the confirmed write path for subsequent requests.
Coordinate that policy transition with producers in the application. Switching
cannot recover already lost data.

## Timeouts, failures and progress

| Situation | Behavior |
|---|---|
| Wait canceled or timed out | That waiter returns its context error. Copying and other waiters continue; writes are not rolled back. |
| Destination append fails, including a lost response after commit | Retry that event in place. Do not confirm it on the failed attempt. Duplicate IDs can reach the destination. |
| First checkpoint save fails | Run fails and wakes unfulfilled waiters; check token compatibility/configuration. |
| Later checkpoint save fails | Retry that save in place. Do not copy the next event or advance confirmed progress. |
| Source read/Tail fails | Report and retry without acknowledging a gap. Permanent errors can require operator recovery. |
| Run stops before the requested boundary | Wait returns an error matching `ErrReplicationStopped` and the Run error via `errors.Is`. |
| Requested boundary was already confirmed | Wait can still succeed after Run stops, subject to the same storage/retention contract. |
| Position belongs to another ID or generation | Fail immediately with `ErrReplicationPosition`. |

`r.Confirmed()` returns the latest confirmed source position and a boolean.
The boolean is false until startup has established progress. The empty prefix is
valid. At startup a checkpoint from the same relationship/generation is trusted:
it certifies earlier destination acknowledgements. Later progress is announced
only after another successful checkpoint save. Reusing a checkpoint with a
replacement or truncated destination violates that assumption.

Positions are JSON-serializable (`id`, `generation`, `offset`). Many callers may
wait on different positions concurrently, including before Run starts. A wait
never starts Run for you. Bound waits with a context and observe the Run result.

## Generations, retention and recovery

An ID identifies one source/destination relationship. A generation identifies its
immutable log incarnations. Change the generation after replacing, rebuilding or
restoring **either** log, even when its offsets happen to look the same. Old
positions are rejected and the new checkpoint namespace starts from the beginning.
Do not hot-swap either store behind a running object. Generation values are
application-owned; a new random value on every ordinary restart would needlessly
recopy the entire history.

A saved checkpoint ahead of the source tail fails startup with
`ErrReplicationHistory`. `MirrorResetOnSourceRewind` is rejected for Replicator:
automatically resetting an acknowledged generation would make old promises
misleading. Offsets cannot detect a replaced log that has already refilled past
its old tail; assigning a new generation is mandatory in that case too.

Keep every unreplicated source event. A fresh relationship requires the entire
source history. Do not compact/truncate it underneath copying. Keep acknowledged
destination history, or an application-managed snapshot that preserves the same
recovery guarantee. Replicator does not establish or validate snapshot coverage.
Sources must return errors for missing required history and must not filter or
silently skip records. For a Durable Streams **source**, use
`durablestream.WithStrictDecoding()` so malformed envelopes stop Read/Tail rather
than disappear from the replicated prefix. That option is unnecessary merely
because Durable Streams is the destination.

Delivery is at-least-once. A crash after destination append but before checkpoint
save causes redelivery, possibly well outside a bounded in-memory dedup window.
Recover with durable event-ID deduplication or version-aware reducers that reject
stale replay. Reapplying an old assignment after a newer one can regress state;
a plain overwrite is not enough to handle every duplicate ordering. Only expose
a recovered projection when its required history has been applied.

For planned handoff: stop/join producers, capture and confirm the final boundary,
cancel and join Run, then close its stores (or shut down their owning bus).
For unexpected worker loss: recover from the independent destination, accepting
that the asynchronous suffix may be absent. Granting the new worker exclusive
write authority and preventing the old worker from resuming are application
responsibilities. This API does not promise zero-downtime failover.

## Verification

The core tests inject blocked and ambiguous appends, failed checkpoint saves,
missing-history errors, cancellation, chunk boundaries, restart and generation
changes. The executable Go example is in `replication_example_test.go`.

From this repository's Go workspace, run the process-loss smoke test:

```sh
go run ./scripts/replication-smoke.go
```

It mirrors a child process's SQLite log over HTTP into a separate SQLite store,
waits for 201 revisions, kills the source process, removes its files, and checks
the full confirmed prefix using only the reopened destination. This exercises
source-process loss, not simultaneous destination power loss or worker fencing.
