# Indexed Projections

Design and first version of queryable, indexed projections that persist next to
an event log. The first version ships in `stores/sqlite` as `Projection`; the
follow-ups listed at the end are not implemented yet.

## Problem

`state.Materializer` keeps every collection in a `state.Store[T]`. The bundled
store is in memory, and `SaveSnapshotTo` serializes *all* entities into one
blob. That works for small, read-mostly state, but not for mutable-row
workloads:

- **Queues** (tasks with claims and due times) need "the next due, unclaimed
  task" — an indexed range query, not a scan of a map.
- **Aggregates** (per-page view counters, rate windows) apply
  non-idempotent updates (`views = views + 1`); re-applying an event after a
  crash corrupts them.
- **Large state** should not live in memory, and compaction should not rewrite
  every payload into a fresh snapshot blob.

## Model

A projection is a set of **caller-owned SQL tables** plus **one checkpoint
row**, both in the same SQLite database:

```
events (the log, optional)   ebu_projections (name, version, checkpoint)   your tables + indexes
```

- **Apply in the checkpoint's transaction.** `CatchUp` reads a batch from the
  source log, runs the caller's `Apply` for each event inside one transaction,
  and advances the checkpoint in that same transaction. The tables therefore
  always reflect exactly the events up to the checkpoint: nothing is lost or
  applied twice across crashes, so non-idempotent updates are safe.
- **The tables are the snapshot.** Reopening resumes from the stored
  checkpoint and applies only the tail. Nothing is held in memory, and no
  payload is ever rewritten for compaction.
- **Compaction is retention.** Once the projection has applied the log through
  its committed checkpoint, those events can be trimmed with
  `RetentionPolicy{Through: committed}`, where `committed, err :=
  p.Committed(ctx)` (bounded by any other reader's checkpoint). The projection
  does not need them again.
- **Queries are plain SQL** through `p.DB()`, with whatever indexes `Setup`
  created. A committed read always observes the tables at some committed
  checkpoint.

The source may be the same `SQLiteStore` (log and projection share one file,
so one backup is a consistent log + projection snapshot, and WAL commit order
keeps them consistent even with `SynchronousNormal`), or any other
`EventStore` — a Durable Streams log projected into a local SQLite file, for
example.

## API (stores/sqlite)

```go
p, err := store.OpenProjection(ctx, sqlite.ProjectionConfig{
    Name:    "tasks",      // checkpoint identity in this database
    Version: 1,            // raise to rebuild (Reset + replay from OffsetOldest)
    Source:  store,        // any eventbus.EventStore
    Setup: func(ctx context.Context, tx *sql.Tx) error { // idempotent DDL
        _, err := tx.ExecContext(ctx, `
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY, state TEXT NOT NULL, due_at INTEGER NOT NULL,
                owner TEXT, claimed_until INTEGER);
            CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(state, due_at)`)
        return err
    },
    Reset: func(ctx context.Context, tx *sql.Tx) error { // before a rebuild
        _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS tasks`)
        return err
    },
    Apply: func(ctx context.Context, tx *sql.Tx, e *eventbus.StoredEvent) error {
        // decode e.Data by e.Type and write through tx only
        return nil
    },
    BatchSize: 500, // events per transaction (default 500)
})

checkpoint, err := p.CatchUp(ctx) // apply everything after the checkpoint
rows, err := p.DB().QueryContext(ctx,
    `SELECT id FROM tasks WHERE state = 'ready' AND due_at <= ? ORDER BY due_at LIMIT 10`, now)
```

| Call | Contract |
|------|----------|
| `OpenProjection` | Creates the checkpoint table on first use, registers or resumes `Name`, runs `Reset` when `Version` increased, then `Setup` — one transaction. A lower `Version`, or a rebuild over a source that reports a trimmed prefix, is refused with `ErrProjectionRebuild` and changes nothing. Read-only stores return `ErrReadOnly`. |
| `CatchUp` | Applies batches until a read returns no events; returns the committed checkpoint. A source error (including `ErrOffsetTrimmed`) or an `Apply` error stops at the last committed batch; the failed batch is rolled back and retried by the next call. |
| `Checkpoint` | The committed source offset this instance last observed (in memory; can lead the stored one after another instance's rebuild). |
| `Committed` | The checkpoint stored in the database, whichever instance committed it; `ErrProjectionVersionChanged` after a rebuild at another version. The retention bound. |
| `DB` | The database holding the tables, for queries. |

### Read-your-writes

`CatchUp` reads until the source returns nothing, so calling it after `Append`
returns makes the appended event visible to queries:

```go
if _, err := store.Append(ctx, event); err != nil { ... }
if _, err := p.CatchUp(ctx); err != nil { ... }
// queries now include the event
```

Run `CatchUp` on a timer (or after local appends) to keep the projection fresh
for other readers.

### Several instances and processes

Any number of `Projection` values — in one or several processes — may serve
the same `Name` on one database. Every batch re-reads the stored checkpoint in
its transaction; when another instance already applied that range, the batch
is discarded and the instance adopts the newer checkpoint. If another instance
reopened the projection with a different `Version`, `CatchUp` fails with
`ErrProjectionVersionChanged` instead of applying stale logic.

### Concurrent commands

The log decides. When two writers race (two consumers claiming the same task),
both events are appended; `Apply` resolves them deterministically, for example
"the first claim of a ready task wins":

```sql
UPDATE tasks SET state = 'claimed', owner = ?, claimed_until = ?
WHERE id = ? AND state = 'ready'
```

Each consumer calls `CatchUp` and checks whether it holds the claim. Claim
expiry should compare against event timestamps (`e.Timestamp`), not the wall
clock, so every replica derives the same state.

### Counters

```go
Apply: func(ctx context.Context, tx *sql.Tx, e *eventbus.StoredEvent) error {
    var v struct{ Page string }
    if err := json.Unmarshal(e.Data, &v); err != nil {
        return err
    }
    _, err := tx.ExecContext(ctx, `
        INSERT INTO page_views (page, views, last_seen) VALUES (?, 1, ?)
        ON CONFLICT(page) DO UPDATE SET views = views + 1, last_seen = excluded.last_seen`,
        v.Page, e.Timestamp)
    return err
}
```

### State Protocol messages

A projection can consume `state.ChangeMessage` events directly: decode
`e.Data` when `e.Type == "state.ChangeMessage"` and upsert or delete the row
for `(msg.Type, msg.Key)`, mapping the fields you query into indexed columns.

## Guarantees and assumptions

- **Exactly once** per event, provided the source's `Read` `nextOffset` does
  not redeliver events (true for MemoryStore, SQLite and Durable Streams, which
  return whole chunks with the chunk's exact next offset). For other sources,
  make `Apply` idempotent, for example by recording applied event IDs.
- **Cross-file sources.** When the source is another store or file, reading
  a batch and committing it are not one transaction. That is safe because the
  checkpoint commits with the tables and the source's `nextOffset` does not
  redeliver; only the projection's own database must be consistent. A
  separate SQLite source should run with `WithSynchronous(SynchronousFull)`:
  with the default `SynchronousNormal`, a power loss can roll back the source's
  newest events after the projection committed them, and the source then
  reissues those positions to new events, which the projection skips as
  already applied. (A source in the projection's own file needs no such care:
  WAL commit order rolls both back together.)
- **Skipped records are invisible.** A Durable Streams source skips
  undecodable records by default; open it with `WithStrictDecoding()` so a
  corrupt record stops the projection instead.
- **`Apply` must only use `tx`.** Side effects outside the transaction are not
  rolled back with a failed batch.
- **Durability** follows the database's `WithSynchronous` mode; use
  `SynchronousFull` when an applied batch must survive power loss.
- **Rebuilds need the whole log.** Raising `Version` replays from
  `OffsetOldest`. When the source reports a trimmed prefix (SQLite and
  MemoryStore `Trim`), the rebuild is refused with `ErrProjectionRebuild`
  before `Reset` runs; migrate the tables in `Setup` and keep the version
  instead. The guard checks the source once, before `Reset`, and does not hold
  retention off: do not trim the source while a rebuild is replaying. A trim
  after the rebuild's first batch makes its next read fail with
  `ErrOffsetTrimmed`, but one between the check and the first read (an age or
  count rule, or a `Through` bound taken before the rebuild) leaves the rebuilt
  tables silently incomplete. The guard only sees prefixes removed by `Trim`'s
  prefix rules. It
  cannot see a prefix removed with the legacy `TruncateBefore`, events removed
  by `MaxPerPartition`, Durable Streams server-side retention, or the history
  of a custom store that does not implement `EventStoreTrimmer`. Raising
  `Version` over such a source rebuilds from whatever it still holds, without
  an error, so the rebuilt tables can be incomplete. Only raise `Version`
  when the source still holds the complete history.
- **No downgrades.** A `Version` lower than the stored one is refused, so
  rolling back to an older binary never discards a newer projection; roll the
  projection forward, or reset it deliberately.
- **Batches hold the write lock.** A batch runs inside one write transaction,
  so a slow `Apply` or a large `BatchSize` delays appends to the same file.
  Keep `Apply` free of external I/O, and lower `BatchSize` when `Apply` is
  heavy (larger batches amortize commits; smaller ones release the lock
  sooner). A panicking `Apply` rolls its batch back and releases the lock.
- **Trim through `Committed`, not `Checkpoint`.** `Checkpoint` is the value
  this instance last observed. It lags while other instances advance the
  projection, but after another instance rebuilds it (raises `Version`) it
  leads the stored checkpoint, and trimming through it would remove history
  the rebuild still needs. `Committed` reads the stored checkpoint and fails
  with `ErrProjectionVersionChanged` on a stale instance; during a rebuild it
  reports `OffsetOldest`.
- **Stale instances stop, even when idle.** `CatchUp` confirms the stored
  version and checkpoint before reporting that it caught up, so an instance
  whose projection was rebuilt by another instance returns
  `ErrProjectionVersionChanged` instead of claiming success, and one whose
  projection another instance advanced adopts the stored checkpoint.
- **Name your tables freely, except `events`, `subscription_positions`,
  `snapshots`, `schema_version` and the `ebu_` prefix**, which belong to the
  store.
- **Gaps are reported.** If the source is trimmed past the checkpoint, `CatchUp`
  returns `ErrOffsetTrimmed` and does not move.

## Follow-ups (not implemented)

1. **`state.Store[T]` on SQLite rows.** A store that keeps each entity as a row
   (JSON value plus declared indexed columns or JSON-path expression indexes)
   would let existing `TypedCollection`s persist without blob snapshots. It
   needs a transaction-scoped binding, because `Store[T]` methods take no
   transaction and `Materializer.Apply` has no hook to run inside the
   checkpoint transaction.
2. **Background runner.** A `Run(ctx)` that tails sources implementing
   `EventStoreTailer` and wakes on local appends, instead of caller-driven
   `CatchUp`.
3. **Read-only trim boundary.** The rebuild guard asks non-SQLite sources
   through `Trim` with a zero policy, which by contract removes nothing but is
   formally a write. A read-only accessor on `EventStoreTrimmer` stores would
   make that cleaner.
4. **Idempotent mode** for sources whose `nextOffset` can redeliver: record
   applied event IDs in the batch transaction.
5. **Read-only query access.** Processes that open the database with
   `OpenReadOnly` cannot open a projection and have no handle for querying its
   tables; a query-only accessor would cover them.
6. **Other SQL backends** (for example PostgreSQL) with the same contract: a
   transaction type, a checkpoint row, and batch-plus-checkpoint commits.
