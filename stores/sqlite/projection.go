package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	eventbus "github.com/jilio/ebu"
)

// ErrProjectionVersionChanged reports that another Projection reopened the
// same projection with a different Version (and rebuilt it) while this one
// was running. This instance's Apply logic is stale; it stops applying.
var ErrProjectionVersionChanged = errors.New("sqlite: projection version changed by another instance")

// ErrProjectionRebuild reports that OpenProjection refused to rebuild a
// projection: Version is lower than the stored one (an older binary must not
// discard a newer projection), or the source reports a trimmed prefix, so
// replaying from OffsetOldest could not restore the tables. Nothing was
// changed.
var ErrProjectionRebuild = errors.New("sqlite: projection cannot be rebuilt")

// defaultProjectionBatchSize is the number of events CatchUp applies per
// transaction when ProjectionConfig.BatchSize is zero.
const defaultProjectionBatchSize = 500

// The ebu_projections table holds one checkpoint row per projection. Like
// ebu_retention it is created on first use, so stores without projections
// keep their schema unchanged; the ebu_ prefix keeps it apart from the
// projections' own tables.
const createProjectionsTable = `
	CREATE TABLE IF NOT EXISTS ebu_projections (
		name       TEXT PRIMARY KEY,
		version    INTEGER NOT NULL,
		checkpoint TEXT NOT NULL,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`

// ProjectionConfig describes a Projection.
type ProjectionConfig struct {
	// Name identifies the projection's checkpoint in the database. Required.
	Name string

	// Version is the version of the projection's tables and Apply logic. When
	// it is greater than the version stored for Name, OpenProjection calls
	// Reset and the projection is rebuilt from OffsetOldest. A rebuild needs
	// the complete source log: it is refused (ErrProjectionRebuild) when the
	// source reports a trimmed prefix — migrate the tables in Setup and keep
	// Version instead. A Version lower than the stored one is refused too, so
	// rolling back to an older binary never discards a newer projection.
	Version int

	// Source is the log to project. It may be the SQLiteStore that holds the
	// projection (log and projection then share one file and one consistent
	// backup) or any other eventbus.EventStore. Required.
	Source eventbus.EventStore

	// Setup creates the projection's tables and indexes. It runs on every
	// OpenProjection, inside the transaction that registers the checkpoint, so
	// it must be idempotent (CREATE TABLE IF NOT EXISTS, ...). Optional.
	Setup func(ctx context.Context, tx *sql.Tx) error

	// Reset discards the projection's tables or rows before a rebuild; Setup
	// runs right after it in the same transaction. Required when Version
	// changes for an existing projection.
	Reset func(ctx context.Context, tx *sql.Tx) error

	// Apply applies one event to the projection's tables using tx only. It
	// runs inside the transaction that also advances the checkpoint, so its
	// writes commit exactly when the checkpoint moves past the event. It must
	// not have effects outside tx, and should stay fast: the batch holds the
	// database's write lock, delaying appends to the same file. An error (or
	// panic) rolls back the whole batch; CatchUp returns the error and the
	// next CatchUp retries from the same checkpoint. Required.
	Apply func(ctx context.Context, tx *sql.Tx, event *eventbus.StoredEvent) error

	// BatchSize is the number of events read and applied per transaction.
	// Zero selects 500; negative values are rejected. Sources whose smallest
	// resumable unit is larger (a Durable Streams chunk) return whole units.
	BatchSize int
}

// Projection maintains caller-owned SQLite tables — queryable with real
// indexes — from an event log. Each batch of events is applied, and the
// projection's checkpoint advanced, in one SQLite transaction, so the tables
// always reflect exactly the events up to the checkpoint: no event is lost
// or applied twice across crashes or restarts, which makes non-idempotent
// updates such as counters safe. The tables and the checkpoint persist
// together; reopening resumes from the checkpoint (the tables are the
// snapshot; only the tail is applied), and a projection never keeps its
// state in memory.
//
// Exactly-once application assumes the source's Read nextOffset does not
// redeliver events (true for the bundled stores). Several Projection
// instances, in one or several processes, may serve the same Name: every
// batch re-checks the stored checkpoint inside its transaction and is
// discarded when another instance got there first.
//
// Query the tables through DB. Compact the source with
// RetentionPolicy{Through: committed}, where committed comes from Committed,
// to drop events the projection no longer needs (bounded by the source's
// other readers), but not while a rebuild is replaying.
type Projection struct {
	store *SQLiteStore
	cfg   ProjectionConfig

	// mu serializes CatchUp on this instance.
	mu sync.Mutex
	// checkpoint is the last committed checkpoint this instance observed;
	// nil means OffsetOldest.
	checkpoint atomic.Pointer[eventbus.Offset]
}

// OpenProjection registers (or resumes) the projection cfg.Name in this
// store's database: it creates the checkpoint table on first use, rebuilds
// when cfg.Version increased (Reset, then the checkpoint returns to
// OffsetOldest), and runs cfg.Setup, all in one transaction. Call CatchUp to
// apply events. Read-only stores return ErrReadOnly.
func (s *SQLiteStore) OpenProjection(ctx context.Context, cfg ProjectionConfig) (p *Projection, err error) {
	if s.cfg.readOnly {
		return nil, ErrReadOnly
	}
	switch {
	case cfg.Name == "":
		return nil, errors.New("sqlite: projection name is required")
	case cfg.Source == nil:
		return nil, fmt.Errorf("sqlite: projection %q requires a Source", cfg.Name)
	case cfg.Apply == nil:
		return nil, fmt.Errorf("sqlite: projection %q requires Apply", cfg.Name)
	case cfg.BatchSize < 0:
		return nil, fmt.Errorf("sqlite: projection %q batch size must not be negative, got %d", cfg.Name, cfg.BatchSize)
	case cfg.BatchSize == 0:
		cfg.BatchSize = defaultProjectionBatchSize
	}

	// Ask a source other than a SQLite store for its trim boundary before
	// taking this database's write lock: its Trim may write, possibly to this
	// very file. The answer only matters if a rebuild turns out to be due.
	var foreignTrimmed eventbus.Offset
	var foreignErr error
	if _, ok := cfg.Source.(*SQLiteStore); !ok {
		foreignTrimmed, foreignErr = sourceTrimmedThrough(ctx, cfg.Source)
	}

	var checkpoint string
	err = s.inTx(ctx, fmt.Sprintf("sqlite: projection %q", cfg.Name), func(tx *sql.Tx) (txErr error) {
		checkpoint, txErr = openProjectionInTx(ctx, tx, cfg, foreignTrimmed, foreignErr)
		return txErr
	})
	if err != nil {
		return nil, err
	}

	p = &Projection{store: s, cfg: cfg}
	p.setCheckpoint(eventbus.Offset(checkpoint))
	return p, nil
}

// openProjectionInTx registers or resumes cfg.Name inside tx (see
// OpenProjection) and returns the checkpoint to resume from.
func openProjectionInTx(ctx context.Context, tx *sql.Tx, cfg ProjectionConfig, foreignTrimmed eventbus.Offset, foreignErr error) (string, error) {
	if _, err := tx.ExecContext(ctx, createProjectionsTable); err != nil {
		return "", fmt.Errorf("sqlite: projection %q: create checkpoint table: %w", cfg.Name, err)
	}
	version, checkpoint, err := loadProjection(ctx, tx, cfg.Name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx, "INSERT INTO ebu_projections (name, version, checkpoint) VALUES (?, ?, '')", cfg.Name, cfg.Version); err != nil {
			return "", fmt.Errorf("sqlite: projection %q: register: %w", cfg.Name, err)
		}
	case err != nil:
		return "", fmt.Errorf("sqlite: projection %q: load checkpoint: %w", cfg.Name, err)
	case version > cfg.Version:
		return "", fmt.Errorf("%w: projection %q is stored at version %d, newer than %d", ErrProjectionRebuild, cfg.Name, version, cfg.Version)
	case version < cfg.Version:
		if cfg.Reset == nil {
			return "", fmt.Errorf("sqlite: projection %q is stored at version %d; rebuilding it at version %d requires Reset", cfg.Name, version, cfg.Version)
		}
		trimmed, err := foreignTrimmed, foreignErr
		if src, ok := cfg.Source.(*SQLiteStore); ok {
			// A read on another connection; WAL readers do not wait for
			// this transaction's write lock.
			trimmed, err = sourceTrimmedThrough(ctx, src)
		}
		if err != nil {
			return "", fmt.Errorf("sqlite: projection %q: check source before rebuild: %w", cfg.Name, err)
		}
		if trimmed != eventbus.OffsetOldest {
			return "", fmt.Errorf("%w: projection %q: the source was trimmed through %q, so a rebuild from OffsetOldest would be incomplete; migrate the tables in Setup and keep Version %d",
				ErrProjectionRebuild, cfg.Name, trimmed, version)
		}
		if err := cfg.Reset(ctx, tx); err != nil {
			return "", fmt.Errorf("sqlite: projection %q: reset: %w", cfg.Name, err)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE ebu_projections SET version = ?, checkpoint = '', updated_at = CURRENT_TIMESTAMP WHERE name = ?", cfg.Version, cfg.Name); err != nil {
			return "", fmt.Errorf("sqlite: projection %q: reset checkpoint: %w", cfg.Name, err)
		}
		checkpoint = ""
	}
	if cfg.Setup != nil {
		if err := cfg.Setup(ctx, tx); err != nil {
			return "", fmt.Errorf("sqlite: projection %q: setup: %w", cfg.Name, err)
		}
	}
	return checkpoint, nil
}

// queryRower is the part of *sql.DB and *sql.Tx that loadProjection needs.
type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// loadProjection reads the stored version and checkpoint of a projection.
func loadProjection(ctx context.Context, q queryRower, name string) (version int, checkpoint string, err error) {
	err = q.QueryRowContext(ctx, "SELECT version, checkpoint FROM ebu_projections WHERE name = ?", name).Scan(&version, &checkpoint)
	return version, checkpoint, err
}

// Name returns the projection's name.
func (p *Projection) Name() string { return p.cfg.Name }

// DB returns the database holding the projection's tables, for queries.
// Committed reads always observe the tables at some committed checkpoint.
// Do not modify ebu's own tables through it.
func (p *Projection) DB() *sql.DB { return p.store.db }

// Checkpoint returns the source offset through which the projection's tables
// are committed, as last observed by this instance (OffsetOldest before the
// first event). While other instances only advance the projection it lags
// the stored checkpoint, but after another instance rebuilds the projection
// (raises Version) it leads it. Use Committed for a retention bound.
func (p *Projection) Checkpoint() eventbus.Offset {
	if checkpoint := p.checkpoint.Load(); checkpoint != nil {
		return *checkpoint
	}
	return eventbus.OffsetOldest
}

// Committed reads the checkpoint stored for the projection: the source
// offset through which its tables are committed, whichever instance committed
// them. It fails with ErrProjectionVersionChanged when another instance
// reopened the projection at a different Version; a rebuild in progress
// reports OffsetOldest. Committed is the safe retention bound for the source:
// trim through it, bounded by the checkpoints of the source's other readers,
// and do not trim while a rebuild is replaying (see docs/PROJECTIONS.md).
func (p *Projection) Committed(ctx context.Context) (eventbus.Offset, error) {
	version, stored, err := loadProjection(ctx, p.store.db, p.cfg.Name)
	if err != nil {
		return eventbus.OffsetOldest, fmt.Errorf("sqlite: projection %q: load checkpoint: %w", p.cfg.Name, err)
	}
	if version != p.cfg.Version {
		return eventbus.OffsetOldest, p.versionChanged(version)
	}
	return eventbus.Offset(stored), nil
}

func (p *Projection) versionChanged(version int) error {
	return fmt.Errorf("sqlite: projection %q is now at version %d, this instance applies version %d: %w", p.cfg.Name, version, p.cfg.Version, ErrProjectionVersionChanged)
}

func (p *Projection) setCheckpoint(offset eventbus.Offset) {
	p.checkpoint.Store(&offset)
}

// CatchUp applies every event the source holds after the checkpoint, one
// batch per transaction, until a read returns no events, and returns the
// committed checkpoint. Calling it right after an Append returned makes the
// appended event visible to queries (read-your-writes). A source error —
// including eventbus.ErrOffsetTrimmed when the source was trimmed past the
// checkpoint — or an Apply error stops it at the last committed batch.
// Concurrent calls on one instance are serialized.
func (p *Projection) CatchUp(ctx context.Context) (eventbus.Offset, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		from := p.Checkpoint()
		if err := ctx.Err(); err != nil {
			return from, fmt.Errorf("sqlite: projection %q: %w", p.cfg.Name, err)
		}
		events, next, err := p.cfg.Source.Read(ctx, from, p.cfg.BatchSize)
		if err != nil {
			return from, fmt.Errorf("sqlite: projection %q: read after %q: %w", p.cfg.Name, from, err)
		}
		// Sources return a nextOffset that never moves backwards (the
		// EventStore contract), so an empty read either ends the loop or
		// persists an advanced resume token exactly once.
		if len(events) == 0 && next == from {
			// Caught up, as far as this instance knows. Confirm against the
			// stored state: another instance may have rebuilt the
			// projection (reset tables) or advanced it.
			stored, err := p.Committed(ctx)
			if err != nil {
				return from, err
			}
			if stored == from {
				return from, nil
			}
			p.setCheckpoint(stored)
			continue
		}
		if next == from {
			// A non-conforming source: applying would commit the same
			// checkpoint and read the same events again, forever.
			return from, fmt.Errorf("sqlite: projection %q: source returned %d events after %q without advancing its offset", p.cfg.Name, len(events), from)
		}
		if err := p.applyBatch(ctx, from, events, next); err != nil {
			return p.Checkpoint(), err
		}
	}
}

// applyBatch applies events and moves the checkpoint from from to next in
// one transaction. When another instance already moved the stored
// checkpoint, nothing is applied and this instance adopts the stored one.
func (p *Projection) applyBatch(ctx context.Context, from eventbus.Offset, events []*eventbus.StoredEvent, next eventbus.Offset) error {
	adopted := eventbus.Offset("")
	adopt := false
	err := p.store.inTx(ctx, fmt.Sprintf("sqlite: projection %q", p.cfg.Name), func(tx *sql.Tx) error {
		version, stored, err := loadProjection(ctx, tx, p.cfg.Name)
		if err != nil {
			return fmt.Errorf("sqlite: projection %q: load checkpoint: %w", p.cfg.Name, err)
		}
		if version != p.cfg.Version {
			return p.versionChanged(version)
		}
		if eventbus.Offset(stored) != from {
			// Another instance applied this range first: adopt its
			// checkpoint (the transaction commits nothing).
			adopted, adopt = eventbus.Offset(stored), true
			return nil
		}
		for _, event := range events {
			if err := p.cfg.Apply(ctx, tx, event); err != nil {
				return fmt.Errorf("sqlite: projection %q: apply event %q at %q: %w", p.cfg.Name, event.ID, event.Offset, err)
			}
		}
		if _, err := tx.ExecContext(ctx, "UPDATE ebu_projections SET checkpoint = ?, updated_at = CURRENT_TIMESTAMP WHERE name = ?", string(next), p.cfg.Name); err != nil {
			return fmt.Errorf("sqlite: projection %q: save checkpoint: %w", p.cfg.Name, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if adopt {
		p.setCheckpoint(adopted)
	} else {
		p.setCheckpoint(next)
	}
	return nil
}

// sourceTrimmedThrough reports the trim boundary of a projection source:
// read-only for SQLite stores, through a zero-policy Trim (which by contract
// removes nothing) for other trimmers, and OffsetOldest for sources that
// cannot tell.
func sourceTrimmedThrough(ctx context.Context, source eventbus.EventStore) (eventbus.Offset, error) {
	switch src := source.(type) {
	case *SQLiteStore:
		position, err := src.trimmedThrough(ctx)
		if err != nil || position == 0 {
			return eventbus.OffsetOldest, err
		}
		return formatOffset(position), nil
	case eventbus.EventStoreTrimmer:
		result, err := src.Trim(ctx, eventbus.RetentionPolicy{})
		return result.TrimmedThrough, err
	default:
		return eventbus.OffsetOldest, nil
	}
}
