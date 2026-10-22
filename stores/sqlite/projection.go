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

// defaultProjectionBatchSize is the number of events CatchUp applies per
// transaction when ProjectionConfig.BatchSize is zero.
const defaultProjectionBatchSize = 500

// The projections table holds one checkpoint row per projection. Like the
// retention table it is created on first use, so stores without projections
// keep their schema unchanged.
const createProjectionsTable = `
	CREATE TABLE IF NOT EXISTS projections (
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
	// it differs from the version stored for Name, OpenProjection calls Reset
	// and the projection is rebuilt from OffsetOldest. A rebuild needs the
	// complete source log; after the source was trimmed, migrate the tables
	// in Setup and keep Version instead.
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
	// not have effects outside tx. An error rolls back the whole batch and is
	// returned by CatchUp; the next CatchUp retries from the same checkpoint.
	// Required.
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
// RetentionPolicy{Through: p.Checkpoint()} (or keep it as a bound for other
// compaction) to drop events the projection no longer needs.
type Projection struct {
	store *SQLiteStore
	cfg   ProjectionConfig

	// mu serializes CatchUp on this instance.
	mu sync.Mutex
	// checkpoint is the last committed checkpoint this instance observed.
	checkpoint atomic.Value // eventbus.Offset
}

// OpenProjection registers (or resumes) the projection cfg.Name in this
// store's database: it creates the checkpoint table on first use, rebuilds
// when cfg.Version changed (Reset, then the checkpoint returns to
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlite: projection %q: begin: %w", cfg.Name, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, createProjectionsTable); err != nil {
		return nil, fmt.Errorf("sqlite: projection %q: create checkpoint table: %w", cfg.Name, err)
	}

	var version int
	var checkpoint string
	err = tx.QueryRowContext(ctx, "SELECT version, checkpoint FROM projections WHERE name = ?", cfg.Name).Scan(&version, &checkpoint)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err = tx.ExecContext(ctx, "INSERT INTO projections (name, version, checkpoint) VALUES (?, ?, '')", cfg.Name, cfg.Version); err != nil {
			return nil, fmt.Errorf("sqlite: projection %q: register: %w", cfg.Name, err)
		}
	case err != nil:
		return nil, fmt.Errorf("sqlite: projection %q: load checkpoint: %w", cfg.Name, err)
	case version != cfg.Version:
		if cfg.Reset == nil {
			return nil, fmt.Errorf("sqlite: projection %q is stored at version %d; rebuilding it at version %d requires Reset", cfg.Name, version, cfg.Version)
		}
		if err = cfg.Reset(ctx, tx); err != nil {
			return nil, fmt.Errorf("sqlite: projection %q: reset: %w", cfg.Name, err)
		}
		if _, err = tx.ExecContext(ctx, "UPDATE projections SET version = ?, checkpoint = '', updated_at = CURRENT_TIMESTAMP WHERE name = ?", cfg.Version, cfg.Name); err != nil {
			return nil, fmt.Errorf("sqlite: projection %q: reset checkpoint: %w", cfg.Name, err)
		}
		checkpoint = ""
	}
	if cfg.Setup != nil {
		if err = cfg.Setup(ctx, tx); err != nil {
			return nil, fmt.Errorf("sqlite: projection %q: setup: %w", cfg.Name, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlite: projection %q: commit: %w", cfg.Name, err)
	}

	p = &Projection{store: s, cfg: cfg}
	p.checkpoint.Store(eventbus.Offset(checkpoint))
	return p, nil
}

// Name returns the projection's name.
func (p *Projection) Name() string { return p.cfg.Name }

// DB returns the database holding the projection's tables, for queries.
// Committed reads always observe the tables at some committed checkpoint.
// Do not modify ebu's own tables through it.
func (p *Projection) DB() *sql.DB { return p.store.db }

// Checkpoint returns the source offset through which the projection's tables
// are committed, as last observed by this instance (OffsetOldest before the
// first event).
func (p *Projection) Checkpoint() eventbus.Offset {
	return p.checkpoint.Load().(eventbus.Offset)
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
		events, next, err := p.cfg.Source.Read(ctx, from, p.cfg.BatchSize)
		if err != nil {
			return from, fmt.Errorf("sqlite: projection %q: read after %q: %w", p.cfg.Name, from, err)
		}
		if len(events) == 0 && next == from {
			return from, nil
		}
		if err := p.applyBatch(ctx, from, events, next); err != nil {
			return p.Checkpoint(), err
		}
	}
}

// applyBatch applies events and moves the checkpoint from from to next in
// one transaction. When another instance already moved the stored
// checkpoint, nothing is applied and this instance adopts the stored one.
func (p *Projection) applyBatch(ctx context.Context, from eventbus.Offset, events []*eventbus.StoredEvent, next eventbus.Offset) (err error) {
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: projection %q: begin: %w", p.cfg.Name, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var version int
	var stored string
	if err = tx.QueryRowContext(ctx, "SELECT version, checkpoint FROM projections WHERE name = ?", p.cfg.Name).Scan(&version, &stored); err != nil {
		return fmt.Errorf("sqlite: projection %q: load checkpoint: %w", p.cfg.Name, err)
	}
	if version != p.cfg.Version {
		err = fmt.Errorf("sqlite: projection %q is now at version %d, this instance applies version %d: %w", p.cfg.Name, version, p.cfg.Version, ErrProjectionVersionChanged)
		return err
	}
	if eventbus.Offset(stored) != from {
		// Another instance applied this range first: adopt its checkpoint.
		_ = tx.Rollback()
		p.checkpoint.Store(eventbus.Offset(stored))
		return nil
	}

	for _, event := range events {
		if err = p.cfg.Apply(ctx, tx, event); err != nil {
			return fmt.Errorf("sqlite: projection %q: apply event %q at %q: %w", p.cfg.Name, event.ID, event.Offset, err)
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE projections SET checkpoint = ?, updated_at = CURRENT_TIMESTAMP WHERE name = ?", string(next), p.cfg.Name); err != nil {
		return fmt.Errorf("sqlite: projection %q: save checkpoint: %w", p.cfg.Name, err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: projection %q: commit: %w", p.cfg.Name, err)
	}
	p.checkpoint.Store(next)
	return nil
}
