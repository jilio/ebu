package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	eventbus "github.com/jilio/ebu"
)

// The ebu_retention table records how far Trim has removed a prefix of the
// log. It is created by the first Trim rather than by a schema migration, so
// databases that never trim keep schema version 4 unchanged and remain
// readable by earlier releases (including their OpenReadOnly). A missing
// table means nothing was trimmed. The ebu_ prefix keeps it apart from tables
// that callers create in the same database (see Projection).
const (
	createRetentionTable = `
		CREATE TABLE IF NOT EXISTS ebu_retention (
			id              INTEGER PRIMARY KEY CHECK (id = 1),
			trimmed_through INTEGER NOT NULL,
			updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`

	retentionTableExists = `SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'ebu_retention')`

	selectTrimmedThrough = `SELECT trimmed_through FROM ebu_retention WHERE id = 1`

	upsertTrimmedThrough = `
		INSERT INTO ebu_retention (id, trimmed_through, updated_at) VALUES (1, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			trimmed_through = MAX(trimmed_through, excluded.trimmed_through),
			updated_at = CURRENT_TIMESTAMP`

	// deleteBeyondPartitionCap keeps the newest ?2 events of every partition,
	// where an event's partition is the value of metadata key ?1 ('' when the
	// key is absent). json_each matches the key exactly, so keys need no
	// JSON-path quoting. Metadata that is not valid JSON (never written by
	// this store) counts as having no key instead of failing the Trim.
	deleteBeyondPartitionCap = `
		DELETE FROM events WHERE position IN (
			SELECT position FROM (
				SELECT position, ROW_NUMBER() OVER (PARTITION BY part ORDER BY position DESC) AS rank
				FROM (
					SELECT e.position,
						COALESCE((SELECT value FROM json_each(CASE WHEN json_valid(e.metadata) THEN e.metadata END) WHERE key = ?1), '') AS part
					FROM events e
				)
			)
			WHERE rank > ?2
		)`
)

var _ eventbus.EventStoreTrimmer = (*SQLiteStore)(nil)

// Trim implements eventbus.EventStoreTrimmer in one write transaction.
//
// Positions are AUTOINCREMENT keys, so retained offsets never change and
// removed offsets are never reused. The prefix rules (Through, OlderThan,
// MaxEvents) record the newest position they removed; Read and ReadStream
// from an older concrete offset then fail with *eventbus.OffsetTrimmedError.
// MaxPerPartition partitions by an event metadata key with key-based
// compaction semantics: used alone it can remove the oldest events without
// moving the boundary, so readers are not told. It scans the retained log
// once per call; run it periodically rather than per append.
//
// The write transaction lasts for the whole call: a first OlderThan run over
// a large backlog, or a partition pass over a large log, delays concurrent
// appends for that long. Removed pages are reused by later appends (the
// database file does not shrink). Unlike TruncateBefore, Trim leaves
// write-ahead-log checkpoints to SQLite's automatic checkpointing: a TRUNCATE
// checkpoint waits for every open reader, which a periodic retention job
// should not do.
func (s *SQLiteStore) Trim(ctx context.Context, policy eventbus.RetentionPolicy) (result eventbus.TrimResult, err error) {
	if s.cfg.readOnly {
		return eventbus.TrimResult{}, ErrReadOnly
	}
	if err := policy.Validate(); err != nil {
		return eventbus.TrimResult{}, err
	}
	if policy == (eventbus.RetentionPolicy{}) {
		// A query: report the boundary without taking the write lock or
		// creating the retention table.
		watermark, err := s.trimmedThrough(ctx)
		if err != nil {
			return eventbus.TrimResult{}, err
		}
		result.TrimmedThrough = eventbus.OffsetOldest
		if watermark > 0 {
			result.TrimmedThrough = formatOffset(watermark)
		}
		return result, nil
	}
	var through int64
	if policy.Through != eventbus.OffsetOldest {
		if through, err = parseOffset(policy.Through); err != nil {
			return eventbus.TrimResult{}, fmt.Errorf("sqlite: invalid trim offset: %w", err)
		}
	}

	var deleted, watermark int64
	err = s.inTx(ctx, "sqlite: trim", func(tx *sql.Tx) (txErr error) {
		deleted, watermark, txErr = trimInTx(ctx, tx, policy, through)
		return txErr
	})
	if err != nil {
		return eventbus.TrimResult{}, err
	}
	if s.logger != nil {
		s.logger.Debug("trimmed events", "deleted", deleted, "trimmed_through", watermark)
	}
	result = eventbus.TrimResult{Deleted: deleted, TrimmedThrough: eventbus.OffsetOldest}
	if watermark > 0 {
		result.TrimmedThrough = formatOffset(watermark)
	}
	return result, nil
}

// trimInTx applies policy inside tx and returns the number of events removed
// and the boundary after the call.
func trimInTx(ctx context.Context, tx *sql.Tx, policy eventbus.RetentionPolicy, through int64) (deleted, watermark int64, err error) {
	if _, err = tx.ExecContext(ctx, createRetentionTable); err != nil {
		return 0, 0, fmt.Errorf("sqlite: trim: create retention table: %w", err)
	}
	if watermark, err = optionalInt64(ctx, tx, selectTrimmedThrough); err != nil {
		return 0, 0, fmt.Errorf("sqlite: trim: read boundary: %w", err)
	}

	cut := through
	if !policy.OlderThan.IsZero() {
		old, err := firstNotOlder(ctx, tx, policy.OlderThan)
		if err != nil {
			return 0, 0, err
		}
		cut = max(cut, old-1)
	}
	if policy.MaxEvents > 0 {
		position, err := optionalInt64(ctx, tx, "SELECT position FROM events ORDER BY position DESC LIMIT 1 OFFSET ?", policy.MaxEvents)
		if err != nil {
			return 0, 0, fmt.Errorf("sqlite: trim: find count boundary: %w", err)
		}
		cut = max(cut, position)
	}

	if cut > 0 {
		// The boundary is the newest event actually removed, never a
		// position beyond the tail that later appends would reuse.
		var newest sql.NullInt64
		if err := tx.QueryRowContext(ctx, "SELECT MAX(position) FROM events WHERE position <= ?", cut).Scan(&newest); err != nil {
			return 0, 0, fmt.Errorf("sqlite: trim: find prefix boundary: %w", err)
		}
		// No event at or below cut (all were removed earlier): nothing to
		// delete and no new boundary to record.
		if newest.Valid {
			res, err := tx.ExecContext(ctx, "DELETE FROM events WHERE position <= ?", newest.Int64)
			if err != nil {
				return 0, 0, fmt.Errorf("sqlite: trim: delete prefix: %w", err)
			}
			n, _ := res.RowsAffected() // never fails for SQLite
			deleted += n
			if _, err := tx.ExecContext(ctx, upsertTrimmedThrough, newest.Int64); err != nil {
				return 0, 0, fmt.Errorf("sqlite: trim: record boundary: %w", err)
			}
			watermark = max(watermark, newest.Int64)
		}
	}

	if policy.MaxPerPartition > 0 {
		res, err := tx.ExecContext(ctx, deleteBeyondPartitionCap, policy.PartitionKey, policy.MaxPerPartition)
		if err != nil {
			return 0, 0, fmt.Errorf("sqlite: trim: apply partition cap: %w", err)
		}
		n, _ := res.RowsAffected()
		deleted += n
	}
	return deleted, watermark, nil
}

// optionalInt64 runs a single-value query and returns 0 when it has no row.
func optionalInt64(ctx context.Context, tx *sql.Tx, query string, args ...any) (int64, error) {
	var value int64
	err := tx.QueryRowContext(ctx, query, args...).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return value, err
}

// firstNotOlder returns the position of the first event, in log order, whose
// timestamp is not before cutoff, or math.MaxInt64 when every event is older.
// Timestamps are stored in Go's text form, which SQLite's date functions do
// not parse, so the comparison runs here; the scan stops at the first event
// that is not older, costing one row beyond those it trims.
func firstNotOlder(ctx context.Context, tx *sql.Tx, cutoff time.Time) (int64, error) {
	rows, err := tx.QueryContext(ctx, "SELECT position, timestamp FROM events ORDER BY position")
	if err != nil {
		return 0, fmt.Errorf("sqlite: trim: scan timestamps: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var position int64
		var timestamp storedTime
		if err := rows.Scan(&position, &timestamp); err != nil {
			return 0, fmt.Errorf("sqlite: trim: scan timestamp: %w", err)
		}
		if !timestamp.Before(cutoff) {
			return position, nil
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("sqlite: trim: scan timestamps: %w", err)
	}
	return math.MaxInt64, nil
}

// trimmedThrough returns the newest position removed by a prefix retention
// rule, or 0 when nothing was trimmed (including databases without the
// retention table). The table is created in the same transaction as the first
// boundary, so its absence proves no trim committed; once seen, it is never
// looked up again.
func (s *SQLiteStore) trimmedThrough(ctx context.Context) (int64, error) {
	watermark, exists, err := lookupBoundary(ctx, s.db, s.retentionTable.Load())
	if exists {
		s.retentionTable.Store(true)
	}
	return watermark, err
}

// lookupBoundary reads the trim boundary through q, a pool or a transaction.
// Unless the retention table is known to exist it checks sqlite_master first;
// exists reports whether the table was found.
func lookupBoundary(ctx context.Context, q queryRower, knownToExist bool) (watermark int64, exists bool, err error) {
	if !knownToExist {
		if err := q.QueryRowContext(ctx, retentionTableExists).Scan(&exists); err != nil {
			return 0, false, fmt.Errorf("sqlite: look up trim boundary: %w", err)
		}
		if !exists {
			return 0, false, nil
		}
	}
	err = q.QueryRowContext(ctx, selectTrimmedThrough).Scan(&watermark)
	switch {
	case err == nil:
		return watermark, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return 0, true, nil
	default:
		return 0, true, fmt.Errorf("sqlite: read trim boundary: %w", err)
	}
}

// checkTrimmed reports whether a read from position (issued as from) skipped
// events removed by Trim. Callers check after their event query has taken its
// read snapshot: if a trim committed before that snapshot, the boundary read
// here already includes it; if it committed later, the query saw every event.
//
// Only the symbolic OffsetOldest ("start of the retained log") is exempt; a
// concrete zero checkpoint, saved before any event existed, is checked like
// any other position.
//
// first is the position of the first event the query returned (0: none).
// When it is position+1 the read cannot have crossed a prefix trim — that
// would have removed position+1 — so the common tailing read skips the
// boundary lookup.
func (s *SQLiteStore) checkTrimmed(ctx context.Context, from eventbus.Offset, position, first int64) error {
	if from == eventbus.OffsetOldest || first == position+1 {
		return nil
	}
	watermark, err := s.trimmedThrough(ctx)
	if err != nil {
		return err
	}
	if position < watermark {
		return &eventbus.OffsetTrimmedError{Offset: from, ResumeFrom: formatOffset(watermark)}
	}
	return nil
}
