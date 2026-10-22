package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	eventbus "github.com/jilio/ebu"
)

// The retention table records how far Trim has removed a prefix of the log.
// It is created by the first Trim rather than by a schema migration, so
// databases that never trim keep schema version 4 unchanged and remain
// readable by earlier releases (including their OpenReadOnly). A missing
// table means nothing was trimmed.
const (
	createRetentionTable = `
		CREATE TABLE IF NOT EXISTS retention (
			id              INTEGER PRIMARY KEY CHECK (id = 1),
			trimmed_through INTEGER NOT NULL,
			updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`

	selectTrimmedThrough = `SELECT trimmed_through FROM retention WHERE id = 1`

	upsertTrimmedThrough = `
		INSERT INTO retention (id, trimmed_through, updated_at) VALUES (1, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			trimmed_through = MAX(trimmed_through, excluded.trimmed_through),
			updated_at = CURRENT_TIMESTAMP`

	// deleteBeyondPartitionCap keeps the newest ?2 events of every partition,
	// where an event's partition is the value of metadata key ?1 ('' when the
	// key is absent). json_each matches the key exactly, so keys need no
	// JSON-path quoting.
	deleteBeyondPartitionCap = `
		DELETE FROM events WHERE position IN (
			SELECT position FROM (
				SELECT position, ROW_NUMBER() OVER (PARTITION BY part ORDER BY position DESC) AS rank
				FROM (
					SELECT e.position,
						COALESCE((SELECT value FROM json_each(e.metadata) WHERE key = ?1), '') AS part
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
// MaxPerPartition partitions by an event metadata key and scans the retained
// log once per call, so run it periodically rather than per append. Removed
// pages are reused by later appends (the database file does not shrink).
// Unlike TruncateBefore, Trim leaves write-ahead-log checkpoints to SQLite's
// automatic checkpointing: a TRUNCATE checkpoint waits for every open reader,
// which a periodic retention job should not do.
func (s *SQLiteStore) Trim(ctx context.Context, policy eventbus.RetentionPolicy) (result eventbus.TrimResult, err error) {
	if s.cfg.readOnly {
		return eventbus.TrimResult{}, ErrReadOnly
	}
	if err := policy.Validate(); err != nil {
		return eventbus.TrimResult{}, err
	}
	var through int64
	if policy.Through != eventbus.OffsetOldest {
		if through, err = parseOffset(policy.Through); err != nil {
			return eventbus.TrimResult{}, fmt.Errorf("sqlite: invalid trim offset: %w", err)
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return eventbus.TrimResult{}, fmt.Errorf("sqlite: trim: begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, createRetentionTable); err != nil {
		return eventbus.TrimResult{}, fmt.Errorf("sqlite: trim: create retention table: %w", err)
	}
	var watermark int64
	if err = tx.QueryRowContext(ctx, selectTrimmedThrough).Scan(&watermark); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return eventbus.TrimResult{}, fmt.Errorf("sqlite: trim: read boundary: %w", err)
	}
	err = nil

	cut := through
	if !policy.OlderThan.IsZero() {
		old, err := firstNotOlder(ctx, tx, policy.OlderThan)
		if err != nil {
			return eventbus.TrimResult{}, err
		}
		cut = max(cut, old-1)
	}
	if policy.MaxEvents > 0 {
		var position int64
		err = tx.QueryRowContext(ctx, "SELECT position FROM events ORDER BY position DESC LIMIT 1 OFFSET ?", policy.MaxEvents).Scan(&position)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return eventbus.TrimResult{}, fmt.Errorf("sqlite: trim: find count boundary: %w", err)
		}
		err = nil
		cut = max(cut, position)
	}

	var deleted int64
	if cut > 0 {
		// The boundary is the newest event actually removed, never a
		// position beyond the tail that later appends would reuse.
		var newest sql.NullInt64
		if err = tx.QueryRowContext(ctx, "SELECT MAX(position) FROM events WHERE position <= ?", cut).Scan(&newest); err != nil {
			return eventbus.TrimResult{}, fmt.Errorf("sqlite: trim: find prefix boundary: %w", err)
		}
		if newest.Valid {
			res, err := tx.ExecContext(ctx, "DELETE FROM events WHERE position <= ?", newest.Int64)
			if err != nil {
				return eventbus.TrimResult{}, fmt.Errorf("sqlite: trim: delete prefix: %w", err)
			}
			n, _ := res.RowsAffected()
			deleted += n
			if _, err = tx.ExecContext(ctx, upsertTrimmedThrough, newest.Int64); err != nil {
				return eventbus.TrimResult{}, fmt.Errorf("sqlite: trim: record boundary: %w", err)
			}
			watermark = max(watermark, newest.Int64)
		}
	}

	if policy.MaxPerPartition > 0 {
		res, err := tx.ExecContext(ctx, deleteBeyondPartitionCap, policy.PartitionKey, policy.MaxPerPartition)
		if err != nil {
			return eventbus.TrimResult{}, fmt.Errorf("sqlite: trim: apply partition cap: %w", err)
		}
		n, _ := res.RowsAffected()
		deleted += n
	}

	if err = tx.Commit(); err != nil {
		return eventbus.TrimResult{}, fmt.Errorf("sqlite: trim: commit: %w", err)
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
// retention table).
func (s *SQLiteStore) trimmedThrough(ctx context.Context) (int64, error) {
	var watermark int64
	err := s.db.QueryRowContext(ctx, selectTrimmedThrough).Scan(&watermark)
	switch {
	case err == nil:
		return watermark, nil
	case errors.Is(err, sql.ErrNoRows), err != nil && strings.Contains(err.Error(), "no such table: retention"):
		return 0, nil
	default:
		return 0, fmt.Errorf("sqlite: read trim boundary: %w", err)
	}
}

// checkTrimmed reports whether a read from position (issued as from) skipped
// events removed by Trim. Callers check after their event query has taken its
// read snapshot: if a trim committed before that snapshot, the boundary read
// here already includes it; if it committed later, the query saw every event.
func (s *SQLiteStore) checkTrimmed(ctx context.Context, from eventbus.Offset, position int64) error {
	if position <= 0 {
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
