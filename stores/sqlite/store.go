package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	eventbus "github.com/jilio/ebu"
	sqlite3 "modernc.org/sqlite"
	sqlite3lib "modernc.org/sqlite/lib"
)

// SQLiteStore implements eventbus.EventStore using SQLite.
// It stores events with integer positions internally and exposes them
// as opaque string offsets externally.
type SQLiteStore struct {
	db          *sql.DB
	cfg         *config
	logger      Logger
	metricsHook MetricsHook

	// memConn pins one dedicated connection for :memory: stores. A
	// shared-cache in-memory database is destroyed the moment its last
	// connection closes, so without this pin the store would silently lose
	// all data if the pool ever drained to zero idle connections.
	memConn *sql.Conn

	// Prepared statements
	appendStmt       *sql.Stmt
	readStmt         *sql.Stmt
	readFromStmt     *sql.Stmt
	saveOffsetStmt   *sql.Stmt
	loadOffsetStmt   *sql.Stmt
	saveSnapshotStmt *sql.Stmt
	loadSnapshotStmt *sql.Stmt
}

// Ensure SQLiteStore implements the required interfaces
var _ eventbus.EventStore = (*SQLiteStore)(nil)
var _ eventbus.EventStoreStreamer = (*SQLiteStore)(nil)
var _ eventbus.SubscriptionStore = (*SQLiteStore)(nil)
var _ eventbus.EventStoreSnapshotter = (*SQLiteStore)(nil)
var _ eventbus.EventStoreTruncator = (*SQLiteStore)(nil)

// dbOpener is used to open database connections, injectable for testing
var dbOpener = sql.Open

// memDBCounter provides unique names for in-memory databases so separate
// :memory: stores never share state through SQLite's shared cache.
var memDBCounter atomic.Int64

// New creates a new SQLiteStore with the given path and options.
//
// Note: When WithAutoMigrate is enabled (the default), migrations run with
// context.Background() and are not cancellable. This ensures migrations
// complete fully to avoid leaving the database in an inconsistent state.
func New(path string, opts ...Option) (*SQLiteStore, error) {
	if path == "" {
		return nil, errors.New("sqlite: path is required")
	}

	// Validate path to prevent URI parameter injection
	if path != ":memory:" && (strings.Contains(path, "?") || strings.Contains(path, "#")) {
		return nil, errors.New("sqlite: path cannot contain '?' or '#' characters")
	}

	cfg := defaultConfig()
	cfg.path = path
	for _, opt := range opts {
		opt(cfg)
	}

	db, err := dbOpener("sqlite", buildDSN(cfg))
	if err != nil {
		return nil, fmt.Errorf("sqlite: open database: %w", err)
	}

	// Pin a dedicated connection for in-memory stores so the shared-cache
	// database survives even if the pool otherwise drops to zero connections.
	var memConn *sql.Conn
	if cfg.path == ":memory:" {
		memConn, err = db.Conn(context.Background())
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("sqlite: pin memory connection: %w", err)
		}
	} else if err := enableWAL(db, cfg.busyTimeout); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: enable WAL: %w", err)
	}

	// Run migrations if enabled
	if cfg.autoMigrate {
		if err := migrate(context.Background(), db); err != nil {
			if memConn != nil {
				memConn.Close()
			}
			db.Close()
			return nil, fmt.Errorf("sqlite: migrate: %w", err)
		}
	}

	return newFromDB(db, cfg, memConn)
}

// newFromDB creates a SQLiteStore from an existing database connection.
// memConn, if non-nil, is the pinned in-memory connection; on failure it is
// released along with db.
func newFromDB(db *sql.DB, cfg *config, memConn *sql.Conn) (*SQLiteStore, error) {
	store := &SQLiteStore{
		db:          db,
		cfg:         cfg,
		logger:      cfg.logger,
		metricsHook: cfg.metricsHook,
		memConn:     memConn,
	}

	if err := store.prepareStatements(); err != nil {
		if memConn != nil {
			memConn.Close()
		}
		db.Close()
		return nil, fmt.Errorf("sqlite: prepare statements: %w", err)
	}

	return store, nil
}

// buildDSN constructs the modernc.org/sqlite connection string.
//
// Pragmas are connection-scoped in SQLite, and database/sql maintains a pool
// of connections, so they must travel as modernc `_pragma=` DSN parameters —
// that way the driver applies them to EVERY pooled connection. Applying them
// via db.Exec would configure only whichever single connection the pool
// happened to hand out (leaving the rest without e.g. busy_timeout, which
// made concurrent appends fail with SQLITE_BUSY).
func buildDSN(cfg *config) string {
	if cfg.path == ":memory:" {
		// Shared cache mode lets database/sql's pooled connections see the
		// same in-memory database. Each store gets a unique name so two
		// independent :memory: stores in the same process don't share data.
		return fmt.Sprintf("file:ebu_memdb_%d?mode=memory&cache=shared", memDBCounter.Add(1))
	}

	pragmas := []string{
		fmt.Sprintf("busy_timeout(%d)", cfg.busyTimeout.Milliseconds()),
		"synchronous(NORMAL)",
		"cache_size(-64000)", // 64MB cache
		"temp_store(MEMORY)",
		"mmap_size(268435456)", // 256MB mmap
	}

	params := make([]string, 0, len(pragmas)+1)
	// Immediate transactions take the write lock at BEGIN (honoring
	// busy_timeout) instead of upgrading mid-transaction, which can return
	// SQLITE_BUSY immediately. Only migrations use transactions here.
	params = append(params, "_txlock=immediate")
	for _, p := range pragmas {
		params = append(params, "_pragma="+p)
	}

	return fmt.Sprintf("file:%s?%s", cfg.path, strings.Join(params, "&"))
}

// enableWAL switches a file-backed database to WAL journaling. Unlike the
// connection-scoped pragmas in buildDSN, journal_mode is database-scoped and
// persists in the file, so a one-time db.Exec is the right scope. The initial
// rollback-to-WAL conversion needs an exclusive lock and — unlike ordinary
// writes — fails with SQLITE_BUSY without consulting the busy handler when
// several processes race it, so busy errors are retried until timeout; the
// losers of the race then see an already-WAL file and no-op.
func enableWAL(db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		_, err := db.Exec("PRAGMA journal_mode = WAL")
		if err == nil || !isBusy(err) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
}

// isBusy reports whether err is SQLITE_BUSY (including extended busy codes
// such as SQLITE_BUSY_RECOVERY, whose low byte is the primary code).
func isBusy(err error) bool {
	var serr *sqlite3.Error
	return errors.As(err, &serr) && serr.Code()&0xff == sqlite3lib.SQLITE_BUSY
}

// prepareStatements prepares all SQL statements
func (s *SQLiteStore) prepareStatements() error {
	type stmtDef struct {
		dest **sql.Stmt
		sql  string
	}

	stmts := []stmtDef{
		{&s.appendStmt, "INSERT INTO events (type, data, timestamp) VALUES (?, ?, ?)"},
		{&s.readStmt, "SELECT position, type, data, timestamp FROM events WHERE position > ? ORDER BY position LIMIT ?"},
		{&s.readFromStmt, "SELECT position, type, data, timestamp FROM events WHERE position > ? ORDER BY position"},
		{&s.saveOffsetStmt, `INSERT INTO subscription_positions (subscription_id, position, updated_at)
			VALUES (?, ?, CURRENT_TIMESTAMP)
			ON CONFLICT(subscription_id) DO UPDATE SET position = excluded.position, updated_at = CURRENT_TIMESTAMP`},
		{&s.loadOffsetStmt, "SELECT position FROM subscription_positions WHERE subscription_id = ?"},
		{&s.saveSnapshotStmt, `INSERT INTO snapshots (snapshot_id, position, data, updated_at)
			VALUES (?, ?, ?, CURRENT_TIMESTAMP)
			ON CONFLICT(snapshot_id) DO UPDATE SET position = excluded.position, data = excluded.data, updated_at = CURRENT_TIMESTAMP`},
		{&s.loadSnapshotStmt, "SELECT position, data FROM snapshots WHERE snapshot_id = ?"},
	}

	for _, def := range stmts {
		stmt, err := s.db.Prepare(def.sql)
		if err != nil {
			return fmt.Errorf("prepare statement: %w", err)
		}
		*def.dest = stmt
	}

	return nil
}

// parseOffset converts an Offset to an int64 position.
// OffsetOldest maps to 0, otherwise parses the numeric string.
// OffsetNewest is not parseable; callers that accept it must resolve it
// to a concrete position first (see resolveNewest).
func parseOffset(offset eventbus.Offset) (int64, error) {
	if offset == eventbus.OffsetOldest {
		return 0, nil
	}
	return strconv.ParseInt(string(offset), 10, 64)
}

// resolveNewest resolves OffsetNewest to the current tail of the log:
// MAX(position) at call time, or 0 when the events table is empty.
func (s *SQLiteStore) resolveNewest(ctx context.Context) (int64, error) {
	var position sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT MAX(position) FROM events").Scan(&position); err != nil {
		return 0, fmt.Errorf("sqlite: resolve newest offset: %w", err)
	}
	return position.Int64, nil
}

// formatOffset converts an int64 position to an Offset.
// Offsets are zero-padded to 20 digits so they compare lexicographically,
// as the eventbus.Offset contract requires ("999" < "1000" fails as plain
// strings). parseOffset accepts both padded and legacy unpadded values.
func formatOffset(position int64) eventbus.Offset {
	return eventbus.Offset(fmt.Sprintf("%020d", position))
}

// Append stores an event and returns its assigned offset.
func (s *SQLiteStore) Append(ctx context.Context, event *eventbus.Event) (eventbus.Offset, error) {
	start := time.Now()

	result, err := s.appendStmt.ExecContext(ctx, event.Type, event.Data, event.Timestamp)
	if err != nil {
		if s.metricsHook != nil {
			s.metricsHook.OnAppend(time.Since(start), err)
		}
		return "", fmt.Errorf("sqlite: append event: %w", err)
	}

	// LastInsertId is always supported by SQLite driver
	position, _ := result.LastInsertId()
	offset := formatOffset(position)

	if s.metricsHook != nil {
		s.metricsHook.OnAppend(time.Since(start), nil)
	}

	if s.logger != nil {
		s.logger.Debug("appended event", "offset", offset, "type", event.Type)
	}

	return offset, nil
}

// rowScanner abstracts sql.Rows for testing
type rowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// Read returns events starting after the given offset.
// OffsetNewest resolves at call time to the current tail of the log: no
// events, nil error, and a concrete resumable offset (the last position,
// or OffsetOldest when the store is empty) as nextOffset.
func (s *SQLiteStore) Read(ctx context.Context, from eventbus.Offset, limit int) ([]*eventbus.StoredEvent, eventbus.Offset, error) {
	start := time.Now()
	var events []*eventbus.StoredEvent
	var err error

	defer func() {
		if s.metricsHook != nil {
			s.metricsHook.OnRead(time.Since(start), len(events), err)
		}
	}()

	if from == eventbus.OffsetNewest {
		var position int64
		position, err = s.resolveNewest(ctx)
		if err != nil {
			return nil, from, err
		}
		if position == 0 {
			return nil, eventbus.OffsetOldest, nil
		}
		return nil, formatOffset(position), nil
	}

	position, err := parseOffset(from)
	if err != nil {
		return nil, from, fmt.Errorf("sqlite: invalid offset: %w", err)
	}

	var rows *sql.Rows
	if limit <= 0 {
		rows, err = s.readFromStmt.QueryContext(ctx, position)
	} else {
		rows, err = s.readStmt.QueryContext(ctx, position, limit)
	}

	if err != nil {
		return nil, from, fmt.Errorf("sqlite: read events: %w", err)
	}

	events, err = s.scanEvents(rows)
	if err != nil {
		return nil, from, err
	}

	// Determine next offset
	nextOffset := from
	if len(events) > 0 {
		nextOffset = events[len(events)-1].Offset
	}

	if s.logger != nil {
		s.logger.Debug("read events", "from", from, "limit", limit, "count", len(events))
	}

	return events, nextOffset, nil
}

// scanEvents scans rows into events - extracted for testability
func (s *SQLiteStore) scanEvents(rows rowScanner) ([]*eventbus.StoredEvent, error) {
	defer rows.Close()

	var events []*eventbus.StoredEvent
	for rows.Next() {
		var position int64
		event := &eventbus.StoredEvent{}
		if err := rows.Scan(&position, &event.Type, &event.Data, &event.Timestamp); err != nil {
			return nil, fmt.Errorf("sqlite: scan event: %w", err)
		}
		event.Offset = formatOffset(position)
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate events: %w", err)
	}

	return events, nil
}

// SaveOffset implements eventbus.SubscriptionStore.
// OffsetNewest is resolved at call time to the current tail of the log and
// that concrete position is persisted, so LoadOffset always returns an
// offset the store can resume from.
func (s *SQLiteStore) SaveOffset(ctx context.Context, subscriptionID string, offset eventbus.Offset) error {
	start := time.Now()

	if offset == eventbus.OffsetNewest {
		position, err := s.resolveNewest(ctx)
		if err != nil {
			return err
		}
		offset = formatOffset(position)
	}

	position, err := parseOffset(offset)
	if err != nil {
		return fmt.Errorf("sqlite: invalid offset: %w", err)
	}

	_, err = s.saveOffsetStmt.ExecContext(ctx, subscriptionID, position)
	if err != nil {
		if s.metricsHook != nil {
			s.metricsHook.OnSaveOffset(time.Since(start), err)
		}
		return fmt.Errorf("sqlite: save offset: %w", err)
	}

	if s.metricsHook != nil {
		s.metricsHook.OnSaveOffset(time.Since(start), nil)
	}

	if s.logger != nil {
		s.logger.Debug("saved subscription offset", "subscription_id", subscriptionID, "offset", offset)
	}

	return nil
}

// LoadOffset implements eventbus.SubscriptionStore
func (s *SQLiteStore) LoadOffset(ctx context.Context, subscriptionID string) (eventbus.Offset, error) {
	start := time.Now()

	var position int64
	err := s.loadOffsetStmt.QueryRowContext(ctx, subscriptionID).Scan(&position)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if s.metricsHook != nil {
				s.metricsHook.OnLoadOffset(time.Since(start), nil)
			}
			return eventbus.OffsetOldest, nil
		}
		if s.metricsHook != nil {
			s.metricsHook.OnLoadOffset(time.Since(start), err)
		}
		return eventbus.OffsetOldest, fmt.Errorf("sqlite: load offset: %w", err)
	}

	if s.metricsHook != nil {
		s.metricsHook.OnLoadOffset(time.Since(start), nil)
	}

	return formatOffset(position), nil
}

// SaveSnapshot upserts the compaction snapshot for snapshotID, recording that
// blob reflects the projection as of (and including) atOffset.
func (s *SQLiteStore) SaveSnapshot(ctx context.Context, snapshotID string, atOffset eventbus.Offset, blob json.RawMessage) error {
	position, err := parseOffset(atOffset)
	if err != nil {
		return fmt.Errorf("sqlite: invalid snapshot offset: %w", err)
	}
	if _, err := s.saveSnapshotStmt.ExecContext(ctx, snapshotID, position, []byte(blob)); err != nil {
		return fmt.Errorf("sqlite: save snapshot: %w", err)
	}
	return nil
}

// LoadSnapshot returns the last saved snapshot for snapshotID, or
// (OffsetOldest, nil, nil) when none exists.
func (s *SQLiteStore) LoadSnapshot(ctx context.Context, snapshotID string) (eventbus.Offset, json.RawMessage, error) {
	var position int64
	var blob []byte
	err := s.loadSnapshotStmt.QueryRowContext(ctx, snapshotID).Scan(&position, &blob)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return eventbus.OffsetOldest, nil, nil
		}
		return eventbus.OffsetOldest, nil, fmt.Errorf("sqlite: load snapshot: %w", err)
	}
	return formatOffset(position), json.RawMessage(blob), nil
}

// TruncateBefore deletes every event whose position is <= beforeOffset and
// returns the number deleted. beforeOffset == OffsetOldest is a no-op. The events
// table uses AUTOINCREMENT, so deleted positions are never reused; a snapshot
// resumed via Replay(atOffset) (which reads position > atOffset) is unaffected.
func (s *SQLiteStore) TruncateBefore(ctx context.Context, beforeOffset eventbus.Offset) (int64, error) {
	position, err := parseOffset(beforeOffset)
	if err != nil {
		return 0, fmt.Errorf("sqlite: invalid truncate offset: %w", err)
	}
	if position <= 0 {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, "DELETE FROM events WHERE position <= ?", position)
	if err != nil {
		return 0, fmt.Errorf("sqlite: truncate events: %w", err)
	}
	deleted, _ := res.RowsAffected()
	// Checkpoint in TRUNCATE mode so the freed pages actually shrink the WAL/db.
	// Best-effort and deliberately unchecked: the rows are already durably gone; a
	// failed checkpoint only means space reclaim lags to the next automatic one.
	_, _ = s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return deleted, nil
}

// Close closes the database connection and releases resources.
// Prepared statement close errors are ignored as they cannot fail in practice
// with SQLite (the driver handles cleanup when the connection closes).
func (s *SQLiteStore) Close() error {
	// Close prepared statements - errors ignored as db.Close() handles cleanup
	stmts := []*sql.Stmt{
		s.appendStmt,
		s.readStmt,
		s.readFromStmt,
		s.saveOffsetStmt,
		s.loadOffsetStmt,
		s.saveSnapshotStmt,
		s.loadSnapshotStmt,
	}
	for _, stmt := range stmts {
		if stmt != nil {
			stmt.Close()
		}
	}

	if s.logger != nil {
		s.logger.Info("closing sqlite store")
	}

	// Release the pinned in-memory connection (if any) so db.Close() can
	// actually tear the pool down.
	if s.memConn != nil {
		s.memConn.Close()
	}

	return s.db.Close()
}

// ReadStream implements eventbus.EventStoreStreamer for memory-efficient event streaming.
// It uses cursor-based iteration, keeping only one row in memory at a time.
// By default events are fetched in batches (see WithStreamBatchSize), so a
// stream does not pin a pooled connection and its WAL read snapshot for the
// whole iteration; as a consequence, batched iteration may observe events
// appended while the iteration is in progress.
// The database rows are properly closed when:
// - The iteration completes naturally
// - The consumer breaks out of the range loop
// - The context is cancelled
//
// OffsetNewest is the tail of the log at call time, so streaming from it
// yields nothing.
func (s *SQLiteStore) ReadStream(ctx context.Context, from eventbus.Offset) iter.Seq2[*eventbus.StoredEvent, error] {
	return func(yield func(*eventbus.StoredEvent, error) bool) {
		start := time.Now()
		var eventCount int
		var iterErr error

		defer func() {
			if s.metricsHook != nil {
				s.metricsHook.OnRead(time.Since(start), eventCount, iterErr)
			}
		}()

		if from == eventbus.OffsetNewest {
			return
		}

		position, err := parseOffset(from)
		if err != nil {
			iterErr = fmt.Errorf("sqlite: invalid offset: %w", err)
			yield(nil, iterErr)
			return
		}

		// If batching is enabled, use cursor-based pagination
		if s.cfg.streamBatchSize > 0 {
			s.streamBatched(ctx, position, &eventCount, &iterErr, yield)
			return
		}

		rows, err := s.readFromStmt.QueryContext(ctx, position)
		if err != nil {
			iterErr = fmt.Errorf("sqlite: read stream: %w", err)
			yield(nil, iterErr)
			return
		}

		s.streamRows(ctx, rows, &eventCount, &iterErr, yield)

		if s.logger != nil {
			s.logger.Debug("streamed events", "from", from, "count", eventCount)
		}
	}
}

// streamRows iterates over rows yielding events. Extracted for testability.
func (s *SQLiteStore) streamRows(
	ctx context.Context,
	rows rowScanner,
	eventCount *int,
	iterErr *error,
	yield func(*eventbus.StoredEvent, error) bool,
) {
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			*iterErr = ctx.Err()
			yield(nil, *iterErr)
			return
		default:
		}

		var position int64
		event := &eventbus.StoredEvent{}
		if err := rows.Scan(&position, &event.Type, &event.Data, &event.Timestamp); err != nil {
			*iterErr = fmt.Errorf("sqlite: scan event: %w", err)
			yield(nil, *iterErr)
			return
		}
		event.Offset = formatOffset(position)

		*eventCount++
		if !yield(event, nil) {
			return
		}
	}

	if err := rows.Err(); err != nil {
		*iterErr = fmt.Errorf("sqlite: iterate events: %w", err)
		yield(nil, *iterErr)
		return
	}
}

// streamBatched fetches events in batches using cursor-based pagination
func (s *SQLiteStore) streamBatched(
	ctx context.Context,
	fromPosition int64,
	eventCount *int,
	iterErr *error,
	yield func(*eventbus.StoredEvent, error) bool,
) {
	batchSize := s.cfg.streamBatchSize
	currentPos := fromPosition

	for {
		select {
		case <-ctx.Done():
			*iterErr = ctx.Err()
			yield(nil, *iterErr)
			return
		default:
		}

		rows, err := s.readStmt.QueryContext(ctx, currentPos, batchSize)
		if err != nil {
			*iterErr = fmt.Errorf("sqlite: read stream batch: %w", err)
			yield(nil, *iterErr)
			return
		}

		batchCount, lastPos, cont := s.streamBatch(ctx, rows, eventCount, iterErr, yield)
		if !cont {
			return
		}

		// If we got fewer rows than batch size, we're done
		if batchCount < batchSize {
			break
		}

		// Move to next batch (position after last seen)
		currentPos = lastPos
	}

	if s.logger != nil {
		s.logger.Debug("streamed events (batched)", "from", fromPosition, "count", *eventCount, "batchSize", batchSize)
	}
}

// streamBatch processes a single batch of rows. Returns (batchCount, lastPos, shouldContinue).
// shouldContinue is false if iteration should stop (error or early termination).
func (s *SQLiteStore) streamBatch(
	ctx context.Context,
	rows rowScanner,
	eventCount *int,
	iterErr *error,
	yield func(*eventbus.StoredEvent, error) bool,
) (batchCount int, lastPos int64, cont bool) {
	for rows.Next() {
		// Checked per row, not just per batch, so a large batch still
		// responds promptly to cancellation.
		select {
		case <-ctx.Done():
			rows.Close() // Best effort close, cancellation takes precedence
			*iterErr = ctx.Err()
			yield(nil, *iterErr)
			return batchCount, lastPos, false
		default:
		}

		var position int64
		event := &eventbus.StoredEvent{}
		if err := rows.Scan(&position, &event.Type, &event.Data, &event.Timestamp); err != nil {
			rows.Close() // Best effort close, scan error takes precedence
			*iterErr = fmt.Errorf("sqlite: scan event: %w", err)
			yield(nil, *iterErr)
			return 0, 0, false
		}
		event.Offset = formatOffset(position)

		lastPos = position
		batchCount++
		*eventCount++

		if !yield(event, nil) {
			rows.Close() // Best effort close on early termination
			return batchCount, lastPos, false
		}
	}

	// rows.Err() is the only place database/sql surfaces mid-iteration
	// failures (including context cancellation); without this check a failed
	// batch is indistinguishable from the end of the log and the stream would
	// silently end after a partial replay.
	if err := rows.Err(); err != nil {
		rows.Close() // Best effort close, iteration error takes precedence
		*iterErr = fmt.Errorf("sqlite: iterate events: %w", err)
		yield(nil, *iterErr)
		return batchCount, lastPos, false
	}

	if err := rows.Close(); err != nil {
		*iterErr = fmt.Errorf("sqlite: close rows: %w", err)
		yield(nil, *iterErr)
		return batchCount, lastPos, false
	}

	return batchCount, lastPos, true
}
