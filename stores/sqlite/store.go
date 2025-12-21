package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"time"

	eventbus "github.com/jilio/ebu"
	_ "modernc.org/sqlite"
)

// SQLiteStore implements eventbus.EventStore using SQLite.
// It stores events with integer positions internally and exposes them
// as opaque string offsets externally.
type SQLiteStore struct {
	db          *sql.DB
	cfg         *config
	logger      Logger
	metricsHook MetricsHook

	// Prepared statements
	appendStmt     *sql.Stmt
	readStmt       *sql.Stmt
	readFromStmt   *sql.Stmt
	saveOffsetStmt *sql.Stmt
	loadOffsetStmt *sql.Stmt
}

// Ensure SQLiteStore implements the required interfaces
var _ eventbus.EventStore = (*SQLiteStore)(nil)
var _ eventbus.EventStoreStreamer = (*SQLiteStore)(nil)
var _ eventbus.SubscriptionStore = (*SQLiteStore)(nil)

// dbOpener is used to open database connections, injectable for testing
var dbOpener = sql.Open

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

	// Build connection string with pragmas
	var dsn string
	if cfg.path == ":memory:" {
		// Use shared cache mode for in-memory databases to allow multiple connections
		dsn = "file::memory:?mode=memory&cache=shared"
	} else {
		dsn = fmt.Sprintf("file:%s?_busy_timeout=%d", cfg.path, cfg.busyTimeout.Milliseconds())
	}

	db, err := dbOpener("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open database: %w", err)
	}

	// Apply pragmas for performance
	// Errors here indicate filesystem issues (read-only, permissions)
	if err := applyPragmas(db, cfg); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: apply pragmas: %w", err)
	}

	// Run migrations if enabled
	if cfg.autoMigrate {
		if err := migrate(context.Background(), db); err != nil {
			db.Close()
			return nil, fmt.Errorf("sqlite: migrate: %w", err)
		}
	}

	return newFromDB(db, cfg)
}

// newFromDB creates a SQLiteStore from an existing database connection
func newFromDB(db *sql.DB, cfg *config) (*SQLiteStore, error) {
	store := &SQLiteStore{
		db:          db,
		cfg:         cfg,
		logger:      cfg.logger,
		metricsHook: cfg.metricsHook,
	}

	if err := store.prepareStatements(); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: prepare statements: %w", err)
	}

	return store, nil
}

// applyPragmas configures SQLite for optimal performance
func applyPragmas(db *sql.DB, cfg *config) error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = -64000", // 64MB cache
		fmt.Sprintf("PRAGMA busy_timeout = %d", cfg.busyTimeout.Milliseconds()),
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 268435456", // 256MB mmap
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("exec %q: %w", pragma, err)
		}
	}

	return nil
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
func parseOffset(offset eventbus.Offset) (int64, error) {
	if offset == eventbus.OffsetOldest {
		return 0, nil
	}
	return strconv.ParseInt(string(offset), 10, 64)
}

// formatOffset converts an int64 position to an Offset.
func formatOffset(position int64) eventbus.Offset {
	return eventbus.Offset(strconv.FormatInt(position, 10))
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
func (s *SQLiteStore) Read(ctx context.Context, from eventbus.Offset, limit int) ([]*eventbus.StoredEvent, eventbus.Offset, error) {
	start := time.Now()
	var events []*eventbus.StoredEvent
	var err error

	defer func() {
		if s.metricsHook != nil {
			s.metricsHook.OnRead(time.Since(start), len(events), err)
		}
	}()

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

// SaveOffset implements eventbus.SubscriptionStore
func (s *SQLiteStore) SaveOffset(ctx context.Context, subscriptionID string, offset eventbus.Offset) error {
	start := time.Now()

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
	}
	for _, stmt := range stmts {
		if stmt != nil {
			stmt.Close()
		}
	}

	if s.logger != nil {
		s.logger.Info("closing sqlite store")
	}

	return s.db.Close()
}

// ReadStream implements eventbus.EventStoreStreamer for memory-efficient event streaming.
// It uses cursor-based iteration, keeping only one row in memory at a time.
// The database rows are properly closed when:
// - The iteration completes naturally
// - The consumer breaks out of the range loop
// - The context is cancelled
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

		query := "SELECT position, type, data, timestamp FROM events WHERE position > ? ORDER BY position LIMIT ?"
		rows, err := s.db.QueryContext(ctx, query, currentPos, batchSize)
		if err != nil {
			*iterErr = fmt.Errorf("sqlite: read stream batch: %w", err)
			yield(nil, *iterErr)
			return
		}

		batchCount, lastPos, cont := s.streamBatch(rows, eventCount, iterErr, yield)
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
	rows rowScanner,
	eventCount *int,
	iterErr *error,
	yield func(*eventbus.StoredEvent, error) bool,
) (batchCount int, lastPos int64, cont bool) {
	for rows.Next() {
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

	if err := rows.Close(); err != nil {
		*iterErr = fmt.Errorf("sqlite: close rows: %w", err)
		yield(nil, *iterErr)
		return batchCount, lastPos, false
	}

	return batchCount, lastPos, true
}
