package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	eventbus "github.com/jilio/ebu"
	_ "modernc.org/sqlite"
)

// SQLiteStore implements eventbus.EventStore using SQLite
type SQLiteStore struct {
	db          *sql.DB
	cfg         *config
	logger      Logger
	metricsHook MetricsHook

	// Prepared statements
	saveStmt        *sql.Stmt
	loadStmt        *sql.Stmt
	loadFromStmt    *sql.Stmt
	getPositionStmt *sql.Stmt
	saveSubPosStmt  *sql.Stmt
	loadSubPosStmt  *sql.Stmt
}

// Ensure SQLiteStore implements eventbus.EventStore
var _ eventbus.EventStore = (*SQLiteStore)(nil)

// dbOpener is used to open database connections, injectable for testing
var dbOpener = sql.Open

// New creates a new SQLiteStore with the given path and options
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
		{&s.saveStmt, "INSERT INTO events (type, data, timestamp) VALUES (?, ?, ?)"},
		{&s.loadStmt, "SELECT position, type, data, timestamp FROM events WHERE position >= ? AND position <= ? ORDER BY position"},
		{&s.loadFromStmt, "SELECT position, type, data, timestamp FROM events WHERE position >= ? ORDER BY position"},
		{&s.getPositionStmt, "SELECT COALESCE(MAX(position), 0) FROM events"},
		{&s.saveSubPosStmt, `INSERT INTO subscription_positions (subscription_id, position, updated_at)
			VALUES (?, ?, CURRENT_TIMESTAMP)
			ON CONFLICT(subscription_id) DO UPDATE SET position = excluded.position, updated_at = CURRENT_TIMESTAMP`},
		{&s.loadSubPosStmt, "SELECT position FROM subscription_positions WHERE subscription_id = ?"},
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

// Save implements eventbus.EventStore
func (s *SQLiteStore) Save(ctx context.Context, event *eventbus.StoredEvent) error {
	start := time.Now()

	result, err := s.saveStmt.ExecContext(ctx, event.Type, event.Data, event.Timestamp)
	if err != nil {
		if s.metricsHook != nil {
			s.metricsHook.OnSave(time.Since(start), err)
		}
		return fmt.Errorf("sqlite: save event: %w", err)
	}

	// LastInsertId is always supported by SQLite driver
	position, _ := result.LastInsertId()
	event.Position = position

	if s.metricsHook != nil {
		s.metricsHook.OnSave(time.Since(start), nil)
	}

	if s.logger != nil {
		s.logger.Debug("saved event", "position", position, "type", event.Type)
	}

	return nil
}

// rowScanner abstracts sql.Rows for testing
type rowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// Load implements eventbus.EventStore
func (s *SQLiteStore) Load(ctx context.Context, from, to int64) (events []*eventbus.StoredEvent, err error) {
	start := time.Now()

	defer func() {
		if s.metricsHook != nil {
			s.metricsHook.OnLoad(time.Since(start), len(events), err)
		}
	}()

	var rows *sql.Rows

	if to == -1 {
		rows, err = s.loadFromStmt.QueryContext(ctx, from)
	} else {
		rows, err = s.loadStmt.QueryContext(ctx, from, to)
	}

	if err != nil {
		return nil, fmt.Errorf("sqlite: load events: %w", err)
	}

	events, err = s.scanEvents(rows)
	if err != nil {
		return nil, err
	}

	if s.logger != nil {
		s.logger.Debug("loaded events", "from", from, "to", to, "count", len(events))
	}

	return events, nil
}

// scanEvents scans rows into events - extracted for testability
func (s *SQLiteStore) scanEvents(rows rowScanner) ([]*eventbus.StoredEvent, error) {
	defer rows.Close()

	var events []*eventbus.StoredEvent
	for rows.Next() {
		event := &eventbus.StoredEvent{}
		if err := rows.Scan(&event.Position, &event.Type, &event.Data, &event.Timestamp); err != nil {
			return nil, fmt.Errorf("sqlite: scan event: %w", err)
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate events: %w", err)
	}

	return events, nil
}

// GetPosition implements eventbus.EventStore
func (s *SQLiteStore) GetPosition(ctx context.Context) (int64, error) {
	start := time.Now()

	var position int64
	err := s.getPositionStmt.QueryRowContext(ctx).Scan(&position)
	if err != nil {
		if s.metricsHook != nil {
			s.metricsHook.OnGetPosition(time.Since(start), err)
		}
		return 0, fmt.Errorf("sqlite: get position: %w", err)
	}

	if s.metricsHook != nil {
		s.metricsHook.OnGetPosition(time.Since(start), nil)
	}

	return position, nil
}

// SaveSubscriptionPosition implements eventbus.EventStore
func (s *SQLiteStore) SaveSubscriptionPosition(ctx context.Context, subscriptionID string, position int64) error {
	start := time.Now()

	_, err := s.saveSubPosStmt.ExecContext(ctx, subscriptionID, position)
	if err != nil {
		if s.metricsHook != nil {
			s.metricsHook.OnSaveSubscriptionPosition(time.Since(start), err)
		}
		return fmt.Errorf("sqlite: save subscription position: %w", err)
	}

	if s.metricsHook != nil {
		s.metricsHook.OnSaveSubscriptionPosition(time.Since(start), nil)
	}

	if s.logger != nil {
		s.logger.Debug("saved subscription position", "subscription_id", subscriptionID, "position", position)
	}

	return nil
}

// LoadSubscriptionPosition implements eventbus.EventStore
func (s *SQLiteStore) LoadSubscriptionPosition(ctx context.Context, subscriptionID string) (int64, error) {
	start := time.Now()

	var position int64
	err := s.loadSubPosStmt.QueryRowContext(ctx, subscriptionID).Scan(&position)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if s.metricsHook != nil {
				s.metricsHook.OnLoadSubscriptionPosition(time.Since(start), nil)
			}
			return 0, nil
		}
		if s.metricsHook != nil {
			s.metricsHook.OnLoadSubscriptionPosition(time.Since(start), err)
		}
		return 0, fmt.Errorf("sqlite: load subscription position: %w", err)
	}

	if s.metricsHook != nil {
		s.metricsHook.OnLoadSubscriptionPosition(time.Since(start), nil)
	}

	return position, nil
}

// Close closes the database connection and releases resources
func (s *SQLiteStore) Close() error {
	// Close prepared statements
	if s.saveStmt != nil {
		s.saveStmt.Close()
	}
	if s.loadStmt != nil {
		s.loadStmt.Close()
	}
	if s.loadFromStmt != nil {
		s.loadFromStmt.Close()
	}
	if s.getPositionStmt != nil {
		s.getPositionStmt.Close()
	}
	if s.saveSubPosStmt != nil {
		s.saveSubPosStmt.Close()
	}
	if s.loadSubPosStmt != nil {
		s.loadSubPosStmt.Close()
	}

	if s.logger != nil {
		s.logger.Info("closing sqlite store")
	}

	return s.db.Close()
}
