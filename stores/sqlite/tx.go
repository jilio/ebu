package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// inTx runs fn in one write transaction on a dedicated connection and
// commits it. label prefixes begin and commit failures; fn's own errors are
// returned unchanged after the transaction is rolled back. A panic in fn also
// rolls back.
//
// SQLite does not always end a transaction whose COMMIT fails: on a deferred
// constraint violation the transaction stays open so the caller could fix and
// retry. database/sql nevertheless treats the transaction as finished and
// would return the connection to the pool mid-transaction, where the
// uncommitted changes stay visible to whoever uses it next. inTx therefore
// rolls the connection back itself before releasing it.
func (s *SQLiteStore) inTx(ctx context.Context, label string, fn func(*sql.Tx) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%s: begin: %w", label, err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: begin: %w", label, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		// Best effort: "no transaction is active" just means SQLite already
		// rolled back on its own.
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		return fmt.Errorf("%s: commit: %w", label, err)
	}
	committed = true
	return nil
}
