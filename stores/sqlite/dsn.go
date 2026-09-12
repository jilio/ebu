package sqlite

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

// buildDSN configures each pooled connection, including connections opened
// after construction. Running connection-scoped pragmas through db.Exec would
// configure only the connection handed out by the pool at that moment.
func buildDSN(cfg *config) (string, error) {
	if cfg.path == ":memory:" {
		// Shared cache mode lets pooled connections see the same database.
		// Each store gets a unique name so independent stores stay isolated.
		return fmt.Sprintf("file:ebu_memdb_%d?mode=memory&cache=shared", memDBCounter.Add(1)), nil
	}
	// SQLite truncates decoded URI paths at NUL. Reject it before encoding so
	// an invalid filename cannot open or create a different, shorter path.
	if strings.ContainsRune(cfg.path, '\x00') {
		return "", fmt.Errorf("sqlite: path cannot contain NUL")
	}
	abs, err := filepath.Abs(cfg.path)
	if err != nil {
		return "", fmt.Errorf("sqlite: resolve database path: %w", err)
	}
	// Both constructors share read-side settings. Writer durability and
	// transaction locking belong only to New; OpenReadOnly never requests a
	// journal-mode change or an immediate (write-locking) transaction.
	query := url.Values{"_pragma": {
		fmt.Sprintf("busy_timeout(%d)", cfg.busyTimeout.Milliseconds()),
		"cache_size(-64000)", // 64MB cache
		"temp_store(MEMORY)",
		"mmap_size(268435456)", // 256MB mmap
	}}
	if cfg.readOnly {
		query.Set("mode", "ro")
		query.Add("_pragma", "query_only(1)")
	} else {
		query.Add("_pragma", "synchronous(NORMAL)")
		// Immediate transactions take the write lock at BEGIN (honoring
		// busy_timeout) instead of upgrading mid-transaction, which can
		// return SQLITE_BUSY immediately. Only migrations use them here.
		query.Set("_txlock", "immediate")
	}
	// Escape the filename and parameters separately. Literal %, ? and # must
	// never redirect the file or inject SQLite options into either constructor.
	// OmitHost also keeps Windows drive letters in the path, not an authority.
	uri := url.URL{Scheme: "file", OmitHost: true, Path: filepath.ToSlash(abs), RawQuery: query.Encode()}
	return uri.String(), nil
}
