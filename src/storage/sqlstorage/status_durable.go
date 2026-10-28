package sqlstorage

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// Removal, revocation and acknowledged policy must survive power loss before
// any external acknowledgement/restore marker is released. Reserve one SQLite
// connection for FULL; ordinary capture/read/chat operations keep their mode.
func (s *SQLStatusHistoryStore) beginPolicy(ctx context.Context, durable bool) (*sqlx.Tx, *statusPolicyRecord, func() error, error) {
	if !durable || (s.db.DriverName() != "sqlite3" && s.db.DriverName() != "sqlite") {
		tx, rec, err := s.begin(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		return tx, rec, func() error { _ = tx.Rollback(); return nil }, nil
	}
	conn, err := s.db.Connx(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	var original int
	if err = conn.GetContext(ctx, &original, "PRAGMA synchronous"); err != nil {
		_ = conn.Close()
		return nil, nil, nil, err
	}
	if original < 0 || original > 3 {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("invalid SQLite synchronous mode")
	}
	var tx *sqlx.Tx
	finish := func() error {
		if tx != nil {
			_ = tx.Rollback()
		}
		// A cancelled request must not leak a changed setting into the pool.
		resetCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, resetErr := conn.ExecContext(resetCtx, fmt.Sprintf("PRAGMA synchronous=%d", original))
		if resetErr != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		closeErr := conn.Close()
		return errors.Join(resetErr, closeErr)
	}
	if _, err = conn.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		return nil, nil, nil, errors.Join(err, finish())
	}
	tx, err = conn.BeginTxx(ctx, nil)
	if err != nil {
		return nil, nil, nil, errors.Join(err, finish())
	}
	_, rec, err := lockStatusPolicy(ctx, tx)
	if err != nil {
		return nil, nil, nil, errors.Join(err, finish())
	}
	return tx, rec, finish, nil
}

func finishStatusPolicy(finish func() error, result *error) {
	if err := finish(); err != nil && *result == nil {
		*result = err
	}
}
