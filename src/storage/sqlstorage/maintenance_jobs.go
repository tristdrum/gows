package sqlstorage

import (
	"context"
	"errors"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
)

func (h *SQLMaintenanceHistoryStore) RegisterHistory(ctx context.Context, id string, notification []byte) error {
	if id == "" || len(notification) == 0 || len(notification) > 2*1024*1024 {
		return errors.New("invalid bounded history notification")
	}
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		var count int
		if err := tx.GetContext(ctx, &count, `SELECT COUNT(*) FROM gows_maintenance_history_jobs WHERE id=$1`, id); err != nil {
			return err
		}
		if count > 0 {
			// A repeated notification after a lost receipt retries only the receipt;
			// already durable history is not downloaded or persisted again.
			_, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_history_jobs SET acked=FALSE,attempts=0 WHERE id=$1 AND acked=TRUE`, id)
			return err
		}
		if err := tx.GetContext(ctx, &count, `SELECT COUNT(*) FROM gows_maintenance_history_jobs WHERE acked=FALSE`); err != nil {
			return err
		}
		if count >= maintenanceMaxJobs {
			s.Fault = "history_queue_full"
			s.Since = time.Now().Unix() + 1
			return nil
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO gows_maintenance_history_jobs(id,notification,created) VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING`, id, notification, time.Now().Unix())
		return err
	})
}
func (h *SQLMaintenanceHistoryStore) HistoryJobs(ctx context.Context) ([]storage.MaintenanceHistoryJob, error) {
	jobs := []storage.MaintenanceHistoryJob{}
	err := h.db.SelectContext(ctx, &jobs, `SELECT id,notification,stored,attempts FROM gows_maintenance_history_jobs WHERE acked=FALSE AND attempts<5 ORDER BY created,id LIMIT 1`)
	return jobs, err
}
func (h *SQLMaintenanceHistoryStore) HistoryAttempt(ctx context.Context, id string) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		_, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_history_jobs SET attempts=attempts+1 WHERE id=$1`, id)
		return err
	})
}
func (h *SQLMaintenanceHistoryStore) HistoryStored(ctx context.Context, id string) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		_, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_history_jobs SET stored=TRUE,last_error='' WHERE id=$1`, id)
		return err
	})
}
func (h *SQLMaintenanceHistoryStore) HistoryAcked(ctx context.Context, id string) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		_, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_history_jobs SET acked=TRUE,last_error='',notification=$1 WHERE id=$2`, []byte{}, id)
		return err
	})
}
func (h *SQLMaintenanceHistoryStore) Prune(ctx context.Context) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		floor := time.Now().Add(-maintenanceRetention).Unix()
		for _, q := range []string{`DELETE FROM gows_maintenance_activity WHERE timestamp<$1`, `DELETE FROM gows_maintenance_history_jobs WHERE created<$1 AND acked=TRUE`} {
			if _, err := tx.ExecContext(ctx, q, floor); err != nil {
				return err
			}
		}
		if s.Since < floor {
			s.Since = floor
		}
		return nil
	})
}

func (h *SQLMaintenanceHistoryStore) HistoryFailed(ctx context.Context, id, reason string) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		_, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_history_jobs SET last_error=$1 WHERE id=$2`, reason, id)
		return err
	})
}
