package sqlstorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

const maintenanceRetention = 48 * time.Hour
const maintenanceMaxJobs = 128

type SQLMaintenanceHistoryStore struct {
	db *sqlx.DB
	mu sync.Mutex
	// A failed ledger write cannot be represented durably in that same failed
	// database. Keep a process-local failure fence as well; restart changes epoch.
	fault bool
}
type maintenanceState struct {
	Epoch     string `json:"epoch"`
	Since     int64  `json:"since"`
	Connected bool   `json:"connected"`
	Offline   bool   `json:"offline"`
	Pending   int    `json:"pending"`
	Fault     string `json:"fault,omitempty"`
}

// Additive side tables deliberately leave gows_migrations at its existing
// version, so the previous image can still open and snapshot the same database.
func (c *GContainer) NewMaintenanceHistoryStorage(ctx context.Context) (*SQLMaintenanceHistoryStore, error) {
	h := &SQLMaintenanceHistoryStore{db: c.db}
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS gows_maintenance_state(id INTEGER PRIMARY KEY,data TEXT NOT NULL,revision BIGINT NOT NULL DEFAULT 0,untracked BOOLEAN NOT NULL DEFAULT FALSE)`,
		`CREATE TABLE IF NOT EXISTS gows_maintenance_activity(id TEXT PRIMARY KEY,provider_jid TEXT NOT NULL,jid TEXT NOT NULL,timestamp BIGINT NOT NULL,from_me BOOLEAN NOT NULL,identity_known BOOLEAN NOT NULL,deleted BOOLEAN NOT NULL DEFAULT FALSE,revision BIGINT NOT NULL DEFAULT 0)`,
		`CREATE INDEX IF NOT EXISTS gows_maintenance_activity_order ON gows_maintenance_activity(timestamp,jid,id)`,
		`CREATE TABLE IF NOT EXISTS gows_maintenance_history_jobs(id TEXT PRIMARY KEY,notification BYTEA NOT NULL,stored BOOLEAN NOT NULL DEFAULT FALSE,acked BOOLEAN NOT NULL DEFAULT FALSE,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',created BIGINT NOT NULL)`,
	} {
		if _, err := c.db.ExecContext(ctx, q); err != nil {
			return nil, err
		}
	}
	// Detect writes by an older image or a path outside registered work. Receipt
	// status-only changes are harmless and do not imply missed ordinary activity.
	if c.dialect == "sqlite3" || c.dialect == "sqlite" {
		for _, op := range []string{"INSERT", "UPDATE", "DELETE"} {
			meaningful := "1"
			if op == "UPDATE" {
				meaningful = `(OLD.jid<>NEW.jid OR OLD.timestamp<>NEW.timestamp OR OLD.is_real<>NEW.is_real OR json_extract(OLD.data,'$.Message') IS NOT json_extract(NEW.data,'$.Message') OR json_extract(OLD.data,'$.Info') IS NOT json_extract(NEW.data,'$.Info'))`
			}
			activityID := "NEW.id"
			deleted := "deleted"
			if op == "DELETE" {
				activityID = "OLD.id"
				deleted = "TRUE"
			}
			activity := fmt.Sprintf(`UPDATE gows_maintenance_activity SET revision=(SELECT revision FROM gows_maintenance_state WHERE id=1),deleted=%s WHERE id=%s AND %s;`, deleted, activityID, meaningful)
			q := fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS gows_maintenance_revision_%s AFTER %s ON gows_messages BEGIN UPDATE gows_maintenance_state SET revision=revision+1,untracked=untracked OR (%s AND COALESCE(json_extract(data,'$.pending'),0)=0 AND NOT EXISTS(SELECT 1 FROM gows_maintenance_history_jobs WHERE acked=FALSE)) WHERE id=1; %s END`, op, op, meaningful, activity)
			if _, err := c.db.ExecContext(ctx, q); err != nil {
				return nil, err
			}
		}
	} else {
		if _, err := c.db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION gows_maintenance_revision() RETURNS TRIGGER AS $$ BEGIN UPDATE gows_maintenance_state SET revision=revision+1,untracked=untracked OR ((CASE WHEN TG_OP='UPDATE' THEN OLD.data::jsonb-'Status' IS DISTINCT FROM NEW.data::jsonb-'Status' ELSE TRUE END) AND COALESCE((data::jsonb->>'pending')::integer,0)=0 AND NOT EXISTS(SELECT 1 FROM gows_maintenance_history_jobs WHERE acked=FALSE)) WHERE id=1; IF TG_OP='DELETE' THEN UPDATE gows_maintenance_activity SET deleted=TRUE,revision=(SELECT revision FROM gows_maintenance_state WHERE id=1) WHERE id=OLD.id; ELSIF TG_OP='INSERT' OR OLD.data::jsonb-'Status' IS DISTINCT FROM NEW.data::jsonb-'Status' THEN UPDATE gows_maintenance_activity SET revision=(SELECT revision FROM gows_maintenance_state WHERE id=1) WHERE id=NEW.id; END IF; RETURN NULL; END; $$ LANGUAGE plpgsql`); err != nil {
			return nil, err
		}
		if _, err := c.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS gows_maintenance_revision ON gows_messages; CREATE TRIGGER gows_maintenance_revision AFTER INSERT OR UPDATE OR DELETE ON gows_messages FOR EACH ROW EXECUTE FUNCTION gows_maintenance_revision()`); err != nil {
			return nil, err
		}
	}

	initial, _ := json.Marshal(maintenanceState{Epoch: uuid.NewString(), Since: time.Now().Unix() + 1})
	inserted, err := c.db.ExecContext(ctx, `INSERT INTO gows_maintenance_state(id,data) VALUES(1,$1) ON CONFLICT(id) DO NOTHING`, string(initial))
	if err != nil {
		return nil, err
	}
	count, err := inserted.RowsAffected()
	if err != nil {
		return nil, err
	}
	needsSeed := count > 0
	err = h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		var untracked bool
		if err := tx.GetContext(ctx, &untracked, `SELECT untracked FROM gows_maintenance_state WHERE id=1`); err != nil {
			return err
		}
		if untracked {
			needsSeed = true
			s.Fault = "untracked_mutation"
			s.Since = time.Now().Unix() + 1
		}
		if _, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_state SET untracked=FALSE WHERE id=1`); err != nil {
			return err
		}
		s.Epoch = uuid.NewString()
		s.Connected = false
		s.Offline = false
		if s.Pending > 0 {
			needsSeed = true
			s.Fault = "storage_interrupted"
			s.Since = time.Now().Unix() + 1
		}
		s.Pending = 0
		return nil
	})
	if err != nil {
		return nil, err
	}
	if needsSeed {
		if err = h.Seed(ctx); err != nil {
			return nil, err
		}
	}
	return h, nil
}

func (h *SQLMaintenanceHistoryStore) change(ctx context.Context, fn func(*sqlx.Tx, *maintenanceState) error) (result error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer func() {
		if result != nil {
			h.fault = true
		}
	}()
	// A FULL commit after the ordinary message writes makes those prior WAL
	// commits durable before a history receipt is allowed onto the network.
	conn, err := h.db.Connx(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	sqlite := h.db.DriverName() == "sqlite3" || h.db.DriverName() == "sqlite"
	if sqlite {
		if _, err = conn.ExecContext(ctx, `PRAGMA synchronous=FULL`); err != nil {
			return err
		}
	}
	tx, err := conn.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE gows_maintenance_state SET revision=revision+1 WHERE id=1`); err != nil {
		return err
	}
	var raw string
	if err = tx.GetContext(ctx, &raw, `SELECT data FROM gows_maintenance_state WHERE id=1`); err != nil {
		return err
	}
	var s maintenanceState
	if err = json.Unmarshal([]byte(raw), &s); err != nil {
		return err
	}
	if err = fn(tx, &s); err != nil {
		return err
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE gows_maintenance_state SET data=$1 WHERE id=1`, string(data)); err != nil {
		return err
	}
	return tx.Commit()
}
func (h *SQLMaintenanceHistoryStore) Connect(ctx context.Context) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		s.Epoch = uuid.NewString()
		s.Connected = true
		s.Offline = false
		return nil
	})
}
func (h *SQLMaintenanceHistoryStore) Disconnect(ctx context.Context) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		s.Epoch = uuid.NewString()
		s.Connected = false
		s.Offline = false
		return nil
	})
}
func (h *SQLMaintenanceHistoryStore) OfflineComplete(ctx context.Context) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Offline = true; return nil })
}
func (h *SQLMaintenanceHistoryStore) Begin(ctx context.Context) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Pending++; return nil })
}
func (h *SQLMaintenanceHistoryStore) Finish(ctx context.Context, storeErr error) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		if s.Pending < 1 {
			return errors.New("maintenance storage registration missing")
		}
		s.Pending--
		if storeErr != nil {
			s.Fault = "storage_failed"
			s.Since = time.Now().Unix() + 1
		}
		return nil
	})
}
func (h *SQLMaintenanceHistoryStore) InvalidateRestore(ctx context.Context) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		s.Since = time.Now().Unix() + 1
		s.Epoch = uuid.NewString()
		s.Offline = false
		s.Fault = "snapshot_restored"
		return nil
	})
}
func (h *SQLMaintenanceHistoryStore) Fail(ctx context.Context, reason string) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		s.Fault = reason
		s.Since = time.Now().Unix() + 1
		return nil
	})
}
