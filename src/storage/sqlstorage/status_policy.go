package sqlstorage

import (
	"context"
	"encoding/json"
	"regexp"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type SQLStatusHistoryStore struct {
	db  *sqlx.DB
	now func() time.Time
}
type statusPolicyRecord struct {
	Policy  storage.StatusPolicy  `json:"policy"`
	Capture storage.StatusCapture `json:"capture"`
}

var statusPhone = regexp.MustCompile(`^[0-9]{5,20}$`)

func (gc *GContainer) NewStatusHistoryStorage() *SQLStatusHistoryStore {
	return &SQLStatusHistoryStore{db: gc.db, now: time.Now}
}

func (s *SQLStatusHistoryStore) begin(ctx context.Context) (*sqlx.Tx, *statusPolicyRecord, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	return lockStatusPolicy(ctx, tx)
}

func lockStatusPolicy(ctx context.Context, tx *sqlx.Tx) (*sqlx.Tx, *statusPolicyRecord, error) {
	empty := `{"policy":{},"capture":{"state":"disabled","started_at_ms":null,"generation":"","authorized_until_ms":0,"acknowledged_at_ms":0,"gaps":[]}}`
	if _, err := tx.ExecContext(ctx, "INSERT INTO gows_status_capture(id,data) VALUES (1,$1) ON CONFLICT(id) DO NOTHING", empty); err != nil {
		tx.Rollback()
		return nil, nil, err
	}
	// A write before reading establishes a single cross-handle serialization boundary
	// for both SQLite and PostgreSQL, including policy, reads, writes and pruning.
	if _, err := tx.ExecContext(ctx, "UPDATE gows_status_capture SET data=data WHERE id=1"); err != nil {
		tx.Rollback()
		return nil, nil, err
	}
	var raw string
	if err := tx.GetContext(ctx, &raw, "SELECT data FROM gows_status_capture WHERE id=1"); err != nil {
		tx.Rollback()
		return nil, nil, err
	}
	var rec statusPolicyRecord
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		tx.Rollback()
		return nil, nil, storage.ErrStatusData
	}
	return tx, &rec, nil
}
func saveStatusPolicy(ctx context.Context, tx *sqlx.Tx, rec *statusPolicyRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE gows_status_capture SET data=$1 WHERE id=1", string(raw))
	return err
}
func statusCapture(rec *statusPolicyRecord, now int64) storage.StatusCapture {
	c := rec.Capture
	if !rec.Policy.Enabled {
		c.State = "disabled"
	} else if rec.Policy.AuthorizedUntilMS <= now {
		c.State = "expired"
	} else {
		c.State = "active"
	}
	c.AuthorizedUntilMS = rec.Policy.AuthorizedUntilMS
	if rec.Policy.Enabled && rec.Policy.AuthorizedUntilMS > 0 && rec.Policy.AuthorizedUntilMS <= now {
		gaps := append([]storage.StatusGap{}, c.Gaps...)
		if len(gaps) == 0 || gaps[len(gaps)-1].UntilMS != nil {
			gaps = append(gaps, storage.StatusGap{SinceMS: rec.Policy.AuthorizedUntilMS, Reason: "authorization_expired"})
		}
		c.Gaps = gaps
	}
	if c.Gaps == nil {
		c.Gaps = []storage.StatusGap{}
	}
	return c
}
func statusGap(rec *statusPolicyRecord, since int64, reason string) {
	if since <= 0 {
		return
	}
	n := len(rec.Capture.Gaps)
	if n > 0 && rec.Capture.Gaps[n-1].UntilMS == nil {
		return
	}
	rec.Capture.Gaps = append(rec.Capture.Gaps, storage.StatusGap{SinceMS: since, Reason: reason})
	if len(rec.Capture.Gaps) > 32 {
		rec.Capture.Gaps = rec.Capture.Gaps[len(rec.Capture.Gaps)-32:]
	}
}
func closeStatusGap(rec *statusPolicyRecord, now int64) {
	n := len(rec.Capture.Gaps)
	if n > 0 && rec.Capture.Gaps[n-1].UntilMS == nil {
		rec.Capture.Gaps[n-1].UntilMS = &now
	}
}
func (s *SQLStatusHistoryStore) Configure(ctx context.Context, policy storage.StatusPolicy) (capture storage.StatusCapture, result error) {
	now := s.now().UnixMilli()
	if policy.AuthorizationSubject == "" || len(policy.AuthorizationSubject) > 100 || policy.AccountID == "" || len(policy.AccountID) > 100 || policy.ShardID == "" || len(policy.ShardID) > 100 || !statusPhone.MatchString(policy.ExpectedPhone) || policy.ObservedAtMS <= 0 || policy.ObservedAtMS > now+5000 {
		return storage.StatusCapture{}, storage.ErrStatusInvalid
	}
	if policy.Enabled && (policy.AuthorizedUntilMS <= now || policy.AuthorizedUntilMS > policy.ObservedAtMS+180000 || policy.AuthorizedUntilMS > now+180000) {
		return storage.StatusCapture{}, storage.ErrStatusInvalid
	}
	if !policy.Enabled {
		policy.AuthorizedUntilMS = 0
	}
	tx, rec, finish, err := s.beginPolicy(ctx, true)
	if err != nil {
		return storage.StatusCapture{}, err
	}
	defer finishStatusPolicy(finish, &result)
	now = s.now().UnixMilli()
	if policy.Enabled && policy.AuthorizedUntilMS <= now {
		return storage.StatusCapture{}, storage.ErrStatusInvalid
	}
	if rec.Policy.ObservedAtMS > policy.ObservedAtMS || rec.Policy.Epoch > policy.Epoch {
		return storage.StatusCapture{}, storage.ErrStatusStaleControl
	}
	if rec.Policy.ObservedAtMS == policy.ObservedAtMS {
		if rec.Policy != policy {
			return storage.StatusCapture{}, storage.ErrStatusStaleControl
		}
		return statusCapture(rec, now), tx.Commit()
	}
	changed := rec.Policy.StatusBinding != policy.StatusBinding
	if rec.Policy.ObservedAtMS > 0 && changed && policy.Epoch <= rec.Policy.Epoch && (rec.Policy.AccountID != policy.AccountID || rec.Policy.ExpectedPhone != policy.ExpectedPhone || rec.Policy.ShardID != policy.ShardID) {
		return storage.StatusCapture{}, storage.ErrStatusBinding
	}
	wasActive := rec.Policy.Enabled && rec.Policy.AuthorizedUntilMS > now
	if rec.Policy.Enabled && !wasActive {
		statusGap(rec, rec.Policy.AuthorizedUntilMS, "authorization_expired")
	}
	if !policy.Enabled || changed || !wasActive {
		if err = purgeStatusContent(ctx, tx); err != nil {
			return storage.StatusCapture{}, err
		}
	}
	if policy.Enabled && (!wasActive || changed) {
		closeStatusGap(rec, now)
		rec.Capture.Generation = uuid.NewString()
		start := now
		rec.Capture.StartedAtMS = &start
		// WhatsApp timestamps may have only whole-second resolution. Do not
		// admit ambiguous pre-authorization content from the partial first second.
		if boundary := ((now + 999) / 1000) * 1000; boundary > now {
			statusGap(rec, now, "timestamp_resolution")
			closeStatusGap(rec, boundary)
		}
	}
	if !policy.Enabled && rec.Policy.Enabled {
		statusGap(rec, now, "capture_disabled")
	}
	rec.Policy = policy
	rec.Capture.AcknowledgedAtMS = now
	if err = saveStatusPolicy(ctx, tx, rec); err != nil {
		return storage.StatusCapture{}, err
	}
	return statusCapture(rec, now), tx.Commit()
}
func statusAuthorized(rec *statusPolicyRecord, binding storage.StatusBinding, now int64) error {
	if rec.Policy.StatusBinding != binding {
		return storage.ErrStatusBinding
	}
	if !rec.Policy.Enabled || rec.Policy.AuthorizedUntilMS <= now {
		return storage.ErrStatusUnauthorized
	}
	return nil
}
func (s *SQLStatusHistoryStore) Restart(ctx context.Context) error {
	tx, rec, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.now().UnixMilli()
	if !rec.Policy.Enabled || rec.Policy.AuthorizedUntilMS <= now {
		if rec.Policy.Enabled {
			statusGap(rec, rec.Policy.AuthorizedUntilMS, "authorization_expired")
		}
		if err = purgeStatusContent(ctx, tx); err != nil {
			return err
		}
	} else {
		// A normal process restart does not consume a still-valid authorization
		// or delete its unexpired buffer. Coverage since the last acknowledged
		// control is conservatively uncertain; do not imply uninterrupted capture.
		statusGap(rec, rec.Capture.AcknowledgedAtMS, "session_restart")
		closeStatusGap(rec, now)
	}
	if err = pruneStatuses(ctx, tx, now); err != nil {
		return err
	}
	if err = saveStatusPolicy(ctx, tx, rec); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *SQLStatusHistoryStore) Prune(ctx context.Context) error {
	tx, rec, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.now().UnixMilli()
	if rec.Policy.Enabled && rec.Policy.AuthorizedUntilMS <= now {
		statusGap(rec, rec.Policy.AuthorizedUntilMS, "authorization_expired")
	}
	if !rec.Policy.Enabled || rec.Policy.AuthorizedUntilMS <= now {
		if err = purgeStatusContent(ctx, tx); err != nil {
			return err
		}
	}
	if err = pruneStatuses(ctx, tx, now); err != nil {
		return err
	}
	if err = saveStatusPolicy(ctx, tx, rec); err != nil {
		return err
	}
	return tx.Commit()
}
func purgeStatusContent(ctx context.Context, tx *sqlx.Tx) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM gows_messages WHERE jid='status@broadcast'"); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "DELETE FROM gows_status_metadata")
	return err
}
func pruneStatuses(ctx context.Context, tx *sqlx.Tx, now int64) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM gows_messages WHERE jid='status@broadcast' AND
  (id NOT IN (SELECT id FROM gows_status_metadata) OR id IN (SELECT id FROM gows_status_metadata WHERE expires_at_ms<=$1))`, now)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM gows_status_metadata WHERE expires_at_ms<=$1", now); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "DELETE FROM gows_status_tombstones WHERE expires_at_ms<=$1", now)
	return err
}
