package sqlstorage

import (
	"context"

	"github.com/google/uuid"
)

// InvalidateRestore drops only the Status buffer from a restored snapshot. A
// snapshot can predate a revoke or grant removal while retaining a valid lease;
// its tombstones and binding therefore cannot prove the old content is safe.
func (s *SQLStatusHistoryStore) InvalidateRestore(ctx context.Context) (result error) {
	tx, rec, finish, err := s.beginPolicy(ctx, true)
	if err != nil {
		return err
	}
	defer finishStatusPolicy(finish, &result)
	now := s.now().UnixMilli()
	if err = purgeStatusContent(ctx, tx); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM gows_status_tombstones"); err != nil {
		return err
	}
	closeStatusGap(rec, now)
	since := rec.Capture.AcknowledgedAtMS
	if since <= 0 || since > now {
		since = now
	}
	statusGap(rec, since, "snapshot_restored")
	rec.Policy.Enabled = false
	rec.Policy.AuthorizedUntilMS = 0
	// Wall clock observation fences controls constructed before this startup.
	if rec.Policy.ObservedAtMS < now {
		rec.Policy.ObservedAtMS = now
	}
	rec.Capture.Generation = uuid.NewString()
	rec.Capture.StartedAtMS = nil
	rec.Capture.AcknowledgedAtMS = now
	if err = saveStatusPolicy(ctx, tx, rec); err != nil {
		return err
	}
	return tx.Commit()
}
