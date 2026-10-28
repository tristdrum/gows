package sqlstorage

import (
	"context"
	"database/sql"
	"errors"

	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
)

type statusRow struct {
	ID          string         `db:"id"`
	Sender      string         `db:"sender"`
	FromMe      bool           `db:"from_me"`
	TimestampMS int64          `db:"timestamp_ms"`
	ExpiresAtMS int64          `db:"expires_at_ms"`
	Data        sql.NullString `db:"data"`
}

const statusSelect = `SELECT m.id,m.sender,m.from_me,m.timestamp_ms,m.expires_at_ms,g.data
 FROM gows_status_metadata m LEFT JOIN gows_messages g ON g.id=m.id AND g.jid='status@broadcast'`

func statusEntry(row statusRow) (storage.StatusEntry, error) {
	var msg storage.StoredMessage
	if !row.Data.Valid || messageMapper.Unmarshal([]byte(row.Data.String), &msg) != nil || !validStatusMessage(&msg) {
		return storage.StatusEntry{}, storage.ErrStatusData
	}
	source := statusSource(&msg)
	if source.ID != row.ID || source.Sender != row.Sender || source.FromMe != row.FromMe || source.TimestampMS != row.TimestampMS || source.ExpiresAtMS != row.ExpiresAtMS {
		return storage.StatusEntry{}, storage.ErrStatusData
	}
	return storage.StatusEntry{Message: &msg, Source: source}, nil
}
func (s *SQLStatusHistoryStore) List(ctx context.Context, q storage.StatusHistoryQuery) (*storage.StatusHistoryPage, error) {
	now := s.now().UnixMilli()
	if q.Limit < 1 || q.Limit > 100 || q.SinceMS < 0 || q.UntilMS <= q.SinceMS || q.UntilMS-q.SinceMS > storage.StatusRetentionMS+1000 || q.UntilMS > now+1000 {
		return nil, storage.ErrStatusInvalid
	}
	if q.Before != nil && (q.Before.ID == "" || len(q.Before.ID) > 100 || q.Before.TimestampMS < q.SinceMS || q.Before.TimestampMS >= q.UntilMS || q.Generation == "") {
		return nil, storage.ErrStatusInvalid
	}
	tx, rec, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now = s.now().UnixMilli()
	if rec.Policy.ObservedAtMS > 0 && rec.Policy.StatusBinding != q.StatusBinding {
		return nil, storage.ErrStatusBinding
	}
	if q.Generation != "" && q.Generation != rec.Capture.Generation {
		return nil, storage.ErrStatusGeneration
	}
	page := &storage.StatusHistoryPage{Messages: []storage.StatusEntry{}, Capture: statusCapture(rec, now), SourcePage: storage.StatusPageSource{Version: 1, SinceMS: q.SinceMS, UntilMS: q.UntilMS, Limit: q.Limit, Complete: true}}
	if !rec.Policy.Enabled || rec.Policy.AuthorizedUntilMS <= now {
		return page, tx.Commit()
	}
	beforeMS := q.UntilMS
	beforeID := ""
	if q.Before != nil {
		beforeMS = q.Before.TimestampMS
		beforeID = q.Before.ID
	}
	rows := []statusRow{}
	err = tx.SelectContext(ctx, &rows, statusSelect+` WHERE m.expires_at_ms>$1 AND m.timestamp_ms>=$2 AND m.timestamp_ms<$3
  AND (m.timestamp_ms<$4 OR (m.timestamp_ms=$4 AND m.id<$5))
  AND NOT EXISTS(SELECT 1 FROM gows_status_tombstones t WHERE t.id=m.id AND t.expires_at_ms>$1)
  ORDER BY m.timestamp_ms DESC,m.id DESC LIMIT $6`, now, q.SinceMS, q.UntilMS, beforeMS, beforeID, q.Limit+1)
	if err != nil {
		return nil, err
	}
	page.SourcePage.Complete = len(rows) <= int(q.Limit)
	if !page.SourcePage.Complete {
		rows = rows[:q.Limit]
	}
	for _, row := range rows {
		page.SourcePage.ScannedCount++
		entry, err := statusEntry(row)
		if err != nil {
			page.SourcePage.UnavailableCount++
		} else {
			page.Messages = append(page.Messages, entry)
		}
		if !page.SourcePage.Complete {
			page.SourcePage.NextCursor = &storage.StatusCursor{TimestampMS: row.TimestampMS, ID: row.ID}
		}
	}
	return page, tx.Commit()
}
func (s *SQLStatusHistoryStore) Exact(ctx context.Context, q storage.StatusExact) (*storage.StatusHistoryResult, error) {
	if q.Generation == "" || q.ID == "" || len(q.ID) > 100 || q.Sender == "" || len(q.Sender) > 100 {
		return nil, storage.ErrStatusInvalid
	}
	tx, rec, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := s.now().UnixMilli()
	if err = statusAuthorized(rec, q.StatusBinding, now); err != nil {
		return nil, err
	}
	if q.Generation != rec.Capture.Generation {
		return nil, storage.ErrStatusGeneration
	}
	entry, err := exactStatus(ctx, tx, q, now)
	if err != nil {
		return nil, err
	}
	return &storage.StatusHistoryResult{StatusEntry: entry, Capture: statusCapture(rec, now)}, tx.Commit()
}
func exactStatus(ctx context.Context, tx *sqlx.Tx, q storage.StatusExact, now int64) (storage.StatusEntry, error) {
	var row statusRow
	err := tx.GetContext(ctx, &row, statusSelect+` WHERE m.id=$1 AND m.sender=$2 AND m.from_me=$3 AND m.expires_at_ms>$4
  AND NOT EXISTS(SELECT 1 FROM gows_status_tombstones t WHERE t.id=m.id AND t.expires_at_ms>$4)`, q.ID, q.Sender, q.FromMe, now)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.StatusEntry{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.StatusEntry{}, err
	}
	return statusEntry(row)
}
