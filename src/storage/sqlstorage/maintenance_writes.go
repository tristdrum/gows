package sqlstorage

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

// Exact identities authorize only the writes registered for this event. A
// pending counter or another unfinished history job never grants SQL authority.
type MaintenanceWrite struct {
	ID        string    `db:"id"`
	JID       string    `db:"jid"`
	Timestamp time.Time `db:"timestamp"`
}

func (h *SQLMaintenanceHistoryStore) BeginMessage(ctx context.Context, msg *events.Message) (writes []MaintenanceWrite, err error) {
	if msg == nil || msg.Info.ID == "" || !maintenanceChat(msg.Info.Chat.String()) || msg.Info.IsNewsletterStatus {
		return nil, errors.New("invalid ordinary maintenance message")
	}
	original := MaintenanceWrite{ID: msg.Info.ID, JID: msg.Info.Chat.String(), Timestamp: msg.Info.Timestamp}
	edit, err := decodeMessageEdit(&storage.StoredMessage{Message: msg})
	if err != nil {
		return nil, err
	}
	target := ""
	if edit != nil {
		target = edit.GetKey().GetID()
	}
	if p := msg.Message.GetProtocolMessage(); p != nil && p.GetType() == waE2E.ProtocolMessage_REVOKE {
		target = p.GetKey().GetID()
	}
	err = h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		writes = []MaintenanceWrite{original}
		if target != "" {
			var prior MaintenanceWrite
			readErr := tx.GetContext(ctx, &prior, `SELECT id,jid,timestamp FROM gows_messages WHERE id=$1`, target)
			if readErr != nil && !errors.Is(readErr, sql.ErrNoRows) {
				return readErr
			}
			if readErr == nil && maintenanceChat(prior.JID) {
				if target == original.ID {
					writes = []MaintenanceWrite{prior}
				} else {
					writes = append(writes, prior)
				}
			}
		}
		for _, write := range writes {
			if _, err := tx.ExecContext(ctx, `INSERT INTO gows_maintenance_writes(id,jid,timestamp,depth) VALUES($1,$2,$3,1) ON CONFLICT(id,jid,timestamp) DO UPDATE SET depth=gows_maintenance_writes.depth+1`, write.ID, write.JID, write.Timestamp); err != nil {
				return err
			}
		}
		// Record ordinary identity before a concurrent deletion can remove content.
		// The later observer supplies checked PN/LID mapping; failures remain gaps.
		if storage.IsRealMessage(msg) && edit == nil {
			known := msg.Info.Chat.Server != "lid"
			if _, err := tx.ExecContext(ctx, `INSERT INTO gows_maintenance_activity(id,provider_jid,jid,timestamp,from_me,identity_known,revision) VALUES($1,$2,$2,$3,$4,$5,(SELECT revision FROM gows_maintenance_state WHERE id=1)) ON CONFLICT(id) DO NOTHING`, original.ID, original.JID, msg.Info.Timestamp.Unix(), msg.Info.IsFromMe, known); err != nil {
				return err
			}
		}
		s.Pending++
		return nil
	})
	return writes, err
}

func (h *SQLMaintenanceHistoryStore) FinishMessage(ctx context.Context, writes []MaintenanceWrite, storeErr error) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		if s.Pending < 1 || len(writes) == 0 {
			return errors.New("maintenance message registration missing")
		}
		for _, write := range writes {
			result, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_writes SET depth=depth-1 WHERE id=$1 AND jid=$2 AND timestamp=$3 AND depth>0`, write.ID, write.JID, write.Timestamp)
			if err != nil {
				return err
			}
			count, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if count != 1 {
				return errors.New("maintenance write identity registration missing")
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM gows_maintenance_writes WHERE depth=0`); err != nil {
			return err
		}
		s.Pending--
		if storeErr != nil {
			s.Fault = "storage_failed"
			s.Since = time.Now().Unix() + 1
		}
		return nil
	})
}
