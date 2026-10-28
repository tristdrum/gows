package sqlstorage

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

func statusSource(msg *storage.StoredMessage) storage.StatusSource {
	stamp := msg.Info.Timestamp.UnixMilli()
	expires := stamp + storage.StatusRetentionMS
	var info *waE2E.ContextInfo
	if msg.Message != nil && msg.Message.Message != nil {
		content := msg.Message.Message
		switch {
		case content.ImageMessage != nil:
			info = content.ImageMessage.GetContextInfo()
		case content.VideoMessage != nil:
			info = content.VideoMessage.GetContextInfo()
		case content.AudioMessage != nil:
			info = content.AudioMessage.GetContextInfo()
		case content.ExtendedTextMessage != nil:
			info = content.ExtendedTextMessage.GetContextInfo()
		}
	}
	if duration := info.GetExpiration(); duration > 0 && int64(duration)*1000 < storage.StatusRetentionMS {
		expires = stamp + int64(duration)*1000
	}
	return storage.StatusSource{ID: msg.Info.ID, Chat: msg.Info.Chat.String(), Sender: msg.Info.Sender.ToNonAD().String(), FromMe: msg.Info.IsFromMe, TimestampMS: stamp, ExpiresAtMS: expires}
}
func validStatusMessage(msg *storage.StoredMessage) bool {
	if msg == nil || msg.Message == nil || msg.Info.Chat != types.StatusBroadcastJID || msg.Info.ID == "" || len(msg.Info.ID) > 100 || msg.Info.Timestamp.IsZero() {
		return false
	}
	sender := msg.Info.Sender.ToNonAD()
	return sender.User != "" && (sender.Server == types.DefaultUserServer || sender.Server == types.HiddenUserServer)
}

func (s *SQLStatusHistoryStore) Capture(ctx context.Context, msg *storage.StoredMessage, ownPhone string, receivedAtMS int64) error {
	tx, rec, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.now().UnixMilli()
	if !rec.Policy.Enabled || rec.Policy.AuthorizedUntilMS <= now {
		return tx.Commit()
	}
	if rec.Policy.ExpectedPhone != ownPhone {
		return storage.ErrStatusBinding
	}
	if !validStatusMessage(msg) {
		statusGap(rec, now, "invalid_status_provenance")
		closeStatusGap(rec, now)
		if err = saveStatusPolicy(ctx, tx, rec); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		return storage.ErrStatusData
	}
	if rec.Capture.StartedAtMS == nil || receivedAtMS < *rec.Capture.StartedAtMS {
		return tx.Commit()
	}
	source := statusSource(msg)
	if source.TimestampMS > now+5000 || source.ExpiresAtMS <= now {
		return tx.Commit()
	}
	if rec.Capture.StartedAtMS == nil || source.TimestampMS < (*rec.Capture.StartedAtMS/1000)*1000 {
		return tx.Commit()
	}
	if source.FromMe && msg.Info.Sender.User != rec.Policy.ExpectedPhone {
		alt := msg.Info.SenderAlt.ToNonAD()
		if msg.Info.Sender.Server != types.HiddenUserServer || alt.Server != types.DefaultUserServer || alt.User != rec.Policy.ExpectedPhone {
			return storage.ErrStatusBinding
		}
	}
	protocol := msg.Message.Message.GetProtocolMessage()
	if protocol != nil && protocol.GetType() == waE2E.ProtocolMessage_REVOKE {
		key := protocol.GetKey()
		id := key.GetID()
		if id == "" || len(id) > 100 || (key.GetRemoteJID() != "" && key.GetRemoteJID() != "status@broadcast") {
			return storage.ErrStatusData
		}
		sender := source.Sender
		if key.GetParticipant() != "" {
			jid, parseErr := types.ParseJID(key.GetParticipant())
			if parseErr != nil || jid.ToNonAD().String() != sender {
				return storage.ErrStatusData
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO gows_status_tombstones(id,sender,from_me,expires_at_ms)
   VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET expires_at_ms=EXCLUDED.expires_at_ms
   WHERE gows_status_tombstones.sender=EXCLUDED.sender AND gows_status_tombstones.from_me=EXCLUDED.from_me`, id, sender, source.FromMe, now+storage.StatusRetentionMS); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM gows_messages WHERE jid='status@broadcast' AND id IN
   (SELECT id FROM gows_status_metadata WHERE id=$1 AND sender=$2 AND from_me=$3)`, id, sender, source.FromMe); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM gows_status_metadata WHERE id=$1 AND sender=$2 AND from_me=$3", id, sender, source.FromMe); err != nil {
			return err
		}
		return tx.Commit()
	}
	if !storage.IsRealMessage(msg.Message) {
		return tx.Commit()
	}
	var exists int
	err = tx.GetContext(ctx, &exists, "SELECT 1 FROM gows_status_tombstones WHERE id=$1 AND expires_at_ms>$2", source.ID, now)
	if err == nil {
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// Existing accepted content wins over duplicate replays; importantly its original
	// timestamp/expiry cannot be extended by a later copy or a receipt.
	var prior string
	err = tx.GetContext(ctx, &prior, "SELECT sender FROM gows_status_metadata WHERE id=$1", source.ID)
	if err == nil {
		if prior != source.Sender {
			return storage.ErrStatusData
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	value := *msg
	value.IsReal = true
	data, err := messageMapper.Marshal(&value)
	if err != nil {
		return storage.ErrStatusData
	}
	// Never allow a Status identity collision to overwrite an ordinary chat row.
	result, err := tx.ExecContext(ctx, `INSERT INTO gows_messages(id,jid,timestamp,from_me,is_real,data)
  VALUES($1,'status@broadcast',$2,$3,true,$4) ON CONFLICT(id) DO NOTHING`, source.ID, time.UnixMilli(source.TimestampMS).UTC(), source.FromMe, string(data))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return storage.ErrStatusData
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO gows_status_metadata(id,sender,from_me,timestamp_ms,expires_at_ms,generation)
  VALUES($1,$2,$3,$4,$5,$6)`, source.ID, source.Sender, source.FromMe, source.TimestampMS, source.ExpiresAtMS, rec.Capture.Generation)
	if err != nil {
		return err
	}
	return tx.Commit()
}
