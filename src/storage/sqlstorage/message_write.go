package sqlstorage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
)

const messageWriteAttempts = 32

// UpsertOneMessage keeps content and its query projections in one atomic write.
// Message delivery and history replay may race each other and receipt handling.
func (s SqlMessageStore) UpsertOneMessage(msg *storage.StoredMessage) error {
	if msg == nil || msg.Message == nil || msg.Info.ID == "" {
		return errors.New("cannot store a message without its provider identity")
	}
	incoming := *msg
	incoming.IsReal = storage.IsRealMessage(incoming.Message)
	data, err := messageMapper.Marshal(&incoming)
	if err != nil {
		return err
	}
	result, err := s.db.Exec(`INSERT INTO gows_messages (id, jid, from_me, timestamp, is_real, data)
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING`,
		incoming.Info.ID, incoming.Info.Chat, incoming.Info.IsFromMe, incoming.Info.Timestamp, incoming.IsReal, string(data))
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil || inserted == 1 {
		return err
	}
	return s.updateStoredMessage(incoming.Info.ID, func(current *storage.StoredMessage, raw []byte) (*storage.StoredMessage, []byte, error) {
		accepted, acceptedData := &incoming, data
		// An incomplete duplicate cannot replace real content. Likewise replaying
		// an original message must not undo an authoritative normalized edit.
		if current.IsReal && (!incoming.IsReal || messageWasEdited(current) && !messageWasEdited(&incoming)) {
			accepted, acceptedData = current, raw
		}
		status := maximumMessageStatus(current.Status, incoming.Status)
		updated, err := patchMessageState(acceptedData, status, accepted.IsReal)
		return accepted, updated, err
	})
}

// UpdateMessageStatus merges a receipt into the current stored payload, never
// the stale message snapshot that a receipt handler might have read earlier.
func (s SqlMessageStore) UpdateMessageStatus(id types.MessageID, status storage.Status) error {
	return s.updateStoredMessage(id, func(current *storage.StoredMessage, raw []byte) (*storage.StoredMessage, []byte, error) {
		updated, err := patchMessageState(raw, maximumMessageStatus(current.Status, &status), current.IsReal)
		return current, updated, err
	})
}

type messageChange func(*storage.StoredMessage, []byte) (*storage.StoredMessage, []byte, error)

// Compare the complete observed payload so an interleaved receipt, edit or
// duplicate forces a fresh merge. A concurrent delete returns ErrNotFound;
// neither this loop nor the receipt path ever inserts a deleted row.
func (s SqlMessageStore) updateStoredMessage(id types.MessageID, change messageChange) error {
	for attempt := 0; attempt < messageWriteAttempts; attempt++ {
		var raw string
		if err := s.db.Get(&raw, "SELECT data FROM gows_messages WHERE id = $1", id); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return storage.ErrNotFound
			}
			return err
		}
		var current storage.StoredMessage
		if err := messageMapper.Unmarshal([]byte(raw), &current); err != nil {
			return fmt.Errorf("cannot update malformed stored message %s: %w", id, err)
		}
		if current.Info.ID != id {
			return fmt.Errorf("stored message identity does not match row %s", id)
		}
		// Historical IsReal fields and indexed columns can disagree. Derive
		// both from the accepted structured content, not either stale value.
		current.IsReal = storage.IsRealMessage(current.Message)
		accepted, data, err := change(&current, []byte(raw))
		if err != nil {
			return err
		}
		result, err := s.db.Exec(`UPDATE gows_messages
			SET jid = $1, from_me = $2, timestamp = $3, is_real = $4, data = $5
			WHERE id = $6 AND data = $7`,
			accepted.Info.Chat, accepted.Info.IsFromMe, accepted.Info.Timestamp,
			accepted.IsReal, string(data), id, raw)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed == 1 {
			return err
		}
	}
	return fmt.Errorf("message %s changed during %d write attempts", id, messageWriteAttempts)
}

func maximumMessageStatus(first, second *storage.Status) storage.Status {
	status := storage.StatusError
	if first != nil && *first > status {
		status = *first
	}
	if second != nil && *second > status {
		status = *second
	}
	return status
}

func messageWasEdited(msg *storage.StoredMessage) bool {
	return msg.IsEdit || msg.NewsletterMeta != nil && !msg.NewsletterMeta.EditTS.IsZero()
}

// Preserve the selected raw payload, including future protobuf and top-level
// fields discarded by the compatibility decoder. Only stored state is changed.
func patchMessageState(raw []byte, status storage.Status, isReal bool) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	payload["Status"], _ = json.Marshal(status)
	payload["IsReal"], _ = json.Marshal(isReal)
	return json.Marshal(payload)
}
