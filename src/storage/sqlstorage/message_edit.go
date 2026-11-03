package sqlstorage

import (
	"errors"
	"fmt"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// Only actual provider edit protocols select a target and an ordering version.
// IsEdit without its protocol is insufficient to insert or replace content.
func decodeMessageEdit(msg *storage.StoredMessage) (*waE2E.ProtocolMessage, error) {
	var selected *waE2E.ProtocolMessage
	for _, candidate := range []*waE2E.Message{msg.Message.Message, msg.RawMessage, msg.SourceWebMsg.GetMessage()} {
		protocol, err := editProtocol(candidate)
		if err != nil {
			return nil, err
		}
		if protocol == nil {
			continue
		}
		if selected != nil && !proto.Equal(selected, protocol) {
			return nil, errors.New("conflicting message edit protocol evidence")
		}
		selected = protocol
	}
	if selected == nil {
		if msg.IsEdit || msg.Info.Edit == types.EditAttributeMessageEdit {
			return nil, errors.New("message edit has no verifiable protocol")
		}
		return nil, nil
	}
	if msg.IsViewOnce || msg.IsViewOnceV2 || msg.IsViewOnceV2Extension || msg.Info.IsNewsletterStatus ||
		msg.Info.Chat.Server == types.NewsletterServer || msg.Info.Chat.Server == types.BroadcastServer {
		return nil, errors.New("unsupported message edit source")
	}
	key := selected.GetKey()
	if key.GetID() == "" || key.FromMe == nil || !key.GetFromMe() || selected.GetTimestampMS() <= 0 || selected.GetEditedMessage() == nil {
		return nil, errors.New("message edit has incomplete target or version")
	}
	remote, err := types.ParseJID(key.GetRemoteJID())
	if err != nil || remote.IsEmpty() {
		return nil, errors.New("message edit has no valid remote identity")
	}
	// The nested key uses the editor's perspective: incoming direct edits name
	// this account as their recipient. Authenticated event identity selects the
	// row; the nested key must never redirect it to that recipient's chat.
	if msg.Info.IsGroup || msg.Info.IsFromMe {
		if remote.ToNonAD() != msg.Info.Chat.ToNonAD() {
			return nil, errors.New("message edit remote identity mismatch")
		}
	} else if remote.Server != types.DefaultUserServer && remote.Server != types.HiddenUserServer {
		return nil, errors.New("incoming direct edit has unsupported remote identity")
	}
	if key.GetParticipant() != "" {
		participant, err := types.ParseJID(key.GetParticipant())
		if err != nil || !sameSender(participant, msg.Info.Sender, msg.Info.SenderAlt) {
			return nil, errors.New("message edit participant mismatch")
		}
	}
	return selected, nil
}

func editProtocol(content *waE2E.Message) (*waE2E.ProtocolMessage, error) {
	for depth := 0; content != nil && depth < 8; depth++ {
		if protocol := content.GetProtocolMessage(); protocol != nil {
			if protocol.GetType() == waE2E.ProtocolMessage_MESSAGE_EDIT {
				return protocol, nil
			}
			return nil, nil
		}
		switch {
		case content.ViewOnceMessage != nil, content.ViewOnceMessageV2 != nil, content.ViewOnceMessageV2Extension != nil:
			// Ordinary view-once storage is unchanged. Never use its wrapped
			// contents as edit authority; an IsEdit event will fail closed.
			return nil, nil
		case content.DeviceSentMessage != nil:
			content = content.GetDeviceSentMessage().GetMessage()
		case content.EphemeralMessage != nil:
			content = content.GetEphemeralMessage().GetMessage()
		case content.EditedMessage != nil:
			content = content.GetEditedMessage().GetMessage()
		default:
			return nil, nil
		}
	}
	return nil, nil
}

func sameSender(sender types.JID, candidates ...types.JID) bool {
	if sender.IsEmpty() {
		return false
	}
	for _, candidate := range candidates {
		if !candidate.IsEmpty() && sender.ToNonAD() == candidate.ToNonAD() {
			return true
		}
	}
	return false
}

func (s SqlMessageStore) applyMessageEdit(incoming *storage.StoredMessage, edit *waE2E.ProtocolMessage) error {
	target := edit.GetKey().GetID()
	return s.updateStoredMessage(target, func(current *storage.StoredMessage, raw []byte) (*storage.StoredMessage, []byte, error) {
		if current.Info.Chat.ToNonAD() != incoming.Info.Chat.ToNonAD() || current.Info.IsFromMe != incoming.Info.IsFromMe ||
			current.Info.IsGroup != incoming.Info.IsGroup ||
			!sameSender(current.Info.Sender, incoming.Info.Sender, incoming.Info.SenderAlt) &&
				!sameSender(current.Info.SenderAlt, incoming.Info.Sender, incoming.Info.SenderAlt) {
			return nil, nil, errors.New("message edit source does not match stored original")
		}
		if !current.IsReal || current.IsViewOnce || current.IsViewOnceV2 || current.IsViewOnceV2Extension || current.Info.IsNewsletterStatus {
			return nil, nil, errors.New("stored original is not an editable history message")
		}
		if edit.GetTimestampMS() < current.Info.Timestamp.UnixMilli() {
			return nil, nil, errors.New("message edit predates its original")
		}
		if current.MinEdit != nil {
			if current.MinEdit.Version != 1 || current.MinEdit.EditedAtMS <= 0 || current.MinEdit.OriginalTimestampMS < 0 ||
				current.MinEdit.OriginalTimestampMS > 0 && current.MinEdit.OriginalTimestampMS != current.Info.Timestamp.UnixMilli() {
				return nil, nil, errors.New("stored edit ordering metadata is invalid")
			}
			if edit.GetTimestampMS() <= current.MinEdit.EditedAtMS {
				// Keep the first representation for equal versions. Arrival order
				// cannot promote an older edit or reinterpret a duplicate.
				return current, raw, nil
			}
		}
		updated, err := patchMessageEdit(raw, edit.GetEditedMessage(), &storage.MessageEditMetadata{
			Version: 1, EditedAtMS: edit.GetTimestampMS(), OriginalTimestampMS: originalMessageTime(current),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("cannot merge message edit: %w", err)
		}
		updated, err = patchMessageState(updated, maximumMessageStatus(current.Status, incoming.Status), true)
		return current, updated, err
	})
}

func originalMessageTime(current *storage.StoredMessage) int64 {
	if current.MinEdit != nil {
		return current.MinEdit.OriginalTimestampMS
	}
	if current.IsEdit || current.Info.Edit == types.EditAttributeMessageEdit || current.Info.Timestamp.IsZero() {
		return 0
	}
	return current.Info.Timestamp.UnixMilli()
}
