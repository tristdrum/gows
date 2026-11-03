package sqlstorage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func editOriginal(content *waE2E.Message) *storage.StoredMessage {
	msg := integrityMessage(content, storage.StatusRead)
	msg.Info.IsGroup = true
	msg.Info.IsFromMe = true
	msg.Info.Sender = types.NewJID("27760000010", types.DefaultUserServer)
	msg.RawMessage = proto.Clone(content).(*waE2E.Message)
	return msg
}

func editEvent(original *storage.StoredMessage, content *waE2E.Message, milliseconds int64, normalized bool) *storage.StoredMessage {
	event := &events.Message{Info: original.Info}
	event.Info.ID = uuid.NewString()
	event.Info.Timestamp = time.UnixMilli(milliseconds)
	event.Info.Sender.Device = 4
	protocol := &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(), TimestampMS: proto.Int64(milliseconds),
		Key:           &waCommon.MessageKey{ID: proto.String(original.Info.ID), RemoteJID: proto.String(original.Info.Chat.String()), FromMe: proto.Bool(true)},
		EditedMessage: content,
	}}
	event.RawMessage = &waE2E.Message{EditedMessage: &waE2E.FutureProofMessage{Message: protocol}}
	event.Message = protocol
	if normalized {
		event.Info.ID = original.Info.ID
		event.IsEdit = true
		event.Message = content
	}
	status := storage.StatusServerAck
	return &storage.StoredMessage{Message: event, Status: &status}
}

func TestMessageEditHistoryKeepsOriginalIdentityAndTimestamp(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		for _, normalized := range []bool{false, true} {
			original := editOriginal(&waE2E.Message{Conversation: proto.String("Original")})
			require.NoError(t, store.UpsertOneMessage(original))
			edited := editEvent(original, &waE2E.Message{Conversation: proto.String("Edited")}, original.Info.Timestamp.Add(time.Minute).UnixMilli(), normalized)
			require.NoError(t, store.UpsertOneMessage(edited))
			got, err := store.GetMessage(original.Info.ID)
			require.NoError(t, err)
			require.Equal(t, "Edited", got.Message.Message.GetConversation())
			require.Equal(t, original.Info, got.Info)
			require.True(t, got.IsEdit)
			require.Equal(t, storage.StatusRead, *got.Status)
			assertStoredProjection(t, store, original, true)
			if !normalized {
				protocol, err := store.GetMessage(edited.Info.ID)
				require.NoError(t, err)
				require.False(t, protocol.IsReal)
				require.True(t, proto.Equal(edited.RawMessage, protocol.RawMessage))
				require.NoError(t, store.UpdateMessageStatus(edited.Info.ID, storage.StatusDeliveryAck))
				protocol, err = store.GetMessage(edited.Info.ID)
				require.NoError(t, err)
				require.Equal(t, storage.StatusDeliveryAck, *protocol.Status)
			}
			var raw string
			require.NoError(t, store.db.Get(&raw, "SELECT data FROM gows_messages WHERE id = $1", original.Info.ID))
			var payload map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(raw), &payload))
			require.JSONEq(t, `{"version":1,"edited_at_ms":1700000060000,"original_timestamp_ms":1700000000000}`, string(payload["MinEdit"]))
		}
	})
}

func TestMessageEditHistoryMissingOriginalNeverCreatesMessage(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		original := editOriginal(&waE2E.Message{Conversation: proto.String("Original")})
		for _, normalized := range []bool{false, true} {
			err := store.UpsertOneMessage(editEvent(original, &waE2E.Message{Conversation: proto.String("Edited")}, original.Info.Timestamp.Add(time.Minute).UnixMilli(), normalized))
			require.ErrorIs(t, err, storage.ErrNotFound)
		}
		_, err := store.GetMessage(original.Info.ID)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}

func TestMessageEditHistoryProtocolIdentityWithNormalizedContentStaysHidden(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		original := editOriginal(&waE2E.Message{Conversation: proto.String("Original")})
		require.NoError(t, store.UpsertOneMessage(original))
		edited := editEvent(original, &waE2E.Message{Conversation: proto.String("Edited")}, original.Info.Timestamp.Add(time.Minute).UnixMilli(), true)
		edited.Info.ID = uuid.NewString()
		require.NoError(t, store.UpsertOneMessage(edited))
		rows, err := store.GetChatMessages(original.Info.Chat, storage.MessageFilter{}, storage.Pagination{Limit: 10}, false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, original.Info.ID, rows[0].Info.ID)
		protocol, err := store.GetMessage(edited.Info.ID)
		require.NoError(t, err)
		require.False(t, protocol.IsReal)
		require.True(t, proto.Equal(edited.RawMessage, protocol.RawMessage))
	})
}
