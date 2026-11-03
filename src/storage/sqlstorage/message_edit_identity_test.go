package sqlstorage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestMessageEditHistoryIncomingAndSenderAliases(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		for _, group := range []bool{false, true} {
			original := editOriginal(&waE2E.Message{Conversation: proto.String("Incoming original")})
			original.Info.IsFromMe = false
			original.Info.IsGroup = group
			if !group {
				original.Info.Chat = original.Info.Sender
			}
			require.NoError(t, store.UpsertOneMessage(original))
			event := editEvent(original, &waE2E.Message{Conversation: proto.String("Incoming edit")}, original.Info.Timestamp.Add(time.Minute).UnixMilli(), false)
			event.Info.SenderAlt = original.Info.Sender
			event.Info.Sender = types.NewJID("199900000001", types.HiddenUserServer)
			event.Info.Sender.Device = 3
			protocol := event.Message.Message.ProtocolMessage
			if group {
				protocol.Key.Participant = proto.String(original.Info.Sender.String())
			} else {
				// The editor's original recipient is this account, while the
				// authenticated incoming chat identifies the remote editor.
				protocol.Key.RemoteJID = proto.String("27760000099@s.whatsapp.net")
			}
			require.True(t, protocol.GetKey().GetFromMe())
			require.False(t, event.Info.IsFromMe)
			require.NoError(t, store.UpsertOneMessage(event))
			got, err := store.GetMessage(original.Info.ID)
			require.NoError(t, err)
			require.Equal(t, original.Info, got.Info)
			require.Equal(t, "Incoming edit", got.Message.Message.GetConversation())
		}
	})
}

func TestMessageEditHistoryReadsProtocolFromSupportedWrappers(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		for _, source := range []string{"raw", "web"} {
			original := editOriginal(&waE2E.Message{Conversation: proto.String("Original")})
			require.NoError(t, store.UpsertOneMessage(original))
			event := editEvent(original, &waE2E.Message{Conversation: proto.String("Edited")}, original.Info.Timestamp.Add(time.Minute).UnixMilli(), true)
			wrapped := &waE2E.Message{DeviceSentMessage: &waE2E.DeviceSentMessage{Message: &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{Message: event.RawMessage}}}}
			event.RawMessage = wrapped
			if source == "web" {
				event.SourceWebMsg = &waWeb.WebMessageInfo{Key: &waCommon.MessageKey{ID: proto.String("protocol-id")}, Message: wrapped}
				event.RawMessage = nil
			}
			require.NoError(t, store.UpsertOneMessage(event))
			got, err := store.GetMessage(original.Info.ID)
			require.NoError(t, err)
			require.Equal(t, "Edited", got.Message.Message.GetConversation())
			assertStoredProjection(t, store, original, true)
		}
	})
}

func TestMessageEditHistoryMalformedRetainedRawFailsWithoutReplacement(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		original := editOriginal(&waE2E.Message{Conversation: proto.String("Original")})
		require.NoError(t, store.UpsertOneMessage(original))
		var raw string
		require.NoError(t, store.db.Get(&raw, "SELECT data FROM gows_messages WHERE id = $1", original.Info.ID))
		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(raw), &payload))
		for _, retained := range []json.RawMessage{json.RawMessage(`{"imageMessage":{"caption":"Unrelated"}}`), json.RawMessage(`{"futureRawWrapper":{"keep":true}}`)} {
			payload["RawMessage"] = retained
			before, err := json.Marshal(payload)
			require.NoError(t, err)
			_, err = store.db.Exec("UPDATE gows_messages SET data = $1 WHERE id = $2", string(before), original.Info.ID)
			require.NoError(t, err)
			require.Error(t, store.UpsertOneMessage(editEvent(original, &waE2E.Message{Conversation: proto.String("Edited")}, original.Info.Timestamp.Add(time.Minute).UnixMilli(), true)))
			var after string
			require.NoError(t, store.db.Get(&after, "SELECT data FROM gows_messages WHERE id = $1", original.Info.ID))
			require.Equal(t, string(before), after)
		}
	})
}

func TestMessageEditHistoryConcurrentDeleteCannotRestoreOriginal(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		original := editOriginal(&waE2E.Message{Conversation: proto.String("Original")})
		require.NoError(t, store.UpsertOneMessage(original))
		err := store.updateStoredMessage(original.Info.ID, func(current *storage.StoredMessage, raw []byte) (*storage.StoredMessage, []byte, error) {
			require.NoError(t, store.DeleteMessage(original.Info.ID))
			updated, err := patchMessageEdit(raw, &waE2E.Message{Conversation: proto.String("Edited")}, &storage.MessageEditMetadata{Version: 1, EditedAtMS: current.Info.Timestamp.Add(time.Minute).UnixMilli(), OriginalTimestampMS: current.Info.Timestamp.UnixMilli()})
			return current, updated, err
		})
		require.ErrorIs(t, err, storage.ErrNotFound)
		for _, normalized := range []bool{false, true} {
			require.ErrorIs(t, store.UpsertOneMessage(editEvent(original, &waE2E.Message{Conversation: proto.String("Late edit")}, original.Info.Timestamp.Add(2*time.Minute).UnixMilli(), normalized)), storage.ErrNotFound)
		}
		_, err = store.GetMessage(original.Info.ID)
		require.ErrorIs(t, err, storage.ErrNotFound)
		rows, err := store.GetChatMessages(original.Info.Chat, storage.MessageFilter{}, storage.Pagination{Limit: 10}, false)
		require.NoError(t, err)
		require.Empty(t, rows)
	})
}
