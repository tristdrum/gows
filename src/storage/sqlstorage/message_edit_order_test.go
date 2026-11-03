package sqlstorage

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestMessageEditHistoryOrderingAndReplay(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		original := editOriginal(&waE2E.Message{Conversation: proto.String("Original")})
		require.NoError(t, store.UpsertOneMessage(original))
		version := original.Info.Timestamp.Add(time.Minute).UnixMilli()
		makeEdit := func(text string, version int64) *storage.StoredMessage {
			return editEvent(original, &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String(text)}}, version, true)
		}
		require.NoError(t, store.UpsertOneMessage(makeEdit("Latest", version+1)))
		var first string
		require.NoError(t, store.db.Get(&first, "SELECT data FROM gows_messages WHERE id = $1", original.Info.ID))
		for _, event := range []*storage.StoredMessage{makeEdit("Older", version), makeEdit("Latest", version+1), makeEdit("Conflicting same version", version+1), original} {
			require.NoError(t, store.UpsertOneMessage(event))
			var after string
			require.NoError(t, store.db.Get(&after, "SELECT data FROM gows_messages WHERE id = $1", original.Info.ID))
			require.Equal(t, first, after)
		}
		require.NoError(t, store.UpsertOneMessage(makeEdit("Next", version+2)))
		got, err := store.GetMessage(original.Info.ID)
		require.NoError(t, err)
		require.Equal(t, "Next", got.Message.Message.GetConversation())
		require.Equal(t, original.Info, got.Info)
		require.NotNil(t, got.MinEdit)
		require.Equal(t, original.Info.Timestamp.UnixMilli(), got.MinEdit.OriginalTimestampMS)
		require.Equal(t, version+2, got.MinEdit.EditedAtMS)
		list, err := store.GetLastMessagesInChats(storage.ChatFilter{Jids: []types.JID{original.Info.Chat}}, storage.Sort{Field: "timestamp", Order: storage.SortDesc}, storage.Pagination{Limit: 10}, false)
		require.NoError(t, err)
		require.Len(t, list, 1)
		require.Equal(t, original.Info.ID, list[0].Info.ID)
		require.Equal(t, "Next", list[0].Message.Message.GetConversation())
		all, err := store.GetAllMessages(storage.MessageFilter{Jid: &original.Info.Chat}, storage.Sort{Field: "timestamp", Order: storage.SortDesc}, storage.Pagination{Limit: 10}, false)
		require.NoError(t, err)
		require.Len(t, all, 1)
		require.Equal(t, original.Info.Timestamp, all[0].Info.Timestamp)
		require.Equal(t, "Next", all[0].Message.Message.GetConversation())
	})
}

func TestMessageEditHistoryRetainsUnknownOriginalTimeForLegacyEdits(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		original := editOriginal(&waE2E.Message{Conversation: proto.String("Legacy edit")})
		require.NoError(t, store.UpsertOneMessage(original))
		var raw string
		require.NoError(t, store.db.Get(&raw, "SELECT data FROM gows_messages WHERE id = $1", original.Info.ID))
		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(raw), &payload))
		payload["IsEdit"] = json.RawMessage(`true`)
		data, err := json.Marshal(payload)
		require.NoError(t, err)
		_, err = store.db.Exec("UPDATE gows_messages SET data = $1 WHERE id = $2", string(data), original.Info.ID)
		require.NoError(t, err)
		require.NoError(t, store.UpsertOneMessage(editEvent(original, &waE2E.Message{Conversation: proto.String("New edit")}, original.Info.Timestamp.Add(time.Minute).UnixMilli(), false)))
		got, err := store.GetMessage(original.Info.ID)
		require.NoError(t, err)
		require.Equal(t, "New edit", got.Message.Message.GetConversation())
		require.Zero(t, got.MinEdit.OriginalTimestampMS)
	})
}

func TestMessageEditHistoryRejectsInvalidEvidenceWithoutWriting(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		original := editOriginal(&waE2E.Message{Conversation: proto.String("Original")})
		require.NoError(t, store.UpsertOneMessage(original))
		for _, mutate := range []func(*storage.StoredMessage){
			func(e *storage.StoredMessage) { e.Info.Chat = types.NewJID("wrong", types.GroupServer) },
			func(e *storage.StoredMessage) { e.Info.Sender.User = "wrong" },
			func(e *storage.StoredMessage) { e.Info.IsFromMe = false },
			func(e *storage.StoredMessage) { e.Info.IsGroup = false },
			func(e *storage.StoredMessage) { e.Message.Message.ProtocolMessage.Key.FromMe = proto.Bool(false) },
			func(e *storage.StoredMessage) {
				e.Message.Message.ProtocolMessage.Key.Participant = proto.String("wrong@s.whatsapp.net")
			},
			func(e *storage.StoredMessage) {
				e.Message.Message.ProtocolMessage.Key.RemoteJID = proto.String("wrong@g.us")
			},
			func(e *storage.StoredMessage) { e.Message.Message.ProtocolMessage.TimestampMS = nil },
			func(e *storage.StoredMessage) { e.Message.Message.ProtocolMessage.EditedMessage = nil },
			func(e *storage.StoredMessage) { e.Message.Message.ProtocolMessage.TimestampMS = proto.Int64(1) },
			func(e *storage.StoredMessage) {
				e.Message.Message.ProtocolMessage.EditedMessage = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("wrong type")}}
			},
			func(e *storage.StoredMessage) {
				e.RawMessage = nil
				e.Message.Message = &waE2E.Message{Conversation: proto.String("No protocol")}
				e.IsEdit = true
			},
			func(e *storage.StoredMessage) { e.IsViewOnce = true },
		} {
			event := editEvent(original, &waE2E.Message{Conversation: proto.String("Edited")}, original.Info.Timestamp.Add(time.Minute).UnixMilli(), false)
			mutate(event)
			require.Error(t, store.UpsertOneMessage(event))
			got, err := store.GetMessage(original.Info.ID)
			require.NoError(t, err)
			require.Equal(t, "Original", got.Message.Message.GetConversation())
			require.Nil(t, got.MinEdit)
		}
	})
}

func TestMessageEditHistoryDoesNotChangeOrdinaryViewOnceOrProtocolStorage(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		for _, content := range []*waE2E.Message{
			{ViewOnceMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("Ordinary view once")}}}},
			{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_EPHEMERAL_SETTING.Enum()}},
		} {
			original := editOriginal(content)
			require.NoError(t, store.UpsertOneMessage(original))
			got, err := store.GetMessage(original.Info.ID)
			require.NoError(t, err)
			require.True(t, proto.Equal(content, got.Message.Message))
		}
	})
}

func TestMessageEditHistoryConcurrentVersionsAndReceipts(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		original := editOriginal(&waE2E.Message{Conversation: proto.String("Original")})
		require.NoError(t, store.UpsertOneMessage(original))
		version := original.Info.Timestamp.Add(time.Minute).UnixMilli()
		var workers sync.WaitGroup
		start := make(chan struct{})
		failures := make(chan error, 10)
		for index := 1; index <= 8; index++ {
			workers.Add(1)
			go func(index int) {
				defer workers.Done()
				<-start
				event := editEvent(original, &waE2E.Message{Conversation: proto.String(fmt.Sprintf("Revision %d", index))}, version+int64(index), index%2 == 0)
				if err := store.UpsertOneMessage(event); err != nil {
					failures <- err
				}
			}(index)
		}
		for _, status := range []storage.Status{storage.StatusRead, storage.StatusPlayed} {
			workers.Add(1)
			go func(status storage.Status) {
				defer workers.Done()
				<-start
				if err := store.UpdateMessageStatus(original.Info.ID, status); err != nil {
					failures <- err
				}
			}(status)
		}
		close(start)
		workers.Wait()
		close(failures)
		for err := range failures {
			require.NoError(t, err)
		}
		got, err := store.GetMessage(original.Info.ID)
		require.NoError(t, err)
		require.Equal(t, "Revision 8", got.Message.Message.GetConversation())
		require.Equal(t, version+8, got.MinEdit.EditedAtMS)
		require.Equal(t, storage.StatusPlayed, *got.Status)
		assertStoredProjection(t, store, original, true)
	})
}
