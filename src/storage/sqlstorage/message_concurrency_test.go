package sqlstorage

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestMessageIntegrityReceiptsDoNotRestoreOldContent(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		original := integrityMessage(&waE2E.Message{Conversation: proto.String("Original")}, storage.StatusServerAck)
		require.NoError(t, store.UpsertOneMessage(original))
		edited := duplicateMessage(original, &waE2E.Message{Conversation: proto.String("Edited")}, storage.StatusDeliveryAck)
		edited.IsEdit = true
		require.NoError(t, store.UpsertOneMessage(edited))
		require.NoError(t, store.UpdateMessageStatus(original.Info.ID, storage.StatusPlayed))
		require.NoError(t, store.UpsertOneMessage(original))
		require.NoError(t, store.UpdateMessageStatus(original.Info.ID, storage.StatusRead))
		actual, err := store.GetMessage(original.Info.ID)
		require.NoError(t, err)
		require.Equal(t, "Edited", actual.Message.Message.GetConversation())
		require.Equal(t, storage.StatusPlayed, *actual.Status)
		assertStoredProjection(t, store, edited, true)

		require.NoError(t, store.DeleteMessage(original.Info.ID))
		require.ErrorIs(t, store.UpdateMessageStatus(original.Info.ID, storage.StatusPlayed), storage.ErrNotFound)
		_, err = store.GetMessage(original.Info.ID)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}

func TestMessageIntegrityReceiptPreservesUnknownPayload(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		message := integrityMessage(&waE2E.Message{Conversation: proto.String("Fixture")}, storage.StatusDeliveryAck)
		require.NoError(t, store.UpsertOneMessage(message))
		var raw string
		require.NoError(t, store.db.Get(&raw, "SELECT data FROM gows_messages WHERE id = $1", message.Info.ID))
		var before map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(raw), &before))
		before["Unknown"] = json.RawMessage(`{"keep":true}`)
		before["Message"] = json.RawMessage(`{"conversation":"Fixture","futureMessage":{"keep":true}}`)
		data, err := json.Marshal(before)
		require.NoError(t, err)
		_, err = store.db.Exec("UPDATE gows_messages SET data = $1 WHERE id = $2", string(data), message.Info.ID)
		require.NoError(t, err)
		require.NoError(t, store.UpdateMessageStatus(message.Info.ID, storage.StatusRead))
		require.NoError(t, store.db.Get(&raw, "SELECT data FROM gows_messages WHERE id = $1", message.Info.ID))
		var after map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(raw), &after))
		before["Status"] = json.RawMessage(`4`)
		expected, err := json.Marshal(before)
		require.NoError(t, err)
		require.JSONEq(t, string(expected), raw)
	})
}

func TestMessageIntegrityCASRetriesCurrentContentAndStopsOnDelete(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		for _, deleteDuringUpdate := range []bool{false, true} {
			message := integrityMessage(&waE2E.Message{Conversation: proto.String("Original")}, storage.StatusServerAck)
			require.NoError(t, store.UpsertOneMessage(message))
			calls := 0
			err := store.updateStoredMessage(message.Info.ID, func(current *storage.StoredMessage, raw []byte) (*storage.StoredMessage, []byte, error) {
				calls++
				if calls == 1 {
					if deleteDuringUpdate {
						require.NoError(t, store.DeleteMessage(message.Info.ID))
					} else {
						edit := duplicateMessage(message, &waE2E.Message{Conversation: proto.String("Concurrent edit")}, storage.StatusPlayed)
						edit.IsEdit = true
						require.NoError(t, store.UpsertOneMessage(edit))
					}
				}
				data, err := patchMessageState(raw, maximumMessageStatus(current.Status, protoStatus(storage.StatusRead)), current.IsReal)
				return current, data, err
			})
			if deleteDuringUpdate {
				require.ErrorIs(t, err, storage.ErrNotFound)
				_, err = store.GetMessage(message.Info.ID)
				require.ErrorIs(t, err, storage.ErrNotFound)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, calls)
				actual, err := store.GetMessage(message.Info.ID)
				require.NoError(t, err)
				require.Equal(t, "Concurrent edit", actual.Message.Message.GetConversation())
				require.Equal(t, storage.StatusPlayed, *actual.Status)
			}
		}
	})
}

func protoStatus(status storage.Status) *storage.Status { return &status }

func TestMessageIntegrityConcurrentDeliveryHistoryAndReceipt(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		message := integrityMessage(&waE2E.Message{Conversation: proto.String("Concurrent fixture")}, storage.StatusServerAck)
		require.NoError(t, store.UpsertOneMessage(message))
		var group sync.WaitGroup
		start := make(chan struct{})
		failures := make(chan error, 12)
		for worker := 0; worker < 12; worker++ {
			group.Add(1)
			go func(worker int) {
				defer group.Done()
				<-start
				for turn := 0; turn < 4; turn++ {
					var err error
					switch worker % 3 {
					case 0:
						err = store.UpdateMessageStatus(message.Info.ID, storage.StatusPlayed)
					case 1:
						err = store.UpsertOneMessage(duplicateMessage(message, message.Message.Message, storage.StatusServerAck))
					case 2:
						err = store.UpsertOneMessage(duplicateMessage(message, &waE2E.Message{
							SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{},
						}, storage.StatusDeliveryAck))
					}
					if err != nil {
						failures <- err
						return
					}
					var row struct {
						Data   string
						IsReal bool `db:"is_real"`
					}
					if err = store.db.Get(&row, "SELECT data, is_real FROM gows_messages WHERE id = $1", message.Info.ID); err != nil {
						failures <- err
						return
					}
					var observed storage.StoredMessage
					if err = messageMapper.Unmarshal([]byte(row.Data), &observed); err != nil {
						failures <- err
						return
					}
					if !row.IsReal || !observed.IsReal || observed.Message.Message.GetConversation() != "Concurrent fixture" {
						failures <- fmt.Errorf("concurrent write exposed inconsistent content/classification")
						return
					}
				}
			}(worker)
		}
		close(start)
		group.Wait()
		close(failures)
		for err := range failures {
			require.NoError(t, err)
		}
		actual, err := store.GetMessage(message.Info.ID)
		require.NoError(t, err)
		require.Equal(t, storage.StatusPlayed, *actual.Status)
		assertStoredProjection(t, store, message, true)
	})
}

func TestMessageIntegrityRejectsMalformedStoredPayload(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		message := integrityMessage(&waE2E.Message{Conversation: proto.String("Fixture")}, storage.StatusRead)
		require.NoError(t, store.UpsertOneMessage(message))
		for _, malformed := range []string{`not-json`, `{"Message":{"conversation":123}}`, `null`} {
			_, err := store.db.Exec("UPDATE gows_messages SET data = $1 WHERE id = $2", malformed, message.Info.ID)
			require.NoError(t, err)
			require.Error(t, store.UpsertOneMessage(message))
			require.Error(t, store.UpdateMessageStatus(message.Info.ID, storage.StatusPlayed))
			var retained string
			require.NoError(t, store.db.Get(&retained, "SELECT data FROM gows_messages WHERE id = $1", message.Info.ID))
			require.Equal(t, malformed, retained)
		}
	})
}
