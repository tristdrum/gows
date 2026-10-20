package sqlstorage

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// PostgreSQL deliberately requires a disposable local database. Never point this
// migration-running harness at a session or application database.
func forMessageDatabases(t *testing.T, test func(*testing.T, *SqlMessageStore)) {
	t.Helper()
	for _, dialect := range []string{"sqlite3", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			dsn := filepath.Join(t.TempDir(), "messages.db") + "?_foreign_keys=on&_busy_timeout=10000"
			if dialect == "postgres" {
				dsn = os.Getenv("GOWS_TEST_POSTGRES_DSN")
				if dsn == "" {
					t.Skip("set GOWS_TEST_POSTGRES_DSN to a disposable local gows_*_test database")
				}
				address, err := url.Parse(dsn)
				require.NoError(t, err)
				require.Contains(t, []string{"localhost", "127.0.0.1", "::1"}, address.Hostname())
				database := strings.TrimPrefix(address.Path, "/")
				require.True(t, strings.HasPrefix(database, "gows_") && strings.HasSuffix(database, "_test"))
			}
			container, err := New(dialect, dsn, waLog.Noop)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, container.Close()) })
			test(t, container.NewMessageStorage())
		})
	}
}

func integrityMessage(content *waE2E.Message, status storage.Status) *storage.StoredMessage {
	return &storage.StoredMessage{
		Message: &events.Message{
			Info: types.MessageInfo{
				MessageSource: types.MessageSource{
					Chat: types.JID{User: uuid.NewString(), Server: types.GroupServer},
				},
				ID:        uuid.NewString(),
				Timestamp: time.Unix(1_700_000_000, 0).UTC(),
			},
			Message: content,
		},
		IsReal: content != nil && content.GetSenderKeyDistributionMessage() == nil,
		Status: &status,
	}
}

func duplicateMessage(original *storage.StoredMessage, content *waE2E.Message, status storage.Status) *storage.StoredMessage {
	duplicate := integrityMessage(content, status)
	duplicate.Info = original.Info
	return duplicate
}

func assertStoredProjection(t *testing.T, store *SqlMessageStore, expected *storage.StoredMessage, isReal bool) {
	t.Helper()
	actual, err := store.GetMessage(expected.Info.ID)
	require.NoError(t, err)
	require.Equal(t, isReal, actual.IsReal)
	require.Equal(t, expected.Info.Chat, actual.Info.Chat)
	require.Equal(t, expected.Info.IsFromMe, actual.Info.IsFromMe)
	require.True(t, expected.Info.Timestamp.Equal(actual.Info.Timestamp))
	var row struct {
		JID       string
		FromMe    bool `db:"from_me"`
		Timestamp time.Time
		IsReal    bool `db:"is_real"`
	}
	require.NoError(t, store.db.Get(&row, "SELECT jid, from_me, timestamp, is_real FROM gows_messages WHERE id = $1", expected.Info.ID))
	require.Equal(t, actual.Info.Chat.String(), row.JID)
	require.Equal(t, actual.Info.IsFromMe, row.FromMe)
	require.True(t, actual.Info.Timestamp.Equal(row.Timestamp))
	require.Equal(t, actual.IsReal, row.IsReal)
	list, err := store.GetChatMessages(actual.Info.Chat, storage.MessageFilter{}, storage.Pagination{Limit: 10}, false)
	require.NoError(t, err)
	if isReal {
		require.Len(t, list, 1)
		require.Equal(t, actual.Info.ID, list[0].Info.ID)
	} else {
		require.Empty(t, list)
	}
}

func TestMessageIntegrityDuplicateOrders(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		contents := map[string]*waE2E.Message{
			"ordinary": {Conversation: proto.String("Fixture text")},
			"forwarded": {ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text: proto.String("Fixture forwarded text"),
				ContextInfo: &waE2E.ContextInfo{
					IsForwarded: proto.Bool(true), ForwardingScore: proto.Uint32(1),
				},
			}},
			"media": {AudioMessage: &waE2E.AudioMessage{Mimetype: proto.String("audio/ogg")}},
		}
		for name, content := range contents {
			for _, realFirst := range []bool{false, true} {
				t.Run(name+"/"+map[bool]string{false: "placeholder-first", true: "real-first"}[realFirst], func(t *testing.T) {
					real := integrityMessage(content, storage.StatusDeliveryAck)
					real.IsReal = false // Incoming cached classification is not authority either.
					placeholder := duplicateMessage(real, &waE2E.Message{
						SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{},
					}, storage.StatusServerAck)
					placeholder.IsReal = true
					placeholder.Info.Chat = types.JID{User: "placeholder", Server: types.DefaultUserServer}
					placeholder.Info.IsFromMe = true
					placeholder.Info.Timestamp = real.Info.Timestamp.Add(-time.Second)
					first, second := placeholder, real
					if realFirst {
						first, second = real, placeholder
					}
					require.NoError(t, store.UpsertOneMessage(first))
					require.NoError(t, store.UpsertOneMessage(second))
					assertStoredProjection(t, store, real, true)
					actual, err := store.GetMessage(real.Info.ID)
					require.NoError(t, err)
					require.True(t, proto.Equal(content, actual.Message.Message))
					require.Equal(t, storage.StatusDeliveryAck, *actual.Status)
				})
			}
		}
	})
}

func TestMessageIntegrityReclassifiesBothStaleDirections(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		for _, real := range []bool{false, true} {
			content := &waE2E.Message{SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{}}
			if real {
				content = &waE2E.Message{Conversation: proto.String("Retained fixture")}
			}
			message := integrityMessage(content, storage.StatusRead)
			require.NoError(t, store.UpsertOneMessage(message))
			// Reproduce both historical index/payload mismatch directions.
			_, err := store.db.Exec("UPDATE gows_messages SET is_real = $1 WHERE id = $2", !real, message.Info.ID)
			require.NoError(t, err)
			require.NoError(t, store.UpsertOneMessage(message))
			assertStoredProjection(t, store, message, real)
		}
	})
}

func TestMessageIntegrityDuplicateCannotDowngradeStatus(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		message := integrityMessage(&waE2E.Message{Conversation: proto.String("Fixture")}, storage.StatusPlayed)
		require.NoError(t, store.UpsertOneMessage(message))
		history := duplicateMessage(message, message.Message.Message, storage.StatusServerAck)
		require.NoError(t, store.UpsertOneMessage(history))
		actual, err := store.GetMessage(message.Info.ID)
		require.NoError(t, err)
		require.Equal(t, storage.StatusPlayed, *actual.Status)
	})
}

func TestMessageIntegrityPreservesUnknownRetainedFields(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		message := integrityMessage(&waE2E.Message{Conversation: proto.String("Fixture")}, storage.StatusRead)
		require.NoError(t, store.UpsertOneMessage(message))
		var raw string
		require.NoError(t, store.db.Get(&raw, "SELECT data FROM gows_messages WHERE id = $1", message.Info.ID))
		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(raw), &payload))
		payload["FutureTopLevel"] = json.RawMessage(`{"value":"keep"}`)
		payload["Message"] = json.RawMessage(`{"conversation":"Fixture","futureMessageField":{"value":"keep"}}`)
		// Neither existing serialized nor indexed classification is authority.
		payload["IsReal"] = json.RawMessage(`false`)
		encoded, err := json.Marshal(payload)
		require.NoError(t, err)
		_, err = store.db.Exec("UPDATE gows_messages SET data = $1, is_real = FALSE WHERE id = $2", string(encoded), message.Info.ID)
		require.NoError(t, err)
		placeholder := duplicateMessage(message, nil, storage.StatusPlayed)
		require.NoError(t, store.UpsertOneMessage(placeholder))
		require.NoError(t, store.db.Get(&raw, "SELECT data FROM gows_messages WHERE id = $1", message.Info.ID))
		var retained map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(raw), &retained))
		require.JSONEq(t, string(payload["Message"]), string(retained["Message"]))
		require.JSONEq(t, string(payload["FutureTopLevel"]), string(retained["FutureTopLevel"]))
		require.Equal(t, "5", string(retained["Status"]))
		assertStoredProjection(t, store, message, true)
	})
}
