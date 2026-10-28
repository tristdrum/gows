package sqlstorage

import (
	"context"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func statusFixture(t *testing.T, messages *SqlMessageStore) (*SQLStatusHistoryStore, *time.Time, storage.StatusBinding) {
	t.Helper()
	for _, table := range []string{"gows_status_capture", "gows_status_tombstones", "gows_status_metadata", "gows_messages"} {
		_, err := messages.db.Exec("DELETE FROM " + table)
		require.NoError(t, err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	store := &SQLStatusHistoryStore{db: messages.db, now: func() time.Time { return now }}
	binding := storage.StatusBinding{AccountID: "account", ExpectedPhone: "15550000001", ShardID: "shard-a", Epoch: 1, AuthorizationSubject: "owner-hash"}
	return store, &now, binding
}

func enableStatus(t *testing.T, store *SQLStatusHistoryStore, now time.Time, binding storage.StatusBinding) storage.StatusCapture {
	t.Helper()
	result, err := store.Configure(context.Background(), storage.StatusPolicy{StatusBinding: binding, ObservedAtMS: now.UnixMilli(), Enabled: true, AuthorizedUntilMS: now.Add(3 * time.Minute).UnixMilli()})
	require.NoError(t, err)
	require.Equal(t, "active", result.State)
	return result
}

func capturedMessage(id string, now time.Time) *storage.StoredMessage {
	message := integrityMessage(&waE2E.Message{Conversation: proto.String("Private Status")}, storage.StatusServerAck)
	message.Info.ID = id
	message.Info.Chat = types.StatusBroadcastJID
	message.Info.Sender = types.NewJID("15550000002", types.DefaultUserServer)
	message.Info.Timestamp = now
	return message
}

func statusQuery(binding storage.StatusBinding, now time.Time) storage.StatusHistoryQuery {
	return storage.StatusHistoryQuery{StatusBinding: binding, SinceMS: now.Add(-24 * time.Hour).UnixMilli(), UntilMS: now.Add(time.Second).UnixMilli(), Limit: 2}
}

func TestStatusCapturePolicyRetentionAndRevocation(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		msg := capturedMessage("one", *now)
		require.ErrorIs(t, messages.UpsertOneMessage(msg), storage.ErrStatusUnauthorized)
		require.NoError(t, st.Capture(ctx, msg, binding.ExpectedPhone, now.UnixMilli()))
		enableStatus(t, st, *now, binding)
		require.NoError(t, st.Capture(ctx, msg, binding.ExpectedPhone, now.UnixMilli()))
		page, err := st.List(ctx, statusQuery(binding, *now))
		require.NoError(t, err)
		require.Len(t, page.Messages, 1)
		disable := storage.StatusPolicy{StatusBinding: binding, ObservedAtMS: now.Add(time.Millisecond).UnixMilli(), Enabled: false}
		_, err = st.Configure(ctx, disable)
		require.NoError(t, err)
		require.NoError(t, st.Capture(ctx, msg, binding.ExpectedPhone, now.UnixMilli()))
		page, err = st.List(ctx, statusQuery(binding, *now))
		require.NoError(t, err)
		require.Empty(t, page.Messages)
		require.Equal(t, "disabled", page.Capture.State)
		_, err = st.Configure(ctx, storage.StatusPolicy{StatusBinding: binding, ObservedAtMS: now.UnixMilli(), Enabled: true, AuthorizedUntilMS: now.Add(time.Minute).UnixMilli()})
		require.ErrorIs(t, err, storage.ErrStatusStaleControl)
	})
}

func TestStatusPolicyIdempotencyAndBoundBindings(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		capture := enableStatus(t, st, *now, binding)
		repeated := enableStatus(t, st, *now, binding)
		require.Equal(t, capture.Generation, repeated.Generation)
		require.NoError(t, st.Capture(ctx, capturedMessage("private", *now), binding.ExpectedPhone, now.UnixMilli()))
		for _, field := range []string{"account", "phone", "shard", "epoch", "subject"} {
			foreign := binding
			switch field {
			case "account":
				foreign.AccountID = "different"
			case "phone":
				foreign.ExpectedPhone = "15550009999"
			case "shard":
				foreign.ShardID = "different"
			case "epoch":
				foreign.Epoch++
			case "subject":
				foreign.AuthorizationSubject = "different"
			}
			_, err := st.List(ctx, statusQuery(foreign, *now))
			require.ErrorIs(t, err, storage.ErrStatusBinding, field)
			_, err = st.Exact(ctx, exactQuery(foreign, capture.Generation, "private"))
			require.ErrorIs(t, err, storage.ErrStatusBinding, field)
		}
		_, err := st.Configure(ctx, storage.StatusPolicy{StatusBinding: binding, ObservedAtMS: now.Add(-time.Second).UnixMilli(), Enabled: false})
		require.ErrorIs(t, err, storage.ErrStatusStaleControl)
		q := statusQuery(binding, *now)
		calls := 0
		st.now = func() time.Time {
			calls++
			if calls == 1 {
				return *now
			}
			return now.Add(4 * time.Minute)
		}
		page, err := st.List(ctx, q)
		require.NoError(t, err)
		require.Empty(t, page.Messages)
		require.Equal(t, "expired", page.Capture.State)
	})
}

func TestStatusKeysetAndIsolation(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		capture := enableStatus(t, st, *now, binding)
		for _, id := range []string{"a", "b", "c"} {
			require.NoError(t, st.Capture(ctx, capturedMessage(id, *now), binding.ExpectedPhone, now.UnixMilli()))
		}
		q := statusQuery(binding, *now)
		page, err := st.List(ctx, q)
		require.NoError(t, err)
		require.Len(t, page.Messages, 2)
		require.Equal(t, "c", page.Messages[0].Source.ID)
		require.False(t, page.SourcePage.Complete)
		q.Before = page.SourcePage.NextCursor
		q.Generation = capture.Generation
		page, err = st.List(ctx, q)
		require.NoError(t, err)
		require.Len(t, page.Messages, 1)
		require.Equal(t, "a", page.Messages[0].Source.ID)
		require.True(t, page.SourcePage.Complete)
		q.AccountID = "foreign"
		_, err = st.List(ctx, q)
		require.ErrorIs(t, err, storage.ErrStatusBinding)
		generic, err := messages.GetAllMessages(storage.MessageFilter{}, storage.Sort{Field: "timestamp", Order: storage.SortDesc}, storage.Pagination{Limit: 10}, false)
		require.NoError(t, err)
		require.Empty(t, generic)
		_, err = messages.GetMessage("a")
		require.ErrorIs(t, err, storage.ErrNotFound)
		index, err := messages.GetChatIndex(ctx, storage.Pagination{Limit: 10}, false)
		require.NoError(t, err)
		require.Empty(t, index.Chats)
	})
}
