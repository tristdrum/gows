package sqlstorage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/types"
)

func forChatIndexDatabases(t *testing.T, test func(*testing.T, *SqlMessageStore)) {
	t.Helper()
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		// The helper accepts only disposable local gows_*_test databases.
		// Index traversal tests require an isolated source, unlike ID lookups.
		for _, table := range []string{"gows_messages", "gows_chat_read_state", "whatsmeow_lid_map"} {
			_, err := messages.db.Exec("DELETE FROM " + table)
			require.NoError(t, err)
		}
		test(t, messages)
	})
}

func TestChatIndexStablePagingAndBoundedPayloads(t *testing.T) {
	forChatIndexDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		stamp := time.Unix(1_700_000_000, 0).UTC()
		for i := 0; i < 51; i++ {
			jid := testJID(fmt.Sprintf("index-%03d", i), types.GroupServer)
			storeTestMessage(t, messages, fmt.Sprintf("index-%03d", i), jid, stamp, false)
		}
		// An unselected payload is deliberately invalid: metadata selection must
		// not load/decode message bodies for all chats before paging.
		_, err := messages.db.Exec("UPDATE gows_messages SET data = '{' WHERE id = 'index-050'")
		require.NoError(t, err)
		page, err := messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 25}, false)
		require.NoError(t, err)
		require.Len(t, page.Chats, 25)
		require.Equal(t, "index-000@g.us", page.Chats[0].Chat.Jid.String())
		require.Equal(t, uint64(25), *page.SourcePage.NextOffset)
		require.False(t, page.SourcePage.Complete)
		page, err = messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 25, Offset: 25}, false)
		require.NoError(t, err)
		require.Equal(t, "index-025@g.us", page.Chats[0].Chat.Jid.String())
		_, err = messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 1, Offset: 50}, false)
		require.ErrorIs(t, err, storage.ErrChatIndexDataUnavailable)
	})
}

func TestChatIndexMergesAliasesAndPreservesUnreadBoundary(t *testing.T) {
	forChatIndexDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		stamp := time.Unix(1_700_000_000, 0).UTC()
		pn, lid := testJID("15550001111", types.DefaultUserServer), testJID("9001", types.HiddenUserServer)
		_, err := messages.db.Exec("INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ($1, $2)", lid.User, pn.User)
		require.NoError(t, err)
		storeTestMessage(t, messages, "pn-old", pn, stamp, false)
		storeTestMessage(t, messages, "lid-new", lid, stamp.Add(time.Second), false)
		_, err = (SqlChatReadStateStore{db: messages.db}).UpsertChatReadState(&storage.StoredChatReadState{
			Jid: pn, UnreadStateKnown: true, CountFrom: stamp, CoveredMessageIDs: []string{"pn-old"}, EvidenceTimestamp: stamp,
		})
		require.NoError(t, err)
		page, err := messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 10}, true)
		require.NoError(t, err)
		require.Len(t, page.Chats, 1)
		require.Equal(t, pn, page.Chats[0].Chat.Jid)
		require.Equal(t, pn, page.Chats[0].Message.Info.Chat)
		require.Equal(t, "lid-new", page.Chats[0].Message.Info.ID)
		require.Equal(t, uint64(1), page.Chats[0].Chat.UnreadCount)
		require.True(t, page.SourcePage.Complete)
		storeTestMessage(t, messages, "uncovered-boundary", pn, stamp, false)
		_, err = messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 10}, true)
		require.ErrorIs(t, err, storage.ErrUnreadStateUnavailable)
	})
}

func TestChatIndexUnreadConsumesOnlyPrefixAndResumes(t *testing.T) {
	forChatIndexDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		stamp := time.Unix(1_700_000_000, 0).UTC()
		for i := 0; i < 1002; i++ {
			jid := testJID(fmt.Sprintf("scan-%04d", i), types.GroupServer)
			storeTestMessage(t, messages, fmt.Sprintf("scan-%04d", i), jid, stamp, true)
			if i == 1 {
				continue
			} // Unknown beyond the first requested match must not fail it.
			_, err := (SqlChatReadStateStore{db: messages.db}).UpsertChatReadState(&storage.StoredChatReadState{
				Jid: jid, UnreadStateKnown: true, MarkedAsUnread: i == 0 || i == 1001, CountFrom: stamp, EvidenceTimestamp: stamp,
			})
			require.NoError(t, err)
		}
		page, err := messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 1}, true)
		require.NoError(t, err)
		require.Equal(t, uint64(1), page.SourcePage.ScannedCount)
		require.Equal(t, uint64(1), *page.SourcePage.NextOffset)
		_, err = messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 1, Offset: 1}, true)
		require.ErrorIs(t, err, storage.ErrUnreadStateUnavailable)
		_, err = messages.db.Exec("INSERT INTO gows_chat_read_state(jid,baseline_unread_count,marked_as_unread,unread_state_known,count_from_timestamp,covered_message_ids,evidence_timestamp) VALUES ($1,0,false,true,0,'invalid',0)", "scan-0001@g.us")
		require.NoError(t, err)
		page, err = messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 1}, true)
		require.NoError(t, err)
		require.Len(t, page.Chats, 1)
		_, err = messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 1, Offset: 1}, true)
		require.ErrorIs(t, err, storage.ErrUnreadStateUnavailable)
		page, err = messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 2, Offset: 2}, true)
		require.NoError(t, err)
		require.Equal(t, uint64(1000), page.SourcePage.ScannedCount)
		require.True(t, page.SourcePage.Complete)
		require.Len(t, page.Chats, 1)
	})
}

func TestChatIndexCancelledAndEmpty(t *testing.T) {
	container := newReadStateTestContainer(t)
	messages := container.NewMessageStorage()
	page, err := messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 50}, false)
	require.NoError(t, err)
	require.Empty(t, page.Chats)
	require.True(t, page.SourcePage.Complete)
	require.Nil(t, page.SourcePage.NextOffset)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = messages.GetChatIndex(ctx, storage.Pagination{Limit: 50}, false)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestChatIndexDeadlineCancelsPoolWaitAndMessageReads(t *testing.T) {
	container := newReadStateTestContainer(t)
	messages := container.NewMessageStorage()
	messages.db.SetMaxOpenConns(1)
	connection, err := messages.db.Conn(context.Background())
	require.NoError(t, err)
	for _, read := range []func(context.Context) error{
		func(ctx context.Context) error {
			_, err := messages.GetChatIndex(ctx, storage.Pagination{Limit: 50}, false)
			return err
		},
		func(ctx context.Context) error {
			_, err := messages.GetAllMessagesContext(ctx, storage.MessageFilter{}, storage.Sort{Field: "timestamp", Order: storage.SortDesc}, storage.Pagination{Limit: 20}, true)
			return err
		},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		started := time.Now()
		require.ErrorIs(t, read(ctx), context.DeadlineExceeded)
		require.Less(t, time.Since(started), time.Second)
		cancel()
	}
	require.NoError(t, connection.Close())
	page, err := messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 50}, false)
	require.NoError(t, err)
	require.True(t, page.SourcePage.Complete)
}

func TestChatIndexLimitsAndScanCap(t *testing.T) {
	forChatIndexDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		stamp := time.Unix(1_700_000_000, 0).UTC()
		for i := 0; i < 1001; i++ {
			jid := testJID(fmt.Sprintf("cap-%04d", i), types.GroupServer)
			storeTestMessage(t, messages, fmt.Sprintf("cap-%04d", i), jid, stamp, true)
			_, err := (SqlChatReadStateStore{db: messages.db}).UpsertChatReadState(&storage.StoredChatReadState{
				Jid: jid, UnreadStateKnown: true, MarkedAsUnread: i == 1000, CountFrom: stamp, EvidenceTimestamp: stamp,
			})
			require.NoError(t, err)
		}
		page, err := messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 50}, true)
		require.NoError(t, err)
		require.Empty(t, page.Chats)
		require.Equal(t, uint64(1000), page.SourcePage.ScannedCount)
		require.False(t, page.SourcePage.Complete)
		require.Equal(t, uint64(1000), *page.SourcePage.NextOffset)
		page, err = messages.GetChatIndex(context.Background(), storage.Pagination{Limit: 50, Offset: *page.SourcePage.NextOffset}, true)
		require.NoError(t, err)
		require.Len(t, page.Chats, 1)
		require.True(t, page.SourcePage.Complete)
		for _, limit := range []uint64{0, 51} {
			_, err = messages.GetChatIndex(context.Background(), storage.Pagination{Limit: limit}, false)
			require.Error(t, err)
		}
	})
}
