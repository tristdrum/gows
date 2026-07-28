package gows

import (
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

func TestChatReadStateFromConversationUsesHistoryUnreadBaseline(t *testing.T) {
	jid := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	conversation := &waHistorySync.Conversation{
		UnreadCount:           proto.Uint32(3),
		MarkedAsUnread:        proto.Bool(true),
		ConversationTimestamp: proto.Uint64(1_700_000_000),
		Messages: []*waHistorySync.HistorySyncMsg{
			{Message: &waWeb.WebMessageInfo{
				Key:              &waCommon.MessageKey{ID: proto.String("covered-history")},
				MessageTimestamp: proto.Uint64(1_700_000_000),
			}},
			{Message: &waWeb.WebMessageInfo{
				Key:              &waCommon.MessageKey{ID: proto.String("older-history")},
				MessageTimestamp: proto.Uint64(1_699_999_999),
			}},
		},
	}

	state, ok := chatReadStateFromConversation(conversation, jid)
	require.True(t, ok)
	require.NotNil(t, state)
	assert.Equal(t, jid, state.Jid)
	assert.Equal(t, uint64(3), state.BaselineUnreadCount)
	assert.True(t, state.MarkedAsUnread)
	assert.True(t, state.UnreadStateKnown)
	assert.Equal(t, time.Unix(1_700_000_000, 0), state.CountFrom)
	assert.Equal(t, []string{"covered-history"}, state.CoveredMessageIDs)
	assert.Equal(t, state.CountFrom, state.EvidenceTimestamp)
}

func TestChatReadStateFromConversationKeepsCountUnknownWhenHistoryOmitsIt(t *testing.T) {
	jid := types.JID{User: "15557654321", Server: types.DefaultUserServer}
	conversation := &waHistorySync.Conversation{
		MarkedAsUnread:        proto.Bool(true),
		ConversationTimestamp: proto.Uint64(1_700_000_000),
	}

	state, ok := chatReadStateFromConversation(conversation, jid)
	require.True(t, ok)
	assert.True(t, state.MarkedAsUnread)
	assert.False(t, state.UnreadStateKnown)
}

func TestChatReadEventUsesMessageRangeWatermark(t *testing.T) {
	jid := types.JID{User: "15550001111", Server: types.DefaultUserServer}
	eventTime := time.Unix(1_700_000_100, 0)
	event := &events.MarkChatAsRead{
		JID:       jid,
		Timestamp: eventTime,
		Action: &waSyncAction.MarkChatAsReadAction{
			Read: proto.Bool(true),
			MessageRange: &waSyncAction.SyncActionMessageRange{
				LastMessageTimestamp: proto.Int64(1_700_000_090),
				Messages: []*waSyncAction.SyncActionMessage{
					{
						Key:       &waCommon.MessageKey{ID: proto.String("covered-read")},
						Timestamp: proto.Int64(1_700_000_090),
					},
				},
			},
		},
	}

	readEvent, ok := chatReadEventFromEvent(event)
	require.True(t, ok)
	assert.Equal(t, jid, readEvent.Jid)
	assert.True(t, readEvent.Read)
	assert.Equal(t, eventTime, readEvent.Timestamp)
	assert.Equal(t, time.Unix(1_700_000_090, 0), readEvent.MessageWatermark)
	assert.Equal(t, []string{"covered-read"}, readEvent.CoveredMessageIDs)
}

type recordingChatReadStateStorage struct {
	baselines []*storage.StoredChatReadState
	events    []storage.ChatReadEvent
}

func (s *recordingChatReadStateStorage) UpsertChatReadState(state *storage.StoredChatReadState) (bool, error) {
	s.baselines = append(s.baselines, state)
	return true, nil
}
func (s *recordingChatReadStateStorage) ApplyChatReadEvent(event storage.ChatReadEvent) (bool, error) {
	s.events = append(s.events, event)
	return true, nil
}
func (s *recordingChatReadStateStorage) GetChatReadStates([]types.JID, bool) (map[string]*storage.StoredChatReadState, error) {
	return nil, nil
}
func (s *recordingChatReadStateStorage) DeleteChatReadState(types.JID) error { return nil }

func TestStorageEventHandlerPersistsMarkChatAsRead(t *testing.T) {
	states := &recordingChatReadStateStorage{}
	handler := &StorageEventHandler{
		log:     waLog.Noop,
		storage: &storage.Storage{ChatReadState: states},
	}
	jid := types.JID{User: "15550002222", Server: types.DefaultUserServer}
	handler.handleEvent(&events.MarkChatAsRead{
		JID:       jid,
		Timestamp: time.Unix(1_700_000_200, 0),
		Action:    &waSyncAction.MarkChatAsReadAction{Read: proto.Bool(false)},
	})

	require.Len(t, states.events, 1)
	assert.Equal(t, jid, states.events[0].Jid)
	assert.False(t, states.events[0].Read)
}

func TestStorageEventHandlerSeedsHistoryUnreadState(t *testing.T) {
	states := &recordingChatReadStateStorage{}
	handler := &StorageEventHandler{
		log:     waLog.Noop,
		storage: &storage.Storage{ChatReadState: states},
	}
	jid := types.JID{User: "15550004444", Server: types.DefaultUserServer}
	handler.saveHistoryForOneChat(&waHistorySync.Conversation{
		UnreadCount:           proto.Uint32(7),
		MarkedAsUnread:        proto.Bool(false),
		ConversationTimestamp: proto.Uint64(1_700_000_300),
	}, jid)

	require.Len(t, states.baselines, 1)
	assert.Equal(t, jid, states.baselines[0].Jid)
	assert.Equal(t, uint64(7), states.baselines[0].BaselineUnreadCount)
	assert.True(t, states.baselines[0].UnreadStateKnown)
}
