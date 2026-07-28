package views

import (
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type chatViewMessageStorage struct {
	lastMessages []*storage.StoredMessage
	counts       map[string]uint64
}

func (s *chatViewMessageStorage) UpsertOneMessage(*storage.StoredMessage) error { return nil }
func (s *chatViewMessageStorage) GetLastMessagesInChats(storage.ChatFilter, storage.Sort, storage.Pagination, bool) ([]*storage.StoredMessage, error) {
	return s.lastMessages, nil
}
func (s *chatViewMessageStorage) GetAllMessages(storage.MessageFilter, storage.Sort, storage.Pagination, bool) ([]*storage.StoredMessage, error) {
	return nil, nil
}
func (s *chatViewMessageStorage) GetChatMessages(types.JID, storage.MessageFilter, storage.Pagination, bool) ([]*storage.StoredMessage, error) {
	return nil, nil
}
func (s *chatViewMessageStorage) GetMessage(types.MessageID) (*storage.StoredMessage, error) {
	return nil, storage.ErrNotFound
}
func (s *chatViewMessageStorage) GetMessageWithRetries(types.MessageID) (*storage.StoredMessage, error) {
	return nil, storage.ErrNotFound
}
func (s *chatViewMessageStorage) DeleteChatMessages(types.JID, time.Time) error { return nil }
func (s *chatViewMessageStorage) DeleteMessage(types.MessageID) error           { return nil }
func (s *chatViewMessageStorage) CountInboundMessagesAfter(map[string]*storage.StoredChatReadState, bool) (map[string]uint64, error) {
	return s.counts, nil
}

type chatViewContactStorage struct{}

func (chatViewContactStorage) GetContact(user types.JID) (*storage.StoredContact, error) {
	return &storage.StoredContact{Jid: user, Name: user.User}, nil
}
func (chatViewContactStorage) GetAllContacts(storage.Sort, storage.Pagination) ([]*storage.StoredContact, error) {
	return nil, nil
}

type chatViewGroupStorage struct{}

func (chatViewGroupStorage) FetchGroups(bool) error                { return nil }
func (chatViewGroupStorage) UpdateGroup(*events.GroupInfo) error   { return nil }
func (chatViewGroupStorage) UpsertOneGroup(*types.GroupInfo) error { return nil }
func (chatViewGroupStorage) GetAllGroups(storage.Sort, storage.Pagination) ([]*types.GroupInfo, error) {
	return nil, nil
}
func (chatViewGroupStorage) GetGroup(types.JID) (*types.GroupInfo, error) { return nil, nil }
func (chatViewGroupStorage) DeleteGroup(types.JID) error                  { return nil }
func (chatViewGroupStorage) DeleteGroups() error                          { return nil }

type chatViewReadStateStorage struct {
	states map[string]*storage.StoredChatReadState
}

func (s *chatViewReadStateStorage) UpsertChatReadState(*storage.StoredChatReadState) (bool, error) {
	return true, nil
}
func (s *chatViewReadStateStorage) ApplyChatReadEvent(storage.ChatReadEvent) (bool, error) {
	return true, nil
}
func (s *chatViewReadStateStorage) GetChatReadStates([]types.JID, bool) (map[string]*storage.StoredChatReadState, error) {
	return s.states, nil
}
func (s *chatViewReadStateStorage) DeleteChatReadState(types.JID) error { return nil }

func TestChatViewExposesKnownAndUnknownUnreadState(t *testing.T) {
	knownJID := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	unknownJID := types.JID{User: "15557654321", Server: types.DefaultUserServer}
	messages := &chatViewMessageStorage{
		lastMessages: []*storage.StoredMessage{
			{Message: &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: knownJID}, Timestamp: time.Unix(20, 0)}}},
			{Message: &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: unknownJID}, Timestamp: time.Unix(10, 0)}}},
		},
		counts: map[string]uint64{knownJID.String(): 2},
	}
	states := &chatViewReadStateStorage{states: map[string]*storage.StoredChatReadState{
		knownJID.String(): {
			Jid:                 knownJID,
			BaselineUnreadCount: 3,
			MarkedAsUnread:      true,
			UnreadStateKnown:    true,
		},
		unknownJID.String(): {
			Jid:              unknownJID,
			MarkedAsUnread:   true,
			UnreadStateKnown: false,
		},
	}}
	view := NewChatView(messages, chatViewContactStorage{}, chatViewGroupStorage{}, states)

	chats, err := view.GetChats(storage.ChatFilter{}, storage.Sort{Field: "timestamp", Order: storage.SortDesc}, storage.Pagination{}, true)
	require.NoError(t, err)
	require.Len(t, chats, 2)
	assert.Equal(t, uint64(5), chats[0].UnreadCount)
	assert.True(t, chats[0].MarkedAsUnread)
	assert.True(t, chats[0].UnreadStateKnown)
	assert.Zero(t, chats[1].UnreadCount)
	assert.True(t, chats[1].MarkedAsUnread)
	assert.False(t, chats[1].UnreadStateKnown)
}
