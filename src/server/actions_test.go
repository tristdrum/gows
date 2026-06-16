package server

import (
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
)

type recordingMessageStorage struct {
	deletedJID   types.JID
	deleteBefore time.Time
}

func (s *recordingMessageStorage) UpsertOneMessage(*storage.StoredMessage) error {
	return nil
}

func (s *recordingMessageStorage) GetLastMessagesInChats(storage.ChatFilter, storage.Sort, storage.Pagination, bool) ([]*storage.StoredMessage, error) {
	return nil, nil
}

func (s *recordingMessageStorage) GetAllMessages(storage.MessageFilter, storage.Sort, storage.Pagination, bool) ([]*storage.StoredMessage, error) {
	return nil, nil
}

func (s *recordingMessageStorage) GetChatMessages(types.JID, storage.MessageFilter, storage.Pagination, bool) ([]*storage.StoredMessage, error) {
	return nil, nil
}

func (s *recordingMessageStorage) GetMessage(types.MessageID) (*storage.StoredMessage, error) {
	return nil, nil
}

func (s *recordingMessageStorage) GetMessageWithRetries(types.MessageID) (*storage.StoredMessage, error) {
	return nil, nil
}

func (s *recordingMessageStorage) DeleteChatMessages(jid types.JID, deleteBefore time.Time) error {
	s.deletedJID = jid
	s.deleteBefore = deleteBefore
	return nil
}

func (s *recordingMessageStorage) DeleteMessage(types.MessageID) error {
	return nil
}

func TestClearLocalChatMessagesDeletesThroughLastKnownMessage(t *testing.T) {
	jid, err := types.ParseJID("123456789-123456@g.us")
	if err != nil {
		t.Fatal(err)
	}
	lastMessageAt := time.Date(2026, 6, 16, 14, 22, 1, 0, time.UTC)
	store := &recordingMessageStorage{}

	deleteBefore, err := clearLocalChatMessages(
		store,
		jid,
		lastMessageAt,
		func() time.Time { return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC) },
	)
	if err != nil {
		t.Fatal(err)
	}

	if store.deletedJID != jid {
		t.Fatalf("deleted JID = %s, want %s", store.deletedJID, jid)
	}
	if want := lastMessageAt.Add(time.Second); !deleteBefore.Equal(want) {
		t.Fatalf("deleteBefore = %s, want %s", deleteBefore, want)
	}
	if !store.deleteBefore.Equal(deleteBefore) {
		t.Fatalf("store deleteBefore = %s, want %s", store.deleteBefore, deleteBefore)
	}
}
