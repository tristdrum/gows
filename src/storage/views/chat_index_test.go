package views

import (
	"context"
	"testing"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/types"
)

type indexViewMessages struct {
	chatViewMessageStorage
	page  *storage.ChatIndexPage
	calls int
}

func (s *indexViewMessages) GetChatIndex(ctx context.Context, pagination storage.Pagination, unreadOnly bool) (*storage.ChatIndexPage, error) {
	s.calls++
	return s.page, ctx.Err()
}

type indexViewContacts struct {
	chatViewContactStorage
	calls int
}

func (s *indexViewContacts) GetContactNames(ctx context.Context, jids []string) (map[string]string, error) {
	s.calls++
	result := map[string]string{}
	for _, jid := range jids {
		result[jid] = "Stored fixture name"
	}
	return result, ctx.Err()
}

func TestCompactChatViewBatchesNamesWithoutLegacyCalls(t *testing.T) {
	messages := &indexViewMessages{page: &storage.ChatIndexPage{Chats: []storage.ChatIndexEntry{}}}
	for i := 0; i < 50; i++ {
		messages.page.Chats = append(messages.page.Chats, storage.ChatIndexEntry{Chat: &storage.StoredChat{Jid: types.NewJID("15550001111", types.DefaultUserServer)}})
	}
	contacts := &indexViewContacts{}
	view := NewChatView(messages, contacts, chatViewGroupStorage{}, nil)
	page, err := view.GetChatIndex(context.Background(), storage.Pagination{Limit: 50}, false)
	require.NoError(t, err)
	require.Len(t, page.Chats, 50)
	require.Equal(t, 1, messages.calls)
	require.Equal(t, 1, contacts.calls)
	require.Equal(t, "Stored fixture name", page.Chats[0].Chat.Name)
}

func TestCompactChatViewDoesNotFallbackWhenUnsupported(t *testing.T) {
	view := NewChatView(&chatViewMessageStorage{}, chatViewContactStorage{}, chatViewGroupStorage{}, nil)
	_, err := view.GetChatIndex(context.Background(), storage.Pagination{Limit: 50}, false)
	require.ErrorIs(t, err, storage.ErrChatIndexUnsupported)
}
