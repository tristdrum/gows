package views

import (
	"context"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
)

func (s ChatView) GetChatIndex(ctx context.Context, pagination storage.Pagination, unreadOnly bool) (*storage.ChatIndexPage, error) {
	messages, ok := s.Messages.(storage.ChatIndexStorage)
	if !ok {
		return nil, storage.ErrChatIndexUnsupported
	}
	page, err := messages.GetChatIndex(ctx, pagination, unreadOnly)
	if err != nil {
		return nil, err
	}
	jids := []string{}
	for _, entry := range page.Chats {
		if entry.Chat.Jid.Server == types.DefaultUserServer {
			jids = append(jids, entry.Chat.Jid.String())
		}
	}
	if len(jids) == 0 {
		return page, nil
	}
	contacts, ok := s.Contacts.(storage.ContactNamesStorage)
	if !ok {
		return nil, storage.ErrChatIndexUnsupported
	}
	names, err := contacts.GetContactNames(ctx, jids)
	if err != nil {
		return nil, err
	}
	for _, entry := range page.Chats {
		if name, ok := names[entry.Chat.Jid.String()]; ok {
			entry.Chat.Name = name
		}
	}
	return page, ctx.Err()
}
