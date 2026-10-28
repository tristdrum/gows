package storage

import (
	"context"
	"errors"
)

var ErrUnreadStateUnavailable = errors.New("Unread chat state unavailable")
var ErrChatIndexDataUnavailable = errors.New("Chat index data unavailable")
var ErrChatIndexUnsupported = errors.New("Chat index unavailable")

const MaxChatIndexScan = 1000

type ChatIndexPage struct {
	Chats      []ChatIndexEntry    `json:"chats"`
	SourcePage ChatIndexSourcePage `json:"source_page"`
}

type ChatIndexEntry struct {
	Chat    *StoredChat    `json:"chat"`
	Message *StoredMessage `json:"message"`
}

type ChatIndexSourcePage struct {
	Version      int     `json:"version"`
	Offset       uint64  `json:"offset"`
	Limit        uint64  `json:"limit"`
	ScannedCount uint64  `json:"scanned_count"`
	NextOffset   *uint64 `json:"next_offset"`
	Complete     bool    `json:"complete"`
}

// ChatIndexStorage is optional: an unsupported store must not fall back to the
// legacy overview's network requests or present empty history as success.
type ChatIndexStorage interface {
	GetChatIndex(context.Context, Pagination, bool) (*ChatIndexPage, error)
}

type ContextMessageStorage interface {
	GetAllMessagesContext(context.Context, MessageFilter, Sort, Pagination, bool) ([]*StoredMessage, error)
}

type ContactNamesStorage interface {
	GetContactNames(context.Context, []string) (map[string]string, error)
}
