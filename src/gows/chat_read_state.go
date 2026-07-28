package gows

import (
	"time"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func chatReadStateFromConversation(conv *waHistorySync.Conversation, jid types.JID) (*storage.StoredChatReadState, bool) {
	if conv == nil || (conv.UnreadCount == nil && conv.MarkedAsUnread == nil) {
		return nil, false
	}
	watermark := conversationMessageWatermark(conv)
	return &storage.StoredChatReadState{
		Jid:                 jid,
		BaselineUnreadCount: uint64(conv.GetUnreadCount()),
		MarkedAsUnread:      conv.GetMarkedAsUnread(),
		UnreadStateKnown:    conv.UnreadCount != nil,
		CountFrom:           watermark,
		EvidenceTimestamp:   watermark,
	}, true
}

func conversationMessageWatermark(conv *waHistorySync.Conversation) time.Time {
	timestamp := conv.GetConversationTimestamp()
	if timestamp == 0 {
		timestamp = conv.GetLastMsgTimestamp()
	}
	if timestamp == 0 {
		for _, historyMessage := range conv.GetMessages() {
			messageTimestamp := historyMessage.GetMessage().GetMessageTimestamp()
			if messageTimestamp > timestamp {
				timestamp = messageTimestamp
			}
		}
	}
	return time.Unix(int64(timestamp), 0)
}

func chatReadEventFromEvent(event *events.MarkChatAsRead) (storage.ChatReadEvent, bool) {
	if event == nil || event.Action == nil || event.Action.Read == nil {
		return storage.ChatReadEvent{}, false
	}
	watermark := event.Timestamp
	if messageRange := event.Action.GetMessageRange(); messageRange != nil && messageRange.GetLastMessageTimestamp() > 0 {
		watermark = time.Unix(messageRange.GetLastMessageTimestamp(), 0)
	}
	evidenceTimestamp := event.Timestamp
	if evidenceTimestamp.IsZero() {
		evidenceTimestamp = watermark
	}
	return storage.ChatReadEvent{
		Jid:              event.JID,
		Timestamp:        evidenceTimestamp,
		Read:             event.Action.GetRead(),
		MessageWatermark: watermark,
	}, true
}
