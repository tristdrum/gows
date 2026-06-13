package gows

import (
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type ConnectedEventData struct {
	ID       *types.JID
	LID      *types.JID
	PushName string
}

type EventMessageResponse struct {
	*events.Message
	EventResponse *waE2E.EventResponseMessage
}

type PollVoteEvent struct {
	*events.Message
	Votes *[]string
}
