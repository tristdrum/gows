package storage

import "go.mau.fi/whatsmeow/types/events"

// IsRealMessage classifies the already-unwrapped provider content used by storage.
func IsRealMessage(event *events.Message) bool {
	if event == nil || event.Message == nil {
		return false
	}
	msg := event.Message
	switch {
	case msg.Conversation != nil:
		return true
	case msg.ExtendedTextMessage != nil:
		return true
	case msg.ImageMessage != nil:
		return true
	case msg.ContactMessage != nil:
		return true
	case msg.LocationMessage != nil:
		return true
	case msg.VideoMessage != nil:
		return true
	case msg.PtvMessage != nil:
		return true
	case msg.AudioMessage != nil:
		return true
	case msg.DocumentMessage != nil:
		return true
	case msg.DocumentWithCaptionMessage != nil:
		return true
	case msg.StickerMessage != nil:
		return true
	case msg.ContactsArrayMessage != nil:
		return true
	case msg.TemplateMessage != nil:
		return true
	case msg.ListMessage != nil:
		return true
	case msg.RichResponseMessage != nil:
		return true
	case msg.PollCreationMessage != nil:
		return true
	case msg.PollCreationMessageV2 != nil:
		return true
	case msg.PollCreationMessageV3 != nil:
		return true
	case msg.PollCreationMessageV4 != nil:
		return true
	case msg.PollCreationMessageV5 != nil:
		return true
	case msg.ButtonsResponseMessage != nil:
		return true
	default:
		return false
	}
}
