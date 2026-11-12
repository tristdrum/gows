package gows

import (
	"context"
	"errors"
	"runtime/debug"
	"time"

	"github.com/avast/retry-go"
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// StorageEventHandler handles events from WhatsApp and stores them in the database.
// It can be configured to ignore events from certain types of JIDs based on their server type.
type StorageEventHandler struct {
	gows    *GoWS
	log     waLog.Logger
	storage *storage.Storage
	// ignoreJids specifies which types of JIDs should be ignored when processing events.
	ignoreJids *IgnoreJidsConfig
}

func (st *StorageEventHandler) shouldIgnoreJID(jid types.JID) bool {
	if jid.IsEmpty() {
		return false
	}

	if st.ignoreJids == nil {
		return false
	}

	// Check if the JID should be ignored based on its server type
	switch jid.Server {
	case types.BroadcastServer:
		// Do not filter status@broadcast when broadcast-ignore is enabled:
		// it is controlled by the dedicated Status flag.
		if jid.User == "status" {
			return st.ignoreJids.Status
		}
		return st.ignoreJids.Broadcast
	case types.GroupServer:
		return st.ignoreJids.Groups
	case types.NewsletterServer:
		return st.ignoreJids.Newsletters
	default:
		return false
	}
}

func (st *StorageEventHandler) GetMessageForRetry(requester, to types.JID, id types.MessageID) *waE2E.Message {
	msg, err := st.storage.Messages.GetMessage(id)
	if err != nil {
		st.log.Errorf("Error getting message for retry - requester %v, to %v, id %v: %v", requester, to, id, err)
		return nil
	}
	if msg == nil || msg.Message == nil {
		st.log.Warnf("No message for retry - requester %v, to %v, id %v", requester, to, id)
		return nil
	}
	return msg.Message.RawMessage
}

func (st *StorageEventHandler) handleEvent(event interface{}) {
	// Handle all panic and log error + stack
	defer func() {
		if err := recover(); err != nil {
			stack := debug.Stack()
			st.log.Errorf("Panic happened in handle event: %v. Stack: %s. Event: %v", err, stack, event)
		}
	}()

	if st.handleStatusHistoryEvent(event, time.Now().UnixMilli()) {
		return
	}
	switch event.(type) {
	case *events.Message:
		msg := event.(*events.Message)
		if st.shouldIgnoreJID(msg.Info.Chat) {
			return
		}
		var status storage.Status
		if msg.Info.IsFromMe {
			status = storage.StatusServerAck
		} else {
			status = storage.StatusDeliveryAck
		}
		st.handleSaveMessage(msg, &status)
		st.handleMessageEvent(msg)
	case *events.Receipt:
		receipt := event.(*events.Receipt)
		if st.shouldIgnoreJID(receipt.Chat) {
			return
		}
		st.handleReceipt(receipt)
	case *events.HistorySync:
		st.handleHistorySync(event.(*events.HistorySync))
	case *events.MarkChatAsRead:
		markRead := event.(*events.MarkChatAsRead)
		if st.shouldIgnoreJID(markRead.JID) {
			return
		}
		st.handleMarkChatAsRead(markRead)
	// Groups
	case *events.JoinedGroup:
		group := event.(*events.JoinedGroup)
		if st.shouldIgnoreJID(group.JID) {
			return
		}
		st.handleMeJoinedGroup(group)
	case *events.GroupInfo:
		info := event.(*events.GroupInfo)
		if st.shouldIgnoreJID(info.JID) {
			return
		}
		left := st.handleMeLeftGroup(info)
		if left {
			return
		}
		st.handleGroupInfo(info)
	case *events.DeleteChat:
		deleteChat := event.(*events.DeleteChat)
		if st.shouldIgnoreJID(deleteChat.JID) {
			return
		}
		st.handleDeleteChat(deleteChat)
	case *events.Contact:
		contact := event.(*events.Contact)
		if st.shouldIgnoreJID(contact.JID) {
			return
		}
		st.handleContact(contact)
	// Labels
	case *events.LabelEdit:
		st.handleLabelEdit(event.(*events.LabelEdit))
	case *events.LabelAssociationChat:
		labelAssoc := event.(*events.LabelAssociationChat)
		if st.shouldIgnoreJID(labelAssoc.JID) {
			return
		}
		st.handleLabelAssociationChat(labelAssoc)
	}
}

func (st *StorageEventHandler) handleSaveMessage(event *events.Message, status *storage.Status) error {
	messageToStore := &storage.StoredMessage{
		Message: event,
		Status:  status,
		IsReal:  storage.IsRealMessage(event),
	}

	err := st.storage.Messages.UpsertOneMessage(messageToStore)
	if err != nil {
		st.log.Errorf("Error storing message %v(%v): %v", event.Info.Chat, event.Info.ID, err)
	}
	return err
}

func (st *StorageEventHandler) handleMessageEvent(event *events.Message) error {
	if event.Message == nil {
		return nil
	}
	protocolMessage := event.Message.GetProtocolMessage()
	if protocolMessage != nil && protocolMessage.GetType() == waE2E.ProtocolMessage_REVOKE {
		id := protocolMessage.GetKey().GetID()
		if id == "" {
			return errors.New("revoke has no target identity")
		}
		if st.gows != nil && st.gows.Maintenance != nil {
			if err := st.gows.Maintenance.Revoke(st.gows.Context, id); err != nil {
				return err
			}
		}
		return st.storage.Messages.DeleteMessage(id)
	}
	if protocolMessage != nil {
		if setting := ExtractEphemeralSettingsFromProtocolMessage(event.Info, protocolMessage); setting != nil {
			return st.storage.ChatEphemeralSetting.UpdateChatEphemeralSetting(setting)
		}
	}
	if setting := ExtractEphemeralSettingsFromMsg(event); setting != nil {
		return st.storage.ChatEphemeralSetting.UpdateChatEphemeralSetting(setting)
	}
	return nil
}

func (st *StorageEventHandler) handleReceipt(event *events.Receipt) {
	var status storage.Status
	switch event.Type {
	case types.ReceiptTypeDelivered:
		status = storage.StatusDeliveryAck
	case types.ReceiptTypeRead:
		status = storage.StatusRead
	case types.ReceiptTypePlayed:
		status = storage.StatusPlayed
	default:
		st.log.Debugf("Unknown receipt type: %v", event.Type)
		return
	}
	if event.Chat.Server == types.StatusBroadcastJID.Server && event.Chat.User == types.StatusBroadcastJID.User {
		// Ignore
		st.log.Debugf("Ignoring receipt for '%v(%v)'", event.Chat, event.MessageIDs)
	}
	for _, id := range event.MessageIDs {
		st.log.Debugf("Updating status for message %v(%v) to %v (receipt type: '%v')", event.Chat, id, status, event.Type.GoString())
		err := retry.Do(func() error {
			return st.storage.Messages.UpdateMessageStatus(id, status)
		}, retry.Attempts(6), retry.RetryIf(func(err error) bool {
			return errors.Is(err, storage.ErrNotFound)
		}))
		if errors.Is(err, storage.ErrNotFound) {
			st.log.Debugf("Message %v(%v) not found", event.Chat, id)
			continue
		}
		if err != nil {
			st.log.Errorf("Error updating status for message %v(%v): %v", event.Chat, id, err)
			continue
		}
		st.log.Debugf("Updated status for message %v(%v) to %v", event.Chat, id, status)
	}
}

func (st *StorageEventHandler) handleHistorySync(event *events.HistorySync) error {
	return st.handleHistorySyncContext(st.historyContext(), event)
}

func (st *StorageEventHandler) handleHistorySyncContext(ctx context.Context, event *events.HistorySync) error {
	var result error
	for _, conv := range event.Data.GetConversations() {
		jid, err := types.ParseJID(conv.GetID())
		if err != nil {
			return err
		}
		if st.shouldIgnoreJID(jid) {
			continue
		}
		result = errors.Join(result, st.saveHistoryForOneChatContext(ctx, conv, jid))
		if ctx.Err() != nil {
			return errors.Join(result, ctx.Err())
		}
	}
	return result
}

func (st *StorageEventHandler) handleMarkChatAsRead(event *events.MarkChatAsRead) {
	if st.storage.ChatReadState == nil {
		return
	}
	readEvent, ok := chatReadEventFromEvent(event)
	if !ok {
		return
	}
	if _, err := st.storage.ChatReadState.ApplyChatReadEvent(readEvent); err != nil {
		st.log.Errorf("Error storing chat read state for %v: %v", event.JID, err)
	}
}

func (st *StorageEventHandler) saveHistoryForOneChat(conv *waHistorySync.Conversation, chatJID types.JID) error {
	return st.saveHistoryForOneChatContext(st.historyContext(), conv, chatJID)
}

func (st *StorageEventHandler) saveHistoryForOneChatContext(ctx context.Context, conv *waHistorySync.Conversation, chatJID types.JID) error {
	for _, historyMsg := range conv.GetMessages() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		msg, err := st.gows.ParseWebMessage(chatJID, historyMsg.GetMessage())
		if err != nil {
			return err
		}
		var status storage.Status
		if msg.SourceWebMsg != nil && msg.SourceWebMsg.Status != nil {
			status = storage.Status(*msg.SourceWebMsg.Status)
		}
		if err = st.handleSaveMessage(msg, &status); err != nil {
			return err
		}
		if err = st.observeMaintenanceMessage(msg); err != nil {
			return err
		}
		if err = st.handleMessageEvent(msg); err != nil {
			return err
		}
	}
	if setting := ExtractEphemeralSettingsFromConversation(conv, chatJID); setting != nil {
		if err := st.storage.ChatEphemeralSetting.UpdateChatEphemeralSetting(setting); err != nil {
			return err
		}
	}
	if st.storage.ChatReadState != nil {
		if state, ok := chatReadStateFromConversation(conv, chatJID); ok {
			if _, err := st.storage.ChatReadState.UpsertChatReadState(state); err != nil {
				return err
			}
		}
	}
	return nil
}

func (st *StorageEventHandler) handleMeJoinedGroup(group *events.JoinedGroup) {
	err := st.storage.Groups.UpsertOneGroup(&group.GroupInfo)
	if err != nil {
		st.log.Errorf("Error storing group %v: %v", group.JID, err)
	}
	st.log.Debugf("I joined group %v", group.JID)
}

func (st *StorageEventHandler) handleMeLeftGroup(info *events.GroupInfo) bool {
	jid := st.gows.Store.ID
	if jid == nil {
		// Not logged in, we can't tell if it's us who left
		return false
	}
	for _, leave := range info.Leave {
		if leave == jid.ToNonAD() {
			st.log.Debugf("I left group %v", info.JID)
			err := st.storage.Groups.DeleteGroup(info.JID)
			if err != nil {
				st.log.Errorf("Error deleting group %v: %v", info.JID, err)
			}
			return true
		}
	}
	return false
}

func (st *StorageEventHandler) handleGroupInfo(info *events.GroupInfo) {
	err := retry.Do(func() error {
		return st.storage.Groups.UpdateGroup(info)
	})
	if err != nil {
		st.log.Errorf("Error updating group %v: %v", info.JID, err)
	}
	return
}

func (st *StorageEventHandler) handleDeleteChat(event *events.DeleteChat) {
	err := st.storage.Messages.DeleteChatMessages(event.JID, event.Timestamp)
	if err != nil {
		st.log.Errorf("Error deleting chat messages %v: %v", event.JID, err)
	}
	err = st.storage.ChatEphemeralSetting.DeleteChatEphemeralSetting(event.JID, event.Timestamp)
	if err != nil {
		st.log.Errorf("Error deleting chat ephemeral setting %v: %v", event.JID, err)
	}
	if st.storage.ChatReadState != nil {
		if err := st.storage.ChatReadState.DeleteChatReadState(event.JID); err != nil {
			st.log.Errorf("Error deleting chat read state %v: %v", event.JID, err)
		}
	}
	st.log.Debugf("Deleted chat %v", event.JID)
}

func (st *StorageEventHandler) handleLabelEdit(event *events.LabelEdit) {
	if event.Action == nil {
		return
	}
	action := event.Action
	// Delete
	if action.GetDeleted() {
		err := st.storage.Labels.DeleteLabel(event.LabelID)
		if err != nil {
			st.log.Errorf("Error deleting label %v: %v", event.LabelID, err)
			return
		}
		st.log.Debugf("Deleted label %v", event.LabelID)
		return
	}

	// Create
	label := &storage.Label{
		ID:    event.LabelID,
		Name:  action.GetName(),
		Color: int(action.GetColor()),
	}

	err := st.storage.Labels.UpsertLabel(label)
	if err != nil {
		st.log.Errorf("Error upserting label %v: %v", label.ID, err)
		return
	}

	st.log.Debugf("Upserted label %v (%v)", label.ID, label.Name)
}

func (st *StorageEventHandler) handleLabelAssociationChat(event *events.LabelAssociationChat) {
	// Get the association data directly from the event
	jid := event.JID
	labelID := event.LabelID
	labeled := event.Action.GetLabeled()

	if labeled {
		// Make sure we have the label
		// it can happen on full sync or event disordering
		err := retry.Do(
			func() error {
				_, err := st.storage.Labels.GetLabelById(labelID)
				if err != nil {
					return err
				}
				return nil
			},
			retry.Attempts(6),
			retry.Delay(100*time.Millisecond),
		)

		err = st.storage.LabelAssociations.AddAssociation(jid, labelID)
		if err != nil {
			st.log.Errorf("Error adding label association for JID %v and label %v: %v", jid, labelID, err)
			return
		}
		st.log.Debugf("Added label association for JID %v and label %v", jid, labelID)
	} else {
		err := st.storage.LabelAssociations.RemoveAssociation(jid, labelID)
		if err != nil {
			st.log.Errorf("Error removing label association for JID %v and label %v: %v", jid, labelID, err)
			return
		}
		st.log.Debugf("Removed label association for JID %v and label %v", jid, labelID)
	}
}

func (st *StorageEventHandler) handleContact(contact *events.Contact) {
	st.handleContactLidJidMapping(contact)
}

func (st *StorageEventHandler) handleContactLidJidMapping(contact *events.Contact) error {
	ctx := st.gows.Context
	cli := st.gows.Client

	// Save lid to jid mapping
	if cli == nil || cli.Store == nil || cli.Store.LIDs == nil {
		return nil
	}

	var err error
	var lid = types.EmptyJID
	var jid = types.EmptyJID

	act := contact.Action
	switch contact.JID.Server {
	// jid => lid
	case types.DefaultUserServer:
		jid = contact.JID
		if act.GetLidJID() != "" {
			lid, err = types.ParseJID(act.GetLidJID())
			if err != nil {
				st.log.Errorf("Failed to parse LID JID: %v", err)
				return err
			}
		}

	// lid => jid
	case types.HiddenUserServer:
		lid = contact.JID
		if act.GetPnJID() != "" {
			jid, err = types.ParseJID(act.GetPnJID())
			if err != nil {
				st.log.Errorf("Failed to parse PN JID: %v", err)
				return err
			}
		}
	}

	// Save lid to jid mapping
	if lid == types.EmptyJID || jid == types.EmptyJID {
		return nil
	}
	err = cli.Store.LIDs.PutLIDMapping(ctx, lid, jid)
	if err != nil {
		st.log.Errorf("Failed to update LID mapping (%v => %v): %v", lid, jid, err)
		return err
	}
	if st.gows.Maintenance != nil {
		return st.gows.Maintenance.MapIdentity(ctx, lid.String(), jid.String())
	}
	return nil
}

func (st *StorageEventHandler) historyContext() context.Context {
	if st.gows != nil && st.gows.Context != nil {
		return st.gows.Context
	}
	return context.Background()
}
