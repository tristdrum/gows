package gows

import (
	"context"
	"errors"
	"time"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// DownloadHistorySync(..., true) waits for WhatsMeow's auxiliary stores but
// logs several errors instead of returning them. Repeat the relevant public
// store writes with checked errors before acknowledging the notification.
func (g *GoWS) persistHistoryAuxiliary(ctx context.Context, blob *waHistorySync.HistorySync) error {
	mappings := make([]store.LIDMapping, 0, len(blob.GetPhoneNumberToLidMappings()))
	for _, mapping := range blob.GetPhoneNumberToLidMappings() {
		pn, err := types.ParseJID(mapping.GetPnJID())
		if err != nil {
			return err
		}
		if pn.Server == types.LegacyUserServer {
			pn.Server = types.DefaultUserServer
		}
		lid, err := types.ParseJID(mapping.GetLidJID())
		if err != nil {
			return err
		}
		if pn.IsEmpty() || lid.IsEmpty() || pn.Server != types.DefaultUserServer || lid.Server != types.HiddenUserServer {
			return errors.New("history mapping identity invalid")
		}
		mappings = append(mappings, store.LIDMapping{PN: pn, LID: lid})
	}
	if len(mappings) > 0 {
		if err := g.Store.LIDs.PutManyLIDMappings(ctx, mappings); err != nil {
			return err
		}
	}
	for _, mapping := range mappings {
		pn, err := g.Store.LIDs.GetPNForLID(ctx, mapping.LID)
		if err != nil {
			return err
		}
		if pn != mapping.PN {
			return errors.New("history mapping readback mismatch")
		}
		if err = g.Maintenance.MapIdentity(ctx, mapping.LID.String(), mapping.PN.String()); err != nil {
			return err
		}
	}
	if salt := blob.GetNctSalt(); len(salt) > 0 {
		if err := g.Store.NCTSalt.PutNCTSalt(ctx, salt); err != nil {
			return err
		}
	}
	for _, push := range blob.GetPushnames() {
		jid, err := types.ParseJID(push.GetID())
		if err != nil {
			return err
		}
		if _, _, err = g.Store.Contacts.PutPushName(ctx, jid, push.GetPushname()); err != nil {
			return err
		}
	}
	own := g.GetOwnId().ToNonAD()
	for _, conv := range blob.GetConversations() {
		chat, err := types.ParseJID(conv.GetID())
		if err != nil {
			return err
		}
		if !maintenanceOrdinaryJID(chat) || g.storageEventHandler.shouldIgnoreJID(chat) {
			continue
		}
		canonical, err := g.maintenanceCanonical(chat)
		if err != nil {
			return err
		}
		pn, err := types.ParseJID(canonical)
		if err != nil {
			return err
		}
		if conv.GetTcToken() != nil && pn.Server == types.DefaultUserServer {
			token := store.PrivacyToken{User: pn, Token: conv.GetTcToken(), Timestamp: time.Unix(int64(conv.GetTcTokenTimestamp()), 0), SenderTimestamp: time.Unix(int64(conv.GetTcTokenSenderTimestamp()), 0)}
			if err = g.Store.PrivacyTokens.PutPrivacyTokens(ctx, token); err != nil {
				return err
			}
		}
		secrets := []store.MessageSecretInsert{}
		for _, entry := range conv.GetMessages() {
			m := entry.GetMessage()
			if m.GetMessageSecret() == nil {
				continue
			}
			key := m.GetKey()
			sender := chat
			if key.GetFromMe() {
				sender = own
			} else if chat.Server == types.GroupServer {
				participant := key.GetParticipant()
				if participant == "" {
					participant = m.GetParticipant()
				}
				sender, err = types.ParseJID(participant)
				if err != nil {
					return err
				}
			}
			if sender.IsEmpty() || key.GetID() == "" {
				return errors.New("history secret identity unavailable")
			}
			secrets = append(secrets, store.MessageSecretInsert{Chat: chat, Sender: sender, ID: key.GetID(), Secret: m.GetMessageSecret()})
		}
		if len(secrets) > 0 {
			if err = g.Store.MsgSecrets.PutMessageSecrets(ctx, secrets); err != nil {
				return err
			}
		}
	}
	return nil
}
