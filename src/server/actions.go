package server

import (
	"context"
	"errors"
	"github.com/devlikeapro/gows/gows"
	"github.com/devlikeapro/gows/proto"
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	"time"
)

func (s *Server) GetProfilePicture(ctx context.Context, req *__.ProfilePictureRequest) (*__.ProfilePictureResponse, error) {
	jid, err := types.ParseJID(req.GetJid())
	if err != nil {
		return nil, err
	}

	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	info, err := cli.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{
		Preview: false,
	})
	if errors.Is(err, whatsmeow.ErrProfilePictureNotSet) {
		return &__.ProfilePictureResponse{Url: ""}, nil
	}
	if errors.Is(err, whatsmeow.ErrProfilePictureUnauthorized) {
		return &__.ProfilePictureResponse{Url: ""}, nil
	}
	if err != nil {
		return nil, err
	}

	return &__.ProfilePictureResponse{Url: info.URL}, nil
}

func (s *Server) CheckPhones(ctx context.Context, req *__.CheckPhonesRequest) (*__.CheckPhonesResponse, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}

	phones := make([]string, len(req.Phones))
	for i, p := range req.Phones {
		// start with +
		if p[0] != '+' {
			p = "+" + p
		}
		phones[i] = p
	}

	res, err := cli.IsOnWhatsApp(ctx, phones)
	if err != nil {
		return nil, err
	}

	infos := make([]*__.PhoneInfo, len(res))
	for i, r := range res {
		infos[i] = &__.PhoneInfo{
			Phone:      r.Query,
			Jid:        r.JID.String(),
			Registered: r.IsIn,
		}
	}
	return &__.CheckPhonesResponse{Infos: infos}, nil
}

// MarkChatUnread marks a chat as read or unread via app state patch
func (s *Server) MarkChatUnread(ctx context.Context, req *__.ChatUnreadRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	jid, err := types.ParseJID(req.GetJid())
	if err != nil {
		return nil, err
	}

	// Try to fetch the last 1 message from storage for this chat
	var lastKeys []*waCommon.MessageKey
	var lastMessageTimestamp = time.Now()
	messages, err := cli.Storage.Messages.GetChatMessages(
		jid,
		storage.MessageFilter{},
		storage.Pagination{Limit: 1, Offset: 0},
		true,
	)
	if err != nil {
		// If storage access fails, log and continue without the last message key
		cli.Log.Errorf("MarkChatUnread: failed to fetch last message for %s: %v", jid, err)
		return nil, err
	}

	if len(messages) == 0 || messages[0] == nil {
		cli.Log.Errorf("MarkChatUnread: no messages found for %s", jid)
	} else {
		m := messages[0]
		fromMe := m.Info.IsFromMe
		key := &waCommon.MessageKey{
			RemoteJID: proto.String(jid.String()),
			FromMe:    proto.Bool(fromMe),
			ID:        proto.String(m.Info.ID),
		}
		if m.Info.IsGroup {
			participant := m.Info.Sender.ToNonAD().String()
			key.Participant = proto.String(participant)
		}
		lastKeys = []*waCommon.MessageKey{key}
		lastMessageTimestamp = m.Info.Timestamp
	}

	patch := gows.BuildChatUnread(jid, req.GetRead(), lastKeys, lastMessageTimestamp)
	if err = cli.SendAppState(ctx, patch); err != nil {
		return nil, err
	}
	return &__.Empty{}, nil
}

// SetChatArchive archives or unarchives a chat via app state patch.
func (s *Server) SetChatArchive(ctx context.Context, req *__.ChatArchiveRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	jid, err := types.ParseJID(req.GetJid())
	if err != nil {
		return nil, err
	}

	lastMessageKey, lastMessageTimestamp := lastMessageForChatAppState(cli, jid, "SetChatArchive")

	buildPatch := func() appstate.PatchInfo {
		return gows.BuildChatArchive(
			jid,
			req.GetArchive(),
			lastMessageKey,
			lastMessageTimestamp,
		)
	}
	if err = gows.SendChatArchiveAppState(
		ctx,
		cli,
		buildPatch,
		gows.DefaultChatArchiveAppStateAttempts,
		gows.DefaultChatArchiveAppStateDelay,
	); err != nil {
		return nil, err
	}
	return &__.Empty{}, nil
}

// ClearChat clears local chat history via regular_high app state patch.
func (s *Server) ClearChat(ctx context.Context, req *__.ClearChatRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	jid, err := types.ParseJID(req.GetJid())
	if err != nil {
		return nil, err
	}

	lastMessageKey, lastMessageTimestamp := lastMessageForChatAppState(cli, jid, "ClearChat")
	buildPatch := func() appstate.PatchInfo {
		return gows.BuildChatClear(
			jid,
			lastMessageKey,
			lastMessageTimestamp,
		)
	}
	if err = gows.SendChatClearAppState(
		ctx,
		cli,
		buildPatch,
		gows.DefaultChatClearAppStateAttempts,
		gows.DefaultChatClearAppStateDelay,
	); err != nil {
		return nil, err
	}
	if _, err = clearLocalChatMessages(cli.Storage.Messages, jid, lastMessageTimestamp, time.Now); err != nil {
		return nil, err
	}
	return &__.Empty{}, nil
}

func clearLocalChatMessages(
	messages storage.MessageStorage,
	jid types.JID,
	lastMessageTimestamp time.Time,
	now func() time.Time,
) (time.Time, error) {
	if messages == nil {
		return time.Time{}, nil
	}
	deleteBefore := lastMessageTimestamp
	if deleteBefore.IsZero() {
		deleteBefore = now()
	}
	deleteBefore = deleteBefore.Add(time.Second)
	return deleteBefore, messages.DeleteChatMessages(jid, deleteBefore)
}

func lastMessageForChatAppState(
	cli *gows.GoWS,
	jid types.JID,
	action string,
) (*waCommon.MessageKey, time.Time) {
	messages, err := cli.Storage.Messages.GetChatMessages(
		jid,
		storage.MessageFilter{},
		storage.Pagination{Limit: 1, Offset: 0},
		true,
	)
	if err != nil {
		cli.Log.Warnf("%s: failed to fetch last message for %s: %v", action, jid, err)
		return nil, time.Time{}
	}
	if len(messages) == 0 || messages[0] == nil {
		cli.Log.Debugf("%s: no messages found for %s", action, jid)
		return nil, time.Time{}
	}
	m := messages[0]
	lastMessageKey := &waCommon.MessageKey{
		RemoteJID: proto.String(jid.String()),
		FromMe:    proto.Bool(m.Info.IsFromMe),
		ID:        proto.String(m.Info.ID),
	}
	if m.Info.IsGroup {
		participant := m.Info.Sender.ToNonAD().String()
		lastMessageKey.Participant = proto.String(participant)
	}
	return lastMessageKey, m.Info.Timestamp
}
