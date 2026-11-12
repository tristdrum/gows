package server

import (
	"context"

	__ "github.com/devlikeapro/gows/proto"
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) GetMaintenanceHistory(ctx context.Context, req *__.MaintenanceHistoryRequest) (*__.Json, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, status.Error(codes.Unavailable, "maintenance_session_unavailable")
	}
	if cli.Maintenance == nil {
		return nil, status.Error(codes.Unimplemented, "maintenance_history_unavailable")
	}
	chatID := req.GetChatId()
	if chatID != "" {
		jid, parseErr := types.ParseJID(chatID)
		if parseErr != nil {
			return nil, status.Error(codes.InvalidArgument, "maintenance_chat_invalid")
		}
		if jid.Server == types.HiddenUserServer {
			pn, mappingErr := cli.Store.LIDs.GetPNForLID(ctx, jid)
			if mappingErr != nil || pn.IsEmpty() {
				return nil, status.Error(codes.FailedPrecondition, "maintenance_identity_unknown")
			}
			chatID = pn.ToNonAD().String()
		}
	}
	page, err := cli.Maintenance.Page(ctx, storage.MaintenanceQuery{From: req.GetFromTimestamp(), To: req.GetToTimestamp(), Limit: int(req.GetLimit()), Cursor: req.GetCursor(), ChatID: chatID, Token: req.GetToken()})
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.Unavailable, "maintenance_history_read_failed")
	}
	return toJson(page)
}
