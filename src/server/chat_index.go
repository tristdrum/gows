package server

import (
	"context"
	"errors"
	"regexp"
	"time"

	__ "github.com/devlikeapro/gows/proto"
	"github.com/devlikeapro/gows/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var historyRequestID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func (s *Server) GetChatIndex(ctx context.Context, req *__.GetChatIndexRequest) (response *__.Json, resultErr error) {
	started := time.Now()
	requestID := ""
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		values := incoming.Get("x-min-request-id")
		if len(values) == 1 && historyRequestID.MatchString(values[0]) {
			requestID = values[0]
		}
	}
	scanned, returned := uint64(0), 0
	defer func() {
		if s.log != nil {
			s.log.Infof("history_read operation=chat_index request_id=%s duration_ms=%d scanned_count=%d result_count=%d status=%s", requestID, time.Since(started).Milliseconds(), scanned, returned, status.Code(resultErr))
		}
	}()
	if req.GetPagination().GetLimit() < 1 || req.GetPagination().GetLimit() > 50 {
		return nil, status.Error(codes.InvalidArgument, "Chat index limit must be 1 to 50")
	}
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, status.Error(codes.Unavailable, "Chat index session unavailable")
	}
	index, ok := cli.Storage.Chats.(storage.ChatIndexStorage)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "Chat index unavailable")
	}
	page, err := index.GetChatIndex(ctx, toPagination(req.GetPagination()), req.GetUnreadOnly())
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		switch {
		case errors.Is(err, storage.ErrUnreadStateUnavailable):
			return nil, status.Error(codes.FailedPrecondition, storage.ErrUnreadStateUnavailable.Error())
		case errors.Is(err, storage.ErrChatIndexDataUnavailable):
			return nil, status.Error(codes.DataLoss, storage.ErrChatIndexDataUnavailable.Error())
		case errors.Is(err, storage.ErrChatIndexUnsupported):
			return nil, status.Error(codes.Unimplemented, storage.ErrChatIndexUnsupported.Error())
		default:
			return nil, status.Error(codes.Unavailable, "Chat index read failed")
		}
	}
	scanned, returned = page.SourcePage.ScannedCount, len(page.Chats)
	response, err = toJson(page)
	if err != nil {
		return nil, status.Error(codes.DataLoss, "Chat index encoding failed")
	}
	return response, nil
}
