package server

import (
	"context"
	"errors"

	"github.com/devlikeapro/gows/gows"
	__ "github.com/devlikeapro/gows/proto"
	"github.com/devlikeapro/gows/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func statusHistoryError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return status.Error(codes.NotFound, "status_not_found")
	case errors.Is(err, storage.ErrStatusInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, storage.ErrStatusBinding), errors.Is(err, storage.ErrStatusStaleControl), errors.Is(err, storage.ErrStatusUnauthorized), errors.Is(err, storage.ErrStatusGeneration):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrStatusData):
		return status.Error(codes.DataLoss, err.Error())
	default:
		return status.Error(codes.Unavailable, "status_history_unavailable")
	}
}
func (s *Server) statusHistoryClient(session string, binding storage.StatusBinding) (*gows.GoWS, error) {
	cli, err := s.Sm.Get(session)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "status_session_unavailable")
	}
	if cli.Storage == nil || cli.Storage.StatusHistory == nil {
		return nil, status.Error(codes.Unimplemented, "status_history_unsupported")
	}
	if cli.GetOwnId().User == "" || cli.GetOwnId().User != binding.ExpectedPhone {
		return nil, status.Error(codes.FailedPrecondition, "status_capture_binding_mismatch")
	}
	return cli, nil
}
func (s *Server) ConfigureStatusHistory(ctx context.Context, req *__.StatusHistoryConfigRequest) (*__.Json, error) {
	binding := storage.StatusBinding{AccountID: req.GetAccountId(), ExpectedPhone: req.GetExpectedPhone(), ShardID: req.GetShardId(), Epoch: req.GetEpoch(), AuthorizationSubject: req.GetAuthorizationSubject()}
	cli, err := s.statusHistoryClient(req.GetSession().GetId(), binding)
	if err != nil {
		return nil, err
	}
	if req.GetEnabled() && (!cli.IsConnected() || !cli.IsLoggedIn()) {
		return nil, status.Error(codes.FailedPrecondition, "status_session_not_connected")
	}
	capture, err := cli.Storage.StatusHistory.Configure(ctx, storage.StatusPolicy{StatusBinding: binding, ObservedAtMS: req.GetObservedAtMs(), Enabled: req.GetEnabled(), AuthorizedUntilMS: req.GetAuthorizedUntilMs()})
	if err != nil {
		return nil, statusHistoryError(ctx, err)
	}
	return toJson(capture)
}
func (s *Server) GetStatusHistory(ctx context.Context, req *__.StatusHistoryListRequest) (*__.Json, error) {
	binding := storage.StatusBinding{AccountID: req.GetAccountId(), ExpectedPhone: req.GetExpectedPhone(), ShardID: req.GetShardId(), Epoch: req.GetEpoch(), AuthorizationSubject: req.GetAuthorizationSubject()}
	cli, err := s.statusHistoryClient(req.GetSession().GetId(), binding)
	if err != nil {
		return nil, err
	}
	q := storage.StatusHistoryQuery{StatusBinding: binding, SinceMS: req.GetSinceMs(), UntilMS: req.GetUntilMs(), Limit: req.GetLimit(), Generation: req.GetGeneration()}
	if req.GetBeforeTimestampMs() != 0 || req.GetBeforeId() != "" {
		q.Before = &storage.StatusCursor{TimestampMS: req.GetBeforeTimestampMs(), ID: req.GetBeforeId()}
	}
	page, err := cli.Storage.StatusHistory.List(ctx, q)
	if err != nil {
		return nil, statusHistoryError(ctx, err)
	}
	return toJson(page)
}
func exactStatusRequest(req *__.StatusHistoryExactRequest) storage.StatusExact {
	return storage.StatusExact{StatusBinding: storage.StatusBinding{AccountID: req.GetAccountId(), ExpectedPhone: req.GetExpectedPhone(), ShardID: req.GetShardId(), Epoch: req.GetEpoch(), AuthorizationSubject: req.GetAuthorizationSubject()}, ID: req.GetId(), Sender: req.GetSender(), FromMe: req.GetFromMe(), Generation: req.GetGeneration()}
}
func (s *Server) GetStatusHistoryMessage(ctx context.Context, req *__.StatusHistoryExactRequest) (*__.Json, error) {
	q := exactStatusRequest(req)
	cli, err := s.statusHistoryClient(req.GetSession().GetId(), q.StatusBinding)
	if err != nil {
		return nil, err
	}
	message, err := cli.Storage.StatusHistory.Exact(ctx, q)
	if err != nil {
		return nil, statusHistoryError(ctx, err)
	}
	return toJson(message)
}
