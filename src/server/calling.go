package server

import (
	"context"
	"errors"
	"io"

	"github.com/devlikeapro/gows/calling"
	pb "github.com/devlikeapro/gows/proto"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) voice(session string) (*calling.Manager, error) {
	cli, err := s.Sm.Get(session)
	if err != nil {
		return nil, status.Error(codes.NotFound, "session not found")
	}
	if cli.Voice == nil {
		return nil, status.Error(codes.FailedPrecondition, "calling is not enabled for this session")
	}
	if !cli.IsConnected() {
		return nil, status.Error(codes.Unavailable, "session is not connected")
	}
	return cli.Voice, nil
}
func (s *Server) Control(ctx context.Context, req *pb.CallControl) (*pb.CallStatus, error) {
	m, err := s.voice(req.GetSession())
	if err != nil {
		return nil, err
	}
	var result calling.Status
	switch req.GetAction() {
	case "dial":
		peer, parseErr := types.ParseJID(req.GetPeer())
		if parseErr != nil || peer.User == "" || (peer.Server != types.DefaultUserServer && peer.Server != types.HiddenUserServer) {
			return nil, status.Error(codes.InvalidArgument, "a canonical one-to-one peer JID is required")
		}
		result, err = m.Dial(ctx, peer.ToNonAD().String(), req.GetRequestId())
	case "accept":
		err = m.Accept(req.GetCallId())
		if err == nil {
			result, err = m.Status(req.GetCallId())
		}
	case "hangup":
		err = m.Hangup(req.GetCallId())
		result = calling.Status{ID: req.GetCallId(), State: "ended"}
	case "status":
		result, err = m.Status(req.GetCallId())
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown call action")
	}
	if err != nil {
		return nil, callError(err)
	}
	return &pb.CallStatus{CallId: result.ID, Peer: result.Peer, State: result.State, Direction: result.Direction}, nil
}
func callError(err error) error {
	switch {
	case errors.Is(err, calling.ErrCallNotFound):
		return status.Error(codes.NotFound, "call not found")
	case errors.Is(err, calling.ErrBusy), errors.Is(err, calling.ErrMediaOwned), errors.Is(err, calling.ErrAttemptUsed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, calling.ErrMediaBackpressure):
		return status.Error(codes.ResourceExhausted, "media backlog exceeded")
	case errors.Is(err, calling.ErrInvalidPCM):
		return status.Error(codes.InvalidArgument, "invalid media frame")
	default:
		return status.Error(codes.Unavailable, "call operation failed")
	}
}
func (s *Server) Media(stream grpc.BidiStreamingServer[pb.MediaPacket, pb.MediaPacket]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetKind() != "open" || first.GetSession() == "" || first.GetCallId() == "" || len(first.GetPcm()) != 0 {
		return status.Error(codes.InvalidArgument, "media open required")
	}
	m, err := s.voice(first.GetSession())
	if err != nil {
		return err
	}
	media, err := m.Open(first.GetCallId())
	if err != nil {
		return callError(err)
	}
	defer media.Close()
	if err = stream.Send(&pb.MediaPacket{CallId: first.GetCallId(), Kind: "ready"}); err != nil {
		return err
	}
	errorsCh := make(chan error, 1)
	go func() {
		var sequence uint64
		for {
			packet, recvErr := stream.Recv()
			if recvErr != nil {
				errorsCh <- recvErr
				return
			}
			if packet.GetSession() != first.GetSession() || packet.GetCallId() != first.GetCallId() || packet.GetSequence() != sequence+1 {
				errorsCh <- status.Error(codes.InvalidArgument, "media identity or sequence changed")
				return
			}
			sequence = packet.GetSequence()
			switch packet.GetKind() {
			case "pcm":
				recvErr = media.Write(packet.GetPcm())
			case "clear":
				if len(packet.GetPcm()) != 0 {
					recvErr = calling.ErrInvalidPCM
				} else {
					media.Clear()
				}
			default:
				recvErr = calling.ErrInvalidPCM
			}
			if recvErr != nil {
				errorsCh <- callError(recvErr)
				return
			}
		}
	}()
	var sequence uint64
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err = <-errorsCh:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-media.Done:
			return stream.Send(&pb.MediaPacket{CallId: first.GetCallId(), Kind: "ended"})
		case data := <-media.Input:
			sequence++
			if err = stream.Send(&pb.MediaPacket{CallId: first.GetCallId(), Kind: "pcm", Pcm: data, Sequence: sequence}); err != nil {
				return err
			}
		}
	}
}
