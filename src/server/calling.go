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
		if req.GetCallId() == "" {
			result, err = m.AttemptStatus(req.GetRequestId())
		} else {
			result, err = m.Status(req.GetCallId())
		}
	case "cancel":
		result, err = m.CancelAttempt(req.GetRequestId())
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
	return serveOwnedCallMedia(stream, first, callMediaEndpoint{
		input: media.Input, done: media.Done, write: media.Write, clear: media.Clear, terminalError: media.TerminalError, close: media.Close,
	})
}

func relayCallMedia(stream grpc.BidiStreamingServer[pb.MediaPacket, pb.MediaPacket], first *pb.MediaPacket,
	input <-chan []byte, done <-chan struct{}, write func([]byte) error, clear func(), terminalError func() error) error {
	return relayCallMediaResult(stream, first, input, done, write, clear, terminalError).err
}

func relayCallMediaResult(stream grpc.BidiStreamingServer[pb.MediaPacket, pb.MediaPacket], first *pb.MediaPacket,
	input <-chan []byte, done <-chan struct{}, write func([]byte) error, clear func(), terminalError func() error) mediaRelayResult {
	errorsCh := make(chan mediaRelayResult, 1)
	go func() {
		var sequence uint64
		for {
			packet, recvErr := stream.Recv()
			if recvErr != nil {
				kind := mediaReceiveError
				if errors.Is(recvErr, io.EOF) {
					kind = mediaClientEOF
				} else if stream.Context().Err() != nil && errors.Is(recvErr, stream.Context().Err()) {
					kind = mediaContextCancelled
				}
				errorsCh <- mediaRelayResult{kind: kind, err: recvErr}
				return
			}
			if packet.GetSession() != first.GetSession() || packet.GetCallId() != first.GetCallId() || packet.GetSequence() != sequence+1 {
				errorsCh <- mediaRelayResult{kind: mediaReceiveError, err: status.Error(codes.InvalidArgument, "media identity or sequence changed")}
				return
			}
			sequence = packet.GetSequence()
			switch packet.GetKind() {
			case "pcm":
				recvErr = write(packet.GetPcm())
			case "clear":
				if len(packet.GetPcm()) != 0 {
					recvErr = calling.ErrInvalidPCM
				} else {
					clear()
				}
			default:
				recvErr = calling.ErrInvalidPCM
			}
			if recvErr != nil {
				kind := mediaOwnedDone
				// The owned output closes before Done is notified. Preserve its
				// EOF so late PCM cannot turn a normal end into Unavailable.
				if !errors.Is(recvErr, io.EOF) {
					kind = mediaSinkError
					recvErr = callError(recvErr)
				}
				errorsCh <- mediaRelayResult{kind: kind, err: recvErr}
				return
			}
		}
	}()
	var sequence uint64
	var err error
	for {
		select {
		case <-stream.Context().Done():
			return mediaRelayResult{kind: mediaContextCancelled, err: stream.Context().Err()}
		case result := <-errorsCh:
			if errors.Is(result.err, io.EOF) {
				if terminalErr := terminalError(); terminalErr != nil {
					return mediaRelayResult{kind: mediaSinkError, err: callError(terminalErr)}
				}
				result.err = nil
			}
			return result
		case <-done:
			if terminalErr := terminalError(); terminalErr != nil {
				return mediaRelayResult{kind: mediaSinkError, err: callError(terminalErr)}
			}
			if err = stream.Send(&pb.MediaPacket{CallId: first.GetCallId(), Kind: "ended"}); err != nil {
				return mediaRelayResult{kind: mediaSendError, err: err}
			}
			return mediaRelayResult{kind: mediaOwnedDone}
		case data := <-input:
			sequence++
			if err = stream.Send(&pb.MediaPacket{CallId: first.GetCallId(), Kind: "pcm", Pcm: data, Sequence: sequence}); err != nil {
				return mediaRelayResult{kind: mediaSendError, err: err}
			}
		}
	}
}
