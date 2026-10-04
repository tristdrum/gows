package server

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/devlikeapro/gows/calling"
	pb "github.com/devlikeapro/gows/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type mediaTestStream struct {
	ctx     context.Context
	packets chan *pb.MediaPacket
	sent    []*pb.MediaPacket
}

func (s *mediaTestStream) Context() context.Context     { return s.ctx }
func (s *mediaTestStream) SetHeader(metadata.MD) error  { return nil }
func (s *mediaTestStream) SendHeader(metadata.MD) error { return nil }
func (s *mediaTestStream) SetTrailer(metadata.MD)       {}
func (s *mediaTestStream) SendMsg(any) error            { return nil }
func (s *mediaTestStream) RecvMsg(any) error            { return io.EOF }
func (s *mediaTestStream) Send(p *pb.MediaPacket) error { s.sent = append(s.sent, p); return nil }
func (s *mediaTestStream) Recv() (*pb.MediaPacket, error) {
	select {
	case packet := <-s.packets:
		return packet, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func newMediaTestStream(t *testing.T) (*mediaTestStream, *pb.MediaPacket) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &mediaTestStream{ctx: ctx, packets: make(chan *pb.MediaPacket, 1)}
	first := &pb.MediaPacket{Session: "owned-session", CallId: "owned-call", Kind: "open"}
	s.packets <- &pb.MediaPacket{Session: first.Session, CallId: first.CallId,
		Kind: "pcm", Pcm: make([]byte, 1920), Sequence: 1}
	return s, first
}

func TestMediaOwnedOutputEndBeforeDoneNotificationClosesNormally(t *testing.T) {
	stream, first := newMediaTestStream(t)
	outputClosed := make(chan struct{})
	close(outputClosed)
	// Stream.finish closes its output queue before notifying Done. A late valid
	// frame must preserve that queue's EOF while this notification is pending.
	done := make(chan struct{})
	writes := 0
	err := relayCallMedia(stream, first, nil, done, func([]byte) error {
		writes++
		<-outputClosed
		return io.EOF
	}, func() {}, func() error { return nil })
	if err != nil || writes != 1 {
		t.Fatalf("owned output end became transport failure: code=%s writes=%d", status.Code(err), writes)
	}
}

func TestMediaOwnedDoneSendsTerminalPacket(t *testing.T) {
	stream, first := newMediaTestStream(t)
	stream.packets = nil
	done := make(chan struct{})
	close(done)
	err := relayCallMedia(stream, first, nil, done, func([]byte) error {
		t.Fatal("terminal stream accepted PCM")
		return nil
	}, func() {}, func() error { return nil })
	if err != nil || len(stream.sent) != 1 || stream.sent[0].Kind != "ended" || stream.sent[0].CallId != first.CallId {
		t.Fatal("owned end did not emit its exact terminal packet")
	}
}

func TestMediaRealWriteFailuresRemainGRPCErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"backpressure", calling.ErrMediaBackpressure, codes.ResourceExhausted},
		{"invalid_pcm", calling.ErrInvalidPCM, codes.InvalidArgument},
		{"missing_binding", calling.ErrCallNotFound, codes.NotFound},
		{"unknown_failure", errors.New("private provider contents"), codes.Unavailable},
		{"text_is_not_eof", errors.New("EOF"), codes.Unavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream, first := newMediaTestStream(t)
			err := relayCallMedia(stream, first, nil, make(chan struct{}),
				func([]byte) error { return test.err }, func() {}, func() error { return nil })
			if status.Code(err) != test.code {
				t.Fatalf("write failure changed category: %s", status.Code(err))
			}
		})
	}
}

func TestMediaChangedIdentitySequenceAndCommandsRemainInvalid(t *testing.T) {
	for _, changed := range []string{"session", "call", "sequence", "kind", "clear_pcm"} {
		t.Run(changed, func(t *testing.T) {
			stream, first := newMediaTestStream(t)
			packet := <-stream.packets
			switch changed {
			case "session":
				packet.Session = "other-session"
			case "call":
				packet.CallId = "other-call"
			case "sequence":
				packet.Sequence = 2
			case "kind":
				packet.Kind = "unknown"
			case "clear_pcm":
				packet.Kind = "clear"
			}
			stream.packets <- packet
			err := relayCallMedia(stream, first, nil, make(chan struct{}), func([]byte) error {
				t.Error("invalid packet reached PCM writer")
				return nil
			}, func() { t.Error("invalid clear reached media") }, func() error { return nil })
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid packet changed category: %s", status.Code(err))
			}
		})
	}
}
