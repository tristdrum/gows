package server

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/devlikeapro/gows/calling"
	pb "github.com/devlikeapro/gows/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNativeRelaySendFailureUsesExistingTypedMediaExit(t *testing.T) {
	for _, lateEOF := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		first := &pb.MediaPacket{Kind: "open", Session: "PRIVATE_SESSION", CallId: "PRIVATE_CALL"}
		done := make(chan struct{})
		stream := &observationTestStream{mediaTestStream: &mediaTestStream{ctx: ctx}, recv: func() (*pb.MediaPacket, error) { <-ctx.Done(); return nil, ctx.Err() }}
		write := func([]byte) error { return nil }
		if lateEOF {
			stream.recv = func() (*pb.MediaPacket, error) {
				return &pb.MediaPacket{Kind: "pcm", Session: first.Session, CallId: first.CallId, Sequence: 1}, nil
			}
			write = func([]byte) error { return io.EOF }
		} else {
			close(done)
		}
		endpoint := callMediaEndpoint{input: make(chan []byte), done: done, write: write, clear: func() {}, terminalError: func() error { return calling.ErrMediaSendFailed }, close: func() error { return nil }}
		var result error
		output := captureMediaStdout(t, func() { result = serveOwnedCallMedia(stream, first, endpoint) })
		cancel()
		if status.Code(result) != codes.Unavailable || status.Convert(result).Message() != "native media send failed" {
			t.Fatal("relay sender failure became a normal completion or lost its finite stage")
		}
		if !strings.Contains(output, "return_kind=sink_error failure_producer=output failure_category=none send_failure_cause=other\n") || strings.Contains(output, "PRIVATE_") {
			t.Fatal("selected sender cause was mislabeled or exposed private identity")
		}
	}
}
