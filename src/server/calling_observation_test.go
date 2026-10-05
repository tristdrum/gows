package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/devlikeapro/gows/calling"
	pb "github.com/devlikeapro/gows/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type observationTestStream struct {
	*mediaTestStream
	recv func() (*pb.MediaPacket, error)
	send func(*pb.MediaPacket) error
}

func (s *observationTestStream) Recv() (*pb.MediaPacket, error) { return s.recv() }
func (s *observationTestStream) Send(packet *pb.MediaPacket) error {
	if s.send != nil {
		return s.send(packet)
	}
	return nil
}

func TestOwnedMediaObservesActualExitBeforeCleanupOnShippingStdout(t *testing.T) {
	for _, name := range []string{
		"client_eof", "receive_error", "identity_changed", "owned_done", "owned_write_eof",
		"owned_sink_failure", "late_eof_sink_failure", "write_failure", "invalid_command",
		"ready_send_error", "ended_send_error", "pcm_send_error", "context_cancelled",
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := &pb.MediaPacket{Kind: "open", Session: "PRIVATE_SESSION_SENTINEL", CallId: "PRIVATE_CALL_SENTINEL"}
			packet := &pb.MediaPacket{Kind: "pcm", Session: first.Session, CallId: first.CallId, Sequence: 1, Pcm: make([]byte, 1920)}
			done := make(chan struct{})
			input := make(chan []byte, 1)
			privateErr := errors.New("PRIVATE_ERROR_SENTINEL")
			endpoint := callMediaEndpoint{input: input, done: done, write: func([]byte) error { return nil }, clear: func() {}, terminalError: func() error { return nil }}
			endpoint.close = func() error { fmt.Println("cleanup-local-hangup"); return nil }
			stream := &observationTestStream{mediaTestStream: &mediaTestStream{ctx: ctx}, recv: func() (*pb.MediaPacket, error) { <-ctx.Done(); return nil, ctx.Err() }}
			wantKind, wantCode := name, codes.OK
			switch name {
			case "client_eof":
				stream.recv = func() (*pb.MediaPacket, error) { return nil, io.EOF }
			case "receive_error":
				stream.recv = func() (*pb.MediaPacket, error) { return nil, privateErr }
				wantCode = codes.Unknown
			case "identity_changed":
				packet.Session = "PRIVATE_OTHER_SESSION"
				stream.recv = func() (*pb.MediaPacket, error) { return packet, nil }
				wantKind, wantCode = "receive_error", codes.InvalidArgument
			case "owned_done", "owned_sink_failure", "ended_send_error":
				close(done)
				wantKind = "owned_done"
				if name == "owned_sink_failure" {
					endpoint.terminalError = func() error { return calling.ErrMediaBackpressure }
					wantKind, wantCode = "sink_error", codes.ResourceExhausted
				}
			case "owned_write_eof", "late_eof_sink_failure", "write_failure", "invalid_command":
				stream.recv = func() (*pb.MediaPacket, error) { return packet, nil }
				endpoint.write = func([]byte) error { return io.EOF }
				wantKind = "owned_done"
				if name == "late_eof_sink_failure" {
					endpoint.terminalError = func() error { return calling.ErrInvalidPCM }
					wantKind, wantCode = "sink_error", codes.InvalidArgument
				}
				if name == "write_failure" {
					endpoint.write = func([]byte) error { return calling.ErrMediaBackpressure }
					wantKind, wantCode = "sink_error", codes.ResourceExhausted
				}
				if name == "invalid_command" {
					packet.Kind = "PRIVATE_COMMAND_SENTINEL"
					wantKind, wantCode = "sink_error", codes.InvalidArgument
				}
			case "pcm_send_error":
				input <- make([]byte, 1920)
			case "context_cancelled":
				cancel()
				wantCode = codes.Unknown
			}
			if strings.HasSuffix(name, "send_error") {
				wantKind, wantCode = "send_error", codes.Unknown
				stream.send = func(p *pb.MediaPacket) error {
					if (name == "ready_send_error" && p.Kind == "ready") || (name == "ended_send_error" && p.Kind == "ended") || (name == "pcm_send_error" && p.Kind == "pcm") {
						return privateErr
					}
					return nil
				}
			}
			var result error
			output := captureMediaStdout(t, func() { result = serveOwnedCallMedia(stream, first, endpoint) })
			if status.Code(result) != wantCode {
				t.Fatalf("observation changed returned error: got %s want %s", status.Code(result), wantCode)
			}
			if name == "context_cancelled" && !errors.Is(result, context.Canceled) {
				t.Fatal("context cancellation identity changed")
			}
			if (name == "receive_error" || strings.HasSuffix(name, "send_error")) && result != privateErr {
				t.Fatal("transport error identity changed")
			}
			wantLine := "[NativeVoice INFO] native_media_terminal binding_hash=" + calling.NativeBindingHash(first.Session, first.CallId) + " return_kind=" + wantKind
			switch name {
			case "owned_sink_failure":
				wantLine += " failure_producer=input failure_category=backlog"
			case "late_eof_sink_failure":
				wantLine += " failure_producer=input failure_category=invalid"
			case "write_failure":
				wantLine += " failure_producer=output failure_category=backlog"
			case "invalid_command":
				wantLine += " failure_producer=none failure_category=invalid"
			}
			wantLine += "\n"
			observation, cleanup := strings.Index(output, wantLine), strings.Index(output, "cleanup-local-hangup")
			if observation < 0 || cleanup <= observation || strings.Count(output, "native_media_terminal") != 1 {
				t.Fatalf("missing, repeated or post-cleanup native observation: %q", output)
			}
			if strings.Contains(output, "PRIVATE_") {
				t.Fatalf("shipping stdout exposed private data: %q", output)
			}
		})
	}
}

func captureMediaStdout(t *testing.T, fn func()) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = previous }()
	fn()
	_ = writer.Close()
	output, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	return string(output)
}
