package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/devlikeapro/gows/calling"
	pb "github.com/devlikeapro/gows/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOwnedMediaObservesSelectedSinkProducerWithoutChangingFailure(t *testing.T) {
	privateErr := errors.New("PRIVATE_ERROR_SENTINEL")
	for _, tc := range []struct {
		name, path, producer, category string
		err                            error
		code                           codes.Code
	}{
		{"input_backlog", "done", "input", "backlog", calling.ErrMediaBackpressure, codes.ResourceExhausted},
		{"input_invalid", "done", "input", "invalid", calling.ErrInvalidPCM, codes.InvalidArgument},
		{"input_unknown", "done", "input", "none", privateErr, codes.Unavailable},
		{"late_eof_input_backlog", "eof", "input", "backlog", calling.ErrMediaBackpressure, codes.ResourceExhausted},
		{"late_eof_input_invalid", "eof", "input", "invalid", calling.ErrInvalidPCM, codes.InvalidArgument},
		{"output_backlog", "write", "output", "backlog", calling.ErrMediaBackpressure, codes.ResourceExhausted},
		{"output_invalid", "write", "output", "invalid", calling.ErrInvalidPCM, codes.InvalidArgument},
		{"output_invalid_with_input_failure_recorded", "write_input_recorded", "output", "invalid", calling.ErrInvalidPCM, codes.InvalidArgument},
		{"output_wrapped_backlog", "write", "output", "backlog", fmt.Errorf("PRIVATE_WRAPPER_SENTINEL: %w", calling.ErrMediaBackpressure), codes.ResourceExhausted},
		{"output_unknown", "write", "output", "none", privateErr, codes.Unavailable},
		{"invalid_command_without_sink_write", "command", "none", "invalid", nil, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := &pb.MediaPacket{Kind: "open", Session: "PRIVATE_SESSION_SENTINEL", CallId: "PRIVATE_CALL_SENTINEL"}
			packet := &pb.MediaPacket{Kind: "pcm", Session: first.Session, CallId: first.CallId, Sequence: 1, Pcm: make([]byte, 1920)}
			done := make(chan struct{})
			stream := &observationTestStream{mediaTestStream: &mediaTestStream{ctx: ctx}, recv: func() (*pb.MediaPacket, error) { <-ctx.Done(); return nil, ctx.Err() }}
			endpoint := callMediaEndpoint{done: done, clear: func() {}, terminalError: func() error { return nil }, write: func([]byte) error { t.Error("unexpected sink write"); return nil }}
			switch tc.path {
			case "done":
				endpoint.terminalError = func() error { return tc.err }
				close(done)
			case "eof", "write", "write_input_recorded", "command":
				stream.recv = func() (*pb.MediaPacket, error) { return packet, nil }
				endpoint.write = func([]byte) error { return tc.err }
				if tc.path == "write_input_recorded" {
					endpoint.terminalError = func() error { return calling.ErrMediaBackpressure }
				}
				if tc.path == "eof" {
					endpoint.write = func([]byte) error { return io.EOF }
					endpoint.terminalError = func() error { return tc.err }
				}
				if tc.path == "command" {
					packet.Kind = "PRIVATE_COMMAND_SENTINEL"
					endpoint.write = func([]byte) error { t.Error("invalid command reached sink write"); return nil }
				}
			}
			closes := 0
			endpoint.close = func() error { closes++; fmt.Println("cleanup-local-hangup"); return nil }
			var result error
			output := captureMediaStdout(t, func() { result = serveOwnedCallMedia(stream, first, endpoint) })
			if status.Code(result) != tc.code || closes != 1 {
				t.Fatalf("diagnostic changed failure or cleanup: code=%s closes=%d", status.Code(result), closes)
			}
			want := "return_kind=sink_error failure_producer=" + tc.producer + " failure_category=" + tc.category + "\n"
			observed, cleanup := strings.Index(output, want), strings.Index(output, "cleanup-local-hangup")
			if observed < 0 || cleanup <= observed || strings.Count(output, "native_media_terminal") != 1 {
				t.Fatalf("missing, repeated or post-cleanup selected sink diagnostic for %s", tc.name)
			}
			if strings.Contains(output, "PRIVATE_") {
				t.Fatal("shipping diagnostic retained private source data")
			}
		})
	}
}

func TestMediaSinkObservationRejectsUncataloguedMetadata(t *testing.T) {
	for _, result := range []mediaRelayResult{
		{kind: mediaSinkError},
		{kind: mediaSinkError, producer: "PRIVATE_PRODUCER_SENTINEL", category: "PRIVATE_CATEGORY_SENTINEL"},
		{kind: mediaOwnedDone, producer: mediaProducerInput, category: mediaCategoryBacklog},
	} {
		producer, category := result.sinkObservation()
		if producer != "none" || category != "none" {
			t.Fatal("uncatalogued or non-sink metadata reached diagnostic")
		}
	}
}
