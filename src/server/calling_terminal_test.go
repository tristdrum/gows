package server

import (
	"io"
	"testing"

	"github.com/devlikeapro/gows/calling"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMediaOwnedSinkFailureDoesNotBecomeNormalEnd(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"backpressure", calling.ErrMediaBackpressure, codes.ResourceExhausted},
		{"invalid_pcm", calling.ErrInvalidPCM, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream, first := newMediaTestStream(t)
			stream.packets = nil
			done := make(chan struct{})
			close(done)
			err := relayCallMedia(stream, first, nil, done, func([]byte) error {
				t.Fatal("closed sink accepted output")
				return nil
			}, func() {}, func() error { return tc.err })
			if status.Code(err) != tc.code || len(stream.sent) != 0 {
				t.Fatalf("sink failure = %s, sent=%d; want %s without normal ended", status.Code(err), len(stream.sent), tc.code)
			}
		})
	}
}

func TestMediaOwnedSinkFailureSurvivesLateOutputEOF(t *testing.T) {
	stream, first := newMediaTestStream(t)
	err := relayCallMedia(stream, first, nil, make(chan struct{}), func([]byte) error {
		return io.EOF
	}, func() {}, func() error { return calling.ErrMediaBackpressure })
	if status.Code(err) != codes.ResourceExhausted || len(stream.sent) != 0 {
		t.Fatalf("late owned EOF erased sink failure: code=%s sent=%d", status.Code(err), len(stream.sent))
	}
}
