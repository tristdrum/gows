package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/devlikeapro/gows/calling"
	pb "github.com/devlikeapro/gows/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOutputBacklogObservationEmitsOnlySelectedFiniteSnapshot(t *testing.T) {
	observation := calling.OutputBacklogObservation{WaitMS: 173, QueueDepth: 2, QueueCapacity: 2, ReadFrameCallsDuringWait: 0, LastReadAgeMS: 441}
	typed := &calling.MediaBackpressureError{Observation: observation}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := &pb.MediaPacket{Kind: "open", Session: "PRIVATE_SESSION_SENTINEL", CallId: "PRIVATE_CALL_SENTINEL"}
	packet := &pb.MediaPacket{Kind: "pcm", Session: first.Session, CallId: first.CallId, Sequence: 1, Pcm: make([]byte, 1920)}
	stream := &observationTestStream{mediaTestStream: &mediaTestStream{ctx: ctx}, recv: func() (*pb.MediaPacket, error) { return packet, nil }}
	closes := 0
	endpoint := callMediaEndpoint{done: make(chan struct{}), clear: func() {}, terminalError: func() error { return nil },
		write: func([]byte) error { return fmt.Errorf("PRIVATE_ERROR_SENTINEL: %w", typed) },
		close: func() error { closes++; fmt.Println("cleanup-local-hangup"); return nil }}
	var result error
	output := captureMediaStdout(t, func() { result = serveOwnedCallMedia(stream, first, endpoint) })
	want := "return_kind=sink_error failure_producer=output failure_category=backlog backlog_wait_ms=173 backlog_queue_depth=2 backlog_queue_capacity=2 backlog_readframe_calls_during_wait=0 backlog_last_read_age_ms=441\n"
	if status.Code(result) != codes.ResourceExhausted || closes != 1 || !strings.Contains(output, want) || strings.Index(output, want) >= strings.Index(output, "cleanup-local-hangup") {
		t.Fatal("numeric snapshot changed failure/cleanup or was not observed before local hangup")
	}
	if strings.Contains(output, "PRIVATE_") || strings.Count(output, "native_media_terminal") != 1 {
		t.Fatal("numeric diagnostic retained private source data or repeated terminal")
	}
}

func TestOutputBacklogObservationRejectsOtherBranchesAndMalformedNumbers(t *testing.T) {
	valid := calling.OutputBacklogObservation{WaitMS: 120, QueueDepth: 2, QueueCapacity: 2, LastReadAgeMS: -1}
	for _, tc := range []struct {
		name   string
		result mediaRelayResult
	}{
		{"bare_backlog", mediaSinkFailure(mediaProducerOutput, calling.ErrMediaBackpressure)},
		{"input_backlog", mediaSinkFailure(mediaProducerInput, &calling.MediaBackpressureError{Observation: valid})},
		{"non_sink", mediaRelayResult{kind: mediaOwnedDone, producer: mediaProducerOutput, category: mediaCategoryBacklog, outputBacklog: &valid}},
		{"unknown_error", mediaSinkFailure(mediaProducerOutput, errors.New("PRIVATE_ERROR_SENTINEL"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := tc.result.outputBacklogObservation(); ok {
				t.Fatal("non-selected or absent typed snapshot was admitted")
			}
		})
	}
	for _, bad := range []calling.OutputBacklogObservation{
		{WaitMS: -1, QueueDepth: 2, QueueCapacity: 2, LastReadAgeMS: 0},
		{WaitMS: 120, QueueDepth: -1, QueueCapacity: 2, LastReadAgeMS: 0},
		{WaitMS: 120, QueueDepth: 3, QueueCapacity: 2, LastReadAgeMS: 0},
		{WaitMS: 120, QueueDepth: 2, QueueCapacity: 3, LastReadAgeMS: 0},
		{WaitMS: 120, QueueDepth: 2, QueueCapacity: 2, LastReadAgeMS: -2},
	} {
		result := mediaSinkFailure(mediaProducerOutput, &calling.MediaBackpressureError{Observation: bad})
		if _, ok := result.outputBacklogObservation(); ok {
			t.Fatal("malformed finite observation reached the logger")
		}
	}
	result := mediaSinkFailure(mediaProducerOutput, &calling.MediaBackpressureError{Observation: valid})
	if got, ok := result.outputBacklogObservation(); !ok || got != valid || status.Code(result.err) != codes.ResourceExhausted {
		t.Fatal("valid never-read typed snapshot was rejected or changed status")
	}
}
