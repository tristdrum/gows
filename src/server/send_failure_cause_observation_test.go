package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/devlikeapro/gows/calling"
	pb "github.com/devlikeapro/gows/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMediaSendCauseObservationPreservesFixedResultBeforeCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	close(done)
	first := &pb.MediaPacket{Kind: "open", Session: "PRIVATE_SESSION_SENTINEL", CallId: "PRIVATE_CALL_SENTINEL"}
	stream := &observationTestStream{mediaTestStream: &mediaTestStream{ctx: ctx}, recv: func() (*pb.MediaPacket, error) { <-ctx.Done(); return nil, ctx.Err() }}
	closes := 0
	endpoint := callMediaEndpoint{done: done, clear: func() {}, write: func([]byte) error { return nil },
		terminalError: func() error { return fmt.Errorf("PRIVATE_ERROR_SENTINEL: %w", calling.ErrMediaSendFailed) },
		close:         func() error { closes++; fmt.Println("cleanup-local-hangup"); return nil }}
	var result error
	output := captureMediaStdout(t, func() { result = serveOwnedCallMedia(stream, first, endpoint) })
	want := "return_kind=sink_error failure_producer=output failure_category=none send_failure_cause=other\n"
	if status.Code(result) != codes.Unavailable || closes != 1 || !strings.Contains(output, want) || strings.Index(output, want) >= strings.Index(output, "cleanup-local-hangup") {
		t.Fatal("finite cause changed fixed result or cleanup ordering")
	}
	if strings.Contains(output, "PRIVATE_") || strings.Count(output, "native_media_terminal") != 1 || strings.Contains(output, "backlog_wait_ms=") {
		t.Fatal("sender cause leaked private data, repeated a terminal or entered backlog branch")
	}
}

func TestMediaSendCauseObservationRejectsOtherBranchesAndUncataloguedValues(t *testing.T) {
	for _, cause := range []string{"closed", "timeout", "other"} {
		result := mediaRelayResult{kind: mediaSinkError, producer: mediaProducerOutput, category: mediaCategoryNone, sendFailureCause: cause}
		if result.sendFailureObservation() != cause {
			t.Fatal("finite cause rejected")
		}
	}
	for _, result := range []mediaRelayResult{
		{kind: mediaSinkError, producer: mediaProducerOutput, category: mediaCategoryNone, sendFailureCause: "PRIVATE_ERROR_SENTINEL"},
		{kind: mediaSinkError, producer: mediaProducerOutput, category: mediaCategoryBacklog, sendFailureCause: "closed"},
		{kind: mediaSinkError, producer: mediaProducerInput, category: mediaCategoryNone, sendFailureCause: "closed"},
		{kind: mediaOwnedDone, producer: mediaProducerOutput, category: mediaCategoryNone, sendFailureCause: "closed"},
		mediaSinkFailure(mediaProducerOutput, calling.ErrMediaBackpressure),
	} {
		if result.sendFailureObservation() != "" {
			t.Fatal("non-sender or uncatalogued metadata reached the diagnostic")
		}
	}
}
