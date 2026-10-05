package server

import (
	"errors"

	"github.com/devlikeapro/gows/calling"
	pb "github.com/devlikeapro/gows/proto"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/grpc"
)

type mediaReturnKind string

const (
	mediaOwnedDone        mediaReturnKind = "owned_done"
	mediaClientEOF        mediaReturnKind = "client_eof"
	mediaContextCancelled mediaReturnKind = "context_cancelled"
	mediaReceiveError     mediaReturnKind = "receive_error"
	mediaSendError        mediaReturnKind = "send_error"
	mediaSinkError        mediaReturnKind = "sink_error"
)

type mediaRelayResult struct {
	kind             mediaReturnKind
	err              error
	producer         mediaFailureProducer
	category         mediaFailureCategory
	outputBacklog    *calling.OutputBacklogObservation
	sendFailureCause string
}

type mediaFailureProducer string
type mediaFailureCategory string

const (
	mediaProducerNone    mediaFailureProducer = "none"
	mediaProducerInput   mediaFailureProducer = "input"
	mediaProducerOutput  mediaFailureProducer = "output"
	mediaCategoryNone    mediaFailureCategory = "none"
	mediaCategoryBacklog mediaFailureCategory = "backlog"
	mediaCategoryInvalid mediaFailureCategory = "invalid"
)

func mediaSinkFailure(producer mediaFailureProducer, err error) mediaRelayResult {
	category := mediaCategoryNone
	if errors.Is(err, calling.ErrMediaBackpressure) {
		category = mediaCategoryBacklog
	} else if errors.Is(err, calling.ErrInvalidPCM) {
		category = mediaCategoryInvalid
	}
	result := mediaRelayResult{kind: mediaSinkError, producer: producer, category: category, sendFailureCause: calling.MediaSendFailureCause(err)}
	var backlog *calling.MediaBackpressureError
	if producer == mediaProducerOutput && category == mediaCategoryBacklog && errors.As(err, &backlog) && backlog != nil {
		observation := backlog.Observation
		result.outputBacklog = &observation
	}
	result.err = callError(err)
	return result
}

func (r mediaRelayResult) sendFailureObservation() string {
	if r.kind != mediaSinkError || r.producer != mediaProducerOutput || r.category != mediaCategoryNone {
		return ""
	}
	switch r.sendFailureCause {
	case "closed", "timeout", "other":
		return r.sendFailureCause
	default:
		return ""
	}
}

func (r mediaRelayResult) outputBacklogObservation() (calling.OutputBacklogObservation, bool) {
	if r.kind != mediaSinkError || r.producer != mediaProducerOutput || r.category != mediaCategoryBacklog || r.outputBacklog == nil {
		return calling.OutputBacklogObservation{}, false
	}
	observation := *r.outputBacklog
	if observation.WaitMS < 0 || observation.QueueCapacity != 2 || observation.QueueDepth < 0 || observation.QueueDepth > observation.QueueCapacity || observation.LastReadAgeMS < -1 {
		return calling.OutputBacklogObservation{}, false
	}
	return observation, true
}

func mediaTerminalFailure(err error) mediaRelayResult {
	producer := mediaProducerInput
	if errors.Is(err, calling.ErrMediaSendFailed) {
		producer = mediaProducerOutput
	}
	return mediaSinkFailure(producer, err)
}

func (r mediaRelayResult) sinkObservation() (mediaFailureProducer, mediaFailureCategory) {
	producer, category := mediaProducerNone, mediaCategoryNone
	if r.kind != mediaSinkError {
		return producer, category
	}
	if r.producer == mediaProducerInput || r.producer == mediaProducerOutput {
		producer = r.producer
	}
	if r.category == mediaCategoryBacklog || r.category == mediaCategoryInvalid {
		category = r.category
	}
	return producer, category
}

type callMediaEndpoint struct {
	input         <-chan []byte
	done          <-chan struct{}
	write         func([]byte) error
	clear         func()
	terminalError func() error
	close         func() error
}

func serveOwnedCallMedia(stream grpc.BidiStreamingServer[pb.MediaPacket, pb.MediaPacket], first *pb.MediaPacket, media callMediaEndpoint) error {
	result := mediaRelayResult{kind: mediaSendError}
	defer media.close()
	// This observation precedes Close, which can itself initiate native hangup.
	// It names the selected media return branch, not the first network initiator.
	defer func() {
		format := "native_media_terminal binding_hash=%s return_kind=%s"
		fields := []any{calling.NativeBindingHash(first.GetSession(), first.GetCallId()), result.kind}
		if result.kind == mediaSinkError {
			producer, category := result.sinkObservation()
			format += " failure_producer=%s failure_category=%s"
			fields = append(fields, producer, category)
			if cause := result.sendFailureObservation(); cause != "" {
				format += " send_failure_cause=%s"
				fields = append(fields, cause)
			}
			if observation, ok := result.outputBacklogObservation(); ok {
				format += " backlog_wait_ms=%d backlog_queue_depth=%d backlog_queue_capacity=%d backlog_readframe_calls_during_wait=%d backlog_last_read_age_ms=%d"
				fields = append(fields, observation.WaitMS, observation.QueueDepth, observation.QueueCapacity, observation.ReadFrameCallsDuringWait, observation.LastReadAgeMS)
			}
		}
		waLog.Stdout("NativeVoice", "INFO", false).Infof(format, fields...)
	}()
	if err := stream.Send(&pb.MediaPacket{CallId: first.GetCallId(), Kind: "ready"}); err != nil {
		return err
	}
	result = relayCallMediaResult(stream, first, media.input, media.done, media.write, media.clear, media.terminalError)
	return result.err
}
