package meowcaller

import (
	"context"
	"errors"
	"io"
	"net"
	"os"

	"github.com/pion/dtls/v3"
	"github.com/pion/sctp"
)

// ErrMediaSendFailed identifies a failed relay sender without exposing transport
// errors, identifiers, or packet contents to the audio sink.
var ErrMediaSendFailed = errors.New("native media send failed")

// MediaSendError retains only a typed, finite cause. The original transport error
// stays inside the sender and is never retained by the sink-facing error.
type MediaSendError struct{ cause string }

func (*MediaSendError) Error() string { return "native media send failed" }
func (*MediaSendError) Unwrap() error { return ErrMediaSendFailed }
func (e *MediaSendError) MediaSendCause() string {
	if e != nil && (e.cause == "closed" || e.cause == "timeout") {
		return e.cause
	}
	return "other"
}

func mediaSendFailure(err error) *MediaSendError {
	cause := "other"
	var timeout net.Error
	switch {
	case errors.Is(err, net.ErrClosed), errors.Is(err, io.ErrClosedPipe),
		errors.Is(err, sctp.ErrStreamClosed), errors.Is(err, sctp.ErrAssociationClosed), errors.Is(err, dtls.ErrConnClosed):
		cause = "closed"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded),
		errors.As(err, &timeout) && timeout.Timeout():
		cause = "timeout"
	}
	return &MediaSendError{cause: cause}
}

type audioRelaySender interface {
	Send([]byte) (int, error)
	Close() error
}

// sendAudioPacket retires the exact call if its sole audio sender fails. Otherwise
// its receiver can stay alive after the queue consumer exits, masking the relay
// failure with a later output backlog.
func (e *engine) sendAudioPacket(callID string, current *engineCall, call *Call, channel audioRelaySender, packet []byte) error {
	_, err := channel.Send(packet)
	if err != nil {
		if current != nil {
			e.finishCallIfCurrent(callID, current, call, "media_send_failed", mediaSendFailure(err))
		}
		_ = channel.Close()
	}
	return err
}
