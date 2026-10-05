package calling

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/purpshell/meowcaller"
)

func TestNativeSendFailureSurvivesOwnedStreamClose(t *testing.T) {
	for _, initial := range []struct {
		finished bool
		prior    error
	}{{}, {finished: true, prior: ErrInvalidPCM}, {finished: true}} {
		m := newManager(&fakeDialer{}, nil)
		result, err := m.Dial(t.Context(), "1@s.whatsapp.net", "sender-failure")
		if err != nil {
			t.Fatal(err)
		}
		stream, err := m.Open(result.ID)
		if err != nil {
			t.Fatal(err)
		}
		if initial.finished {
			stream.finishWithError(initial.prior)
		}
		if err := stream.CloseWithError(ErrMediaSendFailed); err != nil {
			t.Fatal(err)
		}
		_ = stream.Close()
		want := ErrMediaSendFailed
		if initial.finished {
			want = initial.prior
		}
		if stream.TerminalError() != want {
			t.Fatal("sender failure or prior terminal error was overwritten by close")
		}
		select {
		case <-stream.Done:
		default:
			t.Fatal("failed sender did not publish owned media completion")
		}
		if err := stream.Write(make([]byte, frameBytes)); !errors.Is(err, io.EOF) {
			t.Fatal("failed sender left the output queue writable")
		}
		if _, err := m.Status(result.ID); !errors.Is(err, ErrCallNotFound) {
			t.Fatal("failed sender left an active manager call")
		}
	}
}

func TestMediaSendFailureCauseRejectsNonSendErrors(t *testing.T) {
	for _, err := range []error{nil, net.ErrClosed, ErrInvalidPCM, ErrMediaBackpressure, errors.New("native media send failed closed")} {
		if MediaSendFailureCause(err) != "" {
			t.Fatal("non-sender error became a sender cause")
		}
	}
	for _, err := range []error{ErrMediaSendFailed, fmt.Errorf("PRIVATE_DETAILS: %w", ErrMediaSendFailed), &meowcaller.MediaSendError{}} {
		if MediaSendFailureCause(err) != "other" {
			t.Fatal("sender without a proven typed cause did not remain other")
		}
	}
}
