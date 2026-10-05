package meowcaller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/dtls/v3"
	"github.com/pion/sctp"
	"github.com/purpshell/meowcaller/relay"
)

type audioSendTestRelay struct {
	err    error
	closed chan struct{}
	once   sync.Once
	sends  atomic.Int32
}

func (r *audioSendTestRelay) Send(packet []byte) (int, error) {
	r.sends.Add(1)
	if r.err != nil {
		return 0, r.err
	}
	return len(packet), nil
}

func (r *audioSendTestRelay) Recv() error { <-r.closed; return io.EOF }
func (r *audioSendTestRelay) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

type audioSendTestSource struct{ closes atomic.Int32 }

func (*audioSendTestSource) ReadFrame() ([]float32, error) { return make([]float32, FrameSamples), nil }
func (s *audioSendTestSource) Close() error                { s.closes.Add(1); return nil }

type audioSendTestSink struct {
	closes  atomic.Int32
	failure error
}

func (*audioSendTestSink) WriteFrame([]float32) error { return nil }
func (s *audioSendTestSink) Close() error             { s.closes.Add(1); return nil }
func (s *audioSendTestSink) CloseWithError(err error) error {
	s.failure = err
	return s.Close()
}

func TestAudioRelaySendFailureRetiresOwnedCallAndClosesReceiver(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	current := eng.calls[call.ID()]
	call.setPhase(CallPhaseActive)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	current.cancel = cancel
	source, sink := &audioSendTestSource{}, &audioSendTestSink{}
	call.Play(source)
	call.Receive(sink)
	var reason string
	var ends int
	call.OnEnd(func(value string) { reason = value; ends++ })
	failure := errors.New("injected relay failure")
	relay := &audioSendTestRelay{err: failure, closed: make(chan struct{})}
	defer relay.Close()
	receiverDone := make(chan struct{})
	go func() { _ = relay.Recv(); close(receiverDone) }()
	if err := eng.sendAudioPacket(call.ID(), current, call, relay, []byte{1}); !errors.Is(err, failure) {
		t.Fatal("original send failure was masked")
	}
	if call.State() != CallPhaseEnded || eng.lookup(call.ID()) != nil || reason != "media_send_failed" || ends != 1 {
		t.Fatal("exact owned call did not end with its first finite failure stage")
	}
	if source.closes.Load() != 1 || sink.closes.Load() != 1 || ctx.Err() != context.Canceled {
		t.Fatal("existing media lifecycle did not close audio and cancel workers")
	}
	if !errors.Is(sink.failure, ErrMediaSendFailed) || errors.Is(sink.failure, failure) {
		t.Fatal("sink lost the finite sender cause or received a raw relay error")
	}
	select {
	case <-receiverDone:
	case <-time.After(80 * time.Millisecond):
		t.Fatal("relay receiver stayed blocked after its sender failed")
	}
	eng.finishCall(call.ID(), "hangup")
	if reason != "media_send_failed" || ends != 1 || source.closes.Load() != 1 || sink.closes.Load() != 1 {
		t.Fatal("later cleanup overwrote or repeated the first media failure")
	}
}

func TestAudioRelaySuccessfulSendPreservesActiveMedia(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	current := eng.calls[call.ID()]
	call.setPhase(CallPhaseActive)
	source, sink := &audioSendTestSource{}, &audioSendTestSink{}
	call.Play(source)
	call.Receive(sink)
	relay := &audioSendTestRelay{closed: make(chan struct{})}
	defer relay.Close()
	defer eng.finishCall(call.ID(), "test_cleanup")
	if err := eng.sendAudioPacket(call.ID(), current, call, relay, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if call.State() != CallPhaseActive || eng.lookup(call.ID()) != current || source.closes.Load() != 0 || sink.closes.Load() != 0 || relay.sends.Load() != 1 {
		t.Fatal("successful send changed call ownership or audio lifecycle")
	}
	select {
	case <-relay.closed:
		t.Fatal("successful send closed its relay")
	default:
	}
}

func TestAudioRelayRetiredSenderCannotEndReplacementCall(t *testing.T) {
	eng, retired := testEngineWithOutgoingCall()
	prior := eng.calls[retired.ID()]
	replacement := &Call{eng: eng, id: retired.ID(), phase: CallPhaseActive}
	current := &engineCall{call: replacement}
	eng.calls[retired.ID()] = current
	var replacementEnds int
	replacement.OnEnd(func(string) { replacementEnds++ })
	defer eng.finishCall(replacement.ID(), "test_cleanup")
	failure := errors.New("retired relay failed")
	relay := &audioSendTestRelay{err: failure, closed: make(chan struct{})}
	if err := eng.sendAudioPacket(retired.ID(), prior, retired, relay, []byte{1}); !errors.Is(err, failure) {
		t.Fatal("retired sender failure was masked")
	}
	if replacement.State() != CallPhaseActive || eng.lookup(replacement.ID()) != current || replacementEnds != 0 {
		t.Fatal("retired sender ended a replacement call")
	}
	select {
	case <-relay.closed:
	default:
		t.Fatal("retired sender did not close only its owned relay")
	}
}

func TestAudioRelayMissingCaptureCannotEndReplacementCall(t *testing.T) {
	eng, retired := testEngineWithOutgoingCall()
	eng.finishCall(retired.ID(), "cancelled_during_allocation")
	captured := eng.lookup(retired.ID())
	if captured != nil {
		t.Fatal("fixture did not reproduce a missing post-allocation capture")
	}
	replacement := &Call{eng: eng, id: retired.ID(), phase: CallPhaseActive}
	current := &engineCall{call: replacement}
	eng.calls[retired.ID()] = current
	var ends int
	replacement.OnEnd(func(string) { ends++ })
	defer eng.finishCall(replacement.ID(), "test_cleanup")
	failure := errors.New("retired relay failed after cancellation")
	channel := &audioSendTestRelay{err: failure, closed: make(chan struct{})}
	if err := eng.sendAudioPacket(retired.ID(), captured, retired, channel, []byte{1}); err != failure {
		t.Fatal("original failed sender error identity changed")
	}
	if eng.lookup(replacement.ID()) != current || replacement.State() != CallPhaseActive || ends != 0 {
		t.Fatal("sender with no captured owner retired a replacement call")
	}
	select {
	case <-channel.closed:
	default:
		t.Fatal("sender with no captured owner did not close its own relay")
	}
}

func TestAudioRelayConcurrentFailuresAndNormalCloseKeepFirstLifecycle(t *testing.T) {
	for _, normalFirst := range []bool{false, true} {
		eng, call := testEngineWithOutgoingCall()
		current := eng.calls[call.ID()]
		source, sink := &audioSendTestSource{}, &audioSendTestSink{}
		call.Play(source)
		call.Receive(sink)
		var ends atomic.Int32
		var reason string
		call.OnEnd(func(value string) { reason = value; ends.Add(1) })
		if normalFirst {
			eng.finishCall(call.ID(), "hangup")
		}
		relay := &audioSendTestRelay{err: errors.New("injected relay failure"), closed: make(chan struct{})}
		var workers sync.WaitGroup
		for range 2 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				_ = eng.sendAudioPacket(call.ID(), current, call, relay, []byte{1})
			}()
		}
		workers.Wait()
		want := "media_send_failed"
		if normalFirst {
			want = "hangup"
		}
		if ends.Load() != 1 || reason != want || source.closes.Load() != 1 || sink.closes.Load() != 1 {
			t.Fatal("concurrent failure repeated close or erased the first lifecycle winner")
		}
		if normalFirst && sink.failure != nil {
			t.Fatal("a late sender error changed an already normal audio close")
		}
	}
}

func TestAudioRelaySendFailureCarriesOnlyTypedFiniteCause(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		cause string
	}{
		{"net_closed", net.ErrClosed, "closed"},
		{"pipe_closed", io.ErrClosedPipe, "closed"},
		{"sctp_stream_closed", sctp.ErrStreamClosed, "closed"},
		{"sctp_association_closed", sctp.ErrAssociationClosed, "closed"},
		{"dtls_closed", dtls.ErrConnClosed, "closed"},
		{"wrapped_closed", &relay.CallTransportError{Op: "send", Err: fmt.Errorf("PRIVATE_DETAILS: %w", net.ErrClosed)}, "closed"},
		{"context_deadline", context.DeadlineExceeded, "timeout"},
		{"os_deadline", os.ErrDeadlineExceeded, "timeout"},
		{"typed_timeout", &net.DNSError{Err: "PRIVATE_DETAILS", IsTimeout: true}, "timeout"},
		{"wrapped_timeout", fmt.Errorf("PRIVATE_DETAILS: %w", os.ErrDeadlineExceeded), "timeout"},
		{"non_established", sctp.ErrPayloadDataStateNotExist, "other"},
		{"packet_limit", sctp.ErrOutboundPacketTooLarge, "other"},
		{"cancellation", context.Canceled, "other"},
		{"untyped_message", errors.New("PRIVATE_DETAILS closed timeout buffer exceeded"), "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, call := testEngineWithOutgoingCall()
			current := eng.calls[call.ID()]
			sink := &audioSendTestSink{}
			call.Receive(sink)
			channel := &audioSendTestRelay{err: tc.err, closed: make(chan struct{})}
			if err := eng.sendAudioPacket(call.ID(), current, call, channel, []byte{1}); err != tc.err {
				t.Fatal("original internal send error identity changed")
			}
			var finite interface{ MediaSendCause() string }
			if !errors.As(sink.failure, &finite) || finite.MediaSendCause() != tc.cause {
				t.Fatal("typed sender cause missing or misclassified")
			}
			if sink.failure.Error() != "native media send failed" || !errors.Is(sink.failure, ErrMediaSendFailed) || errors.Unwrap(sink.failure) != ErrMediaSendFailed || errors.Is(sink.failure, tc.err) {
				t.Fatal("finite failure leaked or retained the original transport error")
			}
		})
	}
}
