package meowcaller

import (
	"context"
	"errors"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
)

// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L1568-L1583
func TestAnswerFailureMustRetireAmbiguousGeneration(t *testing.T) {
	eng, call, _ := deferredAcceptFixture()
	sendErr := errors.New("transport write failed")
	sends := 0
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		sends++
		return sendErr
	}
	if err := call.Answer(); !errors.Is(err, sendErr) {
		t.Fatalf("send failure not returned: %v", err)
	}
	retryErr := call.Answer()
	if call.State() != CallPhaseEnded || eng.lookup(call.ID()) != nil || retryErr == nil || sends != 1 {
		t.Fatalf("ambiguous generation stayed live: phase=%v registered=%v duplicate_answer_error=%v sends=%d", call.State(), eng.lookup(call.ID()) != nil, retryErr, sends)
	}
}

// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L2456-L2480
func TestAnswerMustNotStartReplacementGeneration(t *testing.T) {
	eng, call, _ := deferredAcceptFixture()
	replacement := &Call{eng: eng, id: call.ID(), phase: CallPhaseRinging}
	replacementState := &engineCall{call: replacement, direction: CallDirectionIncoming, callKey: make([]byte, 32), relay: &relayData{}}
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		eng.finishCall(call.ID(), "terminated_during_write")
		eng.mu.Lock()
		eng.calls[call.ID()] = replacementState
		eng.mu.Unlock()
		return nil
	}
	err := call.Answer()
	eng.mu.Lock()
	started := replacementState.started
	eng.mu.Unlock()
	if err == nil || started {
		t.Fatalf("retired Answer continued against replacement: error=%v replacement_started=%v", err, started)
	}
}

func TestAnswerFailureCannotRetireReplacementGeneration(t *testing.T) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L2456-L2480
	eng, call, _ := deferredAcceptFixture()
	sendErr := errors.New("accept write failed after retirement")
	replacement := &Call{eng: eng, id: call.ID(), phase: CallPhaseRinging}
	replacementState := &engineCall{call: replacement, direction: CallDirectionIncoming}
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		eng.finishCall(call.ID(), "terminated_during_write")
		eng.mu.Lock()
		eng.calls[call.ID()] = replacementState
		eng.mu.Unlock()
		return sendErr
	}
	if err := call.Answer(); !errors.Is(err, sendErr) {
		t.Fatalf("original send error lost: %v", err)
	}
	if eng.lookup(call.ID()) != replacementState || replacement.State() != CallPhaseRinging {
		t.Fatal("failed acceptance retired the replacement generation")
	}
}

func TestAnswerCannotAffectReplacementHandleInSameEntry(t *testing.T) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L2456-L2480
	for _, sendErr := range []error{nil, errors.New("accept write failed")} {
		eng, call, _ := deferredAcceptFixture()
		m := eng.calls[call.ID()]
		replacement := &Call{eng: eng, id: call.ID(), phase: CallPhaseRinging}
		eng.sendCallNode = func(context.Context, waBinary.Node) error {
			eng.mu.Lock()
			m.call = replacement
			m.callKey, m.relay = make([]byte, 32), &relayData{}
			eng.mu.Unlock()
			return sendErr
		}
		if err := call.Answer(); err == nil || (sendErr != nil && !errors.Is(err, sendErr)) {
			t.Fatalf("retired handle did not report the original failure: %v", err)
		}
		eng.mu.Lock()
		started := m.started
		eng.mu.Unlock()
		if eng.lookup(call.ID()) != m || started || replacement.State() != CallPhaseRinging {
			t.Fatal("retired Answer changed a replacement handle in the same registry entry")
		}
	}
}
