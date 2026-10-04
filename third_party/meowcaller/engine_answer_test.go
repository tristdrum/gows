package meowcaller

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func TestAnswerAcceptsWithoutMuteAndOnlyOnce(t *testing.T) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L402-L465
	eng, call, _ := deferredAcceptFixture()
	var accepts atomic.Int32
	eng.sendCallNode = func(context.Context, waBinary.Node) error { accepts.Add(1); return nil }
	if accepts.Load() != 0 {
		t.Fatal("accepted before application Answer")
	}
	var answers sync.WaitGroup
	for range 32 {
		answers.Add(1)
		go func() {
			defer answers.Done()
			if err := call.Answer(); err != nil {
				t.Errorf("Answer: %v", err)
			}
		}()
	}
	answers.Wait()
	if accepts.Load() != 1 || call.State() != CallPhaseConnecting {
		t.Fatalf("explicit Answer did not accept once: count=%d phase=%d", accepts.Load(), call.State())
	}
	call.setPhase(CallPhaseActive)
	if err := call.Answer(); err != nil {
		t.Fatal(err)
	}
	if accepts.Load() != 1 || call.State() != CallPhaseActive {
		t.Fatal("duplicate Answer sent acceptance or regressed active phase")
	}
}

func TestAnswerRetainsOfferBindingAcrossMuteSignals(t *testing.T) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L1484-L1522
	eng, call, mute := deferredAcceptFixture()
	m := eng.calls[call.ID()]
	wantTo, wantCreator := m.from, m.creator
	other := types.NewJID("OTHER", types.HiddenUserServer)
	mute.Attrs["from"] = other
	mute.GetChildren()[0].Attrs["call-creator"] = other
	var sent []waBinary.Node
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error { sent = append(sent, node); return nil }
	eng.onCallRaw(&mute)
	if len(sent) != 0 {
		t.Fatal("mute authorized acceptance before Answer")
	}
	if err := call.Answer(); err != nil {
		t.Fatal(err)
	}
	eng.onCallRaw(&mute)
	if len(sent) != 1 || sent[0].AttrGetter().JID("to") != wantTo || sent[0].GetChildren()[0].AttrGetter().JID("call-creator") != wantCreator {
		t.Fatal("mute changed original offer acceptance binding")
	}
}

func TestAnswerCancelsPendingAcceptanceWhenCallEnds(t *testing.T) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L1568-L1583
	eng, call, mute := deferredAcceptFixture()
	eng.onCallRaw(&mute)
	started := make(chan struct{})
	eng.sendCallNode = func(ctx context.Context, _ waBinary.Node) error {
		close(started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return errors.New("acceptance was not cancelled")
		}
	}
	answered := make(chan error, 1)
	go func() { answered <- call.Answer() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("acceptance did not start")
	}
	eng.finishCall(call.ID(), "terminated")
	select {
	case err := <-answered:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ended call did not cancel acceptance: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Answer stayed blocked after call ended")
	}
	if call.State() != CallPhaseEnded || eng.lookup(call.ID()) != nil {
		t.Fatal("pending Answer resurrected terminated call")
	}
}

func TestAnswerRejectsRetiredOrReplacedHandle(t *testing.T) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L457-L464
	for _, replaced := range []bool{false, true} {
		eng, call, _ := deferredAcceptFixture()
		var sent atomic.Int32
		eng.sendCallNode = func(context.Context, waBinary.Node) error { sent.Add(1); return nil }
		eng.finishCall(call.ID(), "terminated")
		if replaced {
			replacement := &Call{eng: eng, id: call.ID(), phase: CallPhaseRinging}
			eng.calls[call.ID()] = &engineCall{call: replacement, direction: CallDirectionIncoming}
		}
		if err := call.Answer(); err == nil {
			t.Fatal("retired handle authorized acceptance")
		}
		if sent.Load() != 0 || call.State() != CallPhaseEnded {
			t.Fatal("retired handle sent acceptance or returned to connecting")
		}
	}
}
