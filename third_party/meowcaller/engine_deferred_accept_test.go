package meowcaller

import (
	"context"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func TestDeferredAcceptRetainsEitherOrderingAndSendsOnce(t *testing.T) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/27a3c6b18657614c9ec2ed16dfc497eff11de6ec/engine.go#L618-L672
	for _, early := range []bool{true, false} {
		t.Run(map[bool]string{true: "mute_before_answer", false: "answer_before_mute"}[early], func(t *testing.T) {
			eng, call, mute := deferredAcceptFixture()
			var sent []waBinary.Node
			eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
				sent = append(sent, node)
				return nil
			}
			if early {
				eng.onCallRaw(&mute)
			}
			if len(sent) != 0 {
				t.Fatal("setup signal authorized acceptance before Answer")
			}
			if err := call.Answer(); err != nil {
				t.Fatal(err)
			}
			if !early {
				eng.onCallRaw(&mute)
			}
			if len(sent) != 1 {
				t.Fatalf("both prerequisites reached but accept count = %d", len(sent))
			}
			eng.onCallRaw(&mute)
			call.setPhase(CallPhaseActive)
			if err := call.Answer(); err != nil {
				t.Fatal(err)
			}
			if len(sent) != 1 {
				t.Fatalf("accept count = %d", len(sent))
			}
			if call.State() != CallPhaseActive {
				t.Fatal("duplicate Answer regressed active call")
			}
			attrs := sent[0].GetChildren()[0].AttrGetter()
			if attrs.String("call-id") != call.id || attrs.JID("call-creator") != mute.GetChildren()[0].AttrGetter().JID("call-creator") || sent[0].AttrGetter().JID("to") != mute.AttrGetter().JID("from") {
				t.Fatal("accept changed the observed call binding")
			}
		})
	}
}

func TestDeferredAcceptConcurrentAnswerAndSetupSendsOnce(t *testing.T) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/27a3c6b18657614c9ec2ed16dfc497eff11de6ec/engine.go#L618-L672
	for range 20 {
		eng, call, mute := deferredAcceptFixture()
		var lock sync.Mutex
		count := 0
		eng.sendCallNode = func(_ context.Context, _ waBinary.Node) error {
			lock.Lock()
			defer lock.Unlock()
			count++
			return nil
		}
		var work sync.WaitGroup
		work.Add(2)
		go func() { defer work.Done(); _ = call.Answer() }()
		go func() { defer work.Done(); eng.onCallRaw(&mute) }()
		work.Wait()
		if count != 1 {
			t.Fatalf("accept count = %d", count)
		}
	}
}

func TestDeferredAcceptLaterSetupCannotReplaceFirstBinding(t *testing.T) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/27a3c6b18657614c9ec2ed16dfc497eff11de6ec/engine.go#L1016-L1050
	eng, call, first := deferredAcceptFixture()
	other := types.NewJID("200", types.HiddenUserServer)
	later := waBinary.Node{Tag: "call", Attrs: waBinary.Attrs{"from": other}, Content: []waBinary.Node{{Tag: "mute_v2", Attrs: waBinary.Attrs{"call-id": call.id, "call-creator": other, "mute-state": "0"}}}}
	var sent []waBinary.Node
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error { sent = append(sent, node); return nil }
	eng.onCallRaw(&first)
	call.OnStateChange(func(phase CallPhase) {
		if phase == CallPhaseConnecting {
			eng.onCallRaw(&later)
		}
	})
	if err := call.Answer(); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0].AttrGetter().JID("to") != first.AttrGetter().JID("from") || sent[0].GetChildren()[0].AttrGetter().JID("call-creator") != first.GetChildren()[0].AttrGetter().JID("call-creator") {
		t.Fatal("later setup changed first acceptance binding")
	}
}

func TestRetiredCallCannotReturnToConnecting(t *testing.T) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/27a3c6b18657614c9ec2ed16dfc497eff11de6ec/livecall.go#L705-L718
	eng, call, mute := deferredAcceptFixture()
	sent := 0
	eng.sendCallNode = func(context.Context, waBinary.Node) error { sent++; return nil }
	eng.onCallRaw(&mute)
	eng.finishCall(call.id, "test_retired")
	if err := call.Answer(); err == nil {
		t.Fatal("retired handle accepted Answer")
	}
	call.setPhase(CallPhaseConnecting)
	call.setPhase(CallPhaseActive)
	if sent != 0 || call.State() != CallPhaseEnded {
		t.Fatal("late setup revived retired call")
	}
}

func deferredAcceptFixture() (*engine, *Call, waBinary.Node) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/27a3c6b18657614c9ec2ed16dfc497eff11de6ec/engine.go#L618-L672
	client := &Client{log: zerolog.Nop()}
	eng := &engine{c: client, calls: make(map[string]*engineCall)}
	client.eng = eng
	peer := types.NewJID("100", types.HiddenUserServer)
	call := &Call{eng: eng, id: "OWNED_CALL", peer: peer, phase: CallPhaseRinging}
	eng.calls[call.id] = &engineCall{call: call, creator: peer, from: peer, direction: CallDirectionIncoming}
	return eng, call, waBinary.Node{Tag: "call", Attrs: waBinary.Attrs{"from": peer}, Content: []waBinary.Node{{Tag: "mute_v2", Attrs: waBinary.Attrs{"call-id": call.id, "call-creator": peer, "mute-state": "1"}}}}
}
