package meowcaller

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func offerReceiptFixture() (*engine, waBinary.Node, types.JID, types.JID) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/61fda7e2af1d53744ae1ce348075d87964d90c6e/examples/cli/call.go#L509-L557
	ownPN := types.NewADJID("100", 0, 7)
	ownLID := types.JID{User: "1000", Server: types.HiddenUserServer, Device: 7}
	caller := types.JID{User: "2000", Server: types.HiddenUserServer, Device: 2}
	creator := caller.ToNonAD()
	wa := whatsmeow.NewClient(&store.Device{ID: &ownPN, LID: ownLID}, waLog.Noop)
	client := &Client{wa: wa, log: zerolog.Nop()}
	eng := newEngine(client)
	client.eng = eng
	offer := waBinary.Node{Tag: "call", Attrs: waBinary.Attrs{"id": "OFFER_STANZA", "from": caller}, Content: []waBinary.Node{{Tag: "offer", Attrs: waBinary.Attrs{"call-id": "CALL_ID", "call-creator": creator}}}}
	return eng, offer, ownPN, ownLID
}

func TestInboundOfferReceiptPreservesExactStanzaAndCreator(t *testing.T) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/61fda7e2af1d53744ae1ce348075d87964d90c6e/examples/cli/call.go#L509-L557
	for _, namespace := range []string{types.HiddenUserServer, types.DefaultUserServer} {
		t.Run(namespace, func(t *testing.T) {
			eng, offer, ownPN, ownLID := offerReceiptFixture()
			caller := offer.AttrGetter().JID("from")
			caller.Server = namespace
			offer.Attrs["from"] = caller
			var sent []waBinary.Node
			eng.sendCallNode = func(_ context.Context, node waBinary.Node) error { sent = append(sent, node); return nil }
			if eng.onCallRaw(&offer) {
				t.Fatal("offer receipt swallowed the existing generic ack/event handler")
			}
			if len(sent) != 1 {
				t.Fatalf("offer receipt count = %d, want 1", len(sent))
			}
			got := sent[0]
			from := ownPN
			if namespace == types.HiddenUserServer {
				from = ownLID
			}
			attrs := got.AttrGetter()
			if got.Tag != "receipt" || attrs.String("id") != "OFFER_STANZA" || attrs.JID("to") != caller || attrs.JID("from") != from {
				t.Fatalf("receipt did not retain offer routing: %v", got)
			}
			actions := got.GetChildren()
			if len(actions) != 1 || actions[0].Tag != "offer" {
				t.Fatal("receipt action differs from offer")
			}
			identity := actions[0].AttrGetter()
			if identity.String("call-id") != "CALL_ID" || identity.JID("call-creator") != offer.GetChildren()[0].AttrGetter().JID("call-creator") {
				t.Fatal("receipt changed the call identity")
			}
			if len(eng.calls) != 0 {
				t.Fatal("receipt created a call or started media before application Answer")
			}
		})
	}
}

func TestInboundOfferReceiptIgnoresInvalidAndEndedNodes(t *testing.T) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/61fda7e2af1d53744ae1ce348075d87964d90c6e/examples/cli/call.go#L509-L557
	for _, name := range []string{"missing_stanza_id", "missing_from", "missing_call_id", "missing_creator", "not_call", "no_action", "multiple_actions", "unrelated", "ended_flag", "ended_reason", "missing_own_lid", "missing_own_pn"} {
		t.Run(name, func(t *testing.T) {
			eng, offer, _, _ := offerReceiptFixture()
			children := offer.GetChildren()
			switch name {
			case "missing_stanza_id":
				delete(offer.Attrs, "id")
			case "missing_from":
				delete(offer.Attrs, "from")
			case "missing_call_id":
				delete(children[0].Attrs, "call-id")
			case "missing_creator":
				delete(children[0].Attrs, "call-creator")
			case "not_call":
				offer.Tag = "message"
			case "no_action":
				offer.Content = nil
			case "multiple_actions":
				offer.Content = append(children, waBinary.Node{Tag: "offer_notice"})
			case "unrelated":
				children[0].Tag = "offer_notice"
			case "ended_flag":
				children[0].Attrs["is_call_ended"] = "1"
			case "ended_reason":
				children[0].Attrs["terminate_reason"] = "accepted_elsewhere"
			case "missing_own_lid":
				eng.c.wa.Store.LID = types.EmptyJID
			case "missing_own_pn":
				caller := offer.AttrGetter().JID("from")
				caller.Server = types.DefaultUserServer
				offer.Attrs["from"] = caller
				eng.c.wa.Store.ID = nil
			}
			count := 0
			eng.sendCallNode = func(context.Context, waBinary.Node) error { count++; return nil }
			if eng.onCallRaw(&offer) {
				t.Fatal("unhandled raw node swallowed the existing handler")
			}
			if count != 0 {
				t.Fatalf("invalid/ended node sent %d receipts", count)
			}
		})
	}
}

func TestInboundOfferReceiptDuplicatesAndReorderingDoNotChangePhase(t *testing.T) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/61fda7e2af1d53744ae1ce348075d87964d90c6e/examples/cli/call.go#L509-L557
	for _, phase := range []CallPhase{CallPhaseRinging, CallPhaseConnecting, CallPhaseActive, CallPhaseEnded} {
		eng, offer, _, _ := offerReceiptFixture()
		call := &Call{eng: eng, id: "CALL_ID", phase: phase}
		eng.calls[call.ID()] = &engineCall{call: call, direction: CallDirectionIncoming}
		count := 0
		eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
			if node.Tag != "receipt" {
				t.Fatal("receipt authorized additional signaling")
			}
			count++
			return nil
		}
		eng.onCallRaw(&offer)
		eng.onCallRaw(&offer)
		if count != 2 || call.State() != phase || eng.lookup(call.ID()).acceptPending || eng.lookup(call.ID()).acceptSent || eng.lookup(call.ID()).started {
			t.Fatal("duplicate/reordered receipt altered the call state")
		}
	}
	eng, offer, _, _ := offerReceiptFixture()
	eng.sendCallNode = func(context.Context, waBinary.Node) error { return nil }
	retired := &Call{eng: eng, id: "CALL_ID", phase: CallPhaseRinging}
	eng.calls[retired.ID()] = &engineCall{call: retired, direction: CallDirectionIncoming}
	eng.finishCall(retired.ID(), "retired")
	eng.onCallRaw(&offer)
	if eng.lookup(retired.ID()) != nil || retired.State() != CallPhaseEnded {
		t.Fatal("offer receipt resurrected a retired handle")
	}
}

func TestInboundOfferReceiptFailureKeepsOriginalHandler(t *testing.T) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/61fda7e2af1d53744ae1ce348075d87964d90c6e/examples/cli/call.go#L509-L557
	eng, offer, _, _ := offerReceiptFixture()
	attempted := 0
	eng.sendCallNode = func(context.Context, waBinary.Node) error { attempted++; return errors.New("offline") }
	if eng.onCallRaw(&offer) || attempted != 1 {
		t.Fatal("receipt failure suppressed the original handler or retried")
	}
	if len(eng.calls) != 0 {
		t.Fatal("receipt failure created call state")
	}
}

func TestInboundOfferReceiptConcurrentDuplicatesKeepCallState(t *testing.T) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/61fda7e2af1d53744ae1ce348075d87964d90c6e/examples/cli/call.go#L509-L557
	eng, offer, _, _ := offerReceiptFixture()
	call := &Call{eng: eng, id: "CALL_ID", phase: CallPhaseRinging}
	m := &engineCall{call: call, direction: CallDirectionIncoming}
	eng.calls[call.ID()] = m
	var receipts atomic.Int32
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		if node.Tag != "receipt" {
			t.Error("receipt authorized additional signaling")
		}
		receipts.Add(1)
		return nil
	}
	var work sync.WaitGroup
	for range 32 {
		work.Add(1)
		go func() {
			defer work.Done()
			if eng.onCallRaw(&offer) {
				t.Error("offer receipt swallowed the existing handler")
			}
		}()
	}
	work.Wait()
	if receipts.Load() != 32 || call.State() != CallPhaseRinging || m.acceptPending || m.acceptSent || m.started {
		t.Fatal("concurrent receipts altered state or started media")
	}
}
