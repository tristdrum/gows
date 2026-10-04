package meowcaller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func TestSignalObservationKeepsWireContentsPrivateAndCallbacksUnlocked(t *testing.T) {
	// Source of truth: https://github.com/devlikeapro/whatsmeow/blob/868907e7d4878c6cb0d8b54f04bdee2e2664b547/call.go#L13-L102
	const private = "PRIVATE_SENTINEL"
	eng, call, _ := deferredAcceptFixture()
	var samples []SignalObservation
	eng.c.OnSignalObservation(func(sample SignalObservation) {
		if eng.lookup(sample.CallID) == nil {
			t.Fatal("observer saw missing fixture")
		}
		eng.c.OnIncomingCall(nil)
		samples = append(samples, sample)
	})
	eng.sendCallNode = func(context.Context, waBinary.Node) error { return nil }
	node := waBinary.Node{Tag: "call", Attrs: waBinary.Attrs{"from": private}, Content: []waBinary.Node{{Tag: "transport", Attrs: waBinary.Attrs{"call-id": call.ID(), "call-creator": private, "transport-message-type": private}, Content: []byte(private)}}}
	eng.onCallRaw(&node)
	if err := eng.transmitCallNode(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(samples)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 2 || samples[0].Kind != "transport" || samples[1].Kind != "sent_transport" || samples[0].TransportType != "other" || strings.Contains(string(encoded), private) || strings.Contains(string(encoded), call.ID()) {
		t.Fatalf("diagnostics exposed private wire data or lost boundaries: %s", encoded)
	}
}

func TestOfferAckObservationUsesOfferStanzaIdentityAndSeparatesAllocation(t *testing.T) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L2139-L2169
	eng, call, _ := deferredAcceptFixture()
	m := eng.calls[call.ID()]
	m.direction, m.offerStanzaID = CallDirectionOutgoing, "OFFER_STANZA"
	var samples []SignalObservation
	eng.c.OnSignalObservation(func(s SignalObservation) { samples = append(samples, s) })
	ack := waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"class": "call", "type": "offer", "id": "unrelated"}}
	eng.onCallAck(&ack)
	if len(samples) != 0 {
		t.Fatal("unrelated stanza acknowledgement entered call diagnostics")
	}
	ack.Attrs["id"] = m.offerStanzaID
	eng.onCallAck(&ack)
	ack.Content = []waBinary.Node{{Tag: "relay", Attrs: waBinary.Attrs{"call-id": call.ID()}}}
	eng.onCallAck(&ack)
	if len(samples) != 3 || samples[0].Kind != "offer_ack" || samples[0].RelayPresent || samples[1].Kind != "offer_ack" || samples[2].Kind != "relay_arrival" || !samples[2].RelayPresent || m.started {
		t.Fatalf("acknowledgement/allocation observations conflated media readiness: %+v", samples)
	}
}
