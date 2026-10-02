package meowcaller

import (
	"context"
	"errors"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func TestFailedOfferRetiresRegisteredCall(t *testing.T) {
	c := &Client{}
	e := newEngine(c)
	c.eng = e
	call := &Call{eng: e, id: "failed", phase: CallPhaseCalling}
	e.calls[call.id] = &engineCall{call: call, callKey: []byte("sensitive")}
	e.sendCallNode = func(context.Context, waBinary.Node) error { return errors.New("send failed") }
	if err := e.sendOffer(context.Background(), call.id, waBinary.Node{}); err == nil {
		t.Fatal("failed send reported success")
	}
	if e.calls[call.id] != nil || call.State() != CallPhaseEnded {
		t.Fatal("failed send retained an unowned call")
	}
}

func TestGroupDetectionDoesNotDependOnRoster(t *testing.T) {
	e := newEngine(&Client{})
	call := &Call{eng: e, id: "group"}
	e.calls[call.id] = &engineCall{call: call, group: true}
	if !call.IsGroup() {
		t.Fatal("group invitation without roster accepted as direct")
	}
}
