package meowcaller

import (
	waBinary "go.mau.fi/whatsmeow/binary"
)

// SignalObservation describes a control boundary without its wire contents.
// CallID is private correlation metadata and must never be logged.
type SignalObservation struct {
	CallID        string    `json:"-"`
	Kind          string    `json:"kind"`
	TransportType string    `json:"transport_type,omitempty"`
	Phase         CallPhase `json:"phase"`
	RelayPresent  bool      `json:"relay_present"`
}

// OnSignalObservation registers an optional content-free control observer.
func (c *Client) OnSignalObservation(fn func(SignalObservation)) {
	// Source of truth: https://github.com/tristdrum/gows/blob/12c2bbb57238abcc2eb1da90d66ca17d5c87f2e3/third_party/meowcaller/client.go#L227-L239
	c.mu.Lock()
	c.onSignal = fn
	c.mu.Unlock()
}

func (e *engine) observeSignal(callID, kind, transportType string) {
	// Source of truth: https://github.com/tristdrum/gows/blob/12c2bbb57238abcc2eb1da90d66ca17d5c87f2e3/third_party/meowcaller/engine.go#L790-L818
	if e.c == nil || callID == "" {
		return
	}
	sample := SignalObservation{CallID: callID, Kind: kind, TransportType: transportType}
	e.mu.Lock()
	if m := e.calls[callID]; m != nil && m.call != nil {
		sample.Phase = m.call.State()
		sample.RelayPresent = m.relay != nil
	}
	e.mu.Unlock()
	e.c.mu.Lock()
	fn := e.c.onSignal
	e.c.mu.Unlock()
	if fn != nil {
		fn(sample)
	}
}

func (e *engine) observeSignalNode(node *waBinary.Node, sent bool) {
	// Source of truth: https://github.com/devlikeapro/whatsmeow/blob/868907e7d4878c6cb0d8b54f04bdee2e2664b547/call.go#L13-L102
	children := node.GetChildren()
	if len(children) != 1 || (node.Tag != "call" && node.Tag != "receipt") {
		return
	}
	action := children[0]
	kind := action.Tag
	switch kind {
	case "offer", "preaccept", "relaylatency", "transport", "accept", "mute_v2", "terminate", "reject":
	default:
		return
	}
	if node.Tag == "receipt" {
		if !sent || kind != "offer" {
			return
		}
		kind = "offer_receipt"
	}
	transportType := ""
	if action.Tag == "transport" {
		transportType = "other"
		switch value := action.AttrGetter().OptionalString("transport-message-type"); value {
		case "1", "3", "9":
			transportType = value
		}
	}
	if sent {
		kind = "sent_" + kind
	}
	e.observeSignal(action.AttrGetter().OptionalString("call-id"), kind, transportType)
}

func (e *engine) observeOfferAck(node *waBinary.Node) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L2139-L2169
	attrs := node.AttrGetter()
	if attrs.OptionalString("class") != "call" || attrs.OptionalString("type") != "offer" {
		return
	}
	id := attrs.OptionalString("id")
	if id == "" {
		return
	}
	callID := ""
	e.mu.Lock()
	for candidateID, m := range e.calls {
		if m.direction == CallDirectionOutgoing && m.offerStanzaID == id {
			callID = candidateID
			break
		}
	}
	e.mu.Unlock()
	e.observeSignal(callID, "offer_ack", "")
}
