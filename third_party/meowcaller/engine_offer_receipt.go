package meowcaller

import (
	"context"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

// sendOfferReceipt registers this device for the incoming call without answering it.
func (e *engine) sendOfferReceipt(callNode *waBinary.Node) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/61fda7e2af1d53744ae1ce348075d87964d90c6e/examples/cli/call.go#L509-L557
	kids := callNode.GetChildren()
	if callNode.Tag != "call" || len(kids) != 1 || kids[0].Tag != "offer" {
		return
	}
	oag := kids[0].AttrGetter()
	if oag.OptionalString("is_call_ended") == "1" || oag.OptionalString("terminate_reason") != "" {
		return
	}
	cag := callNode.AttrGetter()
	stanzaID := cag.String("id")
	caller := cag.JID("from")
	callID := oag.String("call-id")
	creator := oag.JID("call-creator")
	if stanzaID == "" || caller.IsEmpty() || callID == "" || creator.IsEmpty() {
		return
	}
	if e.c == nil || e.c.wa == nil || e.c.wa.Store == nil {
		return
	}
	ownFrom := e.c.wa.Store.GetJID()
	if caller.Server == types.HiddenUserServer {
		ownFrom = e.c.wa.Store.GetLID()
	}
	if ownFrom.IsEmpty() {
		return
	}
	receipt := waBinary.Node{
		Tag: "receipt",
		Attrs: waBinary.Attrs{
			"to":   caller,
			"id":   stanzaID,
			"from": ownFrom,
		},
		Content: []waBinary.Node{{
			Tag: "offer",
			Attrs: waBinary.Attrs{
				"call-id":      callID,
				"call-creator": creator,
			},
		}},
	}
	if err := e.transmitCallNode(context.Background(), receipt); err != nil {
		e.c.log.Warn().Msg("send offer receipt failed")
	}
}
