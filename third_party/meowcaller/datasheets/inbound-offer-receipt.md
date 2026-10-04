# Inbound offer receipt

**Status:** implemented; offline wire-shape and race checks pass; fresh live proof pending

**Reference pinned at:** 61fda7e2af1d53744ae1ce348075d87964d90c6e

## Reference source

Verbatim historical working CLI implementation: [call.go lines 509–557](https://github.com/purpshell/meowcaller/blob/61fda7e2af1d53744ae1ce348075d87964d90c6e/examples/cli/call.go#L509-L557).

```go
// sendOfferReceipt sends the WA-Web/reference-style <receipt> for an incoming
// <call><offer> (it carries the <call> stanza id, which the CallOffer event
// drops). Real callees and the reference (send_offer_ack_receipt) send this to
// register the device as a call participant; whatsmeow's auto <ack class="call">
// is not a substitute.
func (c *coordinator) sendOfferReceipt(callNode *waBinary.Node) {
	kids := callNode.GetChildren()
	if len(kids) != 1 || kids[0].Tag != "offer" {
		return
	}
	offer := &kids[0]
	oag := offer.AttrGetter()
	// Skip a "call ended" notification (accepted_elsewhere etc.) — whatsmeow still auto-acks
	// the <call>, but we don't receipt or engage an already-dead call.
	if oag.OptionalString("is_call_ended") == "1" || oag.OptionalString("terminate_reason") != "" {
		return
	}
	cag := callNode.AttrGetter()
	stanzaID := cag.String("id")
	caller := cag.JID("from")
	if stanzaID == "" || caller.IsEmpty() {
		return
	}
	// own "from": LID for a LID call, else PN (matches the reference).
	ownFrom := c.cli.Store.GetJID()
	if caller.Server == types.HiddenUserServer {
		ownFrom = c.cli.Store.GetLID()
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
				"call-id":      oag.String("call-id"),
				"call-creator": oag.JID("call-creator"),
			},
		}},
	}
	if err := c.cli.DangerousInternals().SendNode(c.ctx, receipt); err != nil {
		c.log.Error().Err(err).Str("call_id", oag.String("call-id")).Msg("send offer receipt failed")
		return
	}
	c.log.Info().Str("call_id", oag.String("call-id")).Msg("sent offer receipt")
}
```

## Go envelope

```go
func (e *engine) sendOfferReceipt(callNode *waBinary.Node)
```

`engine.onCallRaw` invokes the helper for an `offer` action and returns false,
retaining WhatsMeow's event dispatch and generic call acknowledgement. The receipt
uses the original stanza ID and caller device address, not a generated call ID or
normalized peer address. Its offer child retains the call ID and creator. Own
identity uses LID for a LID caller and PN otherwise.

## Implementation and validation

The helper ports the reference shape through the existing `transmitCallNode` test
seam. It additionally ignores unrelated raw node tags, incomplete call identities
and absent local identities. Send failure retains the original handler and emits
only a fixed warning with no payload or peer data. It does not retry, mutate the
call registry, answer the call or start media.

`engine_offer_receipt_test.go` covers PN/LID routing, original stanza-versus-call ID,
creator preservation, invalid and ended notifications, all active lifecycle phases,
duplicate and concurrent offers, retired handles and failed receipt transmission.
The previously repaired first-mute/Answer acceptance ordering remains unchanged.
Fresh live media proof and the exact cause of earlier remote termination are still
unverified. No outbound transport or mute sequence is added.
