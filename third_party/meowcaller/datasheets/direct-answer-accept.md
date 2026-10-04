# Direct-call application acceptance

Status: implemented; focused offline race regressions pass. Live interoperability
and duplex media remain unverified.

**Reference pinned at:** 102ef4084d2a6e8d41a01e01591d1eb0837d370b

## Reference source

[AcceptCall.start](https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L402-L465)
sends preaccept, prepares the call, checks that its registration is current, then
sends accept on application intent. Its final direct-call acceptance block is:

```rust
        if !self.client.is_socket_connected() {
            return Err(CallError::Connect(ERR_DISCONNECTED_DURING_SETUP.into()));
        }
        registration.ensure_current()?;
        // Final acceptance waits until media setup succeeded and the registered generation is still
        // current; only then may the caller apply the participant keys and enter the call.
        if let Some(accept) = accept {
            send_answer_node(self.client, &registration, &mut teardown, accept).await?;
        }
```

[build_answer_signaling](https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L1484-L1522)
binds direct acceptance to the offering device and the original call creator.
[send_answer_node](https://github.com/oxidezap/whatsapp-rust/blob/102ef4084d2a6e8d41a01e01591d1eb0837d370b/src/voip/facade.rs#L1568-L1583)
guards the live registration around a delivery-ambiguous send.

## Go contract

`engine.answer` is the authority for direct-call acceptance. Before Answer, mute
signals never accept the call. Answer claims one send, retaining `engineCall.from`
and `creator` from the offer. A retired/replaced handle cannot claim acceptance;
terminal cleanup cancels an in-flight acceptance. Failed sends retire only their current generation, preserve the transport error,
and are not retried because their delivery is ambiguous. A successful send must
still belong to the same registry entry and handle; media startup checks that
binding under the engine lock before claiming the media loop.

`mute_v2` remains a remote mute-state notification. The initial mute gate from
the earlier engine implementation is superseded by application-owned acceptance.
Group acceptance and outgoing transport negotiation are unchanged.

## Validation boundary

`engine_answer_test.go` covers no-mute Answer, concurrent duplicates, original
binding, cancelled/failed send, and retired/replaced handles, including delayed send results
and guarded media startup. Existing ordering, offer
receipt, and lifecycle tests remain required. These fixtures prove control flow;
they do not negotiate a real relay or prove audio. Exact outgoing transport rounds
and actual two-direction media still require approved official-client testing.
