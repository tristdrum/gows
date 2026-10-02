# Experimental Min calling transport

Calling is disabled unless the session's exact name appears in `GOWS_CALLING_SESSIONS`.
The existing WhatsMeow client is reused before it connects; no extra login or
identity store is created. Messaging-only sessions retain the original handlers.

The private Unix-socket `calling.Calling` gRPC service supports one-to-one audio
control and one media owner per call. Media is PCM16LE mono 16 kHz in 1920-byte
(60 ms) frames. Sequence and immutable session/call bindings are mandatory.
Queues hold at most two frames; backpressure ends the call. Closing the stream
ends the call. Calls have a ten-minute cap and a thirty-second ringing cap.
Outbound request IDs are consumed before dialing and never retried by this
adapter. The Min coordinator must persist authorization and attempt identity
across process restarts. Video and group calling are outside this contract.

Control `status` with an empty call ID and the outbound request ID recovers an
attempt's call ID after a lost response. Control `cancel` consumes that request ID
even if its dial has not arrived yet, cancels in-flight setup, and fences a late
successful offer. `dialing` is nonterminal; only `ended` proves the attempt has
settled. The call ID remains available for peer-end cleanup. One terminate
attempt is bounded to five seconds; never turn an uncertain outcome into redial.

`calling.Event` emits only ID, peer, state and direction. Raw audio, call keys,
relay credentials and provider signaling payloads are never emitted. The caller
must verify identity and account permissions before accepting or dialing.

The media implementation is the MIT-licensed
[purpshell/meowcaller](https://github.com/purpshell/meowcaller/tree/27a3c6b18657614c9ec2ed16dfc497eff11de6ec),
pinned to `27a3c6b18657614c9ec2ed16dfc497eff11de6ec`. Its raw stanza hook uses
reflection/unsafe, so this exact dependency layout and actual-device media must
be tested before rollout. Min's existing devlikeapro WhatsMeow replacement stays
at `868907e7d4878c6cb0d8b54f04bdee2e2664b547`. The dependency's codec/protocol
tests pass with this replacement; they do not establish live WhatsApp media.

Run `make build-proto`, `cd src && go test ./...`, and
`go test -race ./calling ./gows ./server`. Digital duplex audio and messaging
regression receipts on the approved test pair are required for rollout.
