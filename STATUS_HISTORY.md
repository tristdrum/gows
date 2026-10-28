# Current Status buffer

Min's dedicated Status read API uses these internal RPCs:

| RPC | Purpose |
| --- | --- |
| `ConfigureStatusHistory` | Hot enable/renew or disable/purge without session restart. |
| `GetStatusHistory` | Current captured messages with fixed-window keyset continuation and source accounting. |
| `GetStatusHistoryMessage` | Exact stored Status with sender, direction, generation and binding proof. |
| `DownloadStatusHistoryMedia` | Bounded original image/audio/video bytes without WAHA's media cache. |

The existing per-session `gows_messages` table holds content. Migration 009 adds
only capture authorization/coverage, original expiry/provenance and revoke
metadata. No second content archive is created. Generic message lists, exact
reads, latest-message queries and the compact chat index exclude Status rows.

Min supplies an account, expected phone, shard, failover epoch and opaque owner
authorization subject. WAHA verifies these against its exact local session,
metadata and Min webhook route. GOWS verifies the device phone and requires an
online logged-in device before enabling. Controls carry a monotonically newer
`observed_at_ms` and at most three minutes of authorization. A byte-identical
repeated control is idempotent; stale or conflicting controls fail closed.
Reads repeat the binding and exact reads also repeat capture generation.

A single policy-row transaction serializes controls, content admission,
revoke tombstones, reads and purge for both SQLite and Postgres. A disabled
policy purges Status content immediately. Expired authorization rejects new
capture and all content reads immediately; maintenance removes its content
within a minute under normal database availability. Re-enabling after an
authorization gap or changing the owner/route binding creates a generation
and discards the old buffer. API key rotation retains the same owner subject.

Capture is independent of `ignore.status`. Status events do not enter the
ordinary message/unread/revoke pipeline, and the canonical Min WAHA session
keeps normalized Status webhooks suppressed. These RPCs never mark statuses
viewed, send receipts, set presence, invoke AI or download media eagerly.

The admission timestamp fences work delayed across disable/re-enable.
Original provider timestamps determine expiry, at most 24 hours and earlier
when provider context supplies a shorter lifetime. Only live events dated no
earlier than the current capture start are admitted. This does not backfill
previously ignored content. Missing sender provenance is recorded as a known
coverage gap; malformed stored representations count as unavailable source
items rather than producing a false complete page.

Startup prunes before the store serves reads. A benign restart preserves a
still-authorized, unexpired buffer and generation, recording conservatively
uncertain coverage from the last acknowledged control to restart. A restored
expired authorization purges content before reauthorization can expose it.
Normal backup retention is unchanged. Revoke tombstones expire after 24 hours
and suppress delayed copies without retaining revoked content.

Media download decrypts through WhatsMeow directly into an immediately unlinked
temporary file. Writes, encrypted overhead and final plaintext size are bounded
to the requested cap (maximum 25 MiB). A 14-second RPC context bounds transport,
and authority/expiry/generation are rechecked before every 64 KiB response chunk.
The handle is closed on success, cancellation and error. No transcoding,
MediaManager, S3 object, caller URL or viewed receipt is involved.

Run `make build-proto`, then from `src/`:

```sh
go test ./storage/... ./gows ./server
go test -race ./storage/sqlstorage -run '^TestStatus'
```

For real Postgres coverage use `GOWS_TEST_POSTGRES_DSN` pointing only to a
disposable loopback database named `gows_*_test`. The shared harness runs actual
migrations and clears its source tables. Never use a live session database.
