# Message history integrity

`storage.IsRealMessage` is the canonical classifier for unwrapped provider
content. `SqlMessageStore.UpsertOneMessage` chooses the accepted representation
and updates its JSON, indexed classification and routing fields atomically.
Real content survives key-only duplicates, normalized edits survive original
message replay, and status is monotonic across delivery, history and receipts.
Receipts use `UpdateMessageStatus`; they must not upsert a previously read
message snapshot. Existing revoke/delete handlers remain authoritative.

An update retries a bounded compare-and-set when another writer changes the
payload. Malformed existing payloads fail without replacement. A delete observed
during an update is not reinserted. This does not introduce persistent revoke
tombstones or change the handling of a new delivery after an already completed
delete.

The separate [current Status buffer](STATUS_HISTORY.md) adds bounded revoke
tombstones only for explicitly authorized Status capture. Generic message
storage continues to exclude Status; ordinary chat semantics above are unchanged.

Run focused regressions from `src/`:

```sh
go test ./storage/... ./gows ./server
go test -race ./storage/sqlstorage -run 'TestMessageIntegrity(CAS|Concurrent|Receipts)'
```

The integrity tests always use temporary SQLite databases. To also run against
PostgreSQL, set `GOWS_TEST_POSTGRES_DSN` through a private local environment file.
It must identify a disposable loopback database named `gows_*_test`; the harness
runs the actual provider migrations. Never use a live session or application
database. The tests skip PostgreSQL when the variable is absent.

Generate ignored protobuf sources with `make build-proto` if needed; `protoc`,
`protoc-gen-go` and `protoc-gen-go-grpc` must be on PATH.

## Explicit historical repair

Build from `src/` with
`go build -o ../bin/gows-history-repair ./cmd/history-classification-repair`.
The standalone command opens an existing SQLite file directly; it runs no
migrations, session startup, webhooks or WhatsApp operations.

```sh
gows-history-repair audit --db /absolute/provider.db --group fixture@g.us \
  --since 1700000000 --until 1700000100 --ids exact-provider-id \
  --max-candidates 1 --manifest /private/new-manifest.json
gows-history-repair apply --db /absolute/provider.db --manifest /private/new-manifest.json
gows-history-repair rollback --db /absolute/provider.db --manifest /private/new-manifest.json
```

Audit uses SQLite `mode=ro` and `query_only`, an exact group and exclusive-end
time window, at most 100 candidates and at most 1,000 selected source rows.
`--ids` restricts the scope further and requires every supplied ID to exist
inside the group/window. Reports contain counts only. The manifest is created
exclusively with mode 0600 and contains private message payloads, exact
before/after states and digests; never commit, print or share it.

Only intact content classified as real is eligible. Prefer correcting only the
indexed column; correct serialized `IsReal` only when needed. Unknown fields
are preserved. Malformed/ambiguous rows are reported and excluded, and a
key-only payload with inconsistent classification stays unresolved. This tool
cannot recover overwritten content.

Apply and rollback validate the entire manifest and perform one transaction.
Every row must still match its expected payload, classification, group and
timestamp. A changed/missing candidate refuses the transaction. Repeating the
same operation skips rows already at its target state. Rollback refuses rows
changed by newer provider activity.

The caller must separately approve the exact live scope and revalidate durable
account/session/shard/epoch routing plus the active process's database path
immediately before a production operation. This file-level command cannot
establish those application routing facts. Run the exact known row first, then
separately audit any approved bounded remainder.
