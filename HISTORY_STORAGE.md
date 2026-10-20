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
