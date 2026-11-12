# Min maintenance history

The private `GetMaintenanceHistory` RPC returns fixed-window keyset pages and
provider-observed coverage. It does not claim complete global WhatsApp history.

Keep the notification registration synchronous before goroutines. Manual history
acknowledgements follow checked auxiliary writes, ordinary storage and a durable
stored marker. Never equate `OfflineSyncCompleted` with finished persistence.

The additive `gows_maintenance_*` tables preserve the existing migration version.
Legacy writes must keep working and invalidate coverage. Exact message ID/chat/time
registrations, including original edit/revoke targets, authorize writes; unrelated
pending work never does. Triggers use standard SQL, not connection-local UDFs.
Status, newsletters and broadcasts are excluded from seed, activity, reads and raw
payloads regardless of grants. Validated PN/LID aliases remain intact. Metadata retains no deleted content. Invalidate coverage
on dedicated `.min-maintenance-history-restored` markers before exposing the store.

Run `go test -race -p 2 ./gows ./storage/sqlstorage ./server` from `src` after generating
protobuf bindings. Tests use temporary databases and failure-injection fixtures;
never connect a customer session for this suite. Live-image/delivery proof belongs
to Min's authorized release, whose canonical contract is
`min.cool/docs/GOWS_MAINTENANCE_HISTORY.md`.

PostgreSQL boundary tests use a caller-created local `gows_*_test` database through
`GOWS_TEST_POSTGRES_DSN`; they clear only its public schema. The optional
`GOWS_SEED_TIMING=1 go test ./storage/sqlstorage -run ^TestMaintenanceSeedTiming$ -count=1 -v`
rehearsal creates/removes 28 synthetic SQLite databases with four parallel starters.
Its local warm-cache timing is not Linux image or live readiness proof.

Maintenance pages reconcile every LID in their time window against the durable
provider mapping before chat filtering and token calculation. Cached callback
hints cannot authorize a PN alias. Reassignment/removal invalidates affected
window tokens, including a LID moving out of the selected chat; unresolved LIDs
and unknown LID filters hold coverage. PostgreSQL uses one repeatable-read snapshot
for mapping reconciliation and the page. No WhatsApp request is made.
