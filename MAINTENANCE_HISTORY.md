# Min maintenance history

The private `GetMaintenanceHistory` RPC returns fixed-window keyset pages and
provider-observed coverage. It does not claim complete global WhatsApp history.

Keep the notification registration synchronous before goroutines. Manual history
acknowledgements follow checked auxiliary writes, ordinary storage and a durable
stored marker. Never equate `OfflineSyncCompleted` with finished persistence.

The additive `gows_maintenance_*` tables preserve the existing migration version.
Legacy writes must keep working and invalidate coverage; triggers use standard SQL,
not connection-local UDFs. Metadata retains no deleted content. Invalidate coverage
on dedicated `.min-maintenance-history-restored` markers before exposing the store.

Run `go test -race -p 2 ./gows ./storage/sqlstorage ./server` from `src` after generating
protobuf bindings. Tests use temporary databases and failure-injection fixtures;
never connect a customer session for this suite. Live-image/delivery proof belongs
to Min's authorized release, whose canonical contract is
`min.cool/docs/GOWS_MAINTENANCE_HISTORY.md`.
