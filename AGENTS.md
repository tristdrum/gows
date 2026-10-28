- Resolve the pinned WhatsMeow source from `src/` with `go list -m -f '{{.Dir}}' go.mau.fi/whatsmeow`; treat the shared module cache as read-only.
- Run `make test` after changing the WhatsMeow pin. It includes the offline status-frame acknowledgement regression in a temporary copy of the pinned module; never modify the shared Go module cache.
- For message persistence or classification changes, follow `HISTORY_STORAGE.md` and exercise its actual SQLite/PostgreSQL regressions; receipt writes must use the status-only storage operation.

- For compact chat index changes, preserve `CHAT_INDEX.md` contracts and run its SQLite/Postgres pagination, bounded-query, and cancellation regressions.
- For explicit Status capture/reads, preserve `STATUS_HISTORY.md` lease, expiry and non-caching media boundaries; generic message routes must never expose Status content.
- Restored local `gows.db` snapshots require the Status restore marker before session startup; ordinary restarts retain valid Status history, but restored snapshots must invalidate it to prevent revoke resurrection.
