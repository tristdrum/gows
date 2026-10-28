# Compact stored chat index

`MessageService.GetChatIndex` provides the Min-specific stored-data index. The
request contains the session, a result limit from 1 to 50, a canonical source
offset, and `unread_only`. It returns `Json.data` containing:

```json
{
  "chats": [{"chat": "StoredChat", "message": "StoredMessage"}],
  "source_page": {
    "version": 1, "offset": 0, "limit": 50, "scanned_count": 50,
    "next_offset": 50, "complete": false
  }
}
```

The example's chat/message strings stand for the existing structured objects.
The index uses stored data only; it never fetches pictures, downloads media,
refreshes contacts from WhatsApp, or marks anything read. WAHA converts the
selected stored messages without enrichment. Legacy `GetChats` is unchanged.

Canonical PN/LID aliases merge before ordering by latest timestamp descending
and canonical JID ascending. Latest-message ties prefer PN representation and
then message ID. Source offsets refer to that live canonical sequence, not to
the number of returned unread matches. Separate requests are not a snapshot.

Status rows are excluded from identity selection before canonical merging,
ordering and source pagination. They are available only through the separate
[Status history capability](STATUS_HISTORY.md).

Unread reads consume at most 1,000 source chats and stop on the requested match
count or proven source exhaustion. `scanned_count` and continuation count only
the consumed prefix; unknown unread state beyond it does not invalidate a page.
`complete` requires proven exhaustion, and `next_offset` is null only then.
Unknown consumed unread state returns `FailedPrecondition`. A required malformed
latest message returns `DataLoss`, never an older substitute or false empty
result. Stores lacking this capability return `Unimplemented`.

The SQL path selects lightweight latest-message identities first and loads full
payloads only for selected results. Alias, read-state, inbound-count and name
lookups are batched. Identity selection can still scan the message index; there
is no claim of history-independent database cost. Postgres reads use a
repeatable-read transaction. The gRPC context reaches database acquisition and
queries, including existing message-list reads. Client timeout does not by
itself prove provider work stopped; cancellation tests cover pool and query
waits.

Run `make build-proto` before Go tests. Generated Go bindings remain ignored
build artifacts. Focused validation:

```sh
cd src
go test ./storage/... ./server
```

For Postgres coverage set `GOWS_TEST_POSTGRES_DSN` to a disposable local database
named `gows_*_test`. The harness rejects remote hosts and other database names.
Index tests clear the disposable source tables. The optional Postgres test
records the query plan timing and 20-sample p95 for 50 chats over 101,000 rows,
and proves cancellation of blocked SQL followed by a successful read.

`TestChatIndexDirectContactPostgresPerformance` covers the full `ChatView`,
including the real device-scoped contact-name store, over 101,000 messages,
1,000 direct chats and address books of 1,000 and 10,000 contacts. It checks
1/25/50-result pages, constant SQL round trips, cross-device name isolation,
and 20-sample p95. Contact lookup currently materializes that device's full
address-book metadata in one query; the fixture measures this existing cost
rather than claiming contact materialization is bounded by page size.
