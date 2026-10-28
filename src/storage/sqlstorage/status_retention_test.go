package sqlstorage

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

func exactQuery(binding storage.StatusBinding, generation, id string) storage.StatusExact {
	return storage.StatusExact{StatusBinding: binding, Generation: generation, ID: id, Sender: "15550000002@s.whatsapp.net"}
}
func TestStatusOriginalExpiryRevokeAndTombstone(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		capture := enableStatus(t, st, *now, binding)
		msg := capturedMessage("revoked", *now)
		require.NoError(t, st.Capture(ctx, msg, binding.ExpectedPhone, now.UnixMilli()))
		exact := exactQuery(binding, capture.Generation, "revoked")
		record, err := st.Exact(ctx, exact)
		require.NoError(t, err)
		require.Equal(t, now.Add(24*time.Hour).UnixMilli(), record.Source.ExpiresAtMS)
		revoke := capturedMessage("revoke-event", *now)
		revoke.Message.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum(), Key: &waCommon.MessageKey{ID: proto.String("revoked")}}}
		require.NoError(t, st.Capture(ctx, revoke, binding.ExpectedPhone, now.UnixMilli()))
		require.NoError(t, st.Capture(ctx, msg, binding.ExpectedPhone, now.UnixMilli()))
		_, err = st.Exact(ctx, exact)
		require.ErrorIs(t, err, storage.ErrNotFound)
		msg = capturedMessage("short-lived", *now)
		msg.Message.Message = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("short"), ContextInfo: &waE2E.ContextInfo{Expiration: proto.Uint32(10)}}}
		require.NoError(t, st.Capture(ctx, msg, binding.ExpectedPhone, now.UnixMilli()))
		*now = now.Add(11 * time.Second)
		_, err = st.Exact(ctx, exactQuery(binding, capture.Generation, "short-lived"))
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.NoError(t, st.Prune(ctx))
		var count int
		require.NoError(t, messages.db.Get(&count, "SELECT count(*) FROM gows_messages WHERE jid='status@broadcast'"))
		require.Zero(t, count)
	})
}
func TestStatusLeaseExpiryRestartAndOwnerTransfer(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		first := enableStatus(t, st, *now, binding)
		require.NoError(t, st.Capture(ctx, capturedMessage("old", *now), binding.ExpectedPhone, now.UnixMilli()))
		*now = now.Add(4 * time.Minute)
		page, err := st.List(ctx, statusQuery(binding, *now))
		require.NoError(t, err)
		require.Empty(t, page.Messages)
		require.Equal(t, "expired", page.Capture.State)
		renewed := enableStatus(t, st, *now, binding)
		require.NotEqual(t, first.Generation, renewed.Generation)
		require.NotEmpty(t, renewed.Gaps)
		_, err = st.Exact(ctx, exactQuery(binding, renewed.Generation, "old"))
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.NoError(t, st.Capture(ctx, capturedMessage("new", *now), binding.ExpectedPhone, now.UnixMilli()))
		require.NoError(t, st.Restart(ctx))
		_, err = st.Exact(ctx, exactQuery(binding, renewed.Generation, "new"))
		require.NoError(t, err)
		*now = now.Add(time.Second)
		afterRestart := enableStatus(t, st, *now, binding)
		require.NoError(t, st.Capture(ctx, capturedMessage("new-owner-purge", *now), binding.ExpectedPhone, now.UnixMilli()))
		*now = now.Add(time.Second)
		binding.AuthorizationSubject = "new-owner"
		changed := enableStatus(t, st, *now, binding)
		require.NotEqual(t, afterRestart.Generation, changed.Generation)
		page, err = st.List(ctx, statusQuery(binding, *now))
		require.NoError(t, err)
		require.Empty(t, page.Messages)
	})
}

func TestStatusReopenedStorePreservesAuthorizedBufferAndPurgesExpiredLease(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		capture := enableStatus(t, st, *now, binding)
		require.NoError(t, st.Capture(ctx, capturedMessage("persisted", *now), binding.ExpectedPhone, now.UnixMilli()))
		dsn := os.Getenv("GOWS_TEST_POSTGRES_DSN")
		if messages.db.DriverName() == "sqlite3" {
			var seq int
			var name, path string
			require.NoError(t, messages.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path))
			dsn = path + "?_foreign_keys=on&_busy_timeout=10000"
		}
		require.NoError(t, messages.db.Close())
		container, err := New(messages.db.DriverName(), dsn, waLog.Noop)
		require.NoError(t, err)
		defer container.Close()
		reopened := container.NewStatusHistoryStorage()
		reopened.now = func() time.Time { return *now }
		*now = now.Add(time.Second)
		require.NoError(t, reopened.Restart(ctx))
		got, err := reopened.Exact(ctx, exactQuery(binding, capture.Generation, "persisted"))
		require.NoError(t, err)
		require.Equal(t, capture.Generation, got.Capture.Generation)
		require.NotEmpty(t, got.Capture.Gaps)
		require.NotNil(t, got.Capture.Gaps[len(got.Capture.Gaps)-1].UntilMS)
		*now = now.Add(4 * time.Minute)
		require.NoError(t, reopened.Restart(ctx))
		_, err = reopened.Exact(ctx, exactQuery(binding, capture.Generation, "persisted"))
		require.ErrorIs(t, err, storage.ErrStatusUnauthorized)
		var count int
		require.NoError(t, container.db.Get(&count, "SELECT count(*) FROM gows_messages WHERE jid='status@broadcast'"))
		require.Zero(t, count)
		renewed := enableStatus(t, reopened, *now, binding)
		require.NotEqual(t, capture.Generation, renewed.Generation)
		_, err = reopened.Exact(ctx, exactQuery(binding, renewed.Generation, "persisted"))
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}

func TestStatusOwnLIDRequiresAuthoritativePhoneAlias(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		capture := enableStatus(t, st, *now, binding)
		msg := capturedMessage("own-lid", *now)
		msg.Info.IsFromMe = true
		msg.Info.Sender = types.NewJID("123456789", types.HiddenUserServer)
		require.ErrorIs(t, st.Capture(ctx, msg, binding.ExpectedPhone, now.UnixMilli()), storage.ErrStatusBinding)
		msg.Info.SenderAlt = types.NewJID(binding.ExpectedPhone, types.DefaultUserServer)
		require.NoError(t, st.Capture(ctx, msg, binding.ExpectedPhone, now.UnixMilli()))
		q := exactQuery(binding, capture.Generation, "own-lid")
		q.FromMe, q.Sender = true, msg.Info.Sender.String()
		got, err := st.Exact(ctx, q)
		require.NoError(t, err)
		require.Equal(t, "123456789@lid", got.Source.Sender)
	})
}
func TestStatusDisableRacesAndDelayedAdmissionCannotRecreate(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		enableStatus(t, st, *now, binding)
		msg := capturedMessage("racing", *now)
		receivedAt := now.UnixMilli()
		var wg sync.WaitGroup
		wg.Add(2)
		errs := make(chan error, 2)
		go func() { defer wg.Done(); errs <- st.Capture(ctx, msg, binding.ExpectedPhone, receivedAt) }()
		go func() {
			defer wg.Done()
			_, err := st.Configure(ctx, storage.StatusPolicy{StatusBinding: binding, ObservedAtMS: receivedAt + 1, Enabled: false})
			errs <- err
		}()
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		var count int
		require.NoError(t, messages.db.Get(&count, "SELECT count(*) FROM gows_messages WHERE jid='status@broadcast'"))
		require.Zero(t, count)
		*now = now.Add(2 * time.Second)
		enableStatus(t, st, *now, binding)
		msg.Info.Timestamp = *now
		require.NoError(t, st.Capture(ctx, msg, binding.ExpectedPhone, receivedAt))
		page, err := st.List(ctx, statusQuery(binding, *now))
		require.NoError(t, err)
		require.Empty(t, page.Messages)
	})
}
func TestStatusMalformedAccountingAndExactProvenance(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		capture := enableStatus(t, st, *now, binding)
		for _, id := range []string{"a", "b", "c"} {
			require.NoError(t, st.Capture(ctx, capturedMessage(id, *now), binding.ExpectedPhone, now.UnixMilli()))
		}
		_, err := messages.db.Exec("UPDATE gows_messages SET data='{' WHERE id='c'")
		require.NoError(t, err)
		page, err := st.List(ctx, statusQuery(binding, *now))
		require.NoError(t, err)
		require.Len(t, page.Messages, 1)
		require.Equal(t, uint32(2), page.SourcePage.ScannedCount)
		require.Equal(t, uint32(1), page.SourcePage.UnavailableCount)
		require.Equal(t, "b", page.SourcePage.NextCursor.ID)
		q := exactQuery(binding, capture.Generation, "a")
		q.Sender = "15559999999@s.whatsapp.net"
		_, err = st.Exact(ctx, q)
		require.ErrorIs(t, err, storage.ErrNotFound)
		q = exactQuery(binding, capture.Generation, "a")
		q.FromMe = true
		_, err = st.Exact(ctx, q)
		require.ErrorIs(t, err, storage.ErrNotFound)
		q = exactQuery(binding, "old-generation", "a")
		_, err = st.Exact(ctx, q)
		require.ErrorIs(t, err, storage.ErrStatusGeneration)
	})
}
