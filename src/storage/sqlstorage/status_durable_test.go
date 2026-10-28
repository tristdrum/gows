package sqlstorage

import (
	"context"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestStatusPrivacyChangesUseFullOnActualSQLiteConnection(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		if messages.db.DriverName() != "sqlite3" {
			t.Skip("SQLite connection-specific durability proof")
		}
		st, now, binding := statusFixture(t, messages)
		messages.db.SetMaxOpenConns(1)
		ctx := context.Background()
		assertNormal := func() {
			var synchronous int
			require.NoError(t, messages.db.Get(&synchronous, "PRAGMA synchronous"))
			require.Equal(t, 1, synchronous)
		}
		assertNormal()
		for _, trigger := range []string{
			`CREATE TRIGGER status_policy_full BEFORE UPDATE ON gows_status_capture
			 WHEN NEW.data<>OLD.data AND (SELECT synchronous FROM pragma_synchronous)<>2
			 BEGIN SELECT RAISE(ABORT,'policy requires full'); END`,
			`CREATE TRIGGER status_revoke_full BEFORE INSERT ON gows_status_tombstones
			 WHEN (SELECT synchronous FROM pragma_synchronous)<>2
			 BEGIN SELECT RAISE(ABORT,'revoke requires full'); END`,
			`CREATE TRIGGER status_purge_full BEFORE DELETE ON gows_messages
			 WHEN OLD.jid='status@broadcast' AND (SELECT synchronous FROM pragma_synchronous)<>2
			 BEGIN SELECT RAISE(ABORT,'purge requires full'); END`,
			`CREATE TRIGGER status_capture_normal BEFORE INSERT ON gows_messages
			 WHEN NEW.jid='status@broadcast' AND (SELECT synchronous FROM pragma_synchronous)<>1
			 BEGIN SELECT RAISE(ABORT,'capture retains original mode'); END`,
		} {
			_, err := messages.db.Exec(trigger)
			require.NoError(t, err)
		}
		enableStatus(t, st, *now, binding)
		assertNormal()
		require.NoError(t, st.Capture(ctx, capturedMessage("revoke-target", *now), binding.ExpectedPhone, now.UnixMilli()))
		assertNormal()
		revoke := capturedMessage("revoke", *now)
		revoke.Message.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum(), Key: &waCommon.MessageKey{ID: proto.String("revoke-target")}}}
		require.NoError(t, st.Capture(ctx, revoke, binding.ExpectedPhone, now.UnixMilli()))
		assertNormal()
		require.NoError(t, st.Capture(ctx, capturedMessage("disabled-target", *now), binding.ExpectedPhone, now.UnixMilli()))
		_, err := st.Configure(ctx, storage.StatusPolicy{StatusBinding: binding, ObservedAtMS: now.Add(time.Millisecond).UnixMilli()})
		require.NoError(t, err)
		assertNormal()
		*now = now.Add(time.Second)
		enableStatus(t, st, *now, binding)
		require.NoError(t, st.Capture(ctx, capturedMessage("restore-target", *now), binding.ExpectedPhone, now.UnixMilli()))
		require.NoError(t, st.InvalidateRestore(ctx))
		assertNormal()
		*now = now.Add(time.Second)
		enableStatus(t, st, *now, binding)
		require.NoError(t, st.Capture(ctx, capturedMessage("failed-restore-target", *now), binding.ExpectedPhone, now.UnixMilli()))
		_, err = messages.db.Exec(`CREATE TRIGGER fail_durable_restore BEFORE DELETE ON gows_messages
		 WHEN OLD.jid='status@broadcast' BEGIN SELECT RAISE(ABORT,'synthetic rollback'); END`)
		require.NoError(t, err)
		require.Error(t, st.InvalidateRestore(ctx))
		assertNormal()
		page, err := st.List(ctx, statusQuery(binding, *now))
		require.NoError(t, err)
		require.Len(t, page.Messages, 1, "failed purge rolls back atomically")
	})
}
