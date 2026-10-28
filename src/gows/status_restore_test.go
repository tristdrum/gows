package gows

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/devlikeapro/gows/storage/sqlstorage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

func TestStatusSnapshotRestoreCannotResurrectRevokedContentWithinLease(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "gows.db")
	address := "file:" + path + "?_foreign_keys=on&_busy_timeout=10000"
	open := func() *sqlstorage.GContainer {
		container, err := sqlstorage.New("sqlite3", address, waLog.Noop)
		require.NoError(t, err)
		return container
	}
	container := open()
	st := container.NewStatusHistoryStorage()
	now := time.Now().UTC()
	binding := storage.StatusBinding{AccountID: "account", ExpectedPhone: "15550000001", ShardID: "shard-a", Epoch: 1, AuthorizationSubject: "owner-hash"}
	policy := storage.StatusPolicy{StatusBinding: binding, ObservedAtMS: time.Now().UnixMilli(), Enabled: true, AuthorizedUntilMS: time.Now().Add(3 * time.Minute).UnixMilli()}
	capture, err := st.Configure(ctx, policy)
	require.NoError(t, err)
	now = time.Now().UTC()
	msg := &storage.StoredMessage{Message: &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: types.StatusBroadcastJID, Sender: types.NewJID("15550000002", types.DefaultUserServer)}, ID: "revoked", Timestamp: now}, Message: &waE2E.Message{Conversation: proto.String("Status A")}}}
	require.NoError(t, st.Capture(ctx, msg, binding.ExpectedPhone, time.Now().UnixMilli()))
	dm := *msg
	dmEvent := *msg.Message
	dm.Message = &dmEvent
	dm.Info.Chat, dm.Info.ID = msg.Info.Sender, "ordinary"
	require.NoError(t, container.NewMessageStorage().UpsertOneMessage(&dm))
	require.NoError(t, container.Close())
	// This is the real checkpointed SQLite file, not a reconstructed record.
	snapshot, err := os.ReadFile(path)
	require.NoError(t, err)
	// The same session startup without a restore marker preserves valid history.
	ordinaryRestart, err := BuildSession(ctx, waLog.Noop, "sqlite3", address, &IgnoreJidsConfig{Status: true}, DefaultStorageConfig())
	require.NoError(t, err)
	exact := storage.StatusExact{StatusBinding: binding, Generation: capture.Generation, ID: "revoked", Sender: msg.Info.Sender.String()}
	_, err = ordinaryRestart.Storage.StatusHistory.Exact(ctx, exact)
	require.NoError(t, err)
	ordinaryRestart.Stop()
	container = open()
	st = container.NewStatusHistoryStorage()
	revokeEvent := *msg.Message
	revokeEvent.Info.ID = "revoke"
	revokeEvent.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum(), Key: &waCommon.MessageKey{ID: proto.String("revoked")}}}
	require.NoError(t, st.Capture(ctx, &storage.StoredMessage{Message: &revokeEvent}, binding.ExpectedPhone, time.Now().UnixMilli()))
	_, err = st.Exact(ctx, exact)
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.NoError(t, container.Close())
	require.NoError(t, os.WriteFile(path, snapshot, 0600))
	marker := filepath.Join(filepath.Dir(path), statusHistoryRestoreMarker)
	require.NoError(t, os.WriteFile(marker, []byte("1\n"), 0600))
	// A real purge failure must leave the external marker intact and refuse
	// startup, even though the snapshot's old authorization is still valid.
	db, err := sql.Open("sqlite3", address)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TRIGGER fail_restore_purge BEFORE DELETE ON gows_messages
	 WHEN OLD.jid='status@broadcast' BEGIN SELECT RAISE(ABORT, 'synthetic purge failure'); END`)
	require.NoError(t, err)
	failed, err := BuildSession(ctx, waLog.Noop, "sqlite3", address, &IgnoreJidsConfig{Status: true}, DefaultStorageConfig())
	require.Error(t, err)
	require.Nil(t, failed)
	_, err = os.Lstat(marker)
	require.NoError(t, err)
	_, err = db.Exec("DROP TRIGGER fail_restore_purge")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	// Actual session startup consumes the restore marker before registration or reads.
	client, err := BuildSession(ctx, waLog.Noop, "sqlite3", address, &IgnoreJidsConfig{Status: true}, DefaultStorageConfig())
	require.NoError(t, err)
	defer client.Stop()
	_, err = os.Lstat(marker)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = client.Storage.StatusHistory.Exact(ctx, exact)
	require.ErrorIs(t, err, storage.ErrStatusUnauthorized)
	page, err := client.Storage.StatusHistory.List(ctx, storage.StatusHistoryQuery{StatusBinding: binding, SinceMS: now.Add(-24 * time.Hour).UnixMilli(), UntilMS: time.Now().UnixMilli(), Limit: 10})
	require.NoError(t, err)
	require.Empty(t, page.Messages)
	require.NotEqual(t, capture.Generation, page.Capture.Generation)
	require.Equal(t, "snapshot_restored", page.Capture.Gaps[len(page.Capture.Gaps)-1].Reason)
	_, err = client.Storage.Messages.GetMessage("ordinary")
	require.NoError(t, err)
	_, err = client.Storage.StatusHistory.Configure(ctx, policy)
	require.ErrorIs(t, err, storage.ErrStatusStaleControl)
	policy.ObservedAtMS = time.Now().Add(time.Millisecond).UnixMilli()
	policy.AuthorizedUntilMS = policy.ObservedAtMS + 179000
	renewed, err := client.Storage.StatusHistory.Configure(ctx, policy)
	require.NoError(t, err)
	exact.Generation = renewed.Generation
	_, err = client.Storage.StatusHistory.Exact(ctx, exact)
	require.ErrorIs(t, err, storage.ErrNotFound)
}

type restoreHistorySpy struct {
	storage.StatusHistoryStorage
	err   error
	calls int
}

func (s *restoreHistorySpy) InvalidateRestore(context.Context) error { s.calls++; return s.err }

func TestStatusRestoreMarkerFailureKeepsMarkerAndBlocksStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gows.db")
	marker := filepath.Join(filepath.Dir(path), statusHistoryRestoreMarker)
	require.NoError(t, os.WriteFile(marker, []byte("1"), 0600))
	spy := &restoreHistorySpy{err: context.Canceled}
	require.Error(t, invalidateRestoredStatusHistory(context.Background(), spy, "sqlite3", "file:"+path))
	_, err := os.Lstat(marker)
	require.NoError(t, err)
	require.Equal(t, 1, spy.calls)
	require.NoError(t, os.Remove(marker))
	require.NoError(t, os.Symlink(path, marker))
	require.Error(t, invalidateRestoredStatusHistory(context.Background(), spy, "sqlite3", "file:"+path))
	require.Equal(t, 1, spy.calls)
	_, err = BuildSession(context.Background(), waLog.Noop, "sqlite3", "file:"+path+"?_foreign_keys=on", &IgnoreJidsConfig{Status: true}, DefaultStorageConfig())
	require.Error(t, err)
	_, err = os.Lstat(marker)
	require.NoError(t, err)
}

func TestStatusRestoreMarkerDerivesOnlyConfiguredLocalStore(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "session with spaces", "gows.db")
	for _, address := range []string{path, "file:" + path + "?_foreign_keys=on", "file:" + filepath.ToSlash(path)} {
		marker, err := statusRestoreMarker("sqlite3", address)
		require.NoError(t, err)
		require.Equal(t, filepath.Join(filepath.Dir(path), statusHistoryRestoreMarker), marker)
	}
	marker, err := statusRestoreMarker("postgres", "postgres://fixture/database")
	require.NoError(t, err)
	require.Empty(t, marker)
	marker, err = statusRestoreMarker("sqlite3", filepath.Join(root, "other.db"))
	require.NoError(t, err)
	require.Empty(t, marker)
	_, err = statusRestoreMarker("sqlite3", "file://unexpected-host/session/gows.db")
	require.Error(t, err)
}
