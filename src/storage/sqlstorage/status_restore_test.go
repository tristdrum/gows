package sqlstorage

import (
	"context"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
)

func TestStatusRestoreInvalidatesAuthorizationAndOldContentOnly(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		capture := enableStatus(t, st, *now, binding)
		require.NoError(t, st.Capture(ctx, capturedMessage("snapshot-status", *now), binding.ExpectedPhone, now.UnixMilli()))
		dm := capturedMessage("ordinary", *now)
		dm.Info.Chat = dm.Info.Sender
		require.NoError(t, messages.UpsertOneMessage(dm))
		*now = now.Add(time.Second)
		if messages.db.DriverName() == "sqlite3" {
			messages.db.SetMaxOpenConns(1)
			var synchronous int
			require.NoError(t, messages.db.Get(&synchronous, "PRAGMA synchronous"))
			require.Equal(t, 1, synchronous)
			// This trigger executes in the actual purge transaction. It proves
			// that transaction uses FULL, rather than a different pooled handle.
			_, err := messages.db.Exec(`CREATE TRIGGER status_restore_full_sync
			 BEFORE DELETE ON gows_messages WHEN OLD.jid='status@broadcast'
			 AND (SELECT synchronous FROM pragma_synchronous) <> 2
			 BEGIN SELECT RAISE(ABORT, 'restore requires full sync'); END`)
			require.NoError(t, err)
		}
		require.NoError(t, st.InvalidateRestore(ctx))
		if messages.db.DriverName() == "sqlite3" {
			var synchronous int
			require.NoError(t, messages.db.Get(&synchronous, "PRAGMA synchronous"))
			require.Equal(t, 1, synchronous, "original mode restored before returning connection to pool")
		}
		_, err := st.Exact(ctx, exactQuery(binding, capture.Generation, "snapshot-status"))
		require.ErrorIs(t, err, storage.ErrStatusUnauthorized)
		_, err = messages.GetMessage("ordinary")
		require.NoError(t, err)
		page, err := st.List(ctx, statusQuery(binding, *now))
		require.NoError(t, err)
		require.Empty(t, page.Messages)
		require.NotEqual(t, capture.Generation, page.Capture.Generation)
		require.Equal(t, "snapshot_restored", page.Capture.Gaps[len(page.Capture.Gaps)-1].Reason)
		_, err = st.Configure(ctx, storage.StatusPolicy{StatusBinding: binding, ObservedAtMS: now.Add(-time.Second).UnixMilli(), Enabled: true, AuthorizedUntilMS: now.Add(time.Minute).UnixMilli()})
		require.ErrorIs(t, err, storage.ErrStatusStaleControl)
		*now = now.Add(time.Millisecond)
		renewed := enableStatus(t, st, *now, binding)
		_, err = st.Exact(ctx, exactQuery(binding, renewed.Generation, "snapshot-status"))
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}

func TestStatusCaptureExcludesAmbiguousPartialFirstSecond(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		st, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		priorSecond := *now
		*now = now.Add(200 * time.Millisecond)
		capture := enableStatus(t, st, *now, binding)
		require.Equal(t, "timestamp_resolution", capture.Gaps[len(capture.Gaps)-1].Reason)
		require.Equal(t, now.UnixMilli(), capture.Gaps[len(capture.Gaps)-1].SinceMS)
		require.Equal(t, priorSecond.Add(time.Second).UnixMilli(), *capture.Gaps[len(capture.Gaps)-1].UntilMS)
		require.NoError(t, st.Capture(ctx, capturedMessage("ambiguous-old", priorSecond), binding.ExpectedPhone, now.UnixMilli()))
		*now = priorSecond.Add(time.Second)
		require.NoError(t, st.Capture(ctx, capturedMessage("certain-new", *now), binding.ExpectedPhone, now.UnixMilli()))
		page, err := st.List(ctx, statusQuery(binding, *now))
		require.NoError(t, err)
		require.Len(t, page.Messages, 1)
		require.Equal(t, "certain-new", page.Messages[0].Source.ID)
	})
}
