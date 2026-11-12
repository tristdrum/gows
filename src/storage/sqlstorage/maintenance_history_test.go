package sqlstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestMaintenanceRegistrationOfflineStorageAndTokens(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Since = time.Now().Unix() - 10; return nil }))
	require.NoError(t, err)
	now := time.Now().Unix()
	m := integrityMessage(&waE2E.Message{Conversation: proto.String("fixture")}, storage.StatusDeliveryAck)
	m.Info.Timestamp = time.Unix(now, 0).UTC()
	m.Info.ID = "a"
	require.NoError(t, h.Connect(ctx))
	writes, err := h.BeginMessage(ctx, m.Message)
	require.NoError(t, err)
	require.NoError(t, h.OfflineComplete(ctx))
	q := storage.MaintenanceQuery{From: now, To: now, Limit: 1}
	page, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.False(t, page.Coverage.Complete)
	require.Contains(t, page.Coverage.Errors, "storage_pending")
	require.NoError(t, c.NewMessageStorage().UpsertOneMessage(m))
	require.NoError(t, h.Observe(ctx, m, m.Info.Chat.String()))
	require.NoError(t, h.FinishMessage(ctx, writes, nil))
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, page.Coverage.Complete)
	require.Len(t, page.Items, 1)
	token := page.Coverage.Token
	require.NoError(t, h.Begin(ctx))
	require.NoError(t, h.Observe(ctx, m, m.Info.Chat.String()))
	require.NoError(t, h.Finish(ctx, nil))
	q.Token = token
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.False(t, page.Coverage.Complete)
	require.Contains(t, page.Coverage.Errors, "revision_changed")
}

func TestMaintenanceStableTiesLateInsertTombstonesAndMapping(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Since = time.Now().Unix() - 10; return nil }))
	require.NoError(t, err)
	now := time.Now().Unix()
	require.NoError(t, h.Connect(ctx))
	require.NoError(t, h.OfflineComplete(ctx))
	ms := c.NewMessageStorage()
	for _, id := range []string{"c", "a", "b"} {
		m := integrityMessage(&waE2E.Message{Conversation: proto.String("private fixture")}, storage.StatusDeliveryAck)
		m.Info.Timestamp = time.Unix(now, 0).UTC()
		m.Info.ID = id
		m.Info.Chat.User = "fixture"
		require.NoError(t, ms.UpsertOneMessage(m))
		require.NoError(t, h.Observe(ctx, m, m.Info.Chat.String()))
	}
	q := storage.MaintenanceQuery{From: now, To: now, Limit: 1}
	page, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.Equal(t, "a", page.Items[0].ID)
	q.Cursor = page.NextCursor
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.Equal(t, "b", page.Items[0].ID)
	oldCursor := page.NextCursor
	require.NoError(t, ms.DeleteMessage("c"))
	// The metadata survives ordinary old-reader deletion and contains no content.
	q.Cursor = ""
	q.Limit = 100
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, page.Items[2].Deleted)
	require.Nil(t, page.Items[2].Raw)
	m := integrityMessage(&waE2E.Message{Conversation: proto.String("late")}, storage.StatusDeliveryAck)
	m.Info.Timestamp = time.Unix(now, 0).UTC()
	m.Info.ID = "late"
	require.NoError(t, h.Observe(ctx, m, ""))
	q.Cursor = oldCursor
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.Contains(t, page.Coverage.Errors, "revision_changed")
	q.Cursor = ""
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.Contains(t, page.Coverage.Errors, "identity_unknown")
	var activityRaw string
	require.NoError(t, c.db.Get(&activityRaw, "SELECT data FROM gows_maintenance_state WHERE id=1"))
	require.NotContains(t, activityRaw, "private fixture")
}

func TestMaintenanceDurableJobsRestartAndOldReader(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Since = time.Now().Unix() - 10; return nil }))
	require.NoError(t, err)
	require.NoError(t, h.RegisterHistory(ctx, "notification-1", []byte("fixture")))
	require.NoError(t, h.RegisterHistory(ctx, "notification-1", []byte("fixture")))
	jobs, err := h.HistoryJobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.NoError(t, h.HistoryStored(ctx, "notification-1"))
	h2, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	jobs, err = h2.HistoryJobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.True(t, jobs[0].Stored)
	// Side tables do not advance the legacy migration version.
	require.NoError(t, c.Migrate())
	require.NoError(t, h2.HistoryAcked(ctx, "notification-1"))
	jobs, err = h2.HistoryJobs(ctx)
	require.NoError(t, err)
	require.Empty(t, jobs)
	require.NoError(t, h2.RegisterHistory(ctx, "notification-1", []byte("fixture")))
	jobs, err = h2.HistoryJobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.True(t, jobs[0].Stored)
	var state map[string]any
	var raw string
	require.NoError(t, c.db.Get(&raw, "SELECT data FROM gows_maintenance_state WHERE id=1"))
	require.NoError(t, json.Unmarshal([]byte(raw), &state))
}

func TestMaintenanceRevokeCannotBeUndoneByHistoryReplay(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Since = time.Now().Unix() - 10; return nil }))
	require.NoError(t, err)
	now := time.Now().Unix()
	m := integrityMessage(&waE2E.Message{Conversation: proto.String("revoked fixture")}, storage.StatusDeliveryAck)
	m.Info.Timestamp = time.Unix(now, 0).UTC()
	ms := c.NewMessageStorage()
	require.NoError(t, ms.UpsertOneMessage(m))
	require.NoError(t, h.Observe(ctx, m, m.Info.Chat.String()))
	require.NoError(t, h.Revoke(ctx, m.Info.ID))
	require.NoError(t, ms.DeleteMessage(m.Info.ID))
	require.NoError(t, ms.UpsertOneMessage(m))
	require.NoError(t, h.Observe(ctx, m, m.Info.Chat.String()))
	page, err := h.Page(ctx, storage.MaintenanceQuery{From: now, To: now, Limit: 5})
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.True(t, page.Items[0].Deleted)
	require.Nil(t, page.Items[0].Raw)
}

func TestMaintenanceSnapshotRestoreInvalidatesWindowAndPendingStorageRestart(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Since = time.Now().Unix() - 10; return nil }))
	require.NoError(t, err)
	now := time.Now().Unix()
	require.NoError(t, h.Connect(ctx))
	require.NoError(t, h.OfflineComplete(ctx))
	q := storage.MaintenanceQuery{From: now, To: now, Limit: 5}
	p, err := h.Page(ctx, q)
	require.NoError(t, err)
	q.Token = p.Coverage.Token
	require.NoError(t, h.InvalidateRestore(ctx))
	p, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.False(t, p.Coverage.Complete)
	require.Contains(t, p.Coverage.Errors, "snapshot_restored")
	require.Contains(t, p.Coverage.Errors, "revision_changed")
	require.NoError(t, h.Connect(ctx))
	require.NoError(t, h.OfflineComplete(ctx))
	q.Token = ""
	p, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, p.Coverage.EnumerationComplete)
	require.False(t, p.Coverage.Complete)
	require.NoError(t, h.Begin(ctx))
	h, err = c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	p, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.Contains(t, p.Coverage.Errors, "storage_interrupted")
}

func TestMaintenanceLegacyWritesRemainUsableAndInvalidateCoverage(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	m := integrityMessage(&waE2E.Message{Conversation: proto.String("legacy fixture")}, storage.StatusDeliveryAck)
	m.Info.Timestamp = time.Now().UTC()
	raw, err := messageMapper.Marshal(m)
	require.NoError(t, err)
	// Execute the previous image's ordinary SQL directly: no new callbacks or UDFs.
	_, err = c.db.Exec(`INSERT INTO gows_messages(id,jid,from_me,timestamp,is_real,data) VALUES($1,$2,$3,$4,$5,$6)`, m.Info.ID, m.Info.Chat, false, m.Info.Timestamp, true, string(raw))
	require.NoError(t, err)
	_, err = c.db.Exec(`UPDATE gows_messages SET data=$1 WHERE id=$2`, string(raw), m.Info.ID)
	require.NoError(t, err)
	_, err = c.db.Exec(`DELETE FROM gows_messages WHERE id=$1`, m.Info.ID)
	require.NoError(t, err)
	now := time.Now().Unix()
	page, err := h.Page(ctx, storage.MaintenanceQuery{From: now, To: now, Limit: 5})
	require.NoError(t, err)
	require.Contains(t, page.Coverage.Errors, "untracked_mutation")
	h, err = c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	page, err = h.Page(ctx, storage.MaintenanceQuery{From: now, To: now, Limit: 5})
	require.NoError(t, err)
	require.Contains(t, page.Coverage.Errors, "untracked_mutation")
	require.NoError(t, c.Migrate())
}

func TestMaintenanceWindowTokenIgnoresUnrelatedFreshActivityAndReceipts(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Since = time.Now().Unix() - 20; return nil }))
	require.NoError(t, h.Connect(ctx))
	require.NoError(t, h.OfflineComplete(ctx))
	now := time.Now().Unix()
	q := storage.MaintenanceQuery{From: now - 10, To: now - 5, Limit: 5}
	page, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, page.Coverage.Complete)
	q.Token = page.Coverage.Token
	m := integrityMessage(&waE2E.Message{Conversation: proto.String("fresh fixture")}, storage.StatusDeliveryAck)
	m.Info.Timestamp = time.Unix(now, 0).UTC()
	writes, err := h.BeginMessage(ctx, m.Message)
	require.NoError(t, err)
	require.NoError(t, c.NewMessageStorage().UpsertOneMessage(m))
	require.NoError(t, h.Observe(ctx, m, m.Info.Chat.String()))
	require.NoError(t, h.FinishMessage(ctx, writes, nil))
	require.NoError(t, c.NewMessageStorage().UpdateMessageStatus(m.Info.ID, storage.StatusRead))
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, page.Coverage.Complete)
	require.Equal(t, q.Token, page.Coverage.Token)
}

func TestMaintenanceCleanRestartPreservesEstablishedCoverageFloor(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	now := time.Now().Unix()
	require.NoError(t, h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Since = now - 300; return nil }))
	require.NoError(t, h.Connect(ctx))
	require.NoError(t, h.OfflineComplete(ctx))
	q := storage.MaintenanceQuery{From: now - 200, To: now - 10, Limit: 10}
	before, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, before.Coverage.Complete)
	// Re-run the real unchanged migration/bootstrap path before the new tracker.
	require.NoError(t, c.Migrate())
	h, err = c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.Connect(ctx))
	require.NoError(t, h.OfflineComplete(ctx))
	after, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, after.Coverage.Complete)
	require.True(t, after.Coverage.EnumerationComplete)
	require.NotEqual(t, before.Coverage.Epoch, after.Coverage.Epoch)
}

func TestMaintenanceHistoryExhaustionIsExplicitAndBounded(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.RegisterHistory(ctx, "exhausted", []byte("notification")))
	for i := 0; i < 5; i++ {
		require.NoError(t, h.HistoryAttempt(ctx, "exhausted"))
		require.NoError(t, h.HistoryFailed(ctx, "exhausted", "history_download_failed"))
	}
	jobs, err := h.HistoryJobs(ctx)
	require.NoError(t, err)
	require.Empty(t, jobs)
	now := time.Now().Unix()
	page, err := h.Page(ctx, storage.MaintenanceQuery{From: now, To: now, Limit: 5})
	require.NoError(t, err)
	require.Contains(t, page.Coverage.Errors, "history_retries_exhausted")
	require.False(t, page.Coverage.EnumerationComplete)
}

func TestMaintenancePendingHistoryDoesNotAuthorizeLegacyWrites(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Since = time.Now().Unix() - 10; return nil }))
	require.NoError(t, h.RegisterHistory(ctx, "unfinished", []byte("notification")))
	m := integrityMessage(&waE2E.Message{Conversation: proto.String("legacy unseen")}, storage.StatusDeliveryAck)
	m.Info.Timestamp = time.Now().UTC()
	require.NoError(t, c.NewMessageStorage().UpsertOneMessage(m))
	require.NoError(t, c.NewMessageStorage().DeleteMessage(m.Info.ID))
	var untracked bool
	require.NoError(t, c.db.Get(&untracked, `SELECT untracked FROM gows_maintenance_state WHERE id=1`))
	require.True(t, untracked, "an unacked history job cannot authorize unrelated legacy SQL")
	h, err = c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.Connect(ctx))
	require.NoError(t, h.OfflineComplete(ctx))
	require.NoError(t, h.HistoryStored(ctx, "unfinished"))
	require.NoError(t, h.HistoryAcked(ctx, "unfinished"))
	now := time.Now().Unix()
	page, err := h.Page(ctx, storage.MaintenanceQuery{From: now - 5, To: now, Limit: 10})
	require.NoError(t, err)
	require.False(t, page.Coverage.Complete)
	require.Contains(t, page.Coverage.Errors, "untracked_mutation")
}

func TestMaintenanceSeedCannotExposeExpiredStatusOrOtherNonordinaryChats(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	messages := c.NewMessageStorage()
	statusStore, now, binding := statusFixture(t, messages)
	*now = now.Add(-5 * time.Minute)
	enableStatus(t, statusStore, *now, binding)
	require.NoError(t, statusStore.Capture(ctx, capturedMessage("private-status", *now), binding.ExpectedPhone, now.UnixMilli()))
	for _, jid := range []string{"news@newsletter", "list@broadcast", "ordinary@g.us", "123@s.whatsapp.net", "456@lid"} {
		msg := integrityMessage(&waE2E.Message{Conversation: proto.String("fixture")}, storage.StatusDeliveryAck)
		msg.Info.ID = jid
		msg.Info.Chat, _ = types.ParseJID(jid)
		msg.Info.Timestamp = *now
		require.NoError(t, messages.UpsertOneMessage(msg))
	}
	*now = now.Add(5 * time.Minute) // The grant is expired, but content has not been read/pruned.
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	q := storage.MaintenanceQuery{From: now.Add(-10 * time.Minute).Unix(), To: now.Unix(), Limit: 20}
	page, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.Len(t, page.Items, 3)
	for _, item := range page.Items {
		require.True(t, maintenanceChat(item.ProviderJID))
		require.NotEqual(t, "private-status", item.ID)
	}
	var count int
	require.NoError(t, c.db.Get(&count, `SELECT COUNT(*) FROM gows_maintenance_activity WHERE provider_jid='status@broadcast'`))
	require.Zero(t, count)
	for _, jid := range []string{"status@broadcast", "news@newsletter", "list@broadcast"} {
		unsupported := capturedMessage("observe-"+jid, *now)
		unsupported.Info.Chat, _ = types.ParseJID(jid)
		require.NoError(t, h.Observe(ctx, unsupported, jid))
		// Simulate stale metadata from an earlier seed: the read boundary still excludes it.
		_, err = c.db.Exec(`INSERT INTO gows_maintenance_activity(id,provider_jid,jid,timestamp,from_me,identity_known) VALUES($1,$2,$2,$3,FALSE,TRUE)`, "stale-"+jid, jid, now.Unix())
		require.NoError(t, err)
		q.ChatID = jid
		_, err = h.Page(ctx, q)
		require.Error(t, err)
	}
	q.ChatID = ""
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.Len(t, page.Items, 3)
	h, err = c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, c.db.Get(&count, `SELECT COUNT(*) FROM gows_maintenance_activity WHERE provider_jid LIKE '%@broadcast' OR provider_jid LIKE '%@newsletter'`))
	require.Zero(t, count)
}

func TestMaintenanceExactRegistrationDoesNotHideConcurrentRepair(t *testing.T) {
	for _, sameID := range []bool{false, true} {
		t.Run(fmt.Sprintf("same_id_%t", sameID), func(t *testing.T) {
			c := newMaintenanceTestContainer(t)
			ctx := context.Background()
			h, err := c.NewMaintenanceHistoryStorage(ctx)
			require.NoError(t, err)
			own := integrityMessage(&waE2E.Message{Conversation: proto.String("registered")}, storage.StatusDeliveryAck)
			own.Info.Timestamp = time.Now().UTC()
			writes, err := h.BeginMessage(ctx, own.Message)
			require.NoError(t, err)
			require.NoError(t, c.NewMessageStorage().UpsertOneMessage(own))
			var unknown bool
			require.NoError(t, c.db.Get(&unknown, `SELECT untracked FROM gows_maintenance_state WHERE id=1`))
			require.False(t, unknown)
			if sameID {
				_, err = c.db.Exec(`UPDATE gows_messages SET timestamp=$1 WHERE id=$2`, own.Info.Timestamp.Add(-time.Minute), own.Info.ID)
				require.NoError(t, err)
			} else {
				other := integrityMessage(&waE2E.Message{Conversation: proto.String("repair")}, storage.StatusDeliveryAck)
				require.NoError(t, c.NewMessageStorage().UpsertOneMessage(other))
				require.NoError(t, c.NewMessageStorage().DeleteMessage(other.Info.ID))
			}
			require.NoError(t, c.db.Get(&unknown, `SELECT untracked FROM gows_maintenance_state WHERE id=1`))
			require.True(t, unknown)
			require.NoError(t, h.FinishMessage(ctx, writes, nil))
			require.NoError(t, c.Migrate())
		})
	}
}

func TestMaintenanceStaleRegistrationCannotAuthorizeLaterBoot(t *testing.T) {
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	msg := integrityMessage(&waE2E.Message{Conversation: proto.String("unfinished")}, storage.StatusDeliveryAck)
	msg.Info.Timestamp = time.Now().UTC()
	_, err = h.BeginMessage(ctx, msg.Message)
	require.NoError(t, err)
	h, err = c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	var remaining int
	require.NoError(t, c.db.Get(&remaining, `SELECT COUNT(*) FROM gows_maintenance_writes`))
	require.Zero(t, remaining)
	require.NoError(t, c.NewMessageStorage().UpsertOneMessage(msg))
	var unknown bool
	require.NoError(t, c.db.Get(&unknown, `SELECT untracked FROM gows_maintenance_state WHERE id=1`))
	require.True(t, unknown)
}
