package gows

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

func maintenanceTestClient(t *testing.T) *GoWS {
	address := filepath.Join(t.TempDir(), "fixture.db") + "?_foreign_keys=on&_busy_timeout=10000"
	g, err := BuildSession(context.Background(), waLog.Noop, "sqlite3", address, nil, DefaultStorageConfig())
	require.NoError(t, err)
	t.Cleanup(g.Stop)
	// These fixtures represent a tracker already serving before the tested window.
	conn, err := sql.Open("sqlite3", address)
	require.NoError(t, err)
	_, err = conn.Exec(`UPDATE gows_maintenance_state SET data=json_set(data,'$.since',?) WHERE id=1`, time.Now().Unix()-10)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	return g
}
func TestMaintenanceHistoryDownloadStoreAndAckFailures(t *testing.T) {
	for _, stage := range []string{"download", "auxiliary", "store", "ack", "success"} {
		t.Run(stage, func(t *testing.T) {
			g := maintenanceTestClient(t)
			ctx := context.Background()
			h := g.Maintenance
			g.handleMaintenanceEvent(&events.Connected{})
			notif := &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{IsFromMe: true}, ID: "history-1"}, Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{HistorySyncNotification: &waE2E.HistorySyncNotification{FileLength: proto.Uint64(1)}}}}
			require.True(t, g.handleMaintenanceEvent(notif))
			g.handleMaintenanceEvent(&events.OfflineSyncCompleted{})
			jobs, err := h.HistoryJobs(ctx)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			acknowledgments := 0
			stored := false
			failure := errors.New("fixture failure")
			w := &maintenanceWorker{
				download: func(context.Context, *waE2E.HistorySyncNotification, bool) (*waHistorySync.HistorySync, error) {
					if stage == "download" {
						return nil, failure
					}
					return &waHistorySync.HistorySync{}, nil
				},
				auxiliary: func(context.Context, *waHistorySync.HistorySync) error {
					if stage == "auxiliary" {
						return failure
					}
					return nil
				},
				persist: func(context.Context, *events.HistorySync) error {
					if stage == "store" {
						return failure
					}
					stored = true
					return nil
				},
				acknowledge: func(context.Context, types.MessageID, types.ReceiptType) error {
					require.True(t, stored)
					acknowledgments++
					if stage == "ack" {
						return failure
					}
					return nil
				},
			}
			err = g.processHistoryJob(ctx, w, h, jobs[0])
			if stage == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			now := time.Now().Unix()
			page, err := h.Page(ctx, storage.MaintenanceQuery{From: now, To: now, Limit: 5})
			require.NoError(t, err)
			require.Equal(t, stage == "success", page.Coverage.Complete)
			if stage == "download" || stage == "auxiliary" || stage == "store" {
				require.Zero(t, acknowledgments)
			}
			if stage == "ack" {
				jobs, err = h.HistoryJobs(ctx)
				require.NoError(t, err)
				require.True(t, jobs[0].Stored)
				w.download = func(context.Context, *waE2E.HistorySyncNotification, bool) (*waHistorySync.HistorySync, error) {
					t.Fatal("ack retry downloaded again")
					return nil, nil
				}
				w.acknowledge = func(context.Context, types.MessageID, types.ReceiptType) error { return nil }
				require.NoError(t, g.processHistoryJob(ctx, w, h, jobs[0]))
			}
		})
	}
}

type maintenanceBlockingMessages struct {
	storage.MessageStorage
	entered, release chan struct{}
}

func (m *maintenanceBlockingMessages) UpsertOneMessage(msg *storage.StoredMessage) error {
	close(m.entered)
	<-m.release
	return m.MessageStorage.UpsertOneMessage(msg)
}
func TestMaintenanceOfflineCannotOvertakeAsyncMessageStore(t *testing.T) {
	g := maintenanceTestClient(t)
	ctx := context.Background()
	g.handleMaintenanceEvent(&events.Connected{})
	block := &maintenanceBlockingMessages{MessageStorage: g.Storage.Messages, entered: make(chan struct{}), release: make(chan struct{})}
	g.Storage.Messages = block
	text := "fixture"
	now := time.Now().Unix()
	message := &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: types.NewJID("123456789", types.DefaultUserServer)}, ID: "note", Timestamp: time.Unix(now, 0)}, Message: &waE2E.Message{Conversation: &text}}
	require.True(t, g.handleMaintenanceEvent(message))
	<-block.entered
	g.handleMaintenanceEvent(&events.OfflineSyncCompleted{})
	page, err := g.Maintenance.Page(ctx, storage.MaintenanceQuery{From: now, To: now, Limit: 5})
	require.NoError(t, err)
	require.Contains(t, page.Coverage.Errors, "storage_pending")
	close(block.release)
	require.Eventually(t, func() bool {
		p, e := g.Maintenance.Page(ctx, storage.MaintenanceQuery{From: now, To: now, Limit: 5})
		return e == nil && p.Coverage.Complete && len(p.Items) == 1
	}, time.Second, 10*time.Millisecond)
	g.handleMaintenanceEvent(&events.Disconnected{})
	page, err = g.Maintenance.Page(ctx, storage.MaintenanceQuery{From: now, To: now, Limit: 5})
	require.NoError(t, err)
	require.False(t, page.Coverage.Complete)
}

func TestMaintenanceUnknownContentCannotAssertQuietChat(t *testing.T) {
	g := maintenanceTestClient(t)
	g.handleMaintenanceEvent(&events.Connected{})
	g.handleMaintenanceEvent(&events.OfflineSyncCompleted{})
	now := time.Now().Unix()
	unknown := &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: types.NewJID("fixture", types.GroupServer)}, ID: "future", Timestamp: time.Unix(now, 0)}, Message: &waE2E.Message{EventMessage: &waE2E.EventMessage{}}}
	require.NoError(t, g.storageEventHandler.observeMaintenanceMessage(unknown))
	page, err := g.Maintenance.Page(context.Background(), storage.MaintenanceQuery{From: now, To: now, Limit: 5})
	require.NoError(t, err)
	require.Contains(t, page.Coverage.Errors, "activity_kind_unknown")
	require.True(t, maintenanceNonordinary(&events.Message{Message: &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{}}}))
}

func TestMaintenanceDedicatedRestoreMarkerSurvivesOldStatusConsumer(t *testing.T) {
	directory := t.TempDir()
	address := filepath.Join(directory, "gows.db") + "?_foreign_keys=on&_busy_timeout=10000"
	g, err := BuildSession(context.Background(), waLog.Noop, "sqlite3", address, nil, DefaultStorageConfig())
	require.NoError(t, err)
	g.Stop()
	marker := filepath.Join(directory, ".min-maintenance-history-restored")
	require.NoError(t, os.WriteFile(marker, []byte("1\n"), 0600))
	// No Status marker remains: an old image already consumed it.
	g, err = BuildSession(context.Background(), waLog.Noop, "sqlite3", address, nil, DefaultStorageConfig())
	require.NoError(t, err)
	defer g.Stop()
	_, err = os.Stat(marker)
	require.True(t, errors.Is(err, os.ErrNotExist))
	now := time.Now().Unix()
	page, err := g.Maintenance.Page(context.Background(), storage.MaintenanceQuery{From: now, To: now, Limit: 5})
	require.NoError(t, err)
	require.Contains(t, page.Coverage.Errors, "snapshot_restored")
}

func TestMaintenanceHistoryWritesRegisterEditsAndRevokesExactly(t *testing.T) {
	g := maintenanceTestClient(t)
	ctx := context.Background()
	g.handleMaintenanceEvent(&events.Connected{})
	g.handleMaintenanceEvent(&events.OfflineSyncCompleted{})
	now := time.Now().Unix()
	chat := types.NewJID("fixture", types.GroupServer)
	original := &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: types.NewJID("123456789", types.DefaultUserServer), IsFromMe: true, IsGroup: true}, ID: "original", Timestamp: time.Unix(now, 0)}, Message: &waE2E.Message{Conversation: proto.String("before")}}
	status := storage.StatusServerAck
	require.NoError(t, g.storageEventHandler.persistHistoryMessage(ctx, original, &status))
	edit := &events.Message{Info: original.Info, Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(), Key: &waCommon.MessageKey{ID: proto.String("original"), RemoteJID: proto.String(chat.String()), FromMe: proto.Bool(true)}, TimestampMS: proto.Int64(time.Now().UnixMilli()), EditedMessage: &waE2E.Message{Conversation: proto.String("after")}}}}
	edit.Info.ID = "edit-frame"
	edit.RawMessage = edit.Message
	edit.Message = &waE2E.Message{Conversation: proto.String("after")}
	edit.IsEdit = true
	require.NoError(t, g.storageEventHandler.persistHistoryMessage(ctx, edit, &status))
	revoke := &events.Message{Info: original.Info, Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum(), Key: &waCommon.MessageKey{ID: proto.String("original")}}}}
	revoke.Info.ID = "revoke-frame"
	require.NoError(t, g.storageEventHandler.persistHistoryMessage(ctx, revoke, &status))
	page, err := g.Maintenance.Page(ctx, storage.MaintenanceQuery{From: now, To: now, Limit: 10})
	require.NoError(t, err)
	require.True(t, page.Coverage.Complete, page.Coverage.Errors)
	require.Len(t, page.Items, 1)
	require.True(t, page.Items[0].Deleted)
}
