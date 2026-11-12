package sqlstorage

import (
	"context"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

func maintenanceIdentityFixture(t *testing.T) (*GContainer, *SQLMaintenanceHistoryStore, *whatsmeow.Client, int64) {
	t.Helper()
	c := newMaintenanceTestContainer(t)
	ctx := context.Background()
	h, err := c.NewMaintenanceHistoryStorage(ctx)
	require.NoError(t, err)
	require.NoError(t, h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error { s.Since = time.Now().Unix() - 1000; return nil }))
	require.NoError(t, h.Connect(ctx))
	require.NoError(t, h.OfflineComplete(ctx))
	device, err := c.GetFirstDevice(ctx)
	require.NoError(t, err)
	device.LIDs = c.LIDMap // Real SQL-backed map, without saving a device or connecting.
	return c, h, whatsmeow.NewClient(device, waLog.Noop), time.Now().Unix()
}
func maintenanceIdentityMessage(t *testing.T, c *GContainer, h *SQLMaintenanceHistoryStore, id string, chat types.JID, canonical string, stamp int64) {
	t.Helper()
	ctx := context.Background()
	m := integrityMessage(&waE2E.Message{Conversation: proto.String("identity fixture")}, storage.StatusDeliveryAck)
	m.Info.ID = id
	m.Info.Chat = chat
	m.Info.Timestamp = time.Unix(stamp, 0).UTC()
	writes, err := h.BeginMessage(ctx, m.Message)
	require.NoError(t, err)
	require.NoError(t, c.NewMessageStorage().UpsertOneMessage(m))
	require.NoError(t, h.Observe(ctx, m, canonical))
	require.NoError(t, h.FinishMessage(ctx, writes, nil))
}
func TestMaintenanceCurrentProviderMappingInvalidatesSelectedChatsAndCursor(t *testing.T) {
	c, h, client, now := maintenanceIdentityFixture(t)
	ctx := context.Background()
	pnA := types.NewJID("15550000001", types.DefaultUserServer)
	pnB := types.NewJID("15550000002", types.DefaultUserServer)
	lid := types.NewJID("123456789", types.HiddenUserServer)
	client.StoreLIDPNMapping(ctx, lid, pnB)
	maintenanceIdentityMessage(t, c, h, "note", pnA, pnA.String(), now-10)
	maintenanceIdentityMessage(t, c, h, "later", lid, pnB.String(), now-5)
	// A newer native B row ensures removal cannot hide behind an unchanged MAX.
	maintenanceIdentityMessage(t, c, h, "native-b", pnB, pnB.String(), now-1)
	q := storage.MaintenanceQuery{From: now - 20, To: now, Limit: 10, ChatID: pnA.String()}
	beforeA, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, beforeA.Coverage.Complete)
	require.Len(t, beforeA.Items, 1)
	q.ChatID = pnB.String()
	beforeB, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.Len(t, beforeB.Items, 2)
	all := storage.MaintenanceQuery{From: q.From, To: q.To, Limit: 1}
	first, err := h.Page(ctx, all)
	require.NoError(t, err)
	require.NotEmpty(t, first.NextCursor)
	// This is the real pinned provider method, with no GOWS Contact/history hook.
	client.StoreLIDPNMapping(ctx, lid, pnA)
	var storedPN string
	require.NoError(t, c.db.Get(&storedPN, `SELECT pn FROM whatsmeow_lid_map WHERE lid=$1`, lid.User))
	require.Equal(t, pnA.User, storedPN)
	q.ChatID = pnA.String()
	q.Token = beforeA.Coverage.Token
	page, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.False(t, page.Coverage.Complete)
	require.Contains(t, page.Coverage.Errors, "revision_changed")
	require.Greater(t, page.Coverage.Revision, beforeA.Coverage.Revision)
	q.ChatID = pnB.String()
	q.Token = beforeB.Coverage.Token
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.Contains(t, page.Coverage.Errors, "revision_changed")
	all.Cursor = first.NextCursor
	page, err = h.Page(ctx, all)
	require.NoError(t, err)
	require.Contains(t, page.Coverage.Errors, "revision_changed")
	q.ChatID = pnA.String()
	q.Token = ""
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, page.Coverage.Complete)
	require.Len(t, page.Items, 2)
	require.Equal(t, "later", page.Items[1].ID)
	require.Equal(t, pnA.String(), page.Items[1].JID)
	require.Equal(t, lid.String(), page.Items[1].ProviderJID)
	q.Token = page.Coverage.Token
	stable, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, stable.Coverage.Complete)
	require.Equal(t, q.Token, stable.Coverage.Token)
}
func TestMaintenanceProviderMappingRemovalAndUnknownFilterHold(t *testing.T) {
	c, h, client, now := maintenanceIdentityFixture(t)
	ctx := context.Background()
	pn := types.NewJID("15550000001", types.DefaultUserServer)
	oldLID := types.NewJID("123456789", types.HiddenUserServer)
	newLID := types.NewJID("987654321", types.HiddenUserServer)
	client.StoreLIDPNMapping(ctx, oldLID, pn)
	maintenanceIdentityMessage(t, c, h, "mapped", oldLID, pn.String(), now-5)
	q := storage.MaintenanceQuery{From: now - 20, To: now, Limit: 10, ChatID: pn.String()}
	before, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, before.Coverage.Complete)
	// PutLIDMapping removes the prior LID for the same PN in the provider transaction.
	client.StoreLIDPNMapping(ctx, newLID, pn)
	var remaining int
	require.NoError(t, c.db.Get(&remaining, `SELECT COUNT(*) FROM whatsmeow_lid_map WHERE lid=$1`, oldLID.User))
	require.Zero(t, remaining)
	q.Token = before.Coverage.Token
	page, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.False(t, page.Coverage.Complete)
	require.Contains(t, page.Coverage.Errors, "identity_unknown")
	require.Contains(t, page.Coverage.Errors, "revision_changed")
	q.Token = ""
	q.ChatID = oldLID.String()
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.False(t, page.Coverage.Complete)
	require.Contains(t, page.Coverage.Errors, "identity_unknown")
	q.Token = page.Coverage.Token
	stable, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.Equal(t, q.Token, stable.Coverage.Token)
	client.StoreLIDPNMapping(ctx, oldLID, pn)
	q.Token = ""
	page, err = h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, page.Coverage.Complete)
	require.Len(t, page.Items, 1)
	require.Equal(t, pn.String(), page.Items[0].JID)
}
func TestMaintenanceOutOfWindowMappingCorrectionKeepsFixedWindowToken(t *testing.T) {
	c, h, client, now := maintenanceIdentityFixture(t)
	ctx := context.Background()
	pnA := types.NewJID("15550000001", types.DefaultUserServer)
	pnC := types.NewJID("15550000003", types.DefaultUserServer)
	pnD := types.NewJID("15550000004", types.DefaultUserServer)
	outsideLID := types.NewJID("777777777", types.HiddenUserServer)
	client.StoreLIDPNMapping(ctx, outsideLID, pnC)
	maintenanceIdentityMessage(t, c, h, "outside", outsideLID, pnC.String(), now-100)
	maintenanceIdentityMessage(t, c, h, "inside", pnA, pnA.String(), now-5)
	q := storage.MaintenanceQuery{From: now - 20, To: now, Limit: 10, ChatID: pnA.String()}
	before, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, before.Coverage.Complete)
	client.StoreLIDPNMapping(ctx, outsideLID, pnD)
	outside, err := h.Page(ctx, storage.MaintenanceQuery{From: now - 110, To: now - 90, Limit: 10})
	require.NoError(t, err)
	require.Len(t, outside.Items, 1)
	require.Equal(t, pnD.String(), outside.Items[0].JID)
	q.Token = before.Coverage.Token
	page, err := h.Page(ctx, q)
	require.NoError(t, err)
	require.True(t, page.Coverage.Complete)
	require.Equal(t, q.Token, page.Coverage.Token)
}

func TestMaintenanceUnknownLIDFilterWithoutHistoryCannotProveQuiet(t *testing.T) {
	_, h, _, now := maintenanceIdentityFixture(t)
	page, err := h.Page(context.Background(), storage.MaintenanceQuery{From: now - 20, To: now, Limit: 10, ChatID: "999999999@lid"})
	require.NoError(t, err)
	require.Empty(t, page.Items)
	require.False(t, page.Coverage.Complete)
	require.False(t, page.Coverage.EnumerationComplete)
	require.Contains(t, page.Coverage.Errors, "identity_unknown")
}
