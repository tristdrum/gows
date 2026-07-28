package sqlstorage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

func newReadStateTestContainer(t *testing.T) *GContainer {
	t.Helper()
	container, err := New("sqlite3", filepath.Join(t.TempDir(), "gows.db")+"?_foreign_keys=on", waLog.Noop)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Close()) })
	return container
}

func testJID(user, server string) types.JID {
	return types.JID{User: user, Server: server}
}

func storeTestMessage(
	t *testing.T,
	messages storage.MessageStorage,
	id string,
	jid types.JID,
	timestamp time.Time,
	fromMe bool,
) {
	t.Helper()
	require.NoError(t, messages.UpsertOneMessage(&storage.StoredMessage{
		Message: &events.Message{
			Info: types.MessageInfo{
				MessageSource: types.MessageSource{Chat: jid, IsFromMe: fromMe},
				ID:            id,
				Timestamp:     timestamp,
			},
			Message: &waProto.Message{Conversation: proto.String(id)},
		},
		IsReal: true,
	}))
}

func TestChatReadStateAddsDeduplicatedInboundMessagesAfterHistoryBaseline(t *testing.T) {
	container := newReadStateTestContainer(t)
	states := container.NewChatReadStateStorage()
	messages := container.NewMessageStorage()
	jid := testJID("15551234567", types.DefaultUserServer)
	baseline := time.Unix(1_700_000_000, 0)

	applied, err := states.UpsertChatReadState(&storage.StoredChatReadState{
		Jid:                 jid,
		BaselineUnreadCount: 2,
		MarkedAsUnread:      false,
		UnreadStateKnown:    true,
		CountFrom:           baseline,
		EvidenceTimestamp:   baseline,
	})
	require.NoError(t, err)
	assert.True(t, applied)

	storeTestMessage(t, messages, "before", jid, baseline.Add(-time.Second), false)
	storeTestMessage(t, messages, "incoming", jid, baseline.Add(time.Second), false)
	storeTestMessage(t, messages, "incoming", jid, baseline.Add(time.Second), false)
	storeTestMessage(t, messages, "outgoing", jid, baseline.Add(2*time.Second), true)

	loaded, err := states.GetChatReadStates([]types.JID{jid}, true)
	require.NoError(t, err)
	counts, err := messages.CountInboundMessagesAfter(loaded, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), loaded[jid.String()].BaselineUnreadCount)
	assert.Equal(t, uint64(1), counts[jid.String()])
}

func TestChatReadEventsAreTimestampOrderedAndUnreadPreservesCountWatermark(t *testing.T) {
	container := newReadStateTestContainer(t)
	states := container.NewChatReadStateStorage()
	jid := testJID("15557654321", types.DefaultUserServer)
	t0 := time.Unix(1_700_000_000, 0)

	_, err := states.UpsertChatReadState(&storage.StoredChatReadState{
		Jid:                 jid,
		BaselineUnreadCount: 4,
		MarkedAsUnread:      false,
		UnreadStateKnown:    true,
		CountFrom:           t0,
		EvidenceTimestamp:   t0,
	})
	require.NoError(t, err)

	readWatermark := t0.Add(10 * time.Second)
	applied, err := states.ApplyChatReadEvent(storage.ChatReadEvent{
		Jid:              jid,
		Timestamp:        t0.Add(20 * time.Second),
		Read:             true,
		MessageWatermark: readWatermark,
	})
	require.NoError(t, err)
	assert.True(t, applied)

	applied, err = states.ApplyChatReadEvent(storage.ChatReadEvent{
		Jid:              jid,
		Timestamp:        t0.Add(15 * time.Second),
		Read:             false,
		MessageWatermark: t0.Add(9 * time.Second),
	})
	require.NoError(t, err)
	assert.False(t, applied)

	applied, err = states.ApplyChatReadEvent(storage.ChatReadEvent{
		Jid:              jid,
		Timestamp:        t0.Add(30 * time.Second),
		Read:             false,
		MessageWatermark: t0.Add(25 * time.Second),
	})
	require.NoError(t, err)
	assert.True(t, applied)

	loaded, err := states.GetChatReadStates([]types.JID{jid}, true)
	require.NoError(t, err)
	state := loaded[jid.String()]
	require.NotNil(t, state)
	assert.Zero(t, state.BaselineUnreadCount)
	assert.True(t, state.MarkedAsUnread)
	assert.True(t, state.UnreadStateKnown)
	assert.Equal(t, readWatermark, state.CountFrom)
	assert.Equal(t, t0.Add(30*time.Second), state.EvidenceTimestamp)
}

func TestUnreadEventWithoutBaselineEstablishesAuthoritativeMarkedUnreadState(t *testing.T) {
	container := newReadStateTestContainer(t)
	states := container.NewChatReadStateStorage()
	jid := testJID("15550001111", types.DefaultUserServer)

	watermark := time.Unix(1_700_000_090, 0)
	applied, err := states.ApplyChatReadEvent(storage.ChatReadEvent{
		Jid:              jid,
		Timestamp:        time.Unix(1_700_000_100, 0),
		Read:             false,
		MessageWatermark: watermark,
	})
	require.NoError(t, err)
	assert.True(t, applied)

	loaded, err := states.GetChatReadStates([]types.JID{jid}, true)
	require.NoError(t, err)
	state := loaded[jid.String()]
	require.NotNil(t, state)
	assert.True(t, state.MarkedAsUnread)
	assert.True(t, state.UnreadStateKnown)
	assert.Zero(t, state.BaselineUnreadCount)
	assert.Equal(t, watermark, state.CountFrom)
}

func TestIncompleteNewerHistoryKeepsEstablishedUnreadCount(t *testing.T) {
	container := newReadStateTestContainer(t)
	states := container.NewChatReadStateStorage()
	jid := testJID("15550003333", types.DefaultUserServer)
	t0 := time.Unix(1_700_000_000, 0)

	_, err := states.UpsertChatReadState(&storage.StoredChatReadState{
		Jid:                 jid,
		BaselineUnreadCount: 6,
		UnreadStateKnown:    true,
		CountFrom:           t0,
		EvidenceTimestamp:   t0,
	})
	require.NoError(t, err)
	_, err = states.UpsertChatReadState(&storage.StoredChatReadState{
		Jid:               jid,
		MarkedAsUnread:    true,
		UnreadStateKnown:  false,
		CountFrom:         t0.Add(time.Minute),
		EvidenceTimestamp: t0.Add(time.Minute),
	})
	require.NoError(t, err)

	loaded, err := states.GetChatReadStates([]types.JID{jid}, true)
	require.NoError(t, err)
	state := loaded[jid.String()]
	require.NotNil(t, state)
	assert.Equal(t, uint64(6), state.BaselineUnreadCount)
	assert.True(t, state.MarkedAsUnread)
	assert.True(t, state.UnreadStateKnown)
	assert.Equal(t, t0, state.CountFrom)
}

func TestChatReadStateMergesLIDAndPhoneMessages(t *testing.T) {
	container := newReadStateTestContainer(t)
	states := container.NewChatReadStateStorage()
	messages := container.NewMessageStorage()
	lid := testJID("100000000000001", types.HiddenUserServer)
	pn := testJID("15559990000", types.DefaultUserServer)
	baseline := time.Unix(1_700_000_000, 0)

	_, err := states.UpsertChatReadState(&storage.StoredChatReadState{
		Jid:                 lid,
		BaselineUnreadCount: 1,
		UnreadStateKnown:    true,
		CountFrom:           baseline,
		EvidenceTimestamp:   baseline,
	})
	require.NoError(t, err)
	_, err = container.db.Exec("INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ($1, $2)", lid.User, pn.User)
	require.NoError(t, err)

	storeTestMessage(t, messages, "lid-incoming", lid, baseline.Add(time.Second), false)
	storeTestMessage(t, messages, "pn-incoming", pn, baseline.Add(2*time.Second), false)

	loaded, err := states.GetChatReadStates([]types.JID{pn}, true)
	require.NoError(t, err)
	state := loaded[pn.String()]
	require.NotNil(t, state)
	assert.Equal(t, pn, state.Jid)
	_, err = states.ApplyChatReadEvent(storage.ChatReadEvent{
		Jid:              pn,
		Timestamp:        baseline.Add(10 * time.Second),
		Read:             false,
		MessageWatermark: baseline.Add(9 * time.Second),
	})
	require.NoError(t, err)
	loaded, err = states.GetChatReadStates([]types.JID{pn}, true)
	require.NoError(t, err)
	state = loaded[pn.String()]
	require.NotNil(t, state)
	assert.True(t, state.UnreadStateKnown)
	assert.True(t, state.MarkedAsUnread)
	assert.Equal(t, uint64(1), state.BaselineUnreadCount)

	counts, err := messages.CountInboundMessagesAfter(loaded, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), counts[pn.String()])

	require.NoError(t, states.DeleteChatReadState(pn))
	loaded, err = states.GetChatReadStates([]types.JID{pn}, true)
	require.NoError(t, err)
	assert.Empty(t, loaded)
}

func TestChatReadStateMergesNewerAuthoritativeLIDEventOverPhoneBaseline(t *testing.T) {
	container := newReadStateTestContainer(t)
	states := container.NewChatReadStateStorage()
	lid := testJID("100000000000002", types.HiddenUserServer)
	pn := testJID("15558880000", types.DefaultUserServer)
	t0 := time.Unix(1_700_000_000, 0)

	_, err := states.UpsertChatReadState(&storage.StoredChatReadState{
		Jid:                 pn,
		BaselineUnreadCount: 5,
		UnreadStateKnown:    true,
		CountFrom:           t0,
		EvidenceTimestamp:   t0,
	})
	require.NoError(t, err)
	_, err = states.ApplyChatReadEvent(storage.ChatReadEvent{
		Jid:              lid,
		Timestamp:        t0.Add(time.Minute),
		Read:             false,
		MessageWatermark: t0.Add(30 * time.Second),
	})
	require.NoError(t, err)
	_, err = container.db.Exec("INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ($1, $2)", lid.User, pn.User)
	require.NoError(t, err)

	loaded, err := states.GetChatReadStates([]types.JID{pn}, true)
	require.NoError(t, err)
	state := loaded[pn.String()]
	require.NotNil(t, state)
	assert.Zero(t, state.BaselineUnreadCount)
	assert.True(t, state.MarkedAsUnread)
	assert.True(t, state.UnreadStateKnown)
	assert.Equal(t, t0.Add(30*time.Second), state.CountFrom)
	assert.Equal(t, t0.Add(time.Minute), state.EvidenceTimestamp)
}
