package gows

import (
	"context"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type captureHistorySpy struct {
	storage.StatusHistoryStorage
	captured *storage.StoredMessage
	received int64
	phone    string
}

func (h *captureHistorySpy) Capture(_ context.Context, msg *storage.StoredMessage, phone string, received int64) error {
	h.captured, h.received, h.phone = msg, received, phone
	return nil
}

func TestStatusCaptureIsIndependentOfNormalIgnoreAndStorage(t *testing.T) {
	st, normal, _ := newTestHandler()
	jid := types.NewJID("15550000001", types.DefaultUserServer)
	st.gows = &GoWS{Client: &whatsmeow.Client{Store: &store.Device{ID: &jid}}, Context: context.Background()}
	history := &captureHistorySpy{}
	st.storage.StatusHistory = history
	msg := conversationMessage("status")
	msg.Info.Chat, msg.Info.Timestamp = types.StatusBroadcastJID, time.Now()
	st.ignoreJids = &IgnoreJidsConfig{Status: true}
	received := time.Now().UnixMilli()
	require.True(t, st.handleStatusHistoryEvent(msg, received))
	require.Same(t, msg, history.captured.Message)
	require.Equal(t, received, history.received)
	require.Equal(t, jid.User, history.phone)
	require.Empty(t, normal.deleted)
	st.storage.StatusHistory = nil
	require.True(t, st.handleStatusHistoryEvent(msg, received))
	msg.Info.Chat = jid
	require.False(t, st.handleStatusHistoryEvent(msg, received))
	require.False(t, st.handleStatusHistoryEvent(&events.Receipt{}, received))
}
