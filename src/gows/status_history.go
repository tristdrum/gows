package gows

import (
	"context"
	"time"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func (st *StorageEventHandler) handleStatusHistoryEvent(event interface{}, receivedAtMS int64) bool {
	msg, ok := event.(*events.Message)
	if !ok || msg.Info.Chat != types.StatusBroadcastJID {
		return false
	}
	// Status processing never enters the ordinary message/revoke/unread pipeline,
	// even when capture is disabled or a session's webhook ignore flag is false.
	if st.storage.StatusHistory == nil || st.gows == nil {
		return true
	}
	ctx, cancel := context.WithTimeout(st.gows.Context, 5*time.Second)
	defer cancel()
	state := storage.StatusDeliveryAck
	if msg.Info.IsFromMe {
		state = storage.StatusServerAck
	}
	stored := &storage.StoredMessage{Message: msg, Status: &state, IsReal: storage.IsRealMessage(msg)}
	if err := st.storage.StatusHistory.Capture(ctx, stored, st.gows.GetOwnId().User, receivedAtMS); err != nil {
		st.log.Errorf("Status history capture failed")
	}
	return true
}

func startStatusHistory(ctx context.Context, store storage.StatusHistoryStorage) (func(), error) {
	work, cancel := context.WithCancel(ctx)
	startup, finish := context.WithTimeout(work, 10*time.Second)
	err := store.Restart(startup)
	finish()
	if err != nil {
		cancel()
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-work.Done():
				return
			case <-ticker.C:
				bounded, end := context.WithTimeout(work, 10*time.Second)
				_ = store.Prune(bounded)
				end()
			}
		}
	}()
	return func() { cancel(); <-done }, nil
}
