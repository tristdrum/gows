package gows

import (
	"testing"

	"github.com/devlikeapro/gows/storage"
	"github.com/devlikeapro/gows/storage/noop"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type receiptOnlyStorage struct {
	*noop.MessageStorage
	t       *testing.T
	ids     []types.MessageID
	updates []storage.Status
}

func (s *receiptOnlyStorage) GetMessageWithRetries(types.MessageID) (*storage.StoredMessage, error) {
	s.t.Fatal("receipts must not load a stale message snapshot")
	return nil, nil
}

func (s *receiptOnlyStorage) UpsertOneMessage(*storage.StoredMessage) error {
	s.t.Fatal("receipts must not rewrite message content")
	return nil
}

func (s *receiptOnlyStorage) UpdateMessageStatus(id types.MessageID, status storage.Status) error {
	s.ids = append(s.ids, id)
	s.updates = append(s.updates, status)
	return nil
}

func TestReceiptHandlerOnlyAdvancesStoredStatus(t *testing.T) {
	messages := &receiptOnlyStorage{MessageStorage: noop.NewMessageStorage(), t: t}
	handler := &StorageEventHandler{
		log: waLog.Noop, storage: &storage.Storage{Messages: messages},
	}
	for _, receipt := range []types.ReceiptType{types.ReceiptTypeDelivered, types.ReceiptTypeRead, types.ReceiptTypePlayed} {
		handler.handleReceipt(&events.Receipt{
			MessageSource: types.MessageSource{Chat: types.JID{User: "fixture", Server: types.GroupServer}},
			MessageIDs:    []types.MessageID{"fixture-message"},
			Type:          receipt,
		})
	}
	require.Equal(t, []types.MessageID{"fixture-message", "fixture-message", "fixture-message"}, messages.ids)
	require.Equal(t, []storage.Status{storage.StatusDeliveryAck, storage.StatusRead, storage.StatusPlayed}, messages.updates)
}
