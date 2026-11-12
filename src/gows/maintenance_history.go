package gows

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/devlikeapro/gows/storage/sqlstorage"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type maintenanceWorker struct {
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	wake        chan struct{}
	slots       chan struct{}
	mu          sync.Mutex
	closing     bool
	download    func(context.Context, *waE2E.HistorySyncNotification, bool) (*waHistorySync.HistorySync, error)
	acknowledge func(context.Context, types.MessageID, types.ReceiptType) error
	persist     func(context.Context, *events.HistorySync) error
	auxiliary   func(context.Context, *waHistorySync.HistorySync) error
	publish     func(context.Context, *events.HistorySync) error
}

func (g *GoWS) startMaintenanceHistory(dialect, address string) error {
	h, err := g.container.NewMaintenanceHistoryStorage(g.Context)
	if err != nil {
		return err
	}
	g.Maintenance = h
	marker, err := statusRestoreMarker(dialect, address)
	if err != nil {
		return err
	}
	if marker != "" {
		dedicated := filepath.Join(filepath.Dir(marker), ".min-maintenance-history-restored")
		for _, candidate := range []string{marker, dedicated} {
			if info, statErr := os.Lstat(candidate); statErr == nil {
				if !info.Mode().IsRegular() {
					return errors.New("maintenance restore marker is not regular")
				}
				if err = h.InvalidateRestore(g.Context); err != nil {
					return err
				}
				if candidate == dedicated {
					if err = os.Remove(candidate); err != nil {
						return err
					}
				}
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return statErr
			}
		}
	}

	ctx, cancel := context.WithCancel(g.Context)
	w := &maintenanceWorker{ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), slots: make(chan struct{}, 4), download: g.DownloadHistorySync, acknowledge: g.SendProtocolMessageReceipt, persist: g.storageEventHandler.handleHistorySyncContext, auxiliary: g.persistHistoryAuxiliary}
	w.publish = func(ctx context.Context, event *events.HistorySync) error {
		select {
		case g.events <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	g.maintenanceWorker = w
	w.wg.Add(1)
	go func() { defer w.wg.Done(); g.runMaintenanceHistory(w) }()
	return nil
}
func (g *GoWS) stopMaintenanceHistory() {
	w := g.maintenanceWorker
	if w == nil {
		return
	}
	w.mu.Lock()
	w.closing = true
	w.cancel()
	w.mu.Unlock()
	w.wg.Wait()
}
func (w *maintenanceWorker) notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Registration is synchronous on WhatsMeow's dispatch boundary; the offline
// marker cannot overtake the download or persistence work it is acknowledging.
func (g *GoWS) handleMaintenanceEvent(event any) bool {
	h, w := g.Maintenance, g.maintenanceWorker
	if h == nil || w == nil {
		return false
	}
	switch e := event.(type) {
	case *events.Connected:
		if err := h.Connect(g.Context); err != nil {
			g.Log.Errorf("maintenance connection ledger failed")
		}
		w.notify()
		return false
	case *events.Contact:
		if h.Begin(g.Context) != nil {
			return false
		}
		err := g.storageEventHandler.handleContactLidJidMapping(e)
		_ = h.Finish(g.Context, err)
		return true
	case *events.UndecryptableMessage:
		_ = h.Fail(g.Context, "message_undecryptable")
		return false
	case *events.Disconnected:
		if err := h.Disconnect(g.Context); err != nil {
			g.Log.Errorf("maintenance disconnect ledger failed")
		}
		return false
	case *events.OfflineSyncCompleted:
		if err := h.OfflineComplete(g.Context); err != nil {
			g.Log.Errorf("maintenance offline ledger failed")
		}
		w.notify()
		return false
	case *events.Message:
		if notif := e.Message.GetProtocolMessage().GetHistorySyncNotification(); notif != nil && e.Info.IsFromMe {
			raw, err := proto.Marshal(notif)
			if err == nil {
				err = h.RegisterHistory(g.Context, e.Info.ID, raw)
			}
			if err != nil {
				_ = h.Fail(g.Context, "history_registration_failed")
			}
			w.notify()
			return true
		}
		if g.storageEventHandler.shouldIgnoreJID(e.Info.Chat) {
			return false
		}
		if e.Info.Chat.Server != types.DefaultUserServer && e.Info.Chat.Server != types.HiddenUserServer && e.Info.Chat.Server != types.GroupServer {
			return false
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.closing {
			return true
		}
		select {
		case w.slots <- struct{}{}:
		case <-w.ctx.Done():
			return true
		}
		writes, err := h.BeginMessage(g.Context, e)
		if err != nil {
			_ = h.Fail(g.Context, "storage_failed")
			<-w.slots
			return false
		}
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			defer func() { <-w.slots }()
			var result error
			defer func() {
				if recover() != nil {
					result = errors.New("storage panic")
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if h.FinishMessage(ctx, writes, result) != nil {
					g.Log.Errorf("maintenance persistence ledger failed")
				}
			}()
			status := storage.StatusDeliveryAck
			if e.Info.IsFromMe {
				status = storage.StatusServerAck
			}
			result = g.storageEventHandler.handleSaveMessage(e, &status)
			if result == nil {
				result = g.storageEventHandler.observeMaintenanceMessage(e)
			}
			if result == nil {
				result = g.storageEventHandler.handleMessageEvent(e)
			}
		}()
		return true
	case *events.HistorySync:
		// Manual mode is the only production source; directly injected/synthetic
		// history events still retain the same registration/error boundary.
		if h.Begin(g.Context) != nil {
			return true
		}
		err := g.storageEventHandler.handleHistorySync(e)
		_ = h.Finish(g.Context, err)
		return true
	}
	return false
}
func (st *StorageEventHandler) observeMaintenanceMessage(msg *events.Message) error {
	if !maintenanceOrdinaryJID(msg.Info.Chat) || st.gows == nil || st.gows.Maintenance == nil {
		return nil
	}
	if !storage.IsRealMessage(msg) {
		if maintenanceNonordinary(msg) {
			return nil
		}
		return st.gows.Maintenance.Fail(st.gows.Context, "activity_kind_unknown")
	}
	canonical, err := st.gows.maintenanceCanonical(msg.Info.Chat)
	if err != nil {
		canonical = ""
	}
	// Unknown mapping is retained explicitly; it is never promoted to a PN by
	// shape guesses or a nil DownloadHistorySync error.
	return st.gows.Maintenance.Observe(st.gows.Context, &storage.StoredMessage{Message: msg}, canonical)
}
func (g *GoWS) maintenanceCanonical(jid types.JID) (string, error) {
	jid = jid.ToNonAD()
	switch jid.Server {
	case types.DefaultUserServer, types.GroupServer:
		return jid.String(), nil
	case types.LegacyUserServer:
		jid.Server = types.DefaultUserServer
		return jid.String(), nil
	case types.HiddenUserServer:
		pn, err := g.Store.LIDs.GetPNForLID(g.Context, jid)
		if err != nil {
			return "", err
		}
		if pn.IsEmpty() || pn.Server != types.DefaultUserServer {
			return "", errors.New("identity mapping unavailable")
		}
		return pn.ToNonAD().String(), nil
	default:
		return "", errors.New("unsupported ordinary chat identity")
	}
}
func (g *GoWS) runMaintenanceHistory(w *maintenanceWorker) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	prune := time.NewTicker(10 * time.Minute)
	defer prune.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.wake:
		case <-ticker.C:
		case <-prune.C:
			_ = g.Maintenance.Prune(w.ctx)
		}
		if !g.IsConnected() {
			continue
		}
		jobs, err := g.Maintenance.HistoryJobs(w.ctx)
		if err != nil {
			continue
		}
		if len(jobs) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(w.ctx, 45*time.Second)
		err = g.processHistoryJob(ctx, w, g.Maintenance, jobs[0])
		cancel()
		if err != nil {
			g.Log.Warnf("maintenance history work pending after failed attempt")
		}
	}
}
func (g *GoWS) processHistoryJob(ctx context.Context, w *maintenanceWorker, h *sqlstorage.SQLMaintenanceHistoryStore, job storage.MaintenanceHistoryJob) (result error) {
	stage := "history_download_failed"
	defer func() {
		if result != nil {
			failureCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = h.HistoryFailed(failureCtx, job.ID, stage)
		}
	}()
	if err := h.HistoryAttempt(ctx, job.ID); err != nil {
		return err
	}
	if !job.Stored {
		var notif waE2E.HistorySyncNotification
		if err := proto.Unmarshal(job.Notification, &notif); err != nil {
			return err
		}
		if notif.GetFileLength() > 64*1024*1024 {
			return errors.New("history download exceeds bound")
		}
		blob, err := w.download(ctx, &notif, true)
		if err != nil {
			return err
		}
		if blob == nil {
			return errors.New("history download returned no data")
		}
		count := 0
		for _, conv := range blob.GetConversations() {
			count += len(conv.GetMessages())
		}
		if len(blob.GetConversations()) > 10000 || count > 100000 {
			return errors.New("history persistence exceeds bound")
		}
		stage = "history_store_failed"
		if err = w.auxiliary(ctx, blob); err != nil {
			return err
		}
		if err = w.persist(ctx, &events.HistorySync{Data: blob}); err != nil {
			return err
		}
		if err = h.HistoryStored(ctx, job.ID); err != nil {
			return err
		}
		if w.publish != nil {
			if err = w.publish(ctx, &events.HistorySync{Data: blob}); err != nil {
				return err
			}
		}
	}
	stage = "history_ack_failed"
	if err := w.acknowledge(ctx, job.ID, types.ReceiptTypeHistorySync); err != nil {
		return err
	}
	return h.HistoryAcked(ctx, job.ID)
}

func maintenanceOrdinaryJID(jid types.JID) bool {
	return jid.Server == types.DefaultUserServer || jid.Server == types.LegacyUserServer || jid.Server == types.HiddenUserServer || jid.Server == types.GroupServer
}

// Unknown future content is a coverage gap, not evidence of a quiet chat.
func maintenanceNonordinary(msg *events.Message) bool {
	if msg == nil || msg.Message == nil {
		return false
	}
	allowed := map[protoreflect.Name]bool{"reactionMessage": true, "encReactionMessage": true, "pollUpdateMessage": true, "protocolMessage": true, "senderKeyDistributionMessage": true, "messageContextInfo": true}
	known := false
	unknown := false
	msg.Message.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if !allowed[field.Name()] {
			unknown = true
		}
		if field.Name() != "messageContextInfo" {
			known = true
		}
		return true
	})
	return known && !unknown
}

func (st *StorageEventHandler) persistHistoryMessage(ctx context.Context, msg *events.Message, status *storage.Status) (result error) {
	if st.gows != nil && st.gows.Maintenance != nil && maintenanceOrdinaryJID(msg.Info.Chat) {
		writes, err := st.gows.Maintenance.BeginMessage(ctx, msg)
		if err != nil {
			return err
		}
		defer func() {
			finishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// The durable history job owns retry/failure state for this batch.
			// A retryable message write is not an irrecoverable ordinary-event gap.
			result = errors.Join(result, st.gows.Maintenance.FinishMessage(finishCtx, writes, nil))
		}()
	}
	if result = st.handleSaveMessage(msg, status); result != nil {
		return result
	}
	if result = st.observeMaintenanceMessage(msg); result != nil {
		return result
	}
	return st.handleMessageEvent(msg)
}
