package gows

import (
	"context"
	"time"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
)

const (
	chatReadStateBackfillLimit    = 1000
	chatReadStateBackfillInterval = time.Second
)

type chatReadStateBackfillMessageStorage interface {
	GetLastMessagesInChats(storage.ChatFilter, storage.Sort, storage.Pagination, bool) ([]*storage.StoredMessage, error)
}

type chatReadStateHistoryRequester func(context.Context, *types.MessageInfo) error

type chatReadStateBackfillWaiter func(context.Context) error

type chatReadStateBackfillResult struct {
	Eligible  int
	Requested int
	Failed    int
}

type chatReadStateEvidenceStorage interface {
	HasAnyKnownChatReadState() (bool, error)
}

type appStateSnapshotFetcher interface {
	FetchAppState(context.Context, appstate.WAPatchName, bool, bool) error
}

func bootstrapChatReadState(
	ctx context.Context,
	readStates storage.ChatReadStateStorage,
	fetcher appStateSnapshotFetcher,
) (bool, error) {
	evidence, ok := readStates.(chatReadStateEvidenceStorage)
	if !ok {
		return false, nil
	}
	hasEvidence, err := evidence.HasAnyKnownChatReadState()
	if err != nil || hasEvidence {
		return false, err
	}
	if err := fetcher.FetchAppState(
		ctx,
		appstate.WAPatchRegularLow,
		true,
		false,
	); err != nil {
		return false, err
	}
	return true, nil
}

func backfillUnknownChatReadStates(
	ctx context.Context,
	messages chatReadStateBackfillMessageStorage,
	readStates storage.ChatReadStateStorage,
	request chatReadStateHistoryRequester,
	wait chatReadStateBackfillWaiter,
) (chatReadStateBackfillResult, error) {
	result := chatReadStateBackfillResult{}
	latestMessages, err := messages.GetLastMessagesInChats(
		storage.ChatFilter{},
		storage.Sort{Field: "timestamp", Order: storage.SortDesc},
		storage.Pagination{Limit: chatReadStateBackfillLimit},
		true,
	)
	if err != nil {
		return result, err
	}

	eligible := make([]*storage.StoredMessage, 0, len(latestMessages))
	jids := make([]types.JID, 0, len(latestMessages))
	seen := make(map[string]struct{}, len(latestMessages))
	for _, msg := range latestMessages {
		if !isEligibleChatReadStateBackfillMessage(msg) {
			continue
		}
		key := msg.Info.Chat.String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		eligible = append(eligible, msg)
		jids = append(jids, msg.Info.Chat)
	}
	result.Eligible = len(eligible)
	if len(eligible) == 0 {
		return result, nil
	}

	states, err := readStates.GetChatReadStates(jids, true)
	if err != nil {
		return result, err
	}
	known := make(map[string]struct{}, len(states)*2)
	for key, state := range states {
		if state == nil || !state.UnreadStateKnown {
			continue
		}
		known[key] = struct{}{}
		known[state.Jid.String()] = struct{}{}
	}

	unknown := make([]*storage.StoredMessage, 0, len(eligible))
	for _, msg := range eligible {
		if _, exists := known[msg.Info.Chat.String()]; !exists {
			unknown = append(unknown, msg)
		}
	}

	for index, msg := range unknown {
		if err := request(ctx, &msg.Info); err != nil {
			result.Failed++
		} else {
			result.Requested++
		}
		if index < len(unknown)-1 {
			if err := wait(ctx); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func isEligibleChatReadStateBackfillMessage(msg *storage.StoredMessage) bool {
	if msg == nil || msg.Message == nil || msg.Info.Chat.IsEmpty() || msg.Info.ID == "" || msg.Info.Timestamp.IsZero() {
		return false
	}
	switch msg.Info.Chat.Server {
	case types.DefaultUserServer, types.HiddenUserServer, types.GroupServer:
		return true
	default:
		return false
	}
}

func waitForChatReadStateBackfill(ctx context.Context) error {
	timer := time.NewTimer(chatReadStateBackfillInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
