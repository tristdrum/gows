package gows

import (
	"context"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/appstate"
)

type chatReadStateEvidenceStorage interface {
	HasAnyChatReadState() (bool, error)
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
	hasEvidence, err := evidence.HasAnyChatReadState()
	if err != nil || hasEvidence {
		return false, err
	}
	if err := fetcher.FetchAppState(
		ctx,
		appstate.WAPatchRegularHigh,
		true,
		false,
	); err != nil {
		return false, err
	}
	return true, nil
}
