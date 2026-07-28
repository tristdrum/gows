package gows

import (
	"context"
	"errors"
	"testing"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
)

type bootstrapReadStateStorage struct {
	hasEvidence bool
	err         error
}

func (s *bootstrapReadStateStorage) UpsertChatReadState(*storage.StoredChatReadState) (bool, error) {
	return false, nil
}

func (s *bootstrapReadStateStorage) ApplyChatReadEvent(storage.ChatReadEvent) (bool, error) {
	return false, nil
}

func (s *bootstrapReadStateStorage) GetChatReadStates([]types.JID, bool) (map[string]*storage.StoredChatReadState, error) {
	return nil, nil
}

func (s *bootstrapReadStateStorage) DeleteChatReadState(types.JID) error { return nil }

func (s *bootstrapReadStateStorage) HasAnyChatReadState() (bool, error) {
	return s.hasEvidence, s.err
}

type bootstrapAppStateFetcher struct {
	name            appstate.WAPatchName
	fullSync        bool
	onlyIfNotSynced bool
	calls           int
	err             error
}

func (f *bootstrapAppStateFetcher) FetchAppState(
	_ context.Context,
	name appstate.WAPatchName,
	fullSync bool,
	onlyIfNotSynced bool,
) error {
	f.calls++
	f.name = name
	f.fullSync = fullSync
	f.onlyIfNotSynced = onlyIfNotSynced
	return f.err
}

func TestBootstrapChatReadStateFetchesRegularHighSnapshotWhenEvidenceIsEmpty(t *testing.T) {
	states := &bootstrapReadStateStorage{}
	fetcher := &bootstrapAppStateFetcher{}

	fetched, err := bootstrapChatReadState(context.Background(), states, fetcher)

	require.NoError(t, err)
	assert.True(t, fetched)
	assert.Equal(t, 1, fetcher.calls)
	assert.Equal(t, appstate.WAPatchRegularHigh, fetcher.name)
	assert.True(t, fetcher.fullSync)
	assert.False(t, fetcher.onlyIfNotSynced)
}

func TestBootstrapChatReadStateSkipsSnapshotWhenEvidenceAlreadyExists(t *testing.T) {
	states := &bootstrapReadStateStorage{hasEvidence: true}
	fetcher := &bootstrapAppStateFetcher{}

	fetched, err := bootstrapChatReadState(context.Background(), states, fetcher)

	require.NoError(t, err)
	assert.False(t, fetched)
	assert.Zero(t, fetcher.calls)
}

func TestBootstrapChatReadStateReturnsProbeAndFetchErrors(t *testing.T) {
	probeErr := errors.New("probe failed")
	_, err := bootstrapChatReadState(
		context.Background(),
		&bootstrapReadStateStorage{err: probeErr},
		&bootstrapAppStateFetcher{},
	)
	assert.ErrorIs(t, err, probeErr)

	fetchErr := errors.New("fetch failed")
	_, err = bootstrapChatReadState(
		context.Background(),
		&bootstrapReadStateStorage{},
		&bootstrapAppStateFetcher{err: fetchErr},
	)
	assert.ErrorIs(t, err, fetchErr)
}
