package gows

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type bootstrapReadStateStorage struct {
	hasEvidence bool
	readStates  map[string]*storage.StoredChatReadState
	err         error
}

func (s *bootstrapReadStateStorage) UpsertChatReadState(*storage.StoredChatReadState) (bool, error) {
	return false, nil
}

func (s *bootstrapReadStateStorage) ApplyChatReadEvent(storage.ChatReadEvent) (bool, error) {
	return false, nil
}

func (s *bootstrapReadStateStorage) GetChatReadStates([]types.JID, bool) (map[string]*storage.StoredChatReadState, error) {
	return s.readStates, s.err
}

func (s *bootstrapReadStateStorage) DeleteChatReadState(types.JID) error { return nil }

func (s *bootstrapReadStateStorage) HasAnyKnownChatReadState() (bool, error) {
	return s.hasEvidence, s.err
}

type bootstrapAppStateFetcher struct {
	name            appstate.WAPatchName
	fullSync        bool
	onlyIfNotSynced bool
	calls           int
	err             error
}

type bootstrapMessageStorage struct {
	messages   []*storage.StoredMessage
	sort       storage.Sort
	pagination storage.Pagination
	merge      bool
	err        error
}

func (s *bootstrapMessageStorage) GetLastMessagesInChats(
	_ storage.ChatFilter,
	sortBy storage.Sort,
	pagination storage.Pagination,
	merge bool,
) ([]*storage.StoredMessage, error) {
	s.sort = sortBy
	s.pagination = pagination
	s.merge = merge
	return s.messages, s.err
}

func storedBootstrapMessage(jid types.JID, id string, timestamp time.Time) *storage.StoredMessage {
	return &storage.StoredMessage{Message: &events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: jid},
		ID:            types.MessageID(id),
		Timestamp:     timestamp,
	}}}
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

func TestBootstrapChatReadStateFetchesRegularLowSnapshotWhenEvidenceIsEmpty(t *testing.T) {
	states := &bootstrapReadStateStorage{}
	fetcher := &bootstrapAppStateFetcher{}

	fetched, err := bootstrapChatReadState(context.Background(), states, fetcher)

	require.NoError(t, err)
	assert.True(t, fetched)
	assert.Equal(t, 1, fetcher.calls)
	assert.Equal(t, appstate.WAPatchRegularLow, fetcher.name)
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

func TestBackfillUnknownChatReadStatesRequestsOnlyEligibleUnknownChats(t *testing.T) {
	unknownDirect := types.NewJID("15550000001", types.DefaultUserServer)
	knownGroup := types.NewJID("120363000001", types.GroupServer)
	unknownLID := types.NewJID("987654321", types.HiddenUserServer)
	newsletter := types.NewJID("12345", types.NewsletterServer)
	messages := &bootstrapMessageStorage{messages: []*storage.StoredMessage{
		storedBootstrapMessage(unknownDirect, "direct", time.Unix(40, 0)),
		storedBootstrapMessage(knownGroup, "group", time.Unix(30, 0)),
		storedBootstrapMessage(unknownLID, "lid", time.Unix(20, 0)),
		storedBootstrapMessage(newsletter, "newsletter", time.Unix(10, 0)),
		storedBootstrapMessage(unknownDirect, "duplicate", time.Unix(5, 0)),
		nil,
	}}
	states := &bootstrapReadStateStorage{readStates: map[string]*storage.StoredChatReadState{
		knownGroup.String(): {Jid: knownGroup, UnreadStateKnown: true},
	}}
	requested := make([]types.MessageInfo, 0)
	waits := 0

	result, err := backfillUnknownChatReadStates(
		context.Background(),
		messages,
		states,
		func(_ context.Context, info *types.MessageInfo) error {
			requested = append(requested, *info)
			return nil
		},
		func(context.Context) error {
			waits++
			return nil
		},
	)

	require.NoError(t, err)
	assert.Equal(t, chatReadStateBackfillResult{Eligible: 3, Requested: 2}, result)
	require.Len(t, requested, 2)
	assert.Equal(t, types.MessageID("direct"), requested[0].ID)
	assert.Equal(t, types.MessageID("lid"), requested[1].ID)
	assert.Equal(t, 1, waits)
	assert.Equal(t, storage.Sort{Field: "timestamp", Order: storage.SortDesc}, messages.sort)
	assert.Equal(t, storage.Pagination{Limit: chatReadStateBackfillLimit}, messages.pagination)
	assert.True(t, messages.merge)
}

func TestBackfillUnknownChatReadStatesContinuesAfterRequestFailure(t *testing.T) {
	first := types.NewJID("15550000001", types.DefaultUserServer)
	second := types.NewJID("15550000002", types.DefaultUserServer)
	messages := &bootstrapMessageStorage{messages: []*storage.StoredMessage{
		storedBootstrapMessage(first, "first", time.Unix(20, 0)),
		storedBootstrapMessage(second, "second", time.Unix(10, 0)),
	}}
	states := &bootstrapReadStateStorage{}
	calls := 0

	result, err := backfillUnknownChatReadStates(
		context.Background(),
		messages,
		states,
		func(context.Context, *types.MessageInfo) error {
			calls++
			if calls == 1 {
				return errors.New("request failed")
			}
			return nil
		},
		func(context.Context) error { return nil },
	)

	require.NoError(t, err)
	assert.Equal(t, chatReadStateBackfillResult{Eligible: 2, Requested: 1, Failed: 1}, result)
	assert.Equal(t, 2, calls)
}

func TestBackfillUnknownChatReadStatesStopsWhenPacingContextIsCancelled(t *testing.T) {
	first := types.NewJID("15550000001", types.DefaultUserServer)
	second := types.NewJID("15550000002", types.DefaultUserServer)
	messages := &bootstrapMessageStorage{messages: []*storage.StoredMessage{
		storedBootstrapMessage(first, "first", time.Unix(20, 0)),
		storedBootstrapMessage(second, "second", time.Unix(10, 0)),
	}}
	states := &bootstrapReadStateStorage{}
	cancelled := context.Canceled

	result, err := backfillUnknownChatReadStates(
		context.Background(),
		messages,
		states,
		func(context.Context, *types.MessageInfo) error { return nil },
		func(context.Context) error { return cancelled },
	)

	assert.ErrorIs(t, err, cancelled)
	assert.Equal(t, chatReadStateBackfillResult{Eligible: 2, Requested: 1}, result)
}
