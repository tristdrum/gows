package gows

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waServerSync"
)

type fakeArchiveAppStateClient struct {
	sendErrors            []error
	fetchErrors           []error
	sendCalls             int
	fetchCalls            int
	patchBuilds           int
	requestedKeys         [][]byte
	recoveryRequests      []appstate.WAPatchName
	waitErrors            []error
	waitCalls             int
	waitNames             []appstate.WAPatchName
	waitDurations         []time.Duration
	primeErrors           []error
	primeCalls            int
	primeNames            []appstate.WAPatchName
	lastFetchName         appstate.WAPatchName
	lastFetchFullSync     bool
	lastFetchOnlyIfSynced bool
	fetchFullSyncValues   []bool
}

func (client *fakeArchiveAppStateClient) SendAppState(
	_ context.Context,
	_ appstate.PatchInfo,
) error {
	client.sendCalls++
	if len(client.sendErrors) == 0 {
		return nil
	}
	err := client.sendErrors[0]
	client.sendErrors = client.sendErrors[1:]
	return err
}

func (client *fakeArchiveAppStateClient) FetchAppState(
	_ context.Context,
	name appstate.WAPatchName,
	fullSync bool,
	onlyIfNotSynced bool,
) error {
	client.fetchCalls++
	client.lastFetchName = name
	client.lastFetchFullSync = fullSync
	client.lastFetchOnlyIfSynced = onlyIfNotSynced
	client.fetchFullSyncValues = append(client.fetchFullSyncValues, fullSync)
	if len(client.fetchErrors) == 0 {
		return nil
	}
	err := client.fetchErrors[0]
	client.fetchErrors = client.fetchErrors[1:]
	return err
}

func (client *fakeArchiveAppStateClient) RequestAppStateKeys(
	_ context.Context,
	keyIDs [][]byte,
) {
	client.requestedKeys = append(client.requestedKeys, keyIDs...)
}

func (client *fakeArchiveAppStateClient) RequestAppStateRecovery(
	_ context.Context,
	name appstate.WAPatchName,
) error {
	client.recoveryRequests = append(client.recoveryRequests, name)
	return nil
}

func (client *fakeArchiveAppStateClient) WaitForAppStateSync(
	_ context.Context,
	name appstate.WAPatchName,
	wait time.Duration,
) error {
	client.waitCalls++
	client.waitNames = append(client.waitNames, name)
	client.waitDurations = append(client.waitDurations, wait)
	if len(client.waitErrors) == 0 {
		return nil
	}
	err := client.waitErrors[0]
	client.waitErrors = client.waitErrors[1:]
	return err
}

func (client *fakeArchiveAppStateClient) PrimeAppStateSnapshotVersion(
	_ context.Context,
	name appstate.WAPatchName,
) error {
	client.primeCalls++
	client.primeNames = append(client.primeNames, name)
	if len(client.primeErrors) == 0 {
		return nil
	}
	err := client.primeErrors[0]
	client.primeErrors = client.primeErrors[1:]
	return err
}

func keyIDHex(keyIDs [][]byte) []string {
	values := make([]string, 0, len(keyIDs))
	for _, keyID := range keyIDs {
		values = append(values, strings.ToUpper(hex.EncodeToString(keyID)))
	}
	return values
}

func retryableRegularLowKeyConflict(keyID string) error {
	return retryableAppStateKeyConflict(appstate.WAPatchRegularLow, keyID)
}

func retryableRegularHighKeyConflict(keyID string) error {
	return retryableAppStateKeyConflict(appstate.WAPatchRegularHigh, keyID)
}

func retryableAppStateKeyConflict(patchName appstate.WAPatchName, keyID string) error {
	return fmt.Errorf(
		"server returned error updating app state (%s): "+
			"<error code=\"409\" text=\"conflict\"/> "+
			"(also, applying patches in the response failed: "+
			"failed to decode app state %s patches: "+
			"failed to get key %s to decode mutation: didn't find app state key)",
		patchName,
		patchName,
		keyID,
	)
}

func TestSendChatArchiveAppStateRetriesRegularLowKeyConflict(t *testing.T) {
	client := &fakeArchiveAppStateClient{
		sendErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			nil,
		},
	}

	err := SendChatArchiveAppState(
		context.Background(),
		client,
		func() appstate.PatchInfo {
			client.patchBuilds++
			return appstate.PatchInfo{Type: appstate.WAPatchRegularLow}
		},
		3,
		0,
	)

	assert.NoError(t, err)
	assert.Equal(t, 2, client.sendCalls)
	assert.Equal(t, 1, client.fetchCalls)
	assert.Equal(t, appstate.WAPatchRegularLow, client.lastFetchName)
	assert.False(t, client.lastFetchFullSync)
	assert.False(t, client.lastFetchOnlyIfSynced)
	assert.Equal(t, 2, client.patchBuilds)
	assert.Equal(t, []string{"00080000C23A"}, keyIDHex(client.requestedKeys))
	assert.Empty(t, client.recoveryRequests)
}

func TestSendChatClearAppStateRetriesRegularHighKeyConflict(t *testing.T) {
	client := &fakeArchiveAppStateClient{
		sendErrors: []error{
			retryableRegularHighKeyConflict("00080000C23A"),
			nil,
		},
	}

	err := SendChatClearAppState(
		context.Background(),
		client,
		func() appstate.PatchInfo {
			client.patchBuilds++
			return appstate.PatchInfo{Type: appstate.WAPatchRegularHigh}
		},
		3,
		0,
	)

	assert.NoError(t, err)
	assert.Equal(t, 2, client.sendCalls)
	assert.Equal(t, 1, client.fetchCalls)
	assert.Equal(t, appstate.WAPatchRegularHigh, client.lastFetchName)
	assert.False(t, client.lastFetchFullSync)
	assert.False(t, client.lastFetchOnlyIfSynced)
	assert.Equal(t, 2, client.patchBuilds)
	assert.Equal(t, []string{"00080000C23A"}, keyIDHex(client.requestedKeys))
}

func TestSendChatArchiveAppStateWaitsForMissingKeyRecoveryBeforeRetry(t *testing.T) {
	client := &fakeArchiveAppStateClient{
		sendErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			nil,
		},
		fetchErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			nil,
		},
	}

	err := SendChatArchiveAppState(
		context.Background(),
		client,
		func() appstate.PatchInfo {
			client.patchBuilds++
			return appstate.PatchInfo{Type: appstate.WAPatchRegularLow}
		},
		3,
		0,
	)

	assert.NoError(t, err)
	assert.Equal(t, 2, client.sendCalls)
	assert.Equal(t, 2, client.fetchCalls)
	assert.Equal(t, []string{"00080000C23A", "00080000C23A"}, keyIDHex(client.requestedKeys))
	assert.Equal(t, []appstate.WAPatchName{appstate.WAPatchRegularLow}, client.recoveryRequests)
	assert.Equal(t, []bool{false, false}, client.fetchFullSyncValues)
}

func TestSendChatArchiveAppStateWaitsOnSyncSignalBeforeRefresh(t *testing.T) {
	client := &fakeArchiveAppStateClient{
		sendErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			nil,
		},
	}

	err := SendChatArchiveAppState(
		context.Background(),
		client,
		func() appstate.PatchInfo {
			client.patchBuilds++
			return appstate.PatchInfo{Type: appstate.WAPatchRegularLow}
		},
		3,
		5*time.Second,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, client.waitCalls)
	assert.Equal(t, []appstate.WAPatchName{appstate.WAPatchRegularLow}, client.waitNames)
	assert.Equal(t, []time.Duration{5 * time.Second}, client.waitDurations)
	assert.Equal(t, 1, client.fetchCalls)
	assert.Equal(t, 2, client.sendCalls)
}

func TestSendChatArchiveAppStateContinuesAfterRetryableSyncError(t *testing.T) {
	client := &fakeArchiveAppStateClient{
		sendErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			nil,
		},
		waitErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
		},
	}

	err := SendChatArchiveAppState(
		context.Background(),
		client,
		func() appstate.PatchInfo {
			client.patchBuilds++
			return appstate.PatchInfo{Type: appstate.WAPatchRegularLow}
		},
		3,
		5*time.Second,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, client.waitCalls)
	assert.Equal(t, 1, client.fetchCalls)
	assert.Equal(t, 2, client.sendCalls)
}

func TestSendChatArchiveAppStateStopsAfterNonRetryableSyncError(t *testing.T) {
	syncErr := errors.New("regular_low app state store unavailable")
	client := &fakeArchiveAppStateClient{
		sendErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			nil,
		},
		waitErrors: []error{
			syncErr,
		},
	}

	err := SendChatArchiveAppState(
		context.Background(),
		client,
		func() appstate.PatchInfo {
			client.patchBuilds++
			return appstate.PatchInfo{Type: appstate.WAPatchRegularLow}
		},
		3,
		5*time.Second,
	)

	assert.ErrorIs(t, err, syncErr)
	assert.Equal(t, 1, client.waitCalls)
	assert.Equal(t, 0, client.fetchCalls)
	assert.Equal(t, 1, client.sendCalls)
}

func TestSendChatArchiveAppStateFallsBackToOneFullSyncAfterRecoveryAttempts(t *testing.T) {
	client := &fakeArchiveAppStateClient{
		sendErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			nil,
		},
		fetchErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23A"),
			nil,
		},
	}

	err := SendChatArchiveAppState(
		context.Background(),
		client,
		func() appstate.PatchInfo {
			client.patchBuilds++
			return appstate.PatchInfo{Type: appstate.WAPatchRegularLow}
		},
		3,
		0,
	)

	assert.NoError(t, err)
	assert.Equal(t, 2, client.sendCalls)
	assert.Equal(t, 5, client.fetchCalls)
	assert.Equal(t, []bool{false, false, false, false, true}, client.fetchFullSyncValues)
	assert.Equal(t, []appstate.WAPatchName{appstate.WAPatchRegularLow}, client.recoveryRequests)
}

func TestSendChatArchiveAppStatePrimesSnapshotCursorAfterFullSyncKeyFailure(t *testing.T) {
	client := &fakeArchiveAppStateClient{
		sendErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			nil,
		},
		fetchErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23C"),
		},
	}

	err := SendChatArchiveAppState(
		context.Background(),
		client,
		func() appstate.PatchInfo {
			client.patchBuilds++
			return appstate.PatchInfo{Type: appstate.WAPatchRegularLow}
		},
		3,
		0,
	)

	assert.NoError(t, err)
	assert.Equal(t, 2, client.sendCalls)
	assert.Equal(t, 5, client.fetchCalls)
	assert.Equal(t, []bool{false, false, false, false, true}, client.fetchFullSyncValues)
	assert.Equal(t, 1, client.primeCalls)
	assert.Equal(t, []appstate.WAPatchName{appstate.WAPatchRegularLow}, client.primeNames)
}

func TestSendChatArchiveAppStateReturnsSnapshotPrimeFailure(t *testing.T) {
	primeErr := errors.New("snapshot fetch failed")
	client := &fakeArchiveAppStateClient{
		sendErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			nil,
		},
		fetchErrors: []error{
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23A"),
			retryableRegularLowKeyConflict("00080000C23C"),
		},
		primeErrors: []error{primeErr},
	}

	err := SendChatArchiveAppState(
		context.Background(),
		client,
		func() appstate.PatchInfo {
			return appstate.PatchInfo{Type: appstate.WAPatchRegularLow}
		},
		3,
		0,
	)

	assert.ErrorIs(t, err, primeErr)
	assert.Equal(t, 1, client.sendCalls)
	assert.Equal(t, 1, client.primeCalls)
}

func TestSendChatArchiveAppStateDoesNotRetryUnrelatedErrors(t *testing.T) {
	originalErr := errors.New("network unavailable")
	client := &fakeArchiveAppStateClient{sendErrors: []error{originalErr}}

	err := SendChatArchiveAppState(
		context.Background(),
		client,
		func() appstate.PatchInfo {
			return appstate.PatchInfo{Type: appstate.WAPatchRegularLow}
		},
		3,
		0,
	)

	assert.ErrorIs(t, err, originalErr)
	assert.Equal(t, 1, client.sendCalls)
	assert.Equal(t, 0, client.fetchCalls)
}

func TestBuildAppStateSnapshotCursorAppliesSnapshotAndPatches(t *testing.T) {
	snapshotRecord := syncdRecordForTest(0x01, 0x02)
	patchRecord := syncdRecordForTest(0x01, 0x03)
	patchValueMAC, err := syncdRecordValueMAC(patchRecord)
	assert.NoError(t, err)
	indexMAC, err := syncdRecordIndexMAC(snapshotRecord)
	assert.NoError(t, err)

	cursor, err := buildAppStateSnapshotCursor(&appstate.PatchList{
		Name: appstate.WAPatchRegularLow,
		Snapshot: &waServerSync.SyncdSnapshot{
			Version: &waServerSync.SyncdVersion{Version: uint64Ptr(25)},
			Records: []*waServerSync.SyncdRecord{
				snapshotRecord,
			},
		},
		Patches: []*waServerSync.SyncdPatch{{
			Version: &waServerSync.SyncdVersion{Version: uint64Ptr(26)},
			Mutations: []*waServerSync.SyncdMutation{{
				Operation: waServerSync.SyncdMutation_SET.Enum(),
				Record:    patchRecord,
			}},
		}},
	})

	assert.NoError(t, err)
	assert.Equal(t, uint64(26), cursor.version)
	assert.Equal(t, patchValueMAC, cursor.valueMACs[indexMAC])
	assert.NotEqual(t, [128]byte{}, cursor.hash)
}

func TestMissingAppStateKeyIDsDeduplicatesKeyIDs(t *testing.T) {
	keyIDs := MissingAppStateKeyIDs(
		errors.New(
			"failed to get key 00080000C23A to decode mutation: didn't find app state key; " +
				"failed to get key 00080000c23a to decode mutation",
		),
	)

	assert.Equal(t, []string{"00080000C23A"}, keyIDHex(keyIDs))
}

func uint64Ptr(value uint64) *uint64 {
	return &value
}

func syncdRecordForTest(indexByte, valueByte byte) *waServerSync.SyncdRecord {
	value := append(bytes.Repeat([]byte{valueByte}, 8), bytes.Repeat([]byte{valueByte + 1}, 32)...)
	return &waServerSync.SyncdRecord{
		Index: &waServerSync.SyncdIndex{Blob: bytes.Repeat([]byte{indexByte}, 32)},
		Value: &waServerSync.SyncdValue{Blob: value},
	}
}
