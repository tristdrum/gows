package gows

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	DefaultChatArchiveAppStateAttempts = 4
	DefaultChatArchiveAppStateDelay    = 10 * time.Second

	chatArchiveRegularLowRefreshAttempts = 4
)

var missingAppStateKeyPattern = regexp.MustCompile(`(?i)failed to get key ([0-9a-f]+)`)

type ChatArchiveAppStateClient interface {
	SendAppState(context.Context, appstate.PatchInfo) error
	FetchAppState(context.Context, appstate.WAPatchName, bool, bool) error
}

type ChatArchiveAppStateKeyRequester interface {
	RequestAppStateKeys(context.Context, [][]byte)
}

type ChatArchiveAppStateRecoveryRequester interface {
	RequestAppStateRecovery(context.Context, appstate.WAPatchName) error
}

type ChatArchiveAppStateSyncWaiter interface {
	WaitForAppStateSync(context.Context, appstate.WAPatchName, time.Duration) error
}

type ChatArchiveAppStateSnapshotPrimer interface {
	PrimeAppStateSnapshotVersion(context.Context, appstate.WAPatchName) error
}

func (gows *GoWS) RequestAppStateKeys(ctx context.Context, rawKeyIDs [][]byte) {
	if gows == nil || gows.int == nil || len(rawKeyIDs) == 0 {
		return
	}
	gows.int.RequestAppStateKeys(ctx, rawKeyIDs)
}

func (gows *GoWS) RequestAppStateRecovery(ctx context.Context, name appstate.WAPatchName) error {
	if gows == nil || gows.Client == nil {
		return nil
	}
	gows.Log.Infof("Requesting app state recovery for %s", name)
	resp, err := gows.SendPeerMessage(ctx, whatsmeow.BuildAppStateRecoveryRequest(name))
	if err != nil {
		gows.Log.Warnf("Failed to request app state recovery for %s: %v", name, err)
		return err
	}
	gows.Log.Infof("Requested app state recovery for %s with message %s", name, resp.ID)
	return err
}

func (gows *GoWS) WaitForAppStateSync(ctx context.Context, name appstate.WAPatchName, wait time.Duration) error {
	if gows == nil || gows.Client == nil || wait <= 0 {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	result := make(chan error, 1)
	handlerID := gows.AddEventHandler(func(event interface{}) {
		switch evt := event.(type) {
		case *events.AppStateSyncComplete:
			if evt.Name == name {
				select {
				case result <- nil:
				default:
				}
			}
		case *events.AppStateSyncError:
			if evt.Name == name {
				select {
				case result <- evt.Error:
				default:
				}
			}
		}
	})
	defer gows.RemoveEventHandler(handlerID)

	select {
	case err := <-result:
		return err
	case <-waitCtx.Done():
		return waitCtx.Err()
	}
}

func SendChatArchiveAppState(
	ctx context.Context,
	client ChatArchiveAppStateClient,
	buildPatch func() appstate.PatchInfo,
	attempts int,
	retryDelay time.Duration,
) error {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := client.SendAppState(ctx, buildPatch()); err != nil {
			lastErr = err
			if attempt == attempts || !IsRetryableRegularLowAppStateError(err) {
				return err
			}
			refreshErr := RecoverRegularLowAppState(ctx, client, err, retryDelay)
			if refreshErr != nil {
				lastErr = fmt.Errorf("%w (also failed to recover regular_low app state before retry: %w)", err, refreshErr)
				return lastErr
			}
			continue
		}
		return nil
	}
	return lastErr
}

func RecoverRegularLowAppState(
	ctx context.Context,
	client ChatArchiveAppStateClient,
	cause error,
	retryDelay time.Duration,
) error {
	requestMissingAppStateKeys(ctx, client, cause)

	var lastErr error
	recoveryRequested := false
	for attempt := 1; attempt <= chatArchiveRegularLowRefreshAttempts; attempt++ {
		if retryDelay > 0 {
			if waitErr := waitForChatArchiveAppStateRecovery(ctx, client, retryDelay); waitErr != nil {
				return waitErr
			}
		}

		refreshErr := client.FetchAppState(ctx, appstate.WAPatchRegularLow, false, false)
		if refreshErr == nil {
			return nil
		}
		lastErr = refreshErr
		requestMissingAppStateKeys(ctx, client, refreshErr)
		if !IsRetryableRegularLowAppStateError(refreshErr) {
			return refreshErr
		}
		if !recoveryRequested {
			recoveryRequested = true
			if recoveryErr := requestRegularLowAppStateRecovery(ctx, client); recoveryErr != nil {
				lastErr = fmt.Errorf("%w (also failed to request regular_low app state recovery: %v)", refreshErr, recoveryErr)
			}
		}
	}

	fullSyncErr := client.FetchAppState(ctx, appstate.WAPatchRegularLow, true, false)
	if fullSyncErr == nil {
		return nil
	}
	requestMissingAppStateKeys(ctx, client, fullSyncErr)
	if IsRetryableRegularLowAppStateError(fullSyncErr) {
		if primeErr := primeRegularLowAppStateSnapshot(ctx, client); primeErr == nil {
			return nil
		} else {
			return fmt.Errorf("%w (also failed full regular_low app state sync: %v; also failed to prime regular_low snapshot cursor: %w)", lastErr, fullSyncErr, primeErr)
		}
	}
	return fmt.Errorf("%w (also failed full regular_low app state sync: %v)", lastErr, fullSyncErr)
}

func IsRetryableRegularLowAppStateError(err error) bool {
	if err == nil {
		return false
	}
	errorText := strings.ToLower(err.Error())
	if !strings.Contains(errorText, "regular_low") || !strings.Contains(errorText, "app state") {
		return false
	}
	return strings.Contains(errorText, "conflict") ||
		strings.Contains(errorText, "409") ||
		strings.Contains(errorText, "didn't find app state key") ||
		strings.Contains(errorText, "failed to get key")
}

func requestMissingAppStateKeys(ctx context.Context, client ChatArchiveAppStateClient, err error) {
	requester, ok := client.(ChatArchiveAppStateKeyRequester)
	if !ok {
		return
	}
	keyIDs := MissingAppStateKeyIDs(err)
	if len(keyIDs) == 0 {
		return
	}
	requester.RequestAppStateKeys(ctx, keyIDs)
}

func requestRegularLowAppStateRecovery(ctx context.Context, client ChatArchiveAppStateClient) error {
	requester, ok := client.(ChatArchiveAppStateRecoveryRequester)
	if !ok {
		return nil
	}
	return requester.RequestAppStateRecovery(ctx, appstate.WAPatchRegularLow)
}

func waitForChatArchiveAppStateRecovery(
	ctx context.Context,
	client ChatArchiveAppStateClient,
	retryDelay time.Duration,
) error {
	if retryDelay <= 0 {
		return nil
	}
	if waiter, ok := client.(ChatArchiveAppStateSyncWaiter); ok {
		err := waiter.WaitForAppStateSync(ctx, appstate.WAPatchRegularLow, retryDelay)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) || IsRetryableRegularLowAppStateError(err) {
			return nil
		}
		return err
	}
	timer := time.NewTimer(retryDelay)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func primeRegularLowAppStateSnapshot(ctx context.Context, client ChatArchiveAppStateClient) error {
	primer, ok := client.(ChatArchiveAppStateSnapshotPrimer)
	if !ok {
		return nil
	}
	return primer.PrimeAppStateSnapshotVersion(ctx, appstate.WAPatchRegularLow)
}

func MissingAppStateKeyIDs(err error) [][]byte {
	if err == nil {
		return nil
	}
	matches := missingAppStateKeyPattern.FindAllStringSubmatch(err.Error(), -1)
	if len(matches) == 0 {
		return nil
	}
	keyIDs := make([][]byte, 0, len(matches))
	seen := map[string]bool{}
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		hexKeyID := strings.ToUpper(match[1])
		if seen[hexKeyID] {
			continue
		}
		keyID, decodeErr := hex.DecodeString(hexKeyID)
		if decodeErr != nil || len(keyID) == 0 {
			continue
		}
		seen[hexKeyID] = true
		keyIDs = append(keyIDs, keyID)
	}
	return keyIDs
}
