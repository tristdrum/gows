package gows

import (
	"context"
	"errors"
	"fmt"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/appstate/lthash"
	"go.mau.fi/whatsmeow/proto/waServerSync"
)

type appStateSnapshotCursor struct {
	version   uint64
	hash      [128]byte
	valueMACs map[[32]byte][]byte
}

func (gows *GoWS) PrimeAppStateSnapshotVersion(ctx context.Context, name appstate.WAPatchName) error {
	if gows == nil || gows.int == nil || gows.Client == nil || gows.Store == nil || gows.Store.AppState == nil {
		return nil
	}

	cursor, err := gows.fetchAppStateSnapshotCursor(ctx, name)
	if err != nil {
		gows.Log.Warnf("Failed to prime %s app state snapshot cursor: %v", name, err)
		return err
	}
	if cursor.version == 0 {
		return fmt.Errorf("cannot prime %s app state snapshot cursor from empty version", name)
	}

	err = gows.Store.AppState.PutAppStateVersion(ctx, string(name), cursor.version, cursor.hash)
	if err != nil {
		return fmt.Errorf("failed to store primed %s app state snapshot cursor: %w", name, err)
	}
	gows.Log.Warnf(
		"Primed %s app state snapshot cursor at v%d without mutation MAC cache after key recovery failed",
		name,
		cursor.version,
	)
	return nil
}

func (gows *GoWS) fetchAppStateSnapshotCursor(
	ctx context.Context,
	name appstate.WAPatchName,
) (appStateSnapshotCursor, error) {
	patches, err := gows.int.FetchAppStatePatches(ctx, name, 0, true)
	if err != nil {
		return appStateSnapshotCursor{}, fmt.Errorf("failed to fetch %s snapshot: %w", name, err)
	}
	cursor, err := buildAppStateSnapshotCursor(patches)
	if err != nil {
		return appStateSnapshotCursor{}, err
	}

	for patches.HasMorePatches {
		patches, err = gows.int.FetchAppStatePatches(ctx, name, cursor.version, false)
		if err != nil {
			return appStateSnapshotCursor{}, fmt.Errorf("failed to fetch %s patches from v%d: %w", name, cursor.version, err)
		}
		err = cursor.applyPatchList(patches)
		if err != nil {
			return appStateSnapshotCursor{}, err
		}
	}
	return cursor, nil
}

func buildAppStateSnapshotCursor(patches *appstate.PatchList) (appStateSnapshotCursor, error) {
	if patches == nil {
		return appStateSnapshotCursor{}, errors.New("missing app state patch list")
	}
	if patches.Snapshot == nil {
		return appStateSnapshotCursor{}, fmt.Errorf("missing %s app state snapshot", patches.Name)
	}

	cursor := appStateSnapshotCursor{
		version:   patches.Snapshot.GetVersion().GetVersion(),
		valueMACs: map[[32]byte][]byte{},
	}

	added := make([][]byte, 0, len(patches.Snapshot.GetRecords()))
	for i, record := range patches.Snapshot.GetRecords() {
		indexMAC, err := syncdRecordIndexMAC(record)
		if err != nil {
			return appStateSnapshotCursor{}, fmt.Errorf("invalid %s snapshot record #%d: %w", patches.Name, i+1, err)
		}
		valueMAC, err := syncdRecordValueMAC(record)
		if err != nil {
			return appStateSnapshotCursor{}, fmt.Errorf("invalid %s snapshot record #%d: %w", patches.Name, i+1, err)
		}
		cursor.valueMACs[indexMAC] = valueMAC
		added = append(added, valueMAC)
	}
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(cursor.hash[:], nil, added)

	err := cursor.applyPatchList(patches)
	if err != nil {
		return appStateSnapshotCursor{}, err
	}
	return cursor, nil
}

func (cursor *appStateSnapshotCursor) applyPatchList(patches *appstate.PatchList) error {
	if cursor.valueMACs == nil {
		cursor.valueMACs = map[[32]byte][]byte{}
	}
	for _, patch := range patches.Patches {
		err := cursor.applyPatch(patches.Name, patch)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cursor *appStateSnapshotCursor) applyPatch(
	name appstate.WAPatchName,
	patch *waServerSync.SyncdPatch,
) error {
	if patch == nil {
		return nil
	}

	added := make([][]byte, 0, len(patch.GetMutations()))
	removed := make([][]byte, 0, len(patch.GetMutations()))
	for i, mutation := range patch.GetMutations() {
		record := mutation.GetRecord()
		indexMAC, err := syncdRecordIndexMAC(record)
		if err != nil {
			return fmt.Errorf("invalid %s patch v%d mutation #%d: %w", name, patch.GetVersion().GetVersion(), i+1, err)
		}
		if previousValueMAC, ok := cursor.valueMACs[indexMAC]; ok {
			removed = append(removed, previousValueMAC)
			delete(cursor.valueMACs, indexMAC)
		}
		if mutation.GetOperation() == waServerSync.SyncdMutation_SET {
			valueMAC, err := syncdRecordValueMAC(record)
			if err != nil {
				return fmt.Errorf("invalid %s patch v%d mutation #%d: %w", name, patch.GetVersion().GetVersion(), i+1, err)
			}
			cursor.valueMACs[indexMAC] = valueMAC
			added = append(added, valueMAC)
		}
	}
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(cursor.hash[:], removed, added)
	cursor.version = patch.GetVersion().GetVersion()
	return nil
}

func syncdRecordIndexMAC(record *waServerSync.SyncdRecord) ([32]byte, error) {
	var out [32]byte
	indexMAC := record.GetIndex().GetBlob()
	if len(indexMAC) != len(out) {
		return out, fmt.Errorf("invalid index MAC length %d", len(indexMAC))
	}
	copy(out[:], indexMAC)
	return out, nil
}

func syncdRecordValueMAC(record *waServerSync.SyncdRecord) ([]byte, error) {
	value := record.GetValue().GetBlob()
	if len(value) < 32 {
		return nil, fmt.Errorf("invalid value blob length %d", len(value))
	}
	return append([]byte(nil), value[len(value)-32:]...), nil
}
