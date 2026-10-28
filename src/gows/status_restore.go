package gows

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/devlikeapro/gows/storage"
)

const statusHistoryRestoreMarker = ".min-status-history-restored"

// The filesystem restore protocol is specific to WAHA's local GOWS store.
// Derive the marker from that configured database, never a request or marker
// payload. Postgres stores do not participate in filesystem snapshot restore.
func statusRestoreMarker(dialect, address string) (string, error) {
	if dialect != "sqlite3" && dialect != "sqlite" {
		return "", nil
	}
	path := strings.SplitN(address, "?", 2)[0]
	if strings.HasPrefix(path, "file:") {
		uri, err := url.Parse(path)
		if err != nil || uri.Host != "" {
			return "", fmt.Errorf("invalid local Status store address")
		}
		path = uri.Path
		if path == "" {
			path = uri.Opaque
		}
		if uri.Opaque != "" {
			path, err = url.PathUnescape(path)
			if err != nil {
				return "", fmt.Errorf("invalid local Status store address")
			}
		}
	}
	if filepath.Base(path) != "gows.db" {
		return "", nil // Other standalone stores do not use Min's restore protocol.
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("invalid local Status store address")
	}
	return filepath.Join(filepath.Dir(absolute), statusHistoryRestoreMarker), nil
}

func invalidateRestoredStatusHistory(ctx context.Context, store storage.StatusHistoryStorage, dialect, address string) error {
	marker, err := statusRestoreMarker(dialect, address)
	if err != nil || marker == "" {
		return err
	}
	info, err := os.Lstat(marker)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("Status restore marker is unavailable or not a regular file")
	}
	if err = store.InvalidateRestore(ctx); err != nil {
		return fmt.Errorf("restored Status buffer invalidation failed: %w", err)
	}
	if err = os.Remove(marker); err != nil {
		return fmt.Errorf("Status restore marker removal failed")
	}
	return nil
}
