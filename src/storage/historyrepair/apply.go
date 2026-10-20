package historyrepair

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"
)

func validateManifest(manifest Manifest) error {
	if manifest.Version != 1 {
		return errors.New("unsupported repair manifest version")
	}
	if err := manifest.Scope.validate(); err != nil {
		return err
	}
	if len(manifest.Rows) > manifest.Scope.MaxCandidates {
		return errors.New("manifest exceeds its candidate cap")
	}
	seen := make(map[string]bool)
	for _, row := range manifest.Rows {
		if row.ID == "" || seen[row.ID] || row.Timestamp.Before(time.Unix(manifest.Scope.Since, 0)) || !row.Timestamp.Before(time.Unix(manifest.Scope.Until, 0)) {
			return errors.New("candidate identity/window is invalid or duplicated")
		}
		seen[row.ID] = true
		if len(manifest.Scope.IDs) > 0 {
			permitted := false
			for _, id := range manifest.Scope.IDs {
				permitted = permitted || id == row.ID
			}
			if !permitted {
				return errors.New("candidate is outside explicit ID scope")
			}
		}
		if row.Before != newState(row.Before.Data, row.Before.IsReal) || row.After != newState(row.After.Data, row.After.IsReal) {
			return errors.New("candidate digest does not match")
		}
		expected, real, err := repairedState(row.ID, row.Timestamp, manifest.Scope, row.Before)
		if err != nil || !real || expected != row.After || row.Before == row.After {
			return errors.New("candidate is not an intact, canonical classification correction")
		}
	}
	return nil
}

// Apply changes only exact manifest states. Any changed or missing candidate
// aborts the entire transaction, including earlier candidates. Rollback uses
// the same guards in the reverse direction.
func Apply(manifest Manifest, rollback bool) (Report, error) {
	report := Report{}
	if err := validateManifest(manifest); err != nil {
		return report, err
	}
	db, err := openDatabase(manifest.Scope.Database, false)
	if err != nil {
		return report, err
	}
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	for _, row := range manifest.Rows {
		expected, replacement := row.Before, row.After
		if rollback {
			expected, replacement = replacement, expected
		}
		var raw string
		var indexed bool
		var timestamp time.Time
		err = tx.QueryRow("SELECT data, is_real, timestamp FROM gows_messages WHERE id = ? AND jid = ?", row.ID, manifest.Scope.Group).
			Scan(&raw, &indexed, &timestamp)
		if err != nil || !timestamp.Equal(row.Timestamp) {
			return Report{}, errors.New("candidate disappeared or moved; refusing the transaction")
		}
		current := newState(raw, indexed)
		if current == replacement {
			report.AlreadyApplied++
			continue
		}
		if current != expected {
			return Report{}, errors.New("candidate changed since audit; refusing the transaction")
		}
		result, err := tx.Exec(`UPDATE gows_messages SET is_real = ?, data = ?
			WHERE id = ? AND jid = ? AND data = ? AND is_real = ?`,
			replacement.IsReal, replacement.Data, row.ID, manifest.Scope.Group, expected.Data, expected.IsReal)
		if err != nil {
			return Report{}, err
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			return Report{}, errors.New("candidate compare-and-set failed; refusing the transaction")
		}
		report.Changed++
	}
	if err := tx.Commit(); err != nil {
		return Report{}, err
	}
	return report, nil
}

func WriteManifest(path string, manifest Manifest) error {
	if err := validateManifest(manifest); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(file).Encode(manifest); err != nil {
		file.Close()
		os.Remove(path)
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(path)
		return err
	}
	return file.Close()
}

func ReadManifest(path string) (Manifest, error) {
	var manifest Manifest
	info, err := os.Lstat(path)
	if err != nil {
		return manifest, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 128*1024*1024 {
		return manifest, errors.New("manifest must be a private regular file smaller than 128 MiB")
	}
	file, err := os.Open(path)
	if err != nil {
		return manifest, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, errors.New("invalid repair manifest JSON")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Manifest{}, errors.New("manifest contains trailing content")
	}
	return manifest, validateManifest(manifest)
}
