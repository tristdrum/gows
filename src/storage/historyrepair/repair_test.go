package historyrepair

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/devlikeapro/gows/storage/sqlstorage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func repairFixture(t *testing.T) (Scope, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "provider.db")
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(`CREATE TABLE gows_messages (
		id TEXT PRIMARY KEY, jid TEXT NOT NULL, timestamp TIMESTAMP NOT NULL,
		is_real BOOLEAN NOT NULL, data TEXT NOT NULL)`)
	require.NoError(t, err)
	return Scope{
		Database: path, Group: "fixture-group@g.us", Since: 1_700_000_000,
		Until: 1_700_000_100, MaxCandidates: 100,
	}, db
}

func insertFixture(t *testing.T, db *sql.DB, scope Scope, id string, real, indexed bool) string {
	t.Helper()
	jid, err := types.ParseJID(scope.Group)
	require.NoError(t, err)
	content := &waE2E.Message{SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{}}
	if real {
		content = &waE2E.Message{Conversation: proto.String("Private sanitized fixture")}
	}
	status := storage.StatusRead
	message := &storage.StoredMessage{
		Message: &events.Message{
			Info: types.MessageInfo{
				ID: id, MessageSource: types.MessageSource{Chat: jid},
				Timestamp: time.Unix(scope.Since+1, 0).UTC(),
			},
			Message: content,
		},
		IsReal: real, Status: &status,
	}
	raw, err := (&sqlstorage.MessageMapper{}).Marshal(message)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO gows_messages VALUES (?, ?, ?, ?, ?)", id, scope.Group, message.Info.Timestamp, indexed, string(raw))
	require.NoError(t, err)
	return string(raw)
}

func TestAuditIsReadOnlyScopedAndLeavesLostContentUnresolved(t *testing.T) {
	scope, db := repairFixture(t)
	before := insertFixture(t, db, scope, "candidate", true, false)
	insertFixture(t, db, scope, "lost-content", false, true)
	other := scope
	other.Group = "other@g.us"
	insertFixture(t, db, other, "other-group", true, false)
	manifest, report, err := Audit(scope)
	require.NoError(t, err)
	require.Equal(t, 2, report.Scanned)
	require.Equal(t, 1, report.Candidates)
	require.Equal(t, 1, report.Unresolved)
	require.Len(t, manifest.Rows, 1)
	require.Equal(t, before, manifest.Rows[0].After.Data) // Prefer index-only repair.
	var raw string
	var indexed bool
	require.NoError(t, db.QueryRow("SELECT data, is_real FROM gows_messages WHERE id = 'candidate'").Scan(&raw, &indexed))
	require.Equal(t, before, raw)
	require.False(t, indexed)
	readOnly, err := openDatabase(scope.Database, true)
	require.NoError(t, err)
	defer readOnly.Close()
	_, err = readOnly.Exec("UPDATE gows_messages SET is_real = TRUE")
	require.Error(t, err)
}

func TestRepairApplyRollbackAndIdempotency(t *testing.T) {
	scope, db := repairFixture(t)
	before := insertFixture(t, db, scope, "candidate", true, false)
	manifest, _, err := Audit(scope)
	require.NoError(t, err)
	report, err := Apply(manifest, false)
	require.NoError(t, err)
	require.Equal(t, 1, report.Changed)
	report, err = Apply(manifest, false)
	require.NoError(t, err)
	require.Equal(t, 0, report.Changed)
	require.Equal(t, 1, report.AlreadyApplied)
	report, err = Apply(manifest, true)
	require.NoError(t, err)
	require.Equal(t, 1, report.Changed)
	report, err = Apply(manifest, true)
	require.NoError(t, err)
	require.Equal(t, 1, report.AlreadyApplied)
	var raw string
	var indexed bool
	require.NoError(t, db.QueryRow("SELECT data, is_real FROM gows_messages WHERE id = 'candidate'").Scan(&raw, &indexed))
	require.Equal(t, before, raw)
	require.False(t, indexed)
}

func TestRepairRefusesStaleOrInvalidManifestWithoutPartialWrites(t *testing.T) {
	scope, db := repairFixture(t)
	insertFixture(t, db, scope, "first", true, false)
	insertFixture(t, db, scope, "second", true, false)
	manifest, _, err := Audit(scope)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE gows_messages SET data = data || ' ' WHERE id = 'second'")
	require.NoError(t, err)
	_, err = Apply(manifest, false)
	require.Error(t, err)
	var indexed bool
	require.NoError(t, db.QueryRow("SELECT is_real FROM gows_messages WHERE id = 'first'").Scan(&indexed))
	require.False(t, indexed)
	manifest.Rows[0].Before.Digest = "tampered"
	_, err = Apply(manifest, false)
	require.Error(t, err)
}

func TestAuditCapsCandidatesAndSelectsExactIDs(t *testing.T) {
	scope, db := repairFixture(t)
	insertFixture(t, db, scope, "first", true, false)
	insertFixture(t, db, scope, "second", true, false)
	scope.MaxCandidates = 1
	_, _, err := Audit(scope)
	require.Error(t, err)
	scope.IDs = []string{"first"}
	manifest, report, err := Audit(scope)
	require.NoError(t, err)
	require.Equal(t, 1, report.Scanned)
	require.Len(t, manifest.Rows, 1)
	scope.MaxCandidates = 101
	_, _, err = Audit(scope)
	require.Error(t, err)
}

func TestAuditRejectsMalformedAndWritesPrivateManifest(t *testing.T) {
	scope, db := repairFixture(t)
	insertFixture(t, db, scope, "candidate", true, false)
	insertFixture(t, db, scope, "malformed", true, false)
	_, err := db.Exec("UPDATE gows_messages SET data = 'malformed' WHERE id = 'malformed'")
	require.NoError(t, err)
	manifest, report, err := Audit(scope)
	require.NoError(t, err)
	require.Equal(t, 1, report.Malformed)
	require.Len(t, manifest.Rows, 1)
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, WriteManifest(path, manifest))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	require.Error(t, WriteManifest(path, manifest))
	loaded, err := ReadManifest(path)
	require.NoError(t, err)
	require.Equal(t, manifest, loaded)
	require.NoError(t, os.Chmod(path, 0644))
	_, err = ReadManifest(path)
	require.Error(t, err)
}

func TestRepairCorrectsSerializedProjectionWithoutLosingUnknownFields(t *testing.T) {
	scope, db := repairFixture(t)
	raw := insertFixture(t, db, scope, "candidate", true, false)
	var original map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &original))
	original["IsReal"] = json.RawMessage("false")
	original["Unknown"] = json.RawMessage(`{"keep":true}`)
	original["Message"] = json.RawMessage(`{"conversation":"Private sanitized fixture","futureField":{"keep":true}}`)
	before, err := json.Marshal(original)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE gows_messages SET data = ? WHERE id = 'candidate'", string(before))
	require.NoError(t, err)
	manifest, _, err := Audit(scope)
	require.NoError(t, err)
	require.Len(t, manifest.Rows, 1)
	_, err = Apply(manifest, false)
	require.NoError(t, err)
	require.NoError(t, db.QueryRow("SELECT data FROM gows_messages WHERE id = 'candidate'").Scan(&raw))
	original["IsReal"] = json.RawMessage("true")
	expected, err := json.Marshal(original)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), raw)
	_, err = Apply(manifest, true)
	require.NoError(t, err)
	require.NoError(t, db.QueryRow("SELECT data FROM gows_messages WHERE id = 'candidate'").Scan(&raw))
	require.Equal(t, string(before), raw)
}

func TestAuditHonorsExclusiveEndAndRejectsOutOfScopeIDs(t *testing.T) {
	scope, db := repairFixture(t)
	insertFixture(t, db, scope, "inside", true, false)
	boundary := scope
	boundary.Since = scope.Until - 1
	insertFixture(t, db, boundary, "at-end", true, false)
	manifest, report, err := Audit(scope)
	require.NoError(t, err)
	require.Equal(t, 1, report.Scanned)
	require.Len(t, manifest.Rows, 1)
	require.Equal(t, "inside", manifest.Rows[0].ID)
	scope.IDs = []string{"at-end"}
	_, _, err = Audit(scope)
	require.Error(t, err)
}

func TestRepairValidatesAfterStateAndRollbackGuard(t *testing.T) {
	scope, db := repairFixture(t)
	insertFixture(t, db, scope, "candidate", true, false)
	manifest, _, err := Audit(scope)
	require.NoError(t, err)
	tampered := manifest
	tampered.Rows = append([]Candidate{}, manifest.Rows...)
	tampered.Rows[0].After = newState(tampered.Rows[0].After.Data+" ", true)
	_, err = Apply(tampered, false)
	require.Error(t, err)
	_, err = Apply(manifest, false)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE gows_messages SET data = data || ' ' WHERE id = 'candidate'")
	require.NoError(t, err)
	_, err = Apply(manifest, true)
	require.Error(t, err)
	var indexed bool
	require.NoError(t, db.QueryRow("SELECT is_real FROM gows_messages WHERE id = 'candidate'").Scan(&indexed))
	require.True(t, indexed)
}
