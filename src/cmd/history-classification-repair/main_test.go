package main

import (
	"database/sql"
	"io"
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

func commandOutput(t *testing.T, args ...string) (string, error) {
	t.Helper()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	previous := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = previous }()
	commandErr := run(args)
	require.NoError(t, writer.Close())
	output, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	return string(output), commandErr
}

func TestRepairCommandEmitsCountsAndUsesPrivateExplicitManifest(t *testing.T) {
	directory, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	database := filepath.Join(directory, "fixture.db")
	manifest := filepath.Join(directory, "manifest.json")
	db, err := sql.Open("sqlite3", database)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE gows_messages (id TEXT PRIMARY KEY, jid TEXT, timestamp TIMESTAMP, is_real BOOLEAN, data TEXT)")
	require.NoError(t, err)
	message := &storage.StoredMessage{
		Message: &events.Message{
			Info: types.MessageInfo{
				MessageSource: types.MessageSource{Chat: types.JID{User: "fixture", Server: types.GroupServer}},
				ID:            "private-fixture-id", Timestamp: time.Unix(1_700_000_001, 0).UTC(),
			},
			Message: &waE2E.Message{Conversation: proto.String("Private fixture body")},
		},
		IsReal: true,
	}
	raw, err := (&sqlstorage.MessageMapper{}).Marshal(message)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO gows_messages VALUES (?, ?, ?, FALSE, ?)", message.Info.ID, message.Info.Chat.String(), message.Info.Timestamp, string(raw))
	require.NoError(t, err)
	output, err := commandOutput(t, "audit", "--db", database, "--manifest", manifest,
		"--group", "fixture@g.us", "--since", "1700000000", "--until", "1700000100",
		"--ids", "private-fixture-id", "--max-candidates", "1")
	require.NoError(t, err)
	require.Contains(t, output, `"candidates":1`)
	require.NotContains(t, output, "Private fixture body")
	require.NotContains(t, output, "private-fixture-id")
	output, err = commandOutput(t, "apply", "--db", database, "--manifest", manifest)
	require.NoError(t, err)
	require.Contains(t, output, `"changed":1`)
	output, err = commandOutput(t, "rollback", "--db", database, "--manifest", manifest)
	require.NoError(t, err)
	require.Contains(t, output, `"changed":1`)
	_, err = commandOutput(t, "apply", "--db", database, "--manifest", manifest, "--group", "other@g.us")
	require.Error(t, err)
	_, err = commandOutput(t, "apply", "--db", database, "--manifest", manifest, "unexpected")
	require.Error(t, err)
}
