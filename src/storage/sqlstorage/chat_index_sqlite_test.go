package sqlstorage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/types"
)

func TestChatIndexSQLiteHistoryScansUseCoveringIndexes(t *testing.T) {
	container := newReadStateTestContainer(t)
	messages := container.NewMessageStorage()
	stamp := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 32; i++ {
		storeTestMessage(t, messages, fmt.Sprintf("plan-%02d", i),
			testJID(fmt.Sprintf("plan-%d", i%4), types.GroupServer), stamp.Add(time.Duration(i)*time.Second), false)
	}

	rows, err := messages.db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+latestIndexIdentitiesSQL("sqlite3"))
	require.NoError(t, err)
	defer rows.Close()
	var plan []string
	intermediates := make(map[string]bool)
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan = append(plan, detail)
		for _, prefix := range []string{"CO-ROUTINE ", "MATERIALIZE "} {
			if strings.HasPrefix(detail, prefix) {
				intermediates[strings.TrimPrefix(detail, prefix)] = true
			}
		}
	}
	require.NoError(t, rows.Err())

	for _, detail := range plan {
		if !strings.HasPrefix(detail, "SCAN ") {
			continue
		}
		source := strings.Fields(strings.TrimPrefix(detail, "SCAN "))[0]
		if intermediates[source] {
			continue
		}
		// This query reads one persistent table. Full scans of that history must
		// stay in an index; table lookups are reserved for latest-timestamp rows.
		// CTE/window result scans are intermediate work, not message-table scans.
		require.Contains(t, detail, "USING COVERING INDEX", "history scan fetches table rows: %s; plan: %v", detail, plan)
	}
}

func TestChatIndexSQLiteLatestIdentityTiesAndExclusions(t *testing.T) {
	container := newReadStateTestContainer(t)
	messages := container.NewMessageStorage()
	stamp := time.Unix(1_700_000_000, 0).UTC()
	later := stamp.Add(time.Second)
	for _, row := range []struct {
		id, jid string
		stamp   time.Time
		real    bool
	}{
		{"a-tie", "ties@g.us", stamp, true},
		{"Z-tie", "ties@g.us", stamp, true},
		{"0-older", "ties@g.us", stamp.Add(-time.Second), true},
		{"0-nonreal-tie", "ties@g.us", stamp, false},
		{"0-nonreal-newer", "ties@g.us", later, false},
		{"a-old", "latest@g.us", stamp, true},
		{"z-latest", "latest@g.us", later, true},
		{"a-lid", "0001@lid", stamp, true},
		{"z-pn", "1001@s.whatsapp.net", stamp, true},
		{"status-real", "status@broadcast", later.Add(time.Second), true},
		{"status-nonreal", "status@broadcast", later.Add(time.Second), false},
		{"nonreal-only", "nonreal@g.us", later, false},
	} {
		// Identity selection must use indexed classification and never decode
		// payloads. These deliberately invalid bodies are outside this query.
		_, err := messages.db.Exec("INSERT INTO gows_messages(id,jid,timestamp,from_me,data,is_real) VALUES (?,?,?,false,'{',?)", row.id, row.jid, row.stamp, row.real)
		require.NoError(t, err)
	}

	tx, err := messages.db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	identities, err := latestIndexIdentities(context.Background(), tx, nil)
	require.NoError(t, err)
	require.Equal(t, []indexMessageIdentity{
		{ID: "z-latest", JID: "latest@g.us", Timestamp: later, Canonical: "latest@g.us"},
		{ID: "a-lid", JID: "0001@lid", Timestamp: stamp, Canonical: "0001@lid"},
		{ID: "z-pn", JID: "1001@s.whatsapp.net", Timestamp: stamp, Canonical: "1001@s.whatsapp.net"},
		{ID: "Z-tie", JID: "ties@g.us", Timestamp: stamp, Canonical: "ties@g.us"},
	}, identities)

	// Equal-time aliases still prefer PN representation over a smaller LID ID.
	identities, err = latestIndexIdentities(context.Background(), tx, indexAliases{"0001@lid": "1001@s.whatsapp.net"})
	require.NoError(t, err)
	require.Equal(t, []indexMessageIdentity{
		{ID: "z-latest", JID: "latest@g.us", Timestamp: later, Canonical: "latest@g.us"},
		{ID: "z-pn", JID: "1001@s.whatsapp.net", Timestamp: stamp, Canonical: "1001@s.whatsapp.net"},
		{ID: "Z-tie", JID: "ties@g.us", Timestamp: stamp, Canonical: "ties@g.us"},
	}, identities)
}
