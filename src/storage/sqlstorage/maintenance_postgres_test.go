package sqlstorage

import (
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Tests run sequentially. PostgreSQL uses a caller-created disposable local
// gows_*_test database and clears only that database's public test schema.
func newMaintenanceTestContainer(t *testing.T) *GContainer {
	t.Helper()
	dsn := os.Getenv("GOWS_TEST_POSTGRES_DSN")
	if dsn == "" {
		return newReadStateTestContainer(t)
	}
	address, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"localhost", "127.0.0.1", "::1"}, address.Hostname())
	name := strings.TrimPrefix(address.Path, "/")
	require.True(t, strings.HasPrefix(name, "gows_") && strings.HasSuffix(name, "_test"))
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	_, err = db.Exec(`DROP SCHEMA public CASCADE; CREATE SCHEMA public`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	container, err := New("postgres", dsn, waLog.Noop)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Close()) })
	return container
}
