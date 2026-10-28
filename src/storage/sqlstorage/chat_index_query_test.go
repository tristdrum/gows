package sqlstorage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/types"
)

type indexCountingDriver struct {
	driver.Driver
	queries *atomic.Int64
}
type indexCountingConn struct {
	driver.Conn
	queries *atomic.Int64
}

func (d indexCountingDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return indexCountingConn{conn, d.queries}, nil
}
func (c indexCountingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}
func (c indexCountingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.queries.Add(1)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c indexCountingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func TestChatIndexSQLRoundTripsDoNotGrowWithPageSize(t *testing.T) {
	container := newReadStateTestContainer(t)
	messages := container.NewMessageStorage()
	stamp := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 50; i++ {
		jid := testJID(fmt.Sprintf("queries-%03d", i), types.GroupServer)
		storeTestMessage(t, messages, fmt.Sprintf("queries-%03d", i), jid, stamp, false)
		_, err := container.NewChatReadStateStorage().UpsertChatReadState(&storage.StoredChatReadState{Jid: jid, UnreadStateKnown: true, CountFrom: stamp.Add(-time.Second), EvidenceTimestamp: stamp})
		require.NoError(t, err)
	}
	var databases []struct {
		Seq  int
		Name string
		File string
	}
	require.NoError(t, messages.db.Select(&databases, "PRAGMA database_list"))
	var queries atomic.Int64
	driverName := "chat-index-counting-" + fmt.Sprint(time.Now().UnixNano())
	sql.Register(driverName, indexCountingDriver{&sqlite3.SQLiteDriver{}, &queries})
	db, err := sql.Open(driverName, databases[0].File)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	messages.EntityRepository.db = sqlx.NewDb(db, "sqlite3")
	for _, limit := range []uint64{1, 25, 50} {
		queries.Store(0)
		page, err := messages.GetChatIndex(context.Background(), storage.Pagination{Limit: limit}, false)
		require.NoError(t, err)
		require.Len(t, page.Chats, int(limit))
		require.Equal(t, int64(7), queries.Load(), "alias,identities,states,count,boundary,payloads,names")
	}
}
