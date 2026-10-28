package sqlstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestChatIndexPostgresQueryPlanAndCancellation(t *testing.T) {
	forChatIndexDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		if messages.db.DriverName() != "postgres" {
			t.Skip("Postgres query-plan and cancellation proof")
		}
		ctx := context.Background()
		seed, err := messages.db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		for i := 0; i < 1000; i++ {
			message := integrityMessage(&waE2E.Message{Conversation: proto.String("Synthetic performance fixture")}, storage.StatusRead)
			message.Info.ID = fmt.Sprintf("plan-latest-%04d", i)
			message.Info.Chat = types.NewJID(fmt.Sprintf("plan-%04d", i), types.GroupServer)
			data, err := messageMapper.Marshal(message)
			require.NoError(t, err)
			_, err = seed.ExecContext(ctx, "INSERT INTO gows_messages(id,jid,timestamp,from_me,is_real,data) VALUES ($1,$2,$3,false,true,$4)", message.Info.ID, message.Info.Chat.String(), message.Info.Timestamp, string(data))
			require.NoError(t, err)
		}
		// Old content intentionally cannot decode: only selected latest payloads
		// may cross the SQL boundary, even with a sizeable retained history.
		_, err = seed.ExecContext(ctx, `INSERT INTO gows_messages(id,jid,timestamp,from_me,is_real,data)
   SELECT 'plan-old-' || c || '-' || n, 'plan-' || lpad(c::text,4,'0') || '@g.us', TIMESTAMP '2020-01-01' + n*INTERVAL '1 second', false,true,'unselected'
   FROM generate_series(0,999) AS c CROSS JOIN generate_series(1,100) AS n`)
		require.NoError(t, err)
		require.NoError(t, seed.Commit())
		_, err = messages.db.ExecContext(ctx, "ANALYZE gows_messages")
		require.NoError(t, err)
		var rawPlan string
		require.NoError(t, messages.db.GetContext(ctx, &rawPlan, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+latestIndexIdentitiesSQL("postgres")))
		var plan []struct {
			ExecutionTime float64 `json:"Execution Time"`
			Plan          map[string]any
		}
		require.NoError(t, json.Unmarshal([]byte(rawPlan), &plan))
		require.Len(t, plan, 1)
		t.Logf("Postgres identity query: rows=101000 chats=1000 execution_ms=%.2f root=%v", plan[0].ExecutionTime, plan[0].Plan["Node Type"])
		times := make([]time.Duration, 0, 20)
		for i := 0; i < 20; i++ {
			start := time.Now()
			page, err := messages.GetChatIndex(ctx, storage.Pagination{Limit: 50}, false)
			require.NoError(t, err)
			require.Len(t, page.Chats, 50)
			times = append(times, time.Since(start))
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		p95 := times[18]
		t.Logf("Postgres compact page: chats=50 samples=20 p95=%s", p95)
		require.Less(t, p95, 5*time.Second)
		for _, read := range []func(context.Context) error{
			func(ctx context.Context) error {
				_, err := messages.GetChatIndex(ctx, storage.Pagination{Limit: 50}, false)
				return err
			},
			func(ctx context.Context) error {
				_, err := messages.GetAllMessagesContext(ctx, storage.MessageFilter{}, storage.Sort{Field: "timestamp", Order: storage.SortDesc}, storage.Pagination{Limit: 20}, false)
				return err
			},
		} {
			lock, err := messages.db.BeginTxx(ctx, nil)
			require.NoError(t, err)
			_, err = lock.ExecContext(ctx, "LOCK TABLE gows_messages IN ACCESS EXCLUSIVE MODE")
			require.NoError(t, err)
			deadline, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			started := time.Now()
			require.Error(t, read(deadline))
			require.ErrorIs(t, deadline.Err(), context.DeadlineExceeded)
			require.Less(t, time.Since(started), time.Second)
			cancel()
			require.NoError(t, lock.Rollback())
		}
		page, err := messages.GetChatIndex(ctx, storage.Pagination{Limit: 1}, false)
		require.NoError(t, err)
		require.Len(t, page.Chats, 1)
	})
}
