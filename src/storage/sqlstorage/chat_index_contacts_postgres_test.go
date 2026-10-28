package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	meowstorage "github.com/devlikeapro/gows/storage/meow"
	"github.com/devlikeapro/gows/storage/views"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// Exercise the real device-scoped address book through the full ChatView. The
// group-only storage benchmark does not include this contact-name query.
func TestChatIndexDirectContactPostgresPerformance(t *testing.T) {
	forChatIndexDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		if messages.db.DriverName() != "postgres" {
			t.Skip("Postgres direct-chat performance proof")
		}
		ctx := context.Background()
		cleanDevices := func() {
			_, err := messages.db.ExecContext(ctx, "DELETE FROM whatsmeow_device WHERE jid IN ($1,$2)", "15550009999@s.whatsapp.net", "15550008888@s.whatsapp.net")
			require.NoError(t, err)
		}
		cleanDevices()
		seed, err := messages.db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		for i := 0; i < 1000; i++ {
			message := integrityMessage(&waE2E.Message{Conversation: proto.String("Synthetic direct-chat fixture")}, storage.StatusRead)
			message.Info.ID = fmt.Sprintf("contacts-latest-%04d", i)
			message.Info.Chat = types.NewJID(fmt.Sprintf("1555000%04d", i), types.DefaultUserServer)
			data, err := messageMapper.Marshal(message)
			require.NoError(t, err)
			_, err = seed.ExecContext(ctx, "INSERT INTO gows_messages(id,jid,timestamp,from_me,is_real,data) VALUES ($1,$2,$3,false,true,$4)", message.Info.ID, message.Info.Chat.String(), message.Info.Timestamp, string(data))
			require.NoError(t, err)
			_, err = seed.ExecContext(ctx, "INSERT INTO gows_chat_read_state(jid,baseline_unread_count,marked_as_unread,unread_state_known,count_from_timestamp,covered_message_ids,evidence_timestamp) VALUES ($1,0,false,true,$2,'[]',$2)", message.Info.Chat.String(), message.Info.Timestamp.Add(time.Second).UnixMilli())
			require.NoError(t, err)
		}
		_, err = seed.ExecContext(ctx, `INSERT INTO gows_messages(id,jid,timestamp,from_me,is_real,data)
   SELECT 'contacts-old-' || c || '-' || n, '1555000' || lpad(c::text,4,'0') || '@s.whatsapp.net', TIMESTAMP '2020-01-01' + n*INTERVAL '1 second',false,true,'unselected'
   FROM generate_series(0,999) AS c CROSS JOIN generate_series(1,100) AS n`)
		require.NoError(t, err)
		require.NoError(t, seed.Commit())
		_, err = messages.db.ExecContext(ctx, "ANALYZE gows_messages")
		require.NoError(t, err)

		var queries atomic.Int64
		driverName := "chat-contact-counting-" + fmt.Sprint(time.Now().UnixNano())
		sql.Register(driverName, indexCountingDriver{&pq.Driver{}, &queries})
		db, err := sql.Open(driverName, os.Getenv("GOWS_TEST_POSTGRES_DSN"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		t.Cleanup(cleanDevices)
		messages.EntityRepository.db = sqlx.NewDb(db, "postgres")
		container := sqlstore.NewWithDB(db, "postgres", waLog.Noop)
		device := container.NewDevice()
		deviceID := types.NewJID("15550009999", types.DefaultUserServer)
		device.ID = &deviceID
		device.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte{}, AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
		require.NoError(t, device.Save(ctx))
		// A second device has conflicting names; the real store must still select
		// only this device's address book, not globally keyed contact JIDs.
		other := container.NewDevice()
		otherID := types.NewJID("15550008888", types.DefaultUserServer)
		other.ID = &otherID
		other.Account = device.Account
		require.NoError(t, other.Save(ctx))
		_, err = messages.db.ExecContext(ctx, `INSERT INTO whatsmeow_contacts(our_jid,their_jid,full_name)
   SELECT $1,'1555000' || lpad(c::text,4,'0') || '@s.whatsapp.net','Wrong device name'
   FROM generate_series(0,999) AS c`, otherID.String())
		require.NoError(t, err)
		view := views.NewChatView(messages, meowstorage.NewContactStorage(device), nil, nil)
		for _, contacts := range []int{1000, 10000} {
			_, err = messages.db.ExecContext(ctx, "DELETE FROM whatsmeow_contacts WHERE our_jid=$1", deviceID.String())
			require.NoError(t, err)
			_, err = messages.db.ExecContext(ctx, `INSERT INTO whatsmeow_contacts(our_jid,their_jid,full_name)
    SELECT $1,'1555000' || lpad(c::text,4,'0') || '@s.whatsapp.net','Stored fixture ' || c
    FROM generate_series(0,$2::int-1) AS c`, deviceID.String(), contacts)
			require.NoError(t, err)
			_, err = messages.db.ExecContext(ctx, "ANALYZE whatsmeow_contacts")
			require.NoError(t, err)
			for _, limit := range []uint64{1, 25, 50} {
				elapsed := make([]time.Duration, 0, 20)
				for sample := 0; sample < 20; sample++ {
					queries.Store(0)
					start := time.Now()
					page, err := view.GetChatIndex(ctx, storage.Pagination{Limit: limit}, false)
					elapsed = append(elapsed, time.Since(start))
					require.NoError(t, err)
					require.Len(t, page.Chats, int(limit))
					require.Equal(t, "Stored fixture 0", page.Chats[0].Chat.Name)
					require.Equal(t, int64(7), queries.Load(), "aliases,identities,readstates,count,boundary,payloads,devicecontacts")
				}
				first := elapsed[0]
				sort.Slice(elapsed, func(i, j int) bool { return elapsed[i] < elapsed[j] })
				p95 := elapsed[18]
				t.Logf("full direct-chat index: rows=101000 source_chats=1000 contacts=%d page=%d queries=7 samples=20 first=%s p95=%s", contacts, limit, first, p95)
				require.Less(t, p95, 5*time.Second)
			}
		}
	})
}
