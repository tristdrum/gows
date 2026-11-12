package sqlstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Opt-in provider-free rehearsal, with no device credentials or network calls.
// This exceeds the measured recent-history cohort: 1000 recent rows (~4 MiB)
// plus 40000 older rows per session, 28 sessions and four parallel starters.
func TestMaintenanceSeedTiming(t *testing.T) {
	if os.Getenv("GOWS_SEED_TIMING") != "1" {
		t.Skip("set GOWS_SEED_TIMING=1 for the synthetic startup rehearsal")
	}
	const sessions = 28
	const concurrency = 4
	const oldRows = 40000
	const recentRows = 1000
	const payloadBytes = 4096
	root := t.TempDir()
	now := time.Now().UTC()
	ctx := context.Background()
	body := strings.Repeat("x", payloadBytes)
	addresses := make([]string, sessions)
	var sizes [sessions]int64
	run := func(work func(int) error) {
		jobs := make(chan int)
		errs := make(chan error, sessions)
		var wg sync.WaitGroup
		for n := 0; n < concurrency; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					errs <- work(i)
				}
			}()
		}
		for i := 0; i < sessions; i++ {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
	}
	preparation := time.Now()
	run(func(i int) error {
		file := filepath.Join(root, fmt.Sprintf("synthetic-%02d.db", i))
		addresses[i] = file + "?_foreign_keys=on&_busy_timeout=10000"
		c, err := New("sqlite3", addresses[i], waLog.Noop)
		if err != nil {
			return err
		}
		tx, err := c.db.BeginTxx(ctx, nil)
		if err != nil {
			c.Close()
			return err
		}
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO gows_messages(id,jid,from_me,timestamp,is_real,data) VALUES($1,$2,FALSE,$3,TRUE,$4)`)
		if err != nil {
			tx.Rollback()
			c.Close()
			return err
		}
		jid := fmt.Sprintf("synthetic-%02d@g.us", i)
		for n := 0; n < oldRows+recentRows; n++ {
			stamp := now.Add(-7 * 24 * time.Hour)
			if n >= oldRows {
				stamp = now.Add(-time.Hour)
			}
			id := fmt.Sprintf("synthetic-%02d-%08d", i, n)
			raw := `{"Info":{"ID":"` + id + `","Chat":"` + jid + `","Timestamp":"` + stamp.Format(time.RFC3339Nano) + `","IsNewsletterStatus":false},"Message":{"conversation":"` + body + `"},"IsReal":true}`
			if _, err = stmt.ExecContext(ctx, id, jid, stamp, raw); err != nil {
				stmt.Close()
				tx.Rollback()
				c.Close()
				return err
			}
		}
		if err = stmt.Close(); err != nil {
			tx.Rollback()
			c.Close()
			return err
		}
		if err = tx.Commit(); err != nil {
			c.Close()
			return err
		}
		if err = c.Close(); err != nil {
			return err
		}
		info, err := os.Stat(file)
		if err == nil {
			sizes[i] = info.Size()
		}
		return err
	})
	preparedMS := time.Since(preparation).Milliseconds()
	durations := make([]int64, sessions)
	indexed := make([]bool, sessions)
	started := time.Now()
	run(func(i int) error {
		begin := time.Now()
		c, err := New("sqlite3", addresses[i], waLog.Noop)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.NewMaintenanceHistoryStorage(ctx)
		if err != nil {
			return err
		}
		durations[i] = time.Since(begin).Milliseconds()
		var count int
		if err = c.db.Get(&count, `SELECT COUNT(*) FROM gows_maintenance_activity`); err != nil {
			return err
		}
		if count != recentRows {
			return fmt.Errorf("unexpected synthetic recent count: %d", count)
		}
		rows, err := c.db.Query(`EXPLAIN QUERY PLAN SELECT id FROM gows_messages WHERE is_real=TRUE AND timestamp >= $1 AND `+maintenanceChatSQL("jid")+` AND `+maintenanceContentSQL("sqlite3", "data"), now.Add(-maintenanceRetention))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
				return err
			}
			if strings.Contains(detail, "gows_messages_timestamp_idx") {
				indexed[i] = true
			}
		}
		return rows.Err()
	})
	elapsedMS := time.Since(started).Milliseconds()
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	var totalBytes int64
	for i := range sizes {
		totalBytes += sizes[i]
		require.True(t, indexed[i], "seed must retain its indexed recent-range query")
	}
	report, _ := json.Marshal(map[string]any{"sessions": sessions, "concurrency": concurrency, "old_rows_per_session": oldRows, "recent_rows_per_session": recentRows, "payload_bytes_per_row": payloadBytes, "source_bytes": totalBytes, "fixture_setup_ms": preparedMS, "startup_wall_ms": elapsedMS, "session_p50_ms": durations[sessions/2], "session_max_ms": durations[sessions-1], "all_recent_range_indexed": true, "environment": "local_mac_synthetic_warm_filesystem_cache"})
	t.Logf("SEED_TIMING %s", report)
}
