package sqlstorage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
)

type maintenanceCursor struct {
	Token     string `json:"token"`
	Timestamp int64  `json:"timestamp"`
	JID       string `json:"jid"`
	ID        string `json:"id"`
}

func (h *SQLMaintenanceHistoryStore) Observe(ctx context.Context, msg *storage.StoredMessage, canonical string) error {
	if msg == nil || !storage.IsRealMessage(msg.Message) || !maintenanceChat(msg.Info.Chat.String()) || msg.Info.IsNewsletterStatus {
		return nil
	}
	edit, err := decodeMessageEdit(msg)
	if err != nil {
		return err
	}
	if edit != nil {
		return nil
	}
	if msg.Info.Timestamp.Unix() < time.Now().Add(-maintenanceRetention).Unix() {
		return nil
	}
	if canonical != "" && !maintenanceChat(canonical) {
		return errors.New("invalid maintenance canonical chat")
	}
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		jid := canonical
		if jid == "" {
			jid = msg.Info.Chat.String()
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO gows_maintenance_activity(id,provider_jid,jid,timestamp,from_me,identity_known,revision) VALUES($1,$2,$3,$4,$5,$6,(SELECT revision FROM gows_maintenance_state WHERE id=1))
   ON CONFLICT(id) DO UPDATE SET jid=excluded.jid,identity_known=excluded.identity_known,revision=excluded.revision`, msg.Info.ID, msg.Info.Chat.String(), jid, msg.Info.Timestamp.Unix(), msg.Info.IsFromMe, canonical != "")
		return err
	})
}

func (h *SQLMaintenanceHistoryStore) Page(ctx context.Context, q storage.MaintenanceQuery) (*storage.MaintenancePage, error) {
	if q.From <= 0 || q.To < q.From || q.To-q.From > 86400 || q.To > time.Now().Unix()+2 || q.Limit < 1 || q.Limit > 200 || len(q.Cursor) > 2048 || len(q.Token) > 128 || len(q.ChatID) > 100 {
		return nil, errors.New("invalid bounded maintenance history window")
	}
	if q.ChatID != "" && !maintenanceChat(q.ChatID) {
		return nil, errors.New("unsupported maintenance chat scope")
	}
	q.ChatID = strings.Replace(q.ChatID, "@c.us", "@s.whatsapp.net", 1)
	h.mu.Lock()
	defer h.mu.Unlock()
	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Same lock as registration and all ordinary message mutation triggers.
	if _, err = tx.ExecContext(ctx, `UPDATE gows_maintenance_state SET revision=revision WHERE id=1`); err != nil {
		return nil, err
	}
	var row struct {
		Data      string `db:"data"`
		Revision  int64  `db:"revision"`
		Untracked bool   `db:"untracked"`
	}
	if err = tx.GetContext(ctx, &row, `SELECT data,revision,untracked FROM gows_maintenance_state WHERE id=1`); err != nil {
		return nil, err
	}
	var s maintenanceState
	if err = json.Unmarshal([]byte(row.Data), &s); err != nil {
		return nil, err
	}
	p := &storage.MaintenancePage{Version: 1, Items: []storage.MaintenanceItem{}}
	p.Window.From = q.From
	p.Window.To = q.To
	var windowRevision int64
	if err = tx.GetContext(ctx, &windowRevision, `SELECT COALESCE(MAX(revision),0) FROM gows_maintenance_activity WHERE `+maintenanceChatSQL("provider_jid")+` AND `+maintenanceChatSQL("jid")+` AND timestamp>=$1 AND timestamp<=$2 AND ($3='' OR jid=$3 OR identity_known=FALSE)`, q.From, q.To, q.ChatID); err != nil {
		return nil, err
	}
	tokenData, _ := json.Marshal([]any{s.Epoch, windowRevision, s.Since, s.Fault, row.Untracked, q.From, q.To, q.ChatID})
	digest := sha256.Sum256(tokenData)
	p.Coverage = storage.MaintenanceCoverage{Complete: true, EnumerationComplete: true, Epoch: s.Epoch, Revision: windowRevision, Token: hex.EncodeToString(digest[:]), Scope: "provider_observed", Errors: []string{}}
	add := func(reason string) {
		p.Coverage.Complete = false
		if reason != "tracking_floor" && reason != "snapshot_restored" {
			p.Coverage.EnumerationComplete = false
		}
		p.Coverage.Errors = append(p.Coverage.Errors, reason)
	}
	if row.Untracked {
		add("untracked_mutation")
	}
	if !s.Connected {
		add("disconnected")
	}
	if !s.Offline {
		add("offline_sync_pending")
	}
	if s.Pending > 0 {
		add("storage_pending")
	}
	if s.Fault != "" && (q.From < s.Since || s.Fault == "message_storage_disabled" || s.Fault == "chat_scope_filtered") {
		add(s.Fault)
	}
	if h.fault {
		add("ledger_write_failed")
	}
	if q.From < s.Since || q.From < time.Now().Add(-maintenanceRetention).Unix() {
		add("tracking_floor")
	}
	var pending int
	if err = tx.GetContext(ctx, &pending, `SELECT COUNT(*) FROM gows_maintenance_history_jobs WHERE acked=FALSE`); err != nil {
		return nil, err
	}
	if pending > 0 {
		add("history_pending")
		var exhausted int
		if err = tx.GetContext(ctx, &exhausted, `SELECT COUNT(*) FROM gows_maintenance_history_jobs WHERE acked=FALSE AND attempts>=5 AND last_error<>''`); err != nil {
			return nil, err
		}
		if exhausted > 0 {
			add("history_retries_exhausted")
		}
		reasons := []string{}
		if err = tx.SelectContext(ctx, &reasons, `SELECT DISTINCT last_error FROM gows_maintenance_history_jobs WHERE acked=FALSE AND last_error<>'' ORDER BY last_error`); err != nil {
			return nil, err
		}
		for _, reason := range reasons {
			add(reason)
		}
	}
	var unknown int
	if err = tx.GetContext(ctx, &unknown, `SELECT COUNT(*) FROM gows_maintenance_activity WHERE `+maintenanceChatSQL("provider_jid")+` AND `+maintenanceChatSQL("jid")+` AND timestamp>=$1 AND timestamp<=$2 AND identity_known=FALSE`, q.From, q.To); err != nil {
		return nil, err
	}
	if unknown > 0 {
		add("identity_unknown")
	}
	var cursor maintenanceCursor
	if q.Cursor != "" {
		data, decodeErr := base64.RawURLEncoding.DecodeString(q.Cursor)
		if decodeErr != nil || json.Unmarshal(data, &cursor) != nil {
			return nil, errors.New("invalid maintenance cursor")
		}
	}
	if q.Token != "" && q.Token != p.Coverage.Token || q.Cursor != "" && cursor.Token != p.Coverage.Token {
		add("revision_changed")
		return p, nil
	}
	rows, err := tx.QueryxContext(ctx, `SELECT a.id,a.jid,a.provider_jid,a.timestamp,a.from_me,m.data
  FROM gows_maintenance_activity a LEFT JOIN gows_messages m ON m.id=a.id AND m.is_real=TRUE AND a.deleted=FALSE
  WHERE `+maintenanceChatSQL("a.provider_jid")+` AND `+maintenanceChatSQL("a.jid")+`
  AND (m.id IS NULL OR (`+maintenanceChatSQL("m.jid")+` AND m.jid=a.provider_jid AND `+maintenanceContentSQL(h.db.DriverName(), "m.data")+`))
  AND a.timestamp>=$1 AND a.timestamp<=$2 AND ($3='' OR a.jid=$3)
  AND ($4='' OR a.timestamp>$5 OR (a.timestamp=$5 AND a.jid>$6) OR (a.timestamp=$5 AND a.jid=$6 AND a.id>$4))
  ORDER BY a.timestamp,a.jid,a.id LIMIT $7`, q.From, q.To, q.ChatID, cursor.ID, cursor.Timestamp, cursor.JID, q.Limit+1)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var item storage.MaintenanceItem
		var data sql.NullString
		if err = rows.Scan(&item.ID, &item.JID, &item.ProviderJID, &item.Timestamp, &item.FromMe, &data); err != nil {
			rows.Close()
			return nil, err
		}
		item.Kind = "ordinary"
		item.Deleted = !data.Valid
		if data.Valid {
			var boundary struct {
				Info struct {
					ID                 string
					Chat               string
					IsNewsletterStatus bool
				}
			}
			if json.Unmarshal([]byte(data.String), &boundary) != nil || boundary.Info.ID != item.ID || boundary.Info.Chat != item.ProviderJID || !maintenanceChat(boundary.Info.Chat) || boundary.Info.IsNewsletterStatus {
				rows.Close()
				return nil, errors.New("invalid stored maintenance payload")
			}
			item.Raw = json.RawMessage(data.String)
		}
		p.Items = append(p.Items, item)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(p.Items) > q.Limit {
		p.Items = p.Items[:q.Limit]
		last := p.Items[len(p.Items)-1]
		raw, _ := json.Marshal(maintenanceCursor{Token: p.Coverage.Token, Timestamp: last.Timestamp, JID: last.JID, ID: last.ID})
		p.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
	}
	return p, nil
}

// Retain existing stored notes for candidate recovery on first installation.
// The tracking floor still prevents any retrospective quiet-chat assertion.
func (h *SQLMaintenanceHistoryStore) Seed(ctx context.Context) error {
	timestamp := `CAST(strftime('%s',timestamp) AS INTEGER)`
	if h.db.DriverName() == "postgres" {
		timestamp = `CAST(EXTRACT(EPOCH FROM timestamp) AS BIGINT)`
	}
	_, err := h.db.ExecContext(ctx, `INSERT INTO gows_maintenance_activity(id,provider_jid,jid,timestamp,from_me,identity_known,revision)
 SELECT id,jid,jid,`+timestamp+`,from_me,CASE WHEN jid LIKE '%@lid' THEN FALSE ELSE TRUE END,(SELECT revision FROM gows_maintenance_state WHERE id=1) FROM gows_messages
 WHERE is_real=TRUE AND `+maintenanceChatSQL("jid")+` AND `+maintenanceContentSQL(h.db.DriverName(), "data")+` AND timestamp >= $1 ON CONFLICT(id) DO NOTHING`, time.Now().Add(-maintenanceRetention).UTC())
	return err
}

// Retain no deleted content. An unknown target cannot prove quiet activity.
func (h *SQLMaintenanceHistoryStore) Revoke(ctx context.Context, id string) error {
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		result, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_activity SET deleted=TRUE,revision=(SELECT revision FROM gows_maintenance_state WHERE id=1) WHERE id=$1`, id)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count == 0 {
			s.Fault = "activity_unknown"
			s.Since = time.Now().Unix() + 1
		}
		return nil
	})
}

func (h *SQLMaintenanceHistoryStore) MapIdentity(ctx context.Context, lid, pn string) error {
	if !maintenanceChat(lid) || !strings.HasSuffix(lid, "@lid") || !maintenanceChat(pn) || !strings.HasSuffix(pn, "@s.whatsapp.net") {
		return errors.New("invalid maintenance identity mapping")
	}
	return h.change(ctx, func(tx *sqlx.Tx, s *maintenanceState) error {
		_, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_activity SET jid=$1,identity_known=TRUE,revision=(SELECT revision FROM gows_maintenance_state WHERE id=1) WHERE provider_jid=$2`, pn, lid)
		return err
	})
}
