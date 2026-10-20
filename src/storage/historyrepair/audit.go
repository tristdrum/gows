// Package historyrepair performs explicit, bounded repairs of message
// classification projections. It never starts a client or runs migrations.
package historyrepair

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/devlikeapro/gows/storage/sqlstorage"
	"go.mau.fi/whatsmeow/types"
)

const scanLimit = 1000

type Scope struct {
	Database      string   `json:"database"`
	Group         string   `json:"group"`
	Since         int64    `json:"since"`
	Until         int64    `json:"until"`
	MaxCandidates int      `json:"max_candidates"`
	IDs           []string `json:"ids,omitempty"`
}

type State struct {
	Data   string `json:"data"`
	IsReal bool   `json:"is_real"`
	Digest string `json:"digest"`
}

type Candidate struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Before    State     `json:"before"`
	After     State     `json:"after"`
}

type Manifest struct {
	Version int         `json:"version"`
	Scope   Scope       `json:"scope"`
	Rows    []Candidate `json:"rows"`
}

type Report struct {
	Scanned               int `json:"scanned"`
	Candidates            int `json:"candidates"`
	IndexFalsePayloadTrue int `json:"index_false_payload_true"`
	IndexTruePayloadFalse int `json:"index_true_payload_false"`
	Unresolved            int `json:"unresolved"`
	Malformed             int `json:"malformed"`
	Changed               int `json:"changed"`
	AlreadyApplied        int `json:"already_applied"`
}

func (s Scope) validate() error {
	if !filepath.IsAbs(s.Database) || s.Since <= 0 || s.Until <= s.Since || s.MaxCandidates < 1 || s.MaxCandidates > 100 {
		return errors.New("require an absolute database path, valid [since, until) window, and max-candidates in 1..100")
	}
	jid, err := types.ParseJID(s.Group)
	if err != nil || jid.Server != types.GroupServer || jid.String() != s.Group {
		return errors.New("require an exact group JID")
	}
	seen := make(map[string]bool)
	for _, id := range s.IDs {
		if id == "" || seen[id] {
			return errors.New("explicit candidate IDs must be nonempty and unique")
		}
		seen[id] = true
	}
	if len(s.IDs) > 100 {
		return errors.New("at most 100 explicit candidate IDs are allowed")
	}
	return nil
}

func openDatabase(path string, readOnly bool) (*sql.DB, error) {
	mode := "rw" // Never create a database when an operator supplied the wrong path.
	if readOnly {
		mode = "ro"
	}
	address := &url.URL{Scheme: "file", Path: path}
	query := url.Values{"mode": {mode}, "_busy_timeout": {"5000"}}
	if readOnly {
		query.Set("_query_only", "1")
	}
	address.RawQuery = query.Encode()
	db, err := sql.Open("sqlite3", address.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func newState(raw string, real bool) State {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%t\n%s", real, raw)))
	return State{Data: raw, IsReal: real, Digest: hex.EncodeToString(digest[:])}
}

// repairedState accepts only intact real content. A non-real payload cannot
// reconstruct lost content and is never demoted or otherwise rewritten.
func repairedState(id string, timestamp time.Time, scope Scope, before State) (State, bool, error) {
	var message storage.StoredMessage
	if err := (&sqlstorage.MessageMapper{}).Unmarshal([]byte(before.Data), &message); err != nil {
		return State{}, false, errors.New("malformed stored message")
	}
	if message.Info.ID != id || message.Info.Chat.String() != scope.Group || !message.Info.Timestamp.Equal(timestamp) {
		return State{}, false, errors.New("stored identity or timestamp disagrees with its row")
	}
	if !storage.IsRealMessage(message.Message) {
		return State{}, false, nil
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal([]byte(before.Data), &payload); err != nil {
		return State{}, false, errors.New("malformed stored message")
	}
	raw := before.Data
	if string(payload["IsReal"]) != "true" {
		payload["IsReal"] = json.RawMessage("true")
		encoded, err := json.Marshal(payload)
		if err != nil {
			return State{}, false, err
		}
		raw = string(encoded)
	}
	return newState(raw, true), true, nil
}

func Audit(scope Scope) (Manifest, Report, error) {
	manifest := Manifest{Version: 1, Scope: scope, Rows: []Candidate{}}
	report := Report{}
	if err := scope.validate(); err != nil {
		return manifest, report, err
	}
	db, err := openDatabase(scope.Database, true)
	if err != nil {
		return manifest, report, err
	}
	defer db.Close()
	query := "SELECT id, timestamp, is_real, data FROM gows_messages WHERE jid = ? AND timestamp >= ? AND timestamp < ?"
	args := []any{scope.Group, time.Unix(scope.Since, 0).UTC(), time.Unix(scope.Until, 0).UTC()}
	if len(scope.IDs) > 0 {
		query += " AND id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(scope.IDs)), ",") + ")"
		for _, id := range scope.IDs {
			args = append(args, id)
		}
	}
	query += " ORDER BY timestamp, id LIMIT 1001"
	rows, err := db.Query(query, args...)
	if err != nil {
		return manifest, report, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, raw string
		var timestamp time.Time
		var indexed bool
		if err := rows.Scan(&id, &timestamp, &indexed, &raw); err != nil {
			return manifest, report, err
		}
		report.Scanned++
		if report.Scanned > scanLimit {
			return Manifest{}, report, errors.New("audit source window exceeds 1000 rows; narrow the scope")
		}
		before := newState(raw, indexed)
		after, real, err := repairedState(id, timestamp, scope, before)
		if err != nil {
			report.Malformed++
			continue
		}
		var payload struct{ IsReal bool }
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			report.Malformed++
			continue
		}
		if !indexed && payload.IsReal {
			report.IndexFalsePayloadTrue++
		}
		if indexed && !payload.IsReal {
			report.IndexTruePayloadFalse++
		}
		if !real {
			if indexed || payload.IsReal {
				report.Unresolved++
			}
			continue
		}
		if before == after {
			continue
		}
		report.Candidates++
		if report.Candidates > scope.MaxCandidates {
			return Manifest{}, report, errors.New("candidate cap exceeded; narrow the scope")
		}
		manifest.Rows = append(manifest.Rows, Candidate{ID: id, Timestamp: timestamp, Before: before, After: after})
	}
	if err := rows.Err(); err != nil {
		return Manifest{}, report, err
	}
	if len(scope.IDs) > 0 && report.Scanned != len(scope.IDs) {
		return Manifest{}, report, errors.New("one or more explicit IDs are absent from the exact group/window")
	}
	return manifest, report, nil
}
