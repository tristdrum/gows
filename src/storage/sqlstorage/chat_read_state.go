package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
	"go.mau.fi/whatsmeow/types"
)

type SqlChatReadStateStore struct {
	db *sqlx.DB
}

var _ storage.ChatReadStateStorage = (*SqlChatReadStateStore)(nil)

func (gc *GContainer) NewChatReadStateStorage() *SqlChatReadStateStore {
	return &SqlChatReadStateStore{db: gc.db}
}

func (s SqlChatReadStateStore) UpsertChatReadState(state *storage.StoredChatReadState) (bool, error) {
	canonical, err := canonicalizeJID(s.db, state.Jid)
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(
		context.Background(),
		`INSERT INTO gows_chat_read_state (
            jid, baseline_unread_count, marked_as_unread, unread_state_known,
            count_from_timestamp, evidence_timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (jid) DO UPDATE SET
            baseline_unread_count = CASE
                WHEN excluded.unread_state_known THEN excluded.baseline_unread_count
                ELSE gows_chat_read_state.baseline_unread_count
            END,
            marked_as_unread = excluded.marked_as_unread,
            unread_state_known = CASE
                WHEN excluded.unread_state_known THEN TRUE
                ELSE gows_chat_read_state.unread_state_known
            END,
            count_from_timestamp = CASE
                WHEN excluded.unread_state_known THEN excluded.count_from_timestamp
                ELSE gows_chat_read_state.count_from_timestamp
            END,
            evidence_timestamp = excluded.evidence_timestamp
        WHERE gows_chat_read_state.evidence_timestamp < excluded.evidence_timestamp`,
		canonical.String(),
		state.BaselineUnreadCount,
		state.MarkedAsUnread,
		state.UnreadStateKnown,
		state.CountFrom.UnixMilli(),
		state.EvidenceTimestamp.UnixMilli(),
	)
	if err != nil {
		return false, err
	}
	return rowsWereChanged(result)
}

func (s SqlChatReadStateStore) ApplyChatReadEvent(event storage.ChatReadEvent) (bool, error) {
	canonical, err := canonicalizeJID(s.db, event.Jid)
	if err != nil {
		return false, err
	}
	if event.Read {
		return s.UpsertChatReadState(&storage.StoredChatReadState{
			Jid:                 canonical,
			BaselineUnreadCount: 0,
			MarkedAsUnread:      false,
			UnreadStateKnown:    true,
			CountFrom:           event.MessageWatermark,
			EvidenceTimestamp:   event.Timestamp,
		})
	}

	loaded, err := s.GetChatReadStates([]types.JID{canonical}, true)
	if err != nil {
		return false, err
	}
	state := loaded[canonical.String()]
	if state != nil && !event.Timestamp.After(state.EvidenceTimestamp) {
		return false, nil
	}
	if state == nil {
		state = &storage.StoredChatReadState{
			Jid: canonical,
		}
	}
	if !state.UnreadStateKnown {
		state.BaselineUnreadCount = 0
		state.CountFrom = event.MessageWatermark
		state.UnreadStateKnown = true
	}
	state.Jid = canonical
	state.MarkedAsUnread = true
	state.EvidenceTimestamp = event.Timestamp
	return s.UpsertChatReadState(state)
}

func rowsWereChanged(result sql.Result) (bool, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s SqlChatReadStateStore) GetChatReadStates(jids []types.JID, merge bool) (map[string]*storage.StoredChatReadState, error) {
	result := make(map[string]*storage.StoredChatReadState, len(jids))
	if len(jids) == 0 {
		return result, nil
	}

	aliasToCanonical := make(map[string]types.JID)
	aliases := make([]string, 0, len(jids))
	for _, jid := range jids {
		canonical := jid
		if merge {
			var err error
			canonical, err = canonicalizeJID(s.db, jid)
			if err != nil {
				return nil, err
			}
		}
		chatAliases := []string{canonical.String()}
		if merge {
			lids, err := reverseLookupLIDs(s.db, canonical.User)
			if err != nil {
				return nil, err
			}
			chatAliases = append(chatAliases, lids...)
		}
		for _, alias := range chatAliases {
			if _, exists := aliasToCanonical[alias]; exists {
				continue
			}
			aliasToCanonical[alias] = canonical
			aliases = append(aliases, alias)
		}
	}

	query, args, err := sq.Select(
		"jid",
		"baseline_unread_count",
		"marked_as_unread",
		"unread_state_known",
		"count_from_timestamp",
		"evidence_timestamp",
	).From("gows_chat_read_state").Where(sq.Eq{"jid": aliases}).ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var rawJID string
		var baseline uint64
		var marked, known bool
		var countFromMillis, evidenceMillis int64
		if err := rows.Scan(&rawJID, &baseline, &marked, &known, &countFromMillis, &evidenceMillis); err != nil {
			return nil, err
		}
		canonical, ok := aliasToCanonical[rawJID]
		if !ok {
			return nil, fmt.Errorf("chat read state returned unexpected jid %s", rawJID)
		}
		state := &storage.StoredChatReadState{
			Jid:                 canonical,
			BaselineUnreadCount: baseline,
			MarkedAsUnread:      marked,
			UnreadStateKnown:    known,
			CountFrom:           time.UnixMilli(countFromMillis),
			EvidenceTimestamp:   time.UnixMilli(evidenceMillis),
		}
		key := canonical.String()
		result[key] = mergeChatReadStates(result[key], state, canonical)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func mergeChatReadStates(existing, candidate *storage.StoredChatReadState, canonical types.JID) *storage.StoredChatReadState {
	if existing == nil {
		candidate.Jid = canonical
		return candidate
	}
	newer, older := candidate, existing
	if existing.EvidenceTimestamp.After(candidate.EvidenceTimestamp) ||
		(existing.EvidenceTimestamp.Equal(candidate.EvidenceTimestamp) && existing.UnreadStateKnown && !candidate.UnreadStateKnown) {
		newer, older = existing, candidate
	}
	merged := *newer
	merged.Jid = canonical
	if !merged.UnreadStateKnown && older.UnreadStateKnown {
		merged.BaselineUnreadCount = older.BaselineUnreadCount
		merged.CountFrom = older.CountFrom
		merged.UnreadStateKnown = true
	}
	return &merged
}

func (s SqlChatReadStateStore) DeleteChatReadState(jid types.JID) error {
	canonical, err := canonicalizeJID(s.db, jid)
	if err != nil {
		return err
	}
	aliases := []string{canonical.String()}
	lids, err := reverseLookupLIDs(s.db, canonical.User)
	if err != nil {
		return err
	}
	aliases = append(aliases, lids...)
	query, args, err := sq.Delete("gows_chat_read_state").Where(sq.Eq{"jid": aliases}).ToSql()
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(context.Background(), query, args...)
	return err
}
