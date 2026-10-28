package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
	"go.mau.fi/whatsmeow/types"
)

func indexUnreadStates(ctx context.Context, tx *sqlx.Tx, candidates []indexMessageIdentity, aliases indexAliases) (map[string]*storage.StoredChatReadState, map[string]uint64, error) {
	wanted := make(map[string]bool, len(candidates))
	aliasToCanonical := make(map[string]string, len(candidates))
	for _, item := range candidates {
		wanted[item.Canonical] = true
		aliasToCanonical[item.Canonical] = item.Canonical
	}
	for alias, canonical := range aliases {
		if wanted[canonical] {
			aliasToCanonical[alias] = canonical
		}
	}
	states, err := loadIndexReadStates(ctx, tx, aliasToCanonical)
	if err != nil {
		return nil, nil, err
	}
	counts, err := countIndexInbound(ctx, tx, states, aliasToCanonical)
	return states, counts, err
}

func loadIndexReadStates(ctx context.Context, tx *sqlx.Tx, aliases map[string]string) (map[string]*storage.StoredChatReadState, error) {
	result := make(map[string]*storage.StoredChatReadState)
	invalid := make(map[string]bool)
	keys := make([]string, 0, len(aliases))
	for alias := range aliases {
		keys = append(keys, alias)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return result, nil
	}
	query, args, err := sq.Select("jid", "baseline_unread_count", "marked_as_unread", "unread_state_known", "count_from_timestamp", "covered_message_ids", "evidence_timestamp").From("gows_chat_read_state").Where(sq.Eq{"jid": keys}).OrderBy("jid").ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var rawJID, coveredJSON string
		var baseline, countFrom sql.NullInt64
		var marked, known bool
		var evidence int64
		if err := rows.Scan(&rawJID, &baseline, &marked, &known, &countFrom, &coveredJSON, &evidence); err != nil {
			return nil, err
		}
		canonical, err := types.ParseJID(aliases[rawJID])
		if err != nil {
			return nil, storage.ErrChatIndexDataUnavailable
		}
		var covered []string
		if err := json.Unmarshal([]byte(coveredJSON), &covered); err != nil || (baseline.Valid && baseline.Int64 < 0) {
			// Read-ahead metadata is not a consumed result. Preserve unknown
			// state so only an unread scan that reaches this chat fails.
			invalid[canonical.String()] = true
			known = false
			baseline = sql.NullInt64{}
			covered = nil
		}
		state := &storage.StoredChatReadState{
			Jid: canonical, MarkedAsUnread: marked, UnreadStateKnown: known && baseline.Valid && countFrom.Valid,
			CoveredMessageIDs: storage.NormalizeCoveredMessageIDs(covered), EvidenceTimestamp: time.UnixMilli(evidence),
		}
		if baseline.Valid {
			state.BaselineUnreadCount = uint64(baseline.Int64)
		}
		if countFrom.Valid {
			state.CountFrom = time.UnixMilli(countFrom.Int64)
		}
		result[canonical.String()] = mergeChatReadStates(result[canonical.String()], state, canonical)
	}
	for canonical := range invalid {
		if state := result[canonical]; state != nil {
			state.UnreadStateKnown = false
		}
	}
	return result, rows.Err()
}

func countIndexInbound(ctx context.Context, tx *sqlx.Tx, states map[string]*storage.StoredChatReadState, aliases map[string]string) (map[string]uint64, error) {
	counts := make(map[string]uint64, len(states))
	keys := make([]string, 0, len(aliases))
	for alias, canonical := range aliases {
		if state := states[canonical]; state != nil && state.UnreadStateKnown {
			keys = append(keys, alias)
			counts[canonical] = 0
		}
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return counts, nil
	}
	values, args := make([]string, 0, len(keys)), make([]any, 0, len(keys)*3)
	for _, alias := range keys {
		canonical := aliases[alias]
		n := len(args) + 1
		timeValue := fmt.Sprintf("$%d", n+2)
		if tx.DriverName() == "postgres" {
			timeValue += "::timestamp"
		}
		values = append(values, fmt.Sprintf("($%d, $%d, %s)", n, n+1, timeValue))
		args = append(args, alias, canonical, states[canonical].CountFrom.UTC())
	}
	cte := "WITH bounds(jid, canonical, cutoff) AS (VALUES " + strings.Join(values, ",") + ") "
	join := "FROM bounds b JOIN gows_messages m ON m.jid = b.jid WHERE m.is_real = true AND m.from_me = false "
	rows, err := tx.QueryxContext(ctx, cte+"SELECT b.canonical, COUNT(*) "+join+"AND m.timestamp > b.cutoff GROUP BY b.canonical", args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var canonical string
		var count uint64
		if err := rows.Scan(&canonical, &count); err != nil {
			rows.Close()
			return nil, err
		}
		counts[canonical] = count
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	// Match the existing exact-second boundary rule. Covered message IDs are
	// evidence; an uncovered same-second message makes the count unknown.
	rows, err = tx.QueryxContext(ctx, cte+"SELECT b.canonical, m.id "+join+"AND m.timestamp = b.cutoff", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	covered := make(map[string]map[string]bool)
	for canonical, state := range states {
		covered[canonical] = make(map[string]bool, len(state.CoveredMessageIDs))
		for _, id := range state.CoveredMessageIDs {
			covered[canonical][id] = true
		}
	}
	for rows.Next() {
		var canonical, id string
		if err := rows.Scan(&canonical, &id); err != nil {
			return nil, err
		}
		if !covered[canonical][id] {
			delete(counts, canonical)
		}
	}
	return counts, rows.Err()
}
