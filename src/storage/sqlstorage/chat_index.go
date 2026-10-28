package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
	"go.mau.fi/whatsmeow/types"
)

type indexMessageIdentity struct {
	ID        string
	JID       string
	Timestamp time.Time
	Canonical string
}

type indexAliases map[string]string

func (a indexAliases) canonical(jid string) string {
	if mapped := a[jid]; mapped != "" {
		return mapped
	}
	return jid
}

func loadIndexAliases(ctx context.Context, tx *sqlx.Tx) (indexAliases, error) {
	rows, err := tx.QueryxContext(ctx, "SELECT lid, pn FROM whatsmeow_lid_map ORDER BY lid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	aliases := make(indexAliases)
	for rows.Next() {
		var lid, pn string
		if err := rows.Scan(&lid, &pn); err != nil {
			return nil, err
		}
		if pn != "" {
			aliases[lid+"@lid"] = pn + "@s.whatsapp.net"
		}
	}
	return aliases, rows.Err()
}

func latestIndexIdentitiesSQL(driver string) string {
	if driver == "postgres" {
		return "SELECT DISTINCT ON (jid) id, jid, timestamp FROM gows_messages WHERE is_real = true AND jid <> 'status@broadcast' ORDER BY jid, timestamp DESC, id ASC"
	}
	return "SELECT id, jid, timestamp FROM (SELECT id, jid, timestamp, ROW_NUMBER() OVER (PARTITION BY jid ORDER BY timestamp DESC, id ASC) AS rn FROM gows_messages WHERE is_real = true AND jid <> 'status@broadcast') AS latest WHERE rn = 1"
}

func latestIndexIdentities(ctx context.Context, tx *sqlx.Tx, aliases indexAliases) ([]indexMessageIdentity, error) {
	rows, err := tx.QueryxContext(ctx, latestIndexIdentitiesSQL(tx.DriverName()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	latest := make(map[string]indexMessageIdentity)
	for rows.Next() {
		var item indexMessageIdentity
		if err := rows.Scan(&item.ID, &item.JID, &item.Timestamp); err != nil {
			return nil, err
		}
		item.Canonical = aliases.canonical(item.JID)
		old, exists := latest[item.Canonical]
		// Keep the existing preference for a PN representation at equal time, then
		// use message identity for deterministic ties within the same representation.
		betterTie := (old.JID != old.Canonical && item.JID == item.Canonical) ||
			((old.JID == old.Canonical) == (item.JID == item.Canonical) && item.ID < old.ID)
		if !exists || item.Timestamp.After(old.Timestamp) || (item.Timestamp.Equal(old.Timestamp) && betterTie) {
			latest[item.Canonical] = item
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]indexMessageIdentity, 0, len(latest))
	for _, item := range latest {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Timestamp.Equal(result[j].Timestamp) {
			return result[i].Canonical < result[j].Canonical
		}
		return result[i].Timestamp.After(result[j].Timestamp)
	})
	return result, ctx.Err()
}

func (s SqlMessageStore) GetChatIndex(ctx context.Context, pagination storage.Pagination, unreadOnly bool) (*storage.ChatIndexPage, error) {
	if pagination.Limit < 1 || pagination.Limit > 50 {
		return nil, fmt.Errorf("chat index limit must be 1 to 50")
	}
	opts := &sql.TxOptions{ReadOnly: true}
	if s.db.DriverName() == "postgres" {
		opts.Isolation = sql.LevelRepeatableRead
	}
	tx, err := s.db.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	aliases, err := loadIndexAliases(ctx, tx)
	if err != nil {
		return nil, err
	}
	identities, err := latestIndexIdentities(ctx, tx, aliases)
	if err != nil {
		return nil, err
	}
	page := &storage.ChatIndexPage{
		Chats:      []storage.ChatIndexEntry{},
		SourcePage: storage.ChatIndexSourcePage{Version: 1, Offset: pagination.Offset, Limit: pagination.Limit},
	}
	if pagination.Offset >= uint64(len(identities)) {
		page.SourcePage.Complete = true
		return page, tx.Commit()
	}
	scanLimit := pagination.Limit
	if unreadOnly {
		scanLimit = storage.MaxChatIndexScan
	}
	end := min(uint64(len(identities)), pagination.Offset+scanLimit)
	candidates := identities[pagination.Offset:end]
	states, counts, err := indexUnreadStates(ctx, tx, candidates, aliases)
	if err != nil {
		return nil, err
	}
	selected := make([]indexMessageIdentity, 0, pagination.Limit)
	for _, item := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page.SourcePage.ScannedCount++
		state := states[item.Canonical]
		delta, exact := counts[item.Canonical]
		known := state != nil && state.UnreadStateKnown && exact
		if unreadOnly && !known {
			return nil, storage.ErrUnreadStateUnavailable
		}
		if unreadOnly && !state.MarkedAsUnread && state.BaselineUnreadCount+delta == 0 {
			continue
		}
		selected = append(selected, item)
		if len(selected) == int(pagination.Limit) {
			break
		}
	}
	next := pagination.Offset + page.SourcePage.ScannedCount
	page.SourcePage.Complete = next >= uint64(len(identities))
	if !page.SourcePage.Complete {
		page.SourcePage.NextOffset = &next
	}
	entries, err := indexPayloads(ctx, tx, selected, states, counts)
	if err != nil {
		return nil, err
	}
	page.Chats = entries
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return page, tx.Commit()
}

func indexPayloads(ctx context.Context, tx *sqlx.Tx, selected []indexMessageIdentity, states map[string]*storage.StoredChatReadState, counts map[string]uint64) ([]storage.ChatIndexEntry, error) {
	entries := make([]storage.ChatIndexEntry, 0, len(selected))
	if len(selected) == 0 {
		return entries, nil
	}
	ids, groups := make([]string, 0, len(selected)), []string{}
	for _, item := range selected {
		ids = append(ids, item.ID)
		if strings.HasSuffix(item.Canonical, "@g.us") {
			groups = append(groups, item.Canonical)
		}
	}
	query, args, err := sq.Select("id", "data").From("gows_messages").Where(sq.Eq{"id": ids}).ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	payloads := make(map[string]*storage.StoredMessage, len(ids))
	for rows.Next() {
		var id, data string
		if err := rows.Scan(&id, &data); err != nil {
			rows.Close()
			return nil, err
		}
		var message storage.StoredMessage
		if err := messageMapper.Unmarshal([]byte(data), &message); err != nil || message.Message == nil || !storage.IsRealMessage(message.Message) {
			rows.Close()
			return nil, storage.ErrChatIndexDataUnavailable
		}
		payloads[id] = &message
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	names, err := indexGroupNames(ctx, tx, groups)
	if err != nil {
		return nil, err
	}
	for _, item := range selected {
		message := payloads[item.ID]
		if message == nil || message.Info.ID != item.ID || message.Info.Chat.String() != item.JID || !message.Info.Timestamp.Equal(item.Timestamp) {
			return nil, storage.ErrChatIndexDataUnavailable
		}
		jid, err := types.ParseJID(item.Canonical)
		if err != nil {
			return nil, storage.ErrChatIndexDataUnavailable
		}
		message.Info.Chat = jid
		chat := &storage.StoredChat{Jid: jid, Name: names[item.Canonical], ConversationTimestamp: item.Timestamp}
		if state := states[item.Canonical]; state != nil {
			delta, exact := counts[item.Canonical]
			chat.MarkedAsUnread = state.MarkedAsUnread
			chat.UnreadStateKnown = state.UnreadStateKnown && exact
			if chat.UnreadStateKnown {
				chat.UnreadCount = state.BaselineUnreadCount + delta
			}
		}
		entries = append(entries, storage.ChatIndexEntry{Chat: chat, Message: message})
	}
	return entries, nil
}

func indexGroupNames(ctx context.Context, tx *sqlx.Tx, groups []string) (map[string]string, error) {
	names := make(map[string]string, len(groups))
	if len(groups) == 0 {
		return names, nil
	}
	query, args, err := sq.Select("id", "name").From("gows_groups").Where(sq.Eq{"id": groups}).ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		names[id] = name
	}
	return names, rows.Err()
}
