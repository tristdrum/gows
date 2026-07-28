package sqlstorage

import (
	"database/sql"
	"errors"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"go.mau.fi/whatsmeow/types"
)

func canonicalizeJID(db *sqlx.DB, jid types.JID) (types.JID, error) {
	if jid.Server != types.HiddenUserServer {
		return jid, nil
	}
	query, args, err := sq.Select("pn").
		From("whatsmeow_lid_map").
		Where(sq.Eq{"lid": jid.User}).
		Limit(1).
		ToSql()
	if err != nil {
		return types.JID{}, err
	}
	var pn string
	err = db.Get(&pn, query, args...)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return jid, nil
	case err != nil:
		return types.JID{}, err
	case pn == "":
		return jid, nil
	}
	return types.JID{User: pn, Server: types.DefaultUserServer, Device: jid.Device}, nil
}

func reverseLookupLIDs(db *sqlx.DB, pn string) ([]string, error) {
	query, args, err := sq.Select("lid").From("whatsmeow_lid_map").Where(sq.Eq{"pn": pn}).ToSql()
	if err != nil {
		return nil, err
	}
	var lids []string
	if err := db.Select(&lids, query, args...); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(lids))
	for _, lid := range lids {
		result = append(result, lid+"@"+types.HiddenUserServer)
	}
	return result, nil
}
