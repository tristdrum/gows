package sqlstorage

import (
	"context"
	sq "github.com/Masterminds/squirrel"
	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
	"go.mau.fi/whatsmeow/types"
)

var _ storage.LidmapStorage = (*SqlLidmapStorage)(nil)

func (gc *GContainer) NewLidmapStorage() *SqlLidmapStorage {
	return &SqlLidmapStorage{
		db: gc.db,
	}
}

type SqlLidmapStorage struct {
	db *sqlx.DB
}

// GetAllLidMap returns all lid/pn pairs from the database as an array of LidmapEntry
func (s SqlLidmapStorage) GetAllLidMap() ([]storage.LidmapEntry, error) {
	query, args, err := sq.Select("lid", "pn").
		From("whatsmeow_lid_map").
		OrderBy("lid ASC").
		ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []storage.LidmapEntry
	for rows.Next() {
		var lidStr, pnStr string
		if err := rows.Scan(&lidStr, &pnStr); err != nil {
			return nil, err
		}
		lid := types.JID{User: lidStr, Server: types.HiddenUserServer}
		pn := types.JID{User: pnStr, Server: types.DefaultUserServer}
		result = append(result, storage.LidmapEntry{
			Lid: lid,
			Pn:  pn,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// GetLidCount returns the count of lids in the database
func (s SqlLidmapStorage) GetLidCount() (int, error) {
	query, args, err := sq.Select("COUNT(lid)").
		From("whatsmeow_lid_map").
		ToSql()
	if err != nil {
		return 0, err
	}

	var count int
	err = s.db.QueryRowContext(context.Background(), query, args...).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}
