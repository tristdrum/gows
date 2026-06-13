package sqlstorage

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/devlikeapro/gows/storage"
)

type SqlLabelStore struct {
	*EntityRepository[storage.Label]
}

var _ storage.LabelStorage = (*SqlLabelStore)(nil)

func (gc *GContainer) NewLabelStorage() *SqlLabelStore {
	repo := NewEntityRepository[storage.Label](
		gc.db,
		LabelsTable,
		labelMapper,
	)
	return &SqlLabelStore{
		repo,
	}
}

func (s SqlLabelStore) GetAllLabels() ([]*storage.Label, error) {
	conditions := make([]sq.Sqlizer, 0)
	return s.AllBy(conditions)
}

func (s SqlLabelStore) GetLabelById(id string) (*storage.Label, error) {
	return s.GetById(id)
}

func (s SqlLabelStore) UpsertLabel(label *storage.Label) error {
	return s.UpsertOne(label)
}

func (s SqlLabelStore) DeleteLabel(id string) error {
	return s.DeleteById(id)
}
