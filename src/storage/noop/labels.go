package noop

import "github.com/devlikeapro/gows/storage"

type LabelStorage struct{}

var _ storage.LabelStorage = (*LabelStorage)(nil)

func NewLabelStorage() *LabelStorage {
	return &LabelStorage{}
}

func (s LabelStorage) GetAllLabels() ([]*storage.Label, error) {
	return []*storage.Label{}, nil
}

func (s LabelStorage) GetLabelById(id string) (*storage.Label, error) {
	return nil, storage.StorageDisabled("labels")
}

func (s LabelStorage) UpsertLabel(label *storage.Label) error {
	return nil
}

func (s LabelStorage) DeleteLabel(id string) error {
	return nil
}
