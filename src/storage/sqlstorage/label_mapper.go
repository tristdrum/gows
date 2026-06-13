package sqlstorage

import (
	"encoding/json"
	"github.com/devlikeapro/gows/storage"
)

type LabelMapper struct {
}

var _ Mapper[storage.Label] = (*LabelMapper)(nil)
var labelMapper = &LabelMapper{}

func (f *LabelMapper) ToFields(entity *storage.Label) map[string]interface{} {
	return map[string]interface{}{
		"id": entity.ID,
	}
}

func (f *LabelMapper) Marshal(label *storage.Label) ([]byte, error) {
	return json.Marshal(label)
}

func (f *LabelMapper) Unmarshal(data []byte, label *storage.Label) error {
	return json.Unmarshal(data, label)
}
