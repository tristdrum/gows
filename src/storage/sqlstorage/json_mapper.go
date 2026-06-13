package sqlstorage

import (
	"encoding/json"
	"github.com/devlikeapro/gows/storage"
)

type JsonMapper struct {
}

func (f *JsonMapper) Marshal(entity *storage.StoredMessage) ([]byte, error) {
	return json.Marshal(entity)
}

func (f *JsonMapper) Unmarshal(data []byte, entity *storage.StoredMessage) error {
	return json.Unmarshal(data, entity)
}
