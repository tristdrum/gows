package sqlstorage

import (
	"encoding/json"
	"go.mau.fi/whatsmeow/types"
)

type GroupMapper struct {
}

var _ Mapper[types.GroupInfo] = (*GroupMapper)(nil)
var groupMapper = &GroupMapper{}

func (f *GroupMapper) ToFields(entity *types.GroupInfo) map[string]interface{} {
	return map[string]interface{}{
		"id":   entity.JID,
		"name": entity.Name,
	}
}

func (f *GroupMapper) Marshal(entity *types.GroupInfo) ([]byte, error) {
	return json.Marshal(entity)
}

func (f *GroupMapper) Unmarshal(data []byte, entity *types.GroupInfo) error {
	return json.Unmarshal(data, entity)
}
