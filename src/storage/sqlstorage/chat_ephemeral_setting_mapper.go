package sqlstorage

import (
	"encoding/json"
	"github.com/devlikeapro/gows/storage"
)

type ChatEphemeralSettingMapper struct {
}

var _ Mapper[storage.StoredChatEphemeralSetting] = (*ChatEphemeralSettingMapper)(nil)
var chatEphemeralSettingMapper = &ChatEphemeralSettingMapper{}

func (f *ChatEphemeralSettingMapper) ToFields(entity *storage.StoredChatEphemeralSetting) map[string]interface{} {
	return map[string]interface{}{
		"id": entity.ID,
	}
}
func (f *ChatEphemeralSettingMapper) Marshal(entity *storage.StoredChatEphemeralSetting) ([]byte, error) {
	return json.Marshal(entity)
}

func (f *ChatEphemeralSettingMapper) Unmarshal(data []byte, entity *storage.StoredChatEphemeralSetting) error {
	return json.Unmarshal(data, entity)
}
