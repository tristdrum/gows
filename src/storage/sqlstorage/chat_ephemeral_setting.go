package sqlstorage

import (
	"errors"
	"time"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
)

func (gc *GContainer) NewChatEphemeralSettingStorage() *SqlChatEphemeralSettingStore {
	repo := NewEntityRepository[storage.StoredChatEphemeralSetting](
		gc.db,
		ChatEphemeralSettingsTable,
		chatEphemeralSettingMapper,
	)
	return &SqlChatEphemeralSettingStore{
		repo,
	}
}

type SqlChatEphemeralSettingStore struct {
	*EntityRepository[storage.StoredChatEphemeralSetting]
}

var _ storage.ChatEphemeralSettingStorage = (*SqlChatEphemeralSettingStore)(nil)

func (s *SqlChatEphemeralSettingStore) GetChatEphemeralSetting(id types.JID) (*storage.StoredChatEphemeralSetting, error) {
	return s.GetById(id.String())
}

func (s *SqlChatEphemeralSettingStore) UpdateChatEphemeralSetting(setting *storage.StoredChatEphemeralSetting) error {
	if setting.IsEphemeral {
		return s.UpsertOne(setting)
	} else {
		return s.DeleteChatEphemeralSetting(setting.ID, time.Now())
	}
}

func (s *SqlChatEphemeralSettingStore) DeleteChatEphemeralSetting(id types.JID, deleteBefore time.Time) error {
	// First get the current setting to check its timestamp
	setting, err := s.GetChatEphemeralSetting(id)
	if errors.Is(err, storage.ErrNotFound) {
		// Totally fine, already got removed
		return nil
	}
	if err != nil {
		return err
	}
	if setting == nil {
		return nil
	}
	if setting.Setting == nil || setting.Setting.Timestamp == nil {
		return s.DeleteById(id.String())
	}
	// Only delete if the setting is older than the delete event
	if *setting.Setting.Timestamp < deleteBefore.Unix() {
		return s.DeleteById(id.String())
	}
	return nil
}
