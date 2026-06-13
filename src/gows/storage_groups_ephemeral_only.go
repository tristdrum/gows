package gows

import (
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"time"
)

type GroupEphemeralOnlyStorage struct {
	chatEphemeralSetting storage.ChatEphemeralSettingStorage
	log                  waLog.Logger
}

func NewGroupEphemeralOnlyStorage(gows *GoWS, chatEphemeralSetting storage.ChatEphemeralSettingStorage) *GroupEphemeralOnlyStorage {
	return &GroupEphemeralOnlyStorage{
		chatEphemeralSetting: chatEphemeralSetting,
		log:                  gows.Log.Sub("GroupEphemeralStorage"),
	}
}

var _ storage.GroupStorage = (*GroupEphemeralOnlyStorage)(nil)

func (g *GroupEphemeralOnlyStorage) FetchGroups(force bool) error {
	return nil
}

func (g *GroupEphemeralOnlyStorage) UpdateGroup(update *events.GroupInfo) error {
	setting := ExtractEphemeralSettingsFromGroupUpdate(update)
	if setting == nil {
		return nil
	}
	err := g.chatEphemeralSetting.UpdateChatEphemeralSetting(setting)
	if err != nil {
		g.log.Warnf("Updating chat ephemeral setting for group failed %v: %v", setting.ID, err)
	}
	return nil
}

func (g *GroupEphemeralOnlyStorage) UpsertOneGroup(group *types.GroupInfo) error {
	setting := ExtractEphemeralSettingsFromGroup(group)
	err := g.chatEphemeralSetting.UpdateChatEphemeralSetting(setting)
	if err != nil {
		g.log.Warnf("Upserting chat ephemeral setting for group failed %v: %v", setting.ID, err)
	}
	return nil
}

func (g *GroupEphemeralOnlyStorage) GetAllGroups(sort storage.Sort, pagination storage.Pagination) ([]*types.GroupInfo, error) {
	return []*types.GroupInfo{}, nil
}

func (g *GroupEphemeralOnlyStorage) GetGroup(jid types.JID) (*types.GroupInfo, error) {
	return nil, storage.StorageDisabled("groups")
}

func (g *GroupEphemeralOnlyStorage) DeleteGroup(jid types.JID) error {
	err := g.chatEphemeralSetting.DeleteChatEphemeralSetting(jid, time.Now())
	if err != nil {
		g.log.Warnf("Deleting chat ephemeral setting for group failed %v: %v", jid, err)
	}
	return nil
}

func (g *GroupEphemeralOnlyStorage) DeleteGroups() error {
	return nil
}
