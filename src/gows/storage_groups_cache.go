package gows

import (
	"github.com/devlikeapro/gows/storage"
	"github.com/devlikeapro/gows/storage/helpers"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"sync"
	"time"
)

const refreshInterval = 24 * time.Hour

type GroupCacheStorage struct {
	gows                 *GoWS
	groups               storage.GroupStorage
	chatEphemeralSetting storage.ChatEphemeralSettingStorage

	lastTimeRefreshed time.Time
	lock              sync.Mutex

	log waLog.Logger
}

func NewGroupCacheStorage(gows *GoWS, groups storage.GroupStorage, chatEphemeralSetting storage.ChatEphemeralSettingStorage) *GroupCacheStorage {
	return &GroupCacheStorage{
		groups:               groups,
		gows:                 gows,
		chatEphemeralSetting: chatEphemeralSetting,
		log:                  gows.Log.Sub("GroupCacheStorage"),
	}
}

var _ storage.GroupStorage = (*GroupCacheStorage)(nil)

func (g *GroupCacheStorage) shouldRefresh() bool {
	return time.Since(g.lastTimeRefreshed) > refreshInterval
}

func (g *GroupCacheStorage) FetchGroups(force bool) error {
	if force {
		g.lock.Lock()
		defer g.lock.Unlock()
		return g.fetchGroupsUnlocked()
	}
	_, err := g.fetchGroupsIfNeeded(true)
	return err
}

func (g *GroupCacheStorage) UpdateGroup(update *events.GroupInfo) (err error) {
	g.lock.Lock()
	defer g.lock.Unlock()
	refreshed, err := g.fetchGroupsIfNeeded(false)
	if err != nil {
		return err
	}
	if refreshed {
		g.log.Debugf("Groups refreshed, skipping update of %s", update.JID)
		return nil
	}
	group, err := g.groups.GetGroup(update.JID)
	if err != nil {
		return err
	}
	err = helpers.UpdateGroupInfo(group, update)
	if err != nil {
		return err
	}
	if update.Ephemeral != nil {
		setting := ExtractEphemeralSettingsFromGroup(group)
		err = g.chatEphemeralSetting.UpdateChatEphemeralSetting(setting)
		if err != nil {
			g.log.Warnf("Updating chat ephemeral setting for group failed %v: %v", setting.ID, err)
		}
	}
	return g.groups.UpsertOneGroup(group)
}

func (g *GroupCacheStorage) fetchGroupsUnlocked() error {
	g.log.Debugf("Refreshing groups")
	groups, err := g.gows.GetJoinedGroups(g.gows.Context)
	if err != nil {
		return err
	}
	err = g.groups.DeleteGroups()
	if err != nil {
		return err
	}
	for _, group := range groups {
		err = g.UpsertOneGroup(group)
		if err != nil {
			g.log.Errorf("Error upserting group %s: %v", group.JID, err)
		}
	}
	g.lastTimeRefreshed = time.Now()
	g.log.Debugf("Groups refreshed")
	return nil

}

func (g *GroupCacheStorage) fetchGroupsIfNeeded(lock bool) (bool, error) {
	if lock {
		g.lock.Lock()
		defer g.lock.Unlock()
	}
	if g.shouldRefresh() {
		g.log.Debugf("Last time refreshed groups %s ago", time.Since(g.lastTimeRefreshed))
		return true, g.fetchGroupsUnlocked()
	}
	return false, nil
}

func (g *GroupCacheStorage) UpsertOneGroup(group *types.GroupInfo) error {
	setting := ExtractEphemeralSettingsFromGroup(group)
	err := g.chatEphemeralSetting.UpdateChatEphemeralSetting(setting)
	if err != nil {
		g.log.Warnf("Upserting chat ephemeral setting for group failed %v: %v", setting.ID, err)
	}
	return g.groups.UpsertOneGroup(group)
}

func (g *GroupCacheStorage) GetAllGroups(sort storage.Sort, pagination storage.Pagination) ([]*types.GroupInfo, error) {
	_, err := g.fetchGroupsIfNeeded(true)
	if err != nil {
		return nil, err
	}
	return g.groups.GetAllGroups(sort, pagination)
}

func (g *GroupCacheStorage) GetGroup(jid types.JID) (*types.GroupInfo, error) {
	_, err := g.fetchGroupsIfNeeded(true)
	if err != nil {
		return nil, err
	}
	return g.groups.GetGroup(jid)
}

func (g *GroupCacheStorage) DeleteGroup(jid types.JID) error {
	err := g.chatEphemeralSetting.DeleteChatEphemeralSetting(jid, time.Now())
	if err != nil {
		g.log.Warnf("Deleting chat ephemeral setting for group failed %v: %v", jid, err)
	}
	return g.groups.DeleteGroup(jid)
}

func (g *GroupCacheStorage) DeleteGroups() error {
	return g.groups.DeleteGroups()
}
