package noop

import (
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type GroupStorage struct{}

var _ storage.GroupStorage = (*GroupStorage)(nil)

func NewGroupStorage() *GroupStorage {
	return &GroupStorage{}
}

func (s GroupStorage) FetchGroups(force bool) error {
	return nil
}

func (s GroupStorage) UpdateGroup(update *events.GroupInfo) error {
	return nil
}

func (s GroupStorage) UpsertOneGroup(group *types.GroupInfo) error {
	return nil
}

func (s GroupStorage) GetAllGroups(sort storage.Sort, pagination storage.Pagination) ([]*types.GroupInfo, error) {
	return []*types.GroupInfo{}, nil
}

func (s GroupStorage) GetGroup(jid types.JID) (*types.GroupInfo, error) {
	return nil, storage.StorageDisabled("groups")
}

func (s GroupStorage) DeleteGroup(jid types.JID) error {
	return nil
}

func (s GroupStorage) DeleteGroups() error {
	return nil
}
