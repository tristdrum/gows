package sqlstorage

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func (gc *GContainer) NewGroupStorage() *SqlGroupStore {
	repo := NewEntityRepository[types.GroupInfo](
		gc.db,
		GroupTable,
		groupMapper,
	)
	return &SqlGroupStore{
		repo,
	}
}

type SqlGroupStore struct {
	*EntityRepository[types.GroupInfo]
}

var _ storage.GroupStorage = (*SqlGroupStore)(nil)

func (s SqlGroupStore) FetchGroups(force bool) error {
	return fmt.Errorf("not implemented, use GroupCacheStorage as a wrapper")
}

func (s SqlGroupStore) UpdateGroup(update *events.GroupInfo) error {
	return fmt.Errorf("not implemented, use GroupCacheStorage as a wrapper")
}

func (s SqlGroupStore) UpsertOneGroup(group *types.GroupInfo) error {
	return s.UpsertOne(group)
}

func (s SqlGroupStore) GetAllGroups(sort storage.Sort, pagination storage.Pagination) ([]*types.GroupInfo, error) {
	conditions := make([]sq.Sqlizer, 0)
	sorts := []storage.Sort{
		sort,
	}
	return s.FilterBy(conditions, sorts, pagination)
}

func (s SqlGroupStore) GetGroup(jid types.JID) (group *types.GroupInfo, err error) {
	return s.GetById(jid.String())
}

func (s SqlGroupStore) DeleteGroup(jid types.JID) error {
	return s.DeleteById(jid.String())
}
func (s SqlGroupStore) DeleteGroups() error {
	return s.DeleteAll()
}
