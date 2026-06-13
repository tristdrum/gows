package sqlstorage

import (
	"context"
	"errors"
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"sort"
)

var _ storage.ContactStorage = (*MeowContactStorage)(nil)

func NewContactStorage(store *store.Device) *MeowContactStorage {
	return &MeowContactStorage{
		store,
	}
}

type MeowContactStorage struct {
	store *store.Device
}

func toContact(jid types.JID, info types.ContactInfo) *storage.StoredContact {
	return &storage.StoredContact{
		Jid:      jid,
		Name:     info.FullName,
		PushName: info.PushName,
	}
}

func (s MeowContactStorage) GetContact(user types.JID) (*storage.StoredContact, error) {
	contactInfo, err := s.store.Contacts.GetContact(context.TODO(), user)
	if err != nil {
		return nil, err
	}
	contact := toContact(user, contactInfo)
	return contact, nil
}

func (s MeowContactStorage) GetAllContacts(sortBy storage.Sort, pagination storage.Pagination) ([]*storage.StoredContact, error) {
	contactInfos, err := s.store.Contacts.GetAllContacts(context.TODO())
	if err != nil {
		return nil, err
	}
	contacts := make([]*storage.StoredContact, 0, len(contactInfos))
	for user, info := range contactInfos {
		contacts = append(contacts, toContact(user, info))
	}
	// sortBy by id or name
	if sortBy.Field == "id" {
		sortById(contacts, sortBy.Order)
	} else if sortBy.Field == "name" {
		sortByName(contacts, sortBy.Order)
	} else {
		return nil, errors.New("invalid sort field %s" + sortBy.Field)
	}

	// pagination
	contacts = applyPagination(contacts, pagination)
	return contacts, nil
}

func sortById(contacts []*storage.StoredContact, order storage.SortOrder) {
	if order == storage.SortAsc {
		sort.Slice(contacts, func(i, j int) bool {
			return contacts[i].Jid.User < contacts[j].Jid.User
		})
	} else {
		sort.Slice(contacts, func(i, j int) bool {
			return contacts[i].Jid.User > contacts[j].Jid.User
		})
	}
}

func sortByName(contacts []*storage.StoredContact, order storage.SortOrder) {
	if order == storage.SortAsc {
		sort.Slice(contacts, func(i, j int) bool {
			return contacts[i].Name < contacts[j].Name
		})
	} else {
		sort.Slice(contacts, func(i, j int) bool {
			return contacts[i].Name > contacts[j].Name
		})
	}
}

func applyPagination(contacts []*storage.StoredContact, pagination storage.Pagination) []*storage.StoredContact {
	if pagination.Offset > 0 || pagination.Limit > 0 {
		start := int(pagination.Offset)
		limit := int(pagination.Limit)
		if limit == 0 {
			limit = len(contacts)
		}
		end := start + limit
		if end > len(contacts) {
			end = len(contacts)
		}
		return contacts[start:end]
	}
	return contacts
}
