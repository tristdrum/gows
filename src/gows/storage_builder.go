package gows

import (
	"github.com/devlikeapro/gows/storage"
	meowstorage "github.com/devlikeapro/gows/storage/meow"
	"github.com/devlikeapro/gows/storage/noop"
	"github.com/devlikeapro/gows/storage/sqlstorage"
	"github.com/devlikeapro/gows/storage/views"
)

func BuildStorage(container *sqlstorage.GContainer, gows *GoWS, cfg StorageConfig) *storage.Storage {
	st := &storage.Storage{}
	st.ChatEphemeralSetting = container.NewChatEphemeralSettingStorage()
	st.Contacts = meowstorage.NewContactStorage(gows.Store)

	if cfg.Messages {
		st.Messages = container.NewMessageStorage()
	} else {
		st.Messages = noop.NewMessageStorage()
	}

	if cfg.Groups {
		st.Groups = container.NewGroupStorage()
		st.Groups = NewGroupCacheStorage(gows, st.Groups, st.ChatEphemeralSetting)
	} else {
		st.Groups = NewGroupEphemeralOnlyStorage(gows, st.ChatEphemeralSetting)
	}

	if cfg.Chats {
		st.Chats = views.NewChatView(st.Messages, st.Contacts, st.Groups)
	} else {
		st.Chats = noop.NewChatStorage()
	}

	if cfg.Labels {
		st.Labels = container.NewLabelStorage()
		st.LabelAssociations = container.NewLabelAssociationStorage()
	} else {
		st.Labels = noop.NewLabelStorage()
		st.LabelAssociations = noop.NewLabelAssociationStorage()
	}

	st.Lidmap = container.NewLidmapStorage()
	return st
}
