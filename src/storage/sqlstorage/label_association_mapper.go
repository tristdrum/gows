package sqlstorage

import (
	"encoding/json"
	"github.com/devlikeapro/gows/storage"
)

type LabelAssociationMapper struct {
}

var _ Mapper[storage.LabelAssociation] = (*LabelAssociationMapper)(nil)
var labelAssociationMapper = &LabelAssociationMapper{}

func (f *LabelAssociationMapper) ToFields(entity *storage.LabelAssociation) map[string]interface{} {
	return map[string]interface{}{
		"jid":      entity.JID,
		"label_id": entity.LabelID,
	}
}

func (f *LabelAssociationMapper) Marshal(association *storage.LabelAssociation) ([]byte, error) {
	return json.Marshal(association)
}

func (f *LabelAssociationMapper) Unmarshal(data []byte, association *storage.LabelAssociation) error {
	return json.Unmarshal(data, association)
}
