package helpers

import (
	"errors"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func UpdateGroupInfo(group *types.GroupInfo, update *events.GroupInfo) (err error) {
	err = updateGroupParams(group, update)
	if err != nil {
		return err
	}
	err = updateGroupParticipants(group, update)
	if err != nil {
		return err
	}
	return nil
}

func updateGroupParams(group *types.GroupInfo, update *events.GroupInfo) error {
	if update.Name != nil {
		group.GroupName = *update.Name
	}
	if update.Topic != nil {
		group.GroupTopic = *update.Topic
	}
	if update.Locked != nil {
		group.GroupLocked = *update.Locked
	}
	if update.Announce != nil {
		group.GroupAnnounce = *update.Announce
	}
	if update.Ephemeral != nil {
		group.GroupEphemeral = *update.Ephemeral
	}
	return nil
}

func isEmpty(jids []types.JID) bool {
	return jids == nil || len(jids) == 0
}

var ErrMismatchedParticipantVersion = errors.New("participant version mismatch")

func updateGroupParticipants(group *types.GroupInfo, update *events.GroupInfo) error {
	if isEmpty(update.Join) && isEmpty(update.Leave) && isEmpty(update.Promote) && isEmpty(update.Demote) {
		return nil
	}
	if update.PrevParticipantVersionID != group.ParticipantVersionID {
		return ErrMismatchedParticipantVersion
	}

	pm := NewParticipantsMap(group.Participants)
	pm.Join(update.Join)
	pm.Leave(update.Leave)
	pm.Promote(update.Promote)
	pm.Demote(update.Demote)
	group.Participants = pm.Participants()
	group.ParticipantVersionID = update.ParticipantVersionID
	return nil
}
