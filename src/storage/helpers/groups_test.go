package helpers

import (
	"github.com/stretchr/testify/assert"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"testing"
)

var participantJid, _ = types.ParseJID("888@s.whatsapp.net")
var adminJid, _ = types.ParseJID("999@s.whatsapp.net")
var notInGroupJid, _ = types.ParseJID("111@s.whatsapp.net")
var firstParticipantVersionID = "1740132428878975"
var newParticipantVersionID = "1740132428878976"

func BuildGroup() types.GroupInfo {
	jid, _ := types.ParseJID("123@g.us")

	return types.GroupInfo{
		JID:      jid,
		OwnerJID: adminJid,
		GroupName: types.GroupName{
			Name: "Test Group",
		},
		GroupTopic: types.GroupTopic{
			Topic: "Test Topic",
		},
		GroupLocked: types.GroupLocked{
			IsLocked: false,
		},
		GroupAnnounce: types.GroupAnnounce{
			IsAnnounce: false,
		},

		MemberAddMode:        types.GroupMemberAddModeAllMember,
		ParticipantVersionID: firstParticipantVersionID,
		Participants: []types.GroupParticipant{
			{
				JID:          participantJid,
				IsAdmin:      false,
				IsSuperAdmin: false,
			},
			{
				JID:          adminJid,
				IsAdmin:      false,
				IsSuperAdmin: true,
			},
		},
	}
}

func TestUpdateGroupInfo_UpdateParams(t *testing.T) {
	group := BuildGroup()
	event := events.GroupInfo{
		Name:     &types.GroupName{Name: "New Name"},
		Topic:    &types.GroupTopic{Topic: "New Topic"},
		Locked:   &types.GroupLocked{IsLocked: true},
		Announce: &types.GroupAnnounce{IsAnnounce: true},
	}

	err := UpdateGroupInfo(&group, &event)
	assert.Nil(t, err, "Error should be nil")

	assert.Equal(t, "New Name", group.GroupName.Name)
	assert.Equal(t, "New Topic", group.GroupTopic.Topic)
	assert.True(t, group.GroupLocked.IsLocked)
	assert.True(t, group.GroupAnnounce.IsAnnounce)
}

func TestUpdateGroupInfo_Participant_Join(t *testing.T) {
	group := BuildGroup()
	jids := []types.JID{notInGroupJid}
	event := events.GroupInfo{
		Join:                     jids,
		PrevParticipantVersionID: firstParticipantVersionID,
		ParticipantVersionID:     newParticipantVersionID,
	}

	err := UpdateGroupInfo(&group, &event)
	assert.Nil(t, err, "Error should be nil")

	assert.Equal(t, 3, len(group.Participants))
	expected := types.GroupParticipant{
		JID:          notInGroupJid,
		IsAdmin:      false,
		IsSuperAdmin: false,
	}
	assert.Equal(t, expected, group.Participants[2])
	assert.Equal(t, newParticipantVersionID, group.ParticipantVersionID)
}

func TestUpdateGroupInfo_Participant_WrongPrevVersionId(t *testing.T) {
	group := BuildGroup()
	jids := []types.JID{notInGroupJid}
	event := events.GroupInfo{
		Join:                     jids,
		PrevParticipantVersionID: "123",
		ParticipantVersionID:     newParticipantVersionID,
	}

	err := UpdateGroupInfo(&group, &event)

	assert.NotNil(t, err, "Error should not be nil")
	assert.Equal(t, ErrMismatchedParticipantVersion, err)
	assert.Equal(t, 2, len(group.Participants))
}

func TestUpdateGroupInfo_Participant_Leave(t *testing.T) {
	group := BuildGroup()
	jids := []types.JID{participantJid}
	event := events.GroupInfo{
		Leave:                    jids,
		PrevParticipantVersionID: firstParticipantVersionID,
		ParticipantVersionID:     newParticipantVersionID,
	}

	err := UpdateGroupInfo(&group, &event)
	assert.Nil(t, err, "Error should be nil")

	assert.Equal(t, 1, len(group.Participants))
	expected := types.GroupParticipant{
		JID:          adminJid,
		IsAdmin:      false,
		IsSuperAdmin: true,
	}
	assert.Equal(t, expected, group.Participants[0])
}

func TestUpdateGroupInfo_Participant_Promote(t *testing.T) {
	group := BuildGroup()
	jids := []types.JID{participantJid}
	event := events.GroupInfo{
		Promote:                  jids,
		PrevParticipantVersionID: firstParticipantVersionID,
		ParticipantVersionID:     newParticipantVersionID,
	}

	err := UpdateGroupInfo(&group, &event)
	assert.Nil(t, err, "Error should be nil")

	assert.Equal(t, 2, len(group.Participants))
	expected := types.GroupParticipant{
		JID:          participantJid,
		IsAdmin:      true,
		IsSuperAdmin: false,
	}
	assert.Equal(t, expected, group.Participants[0])
}

func TestUpdateGroupInfo_Participant_Demote(t *testing.T) {
	group := BuildGroup()
	jids := []types.JID{adminJid}
	event := events.GroupInfo{
		Demote:                   jids,
		PrevParticipantVersionID: firstParticipantVersionID,
		ParticipantVersionID:     newParticipantVersionID,
	}

	err := UpdateGroupInfo(&group, &event)
	assert.Nil(t, err, "Error should be nil")

	assert.Equal(t, 2, len(group.Participants))
	expected := types.GroupParticipant{
		JID:          adminJid,
		IsAdmin:      false,
		IsSuperAdmin: false,
	}
	assert.Equal(t, expected, group.Participants[1])
}
