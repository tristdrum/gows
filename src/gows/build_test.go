package gows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestBuildChatArchiveBuildsArchivePatch(t *testing.T) {
	jid, err := types.ParseJID("123456789@s.whatsapp.net")
	assert.NoError(t, err)
	messageID := "ABCDEF"
	fromMe := false
	key := &waCommon.MessageKey{
		RemoteJID: proto.String(jid.String()),
		FromMe:    proto.Bool(fromMe),
		ID:        proto.String(messageID),
	}
	timestamp := time.Unix(1_700_000_000, 0)

	patch := BuildChatArchive(jid, true, key, timestamp)

	assert.Equal(t, appstate.WAPatchRegularLow, patch.Type)
	assert.Len(t, patch.Mutations, 2)
	assert.Equal(t, []string{appstate.IndexArchive, jid.String()}, patch.Mutations[0].Index)
	action := patch.Mutations[0].Value.GetArchiveChatAction()
	assert.NotNil(t, action)
	assert.True(t, action.GetArchived())
	assert.Equal(t, timestamp.Unix(), action.GetMessageRange().GetLastMessageTimestamp())
	assert.Equal(t, messageID, action.GetMessageRange().GetMessages()[0].GetKey().GetID())
	assert.Equal(t, []string{appstate.IndexPin, jid.String()}, patch.Mutations[1].Index)
}

func TestBuildChatArchiveBuildsUnarchivePatchWithoutLastMessage(t *testing.T) {
	jid, err := types.ParseJID("123456789@s.whatsapp.net")
	assert.NoError(t, err)

	patch := BuildChatArchive(jid, false, nil, time.Time{})

	assert.Equal(t, appstate.WAPatchRegularLow, patch.Type)
	assert.Len(t, patch.Mutations, 1)
	assert.Equal(t, []string{appstate.IndexArchive, jid.String()}, patch.Mutations[0].Index)
	action := patch.Mutations[0].Value.GetArchiveChatAction()
	assert.NotNil(t, action)
	assert.False(t, action.GetArchived())
	assert.Greater(t, action.GetMessageRange().GetLastMessageTimestamp(), int64(0))
	assert.Empty(t, action.GetMessageRange().GetMessages())
}

func TestBuildChatClearBuildsClearPatchWithoutDeleteAction(t *testing.T) {
	jid, err := types.ParseJID("123456789-123456@g.us")
	assert.NoError(t, err)
	messageID := "ABCDEF"
	key := &waCommon.MessageKey{
		RemoteJID:   proto.String(jid.String()),
		FromMe:      proto.Bool(true),
		ID:          proto.String(messageID),
		Participant: proto.String("111111111@s.whatsapp.net"),
	}
	timestamp := time.Unix(1_700_000_000, 0)

	patch := BuildChatClear(jid, key, timestamp)

	assert.Equal(t, appstate.WAPatchRegularHigh, patch.Type)
	assert.Len(t, patch.Mutations, 1)
	assert.Equal(t, []string{appstate.IndexClearChat, jid.String(), "1", "0"}, patch.Mutations[0].Index)
	assert.Equal(t, int32(6), patch.Mutations[0].Version)
	value := patch.Mutations[0].Value
	clearAction := value.GetClearChatAction()
	assert.NotNil(t, clearAction)
	assert.Nil(t, value.GetDeleteChatAction())
	assert.Equal(t, timestamp.Unix(), clearAction.GetMessageRange().GetLastMessageTimestamp())
	assert.Equal(t, messageID, clearAction.GetMessageRange().GetMessages()[0].GetKey().GetID())
}
