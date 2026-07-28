package sqlstorage

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func storedMessageWithKnownProtoFields() *storage.StoredMessage {
	return &storage.StoredMessage{Message: &events.Message{
		Message: &waProto.Message{
			ExtendedTextMessage: &waProto.ExtendedTextMessage{
				Text: proto.String("known message"),
				FaviconMmsMetadata: &waE2E.MMSThumbnailMetadata{
					ThumbnailDirectPath: proto.String("/historical-thumbnail"),
				},
			},
		},
		RawMessage: &waProto.Message{Conversation: proto.String("known raw message")},
		SourceWebMsg: &waProto.WebMessageInfo{
			Key: &waProto.MessageKey{
				RemoteJID: proto.String("15551234567@s.whatsapp.net"),
				ID:        proto.String("source-id"),
			},
			PushName: proto.String("known source"),
		},
	}}
}

func addUnknownJSONField(t *testing.T, raw json.RawMessage, name string) json.RawMessage {
	t.Helper()
	require.NotEmpty(t, raw)
	return json.RawMessage(strings.TrimSuffix(string(raw), "}") + `,"` + name + `":{"ignored":true}}`)
}

func rewriteStoredProtoJSON(t *testing.T, data []byte, rewrite func(map[string]json.RawMessage)) []byte {
	t.Helper()
	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &payload))
	rewrite(payload)
	rewritten, err := json.Marshal(payload)
	require.NoError(t, err)
	return rewritten
}

func TestMessageMapperUnmarshalDiscardsHistoricalAndFutureUnknownProtoFields(t *testing.T) {
	encoded, err := messageMapper.Marshal(storedMessageWithKnownProtoFields())
	require.NoError(t, err)

	encoded = rewriteStoredProtoJSON(t, encoded, func(payload map[string]json.RawMessage) {
		payload["Message"] = json.RawMessage(strings.ReplaceAll(
			string(payload["Message"]),
			`"faviconMmsMetadata"`,
			`"faviconMMSMetadata"`,
		))
		payload["Message"] = addUnknownJSONField(t, payload["Message"], "futureMessageField")
		payload["RawMessage"] = addUnknownJSONField(t, payload["RawMessage"], "futureRawMessageField")
		payload["SourceWebMsg"] = addUnknownJSONField(t, payload["SourceWebMsg"], "futureSourceField")
	})

	var decoded storage.StoredMessage
	require.NoError(t, messageMapper.Unmarshal(encoded, &decoded))
	assert.Equal(t, "known message", decoded.Message.Message.GetExtendedTextMessage().GetText())
	assert.Nil(t, decoded.Message.Message.GetExtendedTextMessage().GetFaviconMmsMetadata())
	assert.Equal(t, "known raw message", decoded.RawMessage.GetConversation())
	assert.Equal(t, "known source", decoded.SourceWebMsg.GetPushName())
}

func TestMessageMapperUnmarshalStillRejectsMalformedKnownProtoFields(t *testing.T) {
	encoded, err := messageMapper.Marshal(storedMessageWithKnownProtoFields())
	require.NoError(t, err)

	tests := []struct {
		name  string
		field string
		value json.RawMessage
	}{
		{name: "Message", field: "Message", value: json.RawMessage(`{"conversation":123}`)},
		{name: "RawMessage", field: "RawMessage", value: json.RawMessage(`{"conversation":123}`)},
		{name: "SourceWebMsg", field: "SourceWebMsg", value: json.RawMessage(`{"pushName":123}`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			malformed := rewriteStoredProtoJSON(t, encoded, func(payload map[string]json.RawMessage) {
				payload[tc.field] = tc.value
			})
			var decoded storage.StoredMessage
			assert.Error(t, messageMapper.Unmarshal(malformed, &decoded))
		})
	}
}
