package sqlstorage

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"google.golang.org/protobuf/proto"
)

func TestMessageEditHistoryMediaKeepsArtifactsAndUnknownFields(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, store *SqlMessageStore) {
		for _, kind := range []string{"image", "video", "document", "document-wrapper"} {
			var originalContent, editedContent *waE2E.Message
			switch kind {
			case "image":
				originalContent = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("Original caption"), Mimetype: proto.String("image/jpeg"), URL: proto.String("https://fixture.invalid/media"), MediaKey: []byte{1, 2, 3}, FileSHA256: []byte{4, 5}, FileLength: proto.Uint64(123)}}
				editedContent = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("")}}
			case "video":
				originalContent = &waE2E.Message{VideoMessage: &waE2E.VideoMessage{Caption: proto.String("Original caption"), Mimetype: proto.String("video/mp4"), URL: proto.String("https://fixture.invalid/media"), MediaKey: []byte{1, 2, 3}, FileSHA256: []byte{4, 5}, FileLength: proto.Uint64(123)}}
				editedContent = &waE2E.Message{VideoMessage: &waE2E.VideoMessage{Caption: proto.String("")}}
			default:
				originalContent = &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{Caption: proto.String("Original caption"), Mimetype: proto.String("application/pdf"), URL: proto.String("https://fixture.invalid/media"), MediaKey: []byte{1, 2, 3}, FileSHA256: []byte{4, 5}, FileLength: proto.Uint64(123)}}
				editedContent = &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{Caption: proto.String("")}}
				if kind == "document-wrapper" {
					originalContent = &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{Message: originalContent}}
				}
			}
			original := editOriginal(originalContent)
			original.SourceWebMsg = &waWeb.WebMessageInfo{Key: &waCommon.MessageKey{ID: proto.String(original.Info.ID)}, Message: proto.Clone(originalContent).(*waE2E.Message)}
			require.NoError(t, store.UpsertOneMessage(original))
			var before string
			require.NoError(t, store.db.Get(&before, "SELECT data FROM gows_messages WHERE id = $1", original.Info.ID))
			before = strings.ReplaceAll(before, `"fileLength":"123"`, `"fileLength":"123","futureMediaField":{"keep":true}`)
			before = strings.TrimSuffix(before, "}") + `,"FutureTopLevel":{"keep":true}}`
			_, err := store.db.Exec("UPDATE gows_messages SET data = $1 WHERE id = $2", before, original.Info.ID)
			require.NoError(t, err)
			require.NoError(t, store.UpsertOneMessage(editEvent(original, editedContent, original.Info.Timestamp.Add(time.Minute).UnixMilli(), false)))
			var after string
			require.NoError(t, store.db.Get(&after, "SELECT data FROM gows_messages WHERE id = $1", original.Info.ID))
			var oldFields, newFields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(before), &oldFields))
			require.NoError(t, json.Unmarshal([]byte(after), &newFields))
			for _, key := range []string{"Message", "RawMessage", "SourceWebMsg"} {
				require.JSONEq(t, strings.ReplaceAll(string(oldFields[key]), `"Original caption"`, `""`), string(newFields[key]))
			}
			require.JSONEq(t, string(oldFields["FutureTopLevel"]), string(newFields["FutureTopLevel"]))
			require.Equal(t, 3, strings.Count(after, "futureMediaField"))
			assertStoredProjection(t, store, original, true)
		}
	})
}
