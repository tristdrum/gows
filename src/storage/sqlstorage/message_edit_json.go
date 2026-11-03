package sqlstorage

import (
	"encoding/json"
	"errors"

	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/encoding/protojson"
)

type messageEditField struct {
	media string
	value string
}

func replacementField(content *waE2E.Message) (messageEditField, error) {
	if content == nil {
		return messageEditField{}, errors.New("missing edited content")
	}
	if content.DocumentWithCaptionMessage != nil {
		content = content.GetDocumentWithCaptionMessage().GetMessage()
	}
	if content == nil {
		return messageEditField{}, errors.New("missing edited document")
	}
	fields := []messageEditField{}
	if content.Conversation != nil {
		fields = append(fields, messageEditField{value: content.GetConversation()})
	}
	if content.ExtendedTextMessage != nil && content.ExtendedTextMessage.Text != nil {
		fields = append(fields, messageEditField{value: content.GetExtendedTextMessage().GetText()})
	}
	if content.ImageMessage != nil && content.ImageMessage.Caption != nil {
		fields = append(fields, messageEditField{media: "imageMessage", value: content.GetImageMessage().GetCaption()})
	}
	if content.VideoMessage != nil && content.VideoMessage.Caption != nil {
		fields = append(fields, messageEditField{media: "videoMessage", value: content.GetVideoMessage().GetCaption()})
	}
	if content.DocumentMessage != nil && content.DocumentMessage.Caption != nil {
		fields = append(fields, messageEditField{media: "documentMessage", value: content.GetDocumentMessage().GetCaption()})
	}
	if len(fields) != 1 || content.ProtocolMessage != nil || content.ViewOnceMessage != nil || content.ViewOnceMessageV2 != nil || content.ViewOnceMessageV2Extension != nil {
		return messageEditField{}, errors.New("unsupported edited content")
	}
	return fields[0], nil
}

func jsonObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New("expected stored object")
	}
	return object, nil
}

// Patch only the user-editable field in retained JSON. Re-marshalling the
// protobuf would drop unknown fields; replacing media would drop its keys/URLs.
func patchContent(raw json.RawMessage, field messageEditField, depth int) (json.RawMessage, error) {
	if depth > 8 {
		return nil, errors.New("stored wrapper depth exceeded")
	}
	object, err := jsonObject(raw)
	if err != nil {
		return nil, err
	}
	for _, unsupported := range []string{"viewOnceMessage", "viewOnceMessageV2", "viewOnceMessageV2Extension", "protocolMessage", "editedMessage"} {
		if _, found := object[unsupported]; found {
			return nil, errors.New("unsupported stored content wrapper")
		}
	}
	for _, wrapper := range []string{"deviceSentMessage", "ephemeralMessage", "documentWithCaptionMessage"} {
		if value, found := object[wrapper]; found {
			inner, err := jsonObject(value)
			if err != nil {
				return nil, err
			}
			inner["message"], err = patchContent(inner["message"], field, depth+1)
			if err != nil {
				return nil, err
			}
			object[wrapper], err = json.Marshal(inner)
			if err != nil {
				return nil, err
			}
			return json.Marshal(object)
		}
	}
	encoded, _ := json.Marshal(field.value)
	if field.media != "" {
		media, err := jsonObject(object[field.media])
		if err != nil {
			return nil, errors.New("edited media does not match original type")
		}
		media["caption"] = encoded
		object[field.media], err = json.Marshal(media)
		if err != nil {
			return nil, err
		}
	} else if _, found := object["conversation"]; found {
		object["conversation"] = encoded
	} else if value, found := object["extendedTextMessage"]; found {
		text, err := jsonObject(value)
		if err != nil {
			return nil, err
		}
		text["text"] = encoded
		object["extendedTextMessage"], err = json.Marshal(text)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("edited text does not match original type")
	}
	return json.Marshal(object)
}

func patchRetainedContent(existing, accepted json.RawMessage, field messageEditField) (json.RawMessage, error) {
	patched, err := patchContent(existing, field, 0)
	if err == nil {
		return patched, nil
	}
	// Only the known legacy normalized-edit shape can lack the original raw
	// content. Strict protobuf decoding prevents dropping unknown wrapper data.
	var content waE2E.Message
	if decodeErr := protojson.Unmarshal(existing, &content); decodeErr != nil {
		return nil, err
	}
	protocol, protocolErr := editProtocol(&content)
	if protocolErr != nil || protocol == nil || protocol.GetEditedMessage() == nil {
		return nil, err
	}
	if _, fieldErr := replacementField(protocol.GetEditedMessage()); fieldErr != nil {
		return nil, err
	}
	return accepted, nil
}

func patchMessageEdit(raw []byte, edited *waE2E.Message, metadata *storage.MessageEditMetadata) ([]byte, error) {
	field, err := replacementField(edited)
	if err != nil {
		return nil, err
	}
	payload, err := jsonObject(raw)
	if err != nil {
		return nil, err
	}
	payload["Message"], err = patchContent(payload["Message"], field, 0)
	if err != nil {
		return nil, err
	}
	// Legacy normalized edits may retain an old protocol in RawMessage. Use
	// accepted content if that wrapper cannot be patched; never retain old text.
	if existing := payload["RawMessage"]; len(existing) != 0 && string(existing) != "null" {
		payload["RawMessage"], err = patchRetainedContent(existing, payload["Message"], field)
		if err != nil {
			return nil, err
		}
	}
	if existing := payload["SourceWebMsg"]; len(existing) != 0 && string(existing) != "null" {
		source, err := jsonObject(existing)
		if err != nil {
			return nil, err
		}
		source["message"], err = patchRetainedContent(source["message"], payload["Message"], field)
		if err != nil {
			return nil, err
		}
		payload["SourceWebMsg"], err = json.Marshal(source)
		if err != nil {
			return nil, err
		}
	}
	payload["IsEdit"] = json.RawMessage(`true`)
	payload["MinEdit"], err = json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	return json.Marshal(payload)
}
