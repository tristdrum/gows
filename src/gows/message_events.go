package gows

import (
	"context"
	"errors"
	"fmt"
	"go.mau.fi/util/random"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNotEventResponseMessage = errors.New("given message isn't an event response message")
)

type EventLocation struct {
	Name             string
	DegreesLongitude *float64
	DegreesLatitude  *float64
}

type EventMessage struct {
	Name               string
	Description        *string
	StartTime          int64
	EndTime            *int64
	ExtraGuestsAllowed bool
	Location           *EventLocation
}

func BuildEventCreation(
	event *EventMessage,
) *waE2E.Message {
	msgSecret := random.Bytes(32)
	eventMessage := &waE2E.EventMessage{
		Name:               proto.String(event.Name),
		Description:        event.Description,
		StartTime:          proto.Int64(event.StartTime),
		EndTime:            event.EndTime,
		ExtraGuestsAllowed: proto.Bool(event.ExtraGuestsAllowed),
		IsCanceled:         proto.Bool(false),
	}
	if event.Location != nil {
		eventMessage.Location = &waE2E.LocationMessage{
			Name:             proto.String(event.Location.Name),
			DegreesLongitude: event.Location.DegreesLongitude,
			DegreesLatitude:  event.Location.DegreesLatitude,
		}
	}
	return &waE2E.Message{
		EventMessage: eventMessage,
		MessageContextInfo: &waE2E.MessageContextInfo{
			MessageSecret: msgSecret,
		},
	}
}

func (gows *GoWS) extractEventResponse(ctx context.Context, response *events.Message) (*waE2E.EventResponseMessage, error) {
	encEventResponseMessage := response.Message.GetEncEventResponseMessage()
	if encEventResponseMessage == nil {
		return nil, ErrNotEventResponseMessage
	}
	plaintext, err := gows.int.DecryptMsgSecret(
		ctx,
		response,
		whatsmeow.EncSecretEventResponse,
		encEventResponseMessage,
		encEventResponseMessage.GetEventCreationMessageKey(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt event response: %w", err)
	}
	var msg waE2E.EventResponseMessage
	err = proto.Unmarshal(plaintext, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed to decode event response protobuf: %w", err)
	}
	return &msg, nil
}

func (gows *GoWS) handleEncEventResponse(ctx context.Context, msg *events.Message) {
	eventResponse, err := gows.extractEventResponse(ctx, msg)
	if err != nil {
		gows.Log.Errorf("Failed to decrypt event response: %v", err)
	}
	data := &EventMessageResponse{
		Message:       msg,
		EventResponse: eventResponse,
	}
	gows.emitEvent(data)
}

func (gows *GoWS) BuildEventUpdate(
	ctx context.Context,
	info *types.MessageInfo,
	event *waE2E.EventMessage,
) (*waE2E.Message, error) {
	plaintext, err := proto.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event update protobuf: %w", err)
	}
	ciphertext, iv, err := gows.int.EncryptMsgSecret(ctx, gows.int.GetOwnID(), info.Chat, info.Sender, info.ID, whatsmeow.EncSecretEventEdit, plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt event update event: %w", err)
	}
	msg := &waE2E.SecretEncryptedMessage{
		TargetMessageKey: whatsmeow.GetKeyFromInfo(info),
		EncPayload:       ciphertext,
		EncIV:            iv,
		SecretEncType:    waE2E.SecretEncryptedMessage_EVENT_EDIT.Enum(),
	}
	return &waE2E.Message{SecretEncryptedMessage: msg}, err
}
