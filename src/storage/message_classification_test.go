package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestRealMessageClassificationPreservesUnwrappedContent(t *testing.T) {
	text := &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String("Forwarded fixture"),
		ContextInfo: &waE2E.ContextInfo{
			IsForwarded: proto.Bool(true), ForwardingScore: proto.Uint32(1),
		},
	}}
	wrapped := &waE2E.FutureProofMessage{Message: text}
	for name, raw := range map[string]*waE2E.Message{
		"text":             text,
		"ephemeral":        {EphemeralMessage: wrapped},
		"view-once":        {ViewOnceMessage: wrapped},
		"view-once-v2":     {ViewOnceMessageV2: wrapped},
		"document-caption": {DocumentWithCaptionMessage: wrapped},
	} {
		t.Run(name, func(t *testing.T) {
			event := (&events.Message{RawMessage: raw}).UnwrapRaw()
			require.True(t, IsRealMessage(event))
		})
	}
	require.False(t, IsRealMessage(nil))
	require.False(t, IsRealMessage(&events.Message{}))
	require.False(t, IsRealMessage(&events.Message{Message: &waE2E.Message{
		SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{},
	}}))
	require.False(t, IsRealMessage(&events.Message{Message: &waE2E.Message{
		ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("fixture")},
	}}))
}
