package whatsmeow

import (
	"context"
	"fmt"
	"strings"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type statusAckProofLogger struct{ lines []string }

func (l *statusAckProofLogger) Warnf(f string, a ...any) {
	l.lines = append(l.lines, fmt.Sprintf(f, a...))
}
func (l *statusAckProofLogger) Errorf(f string, a ...any) {
	l.lines = append(l.lines, fmt.Sprintf(f, a...))
}
func (l *statusAckProofLogger) Infof(f string, a ...any) {
	l.lines = append(l.lines, fmt.Sprintf(f, a...))
}
func (l *statusAckProofLogger) Debugf(f string, a ...any) {
	l.lines = append(l.lines, fmt.Sprintf(f, a...))
}
func (l *statusAckProofLogger) Sub(string) waLog.Logger { return l }

// Exercises the real frame parser, registered handler, and sendAck path without
// opening a network connection or touching a provider/session. The expected
// disconnected-socket warning proves the ACK attempt, not delivery on the wire.
func TestStatusMediaFrameAttemptsAcknowledgement(t *testing.T) {
	logger := &statusAckProofLogger{}
	own := types.NewJID("15550000001", types.DefaultUserServer)
	client := NewClient(&store.Device{ID: &own}, logger)
	node := waBinary.Node{Tag: "status", Attrs: waBinary.Attrs{
		"id":          "STATUS_MEDIA_REGRESSION_FIXTURE",
		"from":        types.StatusBroadcastJID,
		"participant": types.NewJID("15550000002", types.DefaultUserServer),
		"type":        "media",
		"t":           "1784692800",
	}}
	frame, err := waBinary.Marshal(node)
	if err != nil {
		t.Fatal(err)
	}
	client.handleFrame(context.Background(), frame)
	select {
	case parsed := <-client.handlerQueue:
		if parsed.Tag != "status" || parsed.Attrs["type"] != "media" || parsed.Attrs["id"] != node.Attrs["id"] {
			t.Fatalf("status identity changed: %v", parsed)
		}
		client.nodeHandlers[parsed.Tag](context.Background(), parsed)
	default:
		t.Fatalf("status media frame was dropped without dispatch or ACK: %s", strings.Join(logger.lines, "\n"))
	}
	want := fmt.Sprintf("Failed to send acknowledgement for status STATUS_MEDIA_REGRESSION_FIXTURE: %v", ErrNotConnected)
	count := 0
	for _, line := range logger.lines {
		if strings.Contains(line, "Failed to parse message") {
			t.Fatalf("fixture parse failed: %s", line)
		}
		if line == want {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected one ACK attempt through sendAck, got %d: %s", count, strings.Join(logger.lines, "\n"))
	}
	t.Log("real status media frame parsed and dispatched; sendAck attempted exactly once on the deliberately disconnected socket")
}
