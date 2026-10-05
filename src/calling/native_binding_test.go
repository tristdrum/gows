package calling

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/purpshell/meowcaller"
)

func TestNativeBindingUsesFramedPrivateSessionAndCall(t *testing.T) {
	if NativeBindingHash("a", "bc") == NativeBindingHash("ab", "c") {
		t.Fatal("unframed private identities collided")
	}
	if NativeBindingHash("first-session", "same-call") == NativeBindingHash("second-session", "same-call") {
		t.Fatal("same native call in two sessions lost isolation")
	}
	const want = "5e1c3982947b270de8cc4b44658d167e6dd4f7714b8ca28fbe69d595c9e9a80e"
	if got := NativeBindingHash("sés", "call"); got != want {
		t.Fatalf("binding framing contract changed: %s", got)
	}
}

func TestNativeSignalBindingSurvivesAdmissionAndCleanupWithoutPrivateData(t *testing.T) {
	const session = "PRIVATE_SESSION_SENTINEL"
	const id = "PRIVATE_CALL_SENTINEL"
	var samples []SignalDiagnostic
	p := &nativeSignalProbe{session: session, now: time.Now, emit: func(sample SignalDiagnostic) { samples = append(samples, sample) }}
	p.observe(meowcaller.SignalObservation{CallID: id, Kind: "terminate"})
	p.admit(&fakeCall{id: id, peer: "PRIVATE_PEER_SENTINEL@lid"}, "inbound")
	p.end(id, "server:PRIVATE_ERROR_SENTINEL")
	if len(samples) != 2 || samples[0].Kind != "terminate" || samples[1].EndCategory != "server_error" {
		t.Fatalf("native terminal sequence changed: %+v", samples)
	}
	for _, sample := range samples {
		if sample.BindingHash != NativeBindingHash(session, id) {
			t.Fatal("native signal cannot join the exact private media binding")
		}
	}
	encoded, err := json.Marshal(samples)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "PRIVATE_") {
		t.Fatalf("native diagnostic exposed private identity or text: %s", encoded)
	}
}
