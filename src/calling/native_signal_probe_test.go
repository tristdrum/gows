package calling

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/purpshell/meowcaller"
)

func TestSignalProbeKeepsPrivateFieldsOutOfSerialization(t *testing.T) {
	const private = "PRIVATE_SENTINEL"
	c := &fakeCall{id: private, peer: private + "@lid"}
	var samples []SignalDiagnostic
	p := &nativeSignalProbe{now: time.Now, emit: func(s SignalDiagnostic) { samples = append(samples, s) }}
	p.admit(c, "inbound")
	p.observe(meowcaller.SignalObservation{CallID: private, Kind: "transport", TransportType: private})
	p.end(c.id, "server:"+private)
	encoded, err := json.Marshal(samples)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), private) || len(samples) != 2 || samples[0].TransportType != "other" || samples[1].EndCategory != "server_error" {
		t.Fatalf("probe exposed private data or lost fixed classification: %s", encoded)
	}
	p.observe(meowcaller.SignalObservation{CallID: private, Kind: private})
	p.observe(meowcaller.SignalObservation{CallID: "different", Kind: "accept"})
	if len(samples) != 2 {
		t.Fatal("probe recorded unadmitted call or unclassified data")
	}
}

func TestSignalProbeRetainsEndAfterLifecycleCleanup(t *testing.T) {
	var samples []SignalDiagnostic
	m := newManager(nil, nil)
	m.probe = &nativeSignalProbe{now: time.Now, emit: func(s SignalDiagnostic) { samples = append(samples, s) }}
	c := &fakeCall{id: "call", peer: "private@lid"}
	if err := m.adopt(c, "inbound"); err != nil {
		t.Fatal(err)
	}
	c.ended = true
	c.state("ended")
	if m.active != nil {
		t.Fatal("existing ended lifecycle did not retire active handle")
	}
	c.end("timeout")
	if len(samples) != 1 || samples[0].Kind != "end" || samples[0].EndCategory != "timeout" || samples[0].Phase != "ended" {
		t.Fatalf("native end reason was erased by phase cleanup: %+v", samples)
	}
}

func TestSignalProbeObservesSetupKindsInReceivedOrder(t *testing.T) {
	var kinds []string
	p := &nativeSignalProbe{now: time.Now, emit: func(s SignalDiagnostic) { kinds = append(kinds, s.Kind) }}
	p.admit(&fakeCall{id: "call"}, "inbound")
	for _, kind := range []string{"offer", "relaylatency", "transport", "mute_v2", "accept", "terminate"} {
		p.observe(meowcaller.SignalObservation{CallID: "call", Kind: kind})
	}
	if strings.Join(kinds, ",") != "offer,relaylatency,transport,mute_v2,accept,terminate" {
		t.Fatalf("unexpected probe kinds: %v", kinds)
	}
}

func TestSignalProbeReplacesPriorAdmissionAndNoopsWhenAbsent(t *testing.T) {
	var count int
	p := &nativeSignalProbe{now: time.Now, emit: func(SignalDiagnostic) { count++ }}
	p.admit(&fakeCall{id: "old"}, "inbound")
	p.admit(&fakeCall{id: "new"}, "outbound")
	p.end("old", "timeout")
	p.observe(meowcaller.SignalObservation{CallID: "old", Kind: "accept"})
	p.observe(meowcaller.SignalObservation{CallID: "new", Kind: "accept"})
	if count != 1 {
		t.Fatal("probe recorded a retired admission")
	}
	var absent *nativeSignalProbe
	absent.admit(nil, "PRIVATE")
	absent.observe(meowcaller.SignalObservation{})
	absent.end("PRIVATE", "PRIVATE")
}

func TestSignalProbeConcurrentObservations(t *testing.T) {
	var count atomic.Int32
	p := &nativeSignalProbe{now: time.Now, emit: func(SignalDiagnostic) { count.Add(1) }}
	p.admit(&fakeCall{id: "call"}, "inbound")
	var work sync.WaitGroup
	for range 32 {
		work.Add(1)
		go func() { defer work.Done(); p.observe(meowcaller.SignalObservation{CallID: "call", Kind: "transport"}) }()
	}
	work.Wait()
	if count.Load() != 32 {
		t.Fatal("probe lost observations")
	}
}

func TestSignalProbeBoundsAndFiltersEarlyObservations(t *testing.T) {
	var count int
	p := &nativeSignalProbe{now: time.Now, emit: func(SignalDiagnostic) { count++ }}
	for range maxEarlySignals * 2 {
		p.observe(meowcaller.SignalObservation{CallID: "other", Kind: "offer"})
	}
	p.observe(meowcaller.SignalObservation{CallID: "owned", Kind: "offer"})
	if len(p.pending) != maxEarlySignals {
		t.Fatal("pre-admission observations were unbounded")
	}
	p.admit(&fakeCall{id: "owned"}, "inbound")
	if count != 1 || len(p.pending) != 0 {
		t.Fatal("admission exposed unrelated observations or retained pending data")
	}
}

func TestSignalProbeRetainsNativeSnapshotDuringAdmission(t *testing.T) {
	var samples []SignalDiagnostic
	p := &nativeSignalProbe{now: time.Now, emit: func(s SignalDiagnostic) { samples = append(samples, s) }}
	p.observe(meowcaller.SignalObservation{CallID: "owned", Kind: "offer", Phase: meowcaller.CallPhaseCalling})
	p.observe(meowcaller.SignalObservation{CallID: "owned", Kind: "relay_arrival", Phase: meowcaller.CallPhaseConnecting, RelayPresent: true})
	p.admit(&fakeCall{id: "owned"}, "outbound")
	if len(samples) != 2 || samples[0].RelayPresent || samples[0].Phase != "calling" || !samples[1].RelayPresent || samples[1].Phase != "connecting" || samples[1].MediaReady {
		t.Fatal("admission replaced boundary state or conflated allocation with media readiness")
	}
}
