package calling

import (
	"testing"
	"time"

	"github.com/purpshell/meowcaller"
)

func TestSignalProbeRetainsNativePreAdmissionOrdering(t *testing.T) {
	var samples []SignalDiagnostic
	now := time.Unix(0, 0)
	p := &nativeSignalProbe{now: func() time.Time { return now }, emit: func(s SignalDiagnostic) { samples = append(samples, s) }}
	p.observe(meowcaller.SignalObservation{CallID: "owned", Kind: "offer"})
	now = now.Add(10 * time.Millisecond)
	p.observe(meowcaller.SignalObservation{CallID: "owned", Kind: "sent_offer_receipt"})
	now = now.Add(10 * time.Millisecond)
	p.observe(meowcaller.SignalObservation{CallID: "other", Kind: "accept"})
	now = now.Add(10 * time.Millisecond)
	p.observe(meowcaller.SignalObservation{CallID: "owned", Kind: "sent_preaccept"})
	if len(samples) != 0 {
		t.Fatal("unadmitted call entered diagnostics")
	}
	p.admit(&fakeCall{id: "owned"}, "inbound")
	if len(samples) != 3 || samples[0].Kind != "offer" || samples[1].Kind != "sent_offer_receipt" || samples[2].Kind != "sent_preaccept" {
		t.Fatalf("native setup before admission was lost or reordered: %+v", samples)
	}
	if samples[0].ElapsedMs != 0 || samples[1].ElapsedMs != 10 || samples[2].ElapsedMs != 30 {
		t.Fatal("buffered observations lost their actual elapsed times")
	}
}

func TestSignalProbeAdmissionCallbackCanReadManager(t *testing.T) {
	m := newManager(nil, nil)
	c := &fakeCall{id: "owned"}
	m.probe = &nativeSignalProbe{now: time.Now, emit: func(SignalDiagnostic) { _, _ = m.Status(c.id) }}
	m.probe.observe(meowcaller.SignalObservation{CallID: c.id, Kind: "offer"})
	done := make(chan error, 1)
	go func() { done <- m.adopt(c, "inbound") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("diagnostic callback ran under the manager lock")
	}
}
