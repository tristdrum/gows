package calling

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeCall struct {
	id, peer    string
	ended       bool
	unsupported bool
	end         func(string)
	state       func(string)
}

type uncertainDialer struct {
	started chan struct{}
	release chan struct{}
	call    *fakeCall
}

func (d *uncertainDialer) Dial(ctx context.Context, peer string) (handle, error) {
	close(d.started)
	<-d.release // Model an offer success racing a canceled/lost HTTP response.
	return d.call, nil
}
func TestCancelAttemptFencesLateDialAndNeverRedials(t *testing.T) {
	d := &uncertainDialer{make(chan struct{}), make(chan struct{}), &fakeCall{id: "uncertain", peer: "1@lid"}}
	m := newManager(d, nil)
	result := make(chan error, 1)
	go func() { _, err := m.Dial(context.Background(), "1@s.whatsapp.net", "lost-response"); result <- err }()
	<-d.started
	state, err := m.CancelAttempt("lost-response")
	if err != nil || state.State != "dialing" {
		t.Fatalf("pending cancellation not tracked: %+v %v", state, err)
	}
	close(d.release)
	select {
	case err = <-result:
		if err == nil {
			t.Fatal("canceled dial adopted")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not settle")
	}
	state, err = m.AttemptStatus("lost-response")
	if err != nil || state.ID != "uncertain" || state.State != "ended" || !d.call.ended {
		t.Fatalf("uncertain call not retired: %+v %v", state, err)
	}
	if _, err = m.Dial(context.Background(), "1@s.whatsapp.net", "lost-response"); !errors.Is(err, ErrAttemptUsed) {
		t.Fatal("canceled offer replayed")
	}
}
func TestCancelBeforeDelayedDialConsumesAttempt(t *testing.T) {
	d := &fakeDialer{}
	m := newManager(d, nil)
	state, err := m.CancelAttempt("not-yet-received")
	if err != nil || state.State != "ended" {
		t.Fatal("cancellation tombstone missing")
	}
	if _, err = m.Dial(context.Background(), "1@s.whatsapp.net", "not-yet-received"); !errors.Is(err, ErrAttemptUsed) || d.n != 0 {
		t.Fatal("late request created offer")
	}
}
func TestAttemptLookupRecoversActiveIDAndCancelClosesIt(t *testing.T) {
	m := newManager(&fakeDialer{}, nil)
	if _, err := m.Dial(context.Background(), "1@s.whatsapp.net", "response-lost"); err != nil {
		t.Fatal(err)
	}
	state, err := m.AttemptStatus("response-lost")
	if err != nil || state.ID != "call" || state.State != "ringing" {
		t.Fatalf("active attempt unavailable: %+v %v", state, err)
	}
	state, err = m.CancelAttempt("response-lost")
	if err != nil || state.ID != "call" || state.State != "ended" {
		t.Fatalf("recovered call not canceled: %+v %v", state, err)
	}
}

func (c *fakeCall) ID() string        { return c.id }
func (c *fakeCall) Peer() string      { return c.peer }
func (c *fakeCall) Unsupported() bool { return c.unsupported }
func (c *fakeCall) State() string {
	if c.ended {
		return "ended"
	}
	return "ringing"
}
func (c *fakeCall) Answer() error { return nil }
func (c *fakeCall) Reject() error { return c.Hangup() }
func (c *fakeCall) Hangup() error {
	c.ended = true
	if c.end != nil {
		c.end("hangup")
	}
	return nil
}
func (c *fakeCall) Audio(*pcmQueue, *Stream) {}
func (c *fakeCall) OnEnd(fn func(string))    { c.end = fn }
func (c *fakeCall) OnState(fn func(string))  { c.state = fn }

type fakeDialer struct{ n int }

func (d *fakeDialer) Dial(context.Context, string) (handle, error) {
	d.n++
	return &fakeCall{id: "call", peer: "1@lid"}, nil
}

func TestDialRequestIsNeverRepeatedAndOnlyOneCall(t *testing.T) {
	d := &fakeDialer{}
	m := newManager(d, nil)
	defer m.Close()
	if _, err := m.Dial(context.Background(), "1@s.whatsapp.net", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Dial(context.Background(), "1@s.whatsapp.net", "attempt-2"); !errors.Is(err, ErrBusy) {
		t.Fatalf("parallel dial permitted: %v", err)
	}
	m.Hangup("call")
	if _, err := m.Dial(context.Background(), "1@s.whatsapp.net", "attempt-1"); !errors.Is(err, ErrAttemptUsed) {
		t.Fatalf("replayed request: %v", err)
	}
	if d.n != 1 {
		t.Fatal("duplicate outbound offer")
	}
}

func TestMediaHasOneOwnerAndCloseEndsCall(t *testing.T) {
	m := newManager(&fakeDialer{}, nil)
	defer m.Close()
	m.Dial(context.Background(), "1@s.whatsapp.net", "attempt")
	stream, err := m.Open("call")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Open("call"); !errors.Is(err, ErrMediaOwned) {
		t.Fatalf("second media owner allowed: %v", err)
	}
	stream.Close()
	select {
	case <-stream.Done:
	default:
		t.Fatal("media close retained call")
	}
	if _, err = m.Open("call"); err == nil {
		t.Fatal("ended audio session resumed")
	}
}

func TestUnknownCallAndBusyIncomingCannotDisplaceOwner(t *testing.T) {
	m := newManager(&fakeDialer{}, nil)
	defer m.Close()
	m.Dial(context.Background(), "1@s.whatsapp.net", "attempt")
	in := &fakeCall{id: "other", peer: "2@lid"}
	m.adopt(in, "inbound")
	if !in.ended {
		t.Fatal("busy incoming left ringing")
	}
	if err := m.Accept("other"); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("unknown accept: %v", err)
	}
	if _, err := m.Status("call"); err != nil {
		t.Fatal("existing owner lost")
	}
}

func TestUnsupportedIncomingNeverOwnsMedia(t *testing.T) {
	m := newManager(&fakeDialer{}, nil)
	c := &fakeCall{id: "group", unsupported: true}
	if err := m.adopt(c, "inbound"); err == nil || !c.ended {
		t.Fatal("unsupported invitation adopted")
	}
	if _, err := m.Open(c.ID()); !errors.Is(err, ErrCallNotFound) {
		t.Fatal("unsupported invitation can own media")
	}
}

func TestLateStateCannotResurrectEndedCall(t *testing.T) {
	var states []string
	m := newManager(&fakeDialer{}, func(e Event) { states = append(states, e.State) })
	c := &fakeCall{id: "late"}
	if err := m.adopt(c, "inbound"); err != nil {
		t.Fatal(err)
	}
	if err := m.Hangup(c.ID()); err != nil {
		t.Fatal(err)
	}
	c.state("active")
	c.state("ended")
	if len(states) != 2 || states[0] != "ringing" || states[1] != "ended" {
		t.Fatalf("terminal state resurrected: %v", states)
	}
}
