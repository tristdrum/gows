package calling

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
)

var ErrBusy = errors.New("session already has a call")
var ErrAttemptUsed = errors.New("outbound attempt already used")
var ErrCallNotFound = errors.New("call not found")
var ErrMediaOwned = errors.New("call media already has an owner")

type Status struct{ ID, Peer, State, Direction string }

// Event contains lifecycle metadata only. Keys, relay credentials and audio
// never enter the existing webhook/event stream.
type Event struct {
	ID        string `json:"id"`
	Peer      string `json:"peer"`
	State     string `json:"state"`
	Direction string `json:"direction"`
}

type handle interface {
	ID() string
	Peer() string
	State() string
	Unsupported() bool
	Answer() error
	Reject() error
	Hangup() error
	Audio(*pcmQueue, *Stream)
	OnEnd(func(string))
	OnState(func(string))
}
type dialer interface {
	Dial(context.Context, string) (handle, error)
}
type nativeDialer struct{ client *meowcaller.Client }

func (d nativeDialer) Dial(ctx context.Context, peer string) (handle, error) {
	c, err := d.client.Call(ctx, peer)
	if c == nil {
		return nil, err
	}
	return nativeCall{c}, err
}

type nativeCall struct{ call *meowcaller.Call }

func (c nativeCall) ID() string                   { return c.call.ID() }
func (c nativeCall) Peer() string                 { return c.call.Peer().ToNonAD().String() }
func (c nativeCall) State() string                { return phase(c.call.State()) }
func (c nativeCall) Unsupported() bool            { return c.call.IsVideo() || c.call.IsGroup() }
func (c nativeCall) Answer() error                { return c.call.Answer() }
func (c nativeCall) Reject() error                { return c.call.Reject() }
func (c nativeCall) Hangup() error                { return c.call.Hangup() }
func (c nativeCall) Audio(q *pcmQueue, s *Stream) { c.call.Receive(s); c.call.Play(q) }
func (c nativeCall) OnEnd(fn func(string))        { c.call.OnEnd(fn) }
func (c nativeCall) OnState(fn func(string)) {
	c.call.OnStateChange(func(p meowcaller.CallPhase) { fn(phase(p)) })
}
func phase(p meowcaller.CallPhase) string {
	switch p {
	case meowcaller.CallPhaseCalling:
		return "calling"
	case meowcaller.CallPhaseRinging:
		return "ringing"
	case meowcaller.CallPhaseConnecting:
		return "connecting"
	case meowcaller.CallPhaseActive:
		return "active"
	case meowcaller.CallPhaseEnded:
		return "ended"
	default:
		return "unavailable"
	}
}

type record struct {
	call             handle
	direction        string
	stream           *Stream
	timer, ringTimer *time.Timer
	terminal         bool
}
type attempt struct {
	id                string
	cancel            context.CancelFunc
	canceled, settled bool
}
type Manager struct {
	mu              sync.Mutex
	eventMu         sync.Mutex
	dialer          dialer
	emit            func(Event)
	active          *record
	dialing, closed bool
	used            map[string]*attempt
}

// New installs the calling adapter before the existing client connects.
// The caller must gate construction to explicitly enabled sessions.
func New(client *whatsmeow.Client, emit func(Event)) *Manager {
	voice := meowcaller.NewClient(client)
	m := newManager(nativeDialer{voice}, emit)
	voice.OnIncomingCall(func(c *meowcaller.Call) {
		m.adopt(nativeCall{c}, "inbound")
	})
	return m
}
func newManager(d dialer, emit func(Event)) *Manager {
	return &Manager{dialer: d, emit: emit, used: make(map[string]*attempt)}
}

func (m *Manager) Dial(ctx context.Context, peer, requestID string) (Status, error) {
	if strings.TrimSpace(requestID) == "" || len(requestID) > 128 {
		return Status{}, ErrAttemptUsed
	}
	m.mu.Lock()
	if _, used := m.used[requestID]; used {
		m.mu.Unlock()
		return Status{}, ErrAttemptUsed
	}
	if m.closed || m.active != nil || m.dialing {
		m.mu.Unlock()
		return Status{}, ErrBusy
	}
	// Bound retained attempts and fail closed instead of evicting dedupe keys.
	if len(m.used) >= 256 {
		m.mu.Unlock()
		return Status{}, ErrAttemptUsed
	}
	dialCtx, cancel := context.WithCancel(ctx)
	a := &attempt{cancel: cancel}
	m.used[requestID] = a
	m.dialing = true
	m.mu.Unlock()
	defer cancel()
	c, err := m.dialer.Dial(dialCtx, peer)
	if err == nil && c == nil {
		err = ErrCallNotFound
	}
	m.mu.Lock()
	if c != nil {
		a.id = c.ID()
	}
	canceled := a.canceled || m.closed
	m.mu.Unlock()
	if err == nil && canceled {
		err = context.Canceled
	}
	if err == nil {
		err = m.adopt(c, "outbound")
	}
	// Cancellation may arrive between the first check and adoption.
	m.mu.Lock()
	canceled = a.canceled || m.closed
	// Linearize successful settlement with the last cancellation check. A
	// later cancel must see settled=true and owns hangup of the adopted call.
	if err == nil && !canceled {
		a.settled = true
		a.cancel = nil
		m.dialing = false
	}
	m.mu.Unlock()
	if err == nil && canceled {
		_ = m.Hangup(c.ID())
		err = context.Canceled
	}
	if err != nil && c != nil && c.State() != "ended" {
		_ = c.Hangup()
	}
	if err != nil {
		m.mu.Lock()
		a.settled = true
		a.cancel = nil
		m.dialing = false
		m.mu.Unlock()
	}
	if err != nil {
		return Status{}, err
	}
	return m.Status(c.ID())
}

// AttemptStatus recovers an offer's identity after a lost control response.
// A dialing result is nonterminal, including while cancellation is settling.
func (m *Manager) AttemptStatus(requestID string) (Status, error) {
	m.mu.Lock()
	a := m.used[requestID]
	if a == nil {
		m.mu.Unlock()
		return Status{}, ErrCallNotFound
	}
	result := Status{ID: a.id, State: "ended", Direction: "outbound"}
	if !a.settled {
		result.State = "dialing"
	}
	r := m.active
	if a.settled && r != nil && r.call.ID() == a.id {
		result.Peer = r.call.Peer()
		result.State = r.call.State()
	}
	m.mu.Unlock()
	return result, nil
}

// CancelAttempt consumes a not-yet-received request and fences late dial success.
// It never creates or retries an offer. Retention has the same bounded lifetime
// as outbound dedupe; the Min coordinator additionally owns durable intents.
func (m *Manager) CancelAttempt(requestID string) (Status, error) {
	if strings.TrimSpace(requestID) == "" || len(requestID) > 128 {
		return Status{}, ErrAttemptUsed
	}
	m.mu.Lock()
	a := m.used[requestID]
	if a == nil {
		if len(m.used) >= 256 {
			m.mu.Unlock()
			return Status{}, ErrAttemptUsed
		}
		a = &attempt{settled: true}
		m.used[requestID] = a
	}
	a.canceled = true
	cancel, id, settled := a.cancel, a.id, a.settled
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if settled && id != "" {
		if err := m.Hangup(id); err != nil && !errors.Is(err, ErrCallNotFound) {
			return Status{}, err
		}
	}
	return m.AttemptStatus(requestID)
}

func (m *Manager) adopt(c handle, direction string) error {
	if c.Unsupported() {
		_ = c.Reject()
		return errors.New("only direct audio calls are supported")
	}
	m.mu.Lock()
	if direction == "outbound" {
		m.dialing = false
	}
	if m.closed || m.active != nil || m.dialing {
		m.mu.Unlock()
		_ = c.Reject()
		return ErrBusy
	}
	r := &record{call: c, direction: direction}
	m.active = r
	r.timer = time.AfterFunc(10*time.Minute, func() { _ = m.Hangup(c.ID()) })
	r.ringTimer = time.AfterFunc(30*time.Second, func() {
		if c.State() != "active" {
			_ = m.Hangup(c.ID())
		}
	})
	m.mu.Unlock()
	c.OnEnd(func(string) { m.finish(r) })
	c.OnState(func(state string) { m.sendEvent(r, state) })
	m.sendEvent(r, c.State())
	if c.State() == "ended" {
		m.finish(r)
	}
	return nil
}
func (m *Manager) sendEvent(r *record, state string) {
	if state == "ended" {
		m.finish(r)
		return
	}
	m.eventMu.Lock()
	defer m.eventMu.Unlock()
	m.mu.Lock()
	current := m.active == r && !r.terminal && !m.closed
	m.mu.Unlock()
	if !current {
		return
	}
	if m.emit != nil {
		m.emit(Event{ID: r.call.ID(), Peer: r.call.Peer(), Direction: r.direction, State: state})
	}
}
func (m *Manager) find(id string) (*record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || m.active.call.ID() != id || m.closed {
		return nil, ErrCallNotFound
	}
	return m.active, nil
}
func (m *Manager) Status(id string) (Status, error) {
	r, err := m.find(id)
	if err != nil {
		return Status{}, err
	}
	return Status{ID: r.call.ID(), Peer: r.call.Peer(), State: r.call.State(), Direction: r.direction}, nil
}
func (m *Manager) Accept(id string) error {
	r, err := m.find(id)
	if err != nil {
		return err
	}
	m.mu.Lock()
	attached := r.stream != nil
	m.mu.Unlock()
	if !attached {
		return errors.New("attach media before accepting")
	}
	if r.direction != "inbound" {
		return errors.New("only inbound calls can be accepted")
	}
	return r.call.Answer()
}
func (m *Manager) Hangup(id string) error {
	r, err := m.find(id)
	if err != nil {
		return err
	}
	m.finish(r)
	err = r.call.Hangup()
	return err
}
func (m *Manager) finish(r *record) {
	m.eventMu.Lock()
	m.mu.Lock()
	if m.active != r {
		m.mu.Unlock()
		m.eventMu.Unlock()
		return
	}
	r.terminal = true
	m.active = nil
	r.timer.Stop()
	r.ringTimer.Stop()
	s := r.stream
	m.mu.Unlock()
	if m.emit != nil {
		m.emit(Event{ID: r.call.ID(), Peer: r.call.Peer(), Direction: r.direction, State: "ended"})
	}
	m.eventMu.Unlock()
	if s != nil {
		s.finish()
	}
}
func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	for _, a := range m.used {
		a.canceled = true
		if a.cancel != nil {
			a.cancel()
		}
	}
	r := m.active
	m.mu.Unlock()
	if r != nil {
		_ = r.call.Hangup()
		m.finish(r)
	}
}

type Stream struct {
	Input   chan []byte
	Done    chan struct{}
	output  *pcmQueue
	manager *Manager
	callID  string
	once    sync.Once
	failure sync.Once
}

func (m *Manager) Open(id string) (*Stream, error) {
	m.mu.Lock()
	r := m.active
	if r == nil || r.call.ID() != id || m.closed {
		m.mu.Unlock()
		return nil, ErrCallNotFound
	}
	if r.stream != nil {
		m.mu.Unlock()
		return nil, ErrMediaOwned
	}
	s := &Stream{Input: make(chan []byte, 2), Done: make(chan struct{}), output: newPCMQueue(), manager: m, callID: id}
	r.stream = s
	r.call.Audio(s.output, s)
	m.mu.Unlock()
	return s, nil
}
func (s *Stream) Write(data []byte) error { return s.output.Push(data) }
func (s *Stream) Clear()                  { s.output.Clear() }
func (s *Stream) WriteFrame(frame []float32) error {
	if len(frame) != 960 {
		s.fail()
		return ErrInvalidPCM
	}
	select {
	case <-s.Done:
		return io.EOF
	default:
	}
	select {
	case s.Input <- encodePCM(frame):
		return nil
	default:
		s.fail()
		return ErrMediaBackpressure
	}
}
func (s *Stream) fail()   { s.failure.Do(func() { go s.Close() }) }
func (s *Stream) finish() { s.once.Do(func() { s.output.Close(); close(s.Done) }) }
func (s *Stream) Close() error {
	s.finish()
	err := s.manager.Hangup(s.callID)
	if errors.Is(err, ErrCallNotFound) {
		return nil
	}
	return err
}
