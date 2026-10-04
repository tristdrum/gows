package calling

import (
	"sync"
	"time"

	"github.com/purpshell/meowcaller"
)

// SignalDiagnostic contains only fixed categories and elapsed time. Call identity
// is used privately to filter observations and never enters this value.
type SignalDiagnostic struct {
	RelayPresent  bool   `json:"relay_present"`
	MediaReady    bool   `json:"media_ready"`
	Kind          string `json:"kind"`
	Direction     string `json:"direction"`
	Phase         string `json:"phase"`
	ElapsedMs     int64  `json:"elapsed_ms"`
	TransportType string `json:"transport_type,omitempty"`
	EndCategory   string `json:"end_category,omitempty"`
}

type probeObservation struct {
	meowcaller.SignalObservation
	at          time.Time
	endCategory string
}

const maxEarlySignals = 16

type nativeSignalProbe struct {
	mu        sync.Mutex
	call      handle
	direction string
	started   time.Time
	pending   []probeObservation
	flushing  bool
	emit      func(SignalDiagnostic)
	now       func() time.Time
}

func (p *nativeSignalProbe) admit(c handle, direction string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.call, p.direction, p.started, p.flushing = c, direction, p.now(), true
	for _, observation := range p.pending {
		if observation.CallID == c.ID() && observation.at.Before(p.started) {
			p.started = observation.at
		}
	}
	for {
		pending := p.pending
		p.pending = nil
		if len(pending) == 0 {
			p.flushing = false
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
		for _, observation := range pending {
			p.sample(observation)
		}
		p.mu.Lock()
	}
}

func (p *nativeSignalProbe) observe(observation meowcaller.SignalObservation) {
	if p == nil || observation.CallID == "" {
		return
	}
	switch observation.Kind {
	case "offer", "preaccept", "relaylatency", "transport", "accept", "mute_v2", "terminate", "reject",
		"sent_offer", "sent_offer_receipt", "sent_preaccept", "sent_relaylatency", "sent_transport", "sent_accept", "sent_mute_v2", "sent_terminate", "sent_reject", "offer_ack", "relay_arrival":
	default:
		return
	}
	if observation.Kind == "transport" || observation.Kind == "sent_transport" {
		switch observation.TransportType {
		case "1", "3", "9":
		default:
			observation.TransportType = "other"
		}
	} else {
		observation.TransportType = ""
	}
	sample := probeObservation{SignalObservation: observation, at: p.now()}
	p.mu.Lock()
	if p.call == nil || p.call.ID() != observation.CallID || p.flushing {
		if len(p.pending) == maxEarlySignals {
			p.pending = p.pending[1:]
		}
		p.pending = append(p.pending, sample)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	p.sample(sample)
}

func (p *nativeSignalProbe) end(id, reason string) {
	if p == nil {
		return
	}
	category := "other"
	switch reason {
	case "normal", "timeout", "busy", "accepted_elsewhere", "hangup", "rejected", "offer_send_failed", "accept_send_failed":
		category = reason
	default:
		if len(reason) >= 7 && reason[:7] == "server:" {
			category = "server_error"
		}
	}
	p.mu.Lock()
	call := p.call
	p.mu.Unlock()
	relayPresent := false
	if call != nil && call.ID() == id {
		if relayState, ok := call.(interface{ RelayPresent() bool }); ok {
			relayPresent = relayState.RelayPresent()
		}
	}
	p.sample(probeObservation{SignalObservation: meowcaller.SignalObservation{CallID: id, Kind: "end", Phase: meowcaller.CallPhaseEnded, RelayPresent: relayPresent}, at: p.now(), endCategory: category})
}

func (p *nativeSignalProbe) sample(observation probeObservation) {
	p.mu.Lock()
	if p.call == nil || p.call.ID() != observation.CallID {
		p.mu.Unlock()
		return
	}
	direction, started, emit := p.direction, p.started, p.emit
	p.mu.Unlock()
	state := phase(observation.Phase)
	if direction != "inbound" && direction != "outbound" {
		direction = "unavailable"
	}
	if emit != nil {
		emit(SignalDiagnostic{Kind: observation.Kind, Direction: direction, Phase: state, ElapsedMs: observation.at.Sub(started).Milliseconds(), TransportType: observation.TransportType, EndCategory: observation.endCategory, RelayPresent: observation.RelayPresent, MediaReady: state == "active"})
	}
}
