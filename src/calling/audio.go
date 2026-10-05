// Package calling adapts the pinned VoIP engine to bounded, ephemeral PCM streams.
package calling

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"time"
)

const frameBytes = 960 * 2 // mono PCM16LE at 16 kHz, 60 ms
const outputBackpressureTimeout = 120 * time.Millisecond

var ErrInvalidPCM = errors.New("invalid PCM frame")
var ErrMediaBackpressure = errors.New("media backlog exceeded 120 ms")

type pcmQueue struct {
	mu        sync.Mutex
	frames    chan []byte
	done      chan struct{}
	closed    bool
	closeOnce sync.Once
}

func newPCMQueue() *pcmQueue {
	return &pcmQueue{frames: make(chan []byte, 2), done: make(chan struct{})}
}

func (q *pcmQueue) Push(data []byte) error {
	if len(data) != frameBytes {
		return ErrInvalidPCM
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return io.EOF
	}
	frame := append([]byte(nil), data...)
	select {
	case <-q.done:
		return io.EOF
	case q.frames <- frame:
	default:
		// A momentarily full queue is not a measured stall. Let the engine's
		// existing frame clock free a slot within the declared handoff budget.
		timer := time.NewTimer(outputBackpressureTimeout)
		defer timer.Stop()
		select {
		case <-q.done:
			return io.EOF
		case q.frames <- frame:
		case <-timer.C:
			return ErrMediaBackpressure
		}
	}
	select {
	case <-q.done:
		return io.EOF
	default:
		return nil
	}
}

func (q *pcmQueue) ReadFrame() ([]float32, error) {
	select {
	case <-q.done:
		return nil, io.EOF
	default:
	}
	select {
	case <-q.done:
		return nil, io.EOF
	case data := <-q.frames:
		// Close publishes cancellation before taking the writer lock. The
		// clocked consumer must never wait behind a full-queue writer.
		select {
		case <-q.done:
			return nil, io.EOF
		default:
		}
		frame := make([]float32, 960)
		for i := range frame {
			frame[i] = float32(int16(binary.LittleEndian.Uint16(data[2*i:]))) / 32768
		}
		return frame, nil
	default:
		// The engine owns the 60 ms clock. Never stall its relay send loop
		// while the voice backend is listening or completing a tool action.
		return make([]float32, 960), nil
	}
}

func (q *pcmQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		select {
		case <-q.frames:
		default:
			return
		}
	}
}

func (q *pcmQueue) Close() error {
	// Cancel a waiting writer before taking its lock, so hangup never waits
	// for the output backpressure budget to expire.
	q.closeOnce.Do(func() { close(q.done) })
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for {
		select {
		case <-q.frames:
		default:
			return nil
		}
	}
}

func encodePCM(frame []float32) []byte {
	data := make([]byte, len(frame)*2)
	for i, sample := range frame {
		v := float64(sample)
		if math.IsNaN(v) {
			v = 0
		}
		v = math.Max(-32768, math.Min(32767, math.Round(v*32768)))
		binary.LittleEndian.PutUint16(data[2*i:], uint16(int16(v)))
	}
	return data
}
