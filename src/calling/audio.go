// Package calling adapts the pinned VoIP engine to bounded, ephemeral PCM streams.
package calling

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
)

const frameBytes = 960 * 2 // mono PCM16LE at 16 kHz, 60 ms
var ErrInvalidPCM = errors.New("invalid PCM frame")
var ErrMediaBackpressure = errors.New("media backlog exceeded 120 ms")

type pcmQueue struct {
	mu     sync.Mutex
	frames chan []byte
	done   chan struct{}
	closed bool
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
	select {
	case q.frames <- append([]byte(nil), data...):
		return nil
	default:
		return ErrMediaBackpressure
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
		q.mu.Lock()
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return nil, io.EOF
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
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.done)
	}
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
