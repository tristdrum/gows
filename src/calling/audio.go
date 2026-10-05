// Package calling adapts the pinned VoIP engine to bounded, ephemeral PCM streams.
package calling

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const frameBytes = 960 * 2 // mono PCM16LE at 16 kHz, 60 ms
const outputBackpressureTimeout = 120 * time.Millisecond

var ErrInvalidPCM = errors.New("invalid PCM frame")
var ErrMediaBackpressure = errors.New("media backlog exceeded 120 ms")

// OutputBacklogObservation contains only clock and queue counters, never PCM.
// ReadFrame calls measure consumer entry, not codec or relay-send completion.
type OutputBacklogObservation struct {
	WaitMS                   int64
	QueueDepth               int
	QueueCapacity            int
	ReadFrameCallsDuringWait uint64
	LastReadAgeMS            int64 // -1 means ReadFrame has never been called
}

// MediaBackpressureError preserves the stable selected backlog failure while
// carrying its numeric observation across the native/server boundary.
type MediaBackpressureError struct {
	Observation OutputBacklogObservation
}

func (e *MediaBackpressureError) Error() string { return ErrMediaBackpressure.Error() }
func (e *MediaBackpressureError) Unwrap() error { return ErrMediaBackpressure }

type pcmQueue struct {
	mu             sync.Mutex
	frames         chan []byte
	done           chan struct{}
	closed         bool
	closeOnce      sync.Once
	created        time.Time
	readFrameCalls atomic.Uint64
	lastReadNS     atomic.Int64
}

func newPCMQueue() *pcmQueue {
	return &pcmQueue{frames: make(chan []byte, 2), done: make(chan struct{}), created: time.Now()}
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
		readsAtWait := q.readFrameCalls.Load()
		waitStarted := time.Now()
		timer := time.NewTimer(outputBackpressureTimeout)
		defer timer.Stop()
		select {
		case <-q.done:
			return io.EOF
		case q.frames <- frame:
		case <-timer.C:
			return q.backpressureError(waitStarted, readsAtWait)
		}
	}
	select {
	case <-q.done:
		return io.EOF
	default:
		return nil
	}
}

func (q *pcmQueue) backpressureError(waitStarted time.Time, readsAtWait uint64) *MediaBackpressureError {
	reads := q.readFrameCalls.Load()
	lastRead := q.lastReadNS.Load()
	now := time.Now()
	lastReadAge := int64(-1)
	if lastRead != 0 {
		lastReadAge = (now.Sub(q.created).Nanoseconds() - (lastRead - 1)) / int64(time.Millisecond)
	}
	return &MediaBackpressureError{Observation: OutputBacklogObservation{
		WaitMS: now.Sub(waitStarted).Milliseconds(), QueueDepth: len(q.frames), QueueCapacity: cap(q.frames),
		ReadFrameCallsDuringWait: reads - readsAtWait, LastReadAgeMS: lastReadAge,
	}}
}

func (q *pcmQueue) ReadFrame() ([]float32, error) {
	// Publish clock progress independently of the blocked writer's mutex.
	q.lastReadNS.Store(time.Since(q.created).Nanoseconds() + 1)
	q.readFrameCalls.Add(1)
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
