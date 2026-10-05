package calling

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"
)

func fullOutputQueue(t *testing.T) *pcmQueue {
	t.Helper()
	q := newPCMQueue()
	t.Cleanup(func() { _ = q.Close() })
	for _, marker := range []uint16{8192, 16384} {
		frame := make([]byte, frameBytes)
		binary.LittleEndian.PutUint16(frame, marker)
		if err := q.Push(frame); err != nil {
			t.Fatal(err)
		}
	}
	return q
}

func pendingOutputWrite(t *testing.T, q *pcmQueue) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		frame := make([]byte, frameBytes)
		binary.LittleEndian.PutUint16(frame, 24576)
		result <- q.Push(frame)
	}()
	select {
	case err := <-result:
		t.Fatalf("full output queue rejected before its consumer could resume: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	return result
}

func TestPCMQueueOutputRecoversWithinBackpressureBudget(t *testing.T) {
	q := fullOutputQueue(t)
	result := pendingOutputWrite(t, q)
	frame, err := q.ReadFrame()
	if err != nil || frame[0] != .25 {
		t.Fatalf("oldest output frame changed: %v", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("clocked consumer resumed but the call still failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("output writer did not resume when its consumer freed a slot")
	}
	if cap(q.frames) != 2 {
		t.Fatal("recovery expanded the output queue")
	}
	for _, want := range []float32{.5, .75} {
		frame, err := q.ReadFrame()
		if err != nil || frame[0] != want {
			t.Fatalf("output ordering changed: %v", err)
		}
	}
}

func TestPCMQueueOutputPersistentStallKeepsTypedFailure(t *testing.T) {
	q := fullOutputQueue(t)
	started := time.Now()
	err := q.Push(make([]byte, frameBytes))
	elapsed := time.Since(started)
	if !errors.Is(err, ErrMediaBackpressure) {
		t.Fatalf("persistent output stall lost its typed failure: %v", err)
	}
	if elapsed < 100*time.Millisecond || elapsed > time.Second {
		t.Fatalf("output stall was not bounded by the declared 120 ms handoff budget: %v", elapsed)
	}
	if cap(q.frames) != 2 || len(q.frames) != 2 {
		t.Fatal("persistent stall expanded or discarded the queued output")
	}
}

func TestPCMQueueOutputCloseCancelsPendingWrite(t *testing.T) {
	q := fullOutputQueue(t)
	result := pendingOutputWrite(t, q)
	closed := make(chan struct{})
	go func() {
		_ = q.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(80 * time.Millisecond):
		t.Fatal("hangup waited for the output backpressure timeout")
	}
	select {
	case err := <-result:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("hangup became an output backlog failure: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("hangup did not cancel the pending output write")
	}
	if len(q.frames) != 0 {
		t.Fatal("hangup retained queued output")
	}
	if err := q.Push(make([]byte, frameBytes)); !errors.Is(err, io.EOF) {
		t.Fatalf("closed output accepted a later frame: %v", err)
	}
	if _, err := q.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("closed output played retained audio: %v", err)
	}
}
