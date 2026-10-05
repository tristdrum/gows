package calling

import (
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"
)

func TestPCMQueueReadDoesNotWaitForOutputWriterLock(t *testing.T) {
	q := fullOutputQueue(t)
	q.mu.Lock() // A full-queue writer owns this lock during its bounded handoff.
	defer q.mu.Unlock()
	result := make(chan error, 1)
	go func() {
		frame, err := q.ReadFrame()
		if err == nil && frame[0] != .25 {
			err = errors.New("available output frame changed")
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(40 * time.Millisecond):
		t.Fatal("clocked output reader waited on the full-queue writer lock")
	}
}

func TestPCMQueueReadCancelsBeforeCloseCanTakeWriterLock(t *testing.T) {
	q := fullOutputQueue(t)
	q.mu.Lock()
	closed := make(chan struct{})
	go func() { _ = q.Close(); close(closed) }()
	select {
	case <-q.done:
	case <-time.After(time.Second):
		q.mu.Unlock()
		t.Fatal("owned cancellation waited on the output writer")
	}
	// Cancellation is authoritative even before Close can publish q.closed or
	// drain its queued frames under the writer's lock.
	frame, err := q.ReadFrame()
	q.mu.Unlock()
	if frame != nil || !errors.Is(err, io.EOF) {
		t.Fatalf("closing output returned queued audio: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("owned Close did not finish after the writer lock was released")
	}
	if _, err := q.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("closed source returned audio after cancellation: %v", err)
	}
}

func TestPCMQueueClockedConsumerDrainsConcurrentBurst(t *testing.T) {
	q := fullOutputQueue(t)
	result := make(chan error, 1)
	go func() {
		for marker := uint16(3); marker <= 10; marker++ {
			frame := make([]byte, frameBytes)
			binary.LittleEndian.PutUint16(frame, marker*1024)
			if err := q.Push(frame); err != nil {
				result <- err
				return
			}
		}
		result <- nil
	}()
	// Start the serial consumer only after the real producer is waiting with
	// a full queue. There is no second reader to free slots behind its back.
	deadline := time.Now().Add(time.Second)
	for q.mu.TryLock() {
		q.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("output producer did not begin its bounded handoff")
		}
		runtime.Gosched()
	}
	clock := time.NewTicker(60 * time.Millisecond)
	defer clock.Stop()
	want := []float32{.25, .5}
	for marker := 3; marker <= 10; marker++ {
		want = append(want, float32(marker)/32)
	}
	for index, value := range want {
		if index > 0 {
			<-clock.C
		}
		frame, err := q.ReadFrame()
		if err != nil || frame[0] != value {
			t.Fatalf("serial output clock lost FIFO progress at frame %d: %v", index, err)
		}
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("healthy serial consumer became a persistent output stall: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("burst producer did not finish after the consumer drained it")
	}
	if cap(q.frames) != 2 {
		t.Fatal("consumer recovery expanded the output queue")
	}
}
