package calling

import (
	"errors"
	"io"
	"testing"
	"time"
)

func TestOutputBacklogTimerPreservesFailureAndMeasuresStoppedReader(t *testing.T) {
	for _, priorRead := range []bool{false, true} {
		q := newPCMQueue()
		if priorRead {
			if _, err := q.ReadFrame(); err != nil {
				t.Fatal(err)
			}
		}
		frame := make([]byte, frameBytes)
		for i := 0; i < 2; i++ {
			if err := q.Push(frame); err != nil {
				t.Fatal(err)
			}
		}
		started := time.Now()
		err := q.Push(frame)
		var observed *MediaBackpressureError
		if !errors.Is(err, ErrMediaBackpressure) || !errors.As(err, &observed) || err.Error() != ErrMediaBackpressure.Error() {
			t.Fatalf("selected backlog lost its stable error identity: %T", err)
		}
		d := observed.Observation
		if d.WaitMS < outputBackpressureTimeout.Milliseconds() || d.WaitMS > time.Since(started).Milliseconds()+1 {
			t.Fatalf("wait is not the actual measured handoff: %d", d.WaitMS)
		}
		if d.QueueDepth != 2 || d.QueueCapacity != 2 || d.ReadFrameCallsDuringWait != 0 {
			t.Fatalf("stopped consumer snapshot is incorrect: %+v", d)
		}
		if !priorRead && d.LastReadAgeMS != -1 {
			t.Fatal("never-read source must be explicit")
		}
		if priorRead && d.LastReadAgeMS < d.WaitMS {
			t.Fatal("monotonic read age must include this wait")
		}
		q.Close()
	}
}

func TestOutputBacklogSnapshotCountsReadFrameProgress(t *testing.T) {
	q := newPCMQueue()
	frame := make([]byte, frameBytes)
	if err := q.Push(frame); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	before := q.readFrameCalls.Load()
	if _, err := q.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	// A silence frame is also consumer clock progress, not a queued packet.
	if _, err := q.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	d := q.backpressureError(started, before).Observation
	if d.ReadFrameCallsDuringWait != 2 || d.QueueDepth != 0 || d.QueueCapacity != 2 || d.LastReadAgeMS < 0 || d.LastReadAgeMS > d.WaitMS {
		t.Fatalf("real ReadFrame progress/age not measured: %+v", d)
	}
	q.Close()
}

func TestOutputBacklogDiagnosticsLeaveOtherQueueResultsUnchanged(t *testing.T) {
	q := newPCMQueue()
	if err := q.Push([]byte{1}); !errors.Is(err, ErrInvalidPCM) {
		t.Fatal("invalid PCM identity changed")
	}
	frame := make([]byte, frameBytes)
	if err := q.Push(frame); err != nil {
		t.Fatal("accepted frame became an observation error")
	}
	q.Close()
	if err := q.Push(frame); !errors.Is(err, io.EOF) {
		t.Fatal("closed source identity changed")
	}
}

func TestOutputBacklogReadTrackingDoesNotBlockConsumerBehindWaitingWriter(t *testing.T) {
	q := newPCMQueue()
	frame := make([]byte, frameBytes)
	for i := 0; i < 2; i++ {
		if err := q.Push(frame); err != nil {
			t.Fatal(err)
		}
	}
	written := make(chan error, 1)
	go func() { written <- q.Push(frame) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	read := make(chan error, 1)
	go func() { _, err := q.ReadFrame(); read <- err }()
	for _, result := range []<-chan error{read, written} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("clock tracking obstructed the primary handoff: %v", err)
			}
		case <-deadline.C:
			t.Fatal("reader waited behind the output writer")
		}
	}
	if q.readFrameCalls.Load() != 1 || q.lastReadNS.Load() == 0 {
		t.Fatal("accepted consumer frame lost its clock observation")
	}
	q.Close()
}
