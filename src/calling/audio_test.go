package calling

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestPCMQueueRejectsInvalidFramesAndBoundsBacklog(t *testing.T) {
	q := newPCMQueue()
	if err := q.Push([]byte{1}); !errors.Is(err, ErrInvalidPCM) {
		t.Fatalf("odd PCM accepted: %v", err)
	}
	frame := make([]byte, frameBytes)
	binary.LittleEndian.PutUint16(frame, 32767)
	if err := q.Push(frame); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(frame); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(frame); !errors.Is(err, ErrMediaBackpressure) {
		t.Fatalf("unbounded backlog: %v", err)
	}
	f, err := q.ReadFrame()
	if err != nil || len(f) != 960 || f[0] < .99 {
		t.Fatalf("bad PCM decode: %v", err)
	}
	q.Close()
	if _, err := q.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("closed source retained audio: %v", err)
	}
}

func TestPCMQueueClearDiscardsInterruptedSpeech(t *testing.T) {
	q := newPCMQueue()
	q.Push(make([]byte, frameBytes))
	q.Clear()
	if len(q.frames) != 0 {
		t.Fatal("stale audio survived interruption")
	}
	q.Close()
}

func TestPCMConversionClipsAndPreservesSilence(t *testing.T) {
	data := encodePCM([]float32{-2, 0, 2})
	if int16(binary.LittleEndian.Uint16(data)) != -32768 || binary.LittleEndian.Uint16(data[2:]) != 0 || binary.LittleEndian.Uint16(data[4:]) != 32767 {
		t.Fatal("PCM clipping/silence incorrect")
	}
}
