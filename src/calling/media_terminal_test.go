package calling

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func newOwnedTerminalTestStream(t *testing.T) (*Manager, *Stream, <-chan struct{}) {
	t.Helper()
	ended := make(chan struct{}, 1)
	m := newManager(&fakeDialer{}, func(event Event) {
		if event.State == "ended" {
			ended <- struct{}{}
		}
	})
	t.Cleanup(m.Close)
	if _, err := m.Dial(context.Background(), "offline-peer", "attempt"); err != nil {
		t.Fatal(err)
	}
	s, err := m.Open("call")
	if err != nil {
		t.Fatal(err)
	}
	return m, s, ended
}

func TestOwnedSinkFailurePreservesTerminalCause(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []float32
		fill  bool
		want  error
	}{
		{"backpressure", make([]float32, 960), true, ErrMediaBackpressure},
		{"invalid_pcm", make([]float32, 959), false, ErrInvalidPCM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, s, ended := newOwnedTerminalTestStream(t)
			if err := s.TerminalError(); err != nil {
				t.Fatalf("open stream has a terminal cause: %v", err)
			}
			if tc.fill {
				for range 2 {
					if err := s.WriteFrame(tc.frame); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := s.WriteFrame(tc.frame); !errors.Is(err, tc.want) {
				t.Fatalf("sink error = %v, want %v", err, tc.want)
			}
			select {
			case <-s.Done:
			case <-time.After(time.Second):
				t.Fatal("failed sink did not close")
			}
			if err := s.TerminalError(); !errors.Is(err, tc.want) {
				t.Fatalf("terminal cause = %v, want %v", err, tc.want)
			}
			if err := s.Write(make([]byte, 1920)); !errors.Is(err, io.EOF) {
				t.Fatalf("late output = %v, want owned EOF", err)
			}
			if err := s.TerminalError(); !errors.Is(err, tc.want) {
				t.Fatalf("late EOF erased terminal cause: %v", err)
			}
			select {
			case <-ended:
			case <-time.After(time.Second):
				t.Fatal("failed sink did not retire its owner")
			}
		})
	}
}

func TestNormalOwnedEndHasNoSinkFailure(t *testing.T) {
	m, s, _ := newOwnedTerminalTestStream(t)
	if err := m.Hangup("call"); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteFrame(make([]float32, 960)); !errors.Is(err, io.EOF) {
		t.Fatalf("late input = %v, want owned EOF", err)
	}
	if err := s.TerminalError(); err != nil {
		t.Fatalf("normal end became a sink failure: %v", err)
	}
}

func TestOwnedSinkDrainingControlStaysOpen(t *testing.T) {
	_, s, _ := newOwnedTerminalTestStream(t)
	for range 60 {
		if err := s.WriteFrame(make([]float32, 960)); err != nil {
			t.Fatal(err)
		}
		<-s.Input
		if err := s.TerminalError(); err != nil {
			t.Fatalf("draining stream failed: %v", err)
		}
		select {
		case <-s.Done:
			t.Fatal("draining stream closed")
		default:
		}
	}
}

func TestOwnedSinkFailureIsLocalWhenCallIDsMatch(t *testing.T) {
	_, first, ended := newOwnedTerminalTestStream(t)
	secondManager, second, _ := newOwnedTerminalTestStream(t)
	frame := make([]float32, 960)
	if err := second.WriteFrame(frame); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := first.WriteFrame(frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.WriteFrame(frame); !errors.Is(err, ErrMediaBackpressure) {
		t.Fatal(err)
	}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("failed owner was not retired")
	}
	if len(second.Input) != 1 || second.TerminalError() != nil {
		t.Fatal("failure changed the other session's queue")
	}
	select {
	case <-second.Done:
		t.Fatal("failure closed the other session")
	default:
	}
	if _, err := secondManager.Status("call"); err != nil {
		t.Fatalf("failure retired the other owner: %v", err)
	}
}
