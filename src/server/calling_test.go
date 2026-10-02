package server

import (
	"context"
	pb "github.com/devlikeapro/gows/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestCallingUnknownSessionCannotControlAnotherSession(t *testing.T) {
	s := NewServer()
	for _, action := range []string{"dial", "accept", "hangup", "status"} {
		_, err := s.Control(context.Background(), &pb.CallControl{Session: "unknown", CallId: "someone-elses-call", Action: action})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("unknown session: %v", err)
		}
	}
}
