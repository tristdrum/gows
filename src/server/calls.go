package server

import (
	"context"
	"fmt"

	"github.com/devlikeapro/gows/proto"
	"go.mau.fi/whatsmeow/types"
)

func (s *Server) RejectCall(ctx context.Context, req *__.RejectCallRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	from, err := types.ParseJID(req.GetFrom())
	if err != nil {
		return nil, fmt.Errorf("parse from JID '%s': %w", req.GetFrom(), err)
	}
	if err = cli.RejectCall(ctx, from, req.GetId()); err != nil {
		return nil, err
	}
	return &__.Empty{}, nil
}
