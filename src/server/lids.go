package server

import (
	"context"
	"go.mau.fi/whatsmeow/types"

	__ "github.com/devlikeapro/gows/proto"
)

func (s *Server) GetAllLids(ctx context.Context, req *__.GetLidsRequest) (*__.JsonList, error) {
	gows, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}

	entries, err := gows.Storage.Lidmap.GetAllLidMap()
	if err != nil {
		return nil, err
	}

	// Convert slice to JSON list
	return toJsonList(entries)
}

func (s *Server) GetLidsCount(ctx context.Context, req *__.Session) (*__.OptionalUInt64, error) {
	gows, err := s.Sm.Get(req.GetId())
	if err != nil {
		return nil, err
	}

	count, err := gows.Storage.Lidmap.GetLidCount()
	if err != nil {
		return nil, err
	}

	return &__.OptionalUInt64{
		Value: uint64(count),
	}, nil
}

func (s *Server) FindPNByLid(ctx context.Context, req *__.EntityByIdRequest) (*__.OptionalString, error) {
	gows, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	pn, err := types.ParseJID(req.GetId())
	if err != nil {
		return nil, err
	}

	cli := gows.Client
	lid, err := cli.Store.LIDs.GetPNForLID(ctx, pn)
	if err != nil {
		return nil, err
	}

	return &__.OptionalString{Value: lid.String()}, nil
}

func (s *Server) FindLIDByPhoneNumber(ctx context.Context, req *__.EntityByIdRequest) (*__.OptionalString, error) {
	gows, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	lid, err := types.ParseJID(req.GetId())
	if err != nil {
		return nil, err
	}

	cli := gows.Client
	pn, err := cli.Store.LIDs.GetLIDForPN(ctx, lid)
	if err != nil {
		return nil, err
	}

	return &__.OptionalString{Value: pn.String()}, nil
}
