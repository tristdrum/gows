package server

import (
	"context"
	"fmt"
	"github.com/devlikeapro/gows/proto"
	"go.mau.fi/whatsmeow/types"
)

func (s *Server) UpdateContact(ctx context.Context, req *__.UpdateContactRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.Session.Id)
	if err != nil {
		return nil, err
	}
	jid, err := types.ParseJID(req.Jid)
	if err != nil {
		return nil, fmt.Errorf("error parsing jid: %w", err)
	}
	err = cli.UpdateContact(ctx, jid, req.FirstName, req.LastName)
	if err != nil {
		return nil, err
	}
	return &__.Empty{}, nil
}

func (s *Server) GetContactById(ctx context.Context, req *__.EntityByIdRequest) (*__.Json, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	user, err := types.ParseJID(req.Id)
	if err != nil {
		return nil, fmt.Errorf("error parsing jid %v: %w", req.Id, err)
	}

	contact, err := cli.Storage.Contacts.GetContact(user)
	if err != nil {
		return nil, fmt.Errorf("error getting contact %v: %w", user, err)
	}
	response, err := toJson(contact)
	if err != nil {
		return nil, fmt.Errorf("error marshaling contact %v: %w", user, err)
	}
	return response, nil
}

func (s *Server) GetContacts(ctx context.Context, req *__.GetContactsRequest) (*__.JsonList, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	pagination := toPagination(req.Pagination)
	sort := toStorageSort(req.SortBy)
	contacts, err := cli.Storage.Contacts.GetAllContacts(sort, pagination)
	if err != nil {
		return nil, err
	}
	response, err := toJsonList(contacts)
	if err != nil {
		return nil, fmt.Errorf("error marshaling contacts: %w", err)
	}
	return response, nil
}
