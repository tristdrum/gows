package gows

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"strings"
)

func HasNewsletterSuffix(s string) bool {
	return strings.HasSuffix(s, "@"+types.NewsletterServer)
}

func IsNewsletter(jid types.JID) bool {
	return jid.Server == types.NewsletterServer
}

type GetNewsletterMessagesByInviteParams struct {
	Count int
}

type NewsletterMessagesResp struct {
	NewsletterJID types.JID
	Messages      []*types.NewsletterMessage
}

// GetNewsletterMessagesByInvite gets messages in a WhatsApp channel using an invite code.
func (gows *GoWS) GetNewsletterMessagesByInvite(code string, params *GetNewsletterMessagesByInviteParams) (*NewsletterMessagesResp, error) {
	key := strings.TrimPrefix(code, whatsmeow.NewsletterLinkPrefix)
	attrs := waBinary.Attrs{
		"type":  "invite",
		"key":   key,
		"count": 100,
	}
	if params != nil {
		if params.Count != 0 {
			attrs["count"] = params.Count
		}
	}

	resp, err := gows.int.SendIQ(
		gows.Context,
		whatsmeow.DangerousInfoQuery{
			Namespace: "newsletter",
			Type:      "get",
			To:        types.ServerJID,
			Content: []waBinary.Node{{
				Tag:   "messages",
				Attrs: attrs,
			}},
		})
	if err != nil {
		return nil, err
	}
	messagesNode, ok := resp.GetOptionalChildByTag("messages")
	if !ok {
		return nil, &whatsmeow.ElementMissingError{Tag: "messages", In: "newsletter messages response"}
	}
	messages := gows.int.ParseNewsletterMessages(&messagesNode)
	messages = filterMessageNull(messages)
	jid, ok := messagesNode.Attrs["jid"].(types.JID)
	if !ok {
		return nil, fmt.Errorf("no jid in messages response")
	}
	response := NewsletterMessagesResp{
		NewsletterJID: jid,
		Messages:      messages,
	}
	return &response, nil
}

func filterMessageNull(messages []*types.NewsletterMessage) []*types.NewsletterMessage {
	var filtered []*types.NewsletterMessage
	for _, message := range messages {
		if message.Message != nil {
			filtered = append(filtered, message)
		}
	}
	return filtered
}

type SearchPageParams struct {
	Count       int
	StartCursor string
}

type SearchNewsletterByViewParams struct {
	Page       SearchPageParams
	View       string
	Categories []string
	Countries  []string
}

type SearchNewsletterByTextParams struct {
	Page       SearchPageParams
	Text       string
	Categories []string
}

type SearchPageResult struct {
	StartCursor     string `json:"startCursor"`
	EndCursor       string `json:"endCursor"`
	HasNextPage     bool   `json:"hasNextPage"`
	HasPreviousPage bool   `json:"hasPreviousPage"`
}

type SearchNewsletterResult struct {
	Page        SearchPageResult
	Newsletters []*types.NewsletterMetadata
}

const (
	queryNewslettersDirectoryList   = "6190824427689257"
	queryNewslettersDirectorySearch = "6802402206520139"
)

type Map map[string]interface{}

type respSearchNewsletter struct {
	PageInfo    *SearchPageResult           `json:"page_info"`
	Newsletters []*types.NewsletterMetadata `json:"result"`
}

type respSearchNewsletterDirectoryList struct {
	Data *respSearchNewsletter `json:"xwa2_newsletters_directory_list"`
}
type respSearchNewsletterDirectorySearch struct {
	Data *respSearchNewsletter `json:"xwa2_newsletters_directory_search"`
}

// SearchNewsletterByView searches for WhatsApp channels by views.
func (gows *GoWS) SearchNewsletterByView(query SearchNewsletterByViewParams) (*SearchNewsletterResult, error) {
	variables := Map{
		"input": Map{
			"view": query.View,
			"filters": Map{
				"country_codes": query.Countries,
				"categories":    query.Categories,
			},
			"limit":        query.Page.Count,
			"start_cursor": query.Page.StartCursor,
		},
	}
	data, err := gows.int.SendMexIQ(context.TODO(), queryNewslettersDirectoryList, variables)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("no data returned")
	}

	var respData respSearchNewsletterDirectoryList
	err = json.Unmarshal(data, &respData)
	if err != nil {
		return nil, err
	}
	result := SearchNewsletterResult{
		Page:        *respData.Data.PageInfo,
		Newsletters: respData.Data.Newsletters,
	}
	return &result, nil
}

// SearchNewsletterByText searches for WhatsApp channels by text.
func (gows *GoWS) SearchNewsletterByText(query SearchNewsletterByTextParams) (*SearchNewsletterResult, error) {
	variables := Map{
		"input": Map{
			"search_text":  query.Text,
			"categories":   query.Categories,
			"limit":        query.Page.Count,
			"start_cursor": query.Page.StartCursor,
		},
	}
	data, err := gows.int.SendMexIQ(context.TODO(), queryNewslettersDirectorySearch, variables)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("no data returned")
	}

	var respData respSearchNewsletterDirectorySearch
	err = json.Unmarshal(data, &respData)
	if err != nil {
		return nil, err
	}
	result := SearchNewsletterResult{
		Page:        *respData.Data.PageInfo,
		Newsletters: respData.Data.Newsletters,
	}
	return &result, nil
}

func (gows *GoWS) SendNewsletterPollVote(
	ctx context.Context,
	jid types.JID,
	messageId types.MessageID,
	serverID types.MessageServerID,
	selectedOptions []string,
) (resp whatsmeow.SendResponse, err error) {
	resp, err = gows.SendNewsletterPollVoteNode(ctx, jid, serverID, selectedOptions)
	if err != nil {
		return resp, err
	}

	// Issue event
	info := types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     jid,
			Sender:   gows.GetOwnId(),
			IsFromMe: true,
			IsGroup:  false,
		},
		ID:        resp.ID,
		Timestamp: resp.Timestamp,
		ServerID:  resp.ServerID,
	}
	msg := waE2E.Message{
		PollUpdateMessage: &waE2E.PollUpdateMessage{
			PollCreationMessageKey: &waCommon.MessageKey{
				RemoteJID: proto.String(jid.String()),
				FromMe:    proto.Bool(false),
				ID:        proto.String(messageId),
			},
			SenderTimestampMS: proto.Int64(resp.Timestamp.UnixMilli()),
		},
	}
	msgEvent := &events.Message{
		Info:       info,
		Message:    &msg,
		RawMessage: &msg,
	}
	evt := PollVoteEvent{
		Message: msgEvent,
		Votes:   &selectedOptions,
	}
	go gows.handleEvent(evt)
	return resp, nil
}

func (gows *GoWS) SendNewsletterPollVoteNode(
	ctx context.Context,
	jid types.JID,
	serverID types.MessageServerID,
	selectedOptions []string,
) (resp whatsmeow.SendResponse, err error) {
	if gows == nil {
		return resp, whatsmeow.ErrClientIsNil
	}
	votes := make([]waBinary.Node, len(selectedOptions))
	hashed := whatsmeow.HashPollOptions(selectedOptions)
	for i, voteHash := range hashed {
		votes[i] = waBinary.Node{
			Tag:     "vote",
			Content: voteHash,
		}
	}
	votesNode := waBinary.Node{
		Tag:     "votes",
		Content: votes,
	}
	metaNode := waBinary.Node{
		Tag: "meta",
		Attrs: waBinary.Attrs{
			"polltype": "vote",
		},
	}
	var content []waBinary.Node
	content = append(content, metaNode)
	content = append(content, votesNode)

	reqID := gows.GenerateMessageID()
	response := gows.int.WaitResponse(reqID)
	node := waBinary.Node{
		Tag: "message",
		Attrs: waBinary.Attrs{
			"to":        jid,
			"id":        reqID,
			"server_id": serverID,
			"type":      "poll",
		},
		Content: content,
	}
	data, err := gows.int.SendNodeAndGetData(ctx, node)
	if err != nil {
		gows.int.CancelResponse(reqID, response)
		return resp, err
	}

	// handle response
	var respNode *waBinary.Node
	select {
	case respNode = <-response:
	case <-ctx.Done():
		gows.int.CancelResponse(reqID, response)
		return resp, ctx.Err()
	}
	if isDisconnectNode(respNode) {
		respNode, err = gows.int.RetryFrame(ctx, "message send", reqID, data, respNode, 0)
		if err != nil {
			return resp, err
		}
	}
	ag := respNode.AttrGetter()
	resp.ID = ag.String("id")
	resp.ServerID = ag.OptionalInt("server_id")
	resp.Timestamp = ag.UnixTime("t")
	if errorCode := ag.Int("error"); errorCode != 0 {
		err = fmt.Errorf("%w %d", whatsmeow.ErrServerReturnedError, errorCode)
		return resp, err
	}
	return resp, nil
}

var xmlStreamEndNode = &waBinary.Node{Tag: "xmlstreamend"}

func isDisconnectNode(node *waBinary.Node) bool {
	return node == xmlStreamEndNode || node.Tag == "stream:error"
}
