package storage

import (
	"sort"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type Status int

const (
	StatusError       Status = 0
	StatusPending     Status = 1
	StatusServerAck   Status = 2
	StatusDeliveryAck Status = 3
	StatusRead        Status = 4
	StatusPlayed      Status = 5
)

// StoredMessage contains a message and some additional data.
type StoredMessage struct {
	*events.Message
	IsReal bool
	Status *Status
}

type StoredContact struct {
	Jid      types.JID
	Name     string
	PushName string
}

type StoredChat struct {
	Jid                   types.JID
	Name                  string
	ConversationTimestamp time.Time
	UnreadCount           uint64
	MarkedAsUnread        bool
	UnreadStateKnown      bool
}

// StoredChatReadState is the durable authoritative baseline used to derive a
// chat's current unread count from subsequently stored inbound messages.
type StoredChatReadState struct {
	Jid                 types.JID
	BaselineUnreadCount uint64
	MarkedAsUnread      bool
	UnreadStateKnown    bool
	CountFrom           time.Time
	CoveredMessageIDs   []string
	EvidenceTimestamp   time.Time
}

// ChatReadEvent is a timestamped cross-device read/unread action. Timestamp
// orders competing state evidence, while MessageWatermark identifies the last
// message covered when the chat is marked read.
type ChatReadEvent struct {
	Jid               types.JID
	Timestamp         time.Time
	Read              bool
	MessageWatermark  time.Time
	CoveredMessageIDs []string
}

// NormalizeCoveredMessageIDs removes empty and duplicate IDs and returns a
// stable order suitable for durable boundary comparisons.
func NormalizeCoveredMessageIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

type EphemeralSetting struct {
	Initiator     *waE2E.DisappearingMode_Initiator
	Trigger       *waE2E.DisappearingMode_Trigger
	InitiatedByMe *bool
	Timestamp     *int64
	Expiration    uint32
}

type StoredChatEphemeralSetting struct {
	ID          types.JID
	IsEphemeral bool
	Setting     *EphemeralSetting
}

func NotEphemeral(jid types.JID) *StoredChatEphemeralSetting {
	return &StoredChatEphemeralSetting{
		ID:          jid,
		IsEphemeral: false,
	}
}

type SortOrder string

const (
	SortAsc  SortOrder = "ASC"
	SortDesc SortOrder = "DESC"
)

type Sort struct {
	Field string
	Order SortOrder
}

type Pagination struct {
	Offset uint64
	Limit  uint64
}

type MessageFilter struct {
	Jid          *types.JID
	TimestampGte *time.Time
	TimestampLte *time.Time
	FromMe       *bool
	Status       *Status
}

type ChatFilter struct {
	Jids []types.JID
}

// Label represents a label that can be associated with chats
type Label struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Color int    `json:"color"`
}

// LabelAssociation represents an association between a label and a JID
type LabelAssociation struct {
	JID     types.JID `json:"jid"`
	LabelID string    `json:"label_id"`
}

// LidmapEntry represents a lid/pn pair
type LidmapEntry struct {
	Lid types.JID `json:"lid"`
	Pn  types.JID `json:"pn"`
}
