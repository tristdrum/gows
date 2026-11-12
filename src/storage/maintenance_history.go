package storage

import "encoding/json"

// This private read contract proves only provider-observed/synced activity.
// Message IDs break pagination ties; they never prove message chronology.
type MaintenanceQuery struct {
	From   int64  `json:"from"`
	To     int64  `json:"to"`
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor,omitempty"`
	ChatID string `json:"chat_id,omitempty"`
	Token  string `json:"token,omitempty"`
}
type MaintenanceCoverage struct {
	EnumerationComplete bool     `json:"enumeration_complete"`
	Complete            bool     `json:"complete"`
	Epoch               string   `json:"epoch"`
	Revision            int64    `json:"revision"`
	Token               string   `json:"token"`
	Scope               string   `json:"scope"`
	Errors              []string `json:"errors"`
}
type MaintenanceItem struct {
	ID          string          `json:"id" db:"id"`
	JID         string          `json:"jid" db:"jid"`
	ProviderJID string          `json:"provider_jid" db:"provider_jid"`
	Timestamp   int64           `json:"timestamp" db:"timestamp"`
	FromMe      bool            `json:"from_me" db:"from_me"`
	Kind        string          `json:"kind"`
	Deleted     bool            `json:"deleted"`
	Raw         json.RawMessage `json:"raw"`
}
type MaintenancePage struct {
	Version int `json:"version"`
	Window  struct {
		From int64 `json:"from"`
		To   int64 `json:"to"`
	} `json:"window"`
	Coverage   MaintenanceCoverage `json:"coverage"`
	Items      []MaintenanceItem   `json:"items"`
	NextCursor string              `json:"next_cursor,omitempty"`
}
type MaintenanceHistoryJob struct {
	ID           string `db:"id"`
	Notification []byte `db:"notification"`
	Stored       bool   `db:"stored"`
	Attempts     int    `db:"attempts"`
}
