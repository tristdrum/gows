package storage

import (
	"context"
	"errors"
)

var (
	ErrStatusBinding      = errors.New("status_capture_binding_mismatch")
	ErrStatusStaleControl = errors.New("status_capture_stale_control")
	ErrStatusUnauthorized = errors.New("status_capture_unavailable")
	ErrStatusInvalid      = errors.New("status_history_invalid_request")
	ErrStatusGeneration   = errors.New("status_capture_generation_changed")
	ErrStatusData         = errors.New("status_history_data_unavailable")
)

const StatusRetentionMS int64 = 24 * 60 * 60 * 1000

type StatusBinding struct {
	AccountID            string `json:"account_id"`
	ExpectedPhone        string `json:"expected_phone"`
	ShardID              string `json:"shard_id"`
	Epoch                uint64 `json:"epoch"`
	AuthorizationSubject string `json:"authorization_subject"`
}
type StatusPolicy struct {
	StatusBinding
	ObservedAtMS      int64 `json:"observed_at_ms"`
	Enabled           bool  `json:"enabled"`
	AuthorizedUntilMS int64 `json:"authorized_until_ms"`
}
type StatusGap struct {
	SinceMS int64  `json:"since_ms"`
	UntilMS *int64 `json:"until_ms"`
	Reason  string `json:"reason"`
}
type StatusCapture struct {
	State             string      `json:"state"`
	StartedAtMS       *int64      `json:"started_at_ms"`
	Generation        string      `json:"generation"`
	AuthorizedUntilMS int64       `json:"authorized_until_ms"`
	AcknowledgedAtMS  int64       `json:"acknowledged_at_ms"`
	Gaps              []StatusGap `json:"gaps"`
}
type StatusCursor struct {
	TimestampMS int64  `json:"timestamp_ms"`
	ID          string `json:"id"`
}
type StatusHistoryQuery struct {
	StatusBinding
	SinceMS    int64
	UntilMS    int64
	Limit      uint32
	Before     *StatusCursor
	Generation string
}
type StatusSource struct {
	ID          string `json:"id"`
	Chat        string `json:"chat"`
	Sender      string `json:"sender"`
	FromMe      bool   `json:"from_me"`
	TimestampMS int64  `json:"timestamp_ms"`
	ExpiresAtMS int64  `json:"expires_at_ms"`
}
type StatusEntry struct {
	Message *StoredMessage `json:"message"`
	Source  StatusSource   `json:"status_source"`
}
type StatusPageSource struct {
	Version          int           `json:"version"`
	SinceMS          int64         `json:"since_ms"`
	UntilMS          int64         `json:"until_ms"`
	Limit            uint32        `json:"limit"`
	ScannedCount     uint32        `json:"scanned_count"`
	UnavailableCount uint32        `json:"unavailable_count"`
	NextCursor       *StatusCursor `json:"next_cursor"`
	Complete         bool          `json:"complete"`
}
type StatusHistoryPage struct {
	Messages   []StatusEntry    `json:"messages"`
	Capture    StatusCapture    `json:"capture"`
	SourcePage StatusPageSource `json:"source_page"`
}
type StatusExact struct {
	Generation string
	StatusBinding
	ID     string
	Sender string
	FromMe bool
}
type StatusHistoryResult struct {
	StatusEntry
	Capture StatusCapture `json:"capture"`
}
type StatusHistoryStorage interface {
	Configure(context.Context, StatusPolicy) (StatusCapture, error)
	Capture(context.Context, *StoredMessage, string, int64) error
	List(context.Context, StatusHistoryQuery) (*StatusHistoryPage, error)
	Exact(context.Context, StatusExact) (*StatusHistoryResult, error)
	Prune(context.Context) error
	Restart(context.Context) error
}
