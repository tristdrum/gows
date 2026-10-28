package server

import (
	"bytes"
	"context"
	"testing"

	__ "github.com/devlikeapro/gows/proto"
	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type exactHistorySpy struct {
	storage.StatusHistoryStorage
	calls int
	exact func(int) (*storage.StatusHistoryResult, error)
}

func (h *exactHistorySpy) Exact(context.Context, storage.StatusExact) (*storage.StatusHistoryResult, error) {
	h.calls++
	return h.exact(h.calls)
}

func TestStatusMediaStreamRechecksAuthorityBeforeEachChunk(t *testing.T) {
	history := &exactHistorySpy{exact: func(call int) (*storage.StatusHistoryResult, error) {
		if call > 1 {
			return nil, storage.ErrStatusUnauthorized
		}
		return &storage.StatusHistoryResult{StatusEntry: storage.StatusEntry{Source: storage.StatusSource{ExpiresAtMS: 123}}}, nil
	}}
	chunks := 0
	err := streamStatusMedia(context.Background(), history, storage.StatusExact{}, bytes.NewReader(make([]byte, 100000)), "image/jpeg", 100000,
		func(chunk *__.StatusHistoryMediaChunk) error {
			chunks++
			require.Len(t, chunk.Data, 64*1024)
			require.Equal(t, int64(123), chunk.ExpiresAtMs)
			return nil
		})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 1, chunks)
}

func TestStatusMediaStreamCancellationStopsBeforeAnotherRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	history := &exactHistorySpy{exact: func(int) (*storage.StatusHistoryResult, error) {
		return &storage.StatusHistoryResult{}, nil
	}}
	err := streamStatusMedia(ctx, history, storage.StatusExact{}, bytes.NewReader(make([]byte, 100000)), "image/jpeg", 100000,
		func(*__.StatusHistoryMediaChunk) error { cancel(); return nil })
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Equal(t, 1, history.calls)
}
