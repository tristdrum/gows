package sqlstorage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStatusExpiredLeaseKeepsCoverageGapsBounded(t *testing.T) {
	forMessageDatabases(t, func(t *testing.T, messages *SqlMessageStore) {
		store, now, binding := statusFixture(t, messages)
		ctx := context.Background()
		// Repeated lease interruptions fill the persisted coverage history.
		for range 33 {
			enableStatus(t, store, *now, binding)
			*now = now.Add(4 * time.Minute)
		}
		active := enableStatus(t, store, *now, binding)
		require.Len(t, active.Gaps, 32)
		require.NotNil(t, active.Gaps[31].UntilMS)
		*now = now.Add(4 * time.Minute)
		for range 2 {
			page, err := store.List(ctx, statusQuery(binding, *now))
			require.NoError(t, err)
			require.Equal(t, "expired", page.Capture.State)
			require.Empty(t, page.Messages)
			require.Len(t, page.Capture.Gaps, 32)
			require.Equal(t, active.Gaps[1], page.Capture.Gaps[0])
			latest := page.Capture.Gaps[31]
			require.Equal(t, "authorization_expired", latest.Reason)
			require.Equal(t, active.AuthorizedUntilMS, latest.SinceMS)
			require.Nil(t, latest.UntilMS)
		}
	})
}
