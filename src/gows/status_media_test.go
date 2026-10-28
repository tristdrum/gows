package gows

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestStatusMediaBoundsActualBytesAndCleansScratch(t *testing.T) {
	message := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{FileLength: proto.Uint64(1), Mimetype: proto.String("image/jpeg")}}
	var path string
	download := func(ctx context.Context, msg whatsmeow.DownloadableMessage, file whatsmeow.File) error {
		wrapper := file.(*boundedStatusFile)
		path = wrapper.file.Name()
		_, err := file.Write(make([]byte, 1000))
		return err
	}
	_, _, err := StatusMediaFile(context.Background(), message, 10, download)
	require.ErrorIs(t, err, ErrStatusMediaTooLarge)
	_, err = os.Stat(path)
	require.True(t, errors.Is(err, os.ErrNotExist))
}

func TestStatusMediaDeclaredLimitRejectsBeforeNetworkAndErrorCleansScratch(t *testing.T) {
	message := &waE2E.Message{VideoMessage: &waE2E.VideoMessage{FileLength: proto.Uint64(100)}}
	called := false
	_, _, err := StatusMediaFile(context.Background(), message, 10, func(context.Context, whatsmeow.DownloadableMessage, whatsmeow.File) error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, ErrStatusMediaTooLarge)
	require.False(t, called)
	message.VideoMessage.FileLength = nil
	var path string
	_, _, err = StatusMediaFile(context.Background(), message, 10, func(_ context.Context, _ whatsmeow.DownloadableMessage, file whatsmeow.File) error {
		wrapper := file.(*boundedStatusFile)
		path = wrapper.file.Name()
		_, writeErr := file.WriteAt(make([]byte, 1), math.MaxInt64)
		require.ErrorIs(t, writeErr, ErrStatusMediaTooLarge)
		return context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}
func TestStatusMediaFileUnlinkedAndRewound(t *testing.T) {
	message := &waE2E.Message{AudioMessage: &waE2E.AudioMessage{Mimetype: proto.String("audio/ogg")}}
	file, mime, err := StatusMediaFile(context.Background(), message, 20, func(ctx context.Context, msg whatsmeow.DownloadableMessage, file whatsmeow.File) error {
		_, err := file.Write([]byte("synthetic-media"))
		return err
	})
	require.NoError(t, err)
	defer file.Close()
	require.Equal(t, "audio/ogg", mime)
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, "synthetic-media", string(data))
	_, err = os.Stat(file.Name())
	require.True(t, errors.Is(err, os.ErrNotExist))
}
