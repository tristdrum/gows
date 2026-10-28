package gows

import (
	"context"
	"errors"
	"io"
	"os"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
)

const StatusMediaMaxBytes int64 = 25 * 1024 * 1024

var ErrStatusMediaTooLarge = errors.New("status_media_too_large")
var ErrStatusMediaUnsupported = errors.New("status_media_unsupported")

type StatusDownloadable interface {
	whatsmeow.DownloadableMessage
	GetFileLength() uint64
	GetMimetype() string
}

func statusMedia(msg *waE2E.Message) (StatusDownloadable, error) {
	msg = unwrapMediaMessage(msg)
	if msg == nil {
		return nil, ErrStatusMediaUnsupported
	}
	switch {
	case msg.ImageMessage != nil:
		return msg.ImageMessage, nil
	case msg.VideoMessage != nil:
		return msg.VideoMessage, nil
	case msg.AudioMessage != nil:
		return msg.AudioMessage, nil
	default:
		return nil, ErrStatusMediaUnsupported
	}
}

// Do not embed *os.File: its ReaderFrom would bypass the bounded Write path.
// Encryption adds at most one AES block and a ten-byte MAC to plaintext.
type boundedStatusFile struct {
	file *os.File
	max  int64
}

func (f *boundedStatusFile) Read(p []byte) (int, error)              { return f.file.Read(p) }
func (f *boundedStatusFile) ReadAt(p []byte, off int64) (int, error) { return f.file.ReadAt(p, off) }
func (f *boundedStatusFile) Seek(off int64, whence int) (int64, error) {
	return f.file.Seek(off, whence)
}
func (f *boundedStatusFile) Stat() (os.FileInfo, error) { return f.file.Stat() }
func (f *boundedStatusFile) Truncate(size int64) error {
	if size < 0 || size > f.max {
		return ErrStatusMediaTooLarge
	}
	return f.file.Truncate(size)
}
func (f *boundedStatusFile) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off > f.max || int64(len(p)) > f.max-off {
		return 0, ErrStatusMediaTooLarge
	}
	return f.file.WriteAt(p, off)
}
func (f *boundedStatusFile) Write(p []byte) (int, error) {
	off, err := f.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if off < 0 || off > f.max || int64(len(p)) > f.max-off {
		return 0, ErrStatusMediaTooLarge
	}
	return f.file.Write(p)
}

type StatusMediaDownload func(context.Context, whatsmeow.DownloadableMessage, whatsmeow.File) error

// StatusMediaFile uses the session's normal authenticated transport/decryption,
// but never the WAHA media cache. The tempfile is unlinked immediately on Unix.
func StatusMediaFile(ctx context.Context, msg *waE2E.Message, maxBytes int64, download StatusMediaDownload) (*os.File, string, error) {
	if maxBytes <= 0 || maxBytes > StatusMediaMaxBytes {
		return nil, "", ErrStatusMediaTooLarge
	}
	media, err := statusMedia(msg)
	if err != nil {
		return nil, "", err
	}
	if media.GetFileLength() > uint64(maxBytes) {
		return nil, "", ErrStatusMediaTooLarge
	}
	file, err := os.CreateTemp("", "gows-status-media-*")
	if err != nil {
		return nil, "", err
	}
	name := file.Name()
	_ = os.Remove(name)
	cleanup := func() { _ = file.Close(); _ = os.Remove(name) }
	bounded := &boundedStatusFile{file: file, max: maxBytes + 26}
	if err = download(ctx, media, bounded); err != nil {
		cleanup()
		return nil, "", err
	}
	info, err := file.Stat()
	if err != nil {
		cleanup()
		return nil, "", err
	}
	if info.Size() > maxBytes {
		cleanup()
		return nil, "", ErrStatusMediaTooLarge
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, "", err
	}
	return file, media.GetMimetype(), nil
}
