package server

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/devlikeapro/gows/gows"
	__ "github.com/devlikeapro/gows/proto"
	"github.com/devlikeapro/gows/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) DownloadStatusHistoryMedia(req *__.StatusHistoryMediaRequest, stream grpc.ServerStreamingServer[__.StatusHistoryMediaChunk]) error {
	ctx, cancel := context.WithTimeout(stream.Context(), 14*time.Second)
	defer cancel()
	exactReq := req.GetMessage()
	q := exactStatusRequest(exactReq)
	cli, err := s.statusHistoryClient(exactReq.GetSession().GetId(), q.StatusBinding)
	if err != nil {
		return err
	}
	maximum := int64(req.GetMaxBytes())
	if req.GetMaxBytes() == 0 {
		maximum = gows.StatusMediaMaxBytes
	}
	if maximum <= 0 || maximum > gows.StatusMediaMaxBytes {
		return status.Error(codes.ResourceExhausted, "status_media_too_large")
	}
	record, err := cli.Storage.StatusHistory.Exact(ctx, q)
	if err != nil {
		return statusHistoryError(ctx, err)
	}
	file, mimetype, err := gows.StatusMediaFile(ctx, record.Message.Message.Message, maximum, cli.DownloadToFile)
	if err != nil {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		if errors.Is(err, gows.ErrStatusMediaTooLarge) {
			return status.Error(codes.ResourceExhausted, "status_media_too_large")
		}
		if errors.Is(err, gows.ErrStatusMediaUnsupported) {
			return status.Error(codes.FailedPrecondition, "status_media_unsupported")
		}
		return status.Error(codes.Unavailable, "status_media_unavailable")
	}
	defer file.Close()
	defer os.Remove(file.Name())
	info, err := file.Stat()
	if err != nil {
		return status.Error(codes.Unavailable, "status_media_unavailable")
	}
	return streamStatusMedia(ctx, cli.Storage.StatusHistory, q, file, mimetype, uint64(info.Size()), stream.Send)
}

func streamStatusMedia(ctx context.Context, history storage.StatusHistoryStorage, q storage.StatusExact, reader io.Reader, mimetype string, total uint64, send func(*__.StatusHistoryMediaChunk) error) error {
	buffer := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		record, err := history.Exact(ctx, q)
		if err != nil {
			return statusHistoryError(ctx, err)
		}
		n, readErr := reader.Read(buffer)
		if n > 0 {
			if err = send(&__.StatusHistoryMediaChunk{Data: buffer[:n], Mimetype: mimetype, TotalBytes: total, ExpiresAtMs: record.Source.ExpiresAtMS}); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return status.Error(codes.Unavailable, "status_media_read_failed")
		}
	}
}
