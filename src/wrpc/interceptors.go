package wrpc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryTimeoutInterceptor enforces a maximum duration for unary RPCs to prevent
// requests that never complete from holding onto memory indefinitely.
func UnaryTimeoutInterceptor(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Respect a shorter deadline if the caller already set one.
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= timeout {
			return handler(ctx, req)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		resp, err := handler(ctx, req)
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return nil, status.Error(codes.DeadlineExceeded, "request deadline exceeded")
		}
		return resp, err
	}
}
