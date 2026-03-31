package server

import (
	"context"
	"io"

	"golang.org/x/time/rate"
)

// ThrottledReadWriteCloser wraps an io.ReadWriteCloser with rate limiters.
type ThrottledReadWriteCloser struct {
	io.ReadWriteCloser
	ctx             context.Context
	uploadLimiter   *rate.Limiter
	downloadLimiter *rate.Limiter
}

func (t *ThrottledReadWriteCloser) Read(p []byte) (n int, err error) {
	n, err = t.ReadWriteCloser.Read(p)
	if n > 0 && t.downloadLimiter != nil {
		if waitErr := t.downloadLimiter.WaitN(t.ctx, n); waitErr != nil {
			return n, waitErr
		}
	}
	return n, err
}

func (t *ThrottledReadWriteCloser) Write(p []byte) (n int, err error) {
	if t.uploadLimiter != nil {
		if waitErr := t.uploadLimiter.WaitN(t.ctx, len(p)); waitErr != nil {
			return 0, waitErr
		}
	}
	return t.ReadWriteCloser.Write(p)
}

// NewThrottledStream creates a wrapped stream that respects the engine's rate limits.
func (e *Engine) NewThrottledStream(ctx context.Context, rw io.ReadWriteCloser) io.ReadWriteCloser {
	if e.UploadLimiter == nil && e.DownloadLimiter == nil {
		return rw
	}
	return &ThrottledReadWriteCloser{
		ReadWriteCloser: rw,
		ctx:             ctx,
		uploadLimiter:   e.UploadLimiter,
		downloadLimiter: e.DownloadLimiter,
	}
}
