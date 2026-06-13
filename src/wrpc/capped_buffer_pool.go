package wrpc

import (
	"google.golang.org/grpc/mem"
)

// cappedBufferPool wraps a base mem.BufferPool and drops buffers larger than capLimit on Put.
// This keeps the gRPC buffer pool from retaining very large allocations after a request completes.
type cappedBufferPool struct {
	capLimit int
	base     mem.BufferPool
}

func (p cappedBufferPool) Get(n int) *[]byte {
	return p.base.Get(n)
}

func (p cappedBufferPool) Put(buf *[]byte) {
	if cap(*buf) > p.capLimit {
		return // drop oversized buffers so they can be reclaimed by GC
	}
	p.base.Put(buf)
}

// NewCappedBufferPool builds a capped buffer pool that only retains buffers up to capLimit bytes.
// Larger buffers are allowed during a call but will not be pooled afterward.
func NewCappedBufferPool(capLimit int) mem.BufferPool {
	return cappedBufferPool{
		capLimit: capLimit,
		base:     mem.NewTieredBufferPool(256, 4<<10, 16<<10, 32<<10, 1<<20),
	}
}
