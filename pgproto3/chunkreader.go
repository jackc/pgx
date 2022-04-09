package pgproto3

import (
	"io"
	"sync"
)

type bigBufPool struct {
	pool     sync.Pool
	byteSize int
}

var bigBufPools []*bigBufPool

func init() {
	KiB := 1024
	bigBufSizes := []int{64 * KiB, 256 * KiB, 1024 * KiB, 4096 * KiB}
	bigBufPools = make([]*bigBufPool, len(bigBufSizes))

	for i := range bigBufPools {
		byteSize := bigBufSizes[i]
		bigBufPools[i] = &bigBufPool{
			pool:     sync.Pool{New: func() any { return make([]byte, byteSize) }},
			byteSize: byteSize,
		}
	}
}

func getBigBuf(size int) []byte {
	for _, bigBufPool := range bigBufPools {
		if size < bigBufPool.byteSize {
			return bigBufPool.pool.Get().([]byte)
		}
	}
	return make([]byte, size)
}

func releaseBigBuf(buf []byte) {
	for _, bigBufPool := range bigBufPools {
		if len(buf) == bigBufPool.byteSize {
			bigBufPool.pool.Put(buf)
			return
		}
	}
}

// chunkReader is a io.Reader wrapper that minimizes IO reads and memory allocations. It allocates memory in chunks and
// will read as much as will fit in the current buffer in a single call regardless of how large a read is actually
// requested. The memory returned via Next is only valid until the next call to Next.
//
// This is roughly equivalent to a bufio.Reader that only uses Peek and Discard to never copy bytes.
type chunkReader struct {
	r io.Reader

	buf    []byte
	rp, wp int // buf read position and write position

	ownBuf []byte // buf owned by chunkReader
}

// newChunkReader creates and returns a new chunkReader for r with default configuration with bufSize internal buffer.
// If bufSize is <= 0 it uses a default value.
func newChunkReader(r io.Reader, bufSize int) *chunkReader {
	if bufSize <= 0 {
		// By historical reasons Postgres currently has 8KB send buffer inside,
		// so here we want to have at least the same size buffer.
		// @see https://github.com/postgres/postgres/blob/249d64999615802752940e017ee5166e726bc7cd/src/backend/libpq/pqcomm.c#L134
		// @see https://www.postgresql.org/message-id/0cdc5485-cb3c-5e16-4a46-e3b2f7a41322%40ya.ru
		//
		// In addition, testing has found no benefit of any larger buffer.
		bufSize = 8192
	}

	buf := make([]byte, bufSize)

	return &chunkReader{
		r:      r,
		buf:    buf,
		ownBuf: buf,
	}
}

// Next returns buf filled with the next n bytes. buf is only valid until next call of Next. If an error occurs, buf
// will be nil.
func (r *chunkReader) Next(n int) (buf []byte, err error) {
	// Reset the buffer if it is empty
	if r.rp == r.wp {
		if len(r.buf) != len(r.ownBuf) {
			releaseBigBuf(r.buf)
			r.buf = r.ownBuf
		}
		r.rp = 0
		r.wp = 0
	}

	// n bytes already in buf
	if (r.wp - r.rp) >= n {
		buf = r.buf[r.rp : r.rp+n : r.rp+n]
		r.rp += n
		return buf, err
	}

	// buf is smaller than requested number of bytes
	if len(r.buf) < n {
		bigBuf := getBigBuf(n)
		r.wp = copy(bigBuf, r.buf[r.rp:r.wp])
		r.rp = 0
		r.buf = bigBuf
	}

	// buf is large enough, but need to shift filled area to start to make enough contiguous space
	minReadCount := n - (r.wp - r.rp)
	if (len(r.buf) - r.wp) < minReadCount {
		r.wp = copy(r.buf, r.buf[r.rp:r.wp])
		r.rp = 0
	}

	// Read at least the required number of bytes from the underlying io.Reader
	readBytesCount, err := io.ReadAtLeast(r.r, r.buf[r.wp:], minReadCount)
	r.wp += readBytesCount
	// fmt.Println("read", n)
	if err != nil {
		return nil, err
	}

	buf = r.buf[r.rp : r.rp+n : r.rp+n]
	r.rp += n
	return buf, nil
}
