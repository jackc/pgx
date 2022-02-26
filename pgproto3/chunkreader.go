package pgproto3

import (
	"io"
)

// chunkReader is a io.Reader wrapper that minimizes IO reads and memory allocations. It allocates memory in chunks and
// will read as much as will fit in the current buffer in a single call regardless of how large a read is actually
// requested. The memory returned via Next is only valid until the next call to Next.
//
// This is roughly equivalent to a bufio.Reader that only uses Peek and Discard to never copy bytes.
type chunkReader struct {
	r io.Reader

	buf    []byte
	rp, wp int // buf read position and write position

	minBufLen int
}

// newChunkReader creates and returns a new chunkReader for r with default configuration with minBufSize internal buffer.
// If bufSize is <= 0 it uses a default value.
func newChunkReader(r io.Reader, minBufSize int) *chunkReader {
	if minBufSize <= 0 {
		// By historical reasons Postgres currently has 8KB send buffer inside,
		// so here we want to have at least the same size buffer.
		// @see https://github.com/postgres/postgres/blob/249d64999615802752940e017ee5166e726bc7cd/src/backend/libpq/pqcomm.c#L134
		// @see https://www.postgresql.org/message-id/0cdc5485-cb3c-5e16-4a46-e3b2f7a41322%40ya.ru
		minBufSize = 8192
	}

	return &chunkReader{
		r:         r,
		buf:       make([]byte, minBufSize),
		minBufLen: minBufSize,
	}
}

// Next returns buf filled with the next n bytes. buf is only valid until next call of Next. If an error occurs, buf
// will be nil.
func (r *chunkReader) Next(n int) (buf []byte, err error) {
	// n bytes already in buf
	if (r.wp - r.rp) >= n {
		buf = r.buf[r.rp : r.rp+n]
		r.rp += n
		r.resetBufIfEmpty()
		return buf, err
	}

	// available space in buf is less than n
	if len(r.buf) < n {
		r.copyBufContents(r.newBuf(n))
	}

	// buf is large enough, but need to shift filled area to start to make enough contiguous space
	minReadCount := n - (r.wp - r.rp)
	if (len(r.buf) - r.wp) < minReadCount {
		newBuf := r.newBuf(n)
		r.copyBufContents(newBuf)
	}

	if err := r.appendAtLeast(minReadCount); err != nil {
		return nil, err
	}

	buf = r.buf[r.rp : r.rp+n]
	r.rp += n
	r.resetBufIfEmpty()
	return buf, nil
}

func (r *chunkReader) appendAtLeast(fillLen int) error {
	n, err := io.ReadAtLeast(r.r, r.buf[r.wp:], fillLen)
	r.wp += n
	return err
}

func (r *chunkReader) newBuf(size int) []byte {
	if size < r.minBufLen {
		size = r.minBufLen
	}
	return make([]byte, size)
}

func (r *chunkReader) copyBufContents(dest []byte) {
	r.wp = copy(dest, r.buf[r.rp:r.wp])
	r.rp = 0
	r.buf = dest
}

func (r *chunkReader) resetBufIfEmpty() {
	if r.rp == r.wp {
		if len(r.buf) > r.minBufLen {
			r.buf = make([]byte, r.minBufLen)
		}
		r.rp = 0
		r.wp = 0
	}
}
