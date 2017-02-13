package chunkreader

import (
	"io"
)

type ChunkReader struct {
	r io.Reader

	buf    []byte
	rp, wp int // buf read position and write position
	taken  bool

	options Options
}

type Options struct {
	MinBufLen int // Minimum buffer length
	BlockLen  int // Increments to expand buffer (e.g. a 8000 byte request with a BlockLen of 1024 would yield a buffer len of 8192)
}

func NewChunkReader(r io.Reader) *ChunkReader {
	cr, err := NewChunkReaderEx(r, Options{})
	if err != nil {
		panic("default options can't be bad")
	}

	return cr
}

func NewChunkReaderEx(r io.Reader, options Options) (*ChunkReader, error) {
	if options.MinBufLen == 0 {
		options.MinBufLen = 4096
	}
	if options.BlockLen == 0 {
		options.BlockLen = 512
	}

	return &ChunkReader{
		r:       r,
		buf:     make([]byte, options.MinBufLen),
		options: options,
	}, nil
}

// Next returns buf filled with the next n bytes. buf is only valid until the
// next call to Next. If an error occurs, buf will be nil.
func (r *ChunkReader) Next(n int) (buf []byte, err error) {
	// n bytes already in buf
	if (r.wp - r.rp) >= n {
		buf = r.buf[r.rp : r.rp+n]
		r.rp += n
		return buf, err
	}

	// available space in buf is less than n
	if len(r.buf) < n {
		r.copyBufContents(r.newBuf(n))
		r.taken = false
	}

	// buf is large enough, but need to shift filled area to start to make enough contiguous space
	minReadCount := n - (r.wp - r.rp)
	if (len(r.buf) - r.wp) < minReadCount {
		newBuf := r.buf
		if r.taken {
			newBuf = r.newBuf(n)
			r.taken = false
		}
		r.copyBufContents(newBuf)
	}

	if err := r.appendAtLeast(minReadCount); err != nil {
		return nil, err
	}

	buf = r.buf[r.rp : r.rp+n]
	r.rp += n
	return buf, nil
}

// KeepLast prevents the last data retrieved by Next from being reused by the
// ChunkReader.
func (r *ChunkReader) KeepLast() {
	r.taken = true
}

func (r *ChunkReader) appendAtLeast(fillLen int) error {
	n, err := io.ReadAtLeast(r.r, r.buf[r.wp:], fillLen)
	r.wp += n
	return err
}

func (r *ChunkReader) newBuf(min int) []byte {
	size := ((min / r.options.BlockLen) + 1) * r.options.BlockLen
	if size < r.options.MinBufLen {
		size = r.options.MinBufLen
	}
	return make([]byte, size)
}

func (r *ChunkReader) copyBufContents(dest []byte) {
	r.wp = copy(dest, r.buf[r.rp:r.wp])
	r.rp = 0
	r.buf = dest
}
