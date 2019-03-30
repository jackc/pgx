package pgproto3

import (
	"io"

	"github.com/jackc/chunkreader"
)

// ChunkReader is an interface to decouple github.com/jackc/chunkreader from this package.
type ChunkReader interface {
	// Next returns buf filled with the next n bytes. If an error occurs, buf will be nil. Next must
	// not reuse buf. In case of error, Next must preserve partially read data.
	Next(n int) (buf []byte, err error)
}

func NewChunkReader(r io.Reader) ChunkReader {
	return chunkreader.NewChunkReader(r)
}
