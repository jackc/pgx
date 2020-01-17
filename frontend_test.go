package pgproto3_test

import (
	"io"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
)

type interruptReader struct {
	chunks [][]byte
}

func (ir *interruptReader) Read(p []byte) (n int, err error) {
	if len(ir.chunks) == 0 {
		return 0, io.EOF
	}

	n = copy(p, ir.chunks[0])
	if n != len(ir.chunks[0]) {
		panic("this test reader doesn't support partial reads of chunks")
	}

	ir.chunks = ir.chunks[1:]

	return n, nil
}

func (ir *interruptReader) push(p []byte) {
	ir.chunks = append(ir.chunks, p)
}

func TestFrontendReceiveInterrupted(t *testing.T) {
	t.Parallel()

	server := &interruptReader{}
	server.push([]byte{'Z', 0, 0, 0, 5})

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(server), nil)

	msg, err := frontend.Receive()
	if err == nil {
		t.Fatal("expected err")
	}
	if msg != nil {
		t.Fatalf("did not expect msg, but %v", msg)
	}

	server.push([]byte{'I'})

	msg, err = frontend.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if msg, ok := msg.(*pgproto3.ReadyForQuery); !ok || msg.TxStatus != 'I' {
		t.Fatalf("unexpected msg: %v", msg)
	}
}

func TestFrontendReceiveUnexpectedEOF(t *testing.T) {
	t.Parallel()

	server := &interruptReader{}
	server.push([]byte{'Z', 0, 0, 0, 5})

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(server), nil)

	msg, err := frontend.Receive()
	if err == nil {
		t.Fatal("expected err")
	}
	if msg != nil {
		t.Fatalf("did not expect msg, but %v", msg)
	}

	msg, err = frontend.Receive()
	assert.Nil(t, msg)
	assert.Equal(t, io.ErrUnexpectedEOF, err)
}
