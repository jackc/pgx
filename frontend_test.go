package pgproto3_test

import (
	"io"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestErrorResponse(t *testing.T) {
	t.Parallel()

	want := &pgproto3.ErrorResponse{
		Severity:            "ERROR",
		SeverityUnlocalized: "ERROR",
		Message:             `column "foo" does not exist`,
		File:                "parse_relation.c",
		Code:                "42703",
		Position:            8,
		Line:                3513,
		Routine:             "errorMissingColumn",
	}

	raw := []byte{
		'E', 0, 0, 0, 'f',
		'S', 'E', 'R', 'R', 'O', 'R', 0,
		'V', 'E', 'R', 'R', 'O', 'R', 0,
		'C', '4', '2', '7', '0', '3', 0,
		'M', 'c', 'o', 'l', 'u', 'm', 'n', 32, '"', 'f', 'o', 'o', '"', 32, 'd', 'o', 'e', 's', 32, 'n', 'o', 't', 32, 'e', 'x', 'i', 's', 't', 0,
		'P', '8', 0,
		'F', 'p', 'a', 'r', 's', 'e', '_', 'r', 'e', 'l', 'a', 't', 'i', 'o', 'n', '.', 'c', 0,
		'L', '3', '5', '1', '3', 0,
		'R', 'e', 'r', 'r', 'o', 'r', 'M', 'i', 's', 's', 'i', 'n', 'g', 'C', 'o', 'l', 'u', 'm', 'n', 0, 0,
	}

	server := &interruptReader{}
	server.push(raw)

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(server), nil)

	got, err := frontend.Receive()
	require.NoError(t, err)
	assert.Equal(t, want, got)
}
