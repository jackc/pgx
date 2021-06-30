package pgproto3_test

import (
	"io"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
)

func TestBackendReceiveInterrupted(t *testing.T) {
	t.Parallel()

	server := &interruptReader{}
	server.push([]byte{'Q', 0, 0, 0, 6})

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(server), nil)

	msg, err := backend.Receive()
	if err == nil {
		t.Fatal("expected err")
	}
	if msg != nil {
		t.Fatalf("did not expect msg, but %v", msg)
	}

	server.push([]byte{'I', 0})

	msg, err = backend.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if msg, ok := msg.(*pgproto3.Query); !ok || msg.String != "I" {
		t.Fatalf("unexpected msg: %v", msg)
	}
}

func TestBackendReceiveUnexpectedEOF(t *testing.T) {
	t.Parallel()

	server := &interruptReader{}
	server.push([]byte{'Q', 0, 0, 0, 6})

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(server), nil)

	// Receive regular msg
	msg, err := backend.Receive()
	assert.Nil(t, msg)
	assert.Equal(t, io.ErrUnexpectedEOF, err)

	// Receive FE msg
	server.push([]byte{'F', 0, 0, 0, 6})
	msg, err = backend.ReceiveStartupMessage()
	assert.Nil(t, msg)
	assert.Equal(t, io.ErrUnexpectedEOF, err)
}
