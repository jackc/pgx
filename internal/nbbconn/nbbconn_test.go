package nbbconn_test

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/internal/nbbconn"
	"github.com/stretchr/testify/require"
)

func TestWriteIsBuffered(t *testing.T) {
	local, remote := net.Pipe()
	defer func() {
		local.Close()
		remote.Close()
	}()

	conn := nbbconn.New(local)

	// net.Pipe is synchronous so the Write would block if not buffered.
	writeBuf := []byte("test")
	n, err := conn.Write(writeBuf)
	require.NoError(t, err)
	require.EqualValues(t, 4, n)

	errChan := make(chan error, 1)
	go func() {
		err := conn.Flush()
		errChan <- err
	}()

	readBuf := make([]byte, len(writeBuf))
	_, err = remote.Read(readBuf)
	require.NoError(t, err)

	require.NoError(t, <-errChan)
}

func TestReadFlushesWriteBuffer(t *testing.T) {
	local, remote := net.Pipe()
	defer func() {
		local.Close()
		remote.Close()
	}()

	conn := nbbconn.New(local)

	writeBuf := []byte("test")
	n, err := conn.Write(writeBuf)
	require.NoError(t, err)
	require.EqualValues(t, 4, n)

	errChan := make(chan error, 2)
	go func() {
		readBuf := make([]byte, len(writeBuf))
		_, err := remote.Read(readBuf)
		errChan <- err

		_, err = remote.Write([]byte("okay"))
		errChan <- err
	}()

	readBuf := make([]byte, 4)
	_, err = conn.Read(readBuf)
	require.NoError(t, err)
	require.Equal(t, []byte("okay"), readBuf)

	require.NoError(t, <-errChan)
	require.NoError(t, <-errChan)
}

func TestCloseFlushesWriteBuffer(t *testing.T) {
	local, remote := net.Pipe()
	defer func() {
		local.Close()
		remote.Close()
	}()

	conn := nbbconn.New(local)

	writeBuf := []byte("test")
	n, err := conn.Write(writeBuf)
	require.NoError(t, err)
	require.EqualValues(t, 4, n)

	errChan := make(chan error, 1)
	go func() {
		readBuf := make([]byte, len(writeBuf))
		_, err := remote.Read(readBuf)
		errChan <- err
	}()

	err = conn.Close()
	require.NoError(t, err)

	require.NoError(t, <-errChan)
}

func TestNonBlockingRead(t *testing.T) {
	local, remote := net.Pipe()
	defer func() {
		local.Close()
		remote.Close()
	}()

	conn := nbbconn.New(local)

	err := conn.SetReadDeadline(nbbconn.NonBlockingDeadline)
	require.NoError(t, err)

	buf := make([]byte, 4)
	n, err := conn.Read(buf)
	require.ErrorIs(t, err, nbbconn.ErrWouldBlock)
	require.EqualValues(t, 0, n)

	errChan := make(chan error, 1)
	go func() {
		_, err := remote.Write([]byte("okay"))
		errChan <- err
	}()

	err = conn.SetReadDeadline(time.Time{})
	require.NoError(t, err)

	n, err = conn.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 4, n)
}
