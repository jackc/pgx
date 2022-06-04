package nbconn_test

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/internal/nbconn"
	"github.com/stretchr/testify/require"
)

func testVariants(t *testing.T, f func(t *testing.T, local *nbconn.Conn, remote net.Conn)) {
	local, remote := net.Pipe()
	defer func() {
		local.Close()
		remote.Close()
	}()

	conn := nbconn.New(local)
	f(t, conn, remote)
}

func TestWriteIsBuffered(t *testing.T) {
	testVariants(t, func(t *testing.T, conn *nbconn.Conn, remote net.Conn) {
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
	})
}

func TestReadFlushesWriteBuffer(t *testing.T) {
	testVariants(t, func(t *testing.T, conn *nbconn.Conn, remote net.Conn) {
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
	})
}

func TestCloseFlushesWriteBuffer(t *testing.T) {
	testVariants(t, func(t *testing.T, conn *nbconn.Conn, remote net.Conn) {
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
	})
}

func TestNonBlockingRead(t *testing.T) {
	testVariants(t, func(t *testing.T, conn *nbconn.Conn, remote net.Conn) {
		err := conn.SetReadDeadline(nbconn.NonBlockingDeadline)
		require.NoError(t, err)

		buf := make([]byte, 4)
		n, err := conn.Read(buf)
		require.ErrorIs(t, err, nbconn.ErrWouldBlock)
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
	})
}

func TestReadPreviouslyBuffered(t *testing.T) {
	testVariants(t, func(t *testing.T, conn *nbconn.Conn, remote net.Conn) {

		errChan := make(chan error, 1)
		go func() {
			err := func() error {
				_, err := remote.Write([]byte("alpha"))
				if err != nil {
					return err
				}

				readBuf := make([]byte, 4)
				_, err = remote.Read(readBuf)
				if err != nil {
					return err
				}

				return nil
			}()
			errChan <- err
		}()

		_, err := conn.Write([]byte("test"))
		require.NoError(t, err)

		// Because net.Pipe() is synchronous conn.Flust must buffer a read.
		err = conn.Flush()
		require.NoError(t, err)

		readBuf := make([]byte, 5)
		n, err := conn.Read(readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 5, n)
		require.Equal(t, []byte("alpha"), readBuf)
	})
}

func TestReadPreviouslyBufferedPartialRead(t *testing.T) {
	testVariants(t, func(t *testing.T, conn *nbconn.Conn, remote net.Conn) {

		errChan := make(chan error, 1)
		go func() {
			err := func() error {
				_, err := remote.Write([]byte("alpha"))
				if err != nil {
					return err
				}

				readBuf := make([]byte, 4)
				_, err = remote.Read(readBuf)
				if err != nil {
					return err
				}

				return nil
			}()
			errChan <- err
		}()

		_, err := conn.Write([]byte("test"))
		require.NoError(t, err)

		// Because net.Pipe() is synchronous conn.Flust must buffer a read.
		err = conn.Flush()
		require.NoError(t, err)

		readBuf := make([]byte, 2)
		n, err := conn.Read(readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 2, n)
		require.Equal(t, []byte("al"), readBuf)

		readBuf = make([]byte, 3)
		n, err = conn.Read(readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 3, n)
		require.Equal(t, []byte("pha"), readBuf)
	})
}

func TestReadMultiplePreviouslyBuffered(t *testing.T) {
	testVariants(t, func(t *testing.T, conn *nbconn.Conn, remote net.Conn) {
		errChan := make(chan error, 1)
		go func() {
			err := func() error {
				_, err := remote.Write([]byte("alpha"))
				if err != nil {
					return err
				}

				_, err = remote.Write([]byte("beta"))
				if err != nil {
					return err
				}

				readBuf := make([]byte, 4)
				_, err = remote.Read(readBuf)
				if err != nil {
					return err
				}

				return nil
			}()
			errChan <- err
		}()

		_, err := conn.Write([]byte("test"))
		require.NoError(t, err)

		// Because net.Pipe() is synchronous conn.Flust must buffer a read.
		err = conn.Flush()
		require.NoError(t, err)

		readBuf := make([]byte, 9)
		n, err := conn.Read(readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 9, n)
		require.Equal(t, []byte("alphabeta"), readBuf)
	})
}

func TestReadPreviouslyBufferedAndReadMore(t *testing.T) {
	testVariants(t, func(t *testing.T, conn *nbconn.Conn, remote net.Conn) {

		flushCompleteChan := make(chan struct{})
		errChan := make(chan error, 1)
		go func() {
			err := func() error {
				_, err := remote.Write([]byte("alpha"))
				if err != nil {
					return err
				}

				readBuf := make([]byte, 4)
				_, err = remote.Read(readBuf)
				if err != nil {
					return err
				}

				<-flushCompleteChan

				_, err = remote.Write([]byte("beta"))
				if err != nil {
					return err
				}

				return nil
			}()
			errChan <- err
		}()

		_, err := conn.Write([]byte("test"))
		require.NoError(t, err)

		// Because net.Pipe() is synchronous conn.Flust must buffer a read.
		err = conn.Flush()
		require.NoError(t, err)

		close(flushCompleteChan)

		readBuf := make([]byte, 9)
		n, err := conn.Read(readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 9, n)
		require.Equal(t, []byte("alphabeta"), readBuf)
	})
}
