//go:build windows

package nbconn

import (
	"errors"
	"golang.org/x/sys/windows"
	"io"
	"syscall"
	"unsafe"
)

var dll = syscall.MustLoadDLL("ws2_32.dll")

// int ioctlsocket(
//
//	[in]      SOCKET s,
//	[in]      long   cmd,
//	[in, out] u_long *argp
//
// );
var ioctlsocket = dll.MustFindProc("ioctlsocket")

type sockMode int

const (
	FIONBIO             int      = 0x8004667e
	sockModeBlocking    sockMode = 0
	sockModeNonBlocking sockMode = 1
)

func setSockMode(fd uintptr, mode sockMode) error {
	res, _, err := ioctlsocket.Call(fd, uintptr(FIONBIO), uintptr(unsafe.Pointer(&mode)))
	// Upon successful completion, the ioctlsocket returns zero.
	if res != 0 && err != nil {
		return err
	}

	return nil
}

// realNonblockingWrite does a non-blocking write. readFlushLock must already be held.
func (c *NetConn) realNonblockingWrite(b []byte) (n int, err error) {
	if c.nonblockWriteFunc == nil {
		c.nonblockWriteFunc = func(fd uintptr) (done bool) {
			// Make sock non-blocking
			if err := setSockMode(fd, sockModeNonBlocking); err != nil {
				c.nonblockWriteErr = err
				return true
			}

			var written uint32
			var buf syscall.WSABuf
			buf.Buf = &c.nonblockWriteBuf[0]
			buf.Len = uint32(len(c.nonblockWriteBuf))
			c.nonblockWriteErr = syscall.WSASend(syscall.Handle(fd), &buf, 1, &written, 0, nil, nil)
			c.nonblockWriteN = int(written)

			// Make sock blocking again
			if err := setSockMode(fd, sockModeBlocking); err != nil {
				c.nonblockWriteErr = err
				return true
			}

			return true
		}
	}
	c.nonblockWriteBuf = b
	c.nonblockWriteN = 0
	c.nonblockWriteErr = nil

	err = c.rawConn.Write(c.nonblockWriteFunc)
	n = c.nonblockWriteN
	c.nonblockWriteBuf = nil // ensure that no reference to b is kept.
	if err == nil && c.nonblockWriteErr != nil {
		if errors.Is(c.nonblockWriteErr, windows.WSAEWOULDBLOCK) {
			err = ErrWouldBlock
		} else {
			err = c.nonblockWriteErr
		}
	}
	if err != nil {
		// n may be -1 when an error occurs.
		if n < 0 {
			n = 0
		}

		return n, err
	}

	return n, nil
}

func (c *NetConn) realNonblockingRead(b []byte) (n int, err error) {
	if c.nonblockReadFunc == nil {
		c.nonblockReadFunc = func(fd uintptr) (done bool) {
			// Make sock non-blocking
			if err := setSockMode(fd, sockModeNonBlocking); err != nil {
				c.nonblockWriteErr = err
				return true
			}

			var read uint32
			var flags uint32
			var buf syscall.WSABuf
			buf.Buf = &c.nonblockReadBuf[0]
			buf.Len = uint32(len(c.nonblockReadBuf))
			c.nonblockReadErr = syscall.WSARecv(syscall.Handle(fd), &buf, 1, &read, &flags, nil, nil)
			c.nonblockReadN = int(read)

			// Make sock blocking again
			if err := setSockMode(fd, sockModeBlocking); err != nil {
				c.nonblockWriteErr = err
				return true
			}

			return true
		}
	}
	c.nonblockReadBuf = b
	c.nonblockReadN = 0
	c.nonblockReadErr = nil

	err = c.rawConn.Read(c.nonblockReadFunc)
	n = c.nonblockReadN
	c.nonblockReadBuf = nil // ensure that no reference to b is kept.
	if err == nil && c.nonblockReadErr != nil {
		if errors.Is(c.nonblockReadErr, windows.WSAEWOULDBLOCK) {
			err = ErrWouldBlock
		} else {
			err = c.nonblockReadErr
		}
	}
	if err != nil {
		// n may be -1 when an error occurs.
		if n < 0 {
			n = 0
		}

		return n, err
	}

	// syscall read did not return an error and 0 bytes were read means EOF.
	if n == 0 {
		return 0, io.EOF
	}

	return n, nil
}
