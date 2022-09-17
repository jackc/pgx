//go:build aix || android || darwin || dragonfly || freebsd || hurd || illumos || ios || linux || netbsd || openbsd || solaris

package nbconn

// Not using unix build tag for support on Go 1.18.

import (
	"errors"
	"io"
	"syscall"
)

// realNonblockingWrite does a non-blocking write. readFlushLock must already be held.
func (c *NetConn) realNonblockingWrite(b []byte) (n int, err error) {
	c.nonblockWriteBuf = b
	c.nonblockWriteN = 0
	c.nonblockWriteErr = nil
	err = c.rawConn.Write(func(fd uintptr) (done bool) {
		c.nonblockWriteN, c.nonblockWriteErr = syscall.Write(int(fd), c.nonblockWriteBuf)
		return true
	})
	n = c.nonblockWriteN
	if err == nil && c.nonblockWriteErr != nil {
		if errors.Is(c.nonblockWriteErr, syscall.EWOULDBLOCK) {
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
	var funcErr error
	err = c.rawConn.Read(func(fd uintptr) (done bool) {
		n, funcErr = syscall.Read(int(fd), b)
		return true
	})
	if err == nil && funcErr != nil {
		if errors.Is(funcErr, syscall.EWOULDBLOCK) {
			err = ErrWouldBlock
		} else {
			err = funcErr
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
