//go:build unix

package nbconn

import (
	"errors"
	"io"
	"syscall"
)

// realNonblockingWrite does a non-blocking write. readFlushLock must already be held.
func (c *NetConn) realNonblockingWrite(b []byte) (n int, err error) {
	if c.nonblockWriteFunc == nil {
		c.nonblockWriteFunc = func(fd uintptr) (done bool) {
			c.nonblockWriteN, c.nonblockWriteErr = syscall.Write(int(fd), c.nonblockWriteBuf)
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
	if c.nonblockReadFunc == nil {
		c.nonblockReadFunc = func(fd uintptr) (done bool) {
			c.nonblockReadN, c.nonblockReadErr = syscall.Read(int(fd), c.nonblockReadBuf)
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
		if errors.Is(c.nonblockReadErr, syscall.EWOULDBLOCK) {
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
