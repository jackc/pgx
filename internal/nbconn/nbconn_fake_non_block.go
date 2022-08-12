//go:build !(aix || android || darwin || dragonfly || freebsd || hurd || illumos || ios || linux || netbsd || openbsd || solaris)

package nbconn

// Not using unix build tag for support on Go 1.18.

func (c *NetConn) realNonblockingWrite(b []byte) (n int, err error) {
	return c.fakeNonblockingWrite(b)
}

func (c *NetConn) realNonblockingRead(b []byte) (n int, err error) {
	return c.fakeNonblockingRead(b)
}
