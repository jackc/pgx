//go:build !unix

package nbconn

func (c *NetConn) realNonblockingWrite(b []byte) (n int, err error) {
	return c.fakeNonblockingWrite(b)
}

func (c *NetConn) realNonblockingRead(b []byte) (n int, err error) {
	return c.fakeNonblockingRead(b)
}
