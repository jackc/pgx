package faultyconn

import (
	"bytes"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Conn is a wrapper for a net.Conn that allows inspection and modification of messages between a PostgreSQL client and
// server. It is designed to be used in tests that use a *pgx.Conn or *pgconn.PgConn connected to a real PostgreSQL
// server. Instead of mocking an entire server connection, this is used to specify and modify only the particular
// aspects of a connection that are necessary. This can be easier to setup and is more true to real world conditions.
//
// It currently only supports handling frontend messages.
type Conn struct {
	// HandleFrontendMessage is called for each frontend message received. It should use backendWriter to write to the
	// backend.
	HandleFrontendMessage func(backendWriter io.Writer, msg pgproto3.FrontendMessage) error

	// TODO: Implement this if we need to handle backend messages.
	// HandleBackendMessage  func(w io.Writer, msg pgproto3.BackendMessage) error

	conn            net.Conn
	fromFrontendBuf *bytes.Buffer
	fromBackendBuf  *bytes.Buffer
	backend         *pgproto3.Backend
	frontend        *pgproto3.Frontend
}

// NewConn creates a new Conn that proxies the messages sent to and from the given net.Conn. New can be used with
// pgconn.Config.AfterNetConnect to wrap a net.Conn for testing purposes.
func New(c net.Conn) *Conn {
	fromFrontendBuf := &bytes.Buffer{}
	fromBackendBuf := &bytes.Buffer{}

	return &Conn{
		conn:            c,
		fromFrontendBuf: fromFrontendBuf,
		fromBackendBuf:  fromBackendBuf,
		backend:         pgproto3.NewBackend(fromFrontendBuf, c),
		frontend:        pgproto3.NewFrontend(fromBackendBuf, c),
	}
}

func (c *Conn) Read(b []byte) (n int, err error) {
	return c.conn.Read(b)
}

func (c *Conn) Write(b []byte) (n int, err error) {
	if c.HandleFrontendMessage == nil {
		return c.conn.Write(b)
	}

	c.fromFrontendBuf.Write(b)

	for {
		msg, err := c.backend.Receive()
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return len(b), err
		}

		err = c.HandleFrontendMessage(c.conn, msg)
		if err != nil {
			return len(b), err
		}
	}

	return len(b), nil
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
