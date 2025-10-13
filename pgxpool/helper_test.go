package pgxpool_test

import (
	"context"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// delayProxy is a that introduces a configurable delay on reads from the database connection.
type delayProxy struct {
	net.Conn
	readDelay time.Duration
}

func newDelayProxy(conn net.Conn, readDelay time.Duration) *delayProxy {
	p := &delayProxy{
		Conn:      conn,
		readDelay: readDelay,
	}

	return p
}

func (dp *delayProxy) Read(b []byte) (int, error) {
	if dp.readDelay > 0 {
		time.Sleep(dp.readDelay)
	}

	return dp.Conn.Read(b)
}

func newDelayProxyDialFunc(readDelay time.Duration) pgconn.DialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := net.Dial(network, addr)
		return newDelayProxy(conn, readDelay), err
	}
}
