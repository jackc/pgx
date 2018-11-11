package base

import (
	"net"

	"github.com/jackc/pgx/pgproto3"
)

// Conn is a low-level PostgreSQL connection handle. It is not safe for concurrent usage.
type Conn struct {
	NetConn       net.Conn          // the underlying TCP or unix domain socket connection
	PID           uint32            // backend pid
	SecretKey     uint32            // key to use to send a cancel query message to the server
	RuntimeParams map[string]string // parameters that have been reported by the server
	TxStatus      byte
	Frontend      *pgproto3.Frontend
}

func (conn *Conn) ReceiveMessage() (pgproto3.BackendMessage, error) {
	msg, err := conn.Frontend.Receive()
	if err != nil {
		return nil, err
	}

	switch msg := msg.(type) {
	case *pgproto3.ReadyForQuery:
		conn.TxStatus = msg.TxStatus
	case *pgproto3.ParameterStatus:
		conn.RuntimeParams[msg.Name] = msg.Value
	}

	return msg, nil
}
