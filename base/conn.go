package base

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/pgproto3"
)

// PgError represents an error reported by the PostgreSQL server. See
// http://www.postgresql.org/docs/9.3/static/protocol-error-fields.html for
// detailed field description.
type PgError struct {
	Severity         string
	Code             string
	Message          string
	Detail           string
	Hint             string
	Position         int32
	InternalPosition int32
	InternalQuery    string
	Where            string
	SchemaName       string
	TableName        string
	ColumnName       string
	DataTypeName     string
	ConstraintName   string
	File             string
	Line             int32
	Routine          string
}

func (pe PgError) Error() string {
	return pe.Severity + ": " + pe.Message + " (SQLSTATE " + pe.Code + ")"
}

// DialFunc is a function that can be used to connect to a PostgreSQL server
type DialFunc func(network, addr string) (net.Conn, error)

// ErrTLSRefused occurs when the connection attempt requires TLS and the
// PostgreSQL server refuses to use TLS
var ErrTLSRefused = errors.New("server refused TLS connection")

type ConnConfig struct {
	Host          string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port          uint16 // default: 5432
	Database      string
	User          string // default: OS user name
	Password      string
	TLSConfig     *tls.Config // config for TLS connection -- nil disables TLS
	Dial          DialFunc
	RuntimeParams map[string]string // Run-time parameters to set on connection as session default values (e.g. search_path or application_name)
}

func (cc *ConnConfig) NetworkAddress() (network, address string) {
	// If host is a valid path, then address is unix socket
	if _, err := os.Stat(cc.Host); err == nil {
		network = "unix"
		address = cc.Host
		if !strings.Contains(address, "/.s.PGSQL.") {
			address = filepath.Join(address, ".s.PGSQL.") + strconv.FormatInt(int64(cc.Port), 10)
		}
	} else {
		network = "tcp"
		address = fmt.Sprintf("%s:%d", cc.Host, cc.Port)
	}

	return network, address
}

func (cc *ConnConfig) assignDefaults() error {
	if cc.User == "" {
		user, err := user.Current()
		if err != nil {
			return err
		}
		cc.User = user.Username
	}

	if cc.Port == 0 {
		cc.Port = 5432
	}

	if cc.Dial == nil {
		defaultDialer := &net.Dialer{KeepAlive: 5 * time.Minute}
		cc.Dial = defaultDialer.Dial
	}

	return nil
}

// Conn is a low-level PostgreSQL connection handle. It is not safe for concurrent usage.
type Conn struct {
	NetConn       net.Conn          // the underlying TCP or unix domain socket connection
	PID           uint32            // backend pid
	SecretKey     uint32            // key to use to send a cancel query message to the server
	RuntimeParams map[string]string // parameters that have been reported by the server
	TxStatus      byte
	Frontend      *pgproto3.Frontend

	Config ConnConfig
}

func Connect(cc ConnConfig) (*Conn, error) {
	err := cc.assignDefaults()
	if err != nil {
		return nil, err
	}

	conn := new(Conn)
	conn.Config = cc

	conn.NetConn, err = cc.Dial(cc.NetworkAddress())
	if err != nil {
		return nil, err
	}

	conn.RuntimeParams = make(map[string]string)

	if cc.TLSConfig != nil {
		if err := conn.startTLS(cc.TLSConfig); err != nil {
			return nil, err
		}
	}

	conn.Frontend, err = pgproto3.NewFrontend(conn.NetConn, conn.NetConn)
	if err != nil {
		return nil, err
	}

	startupMsg := pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      make(map[string]string),
	}

	// Copy default run-time params
	for k, v := range cc.RuntimeParams {
		startupMsg.Parameters[k] = v
	}

	startupMsg.Parameters["user"] = cc.User
	if cc.Database != "" {
		startupMsg.Parameters["database"] = cc.Database
	}

	if _, err := conn.NetConn.Write(startupMsg.Encode(nil)); err != nil {
		return nil, err
	}

	for {
		msg, err := conn.ReceiveMessage()
		if err != nil {
			return nil, err
		}

		switch msg := msg.(type) {
		case *pgproto3.BackendKeyData:
			conn.PID = msg.ProcessID
			conn.SecretKey = msg.SecretKey
		case *pgproto3.Authentication:
			if err = conn.rxAuthenticationX(msg); err != nil {
				return nil, err
			}
		case *pgproto3.ReadyForQuery:
			return conn, nil
		case *pgproto3.ParameterStatus:
			// handled by ReceiveMessage
		case *pgproto3.ErrorResponse:
			return nil, PgError{
				Severity:         msg.Severity,
				Code:             msg.Code,
				Message:          msg.Message,
				Detail:           msg.Detail,
				Hint:             msg.Hint,
				Position:         msg.Position,
				InternalPosition: msg.InternalPosition,
				InternalQuery:    msg.InternalQuery,
				Where:            msg.Where,
				SchemaName:       msg.SchemaName,
				TableName:        msg.TableName,
				ColumnName:       msg.ColumnName,
				DataTypeName:     msg.DataTypeName,
				ConstraintName:   msg.ConstraintName,
				File:             msg.File,
				Line:             msg.Line,
				Routine:          msg.Routine,
			}
		default:
			return nil, errors.New("unexpected message")
		}
	}
}

func (conn *Conn) startTLS(tlsConfig *tls.Config) (err error) {
	err = binary.Write(conn.NetConn, binary.BigEndian, []int32{8, 80877103})
	if err != nil {
		return
	}

	response := make([]byte, 1)
	if _, err = io.ReadFull(conn.NetConn, response); err != nil {
		return
	}

	if response[0] != 'S' {
		return ErrTLSRefused
	}

	conn.NetConn = tls.Client(conn.NetConn, tlsConfig)

	return nil
}

func (c *Conn) rxAuthenticationX(msg *pgproto3.Authentication) (err error) {
	switch msg.Type {
	case pgproto3.AuthTypeOk:
	case pgproto3.AuthTypeCleartextPassword:
		err = c.txPasswordMessage(c.Config.Password)
	case pgproto3.AuthTypeMD5Password:
		digestedPassword := "md5" + hexMD5(hexMD5(c.Config.Password+c.Config.User)+string(msg.Salt[:]))
		err = c.txPasswordMessage(digestedPassword)
	default:
		err = errors.New("Received unknown authentication message")
	}

	return
}

func (conn *Conn) txPasswordMessage(password string) (err error) {
	msg := &pgproto3.PasswordMessage{Password: password}
	_, err = conn.NetConn.Write(msg.Encode(nil))
	return err
}

func hexMD5(s string) string {
	hash := md5.New()
	io.WriteString(hash, s)
	return hex.EncodeToString(hash.Sum(nil))
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
