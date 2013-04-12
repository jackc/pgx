package pqx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

type conn struct {
	conn          net.Conn          // the underlying TCP or unix domain socket connection
	buf           []byte            // work buffer to avoid constant alloc and dealloc
	pid           int32             // backend pid
	secretKey     int32             // key to use to send a cancel query message to the server
	runtimeParams map[string]string // parameters that have been reported by the server
	options       map[string]string // options used when establishing connection
	txStatus      byte
}

// options:
//   socket: path to unix domain socket
//   database: name of database
func Connect(options map[string]string) (c *conn, err error) {
	c = new(conn)

	c.options = make(map[string]string)
	for k, v := range options {
		c.options[k] = v
	}

	var present bool
	var socket string

	if socket, present = options["socket"]; present {
		c.conn, err = net.Dial("unix", socket)
		if err != nil {
			return nil, err
		}
	}

	c.buf = make([]byte, 1024)
	c.runtimeParams = make(map[string]string)

	// conn, err := net.Dial("tcp", "localhost:5432")

	var database string

	msg := newStartupMessage()
	msg.options["user"], _ = options["user"]
	if database, present = options["database"]; present {
		msg.options["database"] = database
	}
	c.txStartupMessage(msg)

	for {
		var t byte
		var r *messageReader
		if t, r, err = c.rxMsg(); err == nil {
			switch t {
			case backendKeyData:
				c.rxBackendKeyData(r)
			case authenticationX:
				if err = c.rxAuthenticationX(r); err != nil {
					return nil, err
				}
			case readyForQuery:
				return c, nil
			default:
				if err = c.processContextFreeMsg(t, r); err != nil {
					return nil, err
				}
			}
		} else {
			return nil, err
		}
	}

	panic("Unreachable")
}

func (c *conn) Close() (err error) {
	buf := c.getBuf(5)
	buf[0] = 'X'
	binary.BigEndian.PutUint32(buf[1:], 4)
	_, err = c.conn.Write(buf)
	return
}

func (c *conn) Query(sql string) (rows []map[string]string, err error) {
	if err = c.sendSimpleQuery(sql); err != nil {
		return
	}

	var fields []fieldDescription
	rows = make([]map[string]string, 0)
	for {
		var t byte
		var r *messageReader
		if t, r, err = c.rxMsg(); err == nil {
			switch t {
			case readyForQuery:
				return rows, nil
			case rowDescription:
				fields = c.rxRowDescription(r)
			case dataRow:
				rows = append(rows, c.rxDataRow(r, fields))
			case commandComplete:
				c.rxCommandComplete(r)
			default:
				if err = c.processContextFreeMsg(t, r); err != nil {
					return nil, err
				}
			}
		} else {
			return nil, err
		}
	}

	panic("Unreachable")
}

func (c *conn) sendSimpleQuery(sql string) (err error) {
	bufSize := 5 + len(sql) + 1 // message identifier (1), message size (4), null string terminator (1)
	buf := c.getBuf(bufSize)
	buf[0] = 'Q'
	binary.BigEndian.PutUint32(buf[1:5], uint32(bufSize-1))
	copy(buf[5:], sql)
	buf[bufSize-1] = 0

	_, err = c.conn.Write(buf)
	return err
}

// Processes messages that are not exclusive to one context such as
// authentication or query response. The response to these messages
// is the same regardless of when they occur.
func (c *conn) processContextFreeMsg(t byte, r *messageReader) (err error) {
	switch t {
	case 'S':
		c.rxParameterStatus(r)
		return nil
	case errorResponse:
		return c.rxErrorResponse(r)
	default:
		return fmt.Errorf("Received unknown message type: %c", t)
	}

	panic("Unreachable")

}

func (c *conn) rxMsg() (t byte, r *messageReader, err error) {
	var bodySize int32
	t, bodySize, err = c.rxMsgHeader()
	if err != nil {
		return
	}

	var body []byte
	if body, err = c.rxMsgBody(bodySize); err != nil {
		return
	}

	r = newMessageReader(body)
	return
}

func (c *conn) rxMsgHeader() (t byte, bodySize int32, err error) {
	buf := c.buf[:5]
	if _, err = io.ReadFull(c.conn, buf); err != nil {
		return 0, 0, err
	}

	t = buf[0]
	bodySize = int32(binary.BigEndian.Uint32(buf[1:5])) - 4
	return t, bodySize, nil
}

func (c *conn) rxMsgBody(bodySize int32) (buf []byte, err error) {
	buf = c.getBuf(int(bodySize))
	_, err = io.ReadFull(c.conn, buf)
	return
}

func (c *conn) rxAuthenticationX(r *messageReader) (err error) {
	code := r.readInt32()
	switch code {
	case 0: // AuthenticationOk
	case 3: // AuthenticationCleartextPassword
		c.txPasswordMessage(c.options["password"])
	default:
		err = errors.New("Received unknown authentication message")
	}

	return
}

func (c *conn) rxParameterStatus(r *messageReader) {
	key := r.readString()
	value := r.readString()
	c.runtimeParams[key] = value
}

func (c *conn) rxErrorResponse(r *messageReader) (err PgError) {
	for {
		switch r.readByte() {
		case 'S':
			err.Severity = r.readString()
		case 'C':
			err.Code = r.readString()
		case 'M':
			err.Message = r.readString()
		case 0: // End of error message
			return
		default: // Ignore other error fields
			r.readString()
		}
	}

	panic("Unreachable")
}

func (c *conn) rxBackendKeyData(r *messageReader) {
	c.pid = r.readInt32()
	c.secretKey = r.readInt32()
}

func (c *conn) rxReadyForQuery(r *messageReader) {
	c.txStatus = r.readByte()
}

func (c *conn) rxRowDescription(r *messageReader) (fields []fieldDescription) {
	fieldCount := r.readInt16()
	fields = make([]fieldDescription, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		f := &fields[i]
		f.name = r.readString()
		f.table = r.readOid()
		f.attributeNumber = r.readInt16()
		f.dataType = r.readOid()
		f.dataTypeSize = r.readInt16()
		f.modifier = r.readInt32()
		f.formatCode = r.readInt16()
	}
	return
}

func (c *conn) rxDataRow(r *messageReader, fields []fieldDescription) (row map[string]string) {
	fieldCount := r.readInt16()

	row = make(map[string]string, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		// TODO - handle nulls
		size := r.readInt32()
		row[fields[i].name] = r.readByteString(size)
	}
	return
}

func (c *conn) rxCommandComplete(r *messageReader) string {
	return r.readString()
}

func (c *conn) txStartupMessage(msg *startupMessage) (err error) {
	_, err = c.conn.Write(msg.Bytes())
	return
}

func (c *conn) txPasswordMessage(password string) (err error) {
	bufSize := 5 + len(password) + 1 // message identifier (1), message size (4), password, null string terminator (1)
	buf := c.getBuf(bufSize)
	buf[0] = 'p'
	binary.BigEndian.PutUint32(buf[1:5], uint32(bufSize-1))
	copy(buf[5:], password)
	buf[bufSize-1] = 0

	_, err = c.conn.Write(buf)
	return err
}

// Gets a []byte of n length. If possible it will reuse the connection buffer
// otherwise it will allocate a new buffer
func (c *conn) getBuf(n int) (buf []byte) {
	if n <= cap(c.buf) {
		buf = c.buf[:n]
	} else {
		buf = make([]byte, n)
	}
	return
}
