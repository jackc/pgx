package pgx

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
)

type ConnectionParameters struct {
	Socket   string // path to unix domain socket (e.g. /private/tmp/.s.PGSQL.5432)
	Host     string
	Port     uint16 // default: 5432
	Database string
	User     string
	Password string
}

type Connection struct {
	conn          net.Conn             // the underlying TCP or unix domain socket connection
	buf           []byte               // work buffer to avoid constant alloc and dealloc
	pid           int32                // backend pid
	secretKey     int32                // key to use to send a cancel query message to the server
	runtimeParams map[string]string    // parameters that have been reported by the server
	parameters    ConnectionParameters // parameters used when establishing this connection
	txStatus      byte
}

func Connect(parameters ConnectionParameters) (c *Connection, err error) {
	c = new(Connection)

	c.parameters = parameters
	if c.parameters.Port == 0 {
		c.parameters.Port = 5432
	}

	if c.parameters.Socket != "" {
		c.conn, err = net.Dial("unix", c.parameters.Socket)
		if err != nil {
			return nil, err
		}
	} else if c.parameters.Host != "" {
		c.conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", c.parameters.Host, c.parameters.Port))
		if err != nil {
			return nil, err
		}
	}

	c.buf = make([]byte, 1024)
	c.runtimeParams = make(map[string]string)

	msg := newStartupMessage()
	msg.options["user"] = c.parameters.User
	if c.parameters.Database != "" {
		msg.options["database"] = c.parameters.Database
	}
	c.txStartupMessage(msg)

	for {
		var t byte
		var r *MessageReader
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

func (c *Connection) Close() (err error) {
	buf := c.getBuf(5)
	buf[0] = 'X'
	binary.BigEndian.PutUint32(buf[1:], 4)
	_, err = c.conn.Write(buf)
	return
}

func (c *Connection) SelectFunc(sql string, onDataRow func(*MessageReader, []FieldDescription) error) (err error) {
	if err = c.sendSimpleQuery(sql); err != nil {
		return
	}

	var callbackError error
	var fields []FieldDescription

	for {
		var t byte
		var r *MessageReader
		if t, r, err = c.rxMsg(); err == nil {
			switch t {
			case readyForQuery:
				if err == nil {
					err = callbackError
				}
				return
			case rowDescription:
				fields = c.rxRowDescription(r)
			case dataRow:
				if callbackError == nil {
					callbackError = onDataRow(r, fields)
				}
			case commandComplete:
			default:
				if err = c.processContextFreeMsg(t, r); err != nil {
					return
				}
			}
		} else {
			return
		}
	}

	panic("Unreachable")
}

// Null values are not included in rows. However, because maps return the 0 value
// for missing values this flattens nulls to empty string. If the caller needs to
// distinguish between a real empty string and a null it can use the comma ok
// pattern when accessing the map
func (c *Connection) SelectRows(sql string) (rows []map[string]string, err error) {
	rows = make([]map[string]string, 0, 8)
	onDataRow := func(r *MessageReader, fields []FieldDescription) error {
		rows = append(rows, c.rxDataRow(r, fields))
		return nil
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) sendSimpleQuery(sql string) (err error) {
	bufSize := 5 + len(sql) + 1 // message identifier (1), message size (4), null string terminator (1)
	buf := c.getBuf(bufSize)
	buf[0] = 'Q'
	binary.BigEndian.PutUint32(buf[1:5], uint32(bufSize-1))
	copy(buf[5:], sql)
	buf[bufSize-1] = 0

	_, err = c.conn.Write(buf)
	return err
}

func (c *Connection) Execute(sql string) (commandTag string, err error) {
	if err = c.sendSimpleQuery(sql); err != nil {
		return
	}

	for {
		var t byte
		var r *MessageReader
		if t, r, err = c.rxMsg(); err == nil {
			switch t {
			case readyForQuery:
				return
			case rowDescription:
			case dataRow:
			case commandComplete:
				commandTag = r.ReadString()
			default:
				if err = c.processContextFreeMsg(t, r); err != nil {
					return
				}
			}
		} else {
			return
		}
	}

	panic("Unreachable")
}

// Processes messages that are not exclusive to one context such as
// authentication or query response. The response to these messages
// is the same regardless of when they occur.
func (c *Connection) processContextFreeMsg(t byte, r *MessageReader) (err error) {
	switch t {
	case 'S':
		c.rxParameterStatus(r)
		return nil
	case errorResponse:
		return c.rxErrorResponse(r)
	case noticeResponse:
		return nil
	default:
		return fmt.Errorf("Received unknown message type: %c", t)
	}

	panic("Unreachable")

}

func (c *Connection) rxMsg() (t byte, r *MessageReader, err error) {
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

func (c *Connection) rxMsgHeader() (t byte, bodySize int32, err error) {
	buf := c.buf[:5]
	if _, err = io.ReadFull(c.conn, buf); err != nil {
		return 0, 0, err
	}

	t = buf[0]
	bodySize = int32(binary.BigEndian.Uint32(buf[1:5])) - 4
	return t, bodySize, nil
}

func (c *Connection) rxMsgBody(bodySize int32) (buf []byte, err error) {
	buf = c.getBuf(int(bodySize))
	_, err = io.ReadFull(c.conn, buf)
	return
}

func (c *Connection) rxAuthenticationX(r *MessageReader) (err error) {
	code := r.ReadInt32()
	switch code {
	case 0: // AuthenticationOk
	case 3: // AuthenticationCleartextPassword
		c.txPasswordMessage(c.parameters.Password)
	case 5: // AuthenticationMD5Password
		salt := r.ReadByteString(4)
		digestedPassword := "md5" + hexMD5(hexMD5(c.parameters.Password+c.parameters.User)+salt)
		c.txPasswordMessage(digestedPassword)
	default:
		err = errors.New("Received unknown authentication message")
	}

	return
}

func hexMD5(s string) string {
	hash := md5.New()
	io.WriteString(hash, s)
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *Connection) rxParameterStatus(r *MessageReader) {
	key := r.ReadString()
	value := r.ReadString()
	c.runtimeParams[key] = value
}

func (c *Connection) rxErrorResponse(r *MessageReader) (err PgError) {
	for {
		switch r.ReadByte() {
		case 'S':
			err.Severity = r.ReadString()
		case 'C':
			err.Code = r.ReadString()
		case 'M':
			err.Message = r.ReadString()
		case 0: // End of error message
			return
		default: // Ignore other error fields
			r.ReadString()
		}
	}

	panic("Unreachable")
}

func (c *Connection) rxBackendKeyData(r *MessageReader) {
	c.pid = r.ReadInt32()
	c.secretKey = r.ReadInt32()
}

func (c *Connection) rxReadyForQuery(r *MessageReader) {
	c.txStatus = r.ReadByte()
}

func (c *Connection) rxRowDescription(r *MessageReader) (fields []FieldDescription) {
	fieldCount := r.ReadInt16()
	fields = make([]FieldDescription, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		f := &fields[i]
		f.Name = r.ReadString()
		f.Table = r.ReadOid()
		f.AttributeNumber = r.ReadInt16()
		f.DataType = r.ReadOid()
		f.DataTypeSize = r.ReadInt16()
		f.Modifier = r.ReadInt32()
		f.FormatCode = r.ReadInt16()
	}
	return
}

func (c *Connection) rxDataRow(r *MessageReader, fields []FieldDescription) (row map[string]string) {
	fieldCount := r.ReadInt16()

	row = make(map[string]string, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		size := r.ReadInt32()
		if size > -1 {
			row[fields[i].Name] = r.ReadByteString(size)
		}
	}
	return
}

func (c *Connection) rxDataRowFirstValue(r *MessageReader) (s string, null bool) {
	r.ReadInt16() // ignore field count

	size := r.ReadInt32()
	if size > -1 {
		s = r.ReadByteString(size)
	} else {
		null = true
	}

	return
}

func (c *Connection) rxCommandComplete(r *MessageReader) string {
	return r.ReadString()
}

func (c *Connection) txStartupMessage(msg *startupMessage) (err error) {
	_, err = c.conn.Write(msg.Bytes())
	return
}

func (c *Connection) txPasswordMessage(password string) (err error) {
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
func (c *Connection) getBuf(n int) (buf []byte) {
	if n <= cap(c.buf) {
		buf = c.buf[:n]
	} else {
		buf = make([]byte, n)
	}
	return
}
