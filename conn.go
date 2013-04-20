package pgx

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

type Connection struct {
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
func Connect(options map[string]string) (c *Connection, err error) {
	c = new(Connection)

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

func (c *Connection) Close() (err error) {
	buf := c.getBuf(5)
	buf[0] = 'X'
	binary.BigEndian.PutUint32(buf[1:], 4)
	_, err = c.conn.Write(buf)
	return
}

func (c *Connection) SelectFunc(sql string, onDataRow func(*messageReader, []fieldDescription) error) (err error) {
	if err = c.sendSimpleQuery(sql); err != nil {
		return
	}

	var callbackError error
	var fields []fieldDescription

	for {
		var t byte
		var r *messageReader
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
	onDataRow := func(r *messageReader, fields []fieldDescription) error {
		rows = append(rows, c.rxDataRow(r, fields))
		return nil
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllString(sql string) (strings []string, err error) {
	strings = make([]string, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) error {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		strings = append(strings, s)
		return nil
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllInt64(sql string) (ints []int64, err error) {
	ints = make([]int64, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		var i int64
		i, parseError = strconv.ParseInt(s, 10, 64)
		ints = append(ints, i)
		return
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllInt32(sql string) (ints []int32, err error) {
	ints = make([]int32, 0, 8)
	onDataRow := func(r *messageReader, fields []fieldDescription) (parseError error) {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		var i int64
		i, parseError = strconv.ParseInt(s, 10, 32)
		ints = append(ints, int32(i))
		return
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllInt16(sql string) (ints []int16, err error) {
	ints = make([]int16, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		var i int64
		i, parseError = strconv.ParseInt(s, 10, 16)
		ints = append(ints, int16(i))
		return
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllFloat64(sql string) (floats []float64, err error) {
	floats = make([]float64, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		var f float64
		f, parseError = strconv.ParseFloat(s, 64)
		floats = append(floats, f)
		return
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllFloat32(sql string) (floats []float32, err error) {
	floats = make([]float32, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		var f float64
		f, parseError = strconv.ParseFloat(s, 32)
		floats = append(floats, float32(f))
		return
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
		var r *messageReader
		if t, r, err = c.rxMsg(); err == nil {
			switch t {
			case readyForQuery:
				return
			case rowDescription:
			case dataRow:
			case commandComplete:
				commandTag = r.readString()
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
func (c *Connection) processContextFreeMsg(t byte, r *messageReader) (err error) {
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

func (c *Connection) rxMsg() (t byte, r *messageReader, err error) {
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

func (c *Connection) rxAuthenticationX(r *messageReader) (err error) {
	code := r.readInt32()
	switch code {
	case 0: // AuthenticationOk
	case 3: // AuthenticationCleartextPassword
		c.txPasswordMessage(c.options["password"])
	case 5: // AuthenticationMD5Password
		salt := r.readByteString(4)
		digestedPassword := "md5" + hexMD5(hexMD5(c.options["password"]+c.options["user"])+salt)
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

func (c *Connection) rxParameterStatus(r *messageReader) {
	key := r.readString()
	value := r.readString()
	c.runtimeParams[key] = value
}

func (c *Connection) rxErrorResponse(r *messageReader) (err PgError) {
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

func (c *Connection) rxBackendKeyData(r *messageReader) {
	c.pid = r.readInt32()
	c.secretKey = r.readInt32()
}

func (c *Connection) rxReadyForQuery(r *messageReader) {
	c.txStatus = r.readByte()
}

func (c *Connection) rxRowDescription(r *messageReader) (fields []fieldDescription) {
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

func (c *Connection) rxDataRow(r *messageReader, fields []fieldDescription) (row map[string]string) {
	fieldCount := r.readInt16()

	row = make(map[string]string, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		size := r.readInt32()
		if size > -1 {
			row[fields[i].name] = r.readByteString(size)
		}
	}
	return
}

func (c *Connection) rxDataRowFirstValue(r *messageReader) (s string, null bool) {
	r.readInt16() // ignore field count

	size := r.readInt32()
	if size > -1 {
		s = r.readByteString(size)
	} else {
		null = true
	}

	return
}

func (c *Connection) rxCommandComplete(r *messageReader) string {
	return r.readString()
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
