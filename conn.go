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

func (c *conn) query(sql string, onDataRow func(*messageReader, []fieldDescription) error) (err error) {
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

func (c *conn) Query(sql string) (rows []map[string]string, err error) {
	rows = make([]map[string]string, 0, 8)
	onDataRow := func(r *messageReader, fields []fieldDescription) error {
		rows = append(rows, c.rxDataRow(r, fields))
		return nil
	}
	err = c.query(sql, onDataRow)
	return
}

func (c *conn) SelectString(sql string) (s string, err error) {
	onDataRow := func(r *messageReader, _ []fieldDescription) error {
		s = c.rxDataRowFirstValue(r)
		return nil
	}
	err = c.query(sql, onDataRow)
	return
}

func (c *conn) selectInt(sql string, size int) (i int64, err error) {
	var s string
	s, err = c.SelectString(sql)
	if err != nil {
		return
	}

	i, err = strconv.ParseInt(s, 10, size)
	return
}

func (c *conn) SelectInt64(sql string) (i int64, err error) {
	return c.selectInt(sql, 64)
}

func (c *conn) SelectInt32(sql string) (i int32, err error) {
	var i64 int64
	i64, err = c.selectInt(sql, 32)
	i = int32(i64)
	return
}

func (c *conn) SelectInt16(sql string) (i int16, err error) {
	var i64 int64
	i64, err = c.selectInt(sql, 16)
	i = int16(i64)
	return
}

func (c *conn) selectFloat(sql string, size int) (f float64, err error) {
	var s string
	s, err = c.SelectString(sql)
	if err != nil {
		return
	}

	f, err = strconv.ParseFloat(s, size)
	return
}

func (c *conn) SelectFloat64(sql string) (f float64, err error) {
	return c.selectFloat(sql, 64)
}

func (c *conn) SelectFloat32(sql string) (f float32, err error) {
	var f64 float64
	f64, err = c.selectFloat(sql, 32)
	f = float32(f64)
	return
}

func (c *conn) SelectAllString(sql string) (strings []string, err error) {
	strings = make([]string, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) error {
		strings = append(strings, c.rxDataRowFirstValue(r))
		return nil
	}
	err = c.query(sql, onDataRow)
	return
}

func (c *conn) SelectAllInt64(sql string) (ints []int64, err error) {
	ints = make([]int64, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		var i int64
		i, parseError = strconv.ParseInt(c.rxDataRowFirstValue(r), 10, 64)
		ints = append(ints, i)
		return
	}
	err = c.query(sql, onDataRow)
	return
}

func (c *conn) SelectAllInt32(sql string) (ints []int32, err error) {
	ints = make([]int32, 0, 8)
	onDataRow := func(r *messageReader, fields []fieldDescription) (parseError error) {
		var i int64
		i, parseError = strconv.ParseInt(c.rxDataRowFirstValue(r), 10, 32)
		ints = append(ints, int32(i))
		return
	}
	err = c.query(sql, onDataRow)
	return
}

func (c *conn) SelectAllInt16(sql string) (ints []int16, err error) {
	ints = make([]int16, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		var i int64
		i, parseError = strconv.ParseInt(c.rxDataRowFirstValue(r), 10, 16)
		ints = append(ints, int16(i))
		return
	}
	err = c.query(sql, onDataRow)
	return
}

func (c *conn) SelectAllFloat64(sql string) (floats []float64, err error) {
	floats = make([]float64, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		var f float64
		f, parseError = strconv.ParseFloat(c.rxDataRowFirstValue(r), 64)
		floats = append(floats, f)
		return
	}
	err = c.query(sql, onDataRow)
	return
}

func (c *conn) SelectAllFloat32(sql string) (floats []float32, err error) {
	floats = make([]float32, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		var f float64
		f, parseError = strconv.ParseFloat(c.rxDataRowFirstValue(r), 32)
		floats = append(floats, float32(f))
		return
	}
	err = c.query(sql, onDataRow)
	return
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

func (c *conn) rxDataRowFirstValue(r *messageReader) (s string) {
	r.readInt16() // ignore field count

	// TODO - handle nulls
	size := r.readInt32()
	s = r.readByteString(size)
	return s
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
