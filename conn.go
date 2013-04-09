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
	rowDesc       rowDescription    // current query rowDescription
	buf           []byte            // work buffer to avoid constant alloc and dealloc
	pid           int32             // backend pid
	secretKey     int32             // key to use to send a cancel query message to the server
	runtimeParams map[string]string // parameters that have been reported by the server
}

func Connect(options map[string]string) (c *conn, err error) {
	c = new(conn)

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

	msg := newStartupMessage()
	msg.options["user"] = "jack"
	c.txStartupMessage(msg)

	var response interface{}
	response, err = c.processMsg()

	for {
		response, err = c.processMsg()
		if err != nil {
			return nil, err
		}
		fmt.Println(response)
		if _, ok := response.(*readyForQuery); ok {
			break
		}
	}

	fmt.Println(c.runtimeParams)

	return c, nil
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

	var response interface{}
	for {
		response, err = c.processMsg()
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		fmt.Println(response)
		if _, ok := response.(*readyForQuery); ok {
			break
		}
	}

	return nil, err
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

func (c *conn) processMsg() (msg interface{}, err error) {
	var t byte
	var r *messageReader
	t, r, err = c.rxMsg()
	if err != nil {
		return
	}

	return c.parseMsg(t, r)
}

// Processes messages that are not exclusive to one context such as
// authentication or query response. The response to these messages
// is the same regardless of when they occur.
func (c *conn) processContextFreeMsg(t byte, r *messageReader) (err error) {
	switch t {
	case 'S':
		c.rxParameterStatus(r)
		return nil
	default:
		return fmt.Errorf("Received unknown message type: %c", t)
	}

	panic("Unreachable")

}

func (c *conn) parseMsg(t byte, r *messageReader) (msg interface{}, err error) {
	switch t {
	case 'K':
		c.rxBackendKeyData(r)
		return nil, nil
	case 'R':
		return c.rxAuthenticationX(r)
	case 'Z':
		return c.rxReadyForQuery(r), nil
	case 'T':
		return c.rxRowDescription(r)
	case 'D':
		return c.rxDataRow(r)
	case 'C':
		return c.rxCommandComplete(r), nil
	default:
		return nil, c.processContextFreeMsg(t, r)
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

func (c *conn) rxAuthenticationX(r *messageReader) (msg interface{}, err error) {
	code := r.readInt32()
	switch code {
	case 0:
		return &authenticationOk{}, nil
	default:
		return nil, errors.New("Received unknown authentication message")
	}

	panic("Unreachable")
}

func (c *conn) rxParameterStatus(r *messageReader) {
	key := r.readString()
	value := r.readString()
	c.runtimeParams[key] = value
}

func (c *conn) rxBackendKeyData(r *messageReader) {
	c.pid = r.readInt32()
	c.secretKey = r.readInt32()
}

func (c *conn) rxReadyForQuery(r *messageReader) (msg *readyForQuery) {
	msg = new(readyForQuery)
	msg.txStatus = r.readByte()
	return
}

func (c *conn) rxRowDescription(r *messageReader) (msg *rowDescription, err error) {
	fieldCount := r.readInt16()
	c.rowDesc.fields = make([]fieldDescription, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		f := &c.rowDesc.fields[i]
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

func (c *conn) rxDataRow(r *messageReader) (row map[string]string, err error) {
	fieldCount := r.readInt16()

	if fieldCount != int16(len(c.rowDesc.fields)) {
		return nil, fmt.Errorf("Received DataRow with %d fields, expected %d fields", fieldCount, c.rowDesc.fields)
	}

	row = make(map[string]string, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		// TODO - handle nulls
		size := r.readInt32()
		fmt.Println(size)
		row[c.rowDesc.fields[i].name] = r.readByteString(size)
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
