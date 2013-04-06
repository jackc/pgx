package pqx

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

type conn struct {
	conn net.Conn // the underlying TCP or unix domain socket connection
	rowDesc rowDescription // current query rowDescription
	buf  []byte   // work buffer to avoid constant alloc and dealloc
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
	var body []byte
	t, body, err = c.rxMsg()
	if err != nil {
		return
	}

	return c.parseMsg(t, body)
}

func (c *conn) parseMsg(t byte, body []byte) (msg interface{}, err error) {
	switch t {
	case 'K':
		return c.rxBackendKeyData(body), nil
	case 'R':
		return c.rxAuthenticationX(body)
	case 'S':
		return c.rxParameterStatus(body)
	case 'Z':
		return c.rxReadyForQuery(body), nil
	case 'T':
		return c.rxRowDescription(body)
	case 'D':
		return c.rxDataRow(body)
	case 'C':
		return c.rxCommandComplete(body), nil
	default:
		return nil, fmt.Errorf("Received unknown message type: %c", t)
	}

	panic("Unreachable")
}

func (c *conn) rxMsg() (t byte, body []byte, err error) {
	var bodySize int32
	t, bodySize, err = c.rxMsgHeader()
	if err != nil {
		return
	}

	body, err = c.rxMsgBody(bodySize)
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

func (c *conn) rxAuthenticationX(buf []byte) (msg interface{}, err error) {
	code := binary.BigEndian.Uint32(buf[:4])
	switch code {
	case 0:
		return &authenticationOk{}, nil
	default:
		return nil, errors.New("Received unknown authentication message")
	}

	panic("Unreachable")
}

func (c *conn) rxParameterStatus(buf []byte) (msg *parameterStatus, err error) {
	msg = new(parameterStatus)

	r := bufio.NewReader(bytes.NewReader(buf))
	msg.name, err = r.ReadString(0)
	if err != nil {
		return
	}

	msg.value, err = r.ReadString(0)
	return
}

func (c *conn) rxBackendKeyData(buf []byte) (msg *backendKeyData) {
	msg = new(backendKeyData)
	msg.pid = int32(binary.BigEndian.Uint32(buf[:4]))
	msg.secretKey = int32(binary.BigEndian.Uint32(buf[4:8]))
	return
}

func (c *conn) rxReadyForQuery(buf []byte) (msg *readyForQuery) {
	msg = new(readyForQuery)
	msg.txStatus = buf[0]
	return
}

func (c *conn) rxRowDescription(buf []byte) (msg *rowDescription, err error) {
	r := newMessageReader(buf)
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

func (c *conn) rxDataRow(buf []byte) (row map[string]string, err error) {
	r := newMessageReader(buf)
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


func (c *conn) rxCommandComplete(buf []byte) string {
	r := newMessageReader(buf)
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
