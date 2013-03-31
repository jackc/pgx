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
	c.conn.Write(msg.Bytes())

	var response interface{}
	response, err = c.rxMsg()
	fmt.Println(err)
	fmt.Println(response)

	for {
		response, err = c.rxMsg()
		if err != nil {
			break
		}
		fmt.Println(response)
	}

	fmt.Println(err)

	return c, nil
}

func (c *conn) rxMsg() (msg interface{}, err error) {
	var t byte
	var bodySize int32
	t, bodySize, err = c.rxMsgHeader()
	if err != nil {
		return nil, err
	}

	var buf []byte
	if buf, err = c.rxMsgBody(bodySize); err != nil {
		return nil, err
	}

	switch t {
	case 'R':
		return c.rxAuthenticationX(buf)
	case 'S':
		return c.rxParameterStatus(buf)
	default:
		return nil, fmt.Errorf("Received unknown message type: %c", t)
	}

	panic("Unreachable")
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
	if int(bodySize) <= cap(c.buf) {
		buf = c.buf[:bodySize]
	} else {
		buf = make([]byte, bodySize)
	}

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
