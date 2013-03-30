package pqx

import (
	"encoding/binary"
	"net"
)

type conn struct {
	conn net.Conn
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

	// conn, err := net.Dial("tcp", "localhost:5432")

	msg := newStartupMessage()
	msg.options["user"] = "jack"
	c.conn.Write(msg.Bytes())

	buf := make([]byte, 512)

	num, _ := c.conn.Read(buf)
	println(string(buf[0:1]))
	println(binary.BigEndian.Uint32(buf[1:5]))
	println(binary.BigEndian.Uint32(buf[5:9]))
	println(num)

	return c, nil
}
