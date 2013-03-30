package pqx

import (
	"encoding/binary"
	"net"
	"testing"
)

func TestXxx(t *testing.T) {
	conn, err := net.Dial("unix", "/private/tmp/.s.PGSQL.5432")
	// conn, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		// handle error
	}

	msg := newStartupMessage()
	msg.options["user"] = "jack"

	msg.WriteTo(conn)


	buf := make([]byte, 512)

	num, _ := conn.Read(buf)
	println(string(buf[0:1]))
	println(binary.BigEndian.Uint32(buf[1:5]))
	println(binary.BigEndian.Uint32(buf[5:9]))
	println(num)
}
