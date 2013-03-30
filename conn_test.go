package pqx

import (
	// "encoding/binary"
	"net"
	"testing"
)

func TestXxx(t *testing.T) {
	conn, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		// handle error
	}

	msg := newStartupMsg()
	msg.options["user"] = "jack"

	msg.WriteTo(conn)


	buf := make([]byte, 128)

	num, _ := conn.Read(buf)
	println(string(buf[0:1]))
	println(num)
}
