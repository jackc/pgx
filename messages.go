package pqx

import (
	"encoding/binary"
	"io"
)

const (
	protocolVersionNumber = 196608 // 3.0
)

type startupMsg struct {
	options map[string] string
}

func newStartupMsg() *startupMsg {
	return &startupMsg{map[string] string{}}
}

func (self *startupMsg) WriteTo(w io.Writer) (n int64, err error) {
	buf := make([]byte, 8, 128)
	binary.BigEndian.PutUint32(buf[4:8], uint32(protocolVersionNumber))
	for key, value := range self.options {
		buf = append(buf, key...)
		buf = append(buf, 0)
		buf = append(buf, value...)
		buf = append(buf, 0)
	}
	buf = append(buf, ("\000")...)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(buf)))

	var n32 int
	n32, err = w.Write(buf)
	return int64(n32), err
}