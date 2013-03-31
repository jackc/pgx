package pqx

import (
	"encoding/binary"
	"fmt"
)

const (
	protocolVersionNumber = 196608 // 3.0
)

type startupMessage struct {
	options map[string]string
}

func newStartupMessage() *startupMessage {
	return &startupMessage{map[string]string{}}
}

func (self *startupMessage) Bytes() (buf []byte) {
	buf = make([]byte, 8, 128)
	binary.BigEndian.PutUint32(buf[4:8], uint32(protocolVersionNumber))
	for key, value := range self.options {
		buf = append(buf, key...)
		buf = append(buf, 0)
		buf = append(buf, value...)
		buf = append(buf, 0)
	}
	buf = append(buf, ("\000")...)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(buf)))
	return buf
}

type authenticationOk struct {
}

func (self *authenticationOk) String() string {
	return "AuthenticationOk"
}

type parameterStatus struct {
	name  string
	value string
}

func (self *parameterStatus) String() string {
	return fmt.Sprintf("ParameterStatus %s: %s", self.name, self.value)
}
