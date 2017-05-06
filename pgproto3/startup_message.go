package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	protocolVersionNumber = 196608 // 3.0
	sslRequestNumber      = 80877103
)

type StartupMessage struct {
	ProtocolVersion uint32
	Parameters      map[string]string
}

func (*StartupMessage) Frontend() {}

func (dst *StartupMessage) Decode(src []byte) error {
	if len(src) < 4 {
		return fmt.Errorf("startup message too short")
	}

	dst.ProtocolVersion = binary.BigEndian.Uint32(src)
	rp := 4

	if dst.ProtocolVersion == sslRequestNumber {
		return fmt.Errorf("can't handle ssl connection request")
	}

	if dst.ProtocolVersion != protocolVersionNumber {
		return fmt.Errorf("Bad startup message version number. Expected %d, got %d", protocolVersionNumber, dst.ProtocolVersion)
	}

	dst.Parameters = make(map[string]string)
	for {
		idx := bytes.IndexByte(src[rp:], 0)
		if idx < 0 {
			return &invalidMessageFormatErr{messageType: "StartupMesage"}
		}
		key := string(src[rp : rp+idx])
		rp += idx + 1

		idx = bytes.IndexByte(src[rp:], 0)
		if idx < 0 {
			return &invalidMessageFormatErr{messageType: "StartupMesage"}
		}
		value := string(src[rp : rp+idx])
		rp += idx + 1

		dst.Parameters[key] = value

		if len(src[rp:]) == 1 {
			if src[rp] != 0 {
				return fmt.Errorf("Bad startup message last byte. Expected 0, got %d", src[rp])
			}
			break
		}
	}

	return nil
}

func (src *StartupMessage) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}
	buf.Write(bigEndian.Uint32(0))
	buf.Write(bigEndian.Uint32(src.ProtocolVersion))
	for k, v := range src.Parameters {
		buf.WriteString(k)
		buf.WriteByte(0)
		buf.WriteString(v)
		buf.WriteByte(0)
	}
	buf.WriteByte(0)

	binary.BigEndian.PutUint32(buf.Bytes()[0:4], uint32(buf.Len()))

	return buf.Bytes(), nil
}

func (src *StartupMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type            string
		ProtocolVersion uint32
		Parameters      map[string]string
	}{
		Type:            "StartupMessage",
		ProtocolVersion: src.ProtocolVersion,
		Parameters:      src.Parameters,
	})
}
