package pgproto3

import (
	"encoding/binary"
	"encoding/json"

	"github.com/jackc/pgx/v5/internal/pgio"
)

type NegotiateProtocolVersion struct {
	NewestMinorProtocol uint32
	UnrecognizedOptions []string
}

// Backend identifies this message as sendable by the PostgreSQL backend.
func (*NegotiateProtocolVersion) Backend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *NegotiateProtocolVersion) Decode(src []byte) error {
	if len(src) < 8 {
		return &invalidMessageLenErr{messageType: "NegotiateProtocolVersion", expectedLen: 8, actualLen: len(src)}
	}

	dst.NewestMinorProtocol = binary.BigEndian.Uint32(src[:4])
	optionCount := int(binary.BigEndian.Uint32(src[4:8]))

	rp := 8
	dst.UnrecognizedOptions = make([]string, 0, optionCount)
	for i := 0; i < optionCount; i++ {
		if rp >= len(src) {
			return &invalidMessageFormatErr{messageType: "NegotiateProtocolVersion"}
		}
		end := rp
		for end < len(src) && src[end] != 0 {
			end++
		}
		if end >= len(src) {
			return &invalidMessageFormatErr{messageType: "NegotiateProtocolVersion"}
		}
		dst.UnrecognizedOptions = append(dst.UnrecognizedOptions, string(src[rp:end]))
		rp = end + 1
	}

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *NegotiateProtocolVersion) Encode(dst []byte) ([]byte, error) {
	dst, sp := beginMessage(dst, 'v')
	dst = pgio.AppendUint32(dst, src.NewestMinorProtocol)
	dst = pgio.AppendUint32(dst, uint32(len(src.UnrecognizedOptions)))
	for _, option := range src.UnrecognizedOptions {
		dst = append(dst, option...)
		dst = append(dst, 0)
	}
	return finishMessage(dst, sp)
}

// MarshalJSON implements encoding/json.Marshaler.
func (src NegotiateProtocolVersion) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type                string
		NewestMinorProtocol uint32
		UnrecognizedOptions []string
	}{
		Type:                "NegotiateProtocolVersion",
		NewestMinorProtocol: src.NewestMinorProtocol,
		UnrecognizedOptions: src.UnrecognizedOptions,
	})
}

// UnmarshalJSON implements encoding/json.Unmarshaler.
func (dst *NegotiateProtocolVersion) UnmarshalJSON(data []byte) error {
	var msg struct {
		NewestMinorProtocol uint32
		UnrecognizedOptions []string
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}

	dst.NewestMinorProtocol = msg.NewestMinorProtocol
	dst.UnrecognizedOptions = msg.UnrecognizedOptions
	return nil
}
