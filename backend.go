package pgproto3

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

// Backend acts as a server for the PostgreSQL wire protocol version 3.
type Backend struct {
	cr ChunkReader
	w  io.Writer

	// Frontend message flyweights
	bind            Bind
	_close          Close
	copyFail        CopyFail
	describe        Describe
	execute         Execute
	flush           Flush
	parse           Parse
	passwordMessage PasswordMessage
	query           Query
	startupMessage  StartupMessage
	sync            Sync
	terminate       Terminate

	bodyLen    int
	msgType    byte
	partialMsg bool
}

// NewBackend creates a new Backend.
func NewBackend(cr ChunkReader, w io.Writer) (*Backend, error) {
	return &Backend{cr: cr, w: w}, nil
}

// Send sends a message to the frontend.
func (b *Backend) Send(msg BackendMessage) error {
	_, err := b.w.Write(msg.Encode(nil))
	return err
}

// ReceiveStartupMessage receives the initial startup message. This method is used of the normal Receive method
// because StartupMessage and SSLRequest are "special" and do not include the message type as the first byte.
func (b *Backend) ReceiveStartupMessage() (*StartupMessage, error) {
	buf, err := b.cr.Next(4)
	if err != nil {
		return nil, err
	}
	msgSize := int(binary.BigEndian.Uint32(buf) - 4)

	buf, err = b.cr.Next(msgSize)
	if err != nil {
		return nil, err
	}

	err = b.startupMessage.Decode(buf)
	if err != nil {
		return nil, err
	}

	return &b.startupMessage, nil
}

// Receive receives a message from the frontend.
func (b *Backend) Receive() (FrontendMessage, error) {
	if !b.partialMsg {
		header, err := b.cr.Next(5)
		if err != nil {
			return nil, err
		}

		b.msgType = header[0]
		b.bodyLen = int(binary.BigEndian.Uint32(header[1:])) - 4
		b.partialMsg = true
	}

	var msg FrontendMessage
	switch b.msgType {
	case 'B':
		msg = &b.bind
	case 'C':
		msg = &b._close
	case 'D':
		msg = &b.describe
	case 'E':
		msg = &b.execute
	case 'f':
		msg = &b.copyFail
	case 'H':
		msg = &b.flush
	case 'P':
		msg = &b.parse
	case 'p':
		msg = &b.passwordMessage
	case 'Q':
		msg = &b.query
	case 'S':
		msg = &b.sync
	case 'X':
		msg = &b.terminate
	default:
		return nil, errors.Errorf("unknown message type: %c", b.msgType)
	}

	msgBody, err := b.cr.Next(b.bodyLen)
	if err != nil {
		return nil, err
	}

	b.partialMsg = false

	err = msg.Decode(msgBody)
	return msg, err
}
