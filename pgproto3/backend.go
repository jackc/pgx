package pgproto3

import (
	"encoding/binary"
	"io"

	"github.com/jackc/pgx/chunkreader"
	"github.com/pkg/errors"
)

type Backend struct {
	cr *chunkreader.ChunkReader
	w  io.Writer

	// Frontend message flyweights
	bind            Bind
	_close          Close
	describe        Describe
	execute         Execute
	flush           Flush
	parse           Parse
	passwordMessage PasswordMessage
	query           Query
	startupMessage  StartupMessage
	sync            Sync
	terminate       Terminate
}

func NewBackend(r io.Reader, w io.Writer) (*Backend, error) {
	cr := chunkreader.NewChunkReader(r)
	return &Backend{cr: cr, w: w}, nil
}

func (b *Backend) Send(msg BackendMessage) error {
	_, err := b.w.Write(msg.Encode(nil))
	return err
}

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

func (b *Backend) Receive() (FrontendMessage, error) {
	header, err := b.cr.Next(5)
	if err != nil {
		return nil, err
	}

	msgType := header[0]
	bodyLen := int(binary.BigEndian.Uint32(header[1:])) - 4

	var msg FrontendMessage
	switch msgType {
	case 'B':
		msg = &b.bind
	case 'C':
		msg = &b._close
	case 'D':
		msg = &b.describe
	case 'E':
		msg = &b.execute
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
		return nil, errors.Errorf("unknown message type: %c", msgType)
	}

	msgBody, err := b.cr.Next(bodyLen)
	if err != nil {
		return nil, err
	}

	err = msg.Decode(msgBody)
	return msg, err
}
