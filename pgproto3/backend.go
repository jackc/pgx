package pgproto3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/chunkreader"
)

type Backend struct {
	cr *chunkreader.ChunkReader
	w  io.Writer

	// Frontend message flyweights
	bind            Bind
	describe        Describe
	execute         Execute
	parse           Parse
	passwordMessage PasswordMessage
	query           Query
	sync            Sync
	terminate       Terminate
}

func NewBackend(r io.Reader, w io.Writer) (*Backend, error) {
	cr := chunkreader.NewChunkReader(r)
	return &Backend{cr: cr, w: w}, nil
}

func (b *Backend) Send(msg BackendMessage) error {
	return errors.New("not implemented")
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
	case 'D':
		msg = &b.describe
	case 'E':
		msg = &b.execute
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
		return nil, fmt.Errorf("unknown message type: %c", msgType)
	}

	msgBody, err := b.cr.Next(bodyLen)
	if err != nil {
		return nil, err
	}

	err = msg.Decode(msgBody)
	return msg, err
}
