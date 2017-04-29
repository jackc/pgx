package pgproto3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/chunkreader"
)

type Frontend struct {
	cr *chunkreader.ChunkReader
	w  io.Writer
}

func NewFrontend(r io.Reader, w io.Writer) (*Frontend, error) {
	cr := chunkreader.NewChunkReader(r)
	return &Frontend{cr: cr, w: w}, nil
}

func (b *Frontend) Send(msg FrontendMessage) error {
	return errors.New("not implemented")
}

func (b *Frontend) Receive() (BackendMessage, error) {
	backendMessages := map[byte]BackendMessage{
		'1': &ParseComplete{},
		'2': &BindComplete{},
		'3': &CloseComplete{},
		'A': &NotificationResponse{},
		'C': &CommandComplete{},
		'd': &CopyData{},
		'D': &DataRow{},
		'E': &ErrorResponse{},
		'G': &CopyInResponse{},
		'H': &CopyOutResponse{},
		'I': &EmptyQueryResponse{},
		'K': &BackendKeyData{},
		'n': &NoData{},
		'N': &NoticeResponse{},
		'R': &Authentication{},
		'S': &ParameterStatus{},
		't': &ParameterDescription{},
		'T': &RowDescription{},
		'V': &FunctionCallResponse{},
		'W': &CopyBothResponse{},
		'Z': &ReadyForQuery{},
	}

	header, err := b.cr.Next(5)
	if err != nil {
		return nil, err
	}

	msgType := header[0]
	bodyLen := int(binary.BigEndian.Uint32(header[1:])) - 4

	msgBody, err := b.cr.Next(bodyLen)
	if err != nil {
		return nil, err
	}

	if msg, ok := backendMessages[msgType]; ok {
		err = msg.UnmarshalBinary(msgBody)
		return msg, err
	}

	return nil, fmt.Errorf("unknown message type: %c", msgType)
}
