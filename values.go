package pgx

import (
	"github.com/jackc/pgx/v5/internal/anynil"
	"github.com/jackc/pgx/v5/internal/pgio"
	"github.com/jackc/pgx/v5/pgtype"
)

// PostgreSQL format codes
const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

func convertSimpleArgument(m *pgtype.Map, arg interface{}) (interface{}, error) {
	if anynil.Is(arg) {
		return nil, nil
	}

	buf, err := m.Encode(0, TextFormatCode, arg, []byte{})
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	return string(buf), nil
}

func encodeCopyValue(m *pgtype.Map, buf []byte, oid uint32, arg interface{}) ([]byte, error) {
	if anynil.Is(arg) {
		return pgio.AppendInt32(buf, -1), nil
	}

	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	argBuf, err := m.Encode(oid, BinaryFormatCode, arg, buf)
	if err != nil {
		return nil, err
	}
	if argBuf != nil {
		buf = argBuf
		pgio.SetInt32(buf[sp:], int32(len(buf[sp:])-4))
	}
	return buf, nil
}
