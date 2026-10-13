package pgx

import (
	"errors"

	"github.com/jackc/pgx/v5/internal/pgio"
	"github.com/jackc/pgx/v5/pgtype"
)

// PostgreSQL format codes
const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

func convertSimpleArgument(m *pgtype.Map, arg any) (any, error) {
	buf, err := m.Encode(0, TextFormatCode, arg, []byte{})
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	return string(buf), nil
}

func encodeCopyValue(m *pgtype.Map, buf []byte, oid uint32, arg any) ([]byte, error) {
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	argBuf, err := m.Encode(oid, BinaryFormatCode, arg, buf)
	if err != nil {
		if argBuf2, err2 := tryScanStringCopyValueThenEncode(m, buf, oid, arg); err2 == nil {
			argBuf = argBuf2
		} else {
			return nil, err
		}
	}

	if argBuf != nil {
		buf = argBuf
		pgio.SetInt32(buf[sp:], int32(len(buf[sp:])-4))
	}
	return buf, nil
}

func tryScanStringCopyValueThenEncode(m *pgtype.Map, buf []byte, oid uint32, arg any) ([]byte, error) {
	s, ok := arg.(string)
	if !ok {
		textBuf, err := m.Encode(oid, TextFormatCode, arg, nil)
		if err != nil {
			return nil, errors.New("not a string and cannot be encoded as text")
		}
		s = string(textBuf)
	}

	// Check if this is an array type. If so, use an ArraySetter to preserve
	// array dimensions during CopyFrom.
	if typ, ok := m.TypeForOID(oid); ok {
		if _, ok := typ.Codec.(*pgtype.ArrayCodec); ok {
			var arr pgtype.Array[any]
			if err := m.Scan(oid, TextFormatCode, []byte(s), &arr); err == nil {
				return m.Encode(oid, BinaryFormatCode, arr, buf)
			}
		}
	}

	var v any
	err := m.Scan(oid, TextFormatCode, []byte(s), &v)
	if err != nil {
		return nil, err
	}

	return m.Encode(oid, BinaryFormatCode, v, buf)
}
